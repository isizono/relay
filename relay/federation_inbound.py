"""federation inbound 受信（`POST /federation/streams/{id}/messages`）。

peer authN（`relay.federation_auth.require_federation_authn`）済みの envelope を受け取り、
disk 永続 dedup → stream 解決（owner stream への返信 or replica stream 自動生成）→
membership add-only upsert → publish_log/outbox 書込（local member のみ fan-out）を行う。

federation v1 設計確定版「(b) 詳細設計要点」の「受信(replica側B)」節を実装する。

## replica stream

`{id}`（owner-relative stream_id、`"{creator}:{name}"` 形式）が自分の owner stream でなければ、
`{creator}@{handle}:{name}`（`handle` は認証済み送信元 peer の handle）へ creator を射影した
replica stream を自動生成する。射影に使う `handle` は `require_federation_authn` が検証済みの
`PeerIdentity.handle` であり envelope の自己申告値ではないため、他 peer の replica namespace への
squatting は構造的にできない（違う peer が同じ `{id}` を送っても異なる replica_id に射影される）。

membership（to_members の read_write upsert、`@{handle}` の read_write 再主張）は毎回のリクエストで
再実行する（add-only、既存の read_write member を格下げ・削除しない）。B（受信側）の
`StreamRegistry` は in-memory（relay 再起動で消える）ため、この再主張が復元経路そのものになる
（federation v1 設計確定版「配達モデル」節）。

## dedup

`(origin_fingerprint, origin_publish_id)` を disk 永続 table（`federation_inbound_dedup`、
`migrations/0005-federation-inbound-dedup.sql`）で dedup する。in-memory の 15 分 window
（`relay.idempotency`）と異なり、store-and-forward の retention（outbox 24h）を跨いだ再送も
捕捉する必要があるため disk 永続にしている。

## rate limit

`require_federation_authn` 自身も per-peer rate limit（`app.state.federation_request_rate_limiter`）
を内部で行うが、そちらは検証失敗を一様 401 に畳む（`FederationAuthenticationError`、
`relay.federation_auth` の設計上の意図的な単純化）。rate limit 超過を 429 として区別するには
この endpoint 専用の別 `RateLimiter` インスタンスが要るため、`get_federation_inbound_rate_limiter`
で別途持つ（`relay.streams.post_stream_message` の publish rate limit と同型パターン）。

## body の復号（JWE）

envelope に `body_jwe`（`relay.federation_egress` 参照）が来た場合、`Settings.jwe_private_key_pem`
で復号してから publish_log/outbox に平文で書き込む（local member への配達は既存の
Bearer token 認証済み SSE 経路であり、federation の HTTP 越しの区間だけを暗号化対象と
みなす）。`body`（平文）が来た場合はそのまま使う。復号鍵未設定・alg/enc 不一致・改竄等の
失敗は理由を区別せず 400 で拒否する（`federation_peers.EnvelopeDecryptionError` 参照）。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from relay import db, federation_peers, observability, streams
from relay.config import DEFAULT_RETAIN_SECONDS, Settings
from relay.errors import (
    FEDERATION_ENVELOPE_DECRYPT_FAILED,
    INVALID_REQUEST,
    RATE_LIMIT_EXCEEDED,
    STREAM_GONE,
    STREAM_NOT_FOUND,
    ResourceLimitExceeded,
    error_response,
    resource_limit_response,
)
from relay.federation_auth import PeerIdentity, require_federation_authn
from relay.ratelimit import RateLimiter
from relay.streams import StreamRegistry

# federation message 投函の per-peer rate limit（既定 30 req/s）。federation v1 設計確定版に
# 明記の無い項目（欠落項目、agent 判断）。`require_federation_authn` が先行適用する認証層の
# per-peer rate limit（`relay.federation_auth.DEFAULT_PEER_REQUEST_RATE_LIMIT_PER_SECOND` = 50）
# より確実に小さい値にする。両者を同値にすると認証層とこの endpoint 層の 2 段を両方通す必要が
# 生じ、実効レートが額面の半分（25 req/s）に落ちてしまうため。
DEFAULT_FEDERATION_INBOUND_RATE_LIMIT_PER_SECOND = 30


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_federation_inbound_rate_limiter(app_state) -> RateLimiter:
    """`app.state.federation_inbound_rate_limiter` を遅延初期化して返す。

    `relay.ratelimit.get_publish_rate_limiter` と同型の遅延初期化パターン。
    """
    limiter = getattr(app_state, "federation_inbound_rate_limiter", None)
    if limiter is None:
        limiter = RateLimiter(DEFAULT_FEDERATION_INBOUND_RATE_LIMIT_PER_SECOND)
        app_state.federation_inbound_rate_limiter = limiter
    return limiter


def _project_replica_id(owner_relative_id: str, handle: str) -> str | None:
    """owner-relative stream_id（`{creator}:{name}`）を peer handle で射影した replica id を返す。

    区切り文字が無い、または creator/name のいずれかが空の不正な id は None を返す
    （呼び出し側で 400 にする）。
    """
    creator, sep, name = owner_relative_id.partition(streams.STREAM_ID_SEPARATOR)
    if not sep or not creator or not name:
        return None
    return streams.canonical_stream_id(f"{creator}@{handle}", name)


def _validate_envelope(body: Any, owner_relative_id: str) -> tuple[dict, Response | None]:
    """envelope 必須フィールド（origin_stream_id/origin_publish_id/from_sub/to_members、
    および body か body_jwe のいずれか）を検証する。

    federation v1 設計確定版に明記の無い項目（欠落・型不正の扱い）。既存 `post_stream_message`
    の入力検証パターンを踏襲した agent 判断で、欠落・型不正はすべて 400 とする。
    `origin_stream_id` は URL の `{id}`（`owner_relative_id`）と一致することも要求する
    （egress は同じ owner-relative id を path と envelope の両方に使う設計であり、不一致は
    リクエストの構造不正とみなす）。

    `body`（平文）と `body_jwe`（JWE compact、モジュール docstring「body の復号」参照）は
    どちらか一方だけが必須（`relay.federation_egress` は暗号化可否に応じて排他的に送る）。
    復号自体はここでは行わず、戻り値にどちらが来たかをそのまま残す（呼び出し側が dedup 判定
    後にだけ復号コストを払えるようにするため）。
    """
    if not isinstance(body, dict):
        return {}, error_response(
            400, INVALID_REQUEST, "リクエストボディは JSON object でなければなりません"
        )

    origin_stream_id = body.get("origin_stream_id")
    if not isinstance(origin_stream_id, str) or not origin_stream_id:
        return {}, error_response(400, INVALID_REQUEST, "origin_stream_id は必須の非空文字列です")
    if origin_stream_id != owner_relative_id:
        return {}, error_response(
            400, INVALID_REQUEST, "origin_stream_id が URL の id と一致しません"
        )

    origin_publish_id = body.get("origin_publish_id")
    if isinstance(origin_publish_id, bool) or not isinstance(origin_publish_id, int):
        return {}, error_response(400, INVALID_REQUEST, "origin_publish_id は整数で指定してください")

    from_sub = body.get("from_sub")
    if not isinstance(from_sub, str) or not from_sub:
        return {}, error_response(400, INVALID_REQUEST, "from_sub は必須の非空文字列です")

    to_members = body.get("to_members")
    if (
        not isinstance(to_members, list)
        or not to_members
        or not all(isinstance(m, str) and "@" not in m for m in to_members)
    ):
        return {}, error_response(
            400,
            INVALID_REQUEST,
            "to_members は '@' を含まない文字列からなる非空リストです",
        )

    message_body = body.get("body")
    message_body_jwe = body.get("body_jwe")
    if message_body_jwe is not None:
        if not isinstance(message_body_jwe, str) or message_body_jwe == "":
            return {}, error_response(400, INVALID_REQUEST, "body_jwe は非空文字列です")
        if message_body is not None:
            return {}, error_response(
                400, INVALID_REQUEST, "body と body_jwe は同時に指定できません"
            )
    elif not isinstance(message_body, str) or message_body == "":
        return {}, error_response(400, INVALID_REQUEST, "body は必須の非空文字列です")

    return {
        "origin_stream_id": origin_stream_id,
        "origin_publish_id": origin_publish_id,
        "from_sub": from_sub,
        "to_members": to_members,
        "body": message_body,
        "body_jwe": message_body_jwe,
    }, None


def _resolve_target_stream(
    registry: StreamRegistry, owner_relative_id: str, peer: PeerIdentity
) -> tuple[str, Response | None]:
    """`{id}` を解決する: 自分の owner stream への返信、または replica stream の自動生成。

    - `{id}` が自分の owner stream（`origin_peer is None` = local が作成した非 replica stream）
      なら返信受け入れ。当該 stream に origin peer namespace（`*@{handle}`）の write member が
      居ることを検証し、居なければ 404（存在も権限も同一の 404 に隠す、identity-authz.md の
      404 collapse パターンを踏襲）。
    - それ以外（`{id}` が未知、または replica 自身）なら replica stream を get-or-create する
      （`registry.create` は既存 stream_id に対して None を返す設計なので、その場合は
      get で拾い直す。get-or-create 全体としては冪等）。
    """
    owner_record = registry.get(owner_relative_id)
    if owner_record is not None and owner_record.origin_peer is None:
        if not registry.has_peer_write_member(owner_relative_id, peer.handle):
            return "", error_response(
                404, STREAM_NOT_FOUND, f"stream '{owner_relative_id}' が見つかりません"
            )
        return owner_relative_id, None

    replica_id = _project_replica_id(owner_relative_id, peer.handle)
    if replica_id is None:
        return "", error_response(
            400,
            INVALID_REQUEST,
            "origin_stream_id の形式が不正です（'{creator}:{name}' 形式が必要）",
        )

    record = registry.get(replica_id)
    if record is None:
        try:
            record = registry.create(
                replica_id,
                f"@{peer.handle}",
                None,
                origin_peer=peer.handle,
                origin_stream_id=owner_relative_id,
            )
        except ResourceLimitExceeded as exc:
            return "", resource_limit_response("stream", exc.scope)
        if record is None:
            # create と get の間に別リクエスト（同一 peer からの並行配達）が同一 replica を
            # 作成済み（race）。get し直せば必ず見つかる（evict は close 経由でしか起きず、
            # このパスでは close されない）。
            record = registry.get(replica_id)
    return replica_id, None


@require_federation_authn
async def receive_message(request: Request) -> Response:
    peer: PeerIdentity = request.state.peer_identity
    settings: Settings = request.app.state.settings
    owner_relative_id = request.path_params["id"]
    registry = streams.get_registry_from_state(request.app.state)

    limiter = get_federation_inbound_rate_limiter(request.app.state)
    allowed, retry_after = limiter.allow(peer.fingerprint)
    if not allowed:
        response = error_response(
            429, RATE_LIMIT_EXCEEDED, "federation message 投函のレート制限を超過しました"
        )
        response.headers["Retry-After"] = str(retry_after)
        return response

    raw = await request.body()
    try:
        parsed = json.loads(raw) if raw else None
    except Exception:
        return error_response(400, INVALID_REQUEST, "リクエストボディが不正な JSON です")

    envelope, err = _validate_envelope(parsed, owner_relative_id)
    if err is not None:
        return err

    origin_publish_id = envelope["origin_publish_id"]
    from_sub = envelope["from_sub"]
    to_members = envelope["to_members"]

    conn = db.get_connection(settings.db_path)
    try:
        # (1) dedup: 既受理なら同一 202（federation v1 設計確定版「受信(replica側B)」節）。
        existing = conn.execute(
            "SELECT local_publish_id FROM federation_inbound_dedup"
            " WHERE origin_fingerprint = ? AND origin_publish_id = ?",
            (peer.fingerprint, origin_publish_id),
        ).fetchone()
        if existing is not None:
            return JSONResponse({"publish_id": existing["local_publish_id"]}, status_code=202)

        # (1.5) body 復号: body_jwe が来ていれば ECDH-ES + A256GCM 固定で復号する
        #       （モジュール docstring「body の復号」参照）。dedup 判定の後に置き、
        #       既受理な再送に復号コストを払わない。
        if envelope["body_jwe"] is not None:
            if not settings.jwe_private_key_pem:
                observability.record_event(
                    request.app.state,
                    "federation_envelope_decrypt_unavailable",
                    level="warning",
                    origin_peer=peer.handle,
                )
                return error_response(
                    400,
                    FEDERATION_ENVELOPE_DECRYPT_FAILED,
                    "envelope が暗号化されていますが、復号鍵が未設定です",
                )
            try:
                message_body = federation_peers.decrypt_envelope_body(
                    envelope["body_jwe"], private_key_pem=settings.jwe_private_key_pem
                )
            except federation_peers.EnvelopeDecryptionError:
                observability.record_event(
                    request.app.state,
                    "federation_envelope_decrypt_failed",
                    level="warning",
                    origin_peer=peer.handle,
                )
                return error_response(
                    400, FEDERATION_ENVELOPE_DECRYPT_FAILED, "envelope の復号に失敗しました"
                )
        else:
            message_body = envelope["body"]

        # (2) stream 解決。
        target_stream_id, err = _resolve_target_stream(registry, owner_relative_id, peer)
        if err is not None:
            return err

        record = registry.get(target_stream_id)
        if record is None:
            # 解決直後の極めて稀な race（evict は close 経由のみのためこのパスでは通常発生しない）。
            return error_response(
                503, STREAM_NOT_FOUND, f"stream '{target_stream_id}' の解決に失敗しました"
            )
        if record.state == "closed":
            return error_response(
                410, STREAM_GONE, f"stream '{target_stream_id}' は close 済みです"
            )

        # (3) membership upsert（add-only, read_write）。既存 write/read_write member を
        #     格下げ・削除しない（`registry.put_member` は常に上書き追加のみ）。
        #     空文字列 sub（reply 方向で owner peer 自体を指す "@handle" 表記の partition 結果）
        #     は local member ではないためスキップする。
        for sub in to_members:
            if not sub:
                continue
            registry.put_member(target_stream_id, sub, "read_write")
        registry.put_member(target_stream_id, f"@{peer.handle}", "read_write")

        # (4) 書込: publish_log INSERT →（local member のみ）outbox INSERT → dedup 記録 → commit。
        #     publisher_identity は relay が {from_sub}@{handle} に強制刻印する（body 自己申告の
        #     namespace は無視、federation v1 設計確定版「受信(replica側B)」節）。
        publisher_identity = f"{from_sub}@{peer.handle}"
        local_members = [m for m in registry.read_members(target_stream_id) if "@" not in m]
        retain_seconds = record.default_ttl or DEFAULT_RETAIN_SECONDS
        expires_at = (
            datetime.now(timezone.utc) + timedelta(seconds=retain_seconds)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        payload_bytes = message_body.encode("utf-8")
        now = _now_iso()

        try:
            cur = conn.execute(
                "INSERT INTO publish_log (lane, stream_id, publisher_identity, enqueued_at)"
                " VALUES ('stream', ?, ?, ?)",
                (target_stream_id, publisher_identity, now),
            )
            local_publish_id = cur.lastrowid
            for member_identity in local_members:
                conn.execute(
                    "INSERT INTO outbox"
                    " (target_type, stream_id, member_identity, publish_id, payload, enqueued_at,"
                    " expires_at)"
                    " VALUES ('stream', ?, ?, ?, ?, ?, ?)",
                    (
                        target_stream_id,
                        member_identity,
                        local_publish_id,
                        payload_bytes,
                        now,
                        expires_at,
                    ),
                )
            conn.execute(
                "INSERT INTO federation_inbound_dedup"
                " (origin_fingerprint, origin_publish_id, local_publish_id, created_at)"
                " VALUES (?, ?, ?, ?)",
                (peer.fingerprint, origin_publish_id, local_publish_id, now),
            )
            conn.commit()
        except sqlite3.IntegrityError:
            # race: 別リクエストが同一 (origin_fingerprint, origin_publish_id) を先に確定させた。
            conn.rollback()
            existing = conn.execute(
                "SELECT local_publish_id FROM federation_inbound_dedup"
                " WHERE origin_fingerprint = ? AND origin_publish_id = ?",
                (peer.fingerprint, origin_publish_id),
            ).fetchone()
            if existing is not None:
                return JSONResponse({"publish_id": existing["local_publish_id"]}, status_code=202)
            raise
    finally:
        conn.close()

    observability.record_event(
        request.app.state,
        "federation_message_received",
        lane="stream",
        origin_peer=peer.handle,
        stream_id=target_stream_id,
        origin_publish_id=origin_publish_id,
        local_publish_id=local_publish_id,
        matched_members=len(local_members),
    )
    return JSONResponse({"publish_id": local_publish_id}, status_code=202)


routes: list[Route] = [
    Route("/federation/streams/{id}/messages", receive_message, methods=["POST"]),
]
