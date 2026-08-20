"""federation egress（`relay.delivery.dispatch_once` への追加ステップ、relay-v2-wire-api.md §6 拡張）。

owner 側の fan-out（既存 `streams.post_stream_message`、無変更）が作る
`target_type = 'stream'` の outbox 行のうち `member_identity` が `"sub@handle"` 形式
（federation peer 宛、`@` を含む）のものを、署名付き HTTP `POST` で対応する peer へ配達する。
既存の SSE dispatch / subscription レーンとは独立した dispatcher step であり、
`dispatch_once` の DB トランザクション（`db_conn`、cycle 末尾で一括 commit）を共有する。

## 配達単位とレーン

配達単位は `(stream_id, peer_handle)` ごとの直列レーンである。1 レーン内は `publish_id`
順に処理し、先頭の `publish_id` の配達が失敗（retry 対象 / DLQ 行き）している間は同一
cycle 内で後続の `publish_id` を送らない（順序保証）。成功が続く限り同一 cycle 内で
複数 `publish_id` を連続処理し、最初の非成功（retry または DLQ）で当該レーンの処理を
打ち切って次のレーンへ進む。

1 つの `publish_id` に複数の `member_identity`（同一 peer 宛の複数 sub、例:
`orch@bob` と `helper@bob`）が紐づく場合は 1 回の HTTP POST に集約する
（envelope の `to_members` に全 sub を列挙）。

## restart-safe 性

`stream_registry`（in-memory、relay 再起動で消える）に stream が存在しない場合、その
レーンは今 cycle では一切手を付けずスキップする（送信もしない、revoked peer の DLQ 化も
しない）。`_sweep_stream_permanent_errors`（`relay.delivery`）と同じ restart-safe パターン
（`stream_registry.get(stream_id) is None` なら誤爆回避のためスキップ）を踏襲する。

## 応答コード対応

| 応答 | 結果 |
|---|---|
| 202 | outbox 行 DELETE（責任移転） |
| 410 | 即 DLQ（`PeerStreamGone`） |
| 413 | 即 DLQ（`PeerRejectedTooLarge`） |
| 400（body の `code` が `FederationEnvelopeDecryptError`） | 即 DLQ（`PeerDecryptFailed`）。
  鍵が揃うか本文サイズが縮まない限り再送しても直らないため retryable にしない |
| 400（それ以外、JSON parse 失敗時含む） | retryable backoff |
| 429 | `Retry-After` に従い backoff |
| 404 / 401 / 5xx / 接続不能 / タイムアウト / その他未分類 | retryable backoff（一時的障害として扱う。
  401 は時刻ずれ・署名不正等の原因を区別せず一律この扱いとする） |

retry の backoff は `outbox.next_attempt_at` / `attempt_count` 列（migration 0001 定義済みだが
本モジュールが初の利用者）で管理し、Full Jitter 方式（`relay_sdk.backoff.full_jitter`、
base=1s・cap=300s）で計算する。`attempt_count` に上限は設けず、24h retain 超過は既存の
`relay.delivery._sweep_retain_exceeded` に委ねる（federation egress 専用の上限を新設しない）。

## replica 側（reply 方向）の owner 相対 id 解決

reply 方向（replica stream からの投函）で送る envelope の `origin_stream_id` は、送信元
自身のローカル stream_id（replica id）ではなく owner 側の元の stream_id でなければならない。
この対応は `StreamRecord` の `origin_peer` / `origin_stream_id` 属性（inbound 側で replica
生成時に設定される）から読む。本モジュールはこれらの属性が未設定でも動作するよう
`getattr` で defensive に参照する（属性が無ければ owner 側送信とみなしローカル stream_id を
そのまま使う）。

## body の暗号化（JWE）

envelope の `body`（メッセージ本文）は、自分に `Settings.jwe_private_key_pem` が設定され
かつ宛先 peer に暗号化用公開鍵（`peers.enc_key_jwk`）が pin 済みの場合、ECDH-ES + A256GCM
の compact JWE（`federation_peers.encrypt_envelope_body`）にして `body_jwe` フィールドで
送る。どちらか一方でも欠けている場合は互換のため平文 `body` を送る（新規ロールアウト・
片側未対応 peer との共存を壊さないフォールバック）。`origin_stream_id` / `origin_publish_id`
/ `from_sub` / `to_members` はいずれの場合も平文のまま送る（配達ルーティングに必要な
メタデータであり、暗号化するとルーティング自体が機能しなくなるため対象外）。

平文が `federation_peers.MAX_ENVELOPE_PLAINTEXT_BYTES` を超える場合、`encrypt_envelope_body`
は暗号化を試みず `EnvelopeTooLargeForEncryptionError` を送出する。この場合ネットワーク送信
自体を行わず、当該 `publish_id` を直接 DLQ（`PeerBodyTooLargeForEncryption`）へ回す（暗号化
しても相手側の復号が必ず失敗するサイズのため、送っても無駄になる）。
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

from relay import federation_auth, federation_net, federation_peers, observability
from relay.config import Settings
from relay.errors import FEDERATION_ENVELOPE_DECRYPT_FAILED
from relay_sdk.backoff import full_jitter

# Full Jitter backoff パラメータ（relay_sdk 側 outbox dispatcher の既定値と同一、
# plan「指数バックオフ上限300秒」）。
FEDERATION_EGRESS_BACKOFF_BASE_SECONDS = 1.0
FEDERATION_EGRESS_BACKOFF_CAP_SECONDS = 300.0

# federation egress 専用の DLQ error_code（`relay.delivery` の DLQ_ERROR_* と同一 namespace、
# dlq.error_code 列の値）。
DLQ_ERROR_PEER_STREAM_GONE = "PeerStreamGone"
DLQ_ERROR_PEER_REJECTED_TOO_LARGE = "PeerRejectedTooLarge"
DLQ_ERROR_PEER_REVOKED = "PeerRevoked"
DLQ_ERROR_PEER_DECRYPT_FAILED = "PeerDecryptFailed"
DLQ_ERROR_BODY_TOO_LARGE_FOR_ENCRYPTION = "PeerBodyTooLargeForEncryption"

_OUTCOME_SUCCESS = "success"
_OUTCOME_RETRY = "retry"
_OUTCOME_DLQ = "dlq"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _add_seconds_iso(seconds: float) -> str:
    return (_now() + timedelta(seconds=max(seconds, 0.0))).strftime("%Y-%m-%dT%H:%M:%SZ")


def _split_member_identity(member_identity: str) -> tuple[str, str] | None:
    """`"sub@handle"` を `(sub, handle)` に分解する。`@` が無ければ `None`。

    `sub` は空文字列（`"@handle"`、reply 方向で owner peer 自体を指す表記）を許容する。
    """
    if "@" not in member_identity:
        return None
    sub, _, handle = member_identity.partition("@")
    return sub, handle


def _parse_retry_after(response: httpx.Response) -> float | None:
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def lookup_publisher_identity(db_conn: sqlite3.Connection, publish_id: int) -> str | None:
    """`publish_log.publisher_identity` を `publish_id` で引く。

    local 由来（`identity.id` のみ、`@` を含まない）と federation 由来
    （`{from_sub}@{peer.handle}` 形式）の両方をそのまま返す。`relay.delivery` の
    `_build_event_data` からも共有で使う。
    """
    row = db_conn.execute(
        "SELECT publisher_identity FROM publish_log WHERE publish_id = ?", (publish_id,)
    ).fetchone()
    return row["publisher_identity"] if row is not None else None


def _dlq_federation_row(
    db_conn: sqlite3.Connection, row: sqlite3.Row, *, error_code: str, app_state: Any = None
) -> None:
    """outbox 行 1 件を DLQ へ移す（`relay.delivery._move_to_dlq` と同一パターン）。

    `delivery.py` を import すると `dispatch_once` からの呼び出しと循環 import になるため、
    小さな INSERT+DELETE をここに複製する（意図的な重複、`relay.delivery` の docstring
    参照）。
    """
    dead_at = _now_iso()
    db_conn.execute(
        "INSERT INTO dlq"
        " (target_type, subscription_id, stream_id, member_identity, publish_id, payload,"
        " labels, error_code, dead_at)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        (
            row["target_type"],
            row["subscription_id"],
            row["stream_id"],
            row["member_identity"],
            row["publish_id"],
            row["payload"],
            row["labels"],
            error_code,
            dead_at,
        ),
    )
    db_conn.execute("DELETE FROM outbox WHERE id = ?", (row["id"],))
    if app_state is not None:
        observability.record_event(
            app_state,
            "outbox_dead",
            level="warning",
            publish_id=row["publish_id"],
            target_type=row["target_type"],
            stream_id=row["stream_id"],
            member_identity=row["member_identity"],
            error_code=error_code,
        )
        observability.inc_metric(app_state, "relay_outbox_dead_total")


def _classify_response(response: httpx.Response) -> tuple[str, str | None, float | None]:
    """peer からの応答を `(outcome, dlq_error_code, retry_after_seconds)` に分類する。"""
    status = response.status_code
    if status == 202:
        return _OUTCOME_SUCCESS, None, None
    if status == 410:
        return _OUTCOME_DLQ, DLQ_ERROR_PEER_STREAM_GONE, None
    if status == 413:
        return _OUTCOME_DLQ, DLQ_ERROR_PEER_REJECTED_TOO_LARGE, None
    if status == 429:
        return _OUTCOME_RETRY, None, _parse_retry_after(response)
    if status == 400:
        # 復号失敗（FederationEnvelopeDecryptError）は鍵が揃うか本文サイズが縮まない限り
        # 再送しても直らないため DLQ に回す。それ以外の 400（JSON parse 失敗・code 不一致を
        # 含む）は fail-safe に従来通り retryable のままにする。
        try:
            body = response.json()
        except Exception:
            body = None
        if isinstance(body, dict) and body.get("code") == FEDERATION_ENVELOPE_DECRYPT_FAILED:
            return _OUTCOME_DLQ, DLQ_ERROR_PEER_DECRYPT_FAILED, None
        return _OUTCOME_RETRY, None, None
    # 404 / 401 / 5xx / その他未分類は一律 retryable（一時的障害として扱う。401 は
    # ts_skew・署名不正等の原因を区別しない、plan 明記のユーザー裁定）。
    return _OUTCOME_RETRY, None, None


async def _send_envelope(
    client: httpx.AsyncClient,
    *,
    locator: str,
    path: str,
    envelope: dict[str, Any],
    origin_fp: str,
    destination_fp: str,
    private_key_pem: str,
    allow_private_locators: bool,
) -> tuple[str, str | None, float | None]:
    """1 envelope を署名付き POST する。

    Returns:
        `(outcome, dlq_error_code, retry_after_seconds)`。`_classify_response` と同じ形。
    """
    try:
        federation_net.validate_locator(locator, allow_private=allow_private_locators)
    except federation_net.LocatorRejected:
        # locator 拒否は peer 側の応答ではなく自側のガードだが、pin 済み locator が
        # 一時的に解決不能（DNS 一時障害等）なケースと区別できないため retryable とする。
        return _OUTCOME_RETRY, None, None

    body_bytes = json.dumps(envelope, ensure_ascii=False).encode("utf-8")
    headers = federation_auth.sign_federation_request(
        method="POST",
        path=path,
        body=body_bytes,
        origin_fp=origin_fp,
        destination_fp=destination_fp,
        private_key_pem=private_key_pem,
    )
    headers["Content-Type"] = "application/json"
    url = f"{locator.rstrip('/')}{path}"

    try:
        response = await client.post(url, content=body_bytes, headers=headers)
    except httpx.TimeoutException:
        return _OUTCOME_RETRY, None, None
    except httpx.TransportError:
        return _OUTCOME_RETRY, None, None

    return _classify_response(response)


def _resolve_path_stream_id(record: Any, stream_id: str, peer_handle: str) -> str:
    """envelope / URL path に使う「owner 相対 id」を解決する。

    reply 方向（このレーンの stream が `peer_handle` からの replica）では
    `record.origin_stream_id`（owner 側の元 stream_id）を使う。owner 側送信、または
    `origin_peer`/`origin_stream_id` 属性が未設定（inbound 側未実装環境）の場合は
    ローカル stream_id をそのまま使う。
    """
    origin_peer = getattr(record, "origin_peer", None)
    origin_stream_id = getattr(record, "origin_stream_id", None)
    if origin_peer == peer_handle and origin_stream_id:
        return origin_stream_id
    return stream_id


def _group_by_publish_id(
    rows: list[sqlite3.Row],
) -> list[tuple[int, list[sqlite3.Row]]]:
    """`publish_id` 昇順の行リストを `(publish_id, [rows...])` の連続 run にグルーピングする。

    呼び出し側は `rows` が既に `publish_id` 昇順であることを保証すること。
    """
    groups: list[tuple[int, list[sqlite3.Row]]] = []
    current_pid: int | None = None
    current: list[sqlite3.Row] = []
    for row in rows:
        pid = row["publish_id"]
        if pid != current_pid:
            if current:
                groups.append((current_pid, current))  # type: ignore[arg-type]
            current_pid = pid
            current = []
        current.append(row)
    if current:
        groups.append((current_pid, current))  # type: ignore[arg-type]
    return groups


async def _process_lane(
    app_state: Any,
    db_conn: sqlite3.Connection,
    client: httpx.AsyncClient,
    settings: Settings,
    stream_registry: Any,
    *,
    stream_id: str,
    peer_handle: str,
    rows: list[sqlite3.Row],
    own_fingerprint: str,
) -> None:
    # restart-safe: stream が registry に不在（再起動直後の空 registry を含む）なら、
    # 送信も revoked sweep もこの cycle では一切行わない（_sweep_stream_permanent_errors
    # と同じ誤爆回避パターン）。
    record = stream_registry.get(stream_id)
    if record is None:
        return

    peer = federation_peers.get_peer_by_handle(settings.db_path, peer_handle)
    if peer is None:
        # 未知 handle（構造的には put_member 検証で発生しないはずだが防御的に no-op）。
        return

    if peer["revoked_at"] is not None:
        # revoked peer 宛の残存行は全て DLQ へ（先頭以外も含め、レーン全体を払い出す）。
        for row in rows:
            _dlq_federation_row(
                db_conn, row, error_code=DLQ_ERROR_PEER_REVOKED, app_state=app_state
            )
        return

    path_stream_id = _resolve_path_stream_id(record, stream_id, peer_handle)
    path = f"/federation/streams/{path_stream_id}/messages"
    now_iso = _now_iso()

    for publish_id, group_rows in _group_by_publish_id(rows):
        head = group_rows[0]
        next_attempt_at = head["next_attempt_at"]
        if next_attempt_at is not None and next_attempt_at > now_iso:
            break  # まだ backoff 中。レーン停止（後続 publish_id も送らない）。

        from_sub = lookup_publisher_identity(db_conn, publish_id)
        to_members = [row["member_identity"].partition("@")[0] for row in group_rows]
        body_text = bytes(head["payload"]).decode("utf-8")
        envelope: dict[str, Any] = {
            "origin_stream_id": path_stream_id,
            "origin_publish_id": publish_id,
            "from_sub": from_sub,
            "to_members": to_members,
        }
        # body のみ暗号化対象（メタデータはルーティングに要るため常に平文、モジュール
        # docstring「body の暗号化（JWE）」参照）。自分の暗号化鍵と宛先の pin 済み
        # 暗号化鍵の両方が揃っているときだけ暗号化し、揃わなければ平文にフォールバックする。
        peer_enc_key_jwk = peer["enc_key_jwk"]
        if settings.jwe_private_key_pem and peer_enc_key_jwk is not None:
            try:
                envelope["body_jwe"] = federation_peers.encrypt_envelope_body(
                    body_text, public_key_jwk=peer_enc_key_jwk
                )
            except federation_peers.EnvelopeTooLargeForEncryptionError:
                # 暗号化しても相手側の復号が必ず失敗するサイズのため、ネットワーク送信を
                # 試みず直接 DLQ へ回す（先頭が dead 化されたのでこのレーンは打ち切る）。
                for row in group_rows:
                    _dlq_federation_row(
                        db_conn,
                        row,
                        error_code=DLQ_ERROR_BODY_TOO_LARGE_FOR_ENCRYPTION,
                        app_state=app_state,
                    )
                break
        else:
            envelope["body"] = body_text

        outcome, dlq_error_code, retry_after = await _send_envelope(
            client,
            locator=peer["locator"],
            path=path,
            envelope=envelope,
            origin_fp=own_fingerprint,
            destination_fp=peer["fingerprint"],
            private_key_pem=settings.jws_private_key_pem,
            allow_private_locators=settings.federation_allow_private_locators,
        )

        if outcome == _OUTCOME_SUCCESS:
            for row in group_rows:
                db_conn.execute("DELETE FROM outbox WHERE id = ?", (row["id"],))
            observability.inc_metric(app_state, "relay_push_delivered_total", lane="federation")
            continue  # 同一レーン内、次の publish_id へ進む（成功が続く限り継続）。

        if outcome == _OUTCOME_DLQ:
            for row in group_rows:
                _dlq_federation_row(
                    db_conn, row, error_code=dlq_error_code, app_state=app_state
                )
            break  # 先頭が dead 化された。このレーンは今 cycle はここで打ち切る。

        # retry: next_attempt_at / attempt_count を更新し、当該レーンを停止する。
        attempt_count = head["attempt_count"]
        backoff_seconds = (
            retry_after
            if retry_after is not None
            else full_jitter(
                FEDERATION_EGRESS_BACKOFF_BASE_SECONDS,
                FEDERATION_EGRESS_BACKOFF_CAP_SECONDS,
                attempt_count,
            )
        )
        next_attempt_iso = _add_seconds_iso(backoff_seconds)
        for row in group_rows:
            db_conn.execute(
                "UPDATE outbox SET attempt_count = attempt_count + 1, next_attempt_at = ?"
                " WHERE id = ?",
                (next_attempt_iso, row["id"]),
            )
        observability.record_event(
            app_state,
            "federation_egress_send_failed",
            level="warning",
            stream_id=stream_id,
            peer=peer_handle,
            publish_id=publish_id,
            attempt_count=attempt_count + 1,
        )
        break  # 先頭失敗。このレーンは今 cycle はここで打ち切る。


async def dispatch_federation_egress(
    app_state: Any,
    db_conn: sqlite3.Connection,
    settings: Settings,
    stream_registry: Any,
) -> None:
    """dispatch_once の federation egress step。

    `member_identity` に `@` を含む未 ack `target_type='stream'` outbox 行を
    `(stream_id, peer_handle)` レーンごとに集約し、各レーンを直列処理する。
    """
    if not settings.jws_private_key_pem:
        return  # federation マシン鍵未設定 = federation 無効（fail-closed）。

    rows = db_conn.execute(
        "SELECT * FROM outbox WHERE target_type = 'stream' AND member_identity LIKE '%@%'"
        " ORDER BY stream_id, publish_id"
    ).fetchall()
    if not rows:
        return

    lanes: dict[tuple[str, str], list[sqlite3.Row]] = {}
    lane_order: list[tuple[str, str]] = []
    for row in rows:
        parsed = _split_member_identity(row["member_identity"])
        if parsed is None:
            continue
        _, handle = parsed
        key = (row["stream_id"], handle)
        if key not in lanes:
            lanes[key] = []
            lane_order.append(key)
        lanes[key].append(row)

    if not lanes:
        return

    own_fingerprint = federation_peers.compute_fingerprint(
        federation_peers.public_jwk_from_pem(settings.jws_private_key_pem)
    )

    async with federation_net.build_async_client() as client:
        for stream_id, peer_handle in lane_order:
            try:
                await _process_lane(
                    app_state,
                    db_conn,
                    client,
                    settings,
                    stream_registry,
                    stream_id=stream_id,
                    peer_handle=peer_handle,
                    rows=lanes[(stream_id, peer_handle)],
                    own_fingerprint=own_fingerprint,
                )
            except Exception:  # noqa: BLE001 — dispatcher は落ちてはいけない常駐処理
                observability.record_event(
                    app_state,
                    "federation_egress_lane_error",
                    level="warning",
                    stream_id=stream_id,
                    peer=peer_handle,
                )
