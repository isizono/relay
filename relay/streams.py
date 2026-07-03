"""stream（場）関連 endpoint（relay-v2-wire-api.md §3）。

`POST /streams` / `GET /streams/{stream_id}` / `DELETE /streams/{stream_id}` /
`POST /streams/{stream_id}/messages` / `PUT`・`DELETE`・`GET /streams/{stream_id}/members` /
`POST /streams/{stream_id}/ack` を実装する。

stream の状態（membership 含む）は relay-v2-wire-api.md §0 の R1 原則により in-memory
実装とする（`StreamRegistry`）。SQLite には streams / memberships table を持たない。
registry は `request.app.state.stream_registry` に app インスタンスごとに遅延生成され、
同一 app を共有する他モジュール（例: delivery.py の `GET /events` が「識別 identity の
member 場を自動含む」判定をする際）からも `request.app.state.stream_registry` として
参照できる。

`relay.identity.require_authn` を各 handler に適用し、`request.state.identity` から
呼び出し元 identity を得る。structural authZ（write 権限 membership の照合、
identity-authz.md §2.2）もここで行う。

投函（`POST /streams/{stream_id}/messages`）は「outbox 永続化完了 = 202」（wire-api.md
§3.2, §6.1）の条件を満たすため、read 権限を持つ member 宛の outbox エントリ作成を
`relay.db` 経由で直接 SQLite に書く（transactional outbox）。publish_id の採番は
`publish_log` への INSERT 1 件で行う。

`idempotency_key` の 15 分 dedup（wire-api.md §6.3）は `relay.idempotency` の共通
ヘルパーを使う（subscription レーンの `POST /publish` と同じ dedup store を app 単位で
共有する）。`ttl`（メッセージ単位の retain 上書き）・`default_ttl`（stream 単位の retain
default）は `migrations/0002-outbox-expires-at.sql` で追加した `outbox.expires_at` 列に
enqueue 時点で計算した期限を書き込み、DLQ sweep（`relay.delivery`）がこれを見て retain
超過を検出する。
"""
from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Literal

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from relay import db, idempotency, observability
from relay.config import DEFAULT_RETAIN_SECONDS, MAX_RETAIN_SECONDS, MIN_RETAIN_SECONDS, Settings
from relay.errors import (
    INVALID_REQUEST,
    MEMBERSHIP_REQUIRED,
    STREAM_ALREADY_EXISTS,
    STREAM_GONE,
    STREAM_NOT_FOUND,
    error_response,
)
from relay.identity import Identity, require_authn

Access = Literal["read", "write", "read_write"]
_VALID_ACCESS: frozenset[str] = frozenset({"read", "write", "read_write"})


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# in-memory stream / membership registry
# ---------------------------------------------------------------------------


@dataclass
class StreamRecord:
    stream_id: str
    created_at: str
    default_ttl: int | None
    state: Literal["open", "closed"] = "open"
    members: dict[str, Access] = field(default_factory=dict)


class StreamRegistry:
    """stream 本体と membership を保持する in-memory registry。

    `threading.Lock` で単純に排他する。relay-v2-wire-api.md §0 の R1 原則により disk
    永続化しない（relay 再起動で消える。docs/ARCHITECTURE.md 参照）。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._streams: dict[str, StreamRecord] = {}

    def create(
        self, stream_id: str, creator_identity: str, default_ttl: int | None
    ) -> StreamRecord | None:
        """新規 stream を作成する。既存なら None を返す（呼び出し側で 409 にする）。

        作成者は bootstrap として write 権限を持つ member として自動登録される
        （wire-api.md §3.1）。
        """
        with self._lock:
            if stream_id in self._streams:
                return None
            record = StreamRecord(
                stream_id=stream_id, created_at=_now_iso(), default_ttl=default_ttl
            )
            record.members[creator_identity] = "write"
            self._streams[stream_id] = record
            return record

    def get(self, stream_id: str) -> StreamRecord | None:
        with self._lock:
            return self._streams.get(stream_id)

    def close(self, stream_id: str) -> None:
        """新規投函を止める（close）。存在しない stream_id は no-op（呼び出し側で 404 判定済み前提）。

        close は冪等: 既に closed な stream への再 close は状態を変えず成功扱いにする。
        """
        with self._lock:
            record = self._streams.get(stream_id)
            if record is not None:
                record.state = "closed"

    def put_member(self, stream_id: str, identity: str, access: Access) -> None:
        with self._lock:
            record = self._streams.get(stream_id)
            if record is not None:
                record.members[identity] = access

    def delete_member(self, stream_id: str, identity: str) -> None:
        with self._lock:
            record = self._streams.get(stream_id)
            if record is not None:
                record.members.pop(identity, None)

    def list_members(self, stream_id: str) -> list[dict[str, str]] | None:
        with self._lock:
            record = self._streams.get(stream_id)
            if record is None:
                return None
            return [{"identity": i, "access": a} for i, a in record.members.items()]

    def has_write_access(self, stream_id: str, identity: str) -> bool:
        with self._lock:
            record = self._streams.get(stream_id)
            if record is None:
                return False
            return record.members.get(identity) in ("write", "read_write")

    def has_read_access(self, stream_id: str, identity: str) -> bool:
        with self._lock:
            record = self._streams.get(stream_id)
            if record is None:
                return False
            return record.members.get(identity) in ("read", "read_write")

    def read_members(self, stream_id: str) -> list[str]:
        with self._lock:
            record = self._streams.get(stream_id)
            if record is None:
                return []
            return [i for i, a in record.members.items() if a in ("read", "read_write")]

    def count(self) -> int:
        """現在の stream 数（`GET /status` の `streams_count` 用）。"""
        with self._lock:
            return len(self._streams)

    def read_streams_for_identity(self, identity: str) -> list[str]:
        """`identity` が read 権限を持つ stream_id の一覧を返す。

        `relay.delivery` の dispatcher が「接続した identity が read 権限を持つ member
        である stream のメッセージも同じ SSE 接続に流す」（wire-api.md §5.5）を実装する
        際に使う。
        """
        with self._lock:
            return [
                stream_id
                for stream_id, record in self._streams.items()
                if record.members.get(identity) in ("read", "read_write")
            ]


def get_registry_from_state(app_state) -> StreamRegistry:
    """`request.app.state.stream_registry` を遅延初期化して返す。

    app インスタンス（テストでは `create_app(settings)` 呼び出しごと）にスコープされ、
    テスト間の状態リークを防ぐ。`request` を持たない呼び出し元（dispatcher 等）からも
    `app.state` を直接渡して呼べる。
    """
    registry = getattr(app_state, "stream_registry", None)
    if registry is None:
        registry = StreamRegistry()
        app_state.stream_registry = registry
    return registry


def get_registry(request: Request) -> StreamRegistry:
    """`request` 経由で呼ぶ場合の `get_registry_from_state` の薄いラッパー。"""
    return get_registry_from_state(request.app.state)


def _get_registry(request: Request) -> StreamRegistry:
    return get_registry_from_state(request.app.state)


def _get_connection(request: Request) -> sqlite3.Connection:
    settings: Settings = request.app.state.settings
    return db.get_connection(settings.db_path)


# ---------------------------------------------------------------------------
# バリデーションヘルパ
# ---------------------------------------------------------------------------


def _validate_retain_seconds(value: object, *, field_name: str) -> tuple[int | None, Response | None]:
    """`default_ttl` / `ttl` の値検証（min 60 / max 86400、wire-api.md §6.4）。

    Returns:
        (検証済みの値 または None, エラー Response または None) のタプル。
    """
    if value is None:
        return None, None
    if isinstance(value, bool) or not isinstance(value, int):
        return None, error_response(
            400, INVALID_REQUEST, f"{field_name} は整数（秒）で指定してください"
        )
    if not (MIN_RETAIN_SECONDS <= value <= MAX_RETAIN_SECONDS):
        return None, error_response(
            400,
            INVALID_REQUEST,
            f"{field_name} は {MIN_RETAIN_SECONDS}〜{MAX_RETAIN_SECONDS} 秒の範囲で指定してください",
        )
    return value, None


async def _read_json_body(request: Request) -> tuple[dict, Response | None]:
    try:
        body = await request.json()
    except Exception:
        return {}, error_response(400, INVALID_REQUEST, "リクエストボディが不正な JSON です")
    if not isinstance(body, dict):
        return {}, error_response(400, INVALID_REQUEST, "リクエストボディは JSON object でなければなりません")
    return body, None


# ---------------------------------------------------------------------------
# endpoint: POST /streams
# ---------------------------------------------------------------------------


@require_authn
async def create_stream(request: Request) -> Response:
    identity: Identity = request.state.identity
    body, err = await _read_json_body(request)
    if err is not None:
        return err

    stream_id = body.get("stream_id")
    if not isinstance(stream_id, str) or not stream_id:
        return error_response(400, INVALID_REQUEST, "stream_id は必須の非空文字列です")

    default_ttl, err = _validate_retain_seconds(body.get("default_ttl"), field_name="default_ttl")
    if err is not None:
        return err

    registry = _get_registry(request)
    record = registry.create(stream_id, identity.id, default_ttl)
    if record is None:
        return error_response(
            409, STREAM_ALREADY_EXISTS, f"stream '{stream_id}' は既に存在します"
        )
    return JSONResponse(
        {"stream_id": record.stream_id, "created_at": record.created_at}, status_code=201
    )


# ---------------------------------------------------------------------------
# endpoint: GET /streams/{stream_id}
# ---------------------------------------------------------------------------


@require_authn
async def get_stream(request: Request) -> Response:
    stream_id = request.path_params["stream_id"]
    registry = _get_registry(request)
    record = registry.get(stream_id)
    if record is None:
        return error_response(404, STREAM_NOT_FOUND, f"stream '{stream_id}' が見つかりません")
    # read は全許可（identity-authz.md §2.1）。membership 照合はしない。
    return JSONResponse(
        {"stream_id": record.stream_id, "state": record.state, "created_at": record.created_at}
    )


# ---------------------------------------------------------------------------
# endpoint: DELETE /streams/{stream_id}（close）
# ---------------------------------------------------------------------------


@require_authn
async def close_stream(request: Request) -> Response:
    identity: Identity = request.state.identity
    stream_id = request.path_params["stream_id"]
    registry = _get_registry(request)
    record = registry.get(stream_id)
    if record is None:
        return error_response(404, STREAM_NOT_FOUND, f"stream '{stream_id}' が見つかりません")
    if not registry.has_write_access(stream_id, identity.id):
        return error_response(
            403, MEMBERSHIP_REQUIRED, f"stream '{stream_id}' の write 権限がありません"
        )
    registry.close(stream_id)
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# endpoint: POST /streams/{stream_id}/messages
# ---------------------------------------------------------------------------


@require_authn
async def post_stream_message(request: Request) -> Response:
    identity: Identity = request.state.identity
    stream_id = request.path_params["stream_id"]
    registry = _get_registry(request)

    record = registry.get(stream_id)
    if record is None:
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="stream_not_found"
        )
        return error_response(404, STREAM_NOT_FOUND, f"stream '{stream_id}' が見つかりません")
    if not registry.has_write_access(stream_id, identity.id):
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="membership_required"
        )
        return error_response(
            403, MEMBERSHIP_REQUIRED, f"stream '{stream_id}' の write 権限がありません"
        )
    if record.state == "closed":
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="stream_gone"
        )
        return error_response(410, STREAM_GONE, f"stream '{stream_id}' は close 済みです")

    body, err = await _read_json_body(request)
    if err is not None:
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="invalid_request"
        )
        return err

    message_body = body.get("body")
    if not isinstance(message_body, str) or message_body == "":
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="invalid_request"
        )
        return error_response(400, INVALID_REQUEST, "body は必須の非空文字列です")

    _ttl, err = _validate_retain_seconds(body.get("ttl"), field_name="ttl")
    if err is not None:
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="invalid_request"
        )
        return err

    idempotency_key = body.get("idempotency_key")
    if idempotency_key is not None and not isinstance(idempotency_key, str):
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="invalid_request"
        )
        return error_response(400, INVALID_REQUEST, "idempotency_key は文字列で指定してください")

    dedup_key = idempotency.build_key(
        lane="stream",
        publisher_identity=identity.id,
        explicit_key=idempotency_key,
        scope=stream_id,
        body=message_body,
    )
    store = idempotency.get_store(request.app.state)
    existing_publish_id = store.check(dedup_key)
    if existing_publish_id is not None:
        return JSONResponse(
            {"publish_id": existing_publish_id, "matched_members": 0}, status_code=202
        )

    retain_seconds = _ttl if _ttl is not None else (record.default_ttl or DEFAULT_RETAIN_SECONDS)
    expires_at = (datetime.now(timezone.utc) + timedelta(seconds=retain_seconds)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    read_members = registry.read_members(stream_id)
    payload = message_body.encode("utf-8")
    now = _now_iso()

    conn = _get_connection(request)
    try:
        cur = conn.execute(
            "INSERT INTO publish_log (lane, stream_id, publisher_identity, enqueued_at)"
            " VALUES ('stream', ?, ?, ?)",
            (stream_id, identity.id, now),
        )
        publish_id = cur.lastrowid
        for member_identity in read_members:
            conn.execute(
                "INSERT INTO outbox"
                " (target_type, stream_id, member_identity, publish_id, payload, enqueued_at,"
                " expires_at)"
                " VALUES ('stream', ?, ?, ?, ?, ?, ?)",
                (stream_id, member_identity, publish_id, payload, now, expires_at),
            )
        conn.commit()
    finally:
        conn.close()
    store.register(dedup_key, publish_id)

    observability.record_event(
        request.app.state,
        "publish_received",
        lane="stream",
        publish_id=publish_id,
        publisher_identity=identity.id,
        stream_id=stream_id,
        matched_members=len(read_members),
    )
    observability.inc_metric(
        request.app.state, "relay_publish_received_total", publisher_identity=identity.id
    )
    return JSONResponse(
        {"publish_id": publish_id, "matched_members": len(read_members)}, status_code=202
    )


# ---------------------------------------------------------------------------
# endpoint: membership API
# ---------------------------------------------------------------------------


@require_authn
async def put_member(request: Request) -> Response:
    identity: Identity = request.state.identity
    stream_id = request.path_params["stream_id"]
    registry = _get_registry(request)

    record = registry.get(stream_id)
    if record is None:
        return error_response(404, STREAM_NOT_FOUND, f"stream '{stream_id}' が見つかりません")
    if not registry.has_write_access(stream_id, identity.id):
        return error_response(
            403, MEMBERSHIP_REQUIRED, f"stream '{stream_id}' の write 権限がありません"
        )

    body, err = await _read_json_body(request)
    if err is not None:
        return err

    target_identity = body.get("identity")
    if not isinstance(target_identity, str) or not target_identity:
        return error_response(400, INVALID_REQUEST, "identity は必須の非空文字列です")
    access = body.get("access")
    if access not in _VALID_ACCESS:
        return error_response(
            400, INVALID_REQUEST, "access は read / write / read_write のいずれかです"
        )

    registry.put_member(stream_id, target_identity, access)
    return JSONResponse({}, status_code=200)


@require_authn
async def delete_member(request: Request) -> Response:
    identity: Identity = request.state.identity
    stream_id = request.path_params["stream_id"]
    registry = _get_registry(request)

    record = registry.get(stream_id)
    if record is None:
        return error_response(404, STREAM_NOT_FOUND, f"stream '{stream_id}' が見つかりません")

    target_identity = request.query_params.get("identity")
    if not target_identity:
        return error_response(400, INVALID_REQUEST, "identity クエリパラメータは必須です")

    # 自分自身の membership 削除（離脱）は本人であれば常に許可する（identity-authz.md §2.2）。
    if target_identity != identity.id and not registry.has_write_access(stream_id, identity.id):
        return error_response(
            403, MEMBERSHIP_REQUIRED, f"stream '{stream_id}' の write 権限がありません"
        )

    registry.delete_member(stream_id, target_identity)
    return Response(status_code=204)


@require_authn
async def list_members(request: Request) -> Response:
    stream_id = request.path_params["stream_id"]
    registry = _get_registry(request)
    members = registry.list_members(stream_id)
    if members is None:
        return error_response(404, STREAM_NOT_FOUND, f"stream '{stream_id}' が見つかりません")
    # read は全許可（identity-authz.md §2.1）。membership 照合はしない。
    return JSONResponse({"members": members})


# ---------------------------------------------------------------------------
# endpoint: POST /streams/{stream_id}/ack
# ---------------------------------------------------------------------------


@require_authn
async def ack_stream(request: Request) -> Response:
    identity: Identity = request.state.identity
    stream_id = request.path_params["stream_id"]
    registry = _get_registry(request)

    # 場が不在、または呼び出し元が read 権限を持つ member でない場合は同一の 404
    # （wire-api.md §5.6 / §5.7 の存在露呈回避）。
    if not registry.has_read_access(stream_id, identity.id):
        return error_response(
            404, STREAM_NOT_FOUND, f"stream '{stream_id}' が見つからないか read 権限がありません"
        )

    body, err = await _read_json_body(request)
    if err is not None:
        return err

    up_to_publish_id = body.get("up_to_publish_id")
    if isinstance(up_to_publish_id, bool) or not isinstance(up_to_publish_id, int):
        return error_response(400, INVALID_REQUEST, "up_to_publish_id は整数で指定してください")

    conn = _get_connection(request)
    try:
        conn.execute(
            "DELETE FROM outbox"
            " WHERE target_type = 'stream' AND stream_id = ? AND member_identity = ?"
            " AND publish_id <= ?",
            (stream_id, identity.id, up_to_publish_id),
        )
        conn.commit()
    finally:
        conn.close()

    observability.record_event(
        request.app.state,
        "ack_received",
        lane="stream",
        stream_id=stream_id,
        member_identity=identity.id,
        up_to_publish_id=up_to_publish_id,
    )
    observability.inc_metric(request.app.state, "relay_ack_received_total")
    return JSONResponse({}, status_code=200)


routes: list[Route] = [
    Route("/streams", create_stream, methods=["POST"]),
    Route("/streams/{stream_id}", get_stream, methods=["GET"]),
    Route("/streams/{stream_id}", close_stream, methods=["DELETE"]),
    Route("/streams/{stream_id}/messages", post_stream_message, methods=["POST"]),
    Route("/streams/{stream_id}/members", put_member, methods=["PUT"]),
    Route("/streams/{stream_id}/members", delete_member, methods=["DELETE"]),
    Route("/streams/{stream_id}/members", list_members, methods=["GET"]),
    Route("/streams/{stream_id}/ack", ack_stream, methods=["POST"]),
]
