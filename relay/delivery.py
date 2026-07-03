"""配達基盤（outbox / SSE / retry / DLQ / server log、relay-v2-wire-api.md §5.5, §6）。

`GET /events`（SSE 多重化購読）、polling dispatcher（未 ack outbox の SELECT → push →
retry → DLQ 化ループ）、DLQ sweep（`dead_at` から 7 日後の物理 DELETE）を実装する。

## アーキテクチャ概要

- `ConnectionManager` が、認証済み identity ごとにアクティブな SSE 接続（`Connection`）を
  保持する。1 つの `Connection` は「その接続が受け取るべき subscription_id 集合
  （`GET /events?subscription_ids=` で指定・ownership 検証済み）」と、`asyncio.Queue`
  （dispatcher → SSE 送信ループ間のバッファ）を持つ。
- dispatcher（`dispatch_once`）はプロセス内シングルトンとして、file lock で排他制御された
  1 つの asyncio task として動く（`relay.app` の lifespan が起動、単一プロセス内
  複数 worker が同じ DB を指す場合の二重 push を防ぐ）。100ms〜1s 間隔で以下を行う:
    1. アクティブな接続ごとに、その接続が担当する delivery target（stream member /
       subscription）の outbox を「前回この接続に push した publish_id より新しいもの」
       だけ SELECT し、push を試みる。push 成功時はカーソル（`Connection.cursor`）を進める。
       接続が新規に張られた直後はカーソルが 0 なので、outbox に残っている未 ack エントリが
       古い順に一括で push される（= 再接続時の暗黙再 push、wire-api.md §6.5）。
    2. push 失敗（接続の `asyncio.Queue` が詰まっている = slow consumer の兆候）時は
       指数バックオフで再試行し（初回 100ms・係数 2・最大 5 回・累積約 3.1 秒、
       wire-api.md §6.4）、それでも失敗したら当該接続を強制切断する（zombie 接続の
       「切断」への正規化）。
    3. DLQ sweep: retain 超過（`outbox.expires_at` 列、`migrations/0002-...`）または
       permanent error（subscription_id が registry に存在しない / lease 切れ）の
       outbox エントリを `dlq` table に移す。
    4. DLQ 物理削除: `dead_at` から 7 日経過した `dlq` 行を DELETE する。
    5. subscription registry 掃除: lease 切れから猶予期間（既定 1 時間）を過ぎた
       subscription を in-memory registry から除去する（無制限メモリ増加の防止）。

- keepalive（30 秒ごとの `: keepalive` コメント行）は SSE 送信側の generator が
  `asyncio.wait_for(queue.get(), timeout=...)` のタイムアウトとして自前で生成する
  （sse-starlette 組み込みの `ping` 機構は無効化し、`send_timeout` による write 失敗検出を
  keepalive にも一様に効かせるため）。
"""
from __future__ import annotations

import asyncio
import contextlib
import fcntl
import json
import os
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

from sse_starlette.event import ServerSentEvent
from sse_starlette.sse import EventSourceResponse
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from relay import db, observability, streams, subscriptions
from relay.config import Settings
from relay.errors import SUBSCRIPTION_GONE, SUBSCRIPTION_NOT_FOUND, error_response
from relay.identity import require_authn

# push retry パラメータ（relay-v2-wire-api.md §6.4 / relay-glossary.md「polling dispatcher」）。
# 初回 100ms・係数 2・最大 5 回。累積約 3.1 秒。
PUSH_RETRY_DELAYS_SECONDS: tuple[float, ...] = (0.1, 0.2, 0.4, 0.8, 1.6)

CONNECTION_QUEUE_MAXSIZE = 256

# sse-starlette の `ping=0` は無効化ではなくビジーループになるため使わない
# (delivery.get_events 参照)。実用上到達しない大きな値で事実上無効化する。
_DISABLE_BUILTIN_PING_INTERVAL_SECONDS = 10_000_000

DLQ_ERROR_RETAIN_EXCEEDED = "RetainExceeded"
DLQ_ERROR_SUBSCRIPTION_UNAVAILABLE = "SubscriptionUnavailable"


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# SSE 接続管理
# ---------------------------------------------------------------------------


@dataclass
class Connection:
    """1 本の `GET /events` SSE 接続。

    `cursor` は delivery target ごとに「この接続へ最後に push した publish_id」を持つ。
    dispatcher はこれより新しいエントリだけを対象に push するため、同じエントリを
    同一接続へ何度も re-push しない（ack 前の再送は「別の」新規接続に対してのみ、
    カーソル 0 から発生する = 再接続時の暗黙再 push）。
    """

    identity: str
    subscription_ids: frozenset[str]
    queue: "asyncio.Queue[dict | None]"
    cursor: dict[str, int] = field(default_factory=dict)
    closed: asyncio.Event = field(default_factory=asyncio.Event)


class ConnectionManager:
    """identity ごとにアクティブな `Connection` を保持する registry。

    `StreamRegistry` / `SubscriptionRegistry` と同じく in-memory・relay 再起動で消える
    （SSE 接続自体が TCP 接続なので再起動を跨いで保持する意味がない）。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._by_identity: dict[str, list[Connection]] = {}

    def register(self, conn: Connection) -> None:
        with self._lock:
            self._by_identity.setdefault(conn.identity, []).append(conn)

    def unregister(self, conn: Connection) -> None:
        with self._lock:
            conns = self._by_identity.get(conn.identity)
            if conns and conn in conns:
                conns.remove(conn)
                if not conns:
                    del self._by_identity[conn.identity]

    def snapshot(self) -> dict[str, list[Connection]]:
        """dispatcher が反復処理するための、現在のアクティブ接続の浅いコピー。"""
        with self._lock:
            return {identity: list(conns) for identity, conns in self._by_identity.items()}


def _get_connection_manager(app_state) -> ConnectionManager:
    manager = getattr(app_state, "connection_manager", None)
    if manager is None:
        manager = ConnectionManager()
        app_state.connection_manager = manager
    return manager


def _get_db_connection(settings: Settings) -> sqlite3.Connection:
    return db.get_connection(settings.db_path)


# ---------------------------------------------------------------------------
# endpoint: GET /events
# ---------------------------------------------------------------------------


def validate_subscription_ids(sub_registry, identity_id: str, subscription_ids: list[str]) -> Response | None:
    """`GET /events` の `subscription_ids=` に対する ownership / lease 検証。

    検証順序は ownership（404）→ lease 状態（410）（wire-api.md §5.7）。問題なければ
    `None` を返す。HTTP 層から切り離してあるため単体テストしやすい。
    """
    for subscription_id in subscription_ids:
        if not sub_registry.is_owner(subscription_id, identity_id):
            return error_response(
                404,
                SUBSCRIPTION_NOT_FOUND,
                f"subscription '{subscription_id}' が見つかりません",
            )
    for subscription_id in subscription_ids:
        if sub_registry.is_lease_expired(subscription_id):
            return error_response(
                410,
                SUBSCRIPTION_GONE,
                f"subscription '{subscription_id}' の lease は切れています",
            )
    return None


async def event_stream(
    conn: Connection, manager: ConnectionManager, settings: Settings, app_state
) -> AsyncIterator[ServerSentEvent]:
    """`conn.queue` を読み、SSE event へ変換して yield する。

    push が無い間は `settings.sse_keepalive_seconds` ごとに `: keepalive` コメント行を
    生成する（wire-api.md §5.5）。`conn.queue` に `None`（強制切断センチネル、
    `_force_disconnect` 参照）が積まれるか `conn.closed` が立つとループを抜ける。
    HTTP 層（`get_events`）から切り離してあるため、実ソケットを使わず単体テストできる。
    """
    try:
        while True:
            if conn.closed.is_set():
                break
            try:
                item = await asyncio.wait_for(
                    conn.queue.get(), timeout=settings.sse_keepalive_seconds
                )
            except asyncio.TimeoutError:
                yield ServerSentEvent(comment="keepalive")
                continue
            if item is None:
                break
            yield ServerSentEvent(
                data=json.dumps(item["data"], ensure_ascii=False),
                event="notification",
                id=str(item["publish_id"]),
            )
    finally:
        manager.unregister(conn)
        observability.record_event(app_state, "sse_disconnected", identity=conn.identity)


@require_authn
async def get_events(request: Request) -> Response:
    identity = request.state.identity
    settings: Settings = request.app.state.settings
    sub_registry = subscriptions.get_registry(request)

    raw_ids = request.query_params.get("subscription_ids", "")
    subscription_ids = [s for s in raw_ids.split(",") if s]

    err = validate_subscription_ids(sub_registry, identity.id, subscription_ids)
    if err is not None:
        return err

    conn = Connection(
        identity=identity.id,
        subscription_ids=frozenset(subscription_ids),
        queue=asyncio.Queue(maxsize=CONNECTION_QUEUE_MAXSIZE),
    )
    manager = _get_connection_manager(request.app.state)
    manager.register(conn)
    observability.record_event(request.app.state, "sse_connected", identity=identity.id)

    # sse-starlette 組み込みの ping は事実上無効化する（keepalive は event_stream 側で
    # 自前生成する。モジュール docstring 参照）。`ping=0` は無効化ではなく
    # `anyio.sleep(0)` のビジーループになる（sse-starlette 側の既知の挙動）ため、
    # 実用上ここに到達しない大きな値を渡す。send_timeout は本物の transport write
    # 詰まり（TCP レベルの zombie 接続）を検出する保険。
    return EventSourceResponse(
        event_stream(conn, manager, settings, request.app.state),
        ping=_DISABLE_BUILTIN_PING_INTERVAL_SECONDS,
        send_timeout=settings.sse_send_timeout_seconds,
    )


# ---------------------------------------------------------------------------
# dispatcher: push 対象の target 一覧構築
# ---------------------------------------------------------------------------


def _stream_target_key(stream_id: str, member_identity: str) -> str:
    return f"stream:{stream_id}:{member_identity}"


def _subscription_target_key(subscription_id: str) -> str:
    return f"sub:{subscription_id}"


def _targets_for_connection(conn: Connection, stream_registry) -> list[tuple[str, str, dict]]:
    """`conn` が担当する delivery target 一覧を `(target_key, target_type, params)` で返す。

    subscription バースト順（wire-api.md「SSE」§4）を保つため、subscription_ids に
    列挙された順 → stream membership の順で並べる。
    """
    targets: list[tuple[str, str, dict]] = []
    for subscription_id in conn.subscription_ids:
        targets.append(
            (
                _subscription_target_key(subscription_id),
                "subscription",
                {"subscription_id": subscription_id},
            )
        )
    for stream_id in stream_registry.read_streams_for_identity(conn.identity):
        targets.append(
            (
                _stream_target_key(stream_id, conn.identity),
                "stream",
                {"stream_id": stream_id, "member_identity": conn.identity},
            )
        )
    return targets


def _select_new_entries(
    db_conn: sqlite3.Connection, target_type: str, params: dict, after_publish_id: int
) -> list[sqlite3.Row]:
    if target_type == "subscription":
        return db_conn.execute(
            "SELECT id, publish_id, payload, labels FROM outbox"
            " WHERE target_type = 'subscription' AND subscription_id = ? AND publish_id > ?"
            " ORDER BY publish_id",
            (params["subscription_id"], after_publish_id),
        ).fetchall()
    return db_conn.execute(
        "SELECT id, publish_id, payload, labels FROM outbox"
        " WHERE target_type = 'stream' AND stream_id = ? AND member_identity = ?"
        " AND publish_id > ?"
        " ORDER BY publish_id",
        (params["stream_id"], params["member_identity"], after_publish_id),
    ).fetchall()


def _build_event_data(target_type: str, params: dict, row: sqlite3.Row) -> dict:
    delivered_at = _now_iso()
    if target_type == "subscription":
        decoded = json.loads(bytes(row["payload"]).decode("utf-8"))
        labels = json.loads(row["labels"]) if row["labels"] else []
        return {
            "delivery_target": f"sub:{params['subscription_id']}",
            "publish_id": row["publish_id"],
            "ref": decoded.get("ref"),
            "labels": labels,
            "title": decoded.get("title"),
            "delivered_at": delivered_at,
        }
    return {
        "delivery_target": f"stream:{params['stream_id']}",
        "publish_id": row["publish_id"],
        "body": bytes(row["payload"]).decode("utf-8"),
        "delivered_at": delivered_at,
    }


async def _force_disconnect(conn: Connection) -> None:
    conn.closed.set()
    try:
        conn.queue.put_nowait(None)
    except asyncio.QueueFull:
        with contextlib.suppress(asyncio.QueueEmpty):
            conn.queue.get_nowait()
        with contextlib.suppress(asyncio.QueueFull):
            conn.queue.put_nowait(None)


async def _push_with_retry(conn: Connection, event_dict: dict) -> bool:
    """`conn.queue` への push を retry-with-backoff する。

    5 回のリトライ（初回 push を含めて最大 6 回試行）すべてで queue が詰まっていたら
    slow consumer とみなし、接続を強制切断する（wire-api.md §6.4）。
    """
    delays = (0.0, *PUSH_RETRY_DELAYS_SECONDS)
    for delay in delays:
        if delay:
            await asyncio.sleep(delay)
        try:
            conn.queue.put_nowait(event_dict)
            return True
        except asyncio.QueueFull:
            continue
    await _force_disconnect(conn)
    return False


async def _dispatch_to_connections(app_state, db_conn: sqlite3.Connection) -> None:
    manager = _get_connection_manager(app_state)
    stream_registry = streams.get_registry_from_state(app_state)

    for identity, conns in manager.snapshot().items():
        for conn in conns:
            if conn.closed.is_set():
                continue
            targets = _targets_for_connection(conn, stream_registry)
            for target_key, target_type, params in targets:
                if conn.closed.is_set():
                    break  # 同一 cycle 内で既に強制切断済みなら残り target は触らない。
                after = conn.cursor.get(target_key, 0)
                rows = _select_new_entries(db_conn, target_type, params, after)
                for row in rows:
                    event_dict = {
                        "publish_id": row["publish_id"],
                        "data": _build_event_data(target_type, params, row),
                    }
                    ok = await _push_with_retry(conn, event_dict)
                    if ok:
                        conn.cursor[target_key] = row["publish_id"]
                    else:
                        break  # 接続が切断された。同一 target 内の残りエントリも打ち切る。


# ---------------------------------------------------------------------------
# DLQ sweep
# ---------------------------------------------------------------------------


def _move_to_dlq(
    db_conn: sqlite3.Connection, row: sqlite3.Row, *, error_code: str, app_state=None
) -> None:
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
            publish_id=row["publish_id"],
            target_type=row["target_type"],
            subscription_id=row["subscription_id"],
            stream_id=row["stream_id"],
            error_code=error_code,
        )


def _sweep_retain_exceeded(db_conn: sqlite3.Connection, app_state=None) -> None:
    """retain 超過（`outbox.expires_at` 経過）のエントリを DLQ に倒す（wire-api.md §6.6）。"""
    now = _now_iso()
    rows = db_conn.execute(
        "SELECT * FROM outbox WHERE expires_at IS NOT NULL AND expires_at <= ?", (now,)
    ).fetchall()
    for row in rows:
        _move_to_dlq(db_conn, row, error_code=DLQ_ERROR_RETAIN_EXCEEDED, app_state=app_state)


def _sweep_permanent_errors(db_conn: sqlite3.Connection, sub_registry, app_state=None) -> None:
    """subscription lane の permanent error（不存在 / lease 切れ）を DLQ に倒す。"""
    subscription_ids = [
        r[0]
        for r in db_conn.execute(
            "SELECT DISTINCT subscription_id FROM outbox WHERE target_type = 'subscription'"
        ).fetchall()
    ]
    for subscription_id in subscription_ids:
        record = sub_registry.get(subscription_id)
        if record is not None and not sub_registry.is_lease_expired(subscription_id):
            continue
        rows = db_conn.execute(
            "SELECT * FROM outbox WHERE target_type = 'subscription' AND subscription_id = ?",
            (subscription_id,),
        ).fetchall()
        for row in rows:
            _move_to_dlq(
                db_conn, row, error_code=DLQ_ERROR_SUBSCRIPTION_UNAVAILABLE, app_state=app_state
            )


def _sweep_dlq_physical_delete(db_conn: sqlite3.Connection, settings: Settings) -> None:
    cutoff = (_now() - timedelta(days=settings.dlq_retention_days)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    db_conn.execute("DELETE FROM dlq WHERE dead_at < ?", (cutoff,))


def _sweep_expired_subscription_registry(
    sub_registry, settings: Settings, app_state=None
) -> None:
    """lease 切れから猶予期間を過ぎた subscription を registry から除去する。

    unsubscribe されないまま放置された subscription による registry の無制限成長を防ぐ
    （`relay.subscriptions.SubscriptionRegistry.evict_expired` docstring 参照）。
    """
    evicted = sub_registry.evict_expired(settings.subscription_registry_retention_seconds)
    if evicted and app_state is not None:
        for subscription_id in evicted:
            observability.record_event(
                app_state, "subscription_registry_evicted", subscription_id=subscription_id
            )


# ---------------------------------------------------------------------------
# dispatcher 本体（polling loop）
# ---------------------------------------------------------------------------


async def dispatch_once(app) -> None:
    """dispatcher の 1 polling cycle。push 試行 + DLQ sweep + DLQ 物理削除を行う。"""
    settings: Settings = app.state.settings
    sub_registry = subscriptions.get_registry_from_state(app.state)

    db_conn = _get_db_connection(settings)
    try:
        await _dispatch_to_connections(app.state, db_conn)
        _sweep_retain_exceeded(db_conn, app_state=app.state)
        _sweep_permanent_errors(db_conn, sub_registry, app_state=app.state)
        _sweep_dlq_physical_delete(db_conn, settings)
        db_conn.commit()
        _sweep_expired_subscription_registry(sub_registry, settings, app_state=app.state)
    finally:
        db_conn.close()


async def run_dispatcher_loop(app) -> None:
    """dispatcher の常駐ループ。`relay.app` の lifespan から起動される。"""
    settings: Settings = app.state.settings
    while True:
        try:
            await dispatch_once(app)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 — dispatcher は落ちてはいけない常駐処理
            observability.record_event(app.state, "dispatcher_error")
        await asyncio.sleep(settings.dispatcher_poll_interval_seconds)


# ---------------------------------------------------------------------------
# dispatcher 単一プロセス enforcement（file lock）
# ---------------------------------------------------------------------------


def try_acquire_dispatcher_lock(lock_path: str) -> int | None:
    """dispatcher 用の排他 file lock を non-blocking で取得する。

    取得できれば file descriptor を返す（呼び出し側がプロセス生存中保持し続ける）。
    既に他プロセスが保持していれば `None`（このプロセスでは dispatcher を起動しない）。
    """
    fd = os.open(lock_path, os.O_CREAT | os.O_RDWR, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        return None
    return fd


def release_dispatcher_lock(fd: int) -> None:
    with contextlib.suppress(OSError):
        fcntl.flock(fd, fcntl.LOCK_UN)
    with contextlib.suppress(OSError):
        os.close(fd)


routes: list[Route] = [
    Route("/events", get_events, methods=["GET"]),
]
