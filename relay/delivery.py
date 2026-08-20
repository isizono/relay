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
    6. stream registry 掃除: close から猶予期間（既定 1 時間）を過ぎ、未配達 outbox が
       drain し切った stream を in-memory registry から除去する（同上の防止、5 と対称）。

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
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator

from sse_starlette.event import ServerSentEvent
from sse_starlette.sse import EventSourceResponse
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from relay import db, federation_egress, observability, streams, subscriptions
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
# 場（stream）レーンの permanent error: 場は生存しているが member が read 権限を喪失した
# （除去された / read_write→write に降格した）ため配達も ack も不能になった状態
# （wire-api.md §6.6）。dlq.error_code 列の値であり、errors.py の HTTP error envelope 用
# error_code（A2A 8 種 + relay 固有最小集合）とは別 namespace。
DLQ_ERROR_STREAM_READ_ACCESS_REVOKED = "StreamReadAccessRevoked"


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
    # ack 未着タイムアウト検知用（`_enforce_ack_timeouts`）。この接続に push 済み
    # （publish_id <= cursor）だが未 ack のまま outbox に残る最古 publish_id と、その floor を
    # 最初に観測した monotonic 時刻。ack が進んで floor が上がる / 全部 ack されて None になると
    # 張り直され、floor が動かないまま猶予を過ぎたら stuck とみなして強制切断する。
    unacked_floor: int | None = None
    unacked_since: float | None = None


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

    def count(self) -> int:
        """アクティブな SSE 接続の総数（`GET /status` / `GET /metrics` 用）。"""
        with self._lock:
            return sum(len(conns) for conns in self._by_identity.values())


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


def _build_event_data(
    target_type: str, params: dict, row: sqlite3.Row, db_conn: sqlite3.Connection
) -> dict:
    """`GET /events` の SSE payload を組み立てる。

    `publisher_identity` は `publish_log`（`row["publish_id"]` で引く）由来で、local 由来
    （`@` を含まない）と federation 由来（`sub@handle` 形式）をそのまま配達先へ伝える
    （relay-v2-wire-api.md §5.5）。publish_log 行が見当たらない場合は `None`。
    """
    delivered_at = _now_iso()
    publisher_identity = federation_egress.lookup_publisher_identity(db_conn, row["publish_id"])
    if target_type == "subscription":
        decoded = json.loads(bytes(row["payload"]).decode("utf-8"))
        labels = json.loads(row["labels"]) if row["labels"] else []
        return {
            "delivery_target": f"sub:{params['subscription_id']}",
            "publish_id": row["publish_id"],
            "ref": decoded.get("ref"),
            "labels": labels,
            "title": decoded.get("title"),
            "publisher_identity": publisher_identity,
            "delivered_at": delivered_at,
        }
    return {
        "delivery_target": f"stream:{params['stream_id']}",
        "publish_id": row["publish_id"],
        "body": bytes(row["payload"]).decode("utf-8"),
        "publisher_identity": publisher_identity,
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


async def _push_with_retry(conn: Connection, event_dict: dict, app_state=None) -> bool:
    """`conn.queue` への push を retry-with-backoff する。

    5 回のリトライ（初回 push を含めて最大 6 回試行）すべてで queue が詰まっていたら
    slow consumer とみなし、接続を強制切断する（wire-api.md §6.4）。強制切断は構造化ログ
    （warning）+ `relay_sse_slow_consumer_disconnects_total` で観測する（§6.4, §7.2）。
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
    if app_state is not None:
        observability.record_event(
            app_state, "sse_slow_consumer_disconnect", level="warning", identity=conn.identity
        )
        observability.inc_metric(app_state, "relay_sse_slow_consumer_disconnects_total")
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
                        "data": _build_event_data(target_type, params, row, db_conn),
                    }
                    ok = await _push_with_retry(conn, event_dict, app_state)
                    if ok:
                        conn.cursor[target_key] = row["publish_id"]
                        observability.inc_metric(
                            app_state, "relay_push_delivered_total", lane=target_type
                        )
                    else:
                        break  # 接続が切断された。同一 target 内の残りエントリも打ち切る。


# ---------------------------------------------------------------------------
# ack 未着タイムアウト（push 済みだが ack が進まない接続の強制切断）
# ---------------------------------------------------------------------------


def _connection_unacked_floor(
    db_conn: sqlite3.Connection, conn: Connection, targets: list[tuple[str, str, dict]]
) -> int | None:
    """`conn` に push 済み（publish_id <= cursor）で未 ack のまま outbox に残る最古 publish_id。

    ack は当該エントリを outbox から削除するため、「push 済み（cursor が越えた）なのに
    まだ outbox に居る」= 未 ack である。全 target を通じた最小値を返す（無ければ None）。
    """
    floor: int | None = None
    for target_key, target_type, params in targets:
        cursor = conn.cursor.get(target_key, 0)
        if cursor <= 0:
            continue
        if target_type == "subscription":
            row = db_conn.execute(
                "SELECT MIN(publish_id) FROM outbox"
                " WHERE target_type = 'subscription' AND subscription_id = ? AND publish_id <= ?",
                (params["subscription_id"], cursor),
            ).fetchone()
        else:
            row = db_conn.execute(
                "SELECT MIN(publish_id) FROM outbox"
                " WHERE target_type = 'stream' AND stream_id = ? AND member_identity = ?"
                " AND publish_id <= ?",
                (params["stream_id"], params["member_identity"], cursor),
            ).fetchone()
        val = row[0] if row is not None else None
        if val is not None:
            floor = val if floor is None else min(floor, val)
    return floor


async def _enforce_ack_timeouts(app_state, db_conn: sqlite3.Connection, settings: Settings) -> None:
    """push は成功しているが ack が進まない接続を強制切断する（wire-api.md §6.4 の別障害モード）。

    slow consumer 切断（`_push_with_retry`、queue backpressure ベース）は「SSE queue に積めない」
    ケースを見るのに対し、こちらは「queue には積めている（= SSE 送信は進んでいる）が subscriber
    側の受信 / ack ループがスタックして ack が返ってこない」ケースを見る。接続ごとに「push 済み
    未 ack エントリの最古 publish_id（floor）」を毎 cycle 観測し、floor が `ack_timeout_seconds`
    の間 1 度も進まない（= その間 1 件も ack されていない）接続を stuck とみなして切断する。
    エントリは outbox に残るため再接続時に resume（§6.5）で回収される。強制切断は warning 構造化
    ログで観測する（Prometheus metric は wire-api.md §7.2 の固定 9 種に含まれないため増設しない）。
    """
    timeout = settings.ack_timeout_seconds
    manager = _get_connection_manager(app_state)
    stream_registry = streams.get_registry_from_state(app_state)
    now = time.monotonic()
    for _identity, conns in manager.snapshot().items():
        for conn in conns:
            if conn.closed.is_set():
                continue
            targets = _targets_for_connection(conn, stream_registry)
            floor = _connection_unacked_floor(db_conn, conn, targets)
            if floor is None:
                # 未 ack の push 済みエントリが無い（全部 ack された / まだ何も push されていない）。
                conn.unacked_floor = None
                conn.unacked_since = None
                continue
            if conn.unacked_floor != floor:
                # floor が動いた = ack 進捗があった or 新たに未 ack エントリを観測した。timer を張り直す。
                conn.unacked_floor = floor
                conn.unacked_since = now
                continue
            if conn.unacked_since is not None and (now - conn.unacked_since) >= timeout:
                await _force_disconnect(conn)
                observability.record_event(
                    app_state,
                    "sse_ack_timeout_disconnect",
                    level="warning",
                    identity=conn.identity,
                    oldest_unacked_publish_id=floor,
                )


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
            level="warning",
            publish_id=row["publish_id"],
            target_type=row["target_type"],
            subscription_id=row["subscription_id"],
            stream_id=row["stream_id"],
            error_code=error_code,
        )
        observability.inc_metric(app_state, "relay_outbox_dead_total")


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


def _sweep_stream_permanent_errors(
    db_conn: sqlite3.Connection, stream_registry, app_state=None
) -> None:
    """stream lane の permanent error（場は生存するが member が read 権限を喪失）を DLQ に倒す。

    subscription lane（`_sweep_permanent_errors`）は subscription_id が registry から消えた
    ことを target 消滅の指標にできるが、stream lane の member_identity は subscription_id
    （再接続毎に使い捨てる UUID）と違い relay 再起動を跨いで安定する識別子であり、「registry に
    居ない」だけでは配達不能を定義できない。そこで「**場が registry に生存しているのに、その
    member が read 権限を持たない**（除去された / read_write→write に降格した）」状態だけを
    permanent error とする（wire-api.md §6.6）。この member 宛の未 ack エントリは §5.6「場レーンは
    membership の存在自体が配達継続の条件」に照らして配達継続の資格を失っており、read 権限が無い
    ため ack（`POST /streams/{id}/ack` は read 権限必須で 404）もできず、放置すると retain 上限まで
    無音の死重として残る。

    「場が registry に生存」を AND 条件に含むことが restart-safe 性の核心である: relay 再起動直後は
    membership registry が空なので `stream_registry.get(stream_id)` が常に None になり、この sweep は
    未配達 outbox を 1 件も dead 化しない（§6.1「再起動でも未配達エントリは保持される」を破らない）。
    判定は毎 sweep cycle の registry 現在値を参照するため、降格直後に cycle が走ると（直後に再昇格
    しても）1 cycle 内で dead 化しうる — lease のような時間的猶予帯は場レーンには無い（§6.6 の
    flapping トレードオフ）。

    自己離脱（本人による membership 解除）はこの sweep の対象ではなく、DELETE handler 側で unsubscribe
    と同型に即時削除される（`relay.streams.delete_member`、wire-api.md §5.3 / §6.6）。ここで dead 化
    するのは他 member による除去・降格という involuntary な read 権限喪失だけである。
    """
    pairs = db_conn.execute(
        "SELECT DISTINCT stream_id, member_identity FROM outbox WHERE target_type = 'stream'"
    ).fetchall()
    for pair in pairs:
        stream_id = pair["stream_id"]
        member_identity = pair["member_identity"]
        if stream_registry.get(stream_id) is None:
            continue  # 場が registry に不在（再起動直後の空 registry を含む）→ 誤爆回避。
        if stream_registry.has_read_access(stream_id, member_identity):
            continue  # まだ read 権限を持つ → 配達継続。
        rows = db_conn.execute(
            "SELECT * FROM outbox"
            " WHERE target_type = 'stream' AND stream_id = ? AND member_identity = ?",
            (stream_id, member_identity),
        ).fetchall()
        for row in rows:
            _move_to_dlq(
                db_conn,
                row,
                error_code=DLQ_ERROR_STREAM_READ_ACCESS_REVOKED,
                app_state=app_state,
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
                app_state,
                "subscription_registry_evicted",
                level="warning",
                subscription_id=subscription_id,
            )
            observability.inc_metric(app_state, "relay_subscription_lease_expirations_total")


def _sweep_idle_streams(
    db_conn: sqlite3.Connection, stream_registry, settings: Settings, app_state=None
) -> None:
    """close から猶予期間を過ぎ、未配達 outbox エントリが無い stream を registry から除去する。

    subscription 側 `_sweep_expired_subscription_registry` と対称の idle-GC。close された
    まま放置された stream record による registry の無制限成長を防ぐ。

    subscription lane は lease 切れが `_sweep_permanent_errors` で outbox を DLQ へ drain
    するのに対し、close 済み stream の outbox は close 後も retain 期間まで配達を継続する
    （wire-api.md §3.4）。そのため候補（`idle_closed_ids`）を無条件に除去すると未配達
    エントリの配達経路（`read_streams_for_identity` 経由の dispatch）を絶つ。これを避け、
    未配達 outbox が drain し切った（ack 済み / retain 超過で DLQ 化済み）stream だけを除去
    する。close 済み stream は新規 outbox を増やせない（POST は 410）ため、一度空なら空の
    ままで、除去は安全である。
    """
    candidates = stream_registry.idle_closed_ids(settings.stream_registry_retention_seconds)
    for stream_id in candidates:
        pending = db_conn.execute(
            "SELECT 1 FROM outbox WHERE target_type = 'stream' AND stream_id = ? LIMIT 1",
            (stream_id,),
        ).fetchone()
        if pending is not None:
            continue
        if stream_registry.evict(stream_id) and app_state is not None:
            observability.record_event(
                app_state,
                "stream_registry_evicted",
                level="warning",
                stream_id=stream_id,
            )


# ---------------------------------------------------------------------------
# dispatcher 本体（polling loop）
# ---------------------------------------------------------------------------


async def dispatch_once(app) -> None:
    """dispatcher の 1 polling cycle。push 試行 + DLQ sweep + DLQ 物理削除を行う。"""
    settings: Settings = app.state.settings
    sub_registry = subscriptions.get_registry_from_state(app.state)
    stream_registry = streams.get_registry_from_state(app.state)

    db_conn = _get_db_connection(settings)
    try:
        await _dispatch_to_connections(app.state, db_conn)
        await _enforce_ack_timeouts(app.state, db_conn, settings)
        _sweep_retain_exceeded(db_conn, app_state=app.state)
        _sweep_permanent_errors(db_conn, sub_registry, app_state=app.state)
        _sweep_stream_permanent_errors(db_conn, stream_registry, app_state=app.state)
        await federation_egress.dispatch_federation_egress(
            app.state, db_conn, settings, stream_registry
        )
        _sweep_dlq_physical_delete(db_conn, settings)
        _sweep_idle_streams(db_conn, stream_registry, settings, app_state=app.state)
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
            observability.record_event(app.state, "dispatcher_error", level="warning")
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
