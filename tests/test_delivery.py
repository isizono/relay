"""relay.delivery テストスイート。

- `ConnectionManager` / `Connection` の単体テスト
- `validate_subscription_ids`（ownership → lease 検証順序）の単体テスト
- dispatcher（`dispatch_once`）の push / retry / DLQ sweep を asyncio 直接呼び出しで検証
  （`asyncio.Queue` / `asyncio.Event` はループに紐づくため、`asyncio.run()` でラップした
  非同期テスト関数内で完結させる。pytest-asyncio は使わず、各テストを同期関数から
  `asyncio.run(...)` する）
- `GET /events` の実際の SSE wire を検証する統合テストは、実ソケット越しの HTTP が必要
  （Starlette TestClient / httpx.ASGITransport はいずれも ASGI app 呼び出し全体の完了を
  待ってから応答を返すため、終端しない SSE stream を『ストリーミングで読む』ことが
  できない。詳細は `LiveServer` fixture の docstring）。そのため uvicorn を実ポートで
  起動する `live_server` fixture を使う。
"""
from __future__ import annotations

import asyncio
import dataclasses
import json
import socket
import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest
import uvicorn
from starlette.testclient import TestClient

from relay import db, delivery, observability, streams, subscriptions
from relay.app import create_app
from relay.config import Settings
from relay.streams import StreamRegistry
from relay.subscriptions import SubscriptionRegistry


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture()
def settings(tmp_path):
    return Settings(
        db_path=str(tmp_path / "test_relay.db"),
        server_log_path=str(tmp_path / "test_relay.jsonl"),
        dispatcher_lock_path=str(tmp_path / "test_relay.lock"),
        auth_tokens={"tok-a": "agent-a", "tok-b": "agent-b", "tok-c": "agent-c"},
        dispatcher_poll_interval_seconds=0.02,
    )


# ---------------------------------------------------------------------------
# ConnectionManager 単体テスト
# ---------------------------------------------------------------------------


class TestConnectionManager:
    def _connection(self, identity="agent-a", subscription_ids=frozenset()):
        return delivery.Connection(
            identity=identity, subscription_ids=subscription_ids, queue=asyncio.Queue()
        )

    def test_register_and_snapshot(self):
        manager = delivery.ConnectionManager()
        conn = self._connection()
        manager.register(conn)
        snapshot = manager.snapshot()
        assert snapshot == {"agent-a": [conn]}

    def test_unregister_removes_connection(self):
        manager = delivery.ConnectionManager()
        conn = self._connection()
        manager.register(conn)
        manager.unregister(conn)
        assert manager.snapshot() == {}

    def test_multiple_connections_same_identity(self):
        manager = delivery.ConnectionManager()
        conn1 = self._connection()
        conn2 = self._connection()
        manager.register(conn1)
        manager.register(conn2)
        assert len(manager.snapshot()["agent-a"]) == 2

    def test_unregister_unknown_connection_is_noop(self):
        manager = delivery.ConnectionManager()
        conn = self._connection()
        manager.unregister(conn)  # 例外を出さない


# ---------------------------------------------------------------------------
# validate_subscription_ids（ownership → lease 検証順序）
# ---------------------------------------------------------------------------


class TestValidateSubscriptionIds:
    def test_no_ids_is_valid(self):
        registry = SubscriptionRegistry()
        assert delivery.validate_subscription_ids(registry, "agent-a", []) is None

    def test_owned_and_alive_is_valid(self):
        registry = SubscriptionRegistry()
        record = registry.create("agent-a", frozenset({"x"}), 300, 86400)
        err = delivery.validate_subscription_ids(registry, "agent-a", [record.subscription_id])
        assert err is None

    def test_unknown_id_returns_404(self):
        registry = SubscriptionRegistry()
        err = delivery.validate_subscription_ids(registry, "agent-a", ["nope"])
        assert err.status_code == 404

    def test_non_owned_id_returns_404(self):
        registry = SubscriptionRegistry()
        record = registry.create("agent-a", frozenset({"x"}), 300, 86400)
        err = delivery.validate_subscription_ids(registry, "agent-b", [record.subscription_id])
        assert err.status_code == 404

    def test_lease_expired_owned_returns_410(self):
        registry = SubscriptionRegistry()
        record = registry.create("agent-a", frozenset({"x"}), 300, 86400)
        record.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        err = delivery.validate_subscription_ids(registry, "agent-a", [record.subscription_id])
        assert err.status_code == 410

    def test_ownership_checked_before_lease_across_multiple_ids(self):
        """複数 id のうち 1 つでも非所有なら、他が lease 切れでも 404 が優先される。"""
        registry = SubscriptionRegistry()
        owned = registry.create("agent-a", frozenset({"x"}), 300, 86400)
        owned.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        other = registry.create("agent-b", frozenset({"y"}), 300, 86400)
        err = delivery.validate_subscription_ids(
            registry, "agent-a", [owned.subscription_id, other.subscription_id]
        )
        assert err.status_code == 404


# ---------------------------------------------------------------------------
# event_stream ジェネレータ単体テスト（実ソケット不要）
# ---------------------------------------------------------------------------


class TestEventStream:
    def test_yields_pushed_item_then_stops_on_sentinel(self):
        asyncio.run(self._run())

    async def _run(self):
        manager = delivery.ConnectionManager()
        conn = delivery.Connection(
            identity="agent-a", subscription_ids=frozenset(), queue=asyncio.Queue()
        )
        manager.register(conn)
        settings = Settings(sse_keepalive_seconds=30)

        await conn.queue.put({"publish_id": 5, "data": {"foo": "bar"}})
        await conn.queue.put(None)  # 強制切断センチネル

        events = []
        async for event in delivery.event_stream(conn, manager, settings, object()):
            events.append(event)

        assert len(events) == 1
        assert events[0].id == "5"
        assert events[0].event == "notification"
        assert json.loads(events[0].data) == {"foo": "bar"}
        # finally 節で unregister されている
        assert manager.snapshot() == {}

    def test_keepalive_comment_on_timeout(self):
        asyncio.run(self._run_keepalive())

    async def _run_keepalive(self):
        manager = delivery.ConnectionManager()
        conn = delivery.Connection(
            identity="agent-a", subscription_ids=frozenset(), queue=asyncio.Queue()
        )
        manager.register(conn)
        settings = Settings(sse_keepalive_seconds=0.01)

        gen = delivery.event_stream(conn, manager, settings, object())
        first = await gen.__anext__()
        assert first.comment == "keepalive"
        await gen.aclose()


# ---------------------------------------------------------------------------
# dispatcher: push / retry / cursor advance（asyncio.run 直接呼び出し）
# ---------------------------------------------------------------------------


def _insert_subscription_outbox_row(
    settings: Settings, subscription_id: str, publish_id: int, *, labels=("x",), expires_at=None
) -> None:
    conn = db.get_connection(settings.db_path)
    try:
        payload = json.dumps({"ref": {"type": "decision", "id": publish_id}, "title": None}).encode()
        conn.execute(
            "INSERT INTO publish_log (lane, stream_id, publisher_identity, enqueued_at)"
            " VALUES ('subscription', NULL, 'agent-a', ?)",
            (delivery._now_iso(),),
        )
        conn.execute(
            "INSERT INTO outbox"
            " (target_type, subscription_id, publish_id, payload, labels, enqueued_at, expires_at)"
            " VALUES ('subscription', ?, ?, ?, ?, ?, ?)",
            (
                subscription_id,
                publish_id,
                payload,
                json.dumps(list(labels)),
                delivery._now_iso(),
                # 既定は now（呼び出し側で上書きする場合あり）。retain sweep に消されず残す
                # テストは未来の expires_at を渡す。
                expires_at if expires_at is not None else delivery._now_iso(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _future_iso(seconds: int = 3600) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


class TestDispatchOnce:
    def test_push_advances_cursor_and_fills_queue(self, settings):
        asyncio.run(self._run(settings))

    async def _run(self, settings):
        db.init_db(settings.db_path)
        app = create_app(settings)

        sub_registry = subscriptions.get_registry_from_state(app.state)
        record = sub_registry.create("agent-b", frozenset({"x"}), 300, 86400)

        _insert_subscription_outbox_row(settings, record.subscription_id, 1)

        manager = delivery._get_connection_manager(app.state)
        conn = delivery.Connection(
            identity="agent-b",
            subscription_ids=frozenset({record.subscription_id}),
            queue=asyncio.Queue(),
        )
        manager.register(conn)

        await delivery.dispatch_once(app)

        assert not conn.queue.empty()
        item = conn.queue.get_nowait()
        assert item["publish_id"] == 1
        target_key = delivery._subscription_target_key(record.subscription_id)
        assert conn.cursor[target_key] == 1

        # 2 回目の cycle では同じエントリを再送しない（cursor が進んでいるため）。
        await delivery.dispatch_once(app)
        assert conn.queue.empty()

    def test_stream_lane_push_via_membership(self, settings):
        asyncio.run(self._run_stream(settings))

    async def _run_stream(self, settings):
        db.init_db(settings.db_path)
        app = create_app(settings)

        stream_registry = StreamRegistry()
        app.state.stream_registry = stream_registry
        stream_registry.create("s1", "agent-a", None)
        stream_registry.put_member("s1", "agent-b", "read")

        db_conn = db.get_connection(settings.db_path)
        try:
            db_conn.execute(
                "INSERT INTO publish_log (lane, stream_id, publisher_identity, enqueued_at)"
                " VALUES ('stream', 's1', 'agent-a', ?)",
                (delivery._now_iso(),),
            )
            db_conn.execute(
                "INSERT INTO outbox"
                " (target_type, stream_id, member_identity, publish_id, payload, enqueued_at,"
                " expires_at)"
                " VALUES ('stream', 's1', 'agent-b', 1, ?, ?, ?)",
                (b"hello", delivery._now_iso(), delivery._now_iso()),
            )
            db_conn.commit()
        finally:
            db_conn.close()

        manager = delivery._get_connection_manager(app.state)
        conn = delivery.Connection(
            identity="agent-b", subscription_ids=frozenset(), queue=asyncio.Queue()
        )
        manager.register(conn)

        await delivery.dispatch_once(app)

        item = conn.queue.get_nowait()
        assert item["publish_id"] == 1
        assert item["data"]["body"] == "hello"
        assert item["data"]["delivery_target"] == "stream:s1"


class TestPushRetryAndSlowConsumer:
    def test_push_succeeds_immediately_when_queue_has_room(self):
        asyncio.run(self._run_success())

    async def _run_success(self):
        conn = delivery.Connection(
            identity="agent-a", subscription_ids=frozenset(), queue=asyncio.Queue(maxsize=1)
        )
        ok = await delivery._push_with_retry(conn, {"publish_id": 1, "data": {}}, None)
        assert ok is True
        assert conn.closed.is_set() is False

    def test_exhausted_retries_force_disconnects(self):
        asyncio.run(self._run_exhausted())

    async def _run_exhausted(self):
        # maxsize=1 の queue を満杯にしておき、以降の push が queue full で
        # retry を使い切って強制切断されることを検証する。backoff の実時間待機
        # （累積約 3.1 秒）は PUSH_RETRY_DELAYS_SECONDS を monkeypatch して短縮する。
        original = delivery.PUSH_RETRY_DELAYS_SECONDS
        delivery.PUSH_RETRY_DELAYS_SECONDS = (0.001,) * 5
        try:
            conn = delivery.Connection(
                identity="agent-a", subscription_ids=frozenset(), queue=asyncio.Queue(maxsize=1)
            )
            conn.queue.put_nowait({"publish_id": 0, "data": {}})  # queue を満杯にする

            app_state = SimpleNamespace()
            ok = await delivery._push_with_retry(conn, {"publish_id": 1, "data": {}}, app_state)

            assert ok is False
            assert conn.closed.is_set() is True
            # 強制切断は warning ログ + relay_sse_slow_consumer_disconnects_total で観測できる
            # （wire-api.md §6.4, §7.2）。
            warnings = list(app_state.recent_warnings)
            assert any(w["event"] == "sse_slow_consumer_disconnect" for w in warnings)
            metrics = observability.get_metrics_registry(app_state).snapshot()
            assert metrics["relay_sse_slow_consumer_disconnects_total"][()] == 1
        finally:
            delivery.PUSH_RETRY_DELAYS_SECONDS = original

    def test_retry_delay_sequence_matches_wire_api_spec(self):
        """初回 100ms・係数 2・最大 5 回・累積約 3.1 秒（wire-api.md §6.4）。"""
        assert delivery.PUSH_RETRY_DELAYS_SECONDS == (0.1, 0.2, 0.4, 0.8, 1.6)
        assert sum(delivery.PUSH_RETRY_DELAYS_SECONDS) == pytest.approx(3.1)


# ---------------------------------------------------------------------------
# DLQ sweep
# ---------------------------------------------------------------------------


class TestDlqSweep:
    def test_retain_exceeded_moves_to_dlq(self, settings):
        db.init_db(settings.db_path)
        conn = db.get_connection(settings.db_path)
        past = (datetime.now(timezone.utc) - timedelta(seconds=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        try:
            conn.execute(
                "INSERT INTO publish_log (lane, stream_id, publisher_identity, enqueued_at)"
                " VALUES ('subscription', NULL, 'agent-a', ?)",
                (delivery._now_iso(),),
            )
            conn.execute(
                "INSERT INTO outbox"
                " (target_type, subscription_id, publish_id, payload, enqueued_at, expires_at)"
                " VALUES ('subscription', 'sub-x', 1, ?, ?, ?)",
                (b"{}", delivery._now_iso(), past),
            )
            conn.commit()
        finally:
            conn.close()

        conn = db.get_connection(settings.db_path)
        try:
            delivery._sweep_retain_exceeded(conn)
            conn.commit()
            outbox_rows = conn.execute("SELECT * FROM outbox").fetchall()
            dlq_rows = [
                tuple(r)
                for r in conn.execute(
                    "SELECT error_code FROM dlq WHERE subscription_id = 'sub-x'"
                ).fetchall()
            ]
        finally:
            conn.close()
        assert outbox_rows == []
        assert dlq_rows == [(delivery.DLQ_ERROR_RETAIN_EXCEEDED,)]

    def test_permanent_error_unknown_subscription_moves_to_dlq(self, settings):
        db.init_db(settings.db_path)
        conn = db.get_connection(settings.db_path)
        try:
            conn.execute(
                "INSERT INTO publish_log (lane, stream_id, publisher_identity, enqueued_at)"
                " VALUES ('subscription', NULL, 'agent-a', ?)",
                (delivery._now_iso(),),
            )
            conn.execute(
                "INSERT INTO outbox"
                " (target_type, subscription_id, publish_id, payload, enqueued_at, expires_at)"
                " VALUES ('subscription', 'ghost-sub', 1, ?, ?, ?)",
                (b"{}", delivery._now_iso(), None),
            )
            conn.commit()
        finally:
            conn.close()

        sub_registry = SubscriptionRegistry()  # 'ghost-sub' を知らない空の registry
        conn = db.get_connection(settings.db_path)
        try:
            delivery._sweep_permanent_errors(conn, sub_registry)
            conn.commit()
            outbox_rows = conn.execute("SELECT * FROM outbox").fetchall()
            dlq_rows = [
                tuple(r)
                for r in conn.execute(
                    "SELECT error_code FROM dlq WHERE subscription_id = 'ghost-sub'"
                ).fetchall()
            ]
        finally:
            conn.close()
        assert outbox_rows == []
        assert dlq_rows == [(delivery.DLQ_ERROR_SUBSCRIPTION_UNAVAILABLE,)]

    def test_lease_expired_subscription_moves_to_dlq(self, settings):
        db.init_db(settings.db_path)
        sub_registry = SubscriptionRegistry()
        record = sub_registry.create("agent-b", frozenset({"x"}), 300, 86400)
        record.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)

        conn = db.get_connection(settings.db_path)
        try:
            conn.execute(
                "INSERT INTO publish_log (lane, stream_id, publisher_identity, enqueued_at)"
                " VALUES ('subscription', NULL, 'agent-a', ?)",
                (delivery._now_iso(),),
            )
            conn.execute(
                "INSERT INTO outbox"
                " (target_type, subscription_id, publish_id, payload, enqueued_at, expires_at)"
                " VALUES ('subscription', ?, 1, ?, ?, ?)",
                (record.subscription_id, b"{}", delivery._now_iso(), None),
            )
            conn.commit()
        finally:
            conn.close()

        conn = db.get_connection(settings.db_path)
        try:
            delivery._sweep_permanent_errors(conn, sub_registry)
            conn.commit()
            dlq_rows = [
                tuple(r)
                for r in conn.execute(
                    "SELECT error_code FROM dlq WHERE subscription_id = ?",
                    (record.subscription_id,),
                ).fetchall()
            ]
        finally:
            conn.close()
        assert dlq_rows == [(delivery.DLQ_ERROR_SUBSCRIPTION_UNAVAILABLE,)]

    def test_live_subscription_not_moved_to_dlq(self, settings):
        db.init_db(settings.db_path)
        sub_registry = SubscriptionRegistry()
        record = sub_registry.create("agent-b", frozenset({"x"}), 300, 86400)

        conn = db.get_connection(settings.db_path)
        try:
            conn.execute(
                "INSERT INTO publish_log (lane, stream_id, publisher_identity, enqueued_at)"
                " VALUES ('subscription', NULL, 'agent-a', ?)",
                (delivery._now_iso(),),
            )
            conn.execute(
                "INSERT INTO outbox"
                " (target_type, subscription_id, publish_id, payload, enqueued_at, expires_at)"
                " VALUES ('subscription', ?, 1, ?, ?, ?)",
                (record.subscription_id, b"{}", delivery._now_iso(), None),
            )
            conn.commit()
        finally:
            conn.close()

        conn = db.get_connection(settings.db_path)
        try:
            delivery._sweep_permanent_errors(conn, sub_registry)
            conn.commit()
            outbox_rows = [
                tuple(r)
                for r in conn.execute(
                    "SELECT publish_id FROM outbox WHERE subscription_id = ?",
                    (record.subscription_id,),
                ).fetchall()
            ]
        finally:
            conn.close()
        assert outbox_rows == [(1,)]

    def test_physical_delete_after_retention_days(self, settings):
        db.init_db(settings.db_path)
        old_dead_at = (
            datetime.now(timezone.utc) - timedelta(days=settings.dlq_retention_days, seconds=1)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        recent_dead_at = delivery._now_iso()

        conn = db.get_connection(settings.db_path)
        try:
            conn.execute(
                "INSERT INTO dlq"
                " (target_type, subscription_id, publish_id, payload, error_code, dead_at)"
                " VALUES ('subscription', 'old-sub', 1, ?, 'x', ?)",
                (b"{}", old_dead_at),
            )
            conn.execute(
                "INSERT INTO dlq"
                " (target_type, subscription_id, publish_id, payload, error_code, dead_at)"
                " VALUES ('subscription', 'recent-sub', 1, ?, 'x', ?)",
                (b"{}", recent_dead_at),
            )
            conn.commit()
        finally:
            conn.close()

        conn = db.get_connection(settings.db_path)
        try:
            delivery._sweep_dlq_physical_delete(conn, settings)
            conn.commit()
            remaining = {
                r[0] for r in conn.execute("SELECT subscription_id FROM dlq").fetchall()
            }
        finally:
            conn.close()
        assert remaining == {"recent-sub"}


def _insert_stream_outbox_row(
    settings: Settings,
    stream_id: str,
    member_identity: str,
    publish_id: int,
    *,
    body: bytes = b"hello",
    expires_at=None,
) -> None:
    conn = db.get_connection(settings.db_path)
    try:
        conn.execute(
            "INSERT INTO publish_log (lane, stream_id, publisher_identity, enqueued_at)"
            " VALUES ('stream', ?, 'agent-a', ?)",
            (stream_id, delivery._now_iso()),
        )
        conn.execute(
            "INSERT INTO outbox"
            " (target_type, stream_id, member_identity, publish_id, payload, enqueued_at,"
            " expires_at)"
            " VALUES ('stream', ?, ?, ?, ?, ?, ?)",
            (
                stream_id,
                member_identity,
                publish_id,
                body,
                delivery._now_iso(),
                # retain sweep に先に消されないよう既定は未来。stream permanent-error sweep のみを検証する。
                expires_at if expires_at is not None else _future_iso(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


class TestStreamDlqSweep:
    """場（stream）レーンの permanent error 検出（`_sweep_stream_permanent_errors`）。

    subscription レーンと非対称: 「場が registry に生存 AND member が read 権限を持たない」を
    permanent error とする。restart-safe 性（registry が空では誤爆しない）が核心（wire-api.md §6.6）。
    """

    def _dlq_error_codes(self, settings, stream_id, member_identity):
        conn = db.get_connection(settings.db_path)
        try:
            return [
                r[0]
                for r in conn.execute(
                    "SELECT error_code FROM dlq WHERE stream_id = ? AND member_identity = ?",
                    (stream_id, member_identity),
                ).fetchall()
            ]
        finally:
            conn.close()

    def _outbox_publish_ids(self, settings, stream_id, member_identity):
        conn = db.get_connection(settings.db_path)
        try:
            return [
                r[0]
                for r in conn.execute(
                    "SELECT publish_id FROM outbox"
                    " WHERE target_type = 'stream' AND stream_id = ? AND member_identity = ?"
                    " ORDER BY publish_id",
                    (stream_id, member_identity),
                ).fetchall()
            ]
        finally:
            conn.close()

    def test_removed_member_moves_to_dlq(self, settings):
        """場が生存し member が除去された（read 権限喪失）エントリは DLQ 化される。"""
        db.init_db(settings.db_path)
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        registry.put_member("s1", "agent-b", "read")
        _insert_stream_outbox_row(settings, "s1", "agent-b", 1)

        registry.delete_member("s1", "agent-b")  # 他 member による involuntary な read 権限喪失

        conn = db.get_connection(settings.db_path)
        try:
            delivery._sweep_stream_permanent_errors(conn, registry)
            conn.commit()
        finally:
            conn.close()

        assert self._outbox_publish_ids(settings, "s1", "agent-b") == []
        assert self._dlq_error_codes(settings, "s1", "agent-b") == [
            delivery.DLQ_ERROR_STREAM_READ_ACCESS_REVOKED
        ]

    def test_demoted_read_write_to_write_moves_to_dlq(self, settings):
        """read_write → write への降格（read 権限を落とす access 変更）も DLQ 化される。"""
        db.init_db(settings.db_path)
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        registry.put_member("s1", "agent-b", "read_write")
        _insert_stream_outbox_row(settings, "s1", "agent-b", 1)

        registry.put_member("s1", "agent-b", "write")  # read を落とす降格

        conn = db.get_connection(settings.db_path)
        try:
            delivery._sweep_stream_permanent_errors(conn, registry)
            conn.commit()
        finally:
            conn.close()

        assert self._dlq_error_codes(settings, "s1", "agent-b") == [
            delivery.DLQ_ERROR_STREAM_READ_ACCESS_REVOKED
        ]

    def test_live_read_member_not_moved_to_dlq(self, settings):
        """read 権限を保持している member のエントリは DLQ 化されない。"""
        db.init_db(settings.db_path)
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        registry.put_member("s1", "agent-b", "read")
        _insert_stream_outbox_row(settings, "s1", "agent-b", 1)

        conn = db.get_connection(settings.db_path)
        try:
            delivery._sweep_stream_permanent_errors(conn, registry)
            conn.commit()
        finally:
            conn.close()

        assert self._outbox_publish_ids(settings, "s1", "agent-b") == [1]
        assert self._dlq_error_codes(settings, "s1", "agent-b") == []

    def test_restart_safe_empty_registry_does_not_move_to_dlq(self, settings):
        """relay 再起動直後（registry が空）を模したケース: 未配達 outbox を誤って DLQ 化しない。

        再起動で membership registry は揮発するが outbox は disk 永続化されて残る（§6.1）。sweep 条件は
        「場が registry に生存」を AND に含むため `registry.get(stream_id)` が None になり、1 件も
        dead 化しない（wire-api.md §6.6）。判定を「member が registry に居ない」だけにすると、ここで
        未配達エントリが全件 dead 化してしまう。
        """
        db.init_db(settings.db_path)
        # 再起動直後を模す: outbox には stream エントリが残っているが registry は空。
        _insert_stream_outbox_row(settings, "s1", "agent-b", 1)
        _insert_stream_outbox_row(settings, "s1", "agent-b", 2)
        empty_registry = StreamRegistry()

        conn = db.get_connection(settings.db_path)
        try:
            delivery._sweep_stream_permanent_errors(conn, empty_registry)
            conn.commit()
        finally:
            conn.close()

        assert self._outbox_publish_ids(settings, "s1", "agent-b") == [1, 2]
        assert self._dlq_error_codes(settings, "s1", "agent-b") == []

    def test_flapping_demote_then_repromote_before_sweep_not_moved(self, settings):
        """降格 → sweep 前に再昇格（flapping）。sweep は現在の registry state を見るため DLQ 化しない。"""
        db.init_db(settings.db_path)
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        registry.put_member("s1", "agent-b", "read")
        _insert_stream_outbox_row(settings, "s1", "agent-b", 1)

        registry.put_member("s1", "agent-b", "write")  # 降格
        registry.put_member("s1", "agent-b", "read")  # sweep が走る前に再昇格

        conn = db.get_connection(settings.db_path)
        try:
            delivery._sweep_stream_permanent_errors(conn, registry)
            conn.commit()
        finally:
            conn.close()

        assert self._outbox_publish_ids(settings, "s1", "agent-b") == [1]
        assert self._dlq_error_codes(settings, "s1", "agent-b") == []

    def test_flapping_demote_caught_by_sweep_moves_to_dlq(self, settings):
        """降格中に sweep cycle が走ると、直後に再昇格しても 1 cycle 内で DLQ 化しうる（§6.6 の代償）。

        場レーンは lease のような時間的猶予帯を持たないため、降格を挟んだ 1 sweep cycle でも
        permanent error として dead 化される。既に dead 化したエントリは再昇格しても outbox へ戻らない。
        """
        db.init_db(settings.db_path)
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        registry.put_member("s1", "agent-b", "read")
        _insert_stream_outbox_row(settings, "s1", "agent-b", 1)

        registry.put_member("s1", "agent-b", "write")  # 降格（read 喪失）中に sweep が走る

        conn = db.get_connection(settings.db_path)
        try:
            delivery._sweep_stream_permanent_errors(conn, registry)
            conn.commit()
        finally:
            conn.close()

        registry.put_member("s1", "agent-b", "read")  # sweep 後の再昇格は dead を巻き戻さない

        assert self._outbox_publish_ids(settings, "s1", "agent-b") == []
        assert self._dlq_error_codes(settings, "s1", "agent-b") == [
            delivery.DLQ_ERROR_STREAM_READ_ACCESS_REVOKED
        ]

    def test_dispatch_once_wires_stream_sweep(self, settings):
        """dispatch_once の polling cycle に stream permanent-error sweep が組み込まれている。"""
        asyncio.run(self._run_dispatch_once(settings))

    async def _run_dispatch_once(self, settings):
        db.init_db(settings.db_path)
        app = create_app(settings)
        stream_registry = StreamRegistry()
        app.state.stream_registry = stream_registry
        stream_registry.create("s1", "agent-a", None)
        stream_registry.put_member("s1", "agent-b", "read")
        _insert_stream_outbox_row(settings, "s1", "agent-b", 1)

        stream_registry.delete_member("s1", "agent-b")  # read 権限喪失

        await delivery.dispatch_once(app)

        assert self._outbox_publish_ids(settings, "s1", "agent-b") == []
        assert self._dlq_error_codes(settings, "s1", "agent-b") == [
            delivery.DLQ_ERROR_STREAM_READ_ACCESS_REVOKED
        ]


class TestAckTimeout:
    """push 済みだが ack が進まない接続の強制切断（`_enforce_ack_timeouts`）。

    queue backpressure ベースの slow consumer 切断とは別の障害モード（SSE 送信は進むが
    subscriber 側の受信 / ack ループがスタックして ack が返らない）を検知する。
    """

    def test_unacked_floor_returns_oldest_pushed_unacked_entry(self, settings):
        db.init_db(settings.db_path)
        _insert_subscription_outbox_row(settings, "sub-1", 3)
        _insert_subscription_outbox_row(settings, "sub-1", 5)

        conn = delivery.Connection(
            identity="agent-b", subscription_ids=frozenset({"sub-1"}), queue=asyncio.Queue()
        )
        target_key = delivery._subscription_target_key("sub-1")
        targets = [(target_key, "subscription", {"subscription_id": "sub-1"})]

        db_conn = db.get_connection(settings.db_path)
        try:
            # 両方 push 済み（cursor=5）→ 最古の未 ack は 3。
            conn.cursor[target_key] = 5
            assert delivery._connection_unacked_floor(db_conn, conn, targets) == 3
            # publish_id 5 のみ push 済み扱い（cursor=3）→ pid 3 が floor（pid 5 は未 push）。
            conn.cursor[target_key] = 3
            assert delivery._connection_unacked_floor(db_conn, conn, targets) == 3
            # 何も push していない（cursor=0）→ floor なし。
            conn.cursor[target_key] = 0
            assert delivery._connection_unacked_floor(db_conn, conn, targets) is None
        finally:
            db_conn.close()

    def test_stuck_subscriber_is_force_disconnected(self, settings):
        asyncio.run(self._run_stuck(settings))

    async def _run_stuck(self, settings):
        settings = dataclasses.replace(settings, ack_timeout_seconds=0.0)
        db.init_db(settings.db_path)
        app = create_app(settings)
        sub_registry = subscriptions.get_registry_from_state(app.state)
        record = sub_registry.create("agent-b", frozenset({"x"}), 300, 86400)
        _insert_subscription_outbox_row(
            settings, record.subscription_id, 1, expires_at=_future_iso()
        )

        manager = delivery._get_connection_manager(app.state)
        conn = delivery.Connection(
            identity="agent-b",
            subscription_ids=frozenset({record.subscription_id}),
            queue=asyncio.Queue(),
        )
        manager.register(conn)

        # cycle 1: push + floor 初観測（timer 起動、まだ切断しない）。
        await delivery.dispatch_once(app)
        assert conn.closed.is_set() is False
        assert conn.unacked_floor == 1

        # cycle 2: floor が動かないまま timeout(0) 経過 → 強制切断 + warning ログ。
        await delivery.dispatch_once(app)
        assert conn.closed.is_set() is True
        warnings = list(app.state.recent_warnings)
        assert any(w["event"] == "sse_ack_timeout_disconnect" for w in warnings)

    def test_ack_progress_resets_timer_and_keeps_connection(self, settings):
        asyncio.run(self._run_ack_progress(settings))

    async def _run_ack_progress(self, settings):
        settings = dataclasses.replace(settings, ack_timeout_seconds=0.0)
        db.init_db(settings.db_path)
        app = create_app(settings)
        sub_registry = subscriptions.get_registry_from_state(app.state)
        record = sub_registry.create("agent-b", frozenset({"x"}), 300, 86400)
        _insert_subscription_outbox_row(
            settings, record.subscription_id, 1, expires_at=_future_iso()
        )

        manager = delivery._get_connection_manager(app.state)
        conn = delivery.Connection(
            identity="agent-b",
            subscription_ids=frozenset({record.subscription_id}),
            queue=asyncio.Queue(),
        )
        manager.register(conn)

        await delivery.dispatch_once(app)  # push + floor 初観測
        assert conn.unacked_floor == 1

        # ack 相当（outbox から削除）で未 ack floor が消える。
        c = db.get_connection(settings.db_path)
        try:
            c.execute(
                "DELETE FROM outbox WHERE target_type='subscription' AND subscription_id=?",
                (record.subscription_id,),
            )
            c.commit()
        finally:
            c.close()

        await delivery.dispatch_once(app)  # floor None → timer リセット、切断しない
        assert conn.closed.is_set() is False
        assert conn.unacked_floor is None


class TestSubscriptionRegistrySweep:
    """dispatch_once が subscription registry の掃除まで一貫して行うことを検証する。"""

    def test_dispatch_once_evicts_long_expired_subscription(self, settings):
        asyncio.run(self._run(settings))

    async def _run(self, settings):
        db.init_db(settings.db_path)
        app = create_app(settings)
        sub_registry = subscriptions.get_registry_from_state(app.state)

        long_expired = sub_registry.create("agent-a", frozenset({"x"}), 300, 86400)
        long_expired.lease_expires_at = datetime.now(timezone.utc) - timedelta(
            seconds=settings.subscription_registry_retention_seconds + 1
        )
        alive = sub_registry.create("agent-b", frozenset({"y"}), 300, 86400)

        await delivery.dispatch_once(app)

        assert sub_registry.get(long_expired.subscription_id) is None
        assert sub_registry.get(alive.subscription_id) is not None


class TestStreamRegistrySweep:
    """dispatch_once が close 済み idle stream の掃除まで一貫して行うことを検証する。"""

    def _closed_at_past_grace(self, settings) -> str:
        return (
            datetime.now(timezone.utc)
            - timedelta(seconds=settings.stream_registry_retention_seconds + 1)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")

    def test_dispatch_once_evicts_drained_idle_closed_stream(self, settings):
        asyncio.run(self._run_evicts(settings))

    async def _run_evicts(self, settings):
        db.init_db(settings.db_path)
        app = create_app(settings)
        stream_registry = streams.get_registry_from_state(app.state)

        stream_registry.create("s-idle", "agent-a", None)
        stream_registry.close("s-idle")
        stream_registry.get("s-idle").closed_at = self._closed_at_past_grace(settings)

        await delivery.dispatch_once(app)

        # 未配達 outbox が無く猶予を過ぎた close 済み stream は registry から消える。
        assert stream_registry.get("s-idle") is None

    def test_dispatch_once_keeps_idle_closed_stream_with_pending_outbox(self, settings):
        asyncio.run(self._run_keeps(settings))

    async def _run_keeps(self, settings):
        db.init_db(settings.db_path)
        app = create_app(settings)
        stream_registry = streams.get_registry_from_state(app.state)

        stream_registry.create("s-pending", "agent-a", None)
        # read 権限を持つ member を残し、permanent-error sweep が outbox を DLQ 化しないようにする。
        stream_registry.put_member("s-pending", "agent-b", "read")
        stream_registry.close("s-pending")
        stream_registry.get("s-pending").closed_at = self._closed_at_past_grace(settings)

        # retain 未超過の未配達 outbox エントリを 1 件残す（expires_at を将来に置く）。
        future = (datetime.now(timezone.utc) + timedelta(hours=1)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        conn = db.get_connection(settings.db_path)
        try:
            conn.execute(
                "INSERT INTO outbox"
                " (target_type, stream_id, member_identity, publish_id, payload,"
                " enqueued_at, expires_at)"
                " VALUES ('stream', 's-pending', 'agent-b', 1, ?, ?, ?)",
                (b"hi", future, future),
            )
            conn.commit()
        finally:
            conn.close()

        await delivery.dispatch_once(app)

        # 未配達エントリが drain し切っていないので evict されず registry に残る。
        assert stream_registry.get("s-pending") is not None


# ---------------------------------------------------------------------------
# dispatcher 単一プロセス enforcement（file lock）
# ---------------------------------------------------------------------------


class TestDispatcherLock:
    def test_second_acquire_fails_while_first_holds(self, tmp_path):
        lock_path = str(tmp_path / "d.lock")
        fd1 = delivery.try_acquire_dispatcher_lock(lock_path)
        assert fd1 is not None
        fd2 = delivery.try_acquire_dispatcher_lock(lock_path)
        assert fd2 is None
        delivery.release_dispatcher_lock(fd1)

    def test_reacquire_succeeds_after_release(self, tmp_path):
        lock_path = str(tmp_path / "d.lock")
        fd1 = delivery.try_acquire_dispatcher_lock(lock_path)
        delivery.release_dispatcher_lock(fd1)
        fd2 = delivery.try_acquire_dispatcher_lock(lock_path)
        assert fd2 is not None
        delivery.release_dispatcher_lock(fd2)


# ---------------------------------------------------------------------------
# 実ソケット統合テスト（真の SSE ストリーミング）
# ---------------------------------------------------------------------------


def _read_until_data_line(resp: httpx.Response, timeout: float) -> str:
    """`data:` で始まる行が来るまで読み進める（keepalive コメント行は読み飛ばす）。

    行が来ない場合は httpx.Client 側の read timeout（`live_client` fixture で設定）で
    例外になる（無限 hang はしない）。
    """
    deadline = time.time() + timeout
    for line in resp.iter_lines():
        if line.startswith("data:"):
            return line
        if time.time() > deadline:
            break
    raise AssertionError("data: 行が timeout 内に観測できませんでした")


def _read_publish_ids(resp: httpx.Response, count: int, timeout: float) -> list[int]:
    deadline = time.time() + timeout
    ids: list[int] = []
    for line in resp.iter_lines():
        if line.startswith("data:"):
            data = json.loads(line[len("data:") :].strip())
            ids.append(data["publish_id"])
            if len(ids) >= count:
                break
        if time.time() > deadline:
            break
    return ids


class LiveServer:
    """uvicorn を実 TCP port で起動する test helper。

    Starlette `TestClient`（httpx ラップ）も `httpx.ASGITransport` も、内部で
    `await app(scope, receive, send)` の完了を待ってからレスポンスを返す実装になっており
    （終端しない SSE stream は待ち続けて hang する）、真のインクリメンタル HTTP
    ストリーミングをサポートしない。そのため `GET /events` を実際にストリームとして
    読む検証だけは、実ソケット越しの uvicorn + `httpx.Client`（非 ASGI transport）を使う。
    """

    def __init__(self, app):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        self.port = sock.getsockname()[1]
        sock.close()

        config = uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="warning")
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self) -> httpx.Client:
        self.thread.start()
        deadline = time.time() + 5
        while not self.server.started and time.time() < deadline:
            time.sleep(0.02)
        self.client = httpx.Client(base_url=f"http://127.0.0.1:{self.port}", timeout=10.0)
        return self.client

    def __exit__(self, *exc_info) -> None:
        self.client.close()
        self.server.should_exit = True
        self.thread.join(timeout=5)


@pytest.fixture()
def live_client(settings):
    app = create_app(settings)
    with LiveServer(app) as client:
        yield client


class TestGetEventsLive:
    def test_subscription_lane_happy_path_and_ack(self, live_client, settings):
        r = live_client.post(
            "/subscriptions",
            json={"subscriber": "agent-b", "labels": ["topic:474"]},
            headers=_auth("tok-b"),
        )
        subscription_id = r.json()["subscription_id"]

        with live_client.stream(
            "GET", f"/events?subscription_ids={subscription_id}", headers=_auth("tok-b")
        ) as resp:
            assert resp.status_code == 200
            assert resp.headers["content-type"].startswith("text/event-stream")

            r2 = live_client.post(
                "/publish",
                json={"ref": {"type": "decision", "id": 1}, "labels": ["topic:474"]},
                headers=_auth("tok-a"),
            )
            publish_id = r2.json()["publish_id"]

            data_line = _read_until_data_line(resp, timeout=5)
            data = json.loads(data_line[len("data:") :].strip())
            assert data["publish_id"] == publish_id
            assert data["delivery_target"] == f"sub:{subscription_id}"

        r3 = live_client.post(
            f"/subscriptions/{subscription_id}/ack",
            json={"up_to_publish_id": publish_id},
            headers=_auth("tok-b"),
        )
        assert r3.status_code == 200

        conn = sqlite3.connect(settings.db_path)
        try:
            rows = conn.execute("SELECT * FROM outbox").fetchall()
        finally:
            conn.close()
        assert rows == []

    def test_stream_lane_auto_included_without_subscription_ids(self, live_client):
        live_client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        live_client.put(
            "/streams/s1/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )

        with live_client.stream("GET", "/events", headers=_auth("tok-b")) as resp:
            assert resp.status_code == 200
            r = live_client.post(
                "/streams/s1/messages", json={"body": "hello"}, headers=_auth("tok-a")
            )
            publish_id = r.json()["publish_id"]

            data_line = _read_until_data_line(resp, timeout=5)
            data = json.loads(data_line[len("data:") :].strip())
            assert data["publish_id"] == publish_id
            assert data["body"] == "hello"
            assert data["delivery_target"] == "stream:s1"

    def test_unknown_subscription_id_returns_404(self, live_client):
        r = live_client.get("/events?subscription_ids=nope", headers=_auth("tok-a"))
        assert r.status_code == 404

    def test_non_owned_subscription_id_returns_404(self, live_client):
        r = live_client.post(
            "/subscriptions", json={"subscriber": "agent-a", "labels": ["x"]}, headers=_auth("tok-a")
        )
        subscription_id = r.json()["subscription_id"]
        r2 = live_client.get(
            f"/events?subscription_ids={subscription_id}", headers=_auth("tok-b")
        )
        assert r2.status_code == 404

    def test_resume_after_reconnect_replays_unacked_entries(self, live_client, settings):
        """再接続時、ack されていない outbox エントリが古い順に再 push される
        （wire-api.md §6.5：暗黙再 push）。"""
        r = live_client.post(
            "/subscriptions",
            json={"subscriber": "agent-b", "labels": ["x"]},
            headers=_auth("tok-b"),
        )
        subscription_id = r.json()["subscription_id"]

        # 誰も接続していない状態で 2 件 publish（outbox に積むだけ）。
        p1 = live_client.post(
            "/publish",
            json={"ref": {"type": "decision", "id": 1}, "labels": ["x"]},
            headers=_auth("tok-a"),
        ).json()["publish_id"]
        p2 = live_client.post(
            "/publish",
            json={"ref": {"type": "decision", "id": 2}, "labels": ["x"]},
            headers=_auth("tok-a"),
        ).json()["publish_id"]

        with live_client.stream(
            "GET", f"/events?subscription_ids={subscription_id}", headers=_auth("tok-b")
        ) as resp:
            seen_ids = _read_publish_ids(resp, count=2, timeout=5)

        assert seen_ids == [p1, p2]
