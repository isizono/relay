"""relay.observability テストスイート。

- `record_event`（サーバーログ append-only sink + `level="warning"` の recent_warnings 連携）
- `purge_expired_server_log`（TTL 90 日の間引き）
- `MetricsRegistry` / `render_prometheus_metrics`（`GET /metrics` の Prometheus 互換出力）
- `GET /status` / `GET /metrics` の HTTP 統合テスト
"""
import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from starlette.testclient import TestClient

from relay import db, delivery, observability
from relay.app import create_app
from relay.config import Settings


def _state(tmp_path, name="server.jsonl"):
    settings = Settings(server_log_path=str(tmp_path / name))
    return SimpleNamespace(settings=settings), settings


class TestRecordEvent:
    def test_appends_json_line(self, tmp_path):
        app_state, settings = _state(tmp_path)
        observability.record_event(app_state, "publish_received", publish_id=1, lane="stream")

        lines = open(settings.server_log_path, encoding="utf-8").readlines()
        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry["event"] == "publish_received"
        assert entry["publish_id"] == 1
        assert entry["lane"] == "stream"
        assert "ts" in entry

    def test_multiple_events_append_in_order(self, tmp_path):
        app_state, settings = _state(tmp_path)
        observability.record_event(app_state, "a")
        observability.record_event(app_state, "b")
        observability.record_event(app_state, "c")

        lines = open(settings.server_log_path, encoding="utf-8").readlines()
        events = [json.loads(line)["event"] for line in lines]
        assert events == ["a", "b", "c"]

    def test_no_settings_is_noop(self, tmp_path):
        """`settings` を持たない app_state（最小 fixture）では例外を出さず無視する。"""
        observability.record_event(SimpleNamespace(), "x")  # 例外を出さない

    def test_default_level_is_info(self, tmp_path):
        app_state, settings = _state(tmp_path)
        observability.record_event(app_state, "publish_received")

        entry = json.loads(open(settings.server_log_path, encoding="utf-8").readline())
        assert entry["level"] == "info"

    def test_warning_level_is_appended_to_recent_warnings(self, tmp_path):
        app_state, settings = _state(tmp_path)
        observability.record_event(
            app_state, "outbox_dead", level="warning", publish_id=1, error_code="RetainExceeded"
        )

        warnings = list(app_state.recent_warnings)
        assert len(warnings) == 1
        assert warnings[0]["event"] == "outbox_dead"
        assert warnings[0]["level"] == "warning"
        assert warnings[0]["error_code"] == "RetainExceeded"

    def test_info_level_does_not_populate_recent_warnings(self, tmp_path):
        app_state, settings = _state(tmp_path)
        observability.record_event(app_state, "sse_connected", identity="agent-a")

        assert list(getattr(app_state, "recent_warnings", [])) == []

    def test_recent_warnings_ring_buffer_caps_at_maxlen(self, tmp_path):
        """`RECENT_WARNINGS_MAXLEN` を超えると古いものから捨てられる。"""
        app_state, settings = _state(tmp_path)
        total = observability.RECENT_WARNINGS_MAXLEN + 5
        for i in range(total):
            observability.record_event(app_state, "dispatcher_error", level="warning", seq=i)

        warnings = list(app_state.recent_warnings)
        assert len(warnings) == observability.RECENT_WARNINGS_MAXLEN
        # 最も古い 5 件（seq=0..4）は捨てられ、直近分だけが残る。
        assert warnings[0]["seq"] == 5
        assert warnings[-1]["seq"] == total - 1

    def test_recent_warnings_works_without_settings(self, tmp_path):
        """ファイル sink が no-op（settings なし）でも recent_warnings は積まれる。"""
        app_state = SimpleNamespace()
        observability.record_event(app_state, "dispatcher_error", level="warning")

        assert len(list(app_state.recent_warnings)) == 1


class TestPurgeExpiredServerLog:
    def test_removes_lines_older_than_ttl(self, tmp_path):
        _, settings = _state(tmp_path)
        old_ts = (
            datetime.now(timezone.utc) - timedelta(days=observability.SERVER_LOG_TTL_DAYS + 1)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        recent_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        with open(settings.server_log_path, "w", encoding="utf-8") as f:
            f.write(json.dumps({"ts": old_ts, "event": "old"}) + "\n")
            f.write(json.dumps({"ts": recent_ts, "event": "recent"}) + "\n")

        observability.purge_expired_server_log(settings)

        lines = open(settings.server_log_path, encoding="utf-8").readlines()
        events = [json.loads(line)["event"] for line in lines]
        assert events == ["recent"]

    def test_missing_file_is_noop(self, tmp_path):
        settings = Settings(server_log_path=str(tmp_path / "nope.jsonl"))
        observability.purge_expired_server_log(settings)  # 例外を出さない

    def test_malformed_line_is_kept(self, tmp_path):
        """パース不能な行は安全側に倒して残す。"""
        _, settings = _state(tmp_path)
        with open(settings.server_log_path, "w", encoding="utf-8") as f:
            f.write("not valid json\n")

        observability.purge_expired_server_log(settings)

        lines = open(settings.server_log_path, encoding="utf-8").readlines()
        assert lines == ["not valid json\n"]

    def test_gc_runs_at_most_once_per_interval(self, tmp_path, monkeypatch):
        app_state, settings = _state(tmp_path)
        calls = []
        monkeypatch.setattr(
            observability, "purge_expired_server_log", lambda s: calls.append(s)
        )
        observability.record_event(app_state, "a")
        observability.record_event(app_state, "b")
        assert len(calls) == 1


# ---------------------------------------------------------------------------
# MetricsRegistry 単体テスト
# ---------------------------------------------------------------------------


class TestMetricsRegistry:
    def test_inc_accumulates_for_same_labels(self):
        registry = observability.MetricsRegistry()
        registry.inc("relay_ack_received_total")
        registry.inc("relay_ack_received_total")
        assert registry.snapshot()["relay_ack_received_total"][()] == 2

    def test_inc_separates_by_label_combination(self):
        registry = observability.MetricsRegistry()
        registry.inc("relay_push_delivered_total", lane="stream")
        registry.inc("relay_push_delivered_total", lane="subscription")
        registry.inc("relay_push_delivered_total", lane="stream")

        snapshot = registry.snapshot()["relay_push_delivered_total"]
        assert snapshot[(("lane", "stream"),)] == 2
        assert snapshot[(("lane", "subscription"),)] == 1

    def test_label_order_does_not_affect_key(self):
        """`sorted(labels.items())` で正規化するため、キーワード引数の指定順に依存しない。"""
        registry = observability.MetricsRegistry()
        registry.inc("m", a="1", b="2")
        registry.inc("m", b="2", a="1")
        assert registry.snapshot()["m"][(("a", "1"), ("b", "2"))] == 2

    def test_inc_with_amount(self):
        registry = observability.MetricsRegistry()
        registry.inc("m", amount=3)
        assert registry.snapshot()["m"][()] == 3

    def test_snapshot_is_a_copy(self):
        """`snapshot()` の戻り値を書き換えても内部状態に影響しない。"""
        registry = observability.MetricsRegistry()
        registry.inc("m")
        snap = registry.snapshot()
        snap["m"][()] = 999
        assert registry.snapshot()["m"][()] == 1


class TestGetMetricsRegistryAndIncMetric:
    def test_get_metrics_registry_is_lazy_and_shared(self):
        app_state = SimpleNamespace()
        r1 = observability.get_metrics_registry(app_state)
        r2 = observability.get_metrics_registry(app_state)
        assert r1 is r2

    def test_inc_metric_wrapper_updates_shared_registry(self):
        app_state = SimpleNamespace()
        observability.inc_metric(app_state, "relay_ack_received_total")
        observability.inc_metric(app_state, "relay_ack_received_total")
        snapshot = observability.get_metrics_registry(app_state).snapshot()
        assert snapshot["relay_ack_received_total"][()] == 2


# ---------------------------------------------------------------------------
# render_prometheus_metrics（`GET /metrics` 本文の組み立てロジック）
# ---------------------------------------------------------------------------


def _app_state_with_db(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "metrics.db"), server_log_path=str(tmp_path / "metrics.jsonl")
    )
    db.init_db(settings.db_path)
    return SimpleNamespace(settings=settings), settings


class TestRenderPrometheusMetrics:
    def test_emits_help_and_type_for_all_documented_metrics(self, tmp_path):
        app_state, _ = _app_state_with_db(tmp_path)
        body = observability.render_prometheus_metrics(app_state)

        for name in observability._METRIC_HELP:
            assert f"# TYPE {name} " in body
            assert f"# HELP {name} " in body

    def test_counter_absent_until_incremented(self, tmp_path):
        """increment されていない counter はサンプル行を出力しない（未使用 label の cardinality を増やさない）。"""
        app_state, _ = _app_state_with_db(tmp_path)
        body = observability.render_prometheus_metrics(app_state)
        sample_lines = [line for line in body.splitlines() if not line.startswith("#")]
        assert not any(line.startswith("relay_ack_received_total") for line in sample_lines)

    def test_counter_sample_line_after_increment(self, tmp_path):
        app_state, _ = _app_state_with_db(tmp_path)
        observability.inc_metric(app_state, "relay_ack_received_total")
        body = observability.render_prometheus_metrics(app_state)
        assert "relay_ack_received_total 1" in body

    def test_labeled_counter_sample_line(self, tmp_path):
        app_state, _ = _app_state_with_db(tmp_path)
        observability.inc_metric(app_state, "relay_push_delivered_total", lane="stream")
        body = observability.render_prometheus_metrics(app_state)
        assert 'relay_push_delivered_total{lane="stream"}' in body

    def test_gauge_outbox_depth_reflects_live_db_state(self, tmp_path):
        app_state, settings = _app_state_with_db(tmp_path)
        conn = db.get_connection(settings.db_path)
        try:
            conn.execute(
                "INSERT INTO outbox (target_type, subscription_id, publish_id, payload,"
                " enqueued_at) VALUES ('subscription', 'sub-1', 1, X'00', '2026-01-01T00:00:00Z')"
            )
            conn.commit()
        finally:
            conn.close()

        body = observability.render_prometheus_metrics(app_state)
        assert "relay_outbox_depth 1" in body

    def test_gauge_sse_connections_reflects_connection_manager(self, tmp_path):
        app_state, _ = _app_state_with_db(tmp_path)
        manager = delivery.ConnectionManager()
        app_state.connection_manager = manager
        conn = delivery.Connection(
            identity="agent-a", subscription_ids=frozenset(), queue=asyncio.Queue()
        )
        manager.register(conn)

        body = observability.render_prometheus_metrics(app_state)
        assert "relay_sse_connections 1" in body

    def test_no_subscription_id_or_delivery_target_label_leaks(self, tmp_path):
        """wire-api.md §7.2: subscription_id / delivery_target をラベルに使わない。"""
        app_state, _ = _app_state_with_db(tmp_path)
        observability.inc_metric(app_state, "relay_push_delivered_total", lane="subscription")
        body = observability.render_prometheus_metrics(app_state)
        assert "subscription_id=" not in body
        assert "delivery_target=" not in body


# ---------------------------------------------------------------------------
# HTTP 統合テスト（`GET /status` / `GET /metrics`）
# ---------------------------------------------------------------------------


@pytest.fixture()
def settings(tmp_path):
    return Settings(
        db_path=str(tmp_path / "test_relay.db"),
        server_log_path=str(tmp_path / "test_relay.jsonl"),
        dispatcher_lock_path=str(tmp_path / "test_relay.lock"),
        auth_tokens={"tok-a": "agent-a", "tok-b": "agent-b"},
    )


@pytest.fixture()
def client(settings):
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


class TestGetStatusEndpoint:
    def test_requires_authn(self, client):
        r = client.get("/status")
        assert r.status_code == 401

    def test_returns_expected_shape(self, client):
        r = client.get("/status", headers=_auth("tok-a"))
        assert r.status_code == 200
        body = r.json()
        for key in (
            "uptime_seconds",
            "subscriptions_count",
            "active_sse_connections",
            "streams_count",
            "outbox_pending_count",
            "outbox_dead_count",
            "publish_rate_5min",
            "recent_warnings",
        ):
            assert key in body

    def test_uptime_seconds_is_nonnegative(self, client):
        r = client.get("/status", headers=_auth("tok-a"))
        assert r.json()["uptime_seconds"] >= 0

    def test_counts_reflect_created_stream_and_subscription(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.post(
            "/subscriptions",
            json={"subscriber": "agent-a", "labels": ["x"]},
            headers=_auth("tok-a"),
        )

        body = client.get("/status", headers=_auth("tok-a")).json()
        assert body["streams_count"] == 1
        assert body["subscriptions_count"] == 1

    def test_outbox_pending_count_reflects_publish(self, client):
        # bootstrap member（作成者）は既定で write のみ（read を持たない）ため、
        # read_write を明示付与しないと outbox にエントリが作られない
        # （wire-api.md §3.1、`relay/streams.py` の `StreamRegistry.create` docstring 参照）。
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-a", "access": "read_write"},
            headers=_auth("tok-a"),
        )
        client.post("/streams/s1/messages", json={"body": "hi"}, headers=_auth("tok-a"))

        body = client.get("/status", headers=_auth("tok-a")).json()
        assert body["outbox_pending_count"] == 1

    def test_recent_warnings_reflects_warning_level_events(self, client):
        observability.record_event(
            client.app.state,
            "outbox_dead",
            level="warning",
            publish_id=42,
            error_code="RetainExceeded",
        )
        body = client.get("/status", headers=_auth("tok-a")).json()
        assert any(
            w["event"] == "outbox_dead" and w["error_code"] == "RetainExceeded"
            for w in body["recent_warnings"]
        )


class TestGetMetricsEndpoint:
    def test_requires_authn(self, client):
        r = client.get("/metrics")
        assert r.status_code == 401

    def test_returns_prometheus_text_format(self, client):
        r = client.get("/metrics", headers=_auth("tok-a"))
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/plain")
        assert "# TYPE relay_outbox_depth gauge" in r.text
        assert "# TYPE relay_ack_received_total counter" in r.text

    def test_reflects_publish_and_ack_activity(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-a", "access": "read_write"},
            headers=_auth("tok-a"),
        )
        client.post("/streams/s1/messages", json={"body": "hi"}, headers=_auth("tok-a"))
        client.post("/streams/s1/ack", json={"up_to_publish_id": 1}, headers=_auth("tok-a"))

        body = client.get("/metrics", headers=_auth("tok-a")).text
        assert 'relay_publish_received_total{publisher_identity="agent-a"} 1' in body
        assert "relay_ack_received_total 1" in body

    def test_publish_failure_increments_failure_counter_with_reason_label(self, client):
        # 存在しない stream への投函は publish 失敗（stream_not_found）としてカウントされる。
        client.post(
            "/streams/does-not-exist/messages", json={"body": "hi"}, headers=_auth("tok-a")
        )
        body = client.get("/metrics", headers=_auth("tok-a")).text
        assert 'relay_publish_failed_total{failure_reason="stream_not_found"} 1' in body
