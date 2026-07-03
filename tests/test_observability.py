"""relay.observability テストスイート。

`record_event`（サーバーログ append-only sink）と `purge_expired_server_log`
（TTL 90 日の間引き）を検証する。`/status` / `/metrics` は未実装（後続タスクの担当分）。
"""
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from relay import observability
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
