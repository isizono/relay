"""relay.subscriptions テストスイート。

SubscriptionRegistry の単体テストと、subscribe / lease renew / unsubscribe /
cumulative ack / publish（subscription レーン）の HTTP 統合テストを検証する。
`GET /events`（SSE）は `tests/test_delivery.py` 側で扱う。
"""
import sqlite3

import pytest
from starlette.testclient import TestClient

from relay.app import create_app
from relay.config import (
    MAX_LEASE_TTL_SECONDS,
    MAX_RETAIN_SECONDS,
    MIN_LEASE_TTL_SECONDS,
    MIN_RETAIN_SECONDS,
)
from relay.subscriptions import SubscriptionRegistry


# ---------------------------------------------------------------------------
# SubscriptionRegistry 単体テスト
# ---------------------------------------------------------------------------


class TestSubscriptionRegistry:
    def test_create_returns_record_with_uuid(self):
        registry = SubscriptionRegistry()
        record = registry.create("agent-a", frozenset({"topic:474"}), 300, 86400)
        assert record.subscriber == "agent-a"
        assert record.labels == frozenset({"topic:474"})
        assert len(record.subscription_id) > 0

    def test_get_missing_returns_none(self):
        registry = SubscriptionRegistry()
        assert registry.get("nope") is None

    def test_is_owner_true_for_subscriber(self):
        registry = SubscriptionRegistry()
        record = registry.create("agent-a", frozenset({"x"}), 300, 86400)
        assert registry.is_owner(record.subscription_id, "agent-a") is True

    def test_is_owner_false_for_other_identity(self):
        registry = SubscriptionRegistry()
        record = registry.create("agent-a", frozenset({"x"}), 300, 86400)
        assert registry.is_owner(record.subscription_id, "agent-b") is False

    def test_is_owner_false_for_missing_id(self):
        registry = SubscriptionRegistry()
        assert registry.is_owner("nope", "agent-a") is False

    def test_is_lease_expired_false_for_missing_id(self):
        """不存在は『lease 切れ』ではない（呼び出し側が is_owner / get と組み合わせて区別）。"""
        registry = SubscriptionRegistry()
        assert registry.is_lease_expired("nope") is False

    def test_is_lease_expired_false_when_fresh(self):
        registry = SubscriptionRegistry()
        record = registry.create("agent-a", frozenset({"x"}), 300, 86400)
        assert registry.is_lease_expired(record.subscription_id) is False

    def test_is_lease_expired_true_when_ttl_zero_elapsed(self):
        registry = SubscriptionRegistry()
        record = registry.create("agent-a", frozenset({"x"}), 30, 86400)
        # lease_ttl min は 30 なので明示的に過去へ書き換えて期限切れを再現する。
        from datetime import datetime, timedelta, timezone

        record.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        assert registry.is_lease_expired(record.subscription_id) is True

    def test_renew_extends_lease_and_updates_ttl(self):
        registry = SubscriptionRegistry()
        record = registry.create("agent-a", frozenset({"x"}), 300, 86400)
        before = record.lease_expires_at
        renewed = registry.renew(record.subscription_id, 600)
        assert renewed is not None
        assert renewed.lease_ttl == 600
        assert renewed.lease_expires_at > before

    def test_renew_missing_returns_none(self):
        registry = SubscriptionRegistry()
        assert registry.renew("nope", 600) is None

    def test_renew_without_ttl_reuses_existing_ttl(self):
        registry = SubscriptionRegistry()
        record = registry.create("agent-a", frozenset({"x"}), 123, 86400)
        renewed = registry.renew(record.subscription_id, None)
        assert renewed.lease_ttl == 123

    def test_delete_removes_record(self):
        registry = SubscriptionRegistry()
        record = registry.create("agent-a", frozenset({"x"}), 300, 86400)
        registry.delete(record.subscription_id)
        assert registry.get(record.subscription_id) is None

    def test_delete_missing_is_noop(self):
        registry = SubscriptionRegistry()
        registry.delete("nope")  # 例外を出さない

    def test_matching_subset_semantics(self):
        registry = SubscriptionRegistry()
        registry.create("agent-a", frozenset({"X", "Y"}), 300, 86400)
        matches = registry.matching(frozenset({"X", "Y", "Z"}))
        assert len(matches) == 1

    def test_matching_excludes_non_subset(self):
        registry = SubscriptionRegistry()
        registry.create("agent-a", frozenset({"X", "Y"}), 300, 86400)
        matches = registry.matching(frozenset({"X"}))
        assert matches == []

    def test_matching_excludes_lease_expired(self):
        from datetime import datetime, timedelta, timezone

        registry = SubscriptionRegistry()
        record = registry.create("agent-a", frozenset({"X"}), 300, 86400)
        record.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        assert registry.matching(frozenset({"X"})) == []


# ---------------------------------------------------------------------------
# HTTP 統合テスト
# ---------------------------------------------------------------------------


@pytest.fixture()
def settings(tmp_path):
    from relay.config import Settings

    return Settings(
        db_path=str(tmp_path / "test_relay.db"),
        server_log_path=str(tmp_path / "test_relay.jsonl"),
        dispatcher_lock_path=str(tmp_path / "test_relay.lock"),
        auth_tokens={"tok-a": "agent-a", "tok-b": "agent-b", "tok-c": "agent-c"},
    )


@pytest.fixture()
def client(settings):
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


class TestCreateSubscription:
    def test_creates_subscription_and_returns_201(self, client):
        r = client.post(
            "/subscriptions",
            json={"subscriber": "agent-a", "labels": ["topic:474"]},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 201
        body = r.json()
        assert "subscription_id" in body
        assert "lease_expires_at" in body

    def test_subscriber_mismatch_returns_403(self, client):
        r = client.post(
            "/subscriptions",
            json={"subscriber": "agent-b", "labels": ["topic:474"]},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 403
        assert r.json()["code"] == "SubscriberMismatchError"

    def test_empty_labels_returns_400(self, client):
        r = client.post(
            "/subscriptions",
            json={"subscriber": "agent-a", "labels": []},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 400
        assert r.json()["code"] == "LabelValidationError"

    def test_missing_labels_returns_400(self, client):
        r = client.post(
            "/subscriptions", json={"subscriber": "agent-a"}, headers=_auth("tok-a")
        )
        assert r.status_code == 400

    def test_non_string_label_returns_400(self, client):
        r = client.post(
            "/subscriptions",
            json={"subscriber": "agent-a", "labels": ["x", 1]},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 400

    @pytest.mark.parametrize(
        "lease_ttl", [MIN_LEASE_TTL_SECONDS - 1, MAX_LEASE_TTL_SECONDS + 1]
    )
    def test_lease_ttl_out_of_range_returns_400(self, client, lease_ttl):
        r = client.post(
            "/subscriptions",
            json={"subscriber": "agent-a", "labels": ["x"], "lease_ttl": lease_ttl},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 400

    def test_lease_ttl_within_range_accepted(self, client):
        r = client.post(
            "/subscriptions",
            json={"subscriber": "agent-a", "labels": ["x"], "lease_ttl": MIN_LEASE_TTL_SECONDS},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 201

    @pytest.mark.parametrize(
        "retain_seconds", [MIN_RETAIN_SECONDS - 1, MAX_RETAIN_SECONDS + 1]
    )
    def test_retain_seconds_out_of_range_returns_400(self, client, retain_seconds):
        r = client.post(
            "/subscriptions",
            json={
                "subscriber": "agent-a",
                "labels": ["x"],
                "delivery_options": {"retain_seconds": retain_seconds},
            },
            headers=_auth("tok-a"),
        )
        assert r.status_code == 400

    def test_same_subscriber_and_labels_creates_independent_subscriptions(self, client):
        r1 = client.post(
            "/subscriptions",
            json={"subscriber": "agent-a", "labels": ["x"]},
            headers=_auth("tok-a"),
        )
        r2 = client.post(
            "/subscriptions",
            json={"subscriber": "agent-a", "labels": ["x"]},
            headers=_auth("tok-a"),
        )
        assert r1.json()["subscription_id"] != r2.json()["subscription_id"]

    def test_requires_auth(self, client):
        r = client.post("/subscriptions", json={"subscriber": "agent-a", "labels": ["x"]})
        assert r.status_code == 401


class TestRenewLease:
    def _subscribe(self, client, token="tok-a", subscriber="agent-a", labels=None):
        r = client.post(
            "/subscriptions",
            json={"subscriber": subscriber, "labels": labels or ["x"]},
            headers=_auth(token),
        )
        return r.json()["subscription_id"]

    def test_owner_can_renew(self, client):
        sid = self._subscribe(client)
        r = client.put(f"/subscriptions/{sid}/lease", json={}, headers=_auth("tok-a"))
        assert r.status_code == 200
        assert "lease_expires_at" in r.json()

    def test_renew_without_body_reuses_ttl(self, client):
        sid = self._subscribe(client)
        r = client.put(f"/subscriptions/{sid}/lease", headers=_auth("tok-a"))
        assert r.status_code == 200

    def test_non_owner_gets_404(self, client):
        sid = self._subscribe(client)
        r = client.put(f"/subscriptions/{sid}/lease", json={}, headers=_auth("tok-b"))
        assert r.status_code == 404
        assert r.json()["code"] == "SubscriptionNotFoundError"

    def test_missing_subscription_returns_404(self, client):
        r = client.put("/subscriptions/nope/lease", json={}, headers=_auth("tok-a"))
        assert r.status_code == 404

    def test_invalid_lease_ttl_returns_400(self, client):
        sid = self._subscribe(client)
        r = client.put(
            f"/subscriptions/{sid}/lease",
            json={"lease_ttl": MAX_LEASE_TTL_SECONDS + 1},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 400


class TestUnsubscribe:
    def test_owner_can_unsubscribe(self, client):
        r = client.post(
            "/subscriptions", json={"subscriber": "agent-a", "labels": ["x"]}, headers=_auth("tok-a")
        )
        sid = r.json()["subscription_id"]
        r2 = client.delete(f"/subscriptions/{sid}", headers=_auth("tok-a"))
        assert r2.status_code == 204

    def test_non_owner_gets_404(self, client):
        r = client.post(
            "/subscriptions", json={"subscriber": "agent-a", "labels": ["x"]}, headers=_auth("tok-a")
        )
        sid = r.json()["subscription_id"]
        r2 = client.delete(f"/subscriptions/{sid}", headers=_auth("tok-b"))
        assert r2.status_code == 404

    def test_missing_subscription_returns_404(self, client):
        r = client.delete("/subscriptions/nope", headers=_auth("tok-a"))
        assert r.status_code == 404

    def test_unsubscribe_deletes_pending_outbox_immediately(self, client, settings):
        """unsubscribe は DLQ を通らず、未 ack outbox を同一 transaction で即時削除する
        （wire-api.md §5.3）。"""
        r = client.post(
            "/subscriptions", json={"subscriber": "agent-b", "labels": ["x"]}, headers=_auth("tok-b")
        )
        sid = r.json()["subscription_id"]
        client.post(
            "/publish",
            json={"ref": {"type": "decision", "id": 1}, "labels": ["x"]},
            headers=_auth("tok-a"),
        )
        conn = sqlite3.connect(settings.db_path)
        try:
            before = conn.execute(
                "SELECT COUNT(*) FROM outbox WHERE subscription_id = ?", (sid,)
            ).fetchone()[0]
        finally:
            conn.close()
        assert before == 1

        client.delete(f"/subscriptions/{sid}", headers=_auth("tok-b"))

        conn = sqlite3.connect(settings.db_path)
        try:
            after = conn.execute(
                "SELECT COUNT(*) FROM outbox WHERE subscription_id = ?", (sid,)
            ).fetchone()[0]
            dlq_count = conn.execute(
                "SELECT COUNT(*) FROM dlq WHERE subscription_id = ?", (sid,)
            ).fetchone()[0]
        finally:
            conn.close()
        assert after == 0
        assert dlq_count == 0


class TestAckSubscription:
    def _subscribe_and_publish(self, client, labels=("x",)):
        r = client.post(
            "/subscriptions",
            json={"subscriber": "agent-b", "labels": list(labels)},
            headers=_auth("tok-b"),
        )
        sid = r.json()["subscription_id"]
        r2 = client.post(
            "/publish",
            json={"ref": {"type": "decision", "id": 1}, "labels": list(labels)},
            headers=_auth("tok-a"),
        )
        return sid, r2.json()["publish_id"]

    def test_owner_can_ack_and_deletes_outbox(self, client, settings):
        sid, publish_id = self._subscribe_and_publish(client)
        r = client.post(
            f"/subscriptions/{sid}/ack",
            json={"up_to_publish_id": publish_id},
            headers=_auth("tok-b"),
        )
        assert r.status_code == 200

        conn = sqlite3.connect(settings.db_path)
        try:
            rows = conn.execute(
                "SELECT * FROM outbox WHERE subscription_id = ?", (sid,)
            ).fetchall()
        finally:
            conn.close()
        assert rows == []

    def test_ack_is_cumulative_and_idempotent(self, client):
        sid, publish_id = self._subscribe_and_publish(client)
        r1 = client.post(
            f"/subscriptions/{sid}/ack",
            json={"up_to_publish_id": publish_id},
            headers=_auth("tok-b"),
        )
        r2 = client.post(
            f"/subscriptions/{sid}/ack",
            json={"up_to_publish_id": publish_id},
            headers=_auth("tok-b"),
        )
        assert r1.status_code == 200
        assert r2.status_code == 200

    def test_non_owner_gets_404(self, client):
        sid, publish_id = self._subscribe_and_publish(client)
        r = client.post(
            f"/subscriptions/{sid}/ack",
            json={"up_to_publish_id": publish_id},
            headers=_auth("tok-c"),
        )
        assert r.status_code == 404

    def test_missing_subscription_returns_404(self, client):
        r = client.post(
            "/subscriptions/nope/ack", json={"up_to_publish_id": 1}, headers=_auth("tok-a")
        )
        assert r.status_code == 404

    def test_invalid_up_to_publish_id_returns_400(self, client):
        sid, _ = self._subscribe_and_publish(client)
        r = client.post(
            f"/subscriptions/{sid}/ack",
            json={"up_to_publish_id": "abc"},
            headers=_auth("tok-b"),
        )
        assert r.status_code == 400


class TestPublish:
    def test_publish_returns_202_with_matched_count(self, client):
        client.post(
            "/subscriptions", json={"subscriber": "agent-b", "labels": ["X", "Y"]}, headers=_auth("tok-b")
        )
        r = client.post(
            "/publish",
            json={"ref": {"type": "decision", "id": 42}, "labels": ["X", "Y", "Z"]},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 202
        body = r.json()
        assert isinstance(body["publish_id"], int)
        assert body["matched_subscriptions"] == 1

    def test_subset_matching_and_semantics(self, client):
        """subscribe.labels=[X] は publish.labels=[X,Y] のみに match。[Y] には match しない。"""
        client.post(
            "/subscriptions", json={"subscriber": "agent-b", "labels": ["X"]}, headers=_auth("tok-b")
        )
        r = client.post(
            "/publish",
            json={"ref": {"type": "decision", "id": 1}, "labels": ["Y"]},
            headers=_auth("tok-a"),
        )
        assert r.json()["matched_subscriptions"] == 0

    def test_missing_ref_returns_400(self, client):
        r = client.post(
            "/publish", json={"labels": ["x"]}, headers=_auth("tok-a")
        )
        assert r.status_code == 400

    def test_missing_labels_returns_400(self, client):
        r = client.post(
            "/publish",
            json={"ref": {"type": "decision", "id": 1}},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 400

    def test_publish_id_is_global_monotonic_across_lanes(self, client):
        """subscription レーンと stream レーンで publish_id を共有（グローバル単調）。"""
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        r1 = client.post(
            "/streams/s1/messages", json={"body": "hello"}, headers=_auth("tok-a")
        )
        r2 = client.post(
            "/publish",
            json={"ref": {"type": "decision", "id": 1}, "labels": ["x"]},
            headers=_auth("tok-a"),
        )
        assert r2.json()["publish_id"] > r1.json()["publish_id"]

    def test_idempotency_key_dedup_within_window(self, client):
        r1 = client.post(
            "/publish",
            json={
                "ref": {"type": "decision", "id": 1},
                "labels": ["x"],
                "idempotency_key": "same-key",
            },
            headers=_auth("tok-a"),
        )
        r2 = client.post(
            "/publish",
            json={
                "ref": {"type": "decision", "id": 999},
                "labels": ["y"],
                "idempotency_key": "same-key",
            },
            headers=_auth("tok-a"),
        )
        assert r1.json()["publish_id"] == r2.json()["publish_id"]

    def test_different_idempotency_key_not_deduped(self, client):
        r1 = client.post(
            "/publish",
            json={"ref": {"type": "decision", "id": 1}, "labels": ["x"], "idempotency_key": "k1"},
            headers=_auth("tok-a"),
        )
        r2 = client.post(
            "/publish",
            json={"ref": {"type": "decision", "id": 1}, "labels": ["x"], "idempotency_key": "k2"},
            headers=_auth("tok-a"),
        )
        assert r1.json()["publish_id"] != r2.json()["publish_id"]

    def test_requires_auth(self, client):
        r = client.post("/publish", json={"ref": {"type": "decision", "id": 1}, "labels": ["x"]})
        assert r.status_code == 401

    def test_rate_limit_returns_429_with_retry_after(self, tmp_path):
        from relay.config import Settings

        limited_settings = Settings(
            db_path=str(tmp_path / "rl.db"),
            server_log_path=str(tmp_path / "rl.jsonl"),
            dispatcher_lock_path=str(tmp_path / "rl.lock"),
            auth_tokens={"tok-a": "agent-a"},
            publish_rate_limit_per_second=1,
        )
        app = create_app(limited_settings)
        with TestClient(app) as c:
            ok = c.post(
                "/publish",
                json={"ref": {"type": "decision", "id": 1}, "labels": ["x"]},
                headers=_auth("tok-a"),
            )
            assert ok.status_code == 202
            limited = c.post(
                "/publish",
                json={"ref": {"type": "decision", "id": 2}, "labels": ["x"]},
                headers=_auth("tok-a"),
            )
            assert limited.status_code == 429
            assert "Retry-After" in limited.headers
