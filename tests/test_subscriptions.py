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
from relay.errors import ResourceLimitExceeded
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

    def test_evict_expired_removes_only_past_grace_period(self):
        """猶予期間を過ぎた lease 切れのみ除去し、猶予内・生存中は残す。"""
        from datetime import datetime, timedelta, timezone

        registry = SubscriptionRegistry()
        now = datetime.now(timezone.utc)

        long_expired = registry.create("agent-a", frozenset({"x"}), 300, 86400)
        long_expired.lease_expires_at = now - timedelta(seconds=7200)  # 2h 前に失効

        recently_expired = registry.create("agent-b", frozenset({"x"}), 300, 86400)
        recently_expired.lease_expires_at = now - timedelta(seconds=10)  # 10s 前に失効

        alive = registry.create("agent-c", frozenset({"x"}), 300, 86400)

        evicted = registry.evict_expired(older_than_seconds=3600)

        assert evicted == [long_expired.subscription_id]
        assert registry.get(long_expired.subscription_id) is None
        assert registry.get(recently_expired.subscription_id) is not None
        assert registry.get(alive.subscription_id) is not None

    def test_evict_expired_noop_when_nothing_past_grace_period(self):
        registry = SubscriptionRegistry()
        registry.create("agent-a", frozenset({"x"}), 300, 86400)
        assert registry.evict_expired(older_than_seconds=3600) == []


class TestSubscriptionRegistryResourceLimits:
    def test_total_limit_rejects_create_beyond_cap(self):
        registry = SubscriptionRegistry(max_total=2, max_per_identity=100)
        registry.create("agent-a", frozenset({"x"}), 300, 86400)
        registry.create("agent-b", frozenset({"x"}), 300, 86400)
        with pytest.raises(ResourceLimitExceeded) as exc:
            registry.create("agent-c", frozenset({"x"}), 300, 86400)
        assert exc.value.scope == "total"

    def test_per_identity_limit_rejects_third_from_same_subscriber(self):
        registry = SubscriptionRegistry(max_total=100, max_per_identity=2)
        registry.create("agent-a", frozenset({"x"}), 300, 86400)
        registry.create("agent-a", frozenset({"y"}), 300, 86400)
        with pytest.raises(ResourceLimitExceeded) as exc:
            registry.create("agent-a", frozenset({"z"}), 300, 86400)
        assert exc.value.scope == "per_identity"

    def test_per_identity_limit_is_counted_per_subscriber(self):
        registry = SubscriptionRegistry(max_total=100, max_per_identity=1)
        registry.create("agent-a", frozenset({"x"}), 300, 86400)
        # agent-a は上限だが agent-b は自分の枠で作成できる。
        assert registry.create("agent-b", frozenset({"x"}), 300, 86400) is not None

    def test_delete_frees_per_identity_slot(self):
        registry = SubscriptionRegistry(max_total=100, max_per_identity=1)
        record = registry.create("agent-a", frozenset({"x"}), 300, 86400)
        with pytest.raises(ResourceLimitExceeded):
            registry.create("agent-a", frozenset({"y"}), 300, 86400)
        registry.delete(record.subscription_id)
        # delete で枠が空いたので同一 subscriber が再び作成できる。
        assert registry.create("agent-a", frozenset({"y"}), 300, 86400) is not None

    def test_evict_expired_frees_per_identity_slot(self):
        from datetime import datetime, timedelta, timezone

        registry = SubscriptionRegistry(max_total=100, max_per_identity=1)
        record = registry.create("agent-a", frozenset({"x"}), 300, 86400)
        record.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=7200)
        assert registry.evict_expired(older_than_seconds=3600) == [record.subscription_id]
        # evict で枠が空いたので同一 subscriber が再び作成できる。
        assert registry.create("agent-a", frozenset({"y"}), 300, 86400) is not None


# ---------------------------------------------------------------------------
# matching() 性能ベンチマーク（wire-api.md §10: 10,000 subs × 100 labels で p99 200ms）
# ---------------------------------------------------------------------------


class TestMatchingPerformance:
    """`matching()` の線形走査が §10 の性能 SLO を満たすかを実測で確認する。

    ARCHITECTURE.md に「10,000 subscriptions × 100 labels で未検証」と明記されていた点の実測。
    測定結果（開発機で p99 が sub-millisecond）から、inverted index 等の最適化は現時点で
    過剰実装と判断できる。この test は実測を残すと同時に、O(n^2) 化のような致命的な性能退化を
    SLO（200ms）を上限として検知する回帰ガードでもある（実測はその 2〜3 桁下で通る）。
    """

    SUBSCRIPTION_COUNT = 10_000
    PUBLISH_LABEL_COUNT = 100
    SLO_P99_MS = 200.0

    def test_matching_meets_p99_slo_at_scale(self, capsys):
        import random
        import time

        rng = random.Random(42)
        vocab = [f"label:{i}" for i in range(500)]

        # 単一 subscriber に SUBSCRIPTION_COUNT 件を積むベンチマークなので、DoS 防御の
        # per-identity 上限（本番既定 1000）を SUBSCRIPTION_COUNT まで引き上げて構築する。
        registry = SubscriptionRegistry(
            max_total=self.SUBSCRIPTION_COUNT, max_per_identity=self.SUBSCRIPTION_COUNT
        )
        for _ in range(self.SUBSCRIPTION_COUNT):
            k = rng.randint(1, 5)
            registry.create("agent-x", frozenset(rng.sample(vocab, k)), 300, 86400)

        publish_labels = frozenset(rng.sample(vocab, self.PUBLISH_LABEL_COUNT))

        registry.matching(publish_labels)  # warmup（import / branch prediction 平準化）

        samples_ms: list[float] = []
        for _ in range(50):
            start = time.perf_counter()
            matches = registry.matching(publish_labels)
            samples_ms.append((time.perf_counter() - start) * 1000)

        samples_ms.sort()
        p50 = samples_ms[len(samples_ms) // 2]
        p99 = samples_ms[min(len(samples_ms) - 1, int(len(samples_ms) * 0.99))]
        with capsys.disabled():
            print(
                f"\n[matching bench] subs={self.SUBSCRIPTION_COUNT}"
                f" publish_labels={self.PUBLISH_LABEL_COUNT} matched={len(matches)}"
                f" | per-call ms: p50={p50:.3f} p99={p99:.3f} max={samples_ms[-1]:.3f}"
            )

        assert p99 < self.SLO_P99_MS

    def test_matching_worst_case_all_match(self):
        """全 subscription が publish にマッチする最悪ケースでも SLO を満たすことを確認する。"""
        import random
        import time

        rng = random.Random(7)
        vocab = [f"label:{i}" for i in range(self.PUBLISH_LABEL_COUNT)]
        publish_labels = frozenset(vocab)

        # 単一 subscriber に SUBSCRIPTION_COUNT 件を積むベンチマークなので、DoS 防御の
        # per-identity 上限（本番既定 1000）を SUBSCRIPTION_COUNT まで引き上げて構築する。
        registry = SubscriptionRegistry(
            max_total=self.SUBSCRIPTION_COUNT, max_per_identity=self.SUBSCRIPTION_COUNT
        )
        for _ in range(self.SUBSCRIPTION_COUNT):
            # 各 subscription は publish labels の subset（= 必ずマッチ）。
            registry.create("agent-x", frozenset({rng.choice(vocab)}), 300, 86400)

        registry.matching(publish_labels)  # warmup

        start = time.perf_counter()
        matches = registry.matching(publish_labels)
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert len(matches) == self.SUBSCRIPTION_COUNT
        assert elapsed_ms < self.SLO_P99_MS


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


class TestCreateSubscriptionResourceLimits:
    @pytest.fixture()
    def limited_client(self, tmp_path):
        from relay.config import Settings

        settings = Settings(
            db_path=str(tmp_path / "limited.db"),
            server_log_path=str(tmp_path / "limited.jsonl"),
            dispatcher_lock_path=str(tmp_path / "limited.lock"),
            auth_tokens={"tok-a": "agent-a", "tok-b": "agent-b"},
            max_subscriptions_total=3,
            max_subscriptions_per_identity=2,
        )
        app = create_app(settings)
        with TestClient(app) as c:
            yield c

    def _subscribe(self, client, token, subscriber, label):
        return client.post(
            "/subscriptions",
            json={"subscriber": subscriber, "labels": [label]},
            headers=_auth(token),
        )

    def test_within_limits_returns_201(self, limited_client):
        r = self._subscribe(limited_client, "tok-a", "agent-a", "x")
        assert r.status_code == 201

    def test_per_identity_limit_returns_429(self, limited_client):
        self._subscribe(limited_client, "tok-a", "agent-a", "x")
        self._subscribe(limited_client, "tok-a", "agent-a", "y")
        # agent-a の 3 件目は per-identity 上限(2)超過で拒否される。
        r = self._subscribe(limited_client, "tok-a", "agent-a", "z")
        assert r.status_code == 429
        assert r.json()["code"] == "ResourceLimitExceededError"

    def test_total_limit_returns_429(self, limited_client):
        # agent-a 2 件 + agent-b 1 件で total 上限(3)に到達させる。
        self._subscribe(limited_client, "tok-a", "agent-a", "x")
        self._subscribe(limited_client, "tok-a", "agent-a", "y")
        self._subscribe(limited_client, "tok-b", "agent-b", "x")
        # agent-b は per-identity 枠に空きがあるが total 上限で拒否される。
        r = self._subscribe(limited_client, "tok-b", "agent-b", "y")
        assert r.status_code == 429
        assert r.json()["code"] == "ResourceLimitExceededError"


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

    def test_owner_lease_expired_returns_410(self, client):
        """所有者本人でも lease 切れ済み subscription への renew は 410（wire-api.md §5.3）。

        renew の目的自体が「切れかけの lease を延命する」ことだが、いったん期限を過ぎた
        subscription は re-subscribe が必要というのが仕様の意図であり、410 は
        registry に残存している間だけ返る best-effort のヒント（§5.7）。
        """
        from datetime import datetime, timedelta, timezone

        sid = self._subscribe(client)
        record = client.app.state.subscription_registry.get(sid)
        record.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)

        r = client.put(f"/subscriptions/{sid}/lease", json={}, headers=_auth("tok-a"))
        assert r.status_code == 410
        assert r.json()["code"] == "SubscriptionGoneError"


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

    def test_owner_lease_expired_returns_410(self, client):
        """所有者本人でも lease 切れ済み subscription への ack は 410（wire-api.md §5.6）。"""
        from datetime import datetime, timedelta, timezone

        sid, publish_id = self._subscribe_and_publish(client)
        record = client.app.state.subscription_registry.get(sid)
        record.lease_expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)

        r = client.post(
            f"/subscriptions/{sid}/ack",
            json={"up_to_publish_id": publish_id},
            headers=_auth("tok-b"),
        )
        assert r.status_code == 410
        assert r.json()["code"] == "SubscriptionGoneError"


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


class TestInputFieldCaps:
    """title 文字列長・labels 個数・label 文字列長のサーバー側上限（POST /subscriptions,
    POST /publish）。上限内は通り、超過は 400 で拒否されることを検証する。
    """

    @pytest.fixture()
    def capped_client(self, tmp_path):
        from relay.config import Settings

        settings = Settings(
            db_path=str(tmp_path / "caps.db"),
            server_log_path=str(tmp_path / "caps.jsonl"),
            dispatcher_lock_path=str(tmp_path / "caps.lock"),
            auth_tokens={"tok-a": "agent-a"},
            max_title_length=10,
            max_labels_count=3,
            max_label_length=5,
        )
        app = create_app(settings)
        with TestClient(app) as c:
            yield c

    # --- POST /subscriptions ---

    def test_subscribe_labels_count_over_cap_returns_400(self, capped_client):
        r = capped_client.post(
            "/subscriptions",
            json={"subscriber": "agent-a", "labels": ["a", "b", "c", "d"]},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 400
        assert r.json()["code"] == "LabelValidationError"

    def test_subscribe_label_length_over_cap_returns_400(self, capped_client):
        r = capped_client.post(
            "/subscriptions",
            json={"subscriber": "agent-a", "labels": ["toolong"]},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 400
        assert r.json()["code"] == "LabelValidationError"

    def test_subscribe_at_cap_boundary_accepted(self, capped_client):
        r = capped_client.post(
            "/subscriptions",
            json={"subscriber": "agent-a", "labels": ["aaaaa", "b", "c"]},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 201

    # --- POST /publish ---

    def test_publish_labels_count_over_cap_returns_400(self, capped_client):
        r = capped_client.post(
            "/publish",
            json={
                "ref": {"type": "decision", "id": 1},
                "labels": ["a", "b", "c", "d"],
            },
            headers=_auth("tok-a"),
        )
        assert r.status_code == 400
        assert r.json()["code"] == "LabelValidationError"

    def test_publish_label_length_over_cap_returns_400(self, capped_client):
        r = capped_client.post(
            "/publish",
            json={"ref": {"type": "decision", "id": 1}, "labels": ["toolong"]},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 400
        assert r.json()["code"] == "LabelValidationError"

    def test_publish_title_length_over_cap_returns_400(self, capped_client):
        r = capped_client.post(
            "/publish",
            json={
                "ref": {"type": "decision", "id": 1},
                "labels": ["x"],
                "title": "x" * 11,
            },
            headers=_auth("tok-a"),
        )
        assert r.status_code == 400
        assert r.json()["code"] == "InvalidRequestError"

    def test_publish_at_cap_boundary_accepted(self, capped_client):
        r = capped_client.post(
            "/publish",
            json={
                "ref": {"type": "decision", "id": 1},
                "labels": ["aaaaa", "b", "c"],
                "title": "x" * 10,
            },
            headers=_auth("tok-a"),
        )
        assert r.status_code == 202
