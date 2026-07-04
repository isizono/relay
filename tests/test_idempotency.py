"""relay.idempotency テストスイート。

IdempotencyStore の atomic な check_and_reserve / finalize / release（同一キーの
並行 publish で勝者が 1 つに絞られること）、resolve_or_reserve の待機・予約取り直し、
build_key の publisher identity 分離を単体テストで検証する。

HTTP 統合テストでは、subscription / stream 両レーンの publish handler について
「同一 idempotency_key でも identity が異なれば dedup されない」ことと、
「publish 失敗（DB エラー）後に同一キーの再送が予約に阻まれず成功する」ことを検証する。
"""
import asyncio
import sqlite3
import threading

import pytest
from starlette.testclient import TestClient

from relay.app import create_app
from relay.config import Settings
from relay.idempotency import IdempotencyStore, build_key, resolve_or_reserve


# ---------------------------------------------------------------------------
# IdempotencyStore 単体テスト
# ---------------------------------------------------------------------------


class TestCheckAndReserve:
    def test_first_caller_wins_reservation(self):
        store = IdempotencyStore()
        outcome = store.check_and_reserve("k1")
        assert outcome.reserved is True
        assert outcome.publish_id is None
        assert outcome.pending is None

    def test_returns_existing_publish_id_after_finalize(self):
        store = IdempotencyStore()
        store.check_and_reserve("k1")
        store.finalize("k1", 42)
        outcome = store.check_and_reserve("k1")
        assert outcome.publish_id == 42
        assert outcome.reserved is False
        assert outcome.pending is None

    def test_second_caller_sees_pending_reservation(self):
        store = IdempotencyStore()
        store.check_and_reserve("k1")
        outcome = store.check_and_reserve("k1")
        assert outcome.reserved is False
        assert outcome.publish_id is None
        assert outcome.pending is not None

    def test_release_allows_new_reservation(self):
        store = IdempotencyStore()
        store.check_and_reserve("k1")
        store.release("k1")
        outcome = store.check_and_reserve("k1")
        assert outcome.reserved is True

    def test_different_keys_reserve_independently(self):
        store = IdempotencyStore()
        assert store.check_and_reserve("k1").reserved is True
        assert store.check_and_reserve("k2").reserved is True

    def test_concurrent_reserve_single_winner(self):
        """同一キーへ 16 スレッドが同時に check_and_reserve しても勝者は 1 つだけになる。

        判定（check）と予約（reserve）が別ロック区間だと複数スレッドが判定を
        すり抜けて publish が二重実行されるため、勝者数 1 がここでの保証対象。
        """
        store = IdempotencyStore()
        n_threads = 16
        barrier = threading.Barrier(n_threads)
        outcomes = []
        outcomes_lock = threading.Lock()

        def attempt():
            barrier.wait()
            outcome = store.check_and_reserve("k1")
            with outcomes_lock:
                outcomes.append(outcome)

        threads = [threading.Thread(target=attempt) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        winners = [o for o in outcomes if o.reserved]
        losers = [o for o in outcomes if not o.reserved]
        assert len(winners) == 1
        assert len(losers) == n_threads - 1
        assert all(o.pending is not None for o in losers)


class TestPendingWait:
    def test_wait_returns_publish_id_after_finalize(self):
        store = IdempotencyStore()
        store.check_and_reserve("k1")
        pending = store.check_and_reserve("k1").pending
        timer = threading.Timer(0.05, store.finalize, args=("k1", 7))
        timer.start()
        try:
            publish_id = asyncio.run(pending.wait(5.0))
        finally:
            timer.cancel()
        assert publish_id == 7

    def test_wait_returns_none_after_release(self):
        store = IdempotencyStore()
        store.check_and_reserve("k1")
        pending = store.check_and_reserve("k1").pending
        timer = threading.Timer(0.05, store.release, args=("k1",))
        timer.start()
        try:
            publish_id = asyncio.run(pending.wait(5.0))
        finally:
            timer.cancel()
        assert publish_id is None

    def test_wait_returns_none_on_timeout(self):
        store = IdempotencyStore()
        store.check_and_reserve("k1")
        pending = store.check_and_reserve("k1").pending
        assert asyncio.run(pending.wait(0.05)) is None


class TestResolveOrReserve:
    def test_first_caller_gets_none_and_holds_reservation(self):
        store = IdempotencyStore()
        assert asyncio.run(resolve_or_reserve(store, "k1")) is None
        # 予約が取られているので、後続の同一キーは pending を観測する。
        assert store.check_and_reserve("k1").pending is not None

    def test_returns_existing_publish_id_on_dedup_hit(self):
        store = IdempotencyStore()
        store.check_and_reserve("k1")
        store.finalize("k1", 9)
        assert asyncio.run(resolve_or_reserve(store, "k1")) == 9

    def test_waits_for_winner_and_returns_winner_publish_id(self):
        """予約中キーに到着したリクエストは勝者の finalize を待って同じ publish_id を得る。"""
        store = IdempotencyStore()
        store.check_and_reserve("k1")
        timer = threading.Timer(0.05, store.finalize, args=("k1", 11))
        timer.start()
        try:
            publish_id = asyncio.run(resolve_or_reserve(store, "k1"))
        finally:
            timer.cancel()
        assert publish_id == 11

    def test_reserves_after_winner_release(self):
        """勝者が release（publish 失敗）した場合、待機側が予約を取り直して勝者になる。"""
        store = IdempotencyStore()
        store.check_and_reserve("k1")
        timer = threading.Timer(0.05, store.release, args=("k1",))
        timer.start()
        try:
            result = asyncio.run(resolve_or_reserve(store, "k1"))
        finally:
            timer.cancel()
        assert result is None
        assert store.check_and_reserve("k1").pending is not None


# ---------------------------------------------------------------------------
# build_key の publisher identity 分離
# ---------------------------------------------------------------------------


class TestBuildKeyIdentityScope:
    def test_explicit_key_differs_across_identities_subscription_lane(self):
        key_a = build_key(
            lane="subscription", publisher_identity="agent-a", explicit_key="k", scope="s"
        )
        key_b = build_key(
            lane="subscription", publisher_identity="agent-b", explicit_key="k", scope="s"
        )
        assert key_a != key_b

    def test_explicit_key_differs_across_identities_stream_lane(self):
        key_a = build_key(lane="stream", publisher_identity="agent-a", explicit_key="k", scope="s1")
        key_b = build_key(lane="stream", publisher_identity="agent-b", explicit_key="k", scope="s1")
        assert key_a != key_b

    def test_explicit_key_matches_for_same_identity(self):
        key_1 = build_key(
            lane="subscription", publisher_identity="agent-a", explicit_key="k", scope="s"
        )
        key_2 = build_key(
            lane="subscription", publisher_identity="agent-a", explicit_key="k", scope="s"
        )
        assert key_1 == key_2

    def test_pseudo_key_differs_across_identities(self):
        """explicit_key 省略時の擬似キーも identity で分離される。"""
        key_a = build_key(
            lane="subscription",
            publisher_identity="agent-a",
            explicit_key=None,
            scope="s",
            labels=["x"],
            body="hello",
        )
        key_b = build_key(
            lane="subscription",
            publisher_identity="agent-b",
            explicit_key=None,
            scope="s",
            labels=["x"],
            body="hello",
        )
        assert key_a != key_b


# ---------------------------------------------------------------------------
# HTTP 統合テスト
# ---------------------------------------------------------------------------


@pytest.fixture()
def settings(tmp_path):
    return Settings(
        db_path=str(tmp_path / "test_relay.db"),
        server_log_path=str(tmp_path / "test_relay.jsonl"),
        auth_tokens={"tok-a": "agent-a", "tok-b": "agent-b"},
    )


@pytest.fixture()
def client(settings):
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


class TestCrossIdentityDedup:
    def test_publish_lane_same_key_different_identity_not_deduped(self, client):
        """subscription レーンで同一 idempotency_key でも identity が違えば別 publish になる。"""
        r1 = client.post(
            "/publish",
            json={
                "ref": {"type": "decision", "id": 1},
                "labels": ["x"],
                "idempotency_key": "shared-key",
            },
            headers=_auth("tok-a"),
        )
        r2 = client.post(
            "/publish",
            json={
                "ref": {"type": "decision", "id": 1},
                "labels": ["x"],
                "idempotency_key": "shared-key",
            },
            headers=_auth("tok-b"),
        )
        assert r1.status_code == 202
        assert r2.status_code == 202
        assert r1.json()["publish_id"] != r2.json()["publish_id"]

    def test_stream_lane_same_key_different_identity_not_deduped(self, client):
        """stream レーンで同一 idempotency_key でも identity が違えば別 publish になる。"""
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-b", "access": "write"},
            headers=_auth("tok-a"),
        )
        r1 = client.post(
            "/streams/s1/messages",
            json={"body": "hello", "idempotency_key": "shared-key"},
            headers=_auth("tok-a"),
        )
        r2 = client.post(
            "/streams/s1/messages",
            json={"body": "hello", "idempotency_key": "shared-key"},
            headers=_auth("tok-b"),
        )
        assert r1.status_code == 202
        assert r2.status_code == 202
        assert r1.json()["publish_id"] != r2.json()["publish_id"]


class TestReservationReleasedOnPublishFailure:
    def test_publish_lane_retry_succeeds_after_db_error(self, client, monkeypatch):
        """subscription レーンで DB エラー（503）後、同一キーの再送が予約に阻まれず成功する。"""

        def broken_connection(request):
            raise sqlite3.OperationalError("disk I/O error")

        monkeypatch.setattr("relay.subscriptions._get_connection", broken_connection)
        r1 = client.post(
            "/publish",
            json={
                "ref": {"type": "decision", "id": 1},
                "labels": ["x"],
                "idempotency_key": "retry-key",
            },
            headers=_auth("tok-a"),
        )
        assert r1.status_code == 503

        monkeypatch.undo()
        r2 = client.post(
            "/publish",
            json={
                "ref": {"type": "decision", "id": 1},
                "labels": ["x"],
                "idempotency_key": "retry-key",
            },
            headers=_auth("tok-a"),
        )
        assert r2.status_code == 202
        assert isinstance(r2.json()["publish_id"], int)

    def test_stream_lane_retry_succeeds_after_db_error(self, client, monkeypatch):
        """stream レーンで DB エラー（503）後、同一キーの再送が予約に阻まれず成功する。"""
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))

        def broken_connection(request):
            raise sqlite3.OperationalError("disk I/O error")

        monkeypatch.setattr("relay.streams._get_connection", broken_connection)
        r1 = client.post(
            "/streams/s1/messages",
            json={"body": "hello", "idempotency_key": "retry-key"},
            headers=_auth("tok-a"),
        )
        assert r1.status_code == 503

        monkeypatch.undo()
        r2 = client.post(
            "/streams/s1/messages",
            json={"body": "hello", "idempotency_key": "retry-key"},
            headers=_auth("tok-a"),
        )
        assert r2.status_code == 202
        assert isinstance(r2.json()["publish_id"], int)
