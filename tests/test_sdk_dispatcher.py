"""relay_sdk.outbox.dispatcher の単体テスト。

daemon loop 全体は FakeRelay に対して 1 本流し、cycle 単位のリトライ / dead 判定は
httpx.MockTransport で精密に検証する。
"""
from __future__ import annotations

import sqlite3
import threading
import time
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from relay_sdk.outbox import create_outbox_table, publish, run_dispatcher
from relay_sdk.outbox.dispatcher import (
    DispatcherAlreadyRunning,
    _acquire_lock,
    _dispatch_once,
    _gc_dlq,
    _release_lock,
)
from relay_sdk.testing import FakeRelay


@pytest.fixture()
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    create_outbox_table(c)
    yield c
    c.close()


def _mock_client(handler) -> httpx.Client:
    return httpx.Client(base_url="http://relay.test", transport=httpx.MockTransport(handler))


def _enqueue(conn, ref_id="1", labels=("a",)):
    row_id = publish(conn, ref_type="log", ref_id=ref_id, labels=list(labels))
    conn.commit()
    return row_id


class TestDispatchCycle:
    def test_success_sets_processed_at(self, conn):
        row_id = _enqueue(conn)

        def handler(request):
            return httpx.Response(202, json={"publish_id": 1, "matched_subscriptions": 0})

        with _mock_client(handler) as client:
            delivered = _dispatch_once(
                conn, client, retry_backoff_base_seconds=0.01, retry_backoff_cap_seconds=300.0,
                transient_retry_deadline_seconds=86400.0, backoff_until={},
            )
        assert delivered == 1
        row = conn.execute("SELECT * FROM relay_outbox WHERE id = ?", (row_id,)).fetchone()
        assert row["processed_at"] is not None
        assert row["dead_at"] is None

    def test_permanent_error_dead_immediately(self, conn):
        row_id = _enqueue(conn)

        def handler(request):
            return httpx.Response(400, json={"code": "InvalidRequestError", "message": "bad"})

        with _mock_client(handler) as client:
            _dispatch_once(
                conn, client, retry_backoff_base_seconds=0.01, retry_backoff_cap_seconds=300.0,
                transient_retry_deadline_seconds=86400.0, backoff_until={},
            )
        row = conn.execute("SELECT * FROM relay_outbox WHERE id = ?", (row_id,)).fetchone()
        assert row["dead_at"] is not None
        assert row["processed_at"] is None
        assert row["retry_count"] == 0  # permanent はカウント進めず即 dead

    def test_transient_increments_retry_and_backs_off(self, conn):
        row_id = _enqueue(conn)

        def handler(request):
            return httpx.Response(503, json={"code": "OutboxUnavailableError", "message": "x"})

        backoff_until: dict[int, float] = {}
        with _mock_client(handler) as client:
            _dispatch_once(
                conn, client, retry_backoff_base_seconds=10.0, retry_backoff_cap_seconds=300.0,
                transient_retry_deadline_seconds=86400.0, backoff_until=backoff_until,
            )
        row = conn.execute("SELECT * FROM relay_outbox WHERE id = ?", (row_id,)).fetchone()
        assert row["retry_count"] == 1
        assert row["dead_at"] is None
        assert row_id in backoff_until  # 次回 polling まで待つ
        # Full Jitter: [0, min(cap, base * 2**0)] = [0, 10] に収まる。
        assert 0.0 <= backoff_until[row_id] - time.monotonic() <= 10.0

    def test_backoff_gates_retry(self, conn):
        """backoff 中の行は同一 cycle で再送されない。"""
        row_id = _enqueue(conn)
        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            return httpx.Response(503)

        with _mock_client(handler) as client:
            _dispatch_once(conn, client, retry_backoff_base_seconds=100.0,
                           retry_backoff_cap_seconds=300.0,
                           transient_retry_deadline_seconds=86400.0, backoff_until={})
            # まだ backoff 中（100s）なので次 cycle では POST が発生しない。
            backoff = {row_id: time.monotonic() + 100.0}
            _dispatch_once(conn, client, retry_backoff_base_seconds=100.0,
                           retry_backoff_cap_seconds=300.0,
                           transient_retry_deadline_seconds=86400.0, backoff_until=backoff)
        assert calls["n"] == 1

    def test_transient_within_deadline_keeps_retrying(self, conn):
        """24h（既定 transient_retry_deadline_seconds）以内は retry 回数によらず dead 化しない
        回帰テスト（旧実装は max_retry=5 回で機械的に dead 化していた）。"""
        row_id = _enqueue(conn)

        def handler(request):
            return httpx.Response(503)

        backoff_until: dict[int, float] = {}
        with _mock_client(handler) as client:
            for _ in range(5):
                backoff_until.clear()  # backoff を無効化して即リトライさせる
                _dispatch_once(
                    conn, client, retry_backoff_base_seconds=0.0, retry_backoff_cap_seconds=300.0,
                    transient_retry_deadline_seconds=86400.0, backoff_until=backoff_until,
                )
        row = conn.execute("SELECT * FROM relay_outbox WHERE id = ?", (row_id,)).fetchone()
        assert row["retry_count"] == 5
        assert row["dead_at"] is None

    def test_transient_reaches_deadline_then_dead(self, conn):
        """created_at から transient_retry_deadline_seconds を過ぎた行は、次の transient
        failure で dead 化する（enqueue から 24h 再送し続けてもダメなら DLQ、§2.1）。"""
        row_id = _enqueue(conn)
        old_created_at = (datetime.now(timezone.utc) - timedelta(hours=25)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        conn.execute(
            "UPDATE relay_outbox SET created_at = ? WHERE id = ?", (old_created_at, row_id)
        )
        conn.commit()

        def handler(request):
            return httpx.Response(503)

        with _mock_client(handler) as client:
            _dispatch_once(
                conn, client, retry_backoff_base_seconds=0.01, retry_backoff_cap_seconds=300.0,
                transient_retry_deadline_seconds=86400.0, backoff_until={},
            )
        row = conn.execute("SELECT * FROM relay_outbox WHERE id = ?", (row_id,)).fetchone()
        assert row["retry_count"] == 1
        assert row["dead_at"] is not None

    def test_429_respects_retry_after(self, conn):
        row_id = _enqueue(conn)

        def handler(request):
            return httpx.Response(429, headers={"Retry-After": "7"})

        backoff_until: dict[int, float] = {}
        before = time.monotonic()
        with _mock_client(handler) as client:
            _dispatch_once(conn, client, retry_backoff_base_seconds=0.01,
                           retry_backoff_cap_seconds=300.0,
                           transient_retry_deadline_seconds=86400.0, backoff_until=backoff_until)
        # Retry-After=7 が backoff に反映される（Full Jitter の base 0.01 ではなく ~7）。
        assert backoff_until[row_id] - before >= 6.0


class TestDlqGc:
    def test_gc_deletes_rows_dead_over_7_days(self, conn):
        old = (datetime.now(timezone.utc) - timedelta(days=8)).strftime("%Y-%m-%dT%H:%M:%SZ")
        recent = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
        id_old = _enqueue(conn, ref_id="old")
        id_recent = _enqueue(conn, ref_id="recent")
        conn.execute("UPDATE relay_outbox SET dead_at = ? WHERE id = ?", (old, id_old))
        conn.execute("UPDATE relay_outbox SET dead_at = ? WHERE id = ?", (recent, id_recent))
        conn.commit()
        deleted = _gc_dlq(conn)
        assert deleted == 1
        remaining = {r["ref_id"] for r in conn.execute("SELECT ref_id FROM relay_outbox").fetchall()}
        assert remaining == {"recent"}


class TestSingletonLock:
    def test_second_dispatcher_raises(self, tmp_path):
        db = str(tmp_path / "app.db")
        lock_path = f"{db}.dispatcher.lock"
        fd = _acquire_lock(lock_path)
        try:
            with pytest.raises(DispatcherAlreadyRunning):
                run_dispatcher(db_path=db, relay_base_url="http://unused.test")
        finally:
            _release_lock(fd)


class TestCli:
    def test_main_requires_outbox_db(self, monkeypatch):
        from relay_sdk.outbox.__main__ import main

        monkeypatch.delenv("RELAY_OUTBOX_DB", raising=False)
        assert main([]) == 2

    def test_main_requires_base_url(self, monkeypatch, tmp_path):
        from relay_sdk.outbox.__main__ import main

        monkeypatch.setenv("RELAY_OUTBOX_DB", str(tmp_path / "app.db"))
        monkeypatch.delenv("RELAY_BASE_URL", raising=False)
        assert main([]) == 2


class TestDaemonAgainstFakeRelay:
    def test_outage_then_recovery_delivers(self, tmp_path):
        with FakeRelay() as fake:
            db = str(tmp_path / "app.db")
            c = sqlite3.connect(db)
            create_outbox_table(c)
            row_id = publish(c, ref_type="log", ref_id="1", labels=["a"])
            c.commit()
            c.close()

            fake.simulate_outage(True)  # POST /publish が 503
            stop = threading.Event()
            t = threading.Thread(
                target=run_dispatcher,
                kwargs=dict(
                    db_path=db,
                    relay_base_url=fake.base_url,
                    agent_card_path=fake.fake_agent_card_path(),
                    poll_interval_seconds=0.03,
                    retry_backoff_base_seconds=0.02,
                    stop_event=stop,
                ),
                daemon=True,
            )
            t.start()
            time.sleep(0.3)  # outage 中は配達されない
            check = sqlite3.connect(db)
            pending = check.execute(
                "SELECT processed_at FROM relay_outbox WHERE id = ?", (row_id,)
            ).fetchone()[0]
            check.close()
            assert pending is None

            fake.simulate_outage(False)  # 復旧
            deadline = time.time() + 5
            delivered = False
            while time.time() < deadline:
                check = sqlite3.connect(db)
                v = check.execute(
                    "SELECT processed_at FROM relay_outbox WHERE id = ?", (row_id,)
                ).fetchone()[0]
                check.close()
                if v is not None:
                    delivered = True
                    break
                time.sleep(0.05)
            stop.set()
            t.join(timeout=3)
            assert delivered


# ---------------------------------------------------------------------------
# medium6: labels 列が壊れた行が dispatcher 全体をクラッシュさせないこと
# ---------------------------------------------------------------------------


class TestMalformedRow:
    def test_bad_labels_json_deadletters_without_crashing_cycle(self, conn):
        """labels のデコードは _deliver_row の try 内で行う。daemon ループは
        sqlite3.Error しか捕捉しないため、これを try 外に置くと不正な 1 行が
        json.JSONDecodeError を送出してループ全体をクラッシュさせ、同一 cycle 内の
        後続行（正常行も含む）が一切処理されなくなる。"""
        # bad を先に enqueue する（ORDER BY id で先に処理される）。旧実装ではここで
        # 例外が飛び、後続の good 行に到達する前に _dispatch_once 全体が失敗していた。
        bad_id = publish(conn, ref_type="log", ref_id="bad", labels=["a"])
        good_id = _enqueue(conn, ref_id="good")
        conn.commit()
        conn.execute("UPDATE relay_outbox SET labels = 'not-json{{' WHERE id = ?", (bad_id,))
        conn.commit()

        def handler(request):
            return httpx.Response(202, json={"publish_id": 1, "matched_subscriptions": 0})

        with _mock_client(handler) as client:
            delivered = _dispatch_once(
                conn, client, retry_backoff_base_seconds=0.01, retry_backoff_cap_seconds=300.0,
                transient_retry_deadline_seconds=86400.0, backoff_until={},
            )

        assert delivered == 1  # good 行はクラッシュせず配達された
        bad_row = conn.execute("SELECT * FROM relay_outbox WHERE id = ?", (bad_id,)).fetchone()
        assert bad_row["dead_at"] is not None
        assert bad_row["processed_at"] is None
        good_row = conn.execute("SELECT * FROM relay_outbox WHERE id = ?", (good_id,)).fetchone()
        assert good_row["processed_at"] is not None

    def test_dispatcher_daemon_survives_malformed_row(self, tmp_path):
        """run_dispatcher の daemon loop レベルで、壊れた行のせいで daemon スレッド
        自体が死んで全配達が止まらないことを確認する。"""
        with FakeRelay() as fake:
            db = str(tmp_path / "app.db")
            c = sqlite3.connect(db)
            create_outbox_table(c)
            bad_id = publish(c, ref_type="log", ref_id="bad", labels=["a"])
            c.commit()
            c.execute("UPDATE relay_outbox SET labels = 'not-json{{' WHERE id = ?", (bad_id,))
            c.commit()
            c.close()

            stop = threading.Event()
            t = threading.Thread(
                target=run_dispatcher,
                kwargs=dict(
                    db_path=db,
                    relay_base_url=fake.base_url,
                    agent_card_path=fake.fake_agent_card_path(),
                    poll_interval_seconds=0.03,
                    stop_event=stop,
                ),
                daemon=True,
            )
            t.start()
            time.sleep(0.3)
            assert t.is_alive(), "dispatcher daemon が壊れた行でクラッシュして終了している"

            # 壊れた行の後に enqueue した正常行が、その後も配達され続けることを確認する。
            c2 = sqlite3.connect(db)
            good_id = publish(c2, ref_type="log", ref_id="good-after", labels=["b"])
            c2.commit()
            c2.close()

            deadline = time.time() + 5
            delivered = False
            while time.time() < deadline:
                check = sqlite3.connect(db)
                v = check.execute(
                    "SELECT processed_at FROM relay_outbox WHERE id = ?", (good_id,)
                ).fetchone()[0]
                check.close()
                if v is not None:
                    delivered = True
                    break
                time.sleep(0.05)
            stop.set()
            t.join(timeout=3)
            assert delivered, (
                "壊れた行の後、正常行が配達され続けなかった（daemon がクラッシュした可能性）"
            )
            bad_row_check = sqlite3.connect(db)
            bad_dead_at = bad_row_check.execute(
                "SELECT dead_at FROM relay_outbox WHERE id = ?", (bad_id,)
            ).fetchone()[0]
            bad_row_check.close()
            assert bad_dead_at is not None
            assert delivered
