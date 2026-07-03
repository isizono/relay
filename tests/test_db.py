"""relay.db テストスイート。

migration 適用の冪等性・table 構成・outbox の delivery target 制約を検証する。
"""
import sqlite3

import pytest

from relay import db


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "test_relay.db")


def _table_names(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    ).fetchall()
    return {r["name"] for r in rows}


class TestApplyMigrations:
    def test_creates_expected_tables(self, db_path):
        """disk 永続化 table は outbox / dlq / publish_log / agent_cards の 4 つのみ。

        streams / memberships / subscriptions は R1 原則（relay-v2-wire-api.md §0）により
        in-memory 実装とするため、SQLite には作らない。
        """
        db.apply_migrations(db_path)
        conn = db.get_connection(db_path)
        try:
            names = _table_names(conn)
        finally:
            conn.close()

        expected_data_tables = {"outbox", "dlq", "publish_log", "agent_cards"}
        assert expected_data_tables <= names
        assert "streams" not in names
        assert "memberships" not in names
        assert "subscriptions" not in names

    def test_is_idempotent(self, db_path):
        """2 回適用しても例外を出さず、同じ schema のまま。"""
        db.apply_migrations(db_path)
        db.apply_migrations(db_path)  # 2 回目は no-op のはず

        conn = db.get_connection(db_path)
        try:
            names = _table_names(conn)
        finally:
            conn.close()
        assert {"outbox", "dlq", "publish_log", "agent_cards"} <= names

    def test_init_db_is_apply_migrations_entrypoint(self, db_path):
        db.init_db(db_path)
        conn = db.get_connection(db_path)
        try:
            names = _table_names(conn)
        finally:
            conn.close()
        assert "outbox" in names


class TestGetConnection:
    def test_wal_mode_enabled(self, db_path):
        db.apply_migrations(db_path)
        conn = db.get_connection(db_path)
        try:
            mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
            assert mode == "wal"
        finally:
            conn.close()

    def test_row_factory_returns_rows(self, db_path):
        db.apply_migrations(db_path)
        conn = db.get_connection(db_path)
        try:
            conn.execute(
                "INSERT INTO publish_log (lane, stream_id, publisher_identity, enqueued_at)"
                " VALUES ('stream', 's1', 'agent-a', '2026-07-03T00:00:00Z')"
            )
            conn.commit()
            row = conn.execute("SELECT * FROM publish_log").fetchone()
            assert row["publisher_identity"] == "agent-a"
        finally:
            conn.close()


class TestOutboxSchema:
    """outbox テーブルが subscription レーン / stream レーン両方の delivery target を扱えること。"""

    @pytest.fixture()
    def conn(self, db_path):
        db.apply_migrations(db_path)
        conn = db.get_connection(db_path)
        yield conn
        conn.close()

    def test_subscription_lane_insert(self, conn):
        conn.execute(
            "INSERT INTO outbox (target_type, subscription_id, publish_id, payload, enqueued_at)"
            " VALUES ('subscription', 'sub-1', 1, X'00', '2026-07-03T00:00:00Z')"
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM outbox WHERE target_type='subscription'"
        ).fetchone()
        assert row["subscription_id"] == "sub-1"
        assert row["stream_id"] is None
        assert row["member_identity"] is None

    def test_stream_lane_insert(self, conn):
        conn.execute(
            "INSERT INTO outbox (target_type, stream_id, member_identity, publish_id, payload, enqueued_at)"
            " VALUES ('stream', 'stream-1', 'agent-a', 1, X'00', '2026-07-03T00:00:00Z')"
        )
        conn.commit()
        row = conn.execute("SELECT * FROM outbox WHERE target_type='stream'").fetchone()
        assert row["stream_id"] == "stream-1"
        assert row["member_identity"] == "agent-a"
        assert row["subscription_id"] is None

    def test_subscription_lane_requires_subscription_id(self, conn):
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO outbox (target_type, publish_id, payload, enqueued_at)"
                " VALUES ('subscription', 1, X'00', '2026-07-03T00:00:00Z')"
            )

    def test_stream_lane_requires_member_identity(self, conn):
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO outbox (target_type, stream_id, publish_id, payload, enqueued_at)"
                " VALUES ('stream', 'stream-1', 1, X'00', '2026-07-03T00:00:00Z')"
            )

    def test_duplicate_subscription_publish_id_rejected(self, conn):
        """(subscription_id, publish_id) の一意性（重複配達エントリ防止）。"""
        conn.execute(
            "INSERT INTO outbox (target_type, subscription_id, publish_id, payload, enqueued_at)"
            " VALUES ('subscription', 'sub-1', 1, X'00', '2026-07-03T00:00:00Z')"
        )
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO outbox (target_type, subscription_id, publish_id, payload, enqueued_at)"
                " VALUES ('subscription', 'sub-1', 1, X'01', '2026-07-03T00:00:01Z')"
            )

    def test_duplicate_stream_member_publish_id_rejected(self, conn):
        """(stream_id, member_identity, publish_id) の一意性。"""
        conn.execute(
            "INSERT INTO outbox (target_type, stream_id, member_identity, publish_id, payload, enqueued_at)"
            " VALUES ('stream', 'stream-1', 'agent-a', 1, X'00', '2026-07-03T00:00:00Z')"
        )
        conn.commit()
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO outbox (target_type, stream_id, member_identity, publish_id, payload, enqueued_at)"
                " VALUES ('stream', 'stream-1', 'agent-a', 1, X'01', '2026-07-03T00:00:01Z')"
            )

    def test_same_publish_id_different_members_allowed(self, conn):
        """同一 stream × 同一 publish_id でも member が違えば別エントリとして許可される。"""
        conn.execute(
            "INSERT INTO outbox (target_type, stream_id, member_identity, publish_id, payload, enqueued_at)"
            " VALUES ('stream', 'stream-1', 'agent-a', 1, X'00', '2026-07-03T00:00:00Z')"
        )
        conn.execute(
            "INSERT INTO outbox (target_type, stream_id, member_identity, publish_id, payload, enqueued_at)"
            " VALUES ('stream', 'stream-1', 'agent-b', 1, X'00', '2026-07-03T00:00:00Z')"
        )
        conn.commit()
        rows = conn.execute(
            "SELECT member_identity FROM outbox WHERE stream_id='stream-1' AND publish_id=1"
        ).fetchall()
        assert {r["member_identity"] for r in rows} == {"agent-a", "agent-b"}


class TestPublishLogSchema:
    def test_publish_id_autoincrements(self, db_path):
        db.apply_migrations(db_path)
        conn = db.get_connection(db_path)
        try:
            cur1 = conn.execute(
                "INSERT INTO publish_log (lane, publisher_identity, enqueued_at)"
                " VALUES ('subscription', 'agent-a', '2026-07-03T00:00:00Z')"
            )
            cur2 = conn.execute(
                "INSERT INTO publish_log (lane, publisher_identity, enqueued_at)"
                " VALUES ('subscription', 'agent-a', '2026-07-03T00:00:01Z')"
            )
            conn.commit()
            assert cur2.lastrowid == cur1.lastrowid + 1
        finally:
            conn.close()

    def test_lane_check_constraint(self, db_path):
        db.apply_migrations(db_path)
        conn = db.get_connection(db_path)
        try:
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO publish_log (lane, publisher_identity, enqueued_at)"
                    " VALUES ('bogus', 'agent-a', '2026-07-03T00:00:00Z')"
                )
        finally:
            conn.close()


class TestDlqSchema:
    def test_insert_and_sweep_query(self, db_path):
        db.apply_migrations(db_path)
        conn = db.get_connection(db_path)
        try:
            conn.execute(
                "INSERT INTO dlq (target_type, subscription_id, publish_id, payload, error_code, dead_at)"
                " VALUES ('subscription', 'sub-1', 1, X'00', 'retain_expired', '2026-07-03T00:00:00Z')"
            )
            conn.commit()
            rows = conn.execute(
                "SELECT * FROM dlq WHERE dead_at < '2026-07-10T00:00:00Z'"
            ).fetchall()
            assert len(rows) == 1
            assert rows[0]["error_code"] == "retain_expired"
        finally:
            conn.close()


class TestAgentCardsSchema:
    def test_upsert_by_identity(self, db_path):
        db.apply_migrations(db_path)
        conn = db.get_connection(db_path)
        try:
            conn.execute(
                "INSERT INTO agent_cards (identity, card_json, fetched_at)"
                " VALUES ('agent-a', '{}', '2026-07-03T00:00:00Z')"
            )
            conn.commit()
            with pytest.raises(sqlite3.IntegrityError):
                conn.execute(
                    "INSERT INTO agent_cards (identity, card_json, fetched_at)"
                    " VALUES ('agent-a', '{}', '2026-07-03T00:00:01Z')"
                )
        finally:
            conn.close()
