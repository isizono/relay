"""relay_sdk.outbox（publisher 側 publish / poll / mark_delivered / schema）の単体テスト。"""
from __future__ import annotations

import json
import sqlite3

import pytest

from relay_sdk.outbox import (
    CREATE_OUTBOX_TABLE,
    create_outbox_table,
    mark_delivered,
    poll,
    publish,
)


@pytest.fixture()
def conn():
    c = sqlite3.connect(":memory:")
    c.row_factory = sqlite3.Row
    create_outbox_table(c)
    yield c
    c.close()


class TestSchema:
    def test_create_is_idempotent(self, conn):
        # 2 回目の適用でも壊れない（CREATE TABLE IF NOT EXISTS）。
        create_outbox_table(conn)
        cols = {r[1] for r in conn.execute("PRAGMA table_info(relay_outbox)").fetchall()}
        assert cols == {
            "id",
            "ref_type",
            "ref_id",
            "labels",
            "title",
            "idempotency_key",
            "created_at",
            "processed_at",
            "retry_count",
            "last_error",
            "dead_at",
        }

    def test_ddl_constant_exported(self):
        assert "CREATE TABLE IF NOT EXISTS relay_outbox" in CREATE_OUTBOX_TABLE


class TestPublish:
    def test_inserts_row_in_caller_tx_without_commit(self, conn):
        row_id = publish(
            conn, ref_type="decision", ref_id=7, labels=["domain:cc-memory"], title="t"
        )
        # SDK は commit しない。別接続からは（未 commit なので）まだ見えない一方、
        # 同一接続では見える。
        row = conn.execute("SELECT * FROM relay_outbox WHERE id = ?", (row_id,)).fetchone()
        assert row["ref_type"] == "decision"
        assert row["ref_id"] == "7"  # TEXT 保存
        assert json.loads(row["labels"]) == ["domain:cc-memory"]
        assert row["title"] == "t"
        assert row["processed_at"] is None
        assert row["dead_at"] is None
        assert row["retry_count"] == 0

    def test_idempotency_key_equals_str_id(self, conn):
        row_id = publish(conn, ref_type="log", ref_id="x", labels=["a"])
        row = conn.execute("SELECT * FROM relay_outbox WHERE id = ?", (row_id,)).fetchone()
        assert row["idempotency_key"] == str(row_id)

    def test_created_at_iso8601_utc(self, conn):
        row_id = publish(conn, ref_type="log", ref_id="x", labels=["a"])
        row = conn.execute("SELECT created_at FROM relay_outbox WHERE id = ?", (row_id,)).fetchone()
        assert row["created_at"].endswith("Z") and "T" in row["created_at"]

    def test_empty_labels_raises(self, conn):
        with pytest.raises(ValueError):
            publish(conn, ref_type="log", ref_id="x", labels=[])

    def test_empty_ref_id_raises(self, conn):
        with pytest.raises(ValueError):
            publish(conn, ref_type="log", ref_id="", labels=["a"])

    def test_title_over_200_chars_raises(self, conn):
        with pytest.raises(ValueError):
            publish(conn, ref_type="log", ref_id="x", labels=["a"], title="z" * 201)

    def test_title_exactly_200_chars_ok(self, conn):
        row_id = publish(conn, ref_type="log", ref_id="x", labels=["a"], title="z" * 200)
        assert row_id > 0


class TestPollAndMarkDelivered:
    def test_poll_returns_pending_in_id_order(self, conn):
        publish(conn, ref_type="log", ref_id="1", labels=["a"])
        publish(conn, ref_type="log", ref_id="2", labels=["a"])
        conn.commit()
        rows = poll(conn)
        assert [r["ref_id"] for r in rows] == ["1", "2"]
        assert rows[0]["labels"] == ["a"]

    def test_poll_excludes_processed_and_dead(self, conn):
        id1 = publish(conn, ref_type="log", ref_id="1", labels=["a"])
        id2 = publish(conn, ref_type="log", ref_id="2", labels=["a"])
        conn.commit()
        mark_delivered(conn, [id1])
        conn.execute("UPDATE relay_outbox SET dead_at = '2020-01-01T00:00:00Z' WHERE id = ?", (id2,))
        conn.commit()
        assert poll(conn) == []

    def test_mark_delivered_sets_processed_at(self, conn):
        id1 = publish(conn, ref_type="log", ref_id="1", labels=["a"])
        conn.commit()
        mark_delivered(conn, [id1])
        row = conn.execute("SELECT processed_at FROM relay_outbox WHERE id = ?", (id1,)).fetchone()
        assert row["processed_at"] is not None

    def test_mark_delivered_empty_noop(self, conn):
        mark_delivered(conn, [])  # 例外にならない
