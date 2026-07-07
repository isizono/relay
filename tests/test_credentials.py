"""relay.credentials テストスイート。

招待 token の発行・redeem（atomic 一回性）・起動時ロード用の現行 credential 一覧・
revoke・gc を検証する。redeem の並行性は同一ファイル DB への複数コネクションで検証する
（tests/test_idempotency.py の並行テストパターンを踏襲）。
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone

import pytest

from relay import credentials, db


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _now_offset(seconds: float) -> str:
    return _iso(datetime.now(timezone.utc) + timedelta(seconds=seconds))


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "credentials.db")
    db.init_db(path)
    return path


def _issue(db_path, *, identity="cc-memory", invite_ttl_seconds=900, credential_ttl_seconds=None):
    return credentials.issue_invite(
        db_path,
        identity=identity,
        invite_ttl_seconds=invite_ttl_seconds,
        credential_ttl_seconds=credential_ttl_seconds,
    )


class TestIssueInvite:
    def test_returns_it_prefixed_token(self, db_path):
        token = _issue(db_path)
        assert token.startswith("it_")

    def test_inserts_one_pending_row(self, db_path):
        _issue(db_path, identity="cc-memory")
        conn = db.get_connection(db_path)
        try:
            rows = conn.execute("SELECT * FROM invitations").fetchall()
        finally:
            conn.close()
        assert len(rows) == 1
        assert rows[0]["identity"] == "cc-memory"
        assert rows[0]["redeemed_at"] is None


class TestRedeemInvite:
    def test_success_returns_bearer_identity_and_expires_at(self, db_path):
        token = _issue(db_path)
        conn = db.get_connection(db_path)
        try:
            result = credentials.redeem_invite(conn, token, credentials._now_iso())
        finally:
            conn.close()
        assert result is not None
        bearer_token, identity, expires_at = result
        assert bearer_token.startswith("bt_")
        assert identity == "cc-memory"
        assert expires_at is None

    def test_success_inserts_credential_row_linked_to_invitation(self, db_path):
        token = _issue(db_path)
        conn = db.get_connection(db_path)
        try:
            bearer_token, _, _ = credentials.redeem_invite(conn, token, credentials._now_iso())
            cred_row = conn.execute(
                "SELECT * FROM credentials WHERE token = ?", (bearer_token,)
            ).fetchone()
            invitation_row = conn.execute(
                "SELECT redeemed_credential_id FROM invitations WHERE token = ?", (token,)
            ).fetchone()
        finally:
            conn.close()
        assert cred_row is not None
        assert cred_row["identity"] == "cc-memory"
        assert invitation_row["redeemed_credential_id"] == cred_row["id"]

    def test_double_redeem_returns_none(self, db_path):
        token = _issue(db_path)
        conn = db.get_connection(db_path)
        try:
            first = credentials.redeem_invite(conn, token, credentials._now_iso())
            second = credentials.redeem_invite(conn, token, credentials._now_iso())
        finally:
            conn.close()
        assert first is not None
        assert second is None

    def test_expired_invite_returns_none(self, db_path):
        token = _issue(db_path, invite_ttl_seconds=-10)
        conn = db.get_connection(db_path)
        try:
            result = credentials.redeem_invite(conn, token, credentials._now_iso())
        finally:
            conn.close()
        assert result is None

    def test_unknown_token_returns_none(self, db_path):
        conn = db.get_connection(db_path)
        try:
            result = credentials.redeem_invite(conn, "it_does-not-exist", credentials._now_iso())
        finally:
            conn.close()
        assert result is None

    def test_credential_ttl_seconds_produces_expires_at(self, db_path):
        token = _issue(db_path, credential_ttl_seconds=3600)
        conn = db.get_connection(db_path)
        try:
            _, _, expires_at = credentials.redeem_invite(conn, token, credentials._now_iso())
        finally:
            conn.close()
        assert expires_at is not None
        assert expires_at > credentials._now_iso()

    def test_concurrent_redeem_single_winner(self, db_path):
        """同一 invite token を複数スレッドが同時に redeem しても勝者は1つだけになる。

        各スレッドは自前の sqlite コネクションを持つ（同一プロセス内の複数コネクションが
        同一 DB ファイルへ書き込む。WAL + busy_timeout が競合を吸収する）。判定（atomic
        UPDATE）と bearer 発行が別区間だと複数スレッドが判定をすり抜けて二重発行しうる点が
        ここでの保証対象。
        """
        token = _issue(db_path)
        n_threads = 8
        barrier = threading.Barrier(n_threads)
        results: list = []
        results_lock = threading.Lock()

        def attempt():
            conn = db.get_connection(db_path)
            try:
                barrier.wait()
                result = credentials.redeem_invite(conn, token, credentials._now_iso())
            finally:
                conn.close()
            with results_lock:
                results.append(result)

        threads = [threading.Thread(target=attempt) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        winners = [r for r in results if r is not None]
        losers = [r for r in results if r is None]
        assert len(winners) == 1
        assert len(losers) == n_threads - 1


class TestWasAlreadyRedeemed:
    def test_false_for_unredeemed_invite(self, db_path):
        token = _issue(db_path)
        conn = db.get_connection(db_path)
        try:
            assert credentials.was_already_redeemed(conn, token) is False
        finally:
            conn.close()

    def test_true_after_redeem(self, db_path):
        token = _issue(db_path)
        conn = db.get_connection(db_path)
        try:
            credentials.redeem_invite(conn, token, credentials._now_iso())
            assert credentials.was_already_redeemed(conn, token) is True
        finally:
            conn.close()

    def test_false_for_expired_but_unredeemed_invite(self, db_path):
        token = _issue(db_path, invite_ttl_seconds=-10)
        conn = db.get_connection(db_path)
        try:
            assert credentials.was_already_redeemed(conn, token) is False
        finally:
            conn.close()

    def test_false_for_unknown_token(self, db_path):
        conn = db.get_connection(db_path)
        try:
            assert credentials.was_already_redeemed(conn, "it_nope") is False
        finally:
            conn.close()


class TestLoadBearers:
    def test_returns_active_credentials(self, db_path):
        token = _issue(db_path)
        conn = db.get_connection(db_path)
        try:
            bearer_token, identity, _ = credentials.redeem_invite(
                conn, token, credentials._now_iso()
            )
        finally:
            conn.close()
        bearers = credentials.load_bearers(db_path, credentials._now_iso())
        assert bearers[bearer_token] == identity

    def test_excludes_revoked(self, db_path):
        token = _issue(db_path)
        conn = db.get_connection(db_path)
        try:
            bearer_token, _, _ = credentials.redeem_invite(conn, token, credentials._now_iso())
        finally:
            conn.close()
        credentials.revoke(db_path, identity="cc-memory", now=credentials._now_iso())
        bearers = credentials.load_bearers(db_path, credentials._now_iso())
        assert bearer_token not in bearers

    def test_excludes_expired(self, db_path):
        token = _issue(db_path, credential_ttl_seconds=-10)
        conn = db.get_connection(db_path)
        try:
            bearer_token, _, _ = credentials.redeem_invite(conn, token, credentials._now_iso())
        finally:
            conn.close()
        bearers = credentials.load_bearers(db_path, credentials._now_iso())
        assert bearer_token not in bearers

    def test_includes_credential_with_no_expiry(self, db_path):
        token = _issue(db_path, credential_ttl_seconds=None)
        conn = db.get_connection(db_path)
        try:
            bearer_token, _, _ = credentials.redeem_invite(conn, token, credentials._now_iso())
        finally:
            conn.close()
        bearers = credentials.load_bearers(db_path, credentials._now_iso())
        assert bearer_token in bearers


class TestRevoke:
    def test_by_identity_sets_revoked_at(self, db_path):
        token = _issue(db_path)
        conn = db.get_connection(db_path)
        try:
            bearer_token, _, _ = credentials.redeem_invite(conn, token, credentials._now_iso())
        finally:
            conn.close()
        count = credentials.revoke(db_path, identity="cc-memory", now=credentials._now_iso())
        assert count == 1
        conn = db.get_connection(db_path)
        try:
            row = conn.execute(
                "SELECT revoked_at FROM credentials WHERE token = ?", (bearer_token,)
            ).fetchone()
        finally:
            conn.close()
        assert row["revoked_at"] is not None

    def test_by_credential_id(self, db_path):
        token = _issue(db_path)
        conn = db.get_connection(db_path)
        try:
            credentials.redeem_invite(conn, token, credentials._now_iso())
            cred_id = conn.execute("SELECT id FROM credentials").fetchone()["id"]
        finally:
            conn.close()
        count = credentials.revoke(db_path, credential_id=cred_id, now=credentials._now_iso())
        assert count == 1

    def test_requires_exactly_one_selector(self, db_path):
        with pytest.raises(ValueError):
            credentials.revoke(db_path, now=credentials._now_iso())
        with pytest.raises(ValueError):
            credentials.revoke(
                db_path, identity="a", credential_id=1, now=credentials._now_iso()
            )

    def test_no_matching_row_returns_zero(self, db_path):
        assert credentials.revoke(db_path, identity="nobody", now=credentials._now_iso()) == 0

    def test_already_revoked_is_not_recounted(self, db_path):
        token = _issue(db_path)
        conn = db.get_connection(db_path)
        try:
            credentials.redeem_invite(conn, token, credentials._now_iso())
        finally:
            conn.close()
        first = credentials.revoke(db_path, identity="cc-memory", now=credentials._now_iso())
        second = credentials.revoke(db_path, identity="cc-memory", now=credentials._now_iso())
        assert first == 1
        assert second == 0


class TestGc:
    def test_removes_old_revoked_credential(self, db_path):
        token = _issue(db_path)
        conn = db.get_connection(db_path)
        try:
            bearer_token, _, _ = credentials.redeem_invite(conn, token, credentials._now_iso())
        finally:
            conn.close()
        credentials.revoke(db_path, identity="cc-memory", now=_now_offset(-1000))
        credentials.gc(db_path, credentials._now_iso(), retention_seconds=1)
        conn = db.get_connection(db_path)
        try:
            row = conn.execute(
                "SELECT * FROM credentials WHERE token = ?", (bearer_token,)
            ).fetchone()
        finally:
            conn.close()
        assert row is None

    def test_keeps_recent_revoked_credential(self, db_path):
        token = _issue(db_path)
        conn = db.get_connection(db_path)
        try:
            bearer_token, _, _ = credentials.redeem_invite(conn, token, credentials._now_iso())
        finally:
            conn.close()
        credentials.revoke(db_path, identity="cc-memory", now=credentials._now_iso())
        credentials.gc(db_path, credentials._now_iso(), retention_seconds=604800)
        conn = db.get_connection(db_path)
        try:
            row = conn.execute(
                "SELECT * FROM credentials WHERE token = ?", (bearer_token,)
            ).fetchone()
        finally:
            conn.close()
        assert row is not None

    def test_removes_old_unredeemed_expired_invitation(self, db_path):
        token = _issue(db_path, invite_ttl_seconds=-2000)
        credentials.gc(db_path, credentials._now_iso(), retention_seconds=1)
        conn = db.get_connection(db_path)
        try:
            row = conn.execute(
                "SELECT * FROM invitations WHERE token = ?", (token,)
            ).fetchone()
        finally:
            conn.close()
        assert row is None
