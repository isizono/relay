"""relay.app テストスイート。

Starlette アプリの骨格（健全性確認・AgentCard 公開・起動時 migration 適用）を検証する。
個別 endpoint（streams / subscriptions / delivery / observability）は後続タスクでの実装
につき、ここでは配線とライフサイクルのみを対象とする。
"""
import sqlite3

import pytest
from starlette.testclient import TestClient

from relay import credentials, db
from relay.app import create_app
from relay.config import Settings


@pytest.fixture()
def settings(tmp_path):
    return Settings(db_path=str(tmp_path / "test_relay.db"), auth_tokens={"tok-abc": "agent-a"})


@pytest.fixture()
def client(settings):
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


class TestHealthEndpoint:
    def test_get_root_returns_200(self, client):
        r = client.get("/")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    def test_health_does_not_require_auth(self, client):
        """health は認証不要（identity-authz.md の対象外、単なる疎通確認）。"""
        r = client.get("/")
        assert r.status_code == 200


class TestAgentCardEndpoint:
    def test_returns_200_with_a2a_media_type(self, client):
        r = client.get("/.well-known/agent-card.json")
        assert r.status_code == 200
        assert r.headers["content-type"].startswith("application/a2a+json")

    def test_does_not_require_auth(self, client):
        """AgentCard 取得自体には認証を要求しない（identity-authz.md §1.1.1）。"""
        r = client.get("/.well-known/agent-card.json")
        assert r.status_code == 200

    def test_body_matches_build_public_agent_card(self, client, settings):
        from relay.identity import build_public_agent_card

        r = client.get("/.well-known/agent-card.json")
        assert r.json() == build_public_agent_card(settings)


class TestStartupMigration:
    def test_db_schema_created_on_startup(self, settings):
        app = create_app(settings)
        with TestClient(app):
            pass  # lifespan startup が走った時点で migration 適用済みのはず

        conn = sqlite3.connect(settings.db_path)
        try:
            rows = conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            ).fetchall()
            names = {r[0] for r in rows}
        finally:
            conn.close()
        assert {"outbox", "dlq", "publish_log", "agent_cards"} <= names


class TestNotFound:
    def test_unregistered_route_returns_404(self, client):
        r = client.get("/nonexistent")
        assert r.status_code == 404


class TestCredentialBootstrap:
    """招待 redeem で発行された DB credential の起動時ロードを検証する。

    静的 env token（`Settings.auth_tokens` に直接渡す静的表）と DB 由来の動的
    credential の併存、および revoke が relay 再起動（= 新しい app インスタンス生成）を
    跨いで初めて反映されること（restart-bounded）を確認する。
    """

    def test_db_bearer_survives_restart(self, tmp_path):
        db_path = str(tmp_path / "boot.db")
        settings1 = Settings(
            db_path=db_path,
            server_log_path=str(tmp_path / "boot.jsonl"),
            dispatcher_lock_path=str(tmp_path / "boot.lock"),
        )
        app1 = create_app(settings1)
        with TestClient(app1) as c1:
            token = credentials.issue_invite(
                db_path,
                identity="cc-memory",
                invite_ttl_seconds=900,
                credential_ttl_seconds=None,
            )
            r = c1.post("/invitations/redeem", json={"invite_token": token})
            assert r.status_code == 200
            bearer_token = r.json()["bearer_token"]

        # 新しい Settings / app インスタンス（relay 再起動を模す）でも同じ DB から
        # 起動時ロードされ、bearer が認証を通ること。
        settings2 = Settings(
            db_path=db_path,
            server_log_path=str(tmp_path / "boot.jsonl"),
            dispatcher_lock_path=str(tmp_path / "boot.lock"),
        )
        app2 = create_app(settings2)
        with TestClient(app2) as c2:
            r2 = c2.post(
                "/streams",
                json={"name": "s1"},
                headers={"Authorization": f"Bearer {bearer_token}"},
            )
            assert r2.status_code == 201

    def test_static_and_db_tokens_coexist(self, tmp_path):
        db_path = str(tmp_path / "coexist.db")
        db.init_db(db_path)
        token = credentials.issue_invite(
            db_path, identity="db-agent", invite_ttl_seconds=900, credential_ttl_seconds=None
        )
        conn = db.get_connection(db_path)
        try:
            bearer_token, _, _ = credentials.redeem_invite(conn, token, credentials._now_iso())
        finally:
            conn.close()

        settings = Settings(
            db_path=db_path,
            server_log_path=str(tmp_path / "coexist.jsonl"),
            dispatcher_lock_path=str(tmp_path / "coexist.lock"),
            auth_tokens={"tok-static": "static-agent"},
        )
        app = create_app(settings)
        with TestClient(app) as c:
            r_static = c.post(
                "/streams", json={"name": "s1"}, headers={"Authorization": "Bearer tok-static"}
            )
            assert r_static.status_code == 201
            r_db = c.post(
                "/streams",
                json={"name": "s2"},
                headers={"Authorization": f"Bearer {bearer_token}"},
            )
            assert r_db.status_code == 201

    def test_revoke_then_reload_fails_auth(self, tmp_path):
        db_path = str(tmp_path / "revoke.db")
        db.init_db(db_path)
        token = credentials.issue_invite(
            db_path, identity="cc-memory", invite_ttl_seconds=900, credential_ttl_seconds=None
        )
        conn = db.get_connection(db_path)
        try:
            bearer_token, _, _ = credentials.redeem_invite(conn, token, credentials._now_iso())
        finally:
            conn.close()

        # revoke は DB を触るだけで、稼働中の in-memory auth_tokens には影響しない
        # （relay 再起動まで有効なまま残る、restart-bounded）。ここでは新しい app
        # インスタンス生成（再起動を模す）まで進めて反映を確認する。
        credentials.revoke(db_path, identity="cc-memory", now=credentials._now_iso())

        settings = Settings(
            db_path=db_path,
            server_log_path=str(tmp_path / "revoke.jsonl"),
            dispatcher_lock_path=str(tmp_path / "revoke.lock"),
        )
        app = create_app(settings)
        with TestClient(app) as c:
            r = c.post(
                "/streams",
                json={"name": "s1"},
                headers={"Authorization": f"Bearer {bearer_token}"},
            )
            assert r.status_code == 401


class TestOutboxUnavailable:
    """outbox（SQLite）書き込み失敗時の共通 exception handler（relay-v2-wire-api.md §8）。

    disk full / DB corrupt 等で `sqlite3.Error` が送出された場合、未処理のまま Starlette
    デフォルトの 500 に落ちず `503` を返すことを検証する（T6 退化モード検証の一部）。
    """

    def test_sqlite_error_returns_503(self, client, monkeypatch):
        headers = {"Authorization": "Bearer tok-abc"}
        r = client.post("/streams", json={"name": "s1"}, headers=headers)
        assert r.status_code == 201
        stream_id = r.json()["stream_id"]

        import sqlite3

        import relay.db as db_module

        def _boom(path):
            raise sqlite3.OperationalError("disk I/O error")

        monkeypatch.setattr(db_module, "get_connection", _boom)

        r = client.post(
            f"/streams/{stream_id}/messages", json={"body": "hello"}, headers=headers
        )
        assert r.status_code == 503
        assert r.json()["code"] == "OutboxUnavailableError"


class TestFederationLifespanState:
    """`require_federation_authn` が参照する per-peer nonce cache / rate limiter が
    lifespan で初期化されていることを検証する（未初期化だと federation サーフェスへの
    リクエストが AttributeError で 500 になる、federation v1 設計確定版 builder 申し送り）。
    """

    def test_federation_nonce_cache_and_rate_limiter_initialized_on_startup(self, tmp_path):
        from relay import federation_auth
        from relay.ratelimit import RateLimiter

        settings = Settings(
            db_path=str(tmp_path / "fed_lifespan.db"),
            server_log_path=str(tmp_path / "fed_lifespan.jsonl"),
            dispatcher_lock_path=str(tmp_path / "fed_lifespan.lock"),
        )
        app = create_app(settings)
        with TestClient(app):
            assert isinstance(app.state.federation_nonce_cache, federation_auth.NonceCache)
            assert isinstance(app.state.federation_request_rate_limiter, RateLimiter)
