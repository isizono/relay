"""relay.app テストスイート。

Starlette アプリの骨格（健全性確認・AgentCard 公開・起動時 migration 適用）を検証する。
個別 endpoint（streams / subscriptions / delivery / observability）は後続タスクでの実装
につき、ここでは配線とライフサイクルのみを対象とする。
"""
import sqlite3

import pytest
from starlette.testclient import TestClient

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
