"""relay.invitations テストスイート。

`POST /invitations/redeem`（無認証）の正常系・一回性・失効・不正 body・rate limit・
存在秘匿（未知/失効/既 redeem の一律 404）を検証する。招待発行は
`relay.credentials.issue_invite` を直接呼んでテスト用招待 token を作る（HTTP 発行
endpoint は存在しない、D1）。
"""
from __future__ import annotations

import pytest
from starlette.testclient import TestClient

from relay import credentials
from relay.app import create_app
from relay.config import Settings


@pytest.fixture()
def settings(tmp_path):
    return Settings(
        db_path=str(tmp_path / "invitations.db"),
        server_log_path=str(tmp_path / "invitations.jsonl"),
        dispatcher_lock_path=str(tmp_path / "invitations.lock"),
    )


@pytest.fixture()
def client(settings):
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def _new_invite(settings, *, ttl_seconds=900, credential_ttl_seconds=None, identity="cc-memory"):
    return credentials.issue_invite(
        settings.db_path,
        identity=identity,
        invite_ttl_seconds=ttl_seconds,
        credential_ttl_seconds=credential_ttl_seconds,
    )


class TestRedeemSuccess:
    def test_returns_bearer_and_identity(self, client, settings):
        token = _new_invite(settings)
        r = client.post("/invitations/redeem", json={"invite_token": token})
        assert r.status_code == 200
        body = r.json()
        assert body["bearer_token"].startswith("bt_")
        assert body["identity"] == "cc-memory"
        assert body["expires_at"] is None

    def test_bearer_authenticates_protected_endpoint(self, client, settings):
        token = _new_invite(settings)
        r = client.post("/invitations/redeem", json={"invite_token": token})
        bearer_token = r.json()["bearer_token"]

        r2 = client.post(
            "/streams",
            json={"name": "s1"},
            headers={"Authorization": f"Bearer {bearer_token}"},
        )
        assert r2.status_code == 201

    def test_credential_ttl_seconds_reflected_in_response(self, client, settings):
        token = _new_invite(settings, credential_ttl_seconds=3600)
        r = client.post("/invitations/redeem", json={"invite_token": token})
        assert r.status_code == 200
        assert r.json()["expires_at"] is not None


class TestRedeemOneTimeUse:
    def test_second_redeem_returns_404(self, client, settings):
        token = _new_invite(settings)
        first = client.post("/invitations/redeem", json={"invite_token": token})
        assert first.status_code == 200
        second = client.post("/invitations/redeem", json={"invite_token": token})
        assert second.status_code == 404
        assert second.json()["code"] == "InviteNotFoundError"


class TestRedeemExpired:
    def test_expired_invite_returns_404(self, client, settings):
        token = _new_invite(settings, ttl_seconds=-10)
        r = client.post("/invitations/redeem", json={"invite_token": token})
        assert r.status_code == 404
        assert r.json()["code"] == "InviteNotFoundError"


class TestRedeemUnknownToken:
    def test_unknown_token_returns_404(self, client):
        r = client.post("/invitations/redeem", json={"invite_token": "it_does-not-exist"})
        assert r.status_code == 404
        assert r.json()["code"] == "InviteNotFoundError"


class TestUniform404:
    def test_unknown_expired_and_already_redeemed_share_response(self, client, settings):
        """未知 / 失効 / 既 redeem を一律 404 として区別しない（存在秘匿）。"""
        used_token = _new_invite(settings)
        client.post("/invitations/redeem", json={"invite_token": used_token})
        expired_token = _new_invite(settings, ttl_seconds=-10)

        r_unknown = client.post("/invitations/redeem", json={"invite_token": "it_nope"})
        r_expired = client.post("/invitations/redeem", json={"invite_token": expired_token})
        r_reused = client.post("/invitations/redeem", json={"invite_token": used_token})

        for r in (r_unknown, r_expired, r_reused):
            assert r.status_code == 404
            assert r.json() == r_unknown.json()


class TestRedeemMalformedBody:
    def test_non_json_body_returns_400(self, client):
        r = client.post(
            "/invitations/redeem",
            content=b"not json",
            headers={"Content-Type": "application/json"},
        )
        assert r.status_code == 400
        assert r.json()["code"] == "InvalidRequestError"

    def test_missing_invite_token_returns_400(self, client):
        r = client.post("/invitations/redeem", json={})
        assert r.status_code == 400
        assert r.json()["code"] == "InvalidRequestError"

    def test_non_string_invite_token_returns_400(self, client):
        r = client.post("/invitations/redeem", json={"invite_token": 123})
        assert r.status_code == 400

    def test_non_object_body_returns_400(self, client):
        r = client.post("/invitations/redeem", json=["nope"])
        assert r.status_code == 400

    def test_oversized_body_returns_400(self, client):
        oversized = b'{"invite_token": "' + b"a" * 5000 + b'"}'
        r = client.post(
            "/invitations/redeem",
            content=oversized,
            headers={"Content-Type": "application/json"},
        )
        assert r.status_code == 400
        assert r.json()["code"] == "InvalidRequestError"


class TestRedeemRateLimit:
    @pytest.fixture()
    def limited_client(self, tmp_path):
        # app.py の redeem_rate_limiter は 5 req/s 固定。連続 6 回叩いて 6 回目が 429 に
        # なることを検証する。
        limited_settings = Settings(
            db_path=str(tmp_path / "rl.db"),
            server_log_path=str(tmp_path / "rl.jsonl"),
            dispatcher_lock_path=str(tmp_path / "rl.lock"),
        )
        app = create_app(limited_settings)
        with TestClient(app) as c:
            yield c

    def test_over_limit_returns_429_with_retry_after(self, limited_client):
        for _ in range(5):
            r = limited_client.post("/invitations/redeem", json={"invite_token": "it_nope"})
            assert r.status_code == 404
        r = limited_client.post("/invitations/redeem", json={"invite_token": "it_nope"})
        assert r.status_code == 429
        assert r.json()["code"] == "RateLimitExceededError"
        assert "Retry-After" in r.headers
