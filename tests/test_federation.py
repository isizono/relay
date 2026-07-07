"""relay.federation テストスイート（`POST /federation/peers/redeem`）。

招待ベース鍵ピン留めの正常系・token 一回性・自己署名検証・チャネルバインディング
（a_fp 照合）・rate limit・private locator の既定拒否と opt-in 許可・federation 機能
無効時の fail-closed を検証する。
"""
from __future__ import annotations

import time

import pytest
from joserfc.jwk import ECKey
from starlette.testclient import TestClient

from relay import federation_peers
from relay.app import create_app
from relay.config import Settings


def _generate_keypair() -> dict:
    key = ECKey.generate_key("P-256", private=True)
    return {
        "private_pem": key.as_pem(private=True).decode("ascii"),
        "public_jwk": key.as_dict(private=False),
    }


@pytest.fixture()
def keypair_a():
    return _generate_keypair()


@pytest.fixture()
def keypair_b():
    return _generate_keypair()


@pytest.fixture()
def settings(tmp_path, keypair_a):
    return Settings(
        db_path=str(tmp_path / "federation.db"),
        server_log_path=str(tmp_path / "federation.jsonl"),
        dispatcher_lock_path=str(tmp_path / "federation.lock"),
        jws_private_key_pem=keypair_a["private_pem"],
        federation_base_url="https://relay-a.example",
    )


@pytest.fixture()
def client(settings):
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def _issue_invite(settings, *, handle="bob", ttl_seconds=900):
    return federation_peers.issue_peer_invite(
        settings.db_path, handle=handle, invite_ttl_seconds=ttl_seconds
    )


def _own_fingerprint(settings) -> str:
    return federation_peers.compute_fingerprint(
        federation_peers.public_jwk_from_pem(settings.jws_private_key_pem)
    )


def _build_redeem_body(
    *,
    token: str,
    keypair_b: dict,
    a_fp: str,
    locator: str = "https://8.8.8.8",
    ts: int | None = None,
) -> dict:
    ts = ts if ts is not None else int(time.time())
    sig_payload = {"typ": "relay-fed-redeem", "token": token, "ts": ts, "a_fp": a_fp}
    sig = federation_peers.sign_detached(
        sig_payload, private_key_pem=keypair_b["private_pem"]
    )
    return {
        "invite_token": token,
        "ts": ts,
        "a_fp": a_fp,
        "card": {"key": keypair_b["public_jwk"], "locator": locator},
        "sig": sig,
    }


class TestRedeemSuccess:
    def test_pins_peer_and_returns_signed_response(self, client, settings, keypair_b):
        token = _issue_invite(settings)
        a_fp = _own_fingerprint(settings)
        body = _build_redeem_body(token=token, keypair_b=keypair_b, a_fp=a_fp)

        r = client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 200
        resp = r.json()
        assert resp["handle"] == "bob"
        assert resp["card"]["locator"] == settings.federation_base_url
        assert resp["card"]["key"] == federation_peers.public_jwk_from_pem(
            settings.jws_private_key_pem
        )

        # 応答署名が A 自身の公開鍵で検証できること。
        verify_payload = {
            "typ": "relay-fed-redeem-resp",
            "handle": resp["handle"],
            "card": resp["card"],
        }
        assert federation_peers.verify_detached(
            verify_payload, resp["sig"], public_key=resp["card"]["key"]
        )

        # peer が pin されていること。
        peer_fp = federation_peers.compute_fingerprint(keypair_b["public_jwk"])
        peer = federation_peers.get_peer_by_fingerprint(settings.db_path, peer_fp)
        assert peer is not None
        assert peer["handle"] == "bob"
        assert peer["locator"] == "https://8.8.8.8"


class TestRedeemOneTimeUse:
    def test_second_redeem_returns_404(self, client, settings, keypair_b):
        token = _issue_invite(settings)
        a_fp = _own_fingerprint(settings)
        body = _build_redeem_body(token=token, keypair_b=keypair_b, a_fp=a_fp)

        first = client.post("/federation/peers/redeem", json=body)
        assert first.status_code == 200

        other_keypair = _generate_keypair()
        second_body = _build_redeem_body(token=token, keypair_b=other_keypair, a_fp=a_fp)
        second = client.post("/federation/peers/redeem", json=second_body)
        assert second.status_code == 404
        assert second.json()["code"] == "PeerInviteNotFoundError"


class TestRedeemExpiredAndUnknown:
    def test_expired_invite_returns_404(self, client, settings, keypair_b):
        token = _issue_invite(settings, ttl_seconds=-10)
        a_fp = _own_fingerprint(settings)
        body = _build_redeem_body(token=token, keypair_b=keypair_b, a_fp=a_fp)
        r = client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 404
        assert r.json()["code"] == "PeerInviteNotFoundError"

    def test_unknown_token_returns_404(self, client, settings, keypair_b):
        a_fp = _own_fingerprint(settings)
        body = _build_redeem_body(token="pi_does-not-exist", keypair_b=keypair_b, a_fp=a_fp)
        r = client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 404
        assert r.json()["code"] == "PeerInviteNotFoundError"

    def test_unknown_expired_and_already_redeemed_share_response(
        self, client, settings, keypair_b
    ):
        a_fp = _own_fingerprint(settings)
        used_token = _issue_invite(settings, handle="carol")
        used_body = _build_redeem_body(token=used_token, keypair_b=keypair_b, a_fp=a_fp)
        client.post("/federation/peers/redeem", json=used_body)

        expired_token = _issue_invite(settings, handle="dave", ttl_seconds=-10)
        expired_body = _build_redeem_body(
            token=expired_token, keypair_b=_generate_keypair(), a_fp=a_fp
        )

        r_unknown = client.post(
            "/federation/peers/redeem",
            json=_build_redeem_body(
                token="pi_nope", keypair_b=_generate_keypair(), a_fp=a_fp
            ),
        )
        r_expired = client.post("/federation/peers/redeem", json=expired_body)
        r_reused = client.post(
            "/federation/peers/redeem",
            json=_build_redeem_body(
                token=used_token, keypair_b=_generate_keypair(), a_fp=a_fp
            ),
        )

        for r in (r_unknown, r_expired, r_reused):
            assert r.status_code == 404
            assert r.json() == r_unknown.json()


class TestRedeemSignatureVerification:
    def test_tampered_signed_field_is_rejected(self, client, settings, keypair_b):
        token = _issue_invite(settings)
        a_fp = _own_fingerprint(settings)
        body = _build_redeem_body(token=token, keypair_b=keypair_b, a_fp=a_fp)
        body["ts"] = body["ts"] + 1  # 署名対象フィールドの改竄
        r = client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 401
        assert r.json()["code"] == "FederationSignatureInvalidError"

    def test_signature_by_unrelated_key_is_rejected(self, client, settings, keypair_b):
        token = _issue_invite(settings)
        a_fp = _own_fingerprint(settings)
        wrong_keypair = _generate_keypair()
        ts = int(time.time())
        sig_payload = {"typ": "relay-fed-redeem", "token": token, "ts": ts, "a_fp": a_fp}
        # 署名は無関係の鍵で行うが、card.key には keypair_b の公開鍵を載せる
        # （鍵所持証明の失敗パターン）。
        sig = federation_peers.sign_detached(
            sig_payload, private_key_pem=wrong_keypair["private_pem"]
        )
        body = {
            "invite_token": token,
            "ts": ts,
            "a_fp": a_fp,
            "card": {"key": keypair_b["public_jwk"], "locator": "https://8.8.8.8"},
            "sig": sig,
        }
        r = client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 401
        assert r.json()["code"] == "FederationSignatureInvalidError"

    def test_wrong_a_fp_is_rejected_and_not_pinned(self, client, settings, keypair_b):
        token = _issue_invite(settings)
        body = _build_redeem_body(token=token, keypair_b=keypair_b, a_fp="wrong-fingerprint")
        r = client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 401
        assert r.json()["code"] == "FederationSignatureInvalidError"

        peer_fp = federation_peers.compute_fingerprint(keypair_b["public_jwk"])
        assert federation_peers.get_peer_by_fingerprint(settings.db_path, peer_fp) is None


class TestRedeemLocatorGuard:
    def test_private_locator_rejected_by_default(self, client, settings, keypair_b):
        token = _issue_invite(settings)
        a_fp = _own_fingerprint(settings)
        body = _build_redeem_body(
            token=token, keypair_b=keypair_b, a_fp=a_fp, locator="https://127.0.0.1:9999"
        )
        r = client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 400

    def test_private_locator_allowed_when_opted_in(self, tmp_path, keypair_a, keypair_b):
        settings = Settings(
            db_path=str(tmp_path / "f2.db"),
            server_log_path=str(tmp_path / "f2.jsonl"),
            dispatcher_lock_path=str(tmp_path / "f2.lock"),
            jws_private_key_pem=keypair_a["private_pem"],
            federation_base_url="https://relay-a.example",
            federation_allow_private_locators=True,
        )
        app = create_app(settings)
        with TestClient(app) as c:
            token = _issue_invite(settings)
            a_fp = _own_fingerprint(settings)
            body = _build_redeem_body(
                token=token,
                keypair_b=keypair_b,
                a_fp=a_fp,
                locator="https://127.0.0.1:9999",
            )
            r = c.post("/federation/peers/redeem", json=body)
        assert r.status_code == 200


class TestRedeemMalformedBody:
    def test_missing_fields_returns_400(self, client):
        r = client.post("/federation/peers/redeem", json={})
        assert r.status_code == 400
        assert r.json()["code"] == "InvalidRequestError"

    def test_non_object_body_returns_400(self, client):
        r = client.post("/federation/peers/redeem", json=["nope"])
        assert r.status_code == 400

    def test_non_json_body_returns_400(self, client):
        r = client.post(
            "/federation/peers/redeem",
            content=b"not json",
            headers={"Content-Type": "application/json"},
        )
        assert r.status_code == 400

    def test_oversized_body_returns_400(self, client):
        oversized = b'{"invite_token": "' + b"a" * 5000 + b'"}'
        r = client.post(
            "/federation/peers/redeem",
            content=oversized,
            headers={"Content-Type": "application/json"},
        )
        assert r.status_code == 400


class TestRedeemRateLimit:
    @pytest.fixture()
    def limited_client(self, tmp_path, keypair_a):
        limited_settings = Settings(
            db_path=str(tmp_path / "rl.db"),
            server_log_path=str(tmp_path / "rl.jsonl"),
            dispatcher_lock_path=str(tmp_path / "rl.lock"),
            jws_private_key_pem=keypair_a["private_pem"],
            federation_base_url="https://relay-a.example",
        )
        app = create_app(limited_settings)
        with TestClient(app) as c:
            yield c

    def test_over_limit_returns_429(self, limited_client, keypair_b):
        body = _build_redeem_body(token="pi_nope", keypair_b=keypair_b, a_fp="x")
        for _ in range(5):
            r = limited_client.post("/federation/peers/redeem", json=body)
            assert r.status_code == 404
        r = limited_client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 429
        assert r.json()["code"] == "RateLimitExceededError"
        assert "Retry-After" in r.headers


class TestFederationDisabled:
    def test_redeem_returns_503_without_federation_key(self, tmp_path, keypair_b):
        settings = Settings(
            db_path=str(tmp_path / "nokey.db"),
            server_log_path=str(tmp_path / "nokey.jsonl"),
            dispatcher_lock_path=str(tmp_path / "nokey.lock"),
        )
        app = create_app(settings)
        with TestClient(app) as c:
            body = _build_redeem_body(token="pi_nope", keypair_b=keypair_b, a_fp="x")
            r = c.post("/federation/peers/redeem", json=body)
        assert r.status_code == 503
        assert r.json()["code"] == "FederationDisabledError"

    def test_redeem_returns_503_without_base_url(self, tmp_path, keypair_a, keypair_b):
        """鍵はあっても RELAY_BASE_URL 未設定なら無効化する（応答 locator が null になるのを防ぐ）。"""
        settings = Settings(
            db_path=str(tmp_path / "nourl.db"),
            server_log_path=str(tmp_path / "nourl.jsonl"),
            dispatcher_lock_path=str(tmp_path / "nourl.lock"),
            jws_private_key_pem=keypair_a["private_pem"],
        )
        app = create_app(settings)
        with TestClient(app) as c:
            body = _build_redeem_body(token="pi_nope", keypair_b=keypair_b, a_fp="x")
            r = c.post("/federation/peers/redeem", json=body)
        assert r.status_code == 503
        assert r.json()["code"] == "FederationDisabledError"
