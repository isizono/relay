"""relay_sdk.http.auth（Bearer / JWS）と relay_sdk.client.reconcile の単体テスト。

JWS（§4.3）は pyjwt[crypto]（mcp の推移的依存として利用可能）+ ES256 を使う。
AgentCard 検証は relay 本体（relay.identity.sign_agent_card）が出力する
JCS(detached) 形式との相互運用を確認する。
"""
from __future__ import annotations

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ec

from relay_sdk.client import reconcile
from relay_sdk.http.auth import (
    build_auth_headers,
    make_client,
    resolve_bearer_token,
    sign_jws,
    verify_relay_agent_card,
)


@pytest.fixture()
def ec_keys():
    priv = ec.generate_private_key(ec.SECP256R1())
    priv_pem = priv.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    ).decode()
    pub_pem = (
        priv.public_key()
        .public_bytes(
            serialization.Encoding.PEM,
            serialization.PublicFormat.SubjectPublicKeyInfo,
        )
        .decode()
    )
    return priv_pem, pub_pem


class TestBearerResolution:
    def test_explicit_token_wins(self, monkeypatch):
        monkeypatch.setenv("RELAY_BEARER_TOKEN", "from-env")
        assert resolve_bearer_token(bearer_token="explicit") == "explicit"

    def test_env_token_used_when_no_explicit(self, monkeypatch):
        monkeypatch.setenv("RELAY_BEARER_TOKEN", "from-env")
        assert resolve_bearer_token() == "from-env"

    def test_none_when_no_source(self, monkeypatch):
        monkeypatch.delenv("RELAY_BEARER_TOKEN", raising=False)
        assert resolve_bearer_token() is None

    def test_build_auth_headers(self):
        assert build_auth_headers("t") == {"Authorization": "Bearer t"}
        assert build_auth_headers(None) == {}

    def test_make_client_attaches_header(self, monkeypatch):
        monkeypatch.setenv("RELAY_BEARER_TOKEN", "tok-9")
        client = make_client("http://relay.test")
        try:
            assert client.headers["Authorization"] == "Bearer tok-9"
        finally:
            client.close()


class TestJws:
    def test_sign_and_decode_roundtrip(self, ec_keys, monkeypatch):
        monkeypatch.delenv("RELAY_BEARER_TOKEN", raising=False)
        priv_pem, pub_pem = ec_keys
        token = sign_jws(private_key_pem=priv_pem, subject="agent-x")
        claims = jwt.decode(token, pub_pem, algorithms=["ES256"])
        assert claims["sub"] == "agent-x"
        assert "iat" in claims

    def test_resolve_bearer_uses_jws_when_key_path_given(self, ec_keys, tmp_path, monkeypatch):
        monkeypatch.delenv("RELAY_BEARER_TOKEN", raising=False)
        priv_pem, pub_pem = ec_keys
        key_path = tmp_path / "jws.pem"
        key_path.write_text(priv_pem)
        token = resolve_bearer_token(jws_key_path=str(key_path), subscriber_identity="agent-x")
        assert token is not None
        claims = jwt.decode(token, pub_pem, algorithms=["ES256"])
        assert claims["sub"] == "agent-x"

    def test_verify_relay_agent_card_interop(self, ec_keys):
        # relay 本体の署名器で署名 → SDK 側 verify で True（相互運用）。
        from relay.identity import sign_agent_card

        priv_pem, pub_pem = ec_keys
        card = {"name": "relay", "version": "2.0.0"}
        signed = sign_agent_card(card, private_key_pem=priv_pem, kid="k1", jku="https://x/jwks")
        assert verify_relay_agent_card(signed, public_key_pem=pub_pem) is True

    def test_verify_fails_with_wrong_key(self, ec_keys):
        from relay.identity import sign_agent_card

        priv_pem, _ = ec_keys
        other = ec.generate_private_key(ec.SECP256R1())
        other_pub = (
            other.public_key()
            .public_bytes(
                serialization.Encoding.PEM,
                serialization.PublicFormat.SubjectPublicKeyInfo,
            )
            .decode()
        )
        signed = sign_agent_card(
            {"name": "relay"}, private_key_pem=priv_pem, kid="k1", jku="https://x/jwks"
        )
        assert verify_relay_agent_card(signed, public_key_pem=other_pub) is False

    def test_verify_unsigned_card_false(self, ec_keys):
        _, pub_pem = ec_keys
        assert verify_relay_agent_card({"name": "relay"}, public_key_pem=pub_pem) is False


class TestReconcile:
    def test_passes_since_ts_and_yields_fetcher_output(self):
        seen_args = []

        def fetcher(since_ts):
            seen_args.append(since_ts)
            yield {"id": 1, "since": since_ts}
            yield {"id": 2, "since": since_ts}

        out = list(reconcile(fetcher=fetcher, labels=["a"], since_ts="2026-01-01T00:00:00Z"))
        assert seen_args == ["2026-01-01T00:00:00Z"]
        assert [o["id"] for o in out] == [1, 2]

    def test_since_ts_none_default(self):
        def fetcher(since_ts):
            assert since_ts is None
            yield "x"

        assert list(reconcile(fetcher=fetcher, labels=["a"])) == ["x"]
