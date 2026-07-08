"""relay.identity テストスイート。

Bearer token authN、AgentCard 構築、JCS 正規化、JWS 署名/検証を検証する。
"""
import json

import pytest
from joserfc.jwk import ECKey
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route
from starlette.testclient import TestClient

from relay.config import Settings
from relay.identity import (
    MEDIA_TYPE_AGENT_CARD,
    AuthenticationError,
    Identity,
    authenticate_request,
    build_public_agent_card,
    canonicalize_agent_card,
    require_authn,
    sign_agent_card,
    verify_agent_card_signature,
)


# ---------------------------------------------------------------------------
# フィクスチャ
# ---------------------------------------------------------------------------


@pytest.fixture()
def settings(tmp_path):
    # server_log_path を tmp_path に向ける。`require_authn` は認証失敗時に
    # `observability.record_event` を呼ぶため（構造化ログ、`level="warning"`）、
    # 既定値のままだとリポジトリ直下の `relay-server.jsonl` に書き込んでしまう。
    return Settings(
        db_path=":memory:",
        auth_tokens={"tok-abc": "agent-a"},
        server_log_path=str(tmp_path / "test-relay-server.jsonl"),
    )


@pytest.fixture()
def ec_key_pair():
    key = ECKey.generate_key(crv="P-256", private=True)
    return key.as_pem(private=True).decode(), key.as_pem(private=False).decode()


def _fake_request(headers: dict[str, str]) -> Request:
    """Authorization ヘッダだけを持つ最小限の ASGI scope から Request を組み立てる。"""
    encoded_headers = [
        (k.lower().encode("latin-1"), v.encode("latin-1")) for k, v in headers.items()
    ]
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": encoded_headers,
    }
    return Request(scope)


# ---------------------------------------------------------------------------
# authenticate_request
# ---------------------------------------------------------------------------


class TestAuthenticateRequest:
    def test_missing_header_raises(self, settings):
        request = _fake_request({})
        with pytest.raises(AuthenticationError):
            authenticate_request(request, settings)

    def test_non_bearer_scheme_raises(self, settings):
        request = _fake_request({"Authorization": "Basic dXNlcjpwYXNz"})
        with pytest.raises(AuthenticationError):
            authenticate_request(request, settings)

    def test_unregistered_token_raises(self, settings):
        request = _fake_request({"Authorization": "Bearer nope"})
        with pytest.raises(AuthenticationError):
            authenticate_request(request, settings)

    def test_valid_token_returns_identity(self, settings):
        request = _fake_request({"Authorization": "Bearer tok-abc"})
        identity = authenticate_request(request, settings)
        assert identity == Identity(id="agent-a")


# ---------------------------------------------------------------------------
# require_authn デコレータ（Starlette route 経由）
# ---------------------------------------------------------------------------


class TestRequireAuthnDecorator:
    @pytest.fixture()
    def client(self, settings):
        @require_authn
        async def protected(request: Request):
            return JSONResponse({"identity": request.state.identity.id})

        app = Starlette(routes=[Route("/protected", protected, methods=["GET"])])
        app.state.settings = settings
        return TestClient(app)

    def test_no_auth_header_returns_401(self, client):
        r = client.get("/protected")
        assert r.status_code == 401

    def test_wrong_token_returns_401(self, client):
        r = client.get("/protected", headers={"Authorization": "Bearer wrong"})
        assert r.status_code == 401

    def test_correct_token_returns_200_with_identity(self, client):
        r = client.get("/protected", headers={"Authorization": "Bearer tok-abc"})
        assert r.status_code == 200
        assert r.json() == {"identity": "agent-a"}

    def test_auth_failure_is_recorded_as_structured_warning(self, client, settings):
        """認証失敗は構造化ログに warning として残る（wire-api.md §7.3）。"""
        client.get("/protected", headers={"Authorization": "Bearer wrong"})

        lines = open(settings.server_log_path, encoding="utf-8").readlines()
        entries = [json.loads(line) for line in lines]
        assert any(e["event"] == "authn_failed" and e["level"] == "warning" for e in entries)


# ---------------------------------------------------------------------------
# AgentCard 構築
# ---------------------------------------------------------------------------


class TestBuildPublicAgentCard:
    def test_minimal_set_has_no_signatures(self, settings):
        card = build_public_agent_card(settings)
        assert "signatures" not in card
        assert card["name"] == "relay"
        assert card["capabilities"]["streaming"] is True
        assert card["capabilities"]["pushNotifications"] is False
        # extended AgentCard の応答 handler は未実装のため false 固定。
        assert card["capabilities"]["extendedAgentCard"] is False

    def test_security_scheme_is_wrapper_key_form(self, settings):
        """OpenAPI flat 形（{"type": "http"}）ではなく discriminated-union wrapper-key 形。"""
        card = build_public_agent_card(settings)
        assert card["securitySchemes"] == {
            "bearer": {"httpAuthSecurityScheme": {"scheme": "bearer"}}
        }
        assert card["security"] == [{"bearer": []}]

    def test_full_set_includes_signatures_when_jws_key_configured(self, ec_key_pair):
        priv_pem, _pub_pem = ec_key_pair
        settings = Settings(
            db_path=":memory:",
            jws_private_key_pem=priv_pem,
            jws_kid="key-1",
            jws_jku="https://relay.example/.well-known/jwks.json",
        )
        card = build_public_agent_card(settings)
        assert "signatures" in card
        assert len(card["signatures"]) == 1

    def test_provider_and_documentation_url_optional_fields(self):
        settings = Settings(
            db_path=":memory:",
            provider="acme",
            documentation_url="https://relay.example/docs",
        )
        card = build_public_agent_card(settings)
        assert card["provider"] == "acme"
        assert card["documentationUrl"] == "https://relay.example/docs"

    def test_provider_omitted_when_not_configured(self, settings):
        card = build_public_agent_card(settings)
        assert "provider" not in card
        assert "documentationUrl" not in card


# ---------------------------------------------------------------------------
# JCS 正規化
# ---------------------------------------------------------------------------


class TestCanonicalizeAgentCard:
    def test_strips_signatures_field(self):
        card = {"name": "relay", "signatures": [{"protected": "x", "signature": "y"}]}
        result = canonicalize_agent_card(card)
        assert b"signatures" not in result

    def test_strips_default_valued_properties(self):
        card = {
            "name": "relay",
            "empty_str": "",
            "false_flag": False,
            "empty_list": [],
            "empty_obj": {},
            "none_val": None,
        }
        result = canonicalize_agent_card(card)
        assert result == canonicalize_agent_card({"name": "relay"})

    def test_preserves_falsy_but_meaningful_values(self):
        """0 や 0.0 は JSON 的に意味のある値なので、default 値除去の対象にしない。"""
        card = {"name": "relay", "count": 0, "ratio": 0.0}
        result = canonicalize_agent_card(card)
        assert b'"count":0' in result
        assert b'"ratio":0' in result

    def test_deterministic_key_ordering(self):
        """JCS は key をソートするため、dict の構築順序に依存せず同じバイト列になる。"""
        card_a = {"b": 1, "a": 2}
        card_b = {"a": 2, "b": 1}
        assert canonicalize_agent_card(card_a) == canonicalize_agent_card(card_b)


# ---------------------------------------------------------------------------
# JWS 署名 / 検証
# ---------------------------------------------------------------------------


class TestJwsSignAndVerify:
    def test_sign_adds_signatures_field(self, ec_key_pair):
        priv_pem, _pub_pem = ec_key_pair
        card = {"name": "relay", "version": "2.0.0"}
        signed = sign_agent_card(card, private_key_pem=priv_pem, kid="k1", jku="https://x/jwks.json")
        assert "signatures" in signed
        assert set(signed["signatures"][0].keys()) == {"protected", "signature"}

    def test_verify_succeeds_with_correct_public_key(self, ec_key_pair):
        priv_pem, pub_pem = ec_key_pair
        card = {"name": "relay", "version": "2.0.0"}
        signed = sign_agent_card(card, private_key_pem=priv_pem, kid="k1", jku="https://x/jwks.json")
        assert verify_agent_card_signature(signed, public_key_pem=pub_pem) is True

    def test_verify_fails_with_wrong_public_key(self, ec_key_pair):
        priv_pem, _pub_pem = ec_key_pair
        other_key = ECKey.generate_key(crv="P-256", private=True)
        other_pub_pem = other_key.as_pem(private=False).decode()

        card = {"name": "relay", "version": "2.0.0"}
        signed = sign_agent_card(card, private_key_pem=priv_pem, kid="k1", jku="https://x/jwks.json")
        assert verify_agent_card_signature(signed, public_key_pem=other_pub_pem) is False

    def test_verify_fails_when_card_tampered_after_signing(self, ec_key_pair):
        priv_pem, pub_pem = ec_key_pair
        card = {"name": "relay", "version": "2.0.0"}
        signed = sign_agent_card(card, private_key_pem=priv_pem, kid="k1", jku="https://x/jwks.json")

        tampered = dict(signed)
        tampered["name"] = "evil-impersonator"
        assert verify_agent_card_signature(tampered, public_key_pem=pub_pem) is False

    def test_verify_fails_when_no_signatures_present(self, ec_key_pair):
        _priv_pem, pub_pem = ec_key_pair
        card = {"name": "relay", "version": "2.0.0"}
        assert verify_agent_card_signature(card, public_key_pem=pub_pem) is False

    @pytest.mark.parametrize(
        "malformed_card",
        [
            # signatures[0] は dict だが必須キー（protected/signature）が欠落。
            {"name": "relay", "signatures": [{"foo": "bar"}]},
            # signatures が list でなく dict（int キー 0 で解決できず KeyError）。
            {"name": "relay", "signatures": {"protected": "x", "signature": "y"}},
            # signatures[0] が dict でなく文字列（sig['protected'] が TypeError）。
            {"name": "relay", "signatures": ["garbage"]},
        ],
        ids=["missing_keys", "signatures_is_dict", "element_is_string"],
    )
    def test_verify_returns_false_for_malformed_signatures_structure(
        self, ec_key_pair, malformed_card
    ):
        """外部 agent から受け取る非信頼 AgentCard の malformed signatures 構造で
        crash せず fail-closed（False）を返すことを検証する（回帰: KeyError / TypeError
        が生の例外として漏れていた）。"""
        _priv_pem, pub_pem = ec_key_pair
        assert verify_agent_card_signature(malformed_card, public_key_pem=pub_pem) is False


# ---------------------------------------------------------------------------
# MEDIA_TYPE_AGENT_CARD 定数
# ---------------------------------------------------------------------------


def test_media_type_constant_matches_a2a_spec():
    assert MEDIA_TYPE_AGENT_CARD == "application/a2a+json"
