"""relay.config テストスイート。"""
import json

import pytest

from relay.config import Settings, load_settings_from_env, validate_local_identity


class TestSettingsDefaults:
    def test_defaults(self):
        settings = Settings()
        assert settings.db_path == "relay.db"
        assert settings.auth_tokens == {}
        assert settings.jws_private_key_pem is None

    def test_registry_limit_defaults(self):
        settings = Settings()
        assert settings.max_streams_total == 20000
        assert settings.max_streams_per_identity == 1000
        assert settings.max_subscriptions_total == 20000
        assert settings.max_subscriptions_per_identity == 1000
        assert settings.stream_registry_retention_seconds == 3600


class TestLoadSettingsFromEnv:
    def test_reads_db_path(self, monkeypatch):
        monkeypatch.setenv("RELAY_DB_PATH", "/tmp/custom.db")
        settings = load_settings_from_env()
        assert settings.db_path == "/tmp/custom.db"

    def test_reads_auth_tokens_json(self, monkeypatch):
        monkeypatch.setenv(
            "RELAY_AUTH_TOKENS", json.dumps({"tok-1": "agent-a", "tok-2": "agent-b"})
        )
        settings = load_settings_from_env()
        assert settings.auth_tokens == {"tok-1": "agent-a", "tok-2": "agent-b"}

    def test_missing_auth_tokens_env_defaults_to_empty(self, monkeypatch):
        monkeypatch.delenv("RELAY_AUTH_TOKENS", raising=False)
        settings = load_settings_from_env()
        assert settings.auth_tokens == {}

    def test_invalid_auth_tokens_json_raises(self, monkeypatch):
        monkeypatch.setenv("RELAY_AUTH_TOKENS", "not-json")
        with pytest.raises(ValueError):
            load_settings_from_env()

    def test_auth_tokens_must_be_object(self, monkeypatch):
        monkeypatch.setenv("RELAY_AUTH_TOKENS", json.dumps(["not", "an", "object"]))
        with pytest.raises(ValueError):
            load_settings_from_env()

    def test_reads_registry_limits_from_env(self, monkeypatch):
        monkeypatch.setenv("RELAY_MAX_STREAMS_TOTAL", "50")
        monkeypatch.setenv("RELAY_MAX_STREAMS_PER_IDENTITY", "5")
        monkeypatch.setenv("RELAY_MAX_SUBSCRIPTIONS_TOTAL", "60")
        monkeypatch.setenv("RELAY_MAX_SUBSCRIPTIONS_PER_IDENTITY", "6")
        monkeypatch.setenv("RELAY_STREAM_REGISTRY_RETENTION_SECONDS", "120")
        settings = load_settings_from_env()
        assert settings.max_streams_total == 50
        assert settings.max_streams_per_identity == 5
        assert settings.max_subscriptions_total == 60
        assert settings.max_subscriptions_per_identity == 6
        assert settings.stream_registry_retention_seconds == 120

    def test_rejects_at_in_auth_token_identity(self, monkeypatch):
        monkeypatch.setenv("RELAY_AUTH_TOKENS", json.dumps({"tok-1": "orch@bob"}))
        with pytest.raises(ValueError):
            load_settings_from_env()

    def test_reads_federation_settings_defaults(self, monkeypatch):
        monkeypatch.delenv("RELAY_BASE_URL", raising=False)
        monkeypatch.delenv("RELAY_FEDERATION_TS_SKEW_SECONDS", raising=False)
        monkeypatch.delenv("RELAY_FEDERATION_ALLOW_PRIVATE_LOCATORS", raising=False)
        settings = load_settings_from_env()
        assert settings.federation_base_url is None
        assert settings.federation_ts_skew_seconds == 300
        assert settings.federation_allow_private_locators is False

    def test_reads_federation_settings_from_env(self, monkeypatch):
        monkeypatch.setenv("RELAY_BASE_URL", "https://relay-a.example")
        monkeypatch.setenv("RELAY_FEDERATION_TS_SKEW_SECONDS", "60")
        monkeypatch.setenv("RELAY_FEDERATION_ALLOW_PRIVATE_LOCATORS", "true")
        settings = load_settings_from_env()
        assert settings.federation_base_url == "https://relay-a.example"
        assert settings.federation_ts_skew_seconds == 60
        assert settings.federation_allow_private_locators is True

    @pytest.mark.parametrize("raw", ["1", "true", "TRUE", "True"])
    def test_federation_allow_private_locators_truthy_values(self, monkeypatch, raw):
        monkeypatch.setenv("RELAY_FEDERATION_ALLOW_PRIVATE_LOCATORS", raw)
        settings = load_settings_from_env()
        assert settings.federation_allow_private_locators is True

    @pytest.mark.parametrize("raw", ["0", "false", "", "no"])
    def test_federation_allow_private_locators_falsy_values(self, monkeypatch, raw):
        monkeypatch.setenv("RELAY_FEDERATION_ALLOW_PRIVATE_LOCATORS", raw)
        settings = load_settings_from_env()
        assert settings.federation_allow_private_locators is False


class TestValidateLocalIdentity:
    def test_accepts_plain_identity(self):
        validate_local_identity("orch")

    def test_rejects_at_sign(self):
        with pytest.raises(ValueError):
            validate_local_identity("orch@bob")
