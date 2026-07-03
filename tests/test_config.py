"""relay.config テストスイート。"""
import json

import pytest

from relay.config import Settings, load_settings_from_env


class TestSettingsDefaults:
    def test_defaults(self):
        settings = Settings()
        assert settings.db_path == "relay.db"
        assert settings.auth_tokens == {}
        assert settings.jws_private_key_pem is None


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
