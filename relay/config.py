"""relay v2 の実行時設定。

環境変数から読み込む。値は起動時に一度だけ解決し、`get_settings()` で共有する
`Settings` インスタンスを返す（テストでは `Settings(...)` を直接組み立てて DI する）。
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from functools import lru_cache


DEFAULT_DB_PATH = "relay.db"
DEFAULT_SERVER_LOG_PATH = "relay-server.jsonl"

# outbox / DLQ の既定値（relay-v2-wire-api.md §5.1, §6.4）
DEFAULT_RETAIN_SECONDS = 86400  # 24h
MIN_RETAIN_SECONDS = 60
MAX_RETAIN_SECONDS = 86400

DEFAULT_LEASE_TTL_SECONDS = 300
MIN_LEASE_TTL_SECONDS = 30
MAX_LEASE_TTL_SECONDS = 86400


@dataclass(frozen=True)
class Settings:
    """relay インスタンスの設定値。"""

    db_path: str = DEFAULT_DB_PATH
    server_log_path: str = DEFAULT_SERVER_LOG_PATH

    # AgentCard 用
    agent_name: str = "relay"
    agent_version: str = "2.0.0"
    provider: str | None = None
    documentation_url: str | None = None

    # Bearer token 検証用の静的 token→identity 対応表（最小セット実装、§3.4 参照）。
    # 本番運用で外部 IdP に差し替える場合は identity.py の TokenVerifier 実装を差し替える。
    auth_tokens: dict[str, str] = field(default_factory=dict)

    # JWS 署名鍵（MAY 機能）。未設定なら relay は署名なし AgentCard を返す（最小セット）。
    jws_private_key_pem: str | None = None
    jws_kid: str | None = None
    jws_jku: str | None = None


def _load_auth_tokens_from_env() -> dict[str, str]:
    """`RELAY_AUTH_TOKENS` 環境変数（JSON: {"<token>": "<identity>"}）から読み込む。"""
    raw = os.environ.get("RELAY_AUTH_TOKENS")
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            "RELAY_AUTH_TOKENS は JSON object ({\"<token>\": \"<identity>\"}) で指定してください"
        ) from exc
    if not isinstance(parsed, dict):
        raise ValueError("RELAY_AUTH_TOKENS は JSON object でなければなりません")
    return {str(k): str(v) for k, v in parsed.items()}


def load_settings_from_env() -> Settings:
    """環境変数から `Settings` を組み立てる。"""
    return Settings(
        db_path=os.environ.get("RELAY_DB_PATH", DEFAULT_DB_PATH),
        server_log_path=os.environ.get("RELAY_SERVER_LOG_PATH", DEFAULT_SERVER_LOG_PATH),
        agent_name=os.environ.get("RELAY_AGENT_NAME", "relay"),
        agent_version=os.environ.get("RELAY_AGENT_VERSION", "2.0.0"),
        provider=os.environ.get("RELAY_PROVIDER"),
        documentation_url=os.environ.get("RELAY_DOCUMENTATION_URL"),
        auth_tokens=_load_auth_tokens_from_env(),
        jws_private_key_pem=os.environ.get("RELAY_JWS_PRIVATE_KEY_PEM"),
        jws_kid=os.environ.get("RELAY_JWS_KID"),
        jws_jku=os.environ.get("RELAY_JWS_JKU"),
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """プロセス内で共有する `Settings` を返す（初回呼び出しで環境変数から解決）。"""
    return load_settings_from_env()
