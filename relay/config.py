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

# dispatcher / push retry / SSE の既定値（relay-v2-wire-api.md §6.2, §6.4, §5.5）
DEFAULT_DISPATCHER_POLL_INTERVAL_SECONDS = 0.2  # 100ms〜1s の範囲内
DEFAULT_DISPATCHER_LOCK_PATH = "relay-dispatcher.lock"
DEFAULT_SSE_KEEPALIVE_SECONDS = 30
DEFAULT_SSE_SEND_TIMEOUT_SECONDS = 5.0
DEFAULT_DLQ_RETENTION_DAYS = 7
DEFAULT_PUBLISH_RATE_LIMIT_PER_SECOND = 100

# lease 切れ済み subscription を in-memory registry に残しておく猶予秒数
# （relay-v2-wire-api.md §5.7 の 410 ヒントを再接続の遅い subscriber にも
# 一定時間だけ提供するため）。この猶予を過ぎたら registry から物理的に除去する
# （unsubscribe されないまま放置された subscription による無制限のメモリ増加を防ぐ）。
# 404 / 410 いずれも subscriber は同一に「re-subscribe せよ」と扱うため（§5.7）、
# 猶予の長さは機能上の互換性には影響しない。
DEFAULT_SUBSCRIPTION_REGISTRY_RETENTION_SECONDS = 3600  # 1h


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

    # 配達基盤（relay-v2-wire-api.md §6）
    dispatcher_poll_interval_seconds: float = DEFAULT_DISPATCHER_POLL_INTERVAL_SECONDS
    dispatcher_lock_path: str = DEFAULT_DISPATCHER_LOCK_PATH
    sse_keepalive_seconds: float = DEFAULT_SSE_KEEPALIVE_SECONDS
    sse_send_timeout_seconds: float = DEFAULT_SSE_SEND_TIMEOUT_SECONDS
    dlq_retention_days: int = DEFAULT_DLQ_RETENTION_DAYS
    publish_rate_limit_per_second: int = DEFAULT_PUBLISH_RATE_LIMIT_PER_SECOND
    subscription_registry_retention_seconds: float = (
        DEFAULT_SUBSCRIPTION_REGISTRY_RETENTION_SECONDS
    )


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
        dispatcher_poll_interval_seconds=float(
            os.environ.get(
                "RELAY_DISPATCHER_POLL_INTERVAL_SECONDS",
                DEFAULT_DISPATCHER_POLL_INTERVAL_SECONDS,
            )
        ),
        dispatcher_lock_path=os.environ.get(
            "RELAY_DISPATCHER_LOCK_PATH", DEFAULT_DISPATCHER_LOCK_PATH
        ),
        sse_keepalive_seconds=float(
            os.environ.get("RELAY_SSE_KEEPALIVE_SECONDS", DEFAULT_SSE_KEEPALIVE_SECONDS)
        ),
        sse_send_timeout_seconds=float(
            os.environ.get(
                "RELAY_SSE_SEND_TIMEOUT_SECONDS", DEFAULT_SSE_SEND_TIMEOUT_SECONDS
            )
        ),
        dlq_retention_days=int(
            os.environ.get("RELAY_DLQ_RETENTION_DAYS", DEFAULT_DLQ_RETENTION_DAYS)
        ),
        publish_rate_limit_per_second=int(
            os.environ.get(
                "RELAY_PUBLISH_RATE_LIMIT_PER_SECOND",
                DEFAULT_PUBLISH_RATE_LIMIT_PER_SECOND,
            )
        ),
        subscription_registry_retention_seconds=float(
            os.environ.get(
                "RELAY_SUBSCRIPTION_REGISTRY_RETENTION_SECONDS",
                DEFAULT_SUBSCRIPTION_REGISTRY_RETENTION_SECONDS,
            )
        ),
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """プロセス内で共有する `Settings` を返す（初回呼び出しで環境変数から解決）。"""
    return load_settings_from_env()
