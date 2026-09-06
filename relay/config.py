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

# push は成功している（SSE queue には積めている）が subscriber 側の受信 / ack ループが
# スタックして ack が進まない接続を強制切断するまでの猶予秒数。queue backpressure ベースの
# slow consumer 切断（`delivery._push_with_retry`）とは別の障害モードを検知する。
DEFAULT_ACK_TIMEOUT_SECONDS = 60.0

# 外部 agent の AgentCard キャッシュ（agent_cards table）の TTL 既定値。
# identity-authz.md §4.2: identity 自体は relay 再起動を跨いで disk 永続化される。
DEFAULT_AGENT_CARD_CACHE_TTL_SECONDS = 3600  # 1h

# request body のサイズ上限（bytes、セキュリティ監査 finding H-4/F2: PayloadTooLargeError が
# 定義のみで未配線だった点の解消）。relay-v2-wire-api.md 等の仕様書に具体的なバイト数の
# 記載は無いため、一般的なメッセージング API の慣行（例: Amazon SQS のメッセージサイズ上限
# 256KiB）を参考にした値であり、実測に基づく数値ではない。
DEFAULT_MAX_PAYLOAD_BYTES = 262_144  # 256 KiB

# lease 切れ済み subscription を in-memory registry に残しておく猶予秒数
# （relay-v2-wire-api.md §5.7 の 410 ヒントを再接続の遅い subscriber にも
# 一定時間だけ提供するため）。この猶予を過ぎたら registry から物理的に除去する
# （unsubscribe されないまま放置された subscription による無制限のメモリ増加を防ぐ）。
# 404 / 410 いずれも subscriber は同一に「re-subscribe せよ」と扱うため（§5.7）、
# 猶予の長さは機能上の互換性には影響しない。
DEFAULT_SUBSCRIPTION_REGISTRY_RETENTION_SECONDS = 3600  # 1h

# close 済み stream を in-memory registry に残しておく猶予秒数。この猶予を過ぎ、かつ
# その stream の未配達 outbox エントリが drain し切ったものだけを registry から除去する
# （close されたまま放置された stream record による registry の無制限成長を防ぐ）。
# subscription 側 lease 猶予（上記）と対称。
DEFAULT_STREAM_REGISTRY_RETENTION_SECONDS = 3600  # 1h

# stream / subscription registry の資源上限（DoS 防御）。total は registry 全体、
# per-identity は 1 identity が保持できる数の上限（単一 peer による総枠の占有を防ぐ）。
# in-memory record は 1 件あたり高々 1KB 程度で、total 上限でも registry 全体のメモリは
# 数十 MB 未満に収まる。値は「想定同時 peer 数（〜20）× 1 peer あたり想定リソース数
# （〜1000）」を目安に total を置き、per-identity をその 1/20 とした現実的な初期値。
DEFAULT_MAX_STREAMS_TOTAL = 20000
DEFAULT_MAX_STREAMS_PER_IDENTITY = 1000
DEFAULT_MAX_SUBSCRIPTIONS_TOTAL = 20000
DEFAULT_MAX_SUBSCRIPTIONS_PER_IDENTITY = 1000

# publish / subscribe の入力フィールド上限（DoS 防御）。無制限の title 文字列や大量の
# label は registry / outbox のメモリを膨らませる。値は routing key（label）と表示用
# 見出し（title）の実運用サイズを目安にした現実的な初期値で、いずれも設定可能。
# title 上限 200 は relay-v2-wire-api.md §5.4 の記載（max 200 UTF-8 chars）に揃える。
DEFAULT_MAX_TITLE_LENGTH = 200
DEFAULT_MAX_LABELS_COUNT = 32
DEFAULT_MAX_LABEL_LENGTH = 128
# stream の name は canonical stream_id（"{creator}:{name}"）の一部として registry・outbox・
# delivery target key に埋め込まれるため、label と同じ識別子系の上限に揃える。
DEFAULT_MAX_STREAM_NAME_LENGTH = 128

# federation（relay 間連合）の既定値。
DEFAULT_FEDERATION_TS_SKEW_SECONDS = 300
DEFAULT_FEDERATION_ALLOW_PRIVATE_LOCATORS = False
DEFAULT_FEDERATION_REQUIRE_ENCRYPTION = False


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

    # JWE 暗号化鍵（federation envelope body の E2E 暗号化用、ECDH-ES + A256GCM 固定）。
    # jws_private_key_pem（署名・peer 認証用）とは意図的に別鍵にする（1 鍵 2 用途の流用を
    # 避ける、federation-design-exploration.md の鍵分離方針）。未設定なら envelope 暗号化
    # は行わず互換のため平文 body を送る（relay.federation_egress 参照）。
    jwe_private_key_pem: str | None = None

    # 配達基盤（relay-v2-wire-api.md §6）
    dispatcher_poll_interval_seconds: float = DEFAULT_DISPATCHER_POLL_INTERVAL_SECONDS
    dispatcher_lock_path: str = DEFAULT_DISPATCHER_LOCK_PATH
    sse_keepalive_seconds: float = DEFAULT_SSE_KEEPALIVE_SECONDS
    sse_send_timeout_seconds: float = DEFAULT_SSE_SEND_TIMEOUT_SECONDS
    dlq_retention_days: int = DEFAULT_DLQ_RETENTION_DAYS
    publish_rate_limit_per_second: int = DEFAULT_PUBLISH_RATE_LIMIT_PER_SECOND
    ack_timeout_seconds: float = DEFAULT_ACK_TIMEOUT_SECONDS
    agent_card_cache_ttl_seconds: int = DEFAULT_AGENT_CARD_CACHE_TTL_SECONDS
    subscription_registry_retention_seconds: float = (
        DEFAULT_SUBSCRIPTION_REGISTRY_RETENTION_SECONDS
    )
    stream_registry_retention_seconds: float = DEFAULT_STREAM_REGISTRY_RETENTION_SECONDS

    # registry 資源上限（DoS 防御）
    max_streams_total: int = DEFAULT_MAX_STREAMS_TOTAL
    max_streams_per_identity: int = DEFAULT_MAX_STREAMS_PER_IDENTITY
    max_subscriptions_total: int = DEFAULT_MAX_SUBSCRIPTIONS_TOTAL
    max_subscriptions_per_identity: int = DEFAULT_MAX_SUBSCRIPTIONS_PER_IDENTITY

    # 入力フィールド上限（DoS 防御）
    max_title_length: int = DEFAULT_MAX_TITLE_LENGTH
    max_labels_count: int = DEFAULT_MAX_LABELS_COUNT
    max_label_length: int = DEFAULT_MAX_LABEL_LENGTH
    max_stream_name_length: int = DEFAULT_MAX_STREAM_NAME_LENGTH

    # request body 全体のサイズ上限（DoS 防御）
    max_payload_bytes: int = DEFAULT_MAX_PAYLOAD_BYTES

    # federation（relay 間連合）。base_url は招待 URL 生成・redeem 応答 card の locator に使う。
    # jws_private_key_pem が未設定なら federation 機能自体を無効化する（fail-closed、
    # relay 自身の federation マシン鍵は AgentCard 署名鍵と共用のため）。
    federation_base_url: str | None = None
    federation_ts_skew_seconds: int = DEFAULT_FEDERATION_TS_SKEW_SECONDS
    federation_allow_private_locators: bool = DEFAULT_FEDERATION_ALLOW_PRIVATE_LOCATORS

    # true の場合、暗号化鍵が双方揃わない peer 宛の配達を平文フォールバックさせず
    # permanent error として DLQ に回す（peer 単位の `peers.require_encryption` との OR、
    # relay.federation_egress 参照）。既定 false は既存ロールアウトの互換性を壊さないため。
    federation_require_encryption: bool = DEFAULT_FEDERATION_REQUIRE_ENCRYPTION


def _parse_bool_env(env_var: str, default: bool) -> bool:
    """`"1"` / `"true"`（大小文字無視）を True、それ以外・未設定を `default` として読む。"""
    raw = os.environ.get(env_var)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true")


def validate_local_identity(identity: str) -> None:
    """local identity が `@` を含まないことを検証する。

    `@` を含む identity は federation の peer namespace（`sub@handle`）用に構造的に予約
    されている。local identity にこれを許すと namespace の構造的分離が崩れる。
    """
    if "@" in identity:
        raise ValueError(
            f"local identity に '@' は使用できません（federation peer namespace 用に予約）: {identity!r}"
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
    result = {str(k): str(v) for k, v in parsed.items()}
    for identity in result.values():
        validate_local_identity(identity)
    return result


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
        jwe_private_key_pem=os.environ.get("RELAY_JWE_PRIVATE_KEY_PEM"),
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
        ack_timeout_seconds=float(
            os.environ.get("RELAY_ACK_TIMEOUT_SECONDS", DEFAULT_ACK_TIMEOUT_SECONDS)
        ),
        agent_card_cache_ttl_seconds=int(
            os.environ.get(
                "RELAY_AGENT_CARD_CACHE_TTL_SECONDS", DEFAULT_AGENT_CARD_CACHE_TTL_SECONDS
            )
        ),
        subscription_registry_retention_seconds=float(
            os.environ.get(
                "RELAY_SUBSCRIPTION_REGISTRY_RETENTION_SECONDS",
                DEFAULT_SUBSCRIPTION_REGISTRY_RETENTION_SECONDS,
            )
        ),
        stream_registry_retention_seconds=float(
            os.environ.get(
                "RELAY_STREAM_REGISTRY_RETENTION_SECONDS",
                DEFAULT_STREAM_REGISTRY_RETENTION_SECONDS,
            )
        ),
        max_streams_total=int(
            os.environ.get("RELAY_MAX_STREAMS_TOTAL", DEFAULT_MAX_STREAMS_TOTAL)
        ),
        max_streams_per_identity=int(
            os.environ.get(
                "RELAY_MAX_STREAMS_PER_IDENTITY", DEFAULT_MAX_STREAMS_PER_IDENTITY
            )
        ),
        max_subscriptions_total=int(
            os.environ.get(
                "RELAY_MAX_SUBSCRIPTIONS_TOTAL", DEFAULT_MAX_SUBSCRIPTIONS_TOTAL
            )
        ),
        max_subscriptions_per_identity=int(
            os.environ.get(
                "RELAY_MAX_SUBSCRIPTIONS_PER_IDENTITY",
                DEFAULT_MAX_SUBSCRIPTIONS_PER_IDENTITY,
            )
        ),
        max_title_length=int(
            os.environ.get("RELAY_MAX_TITLE_LENGTH", DEFAULT_MAX_TITLE_LENGTH)
        ),
        max_labels_count=int(
            os.environ.get("RELAY_MAX_LABELS_COUNT", DEFAULT_MAX_LABELS_COUNT)
        ),
        max_label_length=int(
            os.environ.get("RELAY_MAX_LABEL_LENGTH", DEFAULT_MAX_LABEL_LENGTH)
        ),
        max_payload_bytes=int(
            os.environ.get("RELAY_MAX_PAYLOAD_BYTES", DEFAULT_MAX_PAYLOAD_BYTES)
        ),
        federation_base_url=os.environ.get("RELAY_BASE_URL"),
        federation_ts_skew_seconds=int(
            os.environ.get(
                "RELAY_FEDERATION_TS_SKEW_SECONDS", DEFAULT_FEDERATION_TS_SKEW_SECONDS
            )
        ),
        federation_allow_private_locators=_parse_bool_env(
            "RELAY_FEDERATION_ALLOW_PRIVATE_LOCATORS",
            DEFAULT_FEDERATION_ALLOW_PRIVATE_LOCATORS,
        ),
        federation_require_encryption=_parse_bool_env(
            "RELAY_FEDERATION_REQUIRE_ENCRYPTION",
            DEFAULT_FEDERATION_REQUIRE_ENCRYPTION,
        ),
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """プロセス内で共有する `Settings` を返す（初回呼び出しで環境変数から解決）。"""
    return load_settings_from_env()
