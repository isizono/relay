"""外部 agent の AgentCard 取得・キャッシュ（relay-v2-identity-authz.md §1.2.3, §1.3, §4.2）。

relay が「他の A2A agent」の公開 AgentCard（`<base_url>/.well-known/agent-card.json`）を
取得し、`agent_cards` table（`migrations/0001-initial-schema.sql`）にキャッシュする。
identity-authz.md §4.2 の通り identity 自体（AgentCard / 公開鍵）は relay の in-memory state
（subscription registry / stream membership）とは独立に disk 永続化され、relay 再起動を跨いで
保持される。そのため他の liveness 系 registry と異なり SQLite に置く（`db.py` の 4 table の 1 つ）。

- 取得: `fetch_agent_card` / `fetch_jwks`（HTTP GET）。実 HTTP をスタブできるよう `http_get` を
  注入点にしてある（既定は stdlib `urllib`。production 依存を増やさない）。
- 保存 / 読み出し: `store_agent_card` / `get_cached_agent_card` / `get_cached_jwks`。TTL
  （`expires_at`）超過は cache miss として扱う。
- 統合: `get_or_fetch_agent_card`（cache hit ならそれを返し、miss / 期限切れなら fetch → 任意で
  JWS 署名検証 → store）。`ttl_seconds` 省略時は `settings.agent_card_cache_ttl_seconds`
  （既定 1h）を実際に使う。
- 署名検証: `verify_card_signature`（identity-authz.md §1.2.3、JCS 正規化 + JWS 検証）。
  検証鍵は PEM 直接指定か JWKS（`jku` から取得した KeySet）で与える。
"""
from __future__ import annotations

import base64
import json
import sqlite3
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from joserfc import jws
from joserfc.jwk import KeySet

from relay import identity as identity_mod
from relay.config import DEFAULT_AGENT_CARD_CACHE_TTL_SECONDS, Settings

WELL_KNOWN_AGENT_CARD_PATH = "/.well-known/agent-card.json"

# `http_get(url, timeout) -> (status_code, body_bytes)` の注入点。
HttpGet = Callable[[str, float], "tuple[int, bytes]"]

# `get_or_fetch_agent_card` の `ttl_seconds` 未指定（呼び出し側が明示的に選んでいない）を
# 「明示的に None（無期限キャッシュ）を渡した」場合と区別するためのセンチネル。
_TTL_UNSET = object()


class AgentCardFetchError(Exception):
    """外部 AgentCard / JWKS の取得・パース・検証に失敗。"""


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def agent_card_url(base_url: str) -> str:
    """`base_url` から well-known AgentCard の URL を組み立てる。"""
    return base_url.rstrip("/") + WELL_KNOWN_AGENT_CARD_PATH


def _default_http_get(url: str, timeout: float) -> tuple[int, bytes]:
    req = urllib.request.Request(
        url, headers={"Accept": identity_mod.MEDIA_TYPE_AGENT_CARD}
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:  # noqa: S310 — 呼び出し側が URL を管理
        return resp.status, resp.read()


def _get_json(url: str, http_get: HttpGet | None, timeout: float, *, what: str) -> Any:
    getter = http_get or _default_http_get
    try:
        status, body = getter(url, timeout)
    except Exception as exc:  # noqa: BLE001 — network / URL error を一様に fetch error へ畳む
        raise AgentCardFetchError(f"{what} の取得に失敗しました: {url}") from exc
    if status != 200:
        raise AgentCardFetchError(f"{what} の取得が status {status} を返しました: {url}")
    try:
        return json.loads(body)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise AgentCardFetchError(f"{what} が不正な JSON です: {url}") from exc


def fetch_agent_card(
    base_url: str, *, http_get: HttpGet | None = None, timeout: float = 5.0
) -> dict:
    """外部 agent の `<base_url>/.well-known/agent-card.json` を取得して dict を返す。"""
    card = _get_json(agent_card_url(base_url), http_get, timeout, what="AgentCard")
    if not isinstance(card, dict):
        raise AgentCardFetchError("AgentCard は JSON object でなければなりません")
    return card


def fetch_jwks(jku: str, *, http_get: HttpGet | None = None, timeout: float = 5.0) -> dict:
    """AgentCard の protected header の `jku` が指す JWKS（rfc-7517）を取得する。"""
    jwks = _get_json(jku, http_get, timeout, what="JWKS")
    if not isinstance(jwks, dict) or not isinstance(jwks.get("keys"), list):
        raise AgentCardFetchError("JWKS は { keys: [...] } object でなければなりません")
    return jwks


# ---------------------------------------------------------------------------
# JWS 署名検証（identity-authz.md §1.2.3）
# ---------------------------------------------------------------------------


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def verify_card_signature(
    card: dict, *, public_key_pem: str | None = None, jwks: dict | None = None
) -> bool:
    """AgentCard の `signatures[0]` を検証する（identity-authz.md §1.2.3 の MUST 手順）。

    署名対象は `signatures` フィールドを除外した AgentCard を JCS（rfc-8785）で正規化したもの
    （`relay.identity.canonicalize_agent_card` と同一）。検証鍵は次のいずれかで与える:

    - `public_key_pem`: 単一 EC 公開鍵の PEM（`relay.identity.verify_agent_card_signature` に委譲）
    - `jwks`: `jku` から取得した JWKS。protected header の `kid` で KeySet から鍵を解決する。

    どちらも与えられない場合は検証できないため `False` を返す（fail-closed）。

    `card` は外部 agent から受け取る非信頼入力であり、`signatures` が `{protected, signature}`
    object の非空 list であることを一切仮定できない（list でなく dict / 要素が非 dict
    文字列 / 必須キー欠落等の malformed input が来うる）。構造の取り出しから JWS 検証までを
    単一の try で囲み、KeyError / TypeError / IndexError を含むあらゆる例外を検証失敗として
    畳む（`relay.identity.verify_agent_card_signature` と同じ fail-closed 方針）。
    """
    if public_key_pem is not None:
        return identity_mod.verify_agent_card_signature(card, public_key_pem=public_key_pem)
    if jwks is None:
        return False
    signatures = card.get("signatures")
    if not signatures:
        return False
    try:
        sig = signatures[0]
        payload = identity_mod.canonicalize_agent_card(card)
        compact = f"{sig['protected']}.{_b64url_encode(payload)}.{sig['signature']}"
        key_set = KeySet.import_key_set(jwks)
        result = jws.deserialize_compact(compact, key_set)
    except Exception:  # noqa: BLE001 — 構造不正 / 鍵解決 / 署名不一致はすべて検証失敗に畳む
        return False
    return result.payload == payload


# ---------------------------------------------------------------------------
# agent_cards table の読み書き
# ---------------------------------------------------------------------------


def store_agent_card(
    db_conn: sqlite3.Connection,
    identity: str,
    card: dict,
    *,
    jwks: dict | None = None,
    ttl_seconds: int | None = None,
    fetched_at: datetime | None = None,
) -> None:
    """`agent_cards` に upsert する（`identity` が PRIMARY KEY）。

    `ttl_seconds` を渡すと `expires_at = fetched_at + ttl_seconds` を記録する（None なら
    無期限キャッシュ = `expires_at` は NULL）。同一 transaction 制御は呼び出し側に委ねる
    （本関数は commit しない）。
    """
    now = fetched_at or _now()
    expires_at = _iso(now + timedelta(seconds=ttl_seconds)) if ttl_seconds is not None else None
    db_conn.execute(
        "INSERT INTO agent_cards (identity, card_json, public_keys_jwks, fetched_at, expires_at)"
        " VALUES (?, ?, ?, ?, ?)"
        " ON CONFLICT(identity) DO UPDATE SET"
        " card_json = excluded.card_json, public_keys_jwks = excluded.public_keys_jwks,"
        " fetched_at = excluded.fetched_at, expires_at = excluded.expires_at",
        (
            identity,
            json.dumps(card, ensure_ascii=False),
            json.dumps(jwks, ensure_ascii=False) if jwks is not None else None,
            _iso(now),
            expires_at,
        ),
    )


def _row_is_fresh(expires_at: str | None, now: datetime | None) -> bool:
    if expires_at is None:
        return True  # 無期限キャッシュ
    current = now or _now()
    # expires_at は固定幅 ISO8601（%Y-%m-%dT%H:%M:%SZ）なので辞書順比較が時刻順と一致する。
    return _iso(current) < expires_at


def get_cached_agent_card(
    db_conn: sqlite3.Connection, identity: str, *, now: datetime | None = None
) -> dict | None:
    """キャッシュから AgentCard を取得する。未登録 / TTL 超過は `None`（cache miss）。"""
    row = db_conn.execute(
        "SELECT card_json, expires_at FROM agent_cards WHERE identity = ?", (identity,)
    ).fetchone()
    if row is None:
        return None
    if not _row_is_fresh(row["expires_at"], now):
        return None
    return json.loads(row["card_json"])


def get_cached_jwks(
    db_conn: sqlite3.Connection, identity: str, *, now: datetime | None = None
) -> dict | None:
    """キャッシュ済み JWKS を取得する。未登録 / JWKS 未保存 / TTL 超過は `None`。"""
    row = db_conn.execute(
        "SELECT public_keys_jwks, expires_at FROM agent_cards WHERE identity = ?", (identity,)
    ).fetchone()
    if row is None or row["public_keys_jwks"] is None:
        return None
    if not _row_is_fresh(row["expires_at"], now):
        return None
    return json.loads(row["public_keys_jwks"])


def get_or_fetch_agent_card(
    db_conn: sqlite3.Connection,
    identity: str,
    base_url: str,
    *,
    http_get: HttpGet | None = None,
    ttl_seconds: Any = _TTL_UNSET,
    timeout: float = 5.0,
    now: datetime | None = None,
    settings: Settings | None = None,
    verify_public_key_pem: str | None = None,
    verify_jwks: dict | None = None,
) -> dict:
    """cache hit ならキャッシュを返し、miss / TTL 超過なら fetch → 任意で署名検証 → store する。

    `verify_public_key_pem` / `verify_jwks` のいずれかを渡すと、fetch した AgentCard の JWS
    署名を検証し、検証に失敗したら `AgentCardFetchError` を送出してキャッシュしない
    （fail-closed。identity-authz.md §1.2.3）。どちらも渡さない場合は署名検証をスキップする
    （最小セット AgentCard は署名なしで公開される。§1.2.4）。store 後に commit する。

    `ttl_seconds` を明示的に渡さない場合、`settings.agent_card_cache_ttl_seconds`
    （`settings` も省略時は `DEFAULT_AGENT_CARD_CACHE_TTL_SECONDS`、既定 1h）を使う。無期限
    キャッシュにしたい場合は `ttl_seconds=None` を明示的に渡す（省略とは区別される）。
    """
    cached = get_cached_agent_card(db_conn, identity, now=now)
    if cached is not None:
        return cached

    card = fetch_agent_card(base_url, http_get=http_get, timeout=timeout)
    if verify_public_key_pem is not None or verify_jwks is not None:
        if not verify_card_signature(
            card, public_key_pem=verify_public_key_pem, jwks=verify_jwks
        ):
            raise AgentCardFetchError("AgentCard の JWS 署名検証に失敗しました")

    if ttl_seconds is _TTL_UNSET:
        resolved_ttl_seconds = (
            settings.agent_card_cache_ttl_seconds
            if settings is not None
            else DEFAULT_AGENT_CARD_CACHE_TTL_SECONDS
        )
    else:
        resolved_ttl_seconds = ttl_seconds

    store_agent_card(
        db_conn,
        identity,
        card,
        jwks=verify_jwks,
        ttl_seconds=resolved_ttl_seconds,
        fetched_at=now,
    )
    db_conn.commit()
    return card
