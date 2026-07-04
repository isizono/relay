"""relay v2 の identity / authN。

スコープ（relay-v2-identity-authz.md）:
- `GET /.well-known/agent-card.json` 用の AgentCard 構築（§1.1）
- JCS（rfc-8785, MUST）による正規化、JWS（rfc-7515, MAY）による AgentCard 署名 / 検証（§1.2, §1.3）
- Bearer token 検証（§1.5.1 最小セット: `HTTPAuthSecurityScheme{scheme:"bearer"}`）

authZ（structural / semantic）はこのモジュールの対象外。§2 の境界（instance-global read は
authN のみ、resource 名指しの参照・状態変更は structural authZ、subscribe は authZ 対象外）に
従い、structural authZ（membership / ownership 照合）は各 endpoint 側で追加で行う。
"""
from __future__ import annotations

import base64
from dataclasses import dataclass
from functools import wraps
from typing import Any, Awaitable, Callable

import rfc8785
from joserfc import jws
from joserfc.jwk import ECKey
from starlette.requests import Request
from starlette.responses import JSONResponse

from relay.config import Settings

MEDIA_TYPE_AGENT_CARD = "application/a2a+json"


# ---------------------------------------------------------------------------
# authN: Bearer token 検証
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Identity:
    """認証済み identity。

    relay v2 は DID をスコープ外とする（identity-authz.md §1.4）ため、ここでの `id` は
    単なる文字列識別子であり、DID 形式であることを要求しない。
    """

    id: str


class AuthenticationError(Exception):
    """Bearer token 検証失敗。呼び出し側は 401 Unauthorized を返す。"""


def authenticate_request(request: Request, settings: Settings) -> Identity:
    """`Authorization: Bearer <token>` を検証し `Identity` を返す。

    token → identity の対応表は `settings.auth_tokens`（最小セット実装、静的表）。
    Bearer token 発行主体（relay 自前 vs 外部 IdP）は運用判断（identity-authz.md §7 未決事項）
    のため、外部 IdP 連携が要る場合はこの関数を差し替える。

    Raises:
        AuthenticationError: ヘッダ欠落・形式不正・未登録 token。
    """
    header = request.headers.get("authorization")
    if not header:
        raise AuthenticationError("Authorization ヘッダがありません")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise AuthenticationError("Bearer token 形式で指定してください")
    identity_id = settings.auth_tokens.get(token)
    if identity_id is None:
        raise AuthenticationError("token が無効です")
    return Identity(id=identity_id)


def require_authn(
    handler: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    """Starlette endpoint 用デコレータ。authN 済み `Identity` を `request.state.identity` に積む。

    `request.app.state.settings`（`Settings`）を参照する。全 endpoint はこれを最低限通す
    （identity-authz.md §1.5.1 の MUST）。structural / semantic authZ はこのデコレータの
    対象外で、各 endpoint 実装側が `request.state.identity` を見て追加判定する。

    使い方::

        @require_authn
        async def get_status(request: Request) -> Response:
            identity = request.state.identity
            ...
    """

    @wraps(handler)
    async def wrapper(request: Request, *args: Any, **kwargs: Any) -> Any:
        settings: Settings = request.app.state.settings
        try:
            identity = authenticate_request(request, settings)
        except AuthenticationError as exc:
            # 遅延 import: relay.observability は require_authn（本モジュール）を import
            # するため、モジュールトップレベルで import すると循環 import になる。
            from relay import observability

            observability.record_event(
                request.app.state, "authn_failed", level="warning", reason=str(exc)
            )
            return JSONResponse({"error": str(exc)}, status_code=401)
        request.state.identity = identity
        return await handler(request, *args, **kwargs)

    return wrapper


# ---------------------------------------------------------------------------
# AgentCard 構築
# ---------------------------------------------------------------------------


def build_public_agent_card(settings: Settings) -> dict:
    """公開 AgentCard を構築する（identity-authz.md §1.1.2, §1.1.3, §1.5.2）。

    `settings.jws_private_key_pem` / `jws_kid` / `jws_jku` が揃っていればフル準拠セット
    （ES256 JWS 署名付き）を、揃っていなければ最小セット（署名なし）を返す（§1.2.4）。
    """
    card: dict[str, Any] = {
        "name": settings.agent_name,
        "version": settings.agent_version,
        "supportedInterfaces": [{"protocolBinding": "HTTP+JSON"}],
        "capabilities": {
            "streaming": True,
            "pushNotifications": False,
            "extendedAgentCard": True,
        },
        "securitySchemes": {
            "bearer": {"httpAuthSecurityScheme": {"scheme": "bearer"}},
        },
        # bearer の scope 配列は authZ 判定に使わないため常に空（identity-authz.md §3.4）。
        "security": [{"bearer": []}],
    }
    if settings.provider:
        card["provider"] = settings.provider
    if settings.documentation_url:
        card["documentationUrl"] = settings.documentation_url

    if settings.jws_private_key_pem and settings.jws_kid and settings.jws_jku:
        card = sign_agent_card(
            card,
            private_key_pem=settings.jws_private_key_pem,
            kid=settings.jws_kid,
            jku=settings.jws_jku,
        )
    return card


# ---------------------------------------------------------------------------
# JCS（rfc-8785, MUST）
# ---------------------------------------------------------------------------


def canonicalize_agent_card(card: dict) -> bytes:
    """AgentCard を JCS（rfc-8785）で正規化する（identity-authz.md §1.3）。

    署名対象は `signatures` フィールドを除いた AgentCard（§1.2.1）。protobuf 由来の
    default 値プロパティ（空文字列・false・空配列・空 object）も正規化前に取り除く
    （A2A 1.0 spec §8.4.3 step 3）。
    """
    stripped = {k: v for k, v in card.items() if k != "signatures"}
    stripped = _strip_default_values(stripped)
    return rfc8785.dumps(stripped)


def _strip_default_values(value: Any) -> Any:
    if isinstance(value, dict):
        result = {}
        for k, v in value.items():
            if v is None or v is False or v == "" or v == [] or v == {}:
                continue
            result[k] = _strip_default_values(v)
        return result
    if isinstance(value, list):
        return [_strip_default_values(v) for v in value]
    return value


# ---------------------------------------------------------------------------
# JWS（rfc-7515, MAY）
# ---------------------------------------------------------------------------


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def sign_agent_card(card: dict, *, private_key_pem: str, kid: str, jku: str) -> dict:
    """AgentCard に ES256 JWS 署名を付与した新しい dict を返す（identity-authz.md §1.2）。

    署名は JWS Compact Serialization で計算し、protected header と signature を
    `signatures[0]` に detached 形式（payload は JCS 正規化 AgentCard から再計算できるため
    JSON 内には格納しない）で格納する。
    """
    payload = canonicalize_agent_card(card)
    key = ECKey.import_key(private_key_pem)
    protected = {"alg": "ES256", "kid": kid, "jku": jku}
    compact = jws.serialize_compact(protected, payload, key)
    protected_b64, _payload_b64, signature_b64 = compact.split(".")
    signed = dict(card)
    signed["signatures"] = [{"protected": protected_b64, "signature": signature_b64}]
    return signed


def verify_agent_card_signature(card: dict, *, public_key_pem: str) -> bool:
    """AgentCard の `signatures[0]` を検証する（A2A 1.0 spec §8.4.3 の MUST 手順）。

    `card` は外部 agent から受け取る非信頼入力であり、`signatures` の形が
    `{protected, signature}` object の非空 list であることを一切仮定できない
    （list でなく dict / 要素が非 dict 文字列 / 必須キー欠落等の malformed input が来うる）。
    構造の取り出しから JWS 検証までを単一の try で囲み、KeyError / TypeError / IndexError を
    含むあらゆる例外を検証失敗として畳む（fail-closed。呼び出し側が期待する「検証鍵が無い /
    署名不一致はすべて False」という契約を、構造不正の場合にも一貫させる）。

    Returns:
        署名が正しく、かつ payload（JCS 正規化した signatures 除外後の AgentCard）と
        一致する場合に True。それ以外（構造不正・鍵不一致・署名不一致）はすべて False。
    """
    signatures = card.get("signatures")
    if not signatures:
        return False
    try:
        sig = signatures[0]
        payload = canonicalize_agent_card(card)
        compact = f"{sig['protected']}.{_b64url_encode(payload)}.{sig['signature']}"
        key = ECKey.import_key(public_key_pem)
        result = jws.deserialize_compact(compact, key)
    except Exception:
        return False
    return result.payload == payload
