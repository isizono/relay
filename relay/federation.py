"""federation サーフェス（`/federation/*`）。

`POST /federation/peers/redeem` で招待ベースの鍵ピン留めを実装する。無認証だが、招待 token の一回性
（atomic UPDATE の rowcount 判定、`relay.federation_peers.redeem_peer_invite`）と
自己署名（鍵所持証明）+ チャネルバインディング（`a_fp` 照合）が authN 不在を補う統制。

relay 自身の federation マシン鍵は `Settings.jws_private_key_pem`（AgentCard 署名用と共用）
をそのまま使う。未設定の relay では federation を無効化し、endpoint は 503 で拒否する
（fail-closed）。
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from relay import federation_net, federation_peers, observability
from relay.config import Settings
from relay.errors import (
    FEDERATION_DISABLED,
    FEDERATION_SIGNATURE_INVALID,
    INVALID_REQUEST,
    PEER_ALREADY_REGISTERED,
    PEER_INVITE_NOT_FOUND,
    RATE_LIMIT_EXCEEDED,
    error_response,
)
from relay.federation_peers import PeerAlreadyRegisteredError

# 無認証面の DoS ガード。JWK（EC P-256 公開鍵）+ detached JWS 2 個分の body でも
# 十分な余裕を持つ上限（invitations.redeem と同一値）。
MAX_REDEEM_BODY_BYTES = 4096

REDEEM_SIG_TYP = "relay-fed-redeem"
REDEEM_RESP_SIG_TYP = "relay-fed-redeem-resp"


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _federation_enabled(settings: Settings) -> bool:
    # base_url 未設定のまま redeem を許すと応答 card.locator が null になり、相手が
    # locator を持たない peer を pin してしまう（以後の連絡手段を失う）ため、鍵と
    # base_url の両方を federation 有効の前提条件とする。
    return bool(settings.jws_private_key_pem) and bool(settings.federation_base_url)


async def redeem_peer(request: Request) -> Response:
    settings: Settings = request.app.state.settings

    # 0. fail-closed: federation マシン鍵未設定なら機能自体を無効化する。
    if not _federation_enabled(settings):
        return error_response(
            503, FEDERATION_DISABLED, "federation 機能は無効です（federation マシン鍵未設定）"
        )

    # 1. rate limit（IP キー、invitations.redeem と同型）。
    host = request.client.host if request.client else "127.0.0.1"
    allowed, retry_after = request.app.state.federation_redeem_rate_limiter.allow(host)
    if not allowed:
        response = error_response(
            429, RATE_LIMIT_EXCEEDED, "peer redeem のレート制限を超過しました"
        )
        response.headers["Retry-After"] = str(retry_after)
        return response

    # 2. body cap（invitations.redeem と同型）。
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            too_large = int(content_length) > MAX_REDEEM_BODY_BYTES
        except ValueError:
            return error_response(400, INVALID_REQUEST, "Content-Length が不正です")
        if too_large:
            return error_response(400, INVALID_REQUEST, "リクエストボディが大きすぎます")

    body_bytes = await request.body()
    if len(body_bytes) > MAX_REDEEM_BODY_BYTES:
        return error_response(400, INVALID_REQUEST, "リクエストボディが大きすぎます")

    # 3. parse: fail-closed。
    try:
        body = json.loads(body_bytes)
    except Exception:
        return error_response(400, INVALID_REQUEST, "リクエストボディが不正な JSON です")
    if not isinstance(body, dict):
        return error_response(
            400, INVALID_REQUEST, "リクエストボディは JSON object でなければなりません"
        )

    invite_token = body.get("invite_token")
    ts = body.get("ts")
    a_fp = body.get("a_fp")
    card = body.get("card")
    sig = body.get("sig")
    if not isinstance(invite_token, str) or not invite_token:
        return error_response(400, INVALID_REQUEST, "invite_token は必須の非空文字列です")
    if not isinstance(ts, int):
        return error_response(400, INVALID_REQUEST, "ts は必須の整数（unix 秒）です")
    if not isinstance(a_fp, str) or not a_fp:
        return error_response(400, INVALID_REQUEST, "a_fp は必須の非空文字列です")
    if not isinstance(card, dict):
        return error_response(400, INVALID_REQUEST, "card は必須の JSON object です")
    key_jwk = card.get("key")
    locator = card.get("locator")
    if not isinstance(key_jwk, dict):
        return error_response(400, INVALID_REQUEST, "card.key は必須の JSON object です")
    if not isinstance(locator, str) or not locator:
        return error_response(400, INVALID_REQUEST, "card.locator は必須の非空文字列です")
    if not isinstance(sig, dict) or not isinstance(sig.get("protected"), str) or not isinstance(
        sig.get("signature"), str
    ):
        return error_response(
            400, INVALID_REQUEST, "sig は {protected, signature} の JSON object です"
        )

    # 4. token 消費: atomic に check-and-mark する（未知 / 失効 / 既 redeem は一律 404）。
    # token の一回性（redeemed_at）はこの時点で確定し、後続の署名検証に失敗しても
    # 巻き戻さない（招待 URL の誤所持・総当たりを token 一回性で必ず消尽させる）。
    # どの peer に紐づいたか（redeemed_peer_id）は pin 成功後に 8 で別途記録する。
    db_path = settings.db_path
    now = _now_iso()
    result = federation_peers.consume_peer_invite(db_path, invite_token, now)
    if result is None:
        if federation_peers.was_peer_invite_already_redeemed_db(db_path, invite_token):
            observability.record_event(
                request.app.state, "peer_invite_reredeem", level="warning"
            )
        return error_response(
            404, PEER_INVITE_NOT_FOUND, "招待 URL が無効か、既に使用されています"
        )
    invitation_id, handle = result

    # 5. 署名検証（鍵所持証明）。信頼の由来は token 一回性 + 発行時の人間意図であり、
    # この署名検証自体は鍵の来歴を保証しない。
    sig_payload: dict[str, Any] = {
        "typ": REDEEM_SIG_TYP,
        "token": invite_token,
        "ts": ts,
        "a_fp": a_fp,
    }
    if not federation_peers.verify_detached(sig_payload, sig, public_key=key_jwk):
        observability.record_event(
            request.app.state, "peer_redeem_signature_invalid", level="warning"
        )
        return error_response(
            401, FEDERATION_SIGNATURE_INVALID, "署名検証に失敗しました"
        )

    # 6. チャネルバインディング: a_fp（招待 URL 由来の自 fingerprint）が自鍵と一致すること。
    own_fingerprint = federation_peers.compute_fingerprint(
        federation_peers.public_jwk_from_pem(settings.jws_private_key_pem)
    )
    if a_fp != own_fingerprint:
        observability.record_event(
            request.app.state, "peer_redeem_channel_binding_mismatch", level="warning"
        )
        return error_response(
            401, FEDERATION_SIGNATURE_INVALID, "a_fp が自 fingerprint と一致しません"
        )

    # 7. locator の outbound ガード適用（保存前検証）。
    try:
        federation_net.validate_locator(
            locator, allow_private=settings.federation_allow_private_locators
        )
    except federation_net.LocatorRejected as exc:
        return error_response(400, INVALID_REQUEST, f"card.locator が拒否されました: {exc}")

    # 8. pin。
    peer_fingerprint = federation_peers.compute_fingerprint(key_jwk)
    try:
        peer_id = federation_peers.add_peer(
            db_path, handle=handle, fingerprint=peer_fingerprint, key_jwk=key_jwk, locator=locator
        )
    except PeerAlreadyRegisteredError:
        return error_response(
            400,
            PEER_ALREADY_REGISTERED,
            "同じ handle または鍵で pin 済みの peer が既に存在します",
        )
    federation_peers.mark_peer_invite_redeemed(db_path, invitation_id=invitation_id, peer_id=peer_id)

    # 9. 応答（署名付き）。
    resp_card = {
        "key": federation_peers.public_jwk_from_pem(settings.jws_private_key_pem),
        "locator": settings.federation_base_url,
    }
    resp_payload: dict[str, Any] = {
        "typ": REDEEM_RESP_SIG_TYP,
        "handle": handle,
        "card": resp_card,
    }
    resp_sig = federation_peers.sign_detached(
        resp_payload, private_key_pem=settings.jws_private_key_pem
    )
    observability.record_event(
        request.app.state, "peer_redeemed", level="info", handle=handle
    )
    return JSONResponse(
        {"handle": handle, "card": resp_card, "sig": resp_sig}, status_code=200
    )


routes: list[Route] = [
    Route("/federation/peers/redeem", redeem_peer, methods=["POST"]),
]
