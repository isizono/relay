"""federation サーフェス（`/federation/*`）。

`POST /federation/peers/redeem` で招待ベースの鍵ピン留めを実装する。無認証だが、招待 token の一回性
（atomic UPDATE の rowcount 判定、`relay.federation_peers.redeem_peer_invite`）と
自己署名（鍵所持証明）+ チャネルバインディング（`a_fp` 照合）が authN 不在を補う統制。

`POST /federation/peers/enc-key` は既に pin 済みの peer へ envelope 暗号化用公開鍵
（ECDH-ES, P-256）を追加/更新する。redeem と異なりこちらは `require_federation_authn`
で保護する（既存 peer 関係の上での属性更新であり、招待側の未確立トラストを前提にしない）。

relay 自身の federation マシン鍵は `Settings.jws_private_key_pem`（AgentCard 署名用と共用）
をそのまま使う。未設定の relay では federation を無効化し、endpoint は 503 で拒否する
（fail-closed）。envelope 暗号化鍵（`Settings.jwe_private_key_pem`）はこれとは別の任意
設定で、未設定でも federation 自体は無効化しない（暗号化なしの互換動作にフォールバック
する、`relay.federation_egress` 参照）。
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
from relay.federation_auth import PeerIdentity, require_federation_authn
from relay.federation_peers import PeerAlreadyRegisteredError

# 無認証面の DoS ガード。JWK（EC P-256 公開鍵）+ detached JWS 2 個分の body でも
# 十分な余裕を持つ上限（invitations.redeem と同一値）。
MAX_REDEEM_BODY_BYTES = 4096

REDEEM_SIG_TYP = "relay-fed-redeem"
REDEEM_RESP_SIG_TYP = "relay-fed-redeem-resp"
REGISTER_ENC_KEY_RESP_SIG_TYP = "relay-fed-enc-key-resp"


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
    # card.enc_key（envelope 暗号化用公開鍵）は任意項目。招待側がその時点で暗号化鍵を
    # 持っていれば redeem の 1 往復で pin できる（無くても redeem 自体は成立する）。
    enc_key_jwk = card.get("enc_key")
    if enc_key_jwk is not None:
        try:
            federation_peers.validate_enc_key_jwk(enc_key_jwk)
        except ValueError as exc:
            return error_response(400, INVALID_REQUEST, f"card.enc_key が不正です: {exc}")

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
        "card": card,
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
            db_path,
            handle=handle,
            fingerprint=peer_fingerprint,
            key_jwk=key_jwk,
            locator=locator,
            enc_key_jwk=enc_key_jwk,
        )
    except PeerAlreadyRegisteredError:
        return error_response(
            400,
            PEER_ALREADY_REGISTERED,
            "同じ handle または鍵で pin 済みの peer が既に存在します",
        )
    federation_peers.mark_peer_invite_redeemed(db_path, invitation_id=invitation_id, peer_id=peer_id)

    # 9. 応答（署名付き）。card.enc_key は自分が暗号化鍵を設定していれば含める（任意項目）。
    resp_card: dict[str, Any] = {
        "key": federation_peers.public_jwk_from_pem(settings.jws_private_key_pem),
        "locator": settings.federation_base_url,
    }
    if settings.jwe_private_key_pem:
        resp_card["enc_key"] = federation_peers.public_enc_jwk_from_pem(
            settings.jwe_private_key_pem
        )
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


@require_federation_authn
async def register_enc_key(request: Request) -> Response:
    """`POST /federation/peers/enc-key`: 既に pin 済みの peer へ envelope 暗号化用公開鍵を
    追加/更新する。

    招待・redeem フローをやり直さない鍵の再 pin 経路。`redeem_peer` と異なりこの endpoint
    自体は `require_federation_authn` で保護する（相手は招待未消費の未知者ではなく、既に
    確立した peer 関係である前提のため）。リクエストが Relay-JWS で認証される時点で
    「この pin 済み peer が自分自身の enc_key を更新した」ことは保証されるので、
    redeem のような追加の鍵所持証明・チャネルバインディングは要求しない（自分の属性を
    自分が更新するだけの操作であり、redeem の「未確立トラストの橋渡し」とは前提が違う）。

    応答は呼び出し側 peer の enc_key をエコーバックする（自分が設定していれば）。これにより
    A → B の 1 回の呼び出しで A の鍵が B に pin され、B の鍵も同じ往復で A に返る
    （双方向の鍵交換が 1 ラウンドトリップで完了する）。
    """
    peer: PeerIdentity = request.state.peer_identity
    settings: Settings = request.app.state.settings

    raw = await request.body()
    try:
        body = json.loads(raw) if raw else None
    except Exception:
        return error_response(400, INVALID_REQUEST, "リクエストボディが不正な JSON です")
    if not isinstance(body, dict):
        return error_response(
            400, INVALID_REQUEST, "リクエストボディは JSON object でなければなりません"
        )

    enc_key_jwk = body.get("enc_key")
    try:
        federation_peers.validate_enc_key_jwk(enc_key_jwk)
    except ValueError as exc:
        return error_response(400, INVALID_REQUEST, f"enc_key が不正です: {exc}")

    federation_peers.set_peer_enc_key(
        settings.db_path, fingerprint=peer.fingerprint, enc_key_jwk=enc_key_jwk
    )
    observability.record_event(
        request.app.state, "peer_enc_key_registered", level="info", handle=peer.handle
    )

    resp: dict[str, Any] = {"handle": peer.handle}
    if settings.jwe_private_key_pem:
        resp["enc_key"] = federation_peers.public_enc_jwk_from_pem(settings.jwe_private_key_pem)

    # 応答に detached JWS 署名を付ける。中間者が無署名の enc_key を差し替えて相手鍵を
    # 詐称できないよう、呼び出し側は既に pin 済みの key_jwk（redeem 時に確立済み）で
    # 検証してから pin する（redeem_peer の応答署名パターンと同型）。
    resp_sig_payload: dict[str, Any] = {
        "typ": REGISTER_ENC_KEY_RESP_SIG_TYP,
        "handle": peer.handle,
        "enc_key": resp.get("enc_key"),
    }
    resp["sig"] = federation_peers.sign_detached(
        resp_sig_payload, private_key_pem=settings.jws_private_key_pem
    )
    return JSONResponse(resp, status_code=200)


routes: list[Route] = [
    Route("/federation/peers/redeem", redeem_peer, methods=["POST"]),
    Route("/federation/peers/enc-key", register_enc_key, methods=["POST"]),
]
