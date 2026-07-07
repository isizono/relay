"""招待 token redeem endpoint（`POST /invitations/redeem`、無認証）。

relay-v2-identity-authz.md §1.5.1 の「全 endpoint authN MUST」の例外として、`GET /` /
`GET /.well-known/agent-card.json` に次ぐ3つ目の無認証 route。招待 token の一回性
（atomic UPDATE の rowcount 判定、`relay.credentials.redeem_invite`）が authN 不在を
補う統制であり、未知 / 失効 / 既 redeem を一律 404 で返し token の存在を秘匿する。
"""
from __future__ import annotations

import json
from datetime import datetime, timezone

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from relay import credentials, db, observability
from relay.config import Settings
from relay.errors import INVALID_REQUEST, INVITE_NOT_FOUND, RATE_LIMIT_EXCEEDED, error_response

# 無認証面の DoS ガード。invite token は約26byte のため十分な余裕を持つ上限。
MAX_REDEEM_BODY_BYTES = 4096


def _now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


async def redeem(request: Request) -> Response:
    # 1. rate check: RateLimiter.allow は (allowed, retry_after) の2要素タプルを返す。
    #    if not limiter.allow(host): と真偽値扱いすると2要素タプルが常に truthy になり
    #    rate limit が黙って無効化されるため、必ず unpack する。request.client が None に
    #    なる ASGI 構成があるため IP キーは None ガードする。
    host = request.client.host if request.client else "127.0.0.1"
    allowed, retry_after = request.app.state.redeem_rate_limiter.allow(host)
    if not allowed:
        response = error_response(
            429, RATE_LIMIT_EXCEEDED, "招待 redeem のレート制限を超過しました"
        )
        response.headers["Retry-After"] = str(retry_after)
        return response

    # 2. body cap: Content-Length header が上限超過なら本文を読まず 400。ヘッダ欠落の
    # chunked リクエストは request.body() で全チャンクを buffer し切ってから長さを見る
    # ため早期 abort はしない（localhost の同一 UID DoS は fate-sharing 受容）。
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
    if not isinstance(invite_token, str) or not invite_token:
        return error_response(400, INVALID_REQUEST, "invite_token は必須の非空文字列です")

    # 4. redeem: 単一トランザクションで atomic に消費する（credentials.redeem_invite）。
    settings: Settings = request.app.state.settings
    now = _now_iso()
    conn = db.get_connection(settings.db_path)
    try:
        result = credentials.redeem_invite(conn, invite_token, now)
        if result is None:
            # 既 redeem token の再送は漏洩→第三者 redeem の検知信号として warning ログに
            # 残す。未知 / 失効はこのログを出さない（HTTP 応答は区別しない、存在秘匿）。
            if credentials.was_already_redeemed(conn, invite_token):
                observability.record_event(
                    request.app.state, "invite_reredeem", level="warning"
                )
            return error_response(
                404, INVITE_NOT_FOUND, "招待 URL が無効か、既に使用されています"
            )
        bearer_token, identity, expires_at = result
    finally:
        conn.close()

    # 5. in-process 反映: frozen dataclass 内の可変 dict を in-place 更新。
    settings.auth_tokens[bearer_token] = identity

    # 6. 応答（token 本体はログに出さない）。
    observability.record_event(
        request.app.state, "invite_redeemed", level="info", identity=identity
    )
    return JSONResponse(
        {"bearer_token": bearer_token, "identity": identity, "expires_at": expires_at},
        status_code=200,
    )


routes: list[Route] = [
    Route("/invitations/redeem", redeem, methods=["POST"]),
]
