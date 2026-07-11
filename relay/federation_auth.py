"""federation レーンの Relay-JWS 署名・検証。

origin/destination を鍵 fingerprint で表現した per-request detached JWS（ES256）で、
federation サーフェス（`/federation/*`）への全リクエストを認証する。

```
Authorization:        Relay-JWS <protected_b64>..<signature_b64>
X-Relay-Fed-Payload:  <base64url(JCS(payload))>

protected = {alg: "ES256", kid: <送信側 fingerprint>, typ: "relay-fed-req+jws"}
payload   = {v, method, path, origin, destination, content_sha256, ts, nonce}
```

detached JWS の payload（署名対象そのもの）は Authorization ヘッダーの compact 表記には
含めず、`X-Relay-Fed-Payload` ヘッダーで別送する（JWS Compact Serialization の detached
content 慣習、RFC 7515 Appendix F。federation_peers.sign_detached/verify_detached と同じ
「payload は署名対象 dict から JCS で再計算する」パターンをそのまま踏襲）。

payload に含まれる method / path / content_sha256 は署名対象であって実リクエストの正しさを
それ自体では保証しないため（正しい署名の付いた payload を別の method/path のリクエストに
使い回す攻撃を防げない）、verifier は実際の HTTP リクエストの method / path / body digest と
payload の値を突き合わせて一致を要求する（AWS SigV4 の canonical request 再検証と同型）。

local レーンの `relay.identity`（Bearer token authN）とは無関係に並置する。既存 identity.py
は無改変。
"""
from __future__ import annotations

import base64
import hashlib
import json
import secrets
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Awaitable, Callable

import rfc8785
from starlette.requests import Request
from starlette.responses import JSONResponse

from relay import federation_peers
from relay.config import Settings
from relay.errors import PAYLOAD_TOO_LARGE, error_response
from relay.ratelimit import RateLimiter

REQ_SIG_TYP = "relay-fed-req+jws"

AUTHORIZATION_HEADER = "Authorization"
PAYLOAD_HEADER = "X-Relay-Fed-Payload"

DEFAULT_NONCE_TTL_SECONDS = 600
DEFAULT_MAX_NONCES_PER_PEER = 10000
# federation リクエストの per-peer rate limit（既定 50 req/s）。local レーンの publish
# rate limit（既定 100/s）より控えめに取る。peer は非完全信頼の相手であるため。
DEFAULT_PEER_REQUEST_RATE_LIMIT_PER_SECOND = 50


@dataclass(frozen=True)
class PeerIdentity:
    """federation リクエストの認証済み送信元。

    local の `relay.identity.Identity` とは意図的に別型にする。federation サーフェス限定の
    `require_federation_authn` だけがこの型を積むため、local レーン向けハンドラが誤って
    federation の識別子を受け取る、あるいはその逆のレーン取り違えを型で防ぐ。
    """

    handle: str
    fingerprint: str


class FederationAuthenticationError(Exception):
    """Relay-JWS 検証失敗。呼び出し側は 401 を返す（理由は区別せず一様に扱う）。

    `ts_skew_seconds` は時計ずれが原因の失敗のときのみセットする。呼び出し側はこれが
    非 None なら応答 body に `ts_skew` とサーバー時刻を含め、送信側の自動補正リトライを
    助ける（時計ずれ以外の失敗と区別しても存在秘匿上の問題はない。peer は招待 redemption
    で既に身元を確立済みの相手であり、redemption の一律 404 とは前提が異なる）。
    """

    def __init__(self, message: str, *, ts_skew_seconds: int | None = None) -> None:
        super().__init__(message)
        self.ts_skew_seconds = ts_skew_seconds


class FederationPayloadTooLargeError(Exception):
    """request body が `settings.max_payload_bytes` を超えている。呼び出し側は 413 を返す。

    detached JWS の署名対象 payload には body 自体が含まれない（`content_sha256` のみを
    含む）ため、署名検証だけでは body サイズを制限できない。`FederationAuthenticationError`
    とは別の例外にして、呼び出し側が 401 ではなく 413 に変換できるようにする。
    """


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes:
    padded = value + "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(padded)


def _sha256_b64url(data: bytes) -> str:
    return _b64url_encode(hashlib.sha256(data).digest())


def _now_unix() -> int:
    return int(datetime.now(timezone.utc).timestamp())


# ---------------------------------------------------------------------------
# nonce cache
# ---------------------------------------------------------------------------


class NonceCache:
    """peer 別 in-memory TTL セット（origin fingerprint → 使用済み nonce の集合）。

    TTL 経過エントリはアクセス時に lazy purge する。per-peer 上限超過は evict でなく
    新規追加そのものを拒否する（DoS 防御を優先する）。再起動直後の ±ts_skew 秒窓に
    リプレイが残留する可能性は受容する（in-memory のため再起動で消える。二重配達自体の
    防止は受信側の envelope 冪等性で担保し、この認証レイヤーの責務としない）。
    """

    def __init__(
        self,
        ttl_seconds: float = DEFAULT_NONCE_TTL_SECONDS,
        max_per_peer: int = DEFAULT_MAX_NONCES_PER_PEER,
    ) -> None:
        self._ttl = ttl_seconds
        self._max_per_peer = max_per_peer
        self._lock = threading.Lock()
        self._store: dict[str, dict[str, float]] = {}

    def check_and_add(self, origin_fp: str, nonce: str) -> bool:
        """`nonce` が `origin_fp` にとって未使用なら記録して True を返す。

        既使用（リプレイ）または per-peer 上限超過で記録できない場合は False。
        """
        now = time.monotonic()
        with self._lock:
            bucket = self._store.setdefault(origin_fp, {})
            expired = [n for n, exp in bucket.items() if exp <= now]
            for n in expired:
                del bucket[n]
            if nonce in bucket:
                return False
            if len(bucket) >= self._max_per_peer:
                return False
            bucket[nonce] = now + self._ttl
            return True


# ---------------------------------------------------------------------------
# signer（egress / CLI 共用）
# ---------------------------------------------------------------------------


def sign_federation_request(
    *,
    method: str,
    path: str,
    body: bytes,
    origin_fp: str,
    destination_fp: str,
    private_key_pem: str,
    ts: int | None = None,
) -> dict[str, str]:
    """federation リクエストの署名ヘッダー一式を組み立てる（egress / CLI 共用）。

    毎リクエスト署名し直す（ts は呼び出し時点の現在時刻、`ts` 引数は時計ずれ自動補正
    リトライでの明示上書き用）。nonce は呼び出しごとに新規生成する。

    Returns:
        `{"Authorization": "Relay-JWS <protected>..<signature>",
          "X-Relay-Fed-Payload": <payload の base64url>}`。呼び出し側はこの 2 ヘッダーを
        リクエストにそのまま付与する。
    """
    payload: dict[str, Any] = {
        "typ": REQ_SIG_TYP,
        "v": 1,
        "method": method,
        "path": path,
        "origin": origin_fp,
        "destination": destination_fp,
        "content_sha256": _sha256_b64url(body),
        "ts": ts if ts is not None else _now_unix(),
        "nonce": secrets.token_urlsafe(16),
    }
    sig = federation_peers.sign_detached(payload, private_key_pem=private_key_pem, kid=origin_fp)
    payload_b64 = _b64url_encode(rfc8785.dumps(payload))
    return {
        AUTHORIZATION_HEADER: f"Relay-JWS {sig['protected']}..{sig['signature']}",
        PAYLOAD_HEADER: payload_b64,
    }


# ---------------------------------------------------------------------------
# verifier
# ---------------------------------------------------------------------------


def _parse_authorization_header(header: str) -> tuple[str, str]:
    """`Relay-JWS <protected>..<signature>` を `(protected_b64, signature_b64)` に分解する。"""
    scheme, _, rest = header.partition(" ")
    if scheme != "Relay-JWS" or not rest:
        raise FederationAuthenticationError("Relay-JWS 形式で指定してください")
    protected_b64, sep, signature_b64 = rest.partition("..")
    if not sep or not protected_b64 or not signature_b64:
        raise FederationAuthenticationError("Relay-JWS の形式が不正です")
    return protected_b64, signature_b64


def _decode_json_b64(value: str, *, what: str) -> Any:
    try:
        return json.loads(_b64url_decode(value))
    except Exception as exc:
        raise FederationAuthenticationError(f"{what} のデコードに失敗しました") from exc


async def verify_federation_request(
    request: Request,
    *,
    settings: Settings,
    nonce_cache: NonceCache,
    rate_limiter: RateLimiter,
) -> PeerIdentity:
    """federation リクエストを検証し、認証済み `PeerIdentity` を返す。

    検証手順（fail-closed）: kid → peers lookup（未知 / revoked は暗号検証せず即座に
    reject、pre-auth の暗号検証コストを未信頼リクエストに払わせない）→ per-peer rate
    limit → JWS 検証（payload 再計算 → compact 接合 → joserfc）→ destination == 自 fp
    → origin == kid（署名者と自称 origin の一致）→ 時計ずれ判定 → (origin, nonce) 未使用
    → content_sha256 == 受信 body digest → method / path が実リクエストと一致。

    Raises:
        FederationAuthenticationError: いずれかの検証に失敗した場合。呼び出し側
            （`require_federation_authn`）で理由を区別せず一様 401 に変換すること。
    """
    header = request.headers.get(AUTHORIZATION_HEADER)
    if not header:
        raise FederationAuthenticationError("Authorization ヘッダがありません")
    protected_b64, signature_b64 = _parse_authorization_header(header)
    protected = _decode_json_b64(protected_b64, what="protected header")
    if not isinstance(protected, dict):
        raise FederationAuthenticationError("protected header が不正です")

    if protected.get("typ") != REQ_SIG_TYP:
        raise FederationAuthenticationError("typ が不正です")

    kid = protected.get("kid")
    if not isinstance(kid, str) or not kid:
        raise FederationAuthenticationError("kid がありません")

    # 1. kid -> peers lookup。未知 / revoked は暗号検証せず即座に reject。
    peer = federation_peers.get_peer_by_fingerprint(settings.db_path, kid)
    if peer is None or peer["revoked_at"] is not None:
        raise FederationAuthenticationError("未知または失効した peer です")

    # 2. per-peer rate limit。
    allowed, _retry_after = rate_limiter.allow(kid)
    if not allowed:
        raise FederationAuthenticationError("rate limit を超過しました")

    # 3. payload 取得 + JWS 検証。
    payload_b64 = request.headers.get(PAYLOAD_HEADER)
    if not payload_b64:
        raise FederationAuthenticationError(f"{PAYLOAD_HEADER} ヘッダがありません")
    payload = _decode_json_b64(payload_b64, what="payload")
    if not isinstance(payload, dict):
        raise FederationAuthenticationError("payload が不正です")

    sig = {"protected": protected_b64, "signature": signature_b64}
    if not federation_peers.verify_detached(payload, sig, public_key=peer["key_jwk"]):
        raise FederationAuthenticationError("署名検証に失敗しました")

    # 4. destination == 自 fp。
    own_fingerprint = federation_peers.compute_fingerprint(
        federation_peers.public_jwk_from_pem(settings.jws_private_key_pem)
    )
    if payload.get("destination") != own_fingerprint:
        raise FederationAuthenticationError("destination が自 fingerprint と一致しません")

    # origin は kid と一致すること（署名者と自称 origin の一致。kid で引いた鍵と別の
    # origin を名乗る payload を許すと、後続処理が誤った origin を信頼しうる）。
    if payload.get("origin") != kid:
        raise FederationAuthenticationError("origin が kid と一致しません")

    # 5. 時計ずれ判定。
    ts = payload.get("ts")
    if not isinstance(ts, int):
        raise FederationAuthenticationError("ts が不正です")
    skew = abs(_now_unix() - ts)
    if skew > settings.federation_ts_skew_seconds:
        raise FederationAuthenticationError(
            "時計ずれが許容範囲を超えています",
            ts_skew_seconds=settings.federation_ts_skew_seconds,
        )

    # 6. nonce リプレイ拒否。
    nonce = payload.get("nonce")
    if not isinstance(nonce, str) or not nonce:
        raise FederationAuthenticationError("nonce が不正です")
    if not nonce_cache.check_and_add(kid, nonce):
        raise FederationAuthenticationError("nonce が既に使用されています（リプレイの可能性）")

    # 7. content_sha256 == 受信 body digest。settings.max_payload_bytes を超える body は
    #    拒否する（relay.federation.MAX_REDEEM_BODY_BYTES と同型の Content-Length 事前
    #    チェック + 読了後の再検証パターン。detached JWS の payload に body 自体は含まれず
    #    署名検証だけでは body サイズを制限できないため、ここで別途上限を課す）。
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > settings.max_payload_bytes:
                raise FederationPayloadTooLargeError(
                    f"リクエストボディが上限（{settings.max_payload_bytes} bytes）を超えています"
                )
        except ValueError:
            pass  # 不正な Content-Length は実読み込み側の検証に委ねる
    body_bytes = await request.body()
    if len(body_bytes) > settings.max_payload_bytes:
        raise FederationPayloadTooLargeError(
            f"リクエストボディが上限（{settings.max_payload_bytes} bytes）を超えています"
        )
    if payload.get("content_sha256") != _sha256_b64url(body_bytes):
        raise FederationAuthenticationError("content_sha256 が一致しません")

    # 8. method / path が実際のリクエストと一致すること（署名対象 payload の method/path
    #    を書き換えて別 endpoint への使い回しに転用する攻撃を防ぐ）。
    if payload.get("method") != request.method:
        raise FederationAuthenticationError("method が一致しません")
    if payload.get("path") != request.url.path:
        raise FederationAuthenticationError("path が一致しません")

    return PeerIdentity(handle=peer["handle"], fingerprint=kid)


# ---------------------------------------------------------------------------
# require_federation_authn デコレータ
# ---------------------------------------------------------------------------


def require_federation_authn(
    handler: Callable[..., Awaitable[Any]],
) -> Callable[..., Awaitable[Any]]:
    """Starlette endpoint 用デコレータ。検証済み `PeerIdentity` を `request.state.peer_identity` に積む。

    `request.app.state.settings` / `request.app.state.federation_nonce_cache` /
    `request.app.state.federation_request_rate_limiter` を参照する（呼び出し側 app が
    lifespan で用意すること）。local レーンの `relay.identity.require_authn` とは並置し、
    そちらは無改変のまま federation サーフェス（`/federation/*`）限定でこちらを使う。

    使い方::

        @require_federation_authn
        async def post_message(request: Request) -> Response:
            peer = request.state.peer_identity
            ...
    """

    @wraps(handler)
    async def wrapper(request: Request, *args: Any, **kwargs: Any) -> Any:
        settings: Settings = request.app.state.settings
        nonce_cache: NonceCache = request.app.state.federation_nonce_cache
        rate_limiter: RateLimiter = request.app.state.federation_request_rate_limiter
        try:
            peer_identity = await verify_federation_request(
                request, settings=settings, nonce_cache=nonce_cache, rate_limiter=rate_limiter
            )
        except FederationPayloadTooLargeError as exc:
            return error_response(413, PAYLOAD_TOO_LARGE, str(exc))
        except FederationAuthenticationError as exc:
            from relay import observability

            observability.record_event(
                request.app.state, "federation_authn_failed", level="warning", reason=str(exc)
            )
            body: dict[str, Any] = {"error": str(exc)}
            if exc.ts_skew_seconds is not None:
                body["ts_skew"] = exc.ts_skew_seconds
                body["server_time"] = _now_unix()
            return JSONResponse(body, status_code=401)
        request.state.peer_identity = peer_identity
        return await handler(request, *args, **kwargs)

    return wrapper
