"""relay v2 の error envelope。

A2A 1.0 spec Section 3.3.2 相当の共通構造（`code` / `message` / `details[]`、
すべて string / MUST）で統一する。`code` には A2A spec 予約の 8 種、または以下の relay
固有 error_code を用いる。HTTP status code は各 endpoint の仕様（`relay-v2-wire-api.md`
§8 status code 規約）に従う。

認可エラー（`MembershipRequiredError` 等）は「リソース存在を露呈しない」原則
（identity-authz.md §2.1 の A2A 1.0 spec §7.5 引用）と衡量が必要な場面がある。
`relay-v2-wire-api.md` §3.2 / §8 が明示的に区別している箇所（stream への投函が
write member でない場合は `403`）はその通りに実装し、`relay-v2-wire-api.md` §5.6 /
§5.7 のように「存在も権限も同一の `404` に隠す」と明示されている箇所はそちらに従う。
"""
from __future__ import annotations

from typing import Any

from starlette.responses import JSONResponse

# relay 固有 error_code（`relay-v2-wire-api.md` の該当 endpoint 仕様に対応する最小集合）。
STREAM_NOT_FOUND = "StreamNotFoundError"
STREAM_ALREADY_EXISTS = "StreamAlreadyExistsError"
STREAM_GONE = "StreamGoneError"
SUBSCRIPTION_NOT_FOUND = "SubscriptionNotFoundError"
SUBSCRIPTION_GONE = "SubscriptionGoneError"
MEMBERSHIP_REQUIRED = "MembershipRequiredError"
LABEL_VALIDATION = "LabelValidationError"
INVALID_REQUEST = "InvalidRequestError"
PAYLOAD_TOO_LARGE = "PayloadTooLargeError"
RATE_LIMIT_EXCEEDED = "RateLimitExceededError"
RESOURCE_LIMIT_EXCEEDED = "ResourceLimitExceededError"
SUBSCRIBER_MISMATCH = "SubscriberMismatchError"
OUTBOX_UNAVAILABLE = "OutboxUnavailableError"
INVITE_NOT_FOUND = "InviteNotFoundError"
PEER_INVITE_NOT_FOUND = "PeerInviteNotFoundError"
PEER_ALREADY_REGISTERED = "PeerAlreadyRegisteredError"
FEDERATION_DISABLED = "FederationDisabledError"
FEDERATION_SIGNATURE_INVALID = "FederationSignatureInvalidError"


class ResourceLimitExceeded(Exception):
    """registry の資源上限（総数 / identity 単位）を超過して作成が拒否されたことを表す。

    HTTP 層では `resource_limit_response` で 429 + `ResourceLimitExceededError` に変換する。
    `scope` は "total"（registry 全体の上限）または "per_identity"（1 identity あたりの
    上限）で、応答メッセージの出し分けに使う。registry のロック下で送出されるため、
    呼び出し側 handler で必ず捕捉すること（未捕捉のまま Starlette に抜けると 500 になる）。
    """

    def __init__(self, scope: str) -> None:
        super().__init__(scope)
        self.scope = scope


def resource_limit_response(resource: str, scope: str) -> "JSONResponse":
    """資源上限超過（`ResourceLimitExceeded`）を 429 error envelope に変換する。"""
    dimension = "総数" if scope == "total" else "1 identity あたりの数"
    return error_response(
        429,
        RESOURCE_LIMIT_EXCEEDED,
        f"{resource} の{dimension}が上限に達しています",
    )


def error_response(
    status_code: int, code: str, message: str, *, details: list[dict[str, Any]] | None = None
) -> JSONResponse:
    """`{code, message, details}` 形の JSON error envelope を返す。"""
    body: dict[str, Any] = {"code": code, "message": message}
    if details:
        body["details"] = details
    return JSONResponse(body, status_code=status_code)
