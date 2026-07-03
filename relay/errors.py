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
SUBSCRIBER_MISMATCH = "SubscriberMismatchError"
OUTBOX_UNAVAILABLE = "OutboxUnavailableError"


def error_response(
    status_code: int, code: str, message: str, *, details: list[dict[str, Any]] | None = None
) -> JSONResponse:
    """`{code, message, details}` 形の JSON error envelope を返す。"""
    body: dict[str, Any] = {"code": code, "message": message}
    if details:
        body["details"] = details
    return JSONResponse(body, status_code=status_code)
