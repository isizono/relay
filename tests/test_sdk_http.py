"""relay_sdk.http（protocol 層の request 組み立て + §4.4 の例外分類）の単体テスト。

status code → 例外の翻訳を httpx.MockTransport で精密に検証する。
"""
from __future__ import annotations

import httpx
import pytest

from relay_sdk.errors import PermanentError, RelayProtocolError, TransientError
from relay_sdk.http import (
    delete_subscription,
    post_ack,
    post_publish,
    post_subscription,
    put_lease,
    raise_for_relay_status,
)
from relay_sdk.http.request import _request


def _client(handler) -> httpx.Client:
    return httpx.Client(base_url="http://relay.test", transport=httpx.MockTransport(handler))


class TestRequestBodies:
    def test_post_publish_body_shape(self):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            captured["json"] = httpx.Response(200).json if False else None
            import json as _json

            captured["body"] = _json.loads(request.content)
            return httpx.Response(202, json={"publish_id": 5, "matched_subscriptions": 2})

        with _client(handler) as c:
            result = post_publish(
                c,
                ref={"type": "decision", "id": 9},
                labels=["a", "b"],
                title="hi",
                idempotency_key="42",
            )
        assert captured["url"].endswith("/publish")
        assert captured["body"] == {
            "ref": {"type": "decision", "id": 9},
            "labels": ["a", "b"],
            "idempotency_key": "42",
            "title": "hi",
        }
        assert result == {"publish_id": 5, "matched_subscriptions": 2}

    def test_post_subscription_includes_delivery_options(self):
        captured = {}

        def handler(request: httpx.Request) -> httpx.Response:
            import json as _json

            captured["body"] = _json.loads(request.content)
            return httpx.Response(201, json={"subscription_id": "s1", "lease_expires_at": "t"})

        with _client(handler) as c:
            post_subscription(
                c, subscriber="me", labels=["x"], lease_ttl=120, retain_seconds=600
            )
        assert captured["body"]["subscriber"] == "me"
        assert captured["body"]["lease_ttl"] == 120
        assert captured["body"]["delivery_options"] == {"retain_seconds": 600}

    def test_put_lease_and_ack_and_delete_paths(self):
        seen = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append((request.method, request.url.path))
            return httpx.Response(200, json={"lease_expires_at": "t"})

        with _client(handler) as c:
            put_lease(c, subscription_id="s1", lease_ttl=60)
            post_ack(c, subscription_id="s1", up_to_publish_id=10)
            delete_subscription(c, subscription_id="s1")
        assert ("PUT", "/subscriptions/s1/lease") in seen
        assert ("POST", "/subscriptions/s1/ack") in seen
        assert ("DELETE", "/subscriptions/s1") in seen


class TestErrorClassification:
    @pytest.mark.parametrize("status", [400, 403, 404])
    def test_publish_4xx_is_protocol_error(self, status):
        def handler(request):
            return httpx.Response(status, json={"code": "InvalidRequestError", "message": "no"})

        with _client(handler) as c:
            with pytest.raises(RelayProtocolError) as exc:
                post_publish(c, ref={"type": "d", "id": 1}, labels=["a"], title=None, idempotency_key="1")
        assert exc.value.status_code == status
        assert exc.value.code == "InvalidRequestError"

    def test_publish_429_is_transient_with_retry_after(self):
        def handler(request):
            return httpx.Response(
                429, headers={"Retry-After": "3"}, json={"code": "RateLimitExceededError", "message": "x"}
            )

        with _client(handler) as c:
            with pytest.raises(TransientError) as exc:
                post_publish(c, ref={"type": "d", "id": 1}, labels=["a"], title=None, idempotency_key="1")
        assert exc.value.status_code == 429
        assert exc.value.retry_after == 3.0

    def test_publish_5xx_is_transient(self):
        def handler(request):
            return httpx.Response(503, json={"code": "OutboxUnavailableError", "message": "x"})

        with _client(handler) as c:
            with pytest.raises(TransientError) as exc:
                post_publish(c, ref={"type": "d", "id": 1}, labels=["a"], title=None, idempotency_key="1")
        assert exc.value.status_code == 503

    @pytest.mark.parametrize("status", [404, 410])
    def test_subscription_scoped_404_410_is_permanent(self, status):
        def handler(request):
            return httpx.Response(status, json={"code": "SubscriptionNotFoundError", "message": "x"})

        with _client(handler) as c:
            with pytest.raises(PermanentError) as exc:
                post_ack(c, subscription_id="s1", up_to_publish_id=1)
        assert exc.value.status_code == status

    def test_subscription_op_400_is_protocol_error(self):
        # ack への 400（scoped でも 4xx-非404/410 は caller 起因 → RelayProtocolError）。
        def handler(request):
            return httpx.Response(400, json={"code": "InvalidRequestError", "message": "x"})

        with _client(handler) as c:
            with pytest.raises(RelayProtocolError):
                post_ack(c, subscription_id="s1", up_to_publish_id=1)

    def test_connection_error_is_transient(self):
        def handler(request):
            raise httpx.ConnectError("refused")

        with _client(handler) as c:
            with pytest.raises(TransientError):
                _request(c, "POST", "/publish", json={})

    def test_timeout_is_transient(self):
        def handler(request):
            raise httpx.ReadTimeout("slow")

        with _client(handler) as c:
            with pytest.raises(TransientError):
                _request(c, "GET", "/x")


class TestRaiseForRelayStatus:
    def test_2xx_no_raise(self):
        assert raise_for_relay_status(httpx.Response(200)) is None

    def test_publish_404_not_permanent(self):
        with pytest.raises(RelayProtocolError):
            raise_for_relay_status(httpx.Response(404), subscription_scoped=False)

    def test_scoped_404_permanent(self):
        with pytest.raises(PermanentError):
            raise_for_relay_status(httpx.Response(404), subscription_scoped=True)
