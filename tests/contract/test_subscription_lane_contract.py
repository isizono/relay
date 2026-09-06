"""subscription レーンの contract test（docs/design/relay-v2-wire-api.md §5）。

`relay_sdk.http` の公開関数（dispatcher / `Subscription` が実際に使う唯一のリクエスト
構築経路）を実 relay（`tests/contract/conftest.py` の `LiveServer`）に対して呼び、
応答が wire-api.md の記載形状（必須フィールド・型・status code）と一致することを検証する。
"""
from __future__ import annotations

import json
import time

import httpx

from relay_sdk.http import (
    delete_subscription,
    open_sse,
    post_ack,
    post_publish,
    post_subscription,
    put_lease,
)


def _read_first_notification_frame(resp: httpx.Response, timeout: float) -> tuple[int, dict]:
    """`event: notification` frame を 1 件読み、(`id:` 行の int, `data:` の JSON dict) を返す。

    sse_starlette の `ServerSentEvent.encode()` は同一 frame 内で `id:` を `data:` より
    先に書く（`site-packages/sse_starlette/event.py`）。直近に見た `id:` を次の `data:` と
    組にすればよい。`: keepalive` コメント行は `id:` / `data:` のどちらも持たないため、
    この走査では自然に読み飛ばされる。
    """
    deadline = time.time() + timeout
    pending_id: int | None = None
    for line in resp.iter_lines():
        if line.startswith("id:"):
            pending_id = int(line[len("id:") :].strip())
        elif line.startswith("data:"):
            payload = json.loads(line[len("data:") :].strip())
            if pending_id is not None:
                return pending_id, payload
        if time.time() > deadline:
            break
    raise AssertionError("notification frame が timeout 内に観測できませんでした")


class TestSubscribeContract:
    """`POST /subscriptions`（wire-api.md §5.1）。"""

    def test_returns_subscription_id_and_lease_expires_at(self, sdk_client_factory):
        client = sdk_client_factory("tok-a")
        result = post_subscription(client, subscriber="agent-a", labels=["topic:contract"])
        assert client.last_response.status_code == 201
        assert isinstance(result["subscription_id"], str) and result["subscription_id"]
        assert isinstance(result["lease_expires_at"], str) and result["lease_expires_at"]


class TestLeaseRenewContract:
    """`PUT /subscriptions/{subscription_id}/lease`（wire-api.md §5.3）。"""

    def test_returns_lease_expires_at(self, sdk_client_factory):
        client = sdk_client_factory("tok-a")
        sub = post_subscription(client, subscriber="agent-a", labels=["topic:contract"])
        result = put_lease(client, subscription_id=sub["subscription_id"], lease_ttl=60)
        assert client.last_response.status_code == 200
        assert isinstance(result["lease_expires_at"], str) and result["lease_expires_at"]


class TestPublishContract:
    """`POST /publish`（subscription レーン publish、wire-api.md §5.4）。"""

    def test_returns_publish_id_and_matched_subscriptions(self, sdk_client_factory):
        client = sdk_client_factory("tok-a")
        result = post_publish(
            client,
            ref={"type": "note", "id": 1},
            labels=["topic:contract"],
            title=None,
            idempotency_key="contract-publish-1",
        )
        assert client.last_response.status_code == 202
        assert isinstance(result["publish_id"], int)
        assert isinstance(result["matched_subscriptions"], int)


class TestAckContract:
    """`POST /subscriptions/{subscription_id}/ack`（wire-api.md §5.6）。"""

    def test_returns_200(self, sdk_client_factory):
        publisher = sdk_client_factory("tok-a")
        subscriber = sdk_client_factory("tok-b")
        sub = post_subscription(subscriber, subscriber="agent-b", labels=["topic:ack-contract"])
        published = post_publish(
            publisher,
            ref={"type": "note", "id": 2},
            labels=["topic:ack-contract"],
            title=None,
            idempotency_key="contract-ack-1",
        )
        post_ack(
            subscriber,
            subscription_id=sub["subscription_id"],
            up_to_publish_id=published["publish_id"],
        )
        assert subscriber.last_response.status_code == 200


class TestUnsubscribeContract:
    """`DELETE /subscriptions/{subscription_id}`（wire-api.md §5.3）。"""

    def test_returns_204(self, sdk_client_factory):
        client = sdk_client_factory("tok-a")
        sub = post_subscription(client, subscriber="agent-a", labels=["topic:unsub-contract"])
        delete_subscription(client, subscription_id=sub["subscription_id"])
        assert client.last_response.status_code == 204


class TestSseEventContract:
    """`GET /events`（wire-api.md §5.5）。"""

    def test_id_line_matches_payload_publish_id(self, sdk_client_factory):
        """SSE の `id:` 行と `data:` payload の `publish_id` が一致することを検証する
        （wire-api.md §0.1・§4・§5.5 が明記する 3 者一致のうち id: ↔ payload の対応）。
        """
        subscriber = sdk_client_factory("tok-b")
        publisher = sdk_client_factory("tok-a")
        sub = post_subscription(subscriber, subscriber="agent-b", labels=["topic:sse-contract"])

        with open_sse(
            subscriber, subscription_ids=[sub["subscription_id"]], read_timeout=5.0
        ) as resp:
            assert resp.status_code == 200
            assert resp.headers["content-type"].startswith("text/event-stream")

            published = post_publish(
                publisher,
                ref={"type": "note", "id": 3},
                labels=["topic:sse-contract"],
                title=None,
                idempotency_key="contract-sse-1",
            )

            frame_id, payload = _read_first_notification_frame(resp, timeout=5.0)

        assert frame_id == published["publish_id"]
        assert payload["publish_id"] == published["publish_id"]
        assert payload["delivery_target"] == f"sub:{sub['subscription_id']}"
        assert payload["ref"] == {"type": "note", "id": 3}
        assert payload["labels"] == ["topic:sse-contract"]
