"""主要 error envelope の contract test（docs/design/relay-v2-wire-api.md §8 状況分類、
`relay/errors.py` の `{code, message}` envelope）。

`relay_sdk.http` の公開関数を実 relay に対して呼び、relay が返す生レスポンス
（`client.last_response`、tests/contract/conftest.py 参照）が期待する status code /
error envelope 形状と一致することを検証する。
"""
from __future__ import annotations

import pytest

from relay.config import Settings
from relay_sdk.errors import PermanentError, RelayProtocolError, StreamAlreadyExistsError
from relay_sdk.http import post_stream, post_stream_message, post_subscription, put_lease


class TestSubscriptionOwnership404Contract:
    """`PUT /subscriptions/{id}/lease` の ownership 検証（wire-api.md §5.7）。

    存在しない subscription_id と、別 identity が所有する実在の subscription_id への
    操作が、呼び出し元にとって区別できない同一の 404 になることを検証する
    （§5.7「不一致は 404、存在しない subscription_id と同一応答」）。
    """

    def test_missing_and_non_owned_subscription_get_identical_404(self, sdk_client_factory):
        owner = sdk_client_factory("tok-a")
        other = sdk_client_factory("tok-b")
        sub = post_subscription(owner, subscriber="agent-a", labels=["topic:ownership"])

        with pytest.raises(PermanentError):
            put_lease(other, subscription_id=sub["subscription_id"], lease_ttl=60)
        non_owned_response = other.last_response

        with pytest.raises(PermanentError):
            put_lease(other, subscription_id="does-not-exist", lease_ttl=60)
        missing_response = other.last_response

        assert non_owned_response.status_code == missing_response.status_code == 404
        assert (
            non_owned_response.json()["code"]
            == missing_response.json()["code"]
            == "SubscriptionNotFoundError"
        )


class TestStreamDuplicate409Contract:
    """`POST /streams` の重複作成（wire-api.md §3.1）。"""

    def test_duplicate_name_same_creator_returns_409_envelope(self, sdk_client_factory):
        client = sdk_client_factory("tok-a")
        post_stream(client, name="dup-contract")

        with pytest.raises(StreamAlreadyExistsError):
            post_stream(client, name="dup-contract")

        assert client.last_response.status_code == 409
        body = client.last_response.json()
        assert body["code"] == "StreamAlreadyExistsError"
        assert isinstance(body["message"], str) and body["message"]


class TestPayloadTooLarge413Contract:
    """request body サイズ上限（wire-api.md §6.10）。

    `Settings.max_payload_bytes` を明示的に小さくして境界を決定的に検証する
    （tests/test_streams.py の `TestPostStreamMessagePayloadCapConfigurable` と同じ手法）。
    """

    def test_body_over_configured_cap_returns_413_envelope(
        self, tmp_path, make_relay_app_client_factory
    ):
        settings = Settings(
            db_path=str(tmp_path / "small.db"),
            server_log_path=str(tmp_path / "small.jsonl"),
            dispatcher_lock_path=str(tmp_path / "small.lock"),
            auth_tokens={"tok-a": "agent-a"},
            max_payload_bytes=50,
        )
        with make_relay_app_client_factory(settings) as make_client_fn:
            client = make_client_fn("tok-a")
            stream = post_stream(client, name="s1")

            with pytest.raises(RelayProtocolError) as exc_info:
                post_stream_message(client, stream_id=stream["stream_id"], body="x" * 100)

            assert exc_info.value.status_code == 413
            assert exc_info.value.code == "PayloadTooLargeError"
            assert client.last_response.status_code == 413
            assert client.last_response.json()["code"] == "PayloadTooLargeError"


class TestUnauthenticated401Contract:
    """認証ヘッダ欠落（`Authorization` なし）。

    `relay/identity.py` の `require_authn` は `relay/errors.py` の共通 error envelope
    （`{code, message}`）を経由せず、`{"error": <str>}` を直接返す。この形は
    wire-api.md にも identity-authz.md にも明記されていない実装依存の挙動であり、
    共通 envelope との不一致は本 PR の対象外の申し送り事項として残す
    （StructuredOutput の open_issues 参照）。
    """

    def test_missing_bearer_token_returns_401(self, sdk_client_factory):
        client = sdk_client_factory(token=None)  # Authorization ヘッダなし

        with pytest.raises(RelayProtocolError) as exc_info:
            post_subscription(client, subscriber="agent-a", labels=["topic:401-contract"])

        assert exc_info.value.status_code == 401
        # 共通 error envelope の code は載らない（今の実装は `code` フィールド自体を持たない）。
        assert exc_info.value.code is None
        assert client.last_response.status_code == 401
        body = client.last_response.json()
        assert "error" in body
        assert "code" not in body
