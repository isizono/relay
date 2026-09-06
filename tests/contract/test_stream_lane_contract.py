"""場 (stream) レーンの publish contract test（docs/design/relay-v2-wire-api.md §3.2）。"""
from __future__ import annotations

from relay_sdk.http import post_stream, post_stream_message, put_stream_member


class TestStreamPublishContract:
    """`POST /streams/{stream_id}/messages`（wire-api.md §3.2）。"""

    def test_returns_publish_id_and_matched_members(self, sdk_client_factory):
        creator = sdk_client_factory("tok-a")
        stream = post_stream(creator, name="contract-stream")
        stream_id = stream["stream_id"]
        # bootstrap の作成者は access="write" のみ（read 権限なし）のため、投函の配達対象
        # （read 権限を持つ member）に自身を含めるには明示的に read_write へ引き上げる
        # （wire-api.md §3.1・§3.2）。
        put_stream_member(creator, stream_id=stream_id, identity="agent-a", access="read_write")

        result = post_stream_message(creator, stream_id=stream_id, body="hello contract")
        assert creator.last_response.status_code == 202
        assert isinstance(result["publish_id"], int)
        assert isinstance(result["matched_members"], int)
        assert result["matched_members"] == 1
