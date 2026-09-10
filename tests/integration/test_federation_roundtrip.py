"""2 relay 間の federation 往復 E2E テスト（`federation_harness.FederationPair` 使用）。

招待 → redeem → enc-key 交換 → stream 作成 → federation member 追加 → publish →
（実 HTTP 越しの）egress → inbound → replica stream 生成 → local member への配達 →
`GET /events`（SSE）受信、までを実 TCP port の 2 relay で通し、
`docs/ARCHITECTURE.md`「2 relay federation 統合テスト」節が指す構成の具体例を担う。

egress dispatcher / inbound endpoint / SSE dispatch はいずれも
`tests/test_federation_egress.py`・`tests/test_federation_inbound.py`・
`tests/test_delivery.py` で単体テスト済みだが、それらは各層を分離してテストしており
「2 relay 間で実際に配達が通る」ことそのものは検証していなかった。本ファイルはその
end-to-end 経路（署名付き HTTP 越しの実配達）を最小 slice で埋める。
"""
from __future__ import annotations

import sqlite3
import time

import pytest

from relay import federation_peers

from federation_harness import (
    FederationPair,
    capture_egress_envelope,
    federation_pair,
    read_data_event,
)

# `federation_pair` は import するだけで pytest がこのモジュール内の fixture として
# 認識する（フィクスチャ自体は federation_harness.py 定義、re-export して使い回す）。
__all__ = ["federation_pair"]


def _select_dlq_error_codes(db_path: str, publish_id: int) -> list[str]:
    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT error_code FROM dlq WHERE publish_id = ?", (publish_id,)
        ).fetchall()
    finally:
        conn.close()
    return [row[0] for row in rows]


class TestFederationRoundTrip:
    def test_a_to_b_delivered_with_encryption(
        self, federation_pair: FederationPair, monkeypatch: pytest.MonkeyPatch
    ):
        pair = federation_pair
        stream_id = pair.create_stream(pair.relay_a, pair.TOKEN_A, "chat-ab")
        resp = pair.add_federation_member(
            pair.relay_a,
            pair.TOKEN_A,
            stream_id,
            f"{pair.IDENTITY_B}@{pair.HANDLE_B_ON_A}",
        )
        assert resp.status_code == 200, resp.text

        # enc-key 交換が双方向に完了している前提を明示する（body_jwe 経路、平文
        # フォールバックではないことの事前条件）。
        peer_on_a = federation_peers.get_peer_by_handle(
            pair.relay_a.settings.db_path, pair.HANDLE_B_ON_A
        )
        assert peer_on_a["enc_key_jwk"] is not None
        peer_on_b = federation_peers.get_peer_by_handle(
            pair.relay_b.settings.db_path, pair.HANDLE_A_ON_B
        )
        assert peer_on_b["enc_key_jwk"] is not None

        # 上の enc_key_jwk 存在確認は peer registry 側の前提でしかなく、実際に配達された
        # envelope が JWE 化されたことは示さない（平文フォールバックへの regression でも
        # green になってしまう）。egress が実際に relay_b へ POST する wire を横取りし、
        # `body_jwe` が乗っていること・relay_b の鍵で復号すると原文に戻ることまで確認する。
        captured = capture_egress_envelope(monkeypatch)

        with pair.open_sse(pair.relay_b, pair.TOKEN_B) as resp:
            pair.publish(pair.relay_a, pair.TOKEN_A, stream_id, "hello from alice")
            event = read_data_event(resp)

        assert event["body"] == "hello from alice"
        # publisher_identity は federation 由来の受信で "{from_sub}@{handle}" に relay が
        # 強制刻印する（body 自己申告の namespace は無視、relay.federation_inbound 参照）。
        assert event["publisher_identity"] == f"{pair.IDENTITY_A}@{pair.HANDLE_A_ON_B}"

        assert "envelope" in captured, "egress dispatcher の POST を捕捉できなかった"
        envelope = captured["envelope"]
        assert "body" not in envelope
        assert "body_jwe" in envelope
        assert envelope["body_jwe"] != "hello from alice"
        plaintext = federation_peers.decrypt_envelope_body(
            envelope["body_jwe"], private_key_pem=pair.relay_b.settings.jwe_private_key_pem
        )
        assert plaintext == "hello from alice"

    def test_b_to_a_delivered(self, federation_pair: FederationPair):
        """逆方向（B → A）も同じ経路で届く。"""
        pair = federation_pair
        stream_id = pair.create_stream(pair.relay_b, pair.TOKEN_B, "chat-ba")
        resp = pair.add_federation_member(
            pair.relay_b,
            pair.TOKEN_B,
            stream_id,
            f"{pair.IDENTITY_A}@{pair.HANDLE_A_ON_B}",
        )
        assert resp.status_code == 200, resp.text

        with pair.open_sse(pair.relay_a, pair.TOKEN_A) as resp:
            pair.publish(pair.relay_b, pair.TOKEN_B, stream_id, "hello from carol")
            event = read_data_event(resp)

        assert event["body"] == "hello from carol"
        assert event["publisher_identity"] == f"{pair.IDENTITY_B}@{pair.HANDLE_B_ON_A}"

    def test_unregistered_peer_destination_is_rejected_at_membership(
        self, federation_pair: FederationPair
    ):
        """peer として pin されていない handle 宛の member 追加はそもそも拒否される。

        `relay.streams._validate_peer_member_identity` が「suffix が active な peer
        handle であること」を PUT /members の時点で検証するため、宛先が未登録なら
        outbox にすら乗らない（配達パイプラインに一切入らない、既存の membership
        検証仕様どおりの拒否）。
        """
        pair = federation_pair
        stream_id = pair.create_stream(pair.relay_a, pair.TOKEN_A, "chat-ghost")

        resp = pair.add_federation_member(
            pair.relay_a, pair.TOKEN_A, stream_id, "ghost@unregistered-handle"
        )
        assert resp.status_code == 400, resp.text

        members = pair.relay_a.client.get(
            f"/streams/{stream_id}/members", headers=pair.relay_a.auth_header(pair.TOKEN_A)
        ).json()["members"]
        assert all(m["identity"] != "ghost@unregistered-handle" for m in members)

        conn = sqlite3.connect(pair.relay_a.settings.db_path)
        try:
            rows = conn.execute(
                "SELECT 1 FROM outbox WHERE member_identity LIKE 'ghost@%'"
            ).fetchall()
        finally:
            conn.close()
        assert rows == []

    def test_revoked_peer_destination_moves_to_dlq_without_delivery(
        self, federation_pair: FederationPair
    ):
        """member 追加後に peer が revoke されると、以後の publish は配達されず DLQ 行きになる。

        `relay.federation_egress._process_lane` の `PeerRevoked` DLQ 経路（既存実装）を
        2 relay 間の実配達で確認する。B 側には何も届かない。
        """
        pair = federation_pair
        stream_id = pair.create_stream(pair.relay_a, pair.TOKEN_A, "chat-revoked")
        resp = pair.add_federation_member(
            pair.relay_a,
            pair.TOKEN_A,
            stream_id,
            f"{pair.IDENTITY_B}@{pair.HANDLE_B_ON_A}",
        )
        assert resp.status_code == 200, resp.text

        rc = federation_peers.revoke_peer(
            pair.relay_a.settings.db_path, handle=pair.HANDLE_B_ON_A
        )
        assert rc == 1

        publish_id = pair.publish(pair.relay_a, pair.TOKEN_A, stream_id, "should not arrive")

        deadline = time.time() + 5
        error_codes: list[str] = []
        while time.time() < deadline:
            error_codes = _select_dlq_error_codes(pair.relay_a.settings.db_path, publish_id)
            if error_codes:
                break
            time.sleep(0.05)

        assert error_codes == ["PeerRevoked"]
