"""`relay.invite peer` サブコマンドの E2E テスト（実 uvicorn 1 インスタンスに対して CLI で redeem）。

`tests/integration/test_sdk_roundtrip.py` の `LiveServer` パターンを流用し、実 TCP port で
起動した relay A に対して `peer new` → `peer redeem` を CLI 経由で実行し、双方の DB に
相互 pin が成立することを検証する（設計の E2E 受け入れ手順「3. 登録」に相当する最小
slice。B 側 relay サーバー自体はこの PR のスコープ外〔inbound は別 PR〕のため起動しない）。
"""
from __future__ import annotations

import dataclasses
import socket
import threading
import time

import pytest
import uvicorn
from joserfc.jwk import ECKey

from relay import federation_peers, invite
from relay.app import create_app
from relay.config import Settings


class LiveServer:
    """uvicorn を実 TCP port で起動する（tests/test_delivery.py の LiveServer と同型）。"""

    def __init__(self, app):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        self.port = sock.getsockname()[1]
        sock.close()
        config = uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="warning")
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self) -> str:
        self.thread.start()
        deadline = time.time() + 5
        while not self.server.started and time.time() < deadline:
            time.sleep(0.02)
        return f"http://127.0.0.1:{self.port}"

    def __exit__(self, *exc_info) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=5)


def _generate_private_pem() -> str:
    key = ECKey.generate_key("P-256", private=True)
    return key.as_pem(private=True).decode("ascii")


@pytest.fixture(autouse=True)
def _allow_private_locators(monkeypatch):
    # 実サーバーは 127.0.0.1 の動的ポートで listen するため、outbound ガードの
    # private レンジ既定拒否を opt-in で解除する（同一ホスト E2E 用、本番既定は false のまま）。
    monkeypatch.setenv("RELAY_FEDERATION_ALLOW_PRIVATE_LOCATORS", "true")


class TestPeerNewRedeemRoundTrip:
    def test_mutual_pin_established(self, tmp_path, monkeypatch, capsys):
        key_a_pem = _generate_private_pem()
        key_b_pem = _generate_private_pem()

        db_a = str(tmp_path / "a.db")
        db_b = str(tmp_path / "b.db")

        settings_a = Settings(
            db_path=db_a,
            server_log_path=str(tmp_path / "a.jsonl"),
            dispatcher_lock_path=str(tmp_path / "a.lock"),
            jws_private_key_pem=key_a_pem,
        )
        app_a = create_app(settings_a)

        with LiveServer(app_a) as base_url_a:
            # 動的ポート確定後、A relay 自身が応答 card.locator に載せる base_url を反映する
            # （RELAY_BASE_URL 相当。実運用では起動前に固定値として設定される）。
            app_a.state.settings = dataclasses.replace(
                settings_a, federation_base_url=base_url_a
            )

            # A: invite peer new --handle bob（CLI は A 自身として動くため A の鍵を使う）
            monkeypatch.setenv("RELAY_JWS_PRIVATE_KEY_PEM", key_a_pem)
            rc = invite.main(
                ["peer", "new", "--handle", "bob", "--db", db_a, "--base-url", base_url_a]
            )
            assert rc == 0
            invite_url = capsys.readouterr().out.strip()
            assert invite_url.startswith(f"{base_url_a}/federation/peers/redeem#v=1&t=pi_")

            # B: invite peer redeem <URL> --handle alice（CLI は B 自身として動くため B の鍵を使う）
            monkeypatch.setenv("RELAY_JWS_PRIVATE_KEY_PEM", key_b_pem)
            rc = invite.main(
                [
                    "peer",
                    "redeem",
                    invite_url,
                    "--handle",
                    "alice",
                    "--db",
                    db_b,
                    "--base-url",
                    "https://8.8.8.8",
                ]
            )
            captured = capsys.readouterr()
            assert rc == 0, f"out={captured.out!r} err={captured.err!r}"

        # A 側 DB に bob が pin されていること（B の locator・鍵が正しく反映される）。
        peer_on_a = federation_peers.get_peer_by_handle(db_a, "bob")
        assert peer_on_a is not None
        assert peer_on_a["locator"] == "https://8.8.8.8"
        assert peer_on_a["fingerprint"] == federation_peers.compute_fingerprint(
            federation_peers.public_jwk_from_pem(key_b_pem)
        )

        # B 側 DB に alice が pin されていること（A の locator・鍵が正しく反映される）。
        peer_on_b = federation_peers.get_peer_by_handle(db_b, "alice")
        assert peer_on_b is not None
        assert peer_on_b["locator"] == base_url_a
        assert peer_on_b["fingerprint"] == federation_peers.compute_fingerprint(
            federation_peers.public_jwk_from_pem(key_a_pem)
        )

    def test_fingerprint_mismatch_aborts_without_pinning(self, tmp_path, monkeypatch, capsys):
        """URL の fp を攻撃者が改竄した場合、A 側の a_fp 検証で拒否され双方とも pin されない。"""
        key_a_pem = _generate_private_pem()
        key_b_pem = _generate_private_pem()
        db_a = str(tmp_path / "a2.db")
        db_b = str(tmp_path / "b2.db")

        settings_a = Settings(
            db_path=db_a,
            server_log_path=str(tmp_path / "a2.jsonl"),
            dispatcher_lock_path=str(tmp_path / "a2.lock"),
            jws_private_key_pem=key_a_pem,
        )
        app_a = create_app(settings_a)

        with LiveServer(app_a) as base_url_a:
            app_a.state.settings = dataclasses.replace(
                settings_a, federation_base_url=base_url_a
            )
            monkeypatch.setenv("RELAY_JWS_PRIVATE_KEY_PEM", key_a_pem)
            invite.main(["peer", "new", "--handle", "bob", "--db", db_a, "--base-url", base_url_a])
            invite_url = capsys.readouterr().out.strip()
            # fp フラグメントを別の値に改竄する。
            tampered_url = invite_url.rsplit("fp=", 1)[0] + "fp=tampered-fingerprint"

            monkeypatch.setenv("RELAY_JWS_PRIVATE_KEY_PEM", key_b_pem)
            rc = invite.main(
                [
                    "peer",
                    "redeem",
                    tampered_url,
                    "--handle",
                    "alice",
                    "--db",
                    db_b,
                    "--base-url",
                    "https://8.8.8.8",
                ]
            )
            capsys.readouterr()

        assert rc != 0
        assert federation_peers.get_peer_by_handle(db_b, "alice") is None
