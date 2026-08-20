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


class TestPeerEncKeyRoundTrip:
    """`peer enc-key` の E2E テスト（実 2 relay 間、既存 pin をやり直さない鍵追加登録）。

    `peer redeem` と異なり `peer enc-key` は既存 peer の locator へ実際に POST するため、
    呼び出し先（B）も実サーバーとして起動する必要がある（redeem テストは B が CLI クライアント
    としてのみ動けば足りたが、こちらは双方向）。
    """

    def _mutual_pin_via_redeem(
        self, *, monkeypatch, capsys, app_a, app_b, base_url_a, base_url_b, db_a, db_b, key_a_pem, key_b_pem
    ) -> None:
        monkeypatch.setenv("RELAY_JWS_PRIVATE_KEY_PEM", key_a_pem)
        rc = invite.main(
            ["peer", "new", "--handle", "bob", "--db", db_a, "--base-url", base_url_a]
        )
        assert rc == 0
        invite_url = capsys.readouterr().out.strip()

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
                base_url_b,
            ]
        )
        capsys.readouterr()
        assert rc == 0

    def test_single_call_pins_both_sides_when_both_configured(
        self, tmp_path, monkeypatch, capsys
    ):
        """B が既に暗号化鍵を持っていれば、A → B の 1 回の `peer enc-key` 呼び出しだけで
        双方向に鍵が揃う（応答で B の鍵を返すため、A 側の CLI がその場で pin する）。
        """
        key_a_pem = _generate_private_pem()
        key_b_pem = _generate_private_pem()
        enc_key_a_pem = _generate_private_pem()
        enc_key_b_pem = _generate_private_pem()
        db_a = str(tmp_path / "enc_a.db")
        db_b = str(tmp_path / "enc_b.db")

        settings_a = Settings(
            db_path=db_a,
            server_log_path=str(tmp_path / "enc_a.jsonl"),
            dispatcher_lock_path=str(tmp_path / "enc_a.lock"),
            jws_private_key_pem=key_a_pem,
            federation_allow_private_locators=True,
        )
        settings_b = Settings(
            db_path=db_b,
            server_log_path=str(tmp_path / "enc_b.jsonl"),
            dispatcher_lock_path=str(tmp_path / "enc_b.lock"),
            jws_private_key_pem=key_b_pem,
            federation_allow_private_locators=True,
        )
        app_a = create_app(settings_a)
        app_b = create_app(settings_b)

        with LiveServer(app_a) as base_url_a, LiveServer(app_b) as base_url_b:
            # 動的ポート確定後に base_url / 暗号化鍵を反映する（peer redeem テストの
            # base_url 反映パターンと同型）。
            app_a.state.settings = dataclasses.replace(
                settings_a, federation_base_url=base_url_a, jwe_private_key_pem=enc_key_a_pem
            )
            app_b.state.settings = dataclasses.replace(
                settings_b, federation_base_url=base_url_b, jwe_private_key_pem=enc_key_b_pem
            )

            self._mutual_pin_via_redeem(
                monkeypatch=monkeypatch,
                capsys=capsys,
                app_a=app_a,
                app_b=app_b,
                base_url_a=base_url_a,
                base_url_b=base_url_b,
                db_a=db_a,
                db_b=db_b,
                key_a_pem=key_a_pem,
                key_b_pem=key_b_pem,
            )

            # A: peer enc-key --handle bob（A 自身の署名鍵・暗号化鍵を使う）。
            monkeypatch.setenv("RELAY_JWS_PRIVATE_KEY_PEM", key_a_pem)
            monkeypatch.setenv("RELAY_JWE_PRIVATE_KEY_PEM", enc_key_a_pem)
            rc = invite.main(["peer", "enc-key", "--handle", "bob", "--db", db_a])
            captured = capsys.readouterr()
            assert rc == 0, f"out={captured.out!r} err={captured.err!r}"

        # A 側: bob（B）の enc_key が 1 回の呼び出しで pin されている（応答経由）。
        peer_on_a = federation_peers.get_peer_by_handle(db_a, "bob")
        assert peer_on_a["enc_key_jwk"] == federation_peers.public_enc_jwk_from_pem(enc_key_b_pem)

        # B 側: alice（A）の enc_key も同じ呼び出しで pin されている（request body 経由）。
        peer_on_b = federation_peers.get_peer_by_handle(db_b, "alice")
        assert peer_on_b["enc_key_jwk"] == federation_peers.public_enc_jwk_from_pem(enc_key_a_pem)

        # 署名鍵・locator 等の既存 pin は無傷（招待をやり直していない）。
        assert peer_on_a["locator"] == base_url_b
        assert peer_on_b["locator"] == base_url_a

    def test_pins_only_caller_side_when_peer_not_yet_configured(
        self, tmp_path, monkeypatch, capsys
    ):
        """B がまだ暗号化鍵を設定していない場合（片側だけロールアウト済みの過渡状態）、
        B 側には A の鍵が登録されるが、A 側は pin すべき enc_key を貰えないため未設定のまま。
        """
        key_a_pem = _generate_private_pem()
        key_b_pem = _generate_private_pem()
        enc_key_a_pem = _generate_private_pem()
        db_a = str(tmp_path / "enc_a2.db")
        db_b = str(tmp_path / "enc_b2.db")

        settings_a = Settings(
            db_path=db_a,
            server_log_path=str(tmp_path / "enc_a2.jsonl"),
            dispatcher_lock_path=str(tmp_path / "enc_a2.lock"),
            jws_private_key_pem=key_a_pem,
            federation_allow_private_locators=True,
        )
        settings_b = Settings(
            db_path=db_b,
            server_log_path=str(tmp_path / "enc_b2.jsonl"),
            dispatcher_lock_path=str(tmp_path / "enc_b2.lock"),
            jws_private_key_pem=key_b_pem,
            federation_allow_private_locators=True,
        )
        app_a = create_app(settings_a)
        app_b = create_app(settings_b)

        with LiveServer(app_a) as base_url_a, LiveServer(app_b) as base_url_b:
            app_a.state.settings = dataclasses.replace(
                settings_a, federation_base_url=base_url_a, jwe_private_key_pem=enc_key_a_pem
            )
            # B は暗号化鍵未設定のまま（federation_base_url だけ反映する）。
            app_b.state.settings = dataclasses.replace(settings_b, federation_base_url=base_url_b)

            self._mutual_pin_via_redeem(
                monkeypatch=monkeypatch,
                capsys=capsys,
                app_a=app_a,
                app_b=app_b,
                base_url_a=base_url_a,
                base_url_b=base_url_b,
                db_a=db_a,
                db_b=db_b,
                key_a_pem=key_a_pem,
                key_b_pem=key_b_pem,
            )

            monkeypatch.setenv("RELAY_JWS_PRIVATE_KEY_PEM", key_a_pem)
            monkeypatch.setenv("RELAY_JWE_PRIVATE_KEY_PEM", enc_key_a_pem)
            rc = invite.main(["peer", "enc-key", "--handle", "bob", "--db", db_a])
            captured = capsys.readouterr()
            assert rc == 0, f"out={captured.out!r} err={captured.err!r}"

        peer_on_a = federation_peers.get_peer_by_handle(db_a, "bob")
        assert peer_on_a["enc_key_jwk"] is None  # B が未設定なので貰いようがない。

        peer_on_b = federation_peers.get_peer_by_handle(db_b, "alice")
        assert peer_on_b["enc_key_jwk"] == federation_peers.public_enc_jwk_from_pem(enc_key_a_pem)
