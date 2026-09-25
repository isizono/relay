"""relay.invite CLI テストスイート。

`python -m relay.invite` の `new` / `revoke` / `list` / `peer new` / `peer redeem` /
`peer list` / `peer revoke` サブコマンドと、DB パス解決順序（`--db` 明示 → env
`RELAY_DB_PATH` → canonical 絶対パス、cwd 相対 fallback なし）を検証する。canonical
絶対パス既定はテスト実行者の実ホームディレクトリを指すため、パス解決テストは実際に
DB へ触れず値の一致のみを確認する。

`peer redeem` の実サーバー往復（署名検証・fingerprint 照合込みの正常系）は
`tests/integration/test_federation_cli_roundtrip.py` でカバーする。ここでは CLI 単体の
入力検証・エラーハンドリング（実 HTTP を伴わないパス）に絞る。
"""
from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from joserfc.jwk import ECKey

from relay import credentials, db, federation_peers, invite


class TestResolveDbPath:
    def test_explicit_db_wins_over_env(self, monkeypatch, tmp_path):
        monkeypatch.setenv("RELAY_DB_PATH", str(tmp_path / "env.db"))
        resolved = invite._resolve_db_path(str(tmp_path / "explicit.db"))
        assert resolved == str(tmp_path / "explicit.db")

    def test_env_used_when_no_explicit_db(self, monkeypatch, tmp_path):
        monkeypatch.setenv("RELAY_DB_PATH", str(tmp_path / "env.db"))
        resolved = invite._resolve_db_path(None)
        assert resolved == str(tmp_path / "env.db")

    def test_canonical_absolute_path_when_neither_given(self, monkeypatch):
        monkeypatch.delenv("RELAY_DB_PATH", raising=False)
        resolved = invite._resolve_db_path(None)
        assert resolved == str(Path.home() / ".local" / "state" / "relay" / "relay.db")
        assert Path(resolved).is_absolute()


class TestParseTtl:
    def test_minutes(self):
        assert invite._parse_ttl("15m") == 900

    def test_hours(self):
        assert invite._parse_ttl("2h") == 7200

    def test_days(self):
        assert invite._parse_ttl("90d") == 90 * 86400

    def test_seconds(self):
        assert invite._parse_ttl("30s") == 30

    def test_none(self):
        assert invite._parse_ttl("none") is None

    def test_invalid_unit_raises(self):
        with pytest.raises(ValueError):
            invite._parse_ttl("15x")

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError):
            invite._parse_ttl("abc")


class TestCmdNew:
    def test_prints_fragment_url_and_inserts_row(self, tmp_path, capsys, monkeypatch):
        monkeypatch.delenv("RELAY_BASE_URL", raising=False)
        db_path = str(tmp_path / "cli.db")
        rc = invite.main(["new", "--identity", "cc-memory", "--db", db_path])
        assert rc == 0
        out = capsys.readouterr().out.strip()
        assert out.startswith("http://127.0.0.1:8770/invitations/redeem#v=1&t=it_")

        conn = db.get_connection(db_path)
        try:
            rows = conn.execute("SELECT * FROM invitations").fetchall()
        finally:
            conn.close()
        assert len(rows) == 1
        assert rows[0]["identity"] == "cc-memory"

    def test_custom_base_url(self, tmp_path, capsys):
        db_path = str(tmp_path / "cli.db")
        invite.main(
            [
                "new",
                "--identity",
                "cc-memory",
                "--db",
                db_path,
                "--base-url",
                "http://127.0.0.1:9999",
            ]
        )
        out = capsys.readouterr().out.strip()
        assert out.startswith("http://127.0.0.1:9999/invitations/redeem#v=1&t=it_")

    def test_env_base_url_used_when_no_explicit_flag(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("RELAY_BASE_URL", "https://relay.example.org")
        db_path = str(tmp_path / "cli.db")
        rc = invite.main(["new", "--identity", "cc-memory", "--db", db_path])
        assert rc == 0
        captured = capsys.readouterr()
        assert captured.out.strip().startswith(
            "https://relay.example.org/invitations/redeem#v=1&t=it_"
        )
        assert captured.err == ""  # env で解決できているので警告は出ない

    def test_explicit_flag_wins_over_env(self, tmp_path, capsys, monkeypatch):
        monkeypatch.setenv("RELAY_BASE_URL", "https://relay.example.org")
        db_path = str(tmp_path / "cli.db")
        rc = invite.main(
            [
                "new",
                "--identity",
                "cc-memory",
                "--db",
                db_path,
                "--base-url",
                "https://cli-wins.example.com",
            ]
        )
        assert rc == 0
        captured = capsys.readouterr()
        assert captured.out.strip().startswith(
            "https://cli-wins.example.com/invitations/redeem#v=1&t=it_"
        )
        assert captured.err == ""

    def test_neither_given_falls_back_to_default_and_warns(self, tmp_path, capsys, monkeypatch):
        monkeypatch.delenv("RELAY_BASE_URL", raising=False)
        db_path = str(tmp_path / "cli.db")
        rc = invite.main(["new", "--identity", "cc-memory", "--db", db_path])
        assert rc == 0
        captured = capsys.readouterr()
        assert captured.out.strip().startswith(
            "http://127.0.0.1:8770/invitations/redeem#v=1&t=it_"
        )
        assert "RELAY_BASE_URL" in captured.err
        assert "--base-url" in captured.err

    def test_rejects_none_ttl_for_invite(self, tmp_path, capsys):
        db_path = str(tmp_path / "cli.db")
        rc = invite.main(["new", "--identity", "cc-memory", "--db", db_path, "--ttl", "none"])
        assert rc != 0

    def test_rejects_invalid_ttl_format(self, tmp_path, capsys):
        db_path = str(tmp_path / "cli.db")
        rc = invite.main(["new", "--identity", "cc-memory", "--db", db_path, "--ttl", "bogus"])
        assert rc != 0


class TestCmdList:
    def test_lists_invitation_with_masked_token(self, tmp_path, capsys):
        db_path = str(tmp_path / "cli.db")
        invite.main(["new", "--identity", "cc-memory", "--db", db_path])
        full_url = capsys.readouterr().out.strip()
        full_token = full_url.rsplit("t=", 1)[1]

        rc = invite.main(["list", "--db", db_path])
        assert rc == 0
        out = capsys.readouterr().out
        assert "cc-memory" in out
        assert "pending" in out
        assert full_token not in out  # token 全体は表示しない（先頭数文字のみ）

    def test_lists_redeemed_credential(self, tmp_path, capsys):
        db_path = str(tmp_path / "cli.db")
        invite.main(["new", "--identity", "cc-memory", "--db", db_path])
        full_url = capsys.readouterr().out.strip()
        token = full_url.rsplit("t=", 1)[1]

        conn = db.get_connection(db_path)
        try:
            credentials.redeem_invite(conn, token, credentials._now_iso())
        finally:
            conn.close()

        rc = invite.main(["list", "--db", db_path])
        assert rc == 0
        out = capsys.readouterr().out
        assert "redeemed" in out
        assert "active" in out

    def test_empty_db_lists_nothing_but_succeeds(self, tmp_path, capsys):
        db_path = str(tmp_path / "empty.db")
        rc = invite.main(["list", "--db", db_path])
        assert rc == 0
        out = capsys.readouterr().out
        assert "invitations:" in out
        assert "credentials:" in out


class TestCmdRevoke:
    def test_revoke_by_identity_marks_credential_revoked(self, tmp_path, capsys):
        db_path = str(tmp_path / "cli.db")
        invite.main(["new", "--identity", "cc-memory", "--db", db_path])
        full_url = capsys.readouterr().out.strip()
        token = full_url.rsplit("t=", 1)[1]

        conn = db.get_connection(db_path)
        try:
            bearer_token, _, _ = credentials.redeem_invite(conn, token, credentials._now_iso())
        finally:
            conn.close()

        rc = invite.main(["revoke", "--identity", "cc-memory", "--db", db_path])
        assert rc == 0

        conn = db.get_connection(db_path)
        try:
            row = conn.execute(
                "SELECT revoked_at FROM credentials WHERE token = ?", (bearer_token,)
            ).fetchone()
        finally:
            conn.close()
        assert row["revoked_at"] is not None

    def test_revoke_by_credential_id(self, tmp_path, capsys):
        db_path = str(tmp_path / "cli.db")
        invite.main(["new", "--identity", "cc-memory", "--db", db_path])
        full_url = capsys.readouterr().out.strip()
        token = full_url.rsplit("t=", 1)[1]

        conn = db.get_connection(db_path)
        try:
            credentials.redeem_invite(conn, token, credentials._now_iso())
            cred_id = conn.execute("SELECT id FROM credentials").fetchone()["id"]
        finally:
            conn.close()

        rc = invite.main(["revoke", "--credential-id", str(cred_id), "--db", db_path])
        assert rc == 0

    def test_revoke_no_match_returns_nonzero(self, tmp_path):
        db_path = str(tmp_path / "cli.db")
        db.init_db(db_path)
        rc = invite.main(["revoke", "--identity", "nobody", "--db", db_path])
        assert rc != 0

    def test_revoke_requires_identity_or_credential_id(self, tmp_path):
        db_path = str(tmp_path / "cli.db")
        with pytest.raises(SystemExit):
            invite.main(["revoke", "--db", db_path])


class TestCmdNewRejectsAtInIdentity:
    def test_at_in_identity_returns_nonzero(self, tmp_path):
        db_path = str(tmp_path / "cli.db")
        rc = invite.main(["new", "--identity", "orch@bob", "--db", db_path])
        assert rc != 0


def _generate_private_pem() -> str:
    key = ECKey.generate_key("P-256", private=True)
    return key.as_pem(private=True).decode("ascii")


@pytest.fixture()
def federation_key(monkeypatch):
    pem = _generate_private_pem()
    monkeypatch.setenv("RELAY_JWS_PRIVATE_KEY_PEM", pem)
    return pem


class TestCmdPeerNew:
    def test_requires_federation_key(self, tmp_path, monkeypatch):
        monkeypatch.delenv("RELAY_JWS_PRIVATE_KEY_PEM", raising=False)
        db_path = str(tmp_path / "peer.db")
        rc = invite.main(["peer", "new", "--handle", "bob", "--db", db_path])
        assert rc != 0

    def test_rejects_at_in_handle(self, tmp_path, federation_key):
        db_path = str(tmp_path / "peer.db")
        rc = invite.main(["peer", "new", "--handle", "bob@x", "--db", db_path])
        assert rc != 0

    def test_prints_fragment_url_with_token_and_fingerprint(
        self, tmp_path, federation_key, capsys
    ):
        db_path = str(tmp_path / "peer.db")
        rc = invite.main(
            [
                "peer",
                "new",
                "--handle",
                "bob",
                "--db",
                db_path,
                "--base-url",
                "https://relay-a.example",
            ]
        )
        assert rc == 0
        out = capsys.readouterr().out.strip()
        assert out.startswith("https://relay-a.example/federation/peers/redeem#v=1&t=pi_")
        assert "&fp=" in out

        own_fp = federation_peers.compute_fingerprint(
            federation_peers.public_jwk_from_pem(federation_key)
        )
        assert out.endswith(f"&fp={own_fp}")

        conn = db.get_connection(db_path)
        try:
            rows = conn.execute("SELECT * FROM peer_invitations").fetchall()
        finally:
            conn.close()
        assert len(rows) == 1
        assert rows[0]["handle"] == "bob"

    def test_env_base_url_used_when_no_explicit_flag(
        self, tmp_path, federation_key, capsys, monkeypatch
    ):
        monkeypatch.setenv("RELAY_BASE_URL", "https://relay.example.org")
        db_path = str(tmp_path / "peer.db")
        rc = invite.main(["peer", "new", "--handle", "bob", "--db", db_path])
        assert rc == 0
        captured = capsys.readouterr()
        assert captured.out.strip().startswith(
            "https://relay.example.org/federation/peers/redeem#v=1&t=pi_"
        )
        assert captured.err == ""

    def test_neither_given_falls_back_to_default_and_warns(
        self, tmp_path, federation_key, capsys, monkeypatch
    ):
        monkeypatch.delenv("RELAY_BASE_URL", raising=False)
        db_path = str(tmp_path / "peer.db")
        rc = invite.main(["peer", "new", "--handle", "bob", "--db", db_path])
        assert rc == 0
        captured = capsys.readouterr()
        assert captured.out.strip().startswith(
            "http://127.0.0.1:8770/federation/peers/redeem#v=1&t=pi_"
        )
        assert "RELAY_BASE_URL" in captured.err
        assert "--base-url" in captured.err

    def test_rejects_none_ttl(self, tmp_path, federation_key):
        db_path = str(tmp_path / "peer.db")
        rc = invite.main(
            ["peer", "new", "--handle", "bob", "--db", db_path, "--ttl", "none"]
        )
        assert rc != 0


class TestCmdPeerRedeemInputValidation:
    def test_requires_federation_key(self, tmp_path, monkeypatch):
        monkeypatch.delenv("RELAY_JWS_PRIVATE_KEY_PEM", raising=False)
        db_path = str(tmp_path / "peer.db")
        rc = invite.main(
            [
                "peer",
                "redeem",
                "https://relay-a.example/federation/peers/redeem#v=1&t=pi_x&fp=y",
                "--handle",
                "alice",
                "--db",
                db_path,
            ]
        )
        assert rc != 0

    def test_rejects_at_in_handle(self, tmp_path, federation_key):
        db_path = str(tmp_path / "peer.db")
        rc = invite.main(
            [
                "peer",
                "redeem",
                "https://relay-a.example/federation/peers/redeem#v=1&t=pi_x&fp=y",
                "--handle",
                "alice@x",
                "--db",
                db_path,
            ]
        )
        assert rc != 0

    def test_rejects_url_missing_fragment_params(self, tmp_path, federation_key):
        db_path = str(tmp_path / "peer.db")
        rc = invite.main(
            [
                "peer",
                "redeem",
                "https://relay-a.example/federation/peers/redeem",
                "--handle",
                "alice",
                "--db",
                db_path,
            ]
        )
        assert rc != 0

    def test_rejects_private_locator_in_url_by_default(self, tmp_path, federation_key, monkeypatch):
        monkeypatch.delenv("RELAY_FEDERATION_ALLOW_PRIVATE_LOCATORS", raising=False)
        db_path = str(tmp_path / "peer.db")
        rc = invite.main(
            [
                "peer",
                "redeem",
                "https://127.0.0.1:9999/federation/peers/redeem#v=1&t=pi_x&fp=y",
                "--handle",
                "alice",
                "--db",
                db_path,
            ]
        )
        assert rc != 0


def _generate_keypair() -> dict:
    key = ECKey.generate_key("P-256", private=True)
    return {
        "private_pem": key.as_pem(private=True).decode("ascii"),
        "public_jwk": key.as_dict(private=False),
    }


class TestCmdPeerEncKeySignatureVerification:
    """`peer enc-key` の応答署名検証（既に pin 済みの相手鍵で検証してから pin する）。

    実 HTTP は伴わず `federation_net.build_client` を `httpx.MockTransport` に差し替えて
    応答を模す（`tests/test_federation_egress.py` の `_patch_transport` と同型）。
    正常系・応答が改竄された場合の E2E は `tests/integration/test_federation_cli_roundtrip.py`
    の `TestPeerEncKeyRoundTrip` でも別途カバーする。
    """

    def _setup_pinned_peer(self, tmp_path, monkeypatch) -> tuple[str, dict]:
        own_keypair = _generate_keypair()
        own_enc_keypair = _generate_keypair()
        peer_keypair = _generate_keypair()

        db_path = str(tmp_path / "enc_key_sig.db")
        db.init_db(db_path)
        federation_peers.add_peer(
            db_path,
            handle="bob",
            fingerprint=federation_peers.compute_fingerprint(peer_keypair["public_jwk"]),
            key_jwk=peer_keypair["public_jwk"],
            locator="https://8.8.8.8",
        )

        monkeypatch.setenv("RELAY_JWS_PRIVATE_KEY_PEM", own_keypair["private_pem"])
        monkeypatch.setenv("RELAY_JWE_PRIVATE_KEY_PEM", own_enc_keypair["private_pem"])
        return db_path, peer_keypair

    def _patch_client(self, monkeypatch, handler) -> None:
        def _build(**kwargs):
            return httpx.Client(transport=httpx.MockTransport(handler))

        monkeypatch.setattr(invite.federation_net, "build_client", _build)

    def test_valid_signed_response_pins_peer_enc_key(self, tmp_path, monkeypatch, capsys):
        db_path, peer_keypair = self._setup_pinned_peer(tmp_path, monkeypatch)
        peer_enc_keypair = _generate_keypair()

        resp_body = {"handle": "alice", "enc_key": peer_enc_keypair["public_jwk"]}
        sig_payload = {
            "typ": "relay-fed-enc-key-resp",
            "handle": resp_body["handle"],
            "enc_key": resp_body["enc_key"],
        }
        resp_body["sig"] = federation_peers.sign_detached(
            sig_payload, private_key_pem=peer_keypair["private_pem"]
        )

        self._patch_client(monkeypatch, lambda request: httpx.Response(200, json=resp_body))

        rc = invite.main(["peer", "enc-key", "--handle", "bob", "--db", db_path])
        captured = capsys.readouterr()
        assert rc == 0, f"out={captured.out!r} err={captured.err!r}"

        peer = federation_peers.get_peer_by_handle(db_path, "bob")
        assert peer["enc_key_jwk"] == peer_enc_keypair["public_jwk"]

    def test_tampered_enc_key_is_rejected_and_not_pinned(self, tmp_path, monkeypatch, capsys):
        """署名計算後に enc_key だけを差し替えた応答は署名検証に失敗し pin されない。"""
        db_path, peer_keypair = self._setup_pinned_peer(tmp_path, monkeypatch)
        peer_enc_keypair = _generate_keypair()
        attacker_enc_keypair = _generate_keypair()

        sig_payload = {
            "typ": "relay-fed-enc-key-resp",
            "handle": "alice",
            "enc_key": peer_enc_keypair["public_jwk"],
        }
        sig = federation_peers.sign_detached(sig_payload, private_key_pem=peer_keypair["private_pem"])
        resp_body = {
            "handle": "alice",
            "enc_key": attacker_enc_keypair["public_jwk"],  # 署名計算後の改竄
            "sig": sig,
        }

        self._patch_client(monkeypatch, lambda request: httpx.Response(200, json=resp_body))

        rc = invite.main(["peer", "enc-key", "--handle", "bob", "--db", db_path])
        capsys.readouterr()
        assert rc != 0

        peer = federation_peers.get_peer_by_handle(db_path, "bob")
        assert peer["enc_key_jwk"] is None

    def test_tampered_sig_is_rejected_and_not_pinned(self, tmp_path, monkeypatch, capsys):
        """無関係の鍵で署名された応答（署名者詐称）は pin されない。"""
        db_path, peer_keypair = self._setup_pinned_peer(tmp_path, monkeypatch)
        peer_enc_keypair = _generate_keypair()
        unrelated_keypair = _generate_keypair()

        sig_payload = {
            "typ": "relay-fed-enc-key-resp",
            "handle": "alice",
            "enc_key": peer_enc_keypair["public_jwk"],
        }
        bad_sig = federation_peers.sign_detached(
            sig_payload, private_key_pem=unrelated_keypair["private_pem"]
        )
        resp_body = {
            "handle": "alice",
            "enc_key": peer_enc_keypair["public_jwk"],
            "sig": bad_sig,
        }

        self._patch_client(monkeypatch, lambda request: httpx.Response(200, json=resp_body))

        rc = invite.main(["peer", "enc-key", "--handle", "bob", "--db", db_path])
        capsys.readouterr()
        assert rc != 0

        peer = federation_peers.get_peer_by_handle(db_path, "bob")
        assert peer["enc_key_jwk"] is None

    def test_missing_sig_is_rejected_and_not_pinned(self, tmp_path, monkeypatch, capsys):
        """署名フィールド自体が無い無署名応答は pin されない（旧仕様への回帰防止）。"""
        db_path, peer_keypair = self._setup_pinned_peer(tmp_path, monkeypatch)
        peer_enc_keypair = _generate_keypair()

        resp_body = {"handle": "alice", "enc_key": peer_enc_keypair["public_jwk"]}

        self._patch_client(monkeypatch, lambda request: httpx.Response(200, json=resp_body))

        rc = invite.main(["peer", "enc-key", "--handle", "bob", "--db", db_path])
        capsys.readouterr()
        assert rc != 0

        peer = federation_peers.get_peer_by_handle(db_path, "bob")
        assert peer["enc_key_jwk"] is None


class TestCmdPeerList:
    def test_empty_db_lists_nothing_but_succeeds(self, tmp_path, capsys):
        db_path = str(tmp_path / "peer.db")
        rc = invite.main(["peer", "list", "--db", db_path])
        assert rc == 0
        out = capsys.readouterr().out
        assert "peers:" in out

    def test_lists_pinned_peer(self, tmp_path, capsys):
        db_path = str(tmp_path / "peer.db")
        db.init_db(db_path)
        jwk = ECKey.generate_key("P-256", private=True).as_dict(private=False)
        fp = federation_peers.compute_fingerprint(jwk)
        federation_peers.add_peer(
            db_path, handle="bob", fingerprint=fp, key_jwk=jwk, locator="https://relay-b.example"
        )
        rc = invite.main(["peer", "list", "--db", db_path])
        assert rc == 0
        out = capsys.readouterr().out
        assert "bob" in out
        assert "active" in out


class TestCmdPeerRevoke:
    def test_revoke_marks_peer_revoked(self, tmp_path):
        db_path = str(tmp_path / "peer.db")
        db.init_db(db_path)
        jwk = ECKey.generate_key("P-256", private=True).as_dict(private=False)
        fp = federation_peers.compute_fingerprint(jwk)
        federation_peers.add_peer(
            db_path, handle="bob", fingerprint=fp, key_jwk=jwk, locator="https://relay-b.example"
        )
        rc = invite.main(["peer", "revoke", "--handle", "bob", "--db", db_path])
        assert rc == 0
        peer = federation_peers.get_peer_by_handle(db_path, "bob")
        assert peer["revoked_at"] is not None

    def test_revoke_no_match_returns_nonzero(self, tmp_path):
        db_path = str(tmp_path / "peer.db")
        db.init_db(db_path)
        rc = invite.main(["peer", "revoke", "--handle", "nobody", "--db", db_path])
        assert rc != 0


class TestCmdPeerRequireEncryption:
    def _pin_bob(self, db_path: str) -> None:
        db.init_db(db_path)
        jwk = ECKey.generate_key("P-256", private=True).as_dict(private=False)
        fp = federation_peers.compute_fingerprint(jwk)
        federation_peers.add_peer(
            db_path, handle="bob", fingerprint=fp, key_jwk=jwk, locator="https://relay-b.example"
        )

    def test_on_sets_require_encryption_true(self, tmp_path):
        db_path = str(tmp_path / "peer.db")
        self._pin_bob(db_path)
        rc = invite.main(["peer", "require-encryption", "bob", "on", "--db", db_path])
        assert rc == 0
        peer = federation_peers.get_peer_by_handle(db_path, "bob")
        assert peer["require_encryption"] is True

    def test_off_sets_require_encryption_false(self, tmp_path):
        db_path = str(tmp_path / "peer.db")
        self._pin_bob(db_path)
        federation_peers.set_peer_require_encryption(db_path, handle="bob", required=True)
        rc = invite.main(["peer", "require-encryption", "bob", "off", "--db", db_path])
        assert rc == 0
        peer = federation_peers.get_peer_by_handle(db_path, "bob")
        assert peer["require_encryption"] is False

    def test_unknown_handle_returns_nonzero(self, tmp_path):
        db_path = str(tmp_path / "peer.db")
        db.init_db(db_path)
        rc = invite.main(["peer", "require-encryption", "nobody", "on", "--db", db_path])
        assert rc != 0

    def test_invalid_state_rejected_by_argparse(self, tmp_path):
        db_path = str(tmp_path / "peer.db")
        self._pin_bob(db_path)
        with pytest.raises(SystemExit):
            invite.main(["peer", "require-encryption", "bob", "maybe", "--db", db_path])
