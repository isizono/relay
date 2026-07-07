"""relay.federation_peers テストスイート。

peer pin CRUD・招待 token の発行/一回性消費（並行 redeem 競合含む）・
RFC 7638 JWK thumbprint 計算・detached JWS の署名/検証を検証する。
並行性テストは tests/test_credentials.py の並行 redeem パターンを踏襲する。
"""
from __future__ import annotations

import threading

import pytest
from joserfc.jwk import ECKey

from relay import db, federation_peers


@pytest.fixture()
def db_path(tmp_path):
    path = str(tmp_path / "federation_peers.db")
    db.init_db(path)
    return path


@pytest.fixture()
def keypair():
    private = ECKey.generate_key("P-256", private=True)
    return {
        "private_pem": private.as_pem(private=True).decode("ascii"),
        "public_jwk": private.as_dict(private=False),
    }


class TestValidatePeerHandle:
    def test_accepts_plain_handle(self):
        federation_peers.validate_peer_handle("bob")

    @pytest.mark.parametrize("bad", ["bob@x", "bo:b", "bo/b", ""])
    def test_rejects_forbidden_chars(self, bad):
        with pytest.raises(ValueError):
            federation_peers.validate_peer_handle(bad)


class TestComputeFingerprint:
    def test_matches_rfc7638_known_vector(self):
        """RFC 7638 Appendix A.1 の既知ベクタ（RSA 鍵）と一致することを確認する。"""
        jwk = {
            "kty": "RSA",
            "n": (
                "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aP"
                "FFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93"
                "lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZH"
                "zu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPk"
                "sINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw"
            ),
            "e": "AQAB",
        }
        assert (
            federation_peers.compute_fingerprint(jwk)
            == "NzbLsXh8uDCcd-6MNwXF4W_7noWXFZAfHkxZsRGC9Xs"
        )

    def test_ec_key_thumbprint_is_deterministic(self, keypair):
        fp1 = federation_peers.compute_fingerprint(keypair["public_jwk"])
        fp2 = federation_peers.compute_fingerprint(keypair["public_jwk"])
        assert fp1 == fp2
        assert len(fp1) > 0

    def test_different_keys_produce_different_fingerprints(self, keypair):
        other = ECKey.generate_key("P-256", private=True).as_dict(private=False)
        assert federation_peers.compute_fingerprint(
            keypair["public_jwk"]
        ) != federation_peers.compute_fingerprint(other)


class TestPublicJwkFromPem:
    def test_returns_public_jwk_without_private_component(self, keypair):
        jwk = federation_peers.public_jwk_from_pem(keypair["private_pem"])
        assert "d" not in jwk  # 秘密鍵成分を含まない
        assert jwk["kty"] == "EC"


class TestSignAndVerifyDetached:
    def test_verify_succeeds_for_valid_signature(self, keypair):
        payload = {"typ": "relay-fed-redeem", "token": "pi_abc", "ts": 1000, "a_fp": "fp"}
        sig = federation_peers.sign_detached(payload, private_key_pem=keypair["private_pem"])
        assert federation_peers.verify_detached(payload, sig, public_key=keypair["public_jwk"])

    def test_verify_fails_for_tampered_payload(self, keypair):
        payload = {"typ": "relay-fed-redeem", "token": "pi_abc", "ts": 1000, "a_fp": "fp"}
        sig = federation_peers.sign_detached(payload, private_key_pem=keypair["private_pem"])
        tampered = dict(payload, token="pi_evil")
        assert not federation_peers.verify_detached(
            tampered, sig, public_key=keypair["public_jwk"]
        )

    def test_verify_fails_for_wrong_key(self, keypair):
        payload = {"typ": "relay-fed-redeem", "token": "pi_abc", "ts": 1000, "a_fp": "fp"}
        sig = federation_peers.sign_detached(payload, private_key_pem=keypair["private_pem"])
        other_pub = ECKey.generate_key("P-256", private=True).as_dict(private=False)
        assert not federation_peers.verify_detached(payload, sig, public_key=other_pub)

    def test_verify_fails_for_malformed_sig_structure(self, keypair):
        payload = {"typ": "relay-fed-redeem", "token": "pi_abc", "ts": 1000, "a_fp": "fp"}
        assert not federation_peers.verify_detached(
            payload, {"protected": "not-b64"}, public_key=keypair["public_jwk"]
        )
        assert not federation_peers.verify_detached(
            payload, "not-a-dict", public_key=keypair["public_jwk"]
        )
        assert not federation_peers.verify_detached(payload, None, public_key=keypair["public_jwk"])


class TestPeerCrud:
    def test_add_and_get_by_fingerprint(self, db_path, keypair):
        fp = federation_peers.compute_fingerprint(keypair["public_jwk"])
        federation_peers.add_peer(
            db_path,
            handle="bob",
            fingerprint=fp,
            key_jwk=keypair["public_jwk"],
            locator="https://relay-b.example",
        )
        peer = federation_peers.get_peer_by_fingerprint(db_path, fp)
        assert peer is not None
        assert peer["handle"] == "bob"
        assert peer["key_jwk"] == keypair["public_jwk"]
        assert peer["revoked_at"] is None

    def test_get_by_handle(self, db_path, keypair):
        fp = federation_peers.compute_fingerprint(keypair["public_jwk"])
        federation_peers.add_peer(
            db_path, handle="bob", fingerprint=fp, key_jwk=keypair["public_jwk"], locator="https://x"
        )
        peer = federation_peers.get_peer_by_handle(db_path, "bob")
        assert peer is not None
        assert peer["fingerprint"] == fp

    def test_unknown_returns_none(self, db_path):
        assert federation_peers.get_peer_by_fingerprint(db_path, "nope") is None
        assert federation_peers.get_peer_by_handle(db_path, "nope") is None

    def test_list_peers(self, db_path, keypair):
        fp = federation_peers.compute_fingerprint(keypair["public_jwk"])
        federation_peers.add_peer(
            db_path, handle="bob", fingerprint=fp, key_jwk=keypair["public_jwk"], locator="https://x"
        )
        peers = federation_peers.list_peers(db_path)
        assert len(peers) == 1
        assert peers[0]["handle"] == "bob"

    def test_duplicate_handle_raises(self, db_path, keypair):
        fp1 = federation_peers.compute_fingerprint(keypair["public_jwk"])
        federation_peers.add_peer(
            db_path, handle="bob", fingerprint=fp1, key_jwk=keypair["public_jwk"], locator="https://x"
        )
        other_jwk = ECKey.generate_key("P-256", private=True).as_dict(private=False)
        fp2 = federation_peers.compute_fingerprint(other_jwk)
        with pytest.raises(federation_peers.PeerAlreadyRegisteredError):
            federation_peers.add_peer(
                db_path, handle="bob", fingerprint=fp2, key_jwk=other_jwk, locator="https://y"
            )

    def test_duplicate_fingerprint_raises(self, db_path, keypair):
        fp = federation_peers.compute_fingerprint(keypair["public_jwk"])
        federation_peers.add_peer(
            db_path, handle="bob", fingerprint=fp, key_jwk=keypair["public_jwk"], locator="https://x"
        )
        with pytest.raises(federation_peers.PeerAlreadyRegisteredError):
            federation_peers.add_peer(
                db_path,
                handle="carol",
                fingerprint=fp,
                key_jwk=keypair["public_jwk"],
                locator="https://y",
            )

    def test_revoke_sets_revoked_at_and_is_idempotent_count(self, db_path, keypair):
        fp = federation_peers.compute_fingerprint(keypair["public_jwk"])
        federation_peers.add_peer(
            db_path, handle="bob", fingerprint=fp, key_jwk=keypair["public_jwk"], locator="https://x"
        )
        first = federation_peers.revoke_peer(db_path, handle="bob")
        second = federation_peers.revoke_peer(db_path, handle="bob")
        assert first == 1
        assert second == 0
        peer = federation_peers.get_peer_by_handle(db_path, "bob")
        assert peer["revoked_at"] is not None

    def test_revoke_unknown_handle_returns_zero(self, db_path):
        assert federation_peers.revoke_peer(db_path, handle="nobody") == 0


class TestPeerInviteLifecycle:
    def test_issue_returns_pi_prefixed_token(self, db_path):
        token = federation_peers.issue_peer_invite(db_path, handle="bob", invite_ttl_seconds=900)
        assert token.startswith("pi_")

    def test_consume_success_returns_invitation_id_and_handle(self, db_path):
        token = federation_peers.issue_peer_invite(db_path, handle="bob", invite_ttl_seconds=900)
        result = federation_peers.consume_peer_invite(
            db_path, token, federation_peers._now_iso()
        )
        assert result is not None
        invitation_id, handle = result
        assert handle == "bob"
        assert isinstance(invitation_id, int)

    def test_double_consume_returns_none(self, db_path):
        token = federation_peers.issue_peer_invite(db_path, handle="bob", invite_ttl_seconds=900)
        now = federation_peers._now_iso()
        first = federation_peers.consume_peer_invite(db_path, token, now)
        second = federation_peers.consume_peer_invite(db_path, token, now)
        assert first is not None
        assert second is None

    def test_expired_invite_returns_none(self, db_path):
        token = federation_peers.issue_peer_invite(db_path, handle="bob", invite_ttl_seconds=-10)
        result = federation_peers.consume_peer_invite(
            db_path, token, federation_peers._now_iso()
        )
        assert result is None

    def test_unknown_token_returns_none(self, db_path):
        result = federation_peers.consume_peer_invite(
            db_path, "pi_does-not-exist", federation_peers._now_iso()
        )
        assert result is None

    def test_was_already_redeemed_db_true_after_consume(self, db_path):
        token = federation_peers.issue_peer_invite(db_path, handle="bob", invite_ttl_seconds=900)
        now = federation_peers._now_iso()
        federation_peers.consume_peer_invite(db_path, token, now)
        assert federation_peers.was_peer_invite_already_redeemed_db(db_path, token) is True

    def test_was_already_redeemed_db_false_for_unused(self, db_path):
        token = federation_peers.issue_peer_invite(db_path, handle="bob", invite_ttl_seconds=900)
        assert federation_peers.was_peer_invite_already_redeemed_db(db_path, token) is False

    def test_mark_peer_invite_redeemed_links_peer_id(self, db_path, keypair):
        token = federation_peers.issue_peer_invite(db_path, handle="bob", invite_ttl_seconds=900)
        now = federation_peers._now_iso()
        invitation_id, handle = federation_peers.consume_peer_invite(db_path, token, now)
        fp = federation_peers.compute_fingerprint(keypair["public_jwk"])
        peer_id = federation_peers.add_peer(
            db_path, handle=handle, fingerprint=fp, key_jwk=keypair["public_jwk"], locator="https://x"
        )
        federation_peers.mark_peer_invite_redeemed(
            db_path, invitation_id=invitation_id, peer_id=peer_id
        )
        conn = db.get_connection(db_path)
        try:
            row = conn.execute(
                "SELECT redeemed_peer_id FROM peer_invitations WHERE id = ?", (invitation_id,)
            ).fetchone()
        finally:
            conn.close()
        assert row["redeemed_peer_id"] == peer_id

    def test_concurrent_consume_single_winner(self, db_path):
        """同一 peer 招待 token を複数スレッドが同時に消費しても勝者は1つだけになる。"""
        token = federation_peers.issue_peer_invite(db_path, handle="bob", invite_ttl_seconds=900)
        n_threads = 8
        barrier = threading.Barrier(n_threads)
        results: list = []
        results_lock = threading.Lock()

        def attempt():
            barrier.wait()
            result = federation_peers.consume_peer_invite(
                db_path, token, federation_peers._now_iso()
            )
            with results_lock:
                results.append(result)

        threads = [threading.Thread(target=attempt) for _ in range(n_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        winners = [r for r in results if r is not None]
        losers = [r for r in results if r is None]
        assert len(winners) == 1
        assert len(losers) == n_threads - 1
