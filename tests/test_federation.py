"""relay.federation テストスイート（`POST /federation/peers/redeem`、
`POST /federation/peers/enc-key`）。

招待ベース鍵ピン留めの正常系・token 一回性・自己署名検証・チャネルバインディング
（a_fp 照合）・rate limit・private locator の既定拒否と opt-in 許可・federation 機能
無効時の fail-closed、および既存 peer への envelope 暗号化鍵の追加登録を検証する。
"""
from __future__ import annotations

import time

import pytest
from joserfc.jwk import ECKey
from starlette.testclient import TestClient

from relay import federation_auth, federation_peers
from relay.app import create_app
from relay.config import Settings


def _generate_keypair() -> dict:
    key = ECKey.generate_key("P-256", private=True)
    return {
        "private_pem": key.as_pem(private=True).decode("ascii"),
        "public_jwk": key.as_dict(private=False),
    }


@pytest.fixture()
def keypair_a():
    return _generate_keypair()


@pytest.fixture()
def keypair_b():
    return _generate_keypair()


@pytest.fixture()
def enc_keypair_a():
    """A の envelope 暗号化鍵。署名鍵（keypair_a）とは無関係の別鍵。"""
    return _generate_keypair()


@pytest.fixture()
def enc_keypair_b():
    """B の envelope 暗号化鍵。署名鍵（keypair_b）とは無関係の別鍵。"""
    return _generate_keypair()


@pytest.fixture()
def settings(tmp_path, keypair_a):
    return Settings(
        db_path=str(tmp_path / "federation.db"),
        server_log_path=str(tmp_path / "federation.jsonl"),
        dispatcher_lock_path=str(tmp_path / "federation.lock"),
        jws_private_key_pem=keypair_a["private_pem"],
        federation_base_url="https://relay-a.example",
    )


@pytest.fixture()
def client(settings):
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def _issue_invite(settings, *, handle="bob", ttl_seconds=900):
    return federation_peers.issue_peer_invite(
        settings.db_path, handle=handle, invite_ttl_seconds=ttl_seconds
    )


def _own_fingerprint(settings) -> str:
    return federation_peers.compute_fingerprint(
        federation_peers.public_jwk_from_pem(settings.jws_private_key_pem)
    )


def _build_redeem_body(
    *,
    token: str,
    keypair_b: dict,
    a_fp: str,
    locator: str = "https://8.8.8.8",
    ts: int | None = None,
) -> dict:
    ts = ts if ts is not None else int(time.time())
    sig_payload = {"typ": "relay-fed-redeem", "token": token, "ts": ts, "a_fp": a_fp}
    sig = federation_peers.sign_detached(
        sig_payload, private_key_pem=keypair_b["private_pem"]
    )
    return {
        "invite_token": token,
        "ts": ts,
        "a_fp": a_fp,
        "card": {"key": keypair_b["public_jwk"], "locator": locator},
        "sig": sig,
    }


class TestRedeemSuccess:
    def test_pins_peer_and_returns_signed_response(self, client, settings, keypair_b):
        token = _issue_invite(settings)
        a_fp = _own_fingerprint(settings)
        body = _build_redeem_body(token=token, keypair_b=keypair_b, a_fp=a_fp)

        r = client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 200
        resp = r.json()
        assert resp["handle"] == "bob"
        assert resp["card"]["locator"] == settings.federation_base_url
        assert resp["card"]["key"] == federation_peers.public_jwk_from_pem(
            settings.jws_private_key_pem
        )

        # 応答署名が A 自身の公開鍵で検証できること。
        verify_payload = {
            "typ": "relay-fed-redeem-resp",
            "handle": resp["handle"],
            "card": resp["card"],
        }
        assert federation_peers.verify_detached(
            verify_payload, resp["sig"], public_key=resp["card"]["key"]
        )

        # peer が pin されていること。
        peer_fp = federation_peers.compute_fingerprint(keypair_b["public_jwk"])
        peer = federation_peers.get_peer_by_fingerprint(settings.db_path, peer_fp)
        assert peer is not None
        assert peer["handle"] == "bob"
        assert peer["locator"] == "https://8.8.8.8"
        assert peer["enc_key_jwk"] is None  # card.enc_key を送っていないので未設定。

    def test_card_enc_key_is_pinned_when_provided(
        self, client, settings, keypair_b, enc_keypair_b
    ):
        token = _issue_invite(settings)
        a_fp = _own_fingerprint(settings)
        body = _build_redeem_body(token=token, keypair_b=keypair_b, a_fp=a_fp)
        body["card"]["enc_key"] = enc_keypair_b["public_jwk"]

        r = client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 200

        peer_fp = federation_peers.compute_fingerprint(keypair_b["public_jwk"])
        peer = federation_peers.get_peer_by_fingerprint(settings.db_path, peer_fp)
        assert peer["enc_key_jwk"] == enc_keypair_b["public_jwk"]

    def test_response_includes_own_enc_key_when_configured(
        self, tmp_path, keypair_a, keypair_b, enc_keypair_a
    ):
        settings = Settings(
            db_path=str(tmp_path / "federation_enc.db"),
            server_log_path=str(tmp_path / "federation_enc.jsonl"),
            dispatcher_lock_path=str(tmp_path / "federation_enc.lock"),
            jws_private_key_pem=keypair_a["private_pem"],
            jwe_private_key_pem=enc_keypair_a["private_pem"],
            federation_base_url="https://relay-a.example",
        )
        app = create_app(settings)
        with TestClient(app) as c:
            token = _issue_invite(settings)
            a_fp = _own_fingerprint(settings)
            body = _build_redeem_body(token=token, keypair_b=keypair_b, a_fp=a_fp)
            r = c.post("/federation/peers/redeem", json=body)
        assert r.status_code == 200
        assert r.json()["card"]["enc_key"] == federation_peers.public_enc_jwk_from_pem(
            enc_keypair_a["private_pem"]
        )

    def test_response_omits_enc_key_when_not_configured(self, client, settings, keypair_b):
        token = _issue_invite(settings)
        a_fp = _own_fingerprint(settings)
        body = _build_redeem_body(token=token, keypair_b=keypair_b, a_fp=a_fp)
        r = client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 200
        assert "enc_key" not in r.json()["card"]

    def test_malformed_card_enc_key_returns_400_and_does_not_pin(
        self, client, settings, keypair_b
    ):
        token = _issue_invite(settings)
        a_fp = _own_fingerprint(settings)
        body = _build_redeem_body(token=token, keypair_b=keypair_b, a_fp=a_fp)
        body["card"]["enc_key"] = {"kty": "oct", "k": "not-an-ec-key"}

        r = client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 400
        assert r.json()["code"] == "InvalidRequestError"

        peer_fp = federation_peers.compute_fingerprint(keypair_b["public_jwk"])
        assert federation_peers.get_peer_by_fingerprint(settings.db_path, peer_fp) is None


class TestRedeemOneTimeUse:
    def test_second_redeem_returns_404(self, client, settings, keypair_b):
        token = _issue_invite(settings)
        a_fp = _own_fingerprint(settings)
        body = _build_redeem_body(token=token, keypair_b=keypair_b, a_fp=a_fp)

        first = client.post("/federation/peers/redeem", json=body)
        assert first.status_code == 200

        other_keypair = _generate_keypair()
        second_body = _build_redeem_body(token=token, keypair_b=other_keypair, a_fp=a_fp)
        second = client.post("/federation/peers/redeem", json=second_body)
        assert second.status_code == 404
        assert second.json()["code"] == "PeerInviteNotFoundError"


class TestRedeemExpiredAndUnknown:
    def test_expired_invite_returns_404(self, client, settings, keypair_b):
        token = _issue_invite(settings, ttl_seconds=-10)
        a_fp = _own_fingerprint(settings)
        body = _build_redeem_body(token=token, keypair_b=keypair_b, a_fp=a_fp)
        r = client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 404
        assert r.json()["code"] == "PeerInviteNotFoundError"

    def test_unknown_token_returns_404(self, client, settings, keypair_b):
        a_fp = _own_fingerprint(settings)
        body = _build_redeem_body(token="pi_does-not-exist", keypair_b=keypair_b, a_fp=a_fp)
        r = client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 404
        assert r.json()["code"] == "PeerInviteNotFoundError"

    def test_unknown_expired_and_already_redeemed_share_response(
        self, client, settings, keypair_b
    ):
        a_fp = _own_fingerprint(settings)
        used_token = _issue_invite(settings, handle="carol")
        used_body = _build_redeem_body(token=used_token, keypair_b=keypair_b, a_fp=a_fp)
        client.post("/federation/peers/redeem", json=used_body)

        expired_token = _issue_invite(settings, handle="dave", ttl_seconds=-10)
        expired_body = _build_redeem_body(
            token=expired_token, keypair_b=_generate_keypair(), a_fp=a_fp
        )

        r_unknown = client.post(
            "/federation/peers/redeem",
            json=_build_redeem_body(
                token="pi_nope", keypair_b=_generate_keypair(), a_fp=a_fp
            ),
        )
        r_expired = client.post("/federation/peers/redeem", json=expired_body)
        r_reused = client.post(
            "/federation/peers/redeem",
            json=_build_redeem_body(
                token=used_token, keypair_b=_generate_keypair(), a_fp=a_fp
            ),
        )

        for r in (r_unknown, r_expired, r_reused):
            assert r.status_code == 404
            assert r.json() == r_unknown.json()


class TestRedeemSignatureVerification:
    def test_tampered_signed_field_is_rejected(self, client, settings, keypair_b):
        token = _issue_invite(settings)
        a_fp = _own_fingerprint(settings)
        body = _build_redeem_body(token=token, keypair_b=keypair_b, a_fp=a_fp)
        body["ts"] = body["ts"] + 1  # 署名対象フィールドの改竄
        r = client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 401
        assert r.json()["code"] == "FederationSignatureInvalidError"

    def test_signature_by_unrelated_key_is_rejected(self, client, settings, keypair_b):
        token = _issue_invite(settings)
        a_fp = _own_fingerprint(settings)
        wrong_keypair = _generate_keypair()
        ts = int(time.time())
        sig_payload = {"typ": "relay-fed-redeem", "token": token, "ts": ts, "a_fp": a_fp}
        # 署名は無関係の鍵で行うが、card.key には keypair_b の公開鍵を載せる
        # （鍵所持証明の失敗パターン）。
        sig = federation_peers.sign_detached(
            sig_payload, private_key_pem=wrong_keypair["private_pem"]
        )
        body = {
            "invite_token": token,
            "ts": ts,
            "a_fp": a_fp,
            "card": {"key": keypair_b["public_jwk"], "locator": "https://8.8.8.8"},
            "sig": sig,
        }
        r = client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 401
        assert r.json()["code"] == "FederationSignatureInvalidError"

    def test_wrong_a_fp_is_rejected_and_not_pinned(self, client, settings, keypair_b):
        token = _issue_invite(settings)
        body = _build_redeem_body(token=token, keypair_b=keypair_b, a_fp="wrong-fingerprint")
        r = client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 401
        assert r.json()["code"] == "FederationSignatureInvalidError"

        peer_fp = federation_peers.compute_fingerprint(keypair_b["public_jwk"])
        assert federation_peers.get_peer_by_fingerprint(settings.db_path, peer_fp) is None


class TestRedeemLocatorGuard:
    def test_private_locator_rejected_by_default(self, client, settings, keypair_b):
        token = _issue_invite(settings)
        a_fp = _own_fingerprint(settings)
        body = _build_redeem_body(
            token=token, keypair_b=keypair_b, a_fp=a_fp, locator="https://127.0.0.1:9999"
        )
        r = client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 400

    def test_private_locator_allowed_when_opted_in(self, tmp_path, keypair_a, keypair_b):
        settings = Settings(
            db_path=str(tmp_path / "f2.db"),
            server_log_path=str(tmp_path / "f2.jsonl"),
            dispatcher_lock_path=str(tmp_path / "f2.lock"),
            jws_private_key_pem=keypair_a["private_pem"],
            federation_base_url="https://relay-a.example",
            federation_allow_private_locators=True,
        )
        app = create_app(settings)
        with TestClient(app) as c:
            token = _issue_invite(settings)
            a_fp = _own_fingerprint(settings)
            body = _build_redeem_body(
                token=token,
                keypair_b=keypair_b,
                a_fp=a_fp,
                locator="https://127.0.0.1:9999",
            )
            r = c.post("/federation/peers/redeem", json=body)
        assert r.status_code == 200


class TestRedeemMalformedBody:
    def test_missing_fields_returns_400(self, client):
        r = client.post("/federation/peers/redeem", json={})
        assert r.status_code == 400
        assert r.json()["code"] == "InvalidRequestError"

    def test_non_object_body_returns_400(self, client):
        r = client.post("/federation/peers/redeem", json=["nope"])
        assert r.status_code == 400

    def test_non_json_body_returns_400(self, client):
        r = client.post(
            "/federation/peers/redeem",
            content=b"not json",
            headers={"Content-Type": "application/json"},
        )
        assert r.status_code == 400

    def test_oversized_body_returns_400(self, client):
        oversized = b'{"invite_token": "' + b"a" * 5000 + b'"}'
        r = client.post(
            "/federation/peers/redeem",
            content=oversized,
            headers={"Content-Type": "application/json"},
        )
        assert r.status_code == 400


class TestRedeemRateLimit:
    @pytest.fixture()
    def limited_client(self, tmp_path, keypair_a):
        limited_settings = Settings(
            db_path=str(tmp_path / "rl.db"),
            server_log_path=str(tmp_path / "rl.jsonl"),
            dispatcher_lock_path=str(tmp_path / "rl.lock"),
            jws_private_key_pem=keypair_a["private_pem"],
            federation_base_url="https://relay-a.example",
        )
        app = create_app(limited_settings)
        with TestClient(app) as c:
            yield c

    def test_over_limit_returns_429(self, limited_client, keypair_b):
        body = _build_redeem_body(token="pi_nope", keypair_b=keypair_b, a_fp="x")
        for _ in range(5):
            r = limited_client.post("/federation/peers/redeem", json=body)
            assert r.status_code == 404
        r = limited_client.post("/federation/peers/redeem", json=body)
        assert r.status_code == 429
        assert r.json()["code"] == "RateLimitExceededError"
        assert "Retry-After" in r.headers


class TestFederationDisabled:
    def test_redeem_returns_503_without_federation_key(self, tmp_path, keypair_b):
        settings = Settings(
            db_path=str(tmp_path / "nokey.db"),
            server_log_path=str(tmp_path / "nokey.jsonl"),
            dispatcher_lock_path=str(tmp_path / "nokey.lock"),
        )
        app = create_app(settings)
        with TestClient(app) as c:
            body = _build_redeem_body(token="pi_nope", keypair_b=keypair_b, a_fp="x")
            r = c.post("/federation/peers/redeem", json=body)
        assert r.status_code == 503
        assert r.json()["code"] == "FederationDisabledError"

    def test_redeem_returns_503_without_base_url(self, tmp_path, keypair_a, keypair_b):
        """鍵はあっても RELAY_BASE_URL 未設定なら無効化する（応答 locator が null になるのを防ぐ）。"""
        settings = Settings(
            db_path=str(tmp_path / "nourl.db"),
            server_log_path=str(tmp_path / "nourl.jsonl"),
            dispatcher_lock_path=str(tmp_path / "nourl.lock"),
            jws_private_key_pem=keypair_a["private_pem"],
        )
        app = create_app(settings)
        with TestClient(app) as c:
            body = _build_redeem_body(token="pi_nope", keypair_b=keypair_b, a_fp="x")
            r = c.post("/federation/peers/redeem", json=body)
        assert r.status_code == 503
        assert r.json()["code"] == "FederationDisabledError"


ENC_KEY_PATH = "/federation/peers/enc-key"


def _post_enc_key(client, *, settings, sender_keypair, body: dict):
    """`ENC_KEY_PATH` へ `sender_keypair` の署名付きリクエストを送る（`require_federation_authn`
    が要求する Relay-JWS を組み立てる、tests/test_federation_inbound.py `_post_message` と同型）。
    """
    import json

    body_bytes = json.dumps(body).encode("utf-8")
    headers = federation_auth.sign_federation_request(
        method="POST",
        path=ENC_KEY_PATH,
        body=body_bytes,
        origin_fp=federation_peers.compute_fingerprint(sender_keypair["public_jwk"]),
        destination_fp=_own_fingerprint(settings),
        private_key_pem=sender_keypair["private_pem"],
    )
    return client.post(ENC_KEY_PATH, content=body_bytes, headers=headers)


class TestEncKeyEndpoint:
    """`POST /federation/peers/enc-key`: 既存 pin 済み peer への envelope 暗号化鍵の追加登録。"""

    @pytest.fixture()
    def pinned_bob(self, settings, keypair_b):
        fp = federation_peers.compute_fingerprint(keypair_b["public_jwk"])
        federation_peers.add_peer(
            settings.db_path,
            handle="bob",
            fingerprint=fp,
            key_jwk=keypair_b["public_jwk"],
            locator="https://8.8.8.8",
        )
        return fp

    def test_registers_callers_enc_key_without_redoing_invite(
        self, client, settings, keypair_b, enc_keypair_b, pinned_bob
    ):
        r = _post_enc_key(
            client,
            settings=settings,
            sender_keypair=keypair_b,
            body={"enc_key": enc_keypair_b["public_jwk"]},
        )
        assert r.status_code == 200
        assert r.json()["handle"] == "bob"

        peer = federation_peers.get_peer_by_fingerprint(settings.db_path, pinned_bob)
        assert peer["enc_key_jwk"] == enc_keypair_b["public_jwk"]
        # 署名鍵・locator 等、既存の pin はそのまま（招待をやり直していない）。
        assert peer["key_jwk"] == keypair_b["public_jwk"]
        assert peer["locator"] == "https://8.8.8.8"

    def test_response_echoes_own_enc_key_for_single_round_trip_exchange(
        self, tmp_path, keypair_a, keypair_b, enc_keypair_a, enc_keypair_b
    ):
        """A に暗号化鍵が設定済みなら、B → A の 1 リクエストで双方向に鍵が揃う
        （応答で A の enc_key を返し、B 側 CLI がその場で pin できる設計）。"""
        settings = Settings(
            db_path=str(tmp_path / "enc_roundtrip.db"),
            server_log_path=str(tmp_path / "enc_roundtrip.jsonl"),
            dispatcher_lock_path=str(tmp_path / "enc_roundtrip.lock"),
            jws_private_key_pem=keypair_a["private_pem"],
            jwe_private_key_pem=enc_keypair_a["private_pem"],
            federation_base_url="https://relay-a.example",
        )
        fp_b = federation_peers.compute_fingerprint(keypair_b["public_jwk"])
        app = create_app(settings)
        with TestClient(app) as c:
            federation_peers.add_peer(
                settings.db_path,
                handle="bob",
                fingerprint=fp_b,
                key_jwk=keypair_b["public_jwk"],
                locator="https://8.8.8.8",
            )
            r = _post_enc_key(
                c,
                settings=settings,
                sender_keypair=keypair_b,
                body={"enc_key": enc_keypair_b["public_jwk"]},
            )
        assert r.status_code == 200
        assert r.json()["enc_key"] == federation_peers.public_enc_jwk_from_pem(
            enc_keypair_a["private_pem"]
        )

    def test_response_omits_enc_key_when_caller_side_not_configured(
        self, client, settings, keypair_b, enc_keypair_b, pinned_bob
    ):
        """A（受信側）が暗号化鍵未設定なら応答に enc_key を含めない（B 側は自分の鍵だけ登録される）。"""
        r = _post_enc_key(
            client,
            settings=settings,
            sender_keypair=keypair_b,
            body={"enc_key": enc_keypair_b["public_jwk"]},
        )
        assert r.status_code == 200
        assert "enc_key" not in r.json()

    def test_unknown_peer_is_rejected(self, client, settings, keypair_b, enc_keypair_b):
        """未 pin の相手からのリクエストは `require_federation_authn` の認証段階で 401 になる
        （招待済みの既存 peer 関係を前提にした endpoint であり、未知の相手は redeem を先に通る
        必要がある）。"""
        r = _post_enc_key(
            client,
            settings=settings,
            sender_keypair=keypair_b,
            body={"enc_key": enc_keypair_b["public_jwk"]},
        )
        assert r.status_code == 401

    def test_malformed_enc_key_returns_400_and_does_not_update(
        self, client, settings, keypair_b, pinned_bob
    ):
        r = _post_enc_key(
            client,
            settings=settings,
            sender_keypair=keypair_b,
            body={"enc_key": {"kty": "oct", "k": "nope"}},
        )
        assert r.status_code == 400
        assert r.json()["code"] == "InvalidRequestError"

        peer = federation_peers.get_peer_by_fingerprint(settings.db_path, pinned_bob)
        assert peer["enc_key_jwk"] is None

    def test_missing_enc_key_field_returns_400(self, client, settings, keypair_b, pinned_bob):
        r = _post_enc_key(client, settings=settings, sender_keypair=keypair_b, body={})
        assert r.status_code == 400

    def test_can_update_previously_registered_enc_key(
        self, client, settings, keypair_b, enc_keypair_b, pinned_bob
    ):
        """鍵ローテーション: 既に enc_key を登録済みの peer が再度呼ぶと上書きされる。"""
        first = _post_enc_key(
            client,
            settings=settings,
            sender_keypair=keypair_b,
            body={"enc_key": enc_keypair_b["public_jwk"]},
        )
        assert first.status_code == 200

        rotated = _generate_keypair()
        second = _post_enc_key(
            client, settings=settings, sender_keypair=keypair_b, body={"enc_key": rotated["public_jwk"]}
        )
        assert second.status_code == 200

        peer = federation_peers.get_peer_by_fingerprint(settings.db_path, pinned_bob)
        assert peer["enc_key_jwk"] == rotated["public_jwk"]
