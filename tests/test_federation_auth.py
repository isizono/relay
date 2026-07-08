"""relay.federation_auth テストスイート（Relay-JWS 署名・検証）。

正常署名往復・payload 各フィールド（method・path・body・destination）の改竄検出・nonce
リプレイ拒否・時計ずれ 401 と自動補正・未知 kid / revoked peer の一様 401・typ 混用
（redeem 署名の req 流用）拒否・unpin 後の即 401 を検証する。

`require_federation_authn` の検証は federation サーフェス自体の実 endpoint に依存させず、
専用の小さな Starlette アプリを組み立てて行う。
"""
from __future__ import annotations

import json

import pytest
import rfc8785
from joserfc.jwk import ECKey
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route
from starlette.testclient import TestClient

from relay import db, federation_auth, federation_peers
from relay.config import Settings
from relay.ratelimit import RateLimiter

ECHO_PATH = "/federation/echo"
ECHO_BODY = b'{"ok": true}'


def _generate_keypair() -> dict:
    key = ECKey.generate_key("P-256", private=True)
    return {
        "private_pem": key.as_pem(private=True).decode("ascii"),
        "public_jwk": key.as_dict(private=False),
    }


def _fp(keypair: dict) -> str:
    return federation_peers.compute_fingerprint(keypair["public_jwk"])


@pytest.fixture()
def keypair_a():
    return _generate_keypair()


@pytest.fixture()
def keypair_b():
    return _generate_keypair()


@pytest.fixture()
def settings(tmp_path, keypair_b):
    """B（受信側）の Settings。"""
    db_path = str(tmp_path / "auth.db")
    db.init_db(db_path)
    return Settings(
        db_path=db_path,
        server_log_path=str(tmp_path / "auth.jsonl"),
        dispatcher_lock_path=str(tmp_path / "auth.lock"),
        jws_private_key_pem=keypair_b["private_pem"],
    )


@pytest.fixture()
def pinned_peer_fp(settings, keypair_a):
    """A を B 側 peers に pin 済みにしておく。"""
    fp_a = _fp(keypair_a)
    federation_peers.add_peer(
        settings.db_path,
        handle="alice",
        fingerprint=fp_a,
        key_jwk=keypair_a["public_jwk"],
        locator="https://relay-a.example",
    )
    return fp_a


async def _echo_handler(request: Request) -> Response:
    peer = request.state.peer_identity
    return JSONResponse({"handle": peer.handle, "fingerprint": peer.fingerprint})


@pytest.fixture()
def app(settings):
    routes = [
        Route(ECHO_PATH, federation_auth.require_federation_authn(_echo_handler), methods=["POST"])
    ]
    starlette_app = Starlette(routes=routes)
    starlette_app.state.settings = settings
    starlette_app.state.federation_nonce_cache = federation_auth.NonceCache()
    starlette_app.state.federation_request_rate_limiter = RateLimiter(1000)
    return starlette_app


@pytest.fixture()
def client(app):
    with TestClient(app) as c:
        yield c


def _sign(
    *,
    keypair_sender: dict,
    destination_fp: str,
    method: str = "POST",
    path: str = ECHO_PATH,
    body: bytes = ECHO_BODY,
    ts: int | None = None,
) -> dict[str, str]:
    return federation_auth.sign_federation_request(
        method=method,
        path=path,
        body=body,
        origin_fp=_fp(keypair_sender),
        destination_fp=destination_fp,
        private_key_pem=keypair_sender["private_pem"],
        ts=ts,
    )


def _tamper_payload(headers: dict[str, str], **overrides) -> dict[str, str]:
    """署名済みヘッダーの payload 部分だけ書き換える（signature は元のまま）。"""
    payload = json.loads(federation_auth._b64url_decode(headers[federation_auth.PAYLOAD_HEADER]))
    payload.update(overrides)
    tampered = dict(headers)
    tampered[federation_auth.PAYLOAD_HEADER] = federation_auth._b64url_encode(
        json.dumps(payload).encode("utf-8")
    )
    return tampered


class TestSignAndVerifyRoundTrip:
    def test_valid_request_returns_200_with_peer_identity(
        self, client, keypair_a, keypair_b, pinned_peer_fp
    ):
        headers = _sign(keypair_sender=keypair_a, destination_fp=_fp(keypair_b))
        r = client.post(ECHO_PATH, content=ECHO_BODY, headers=headers)
        assert r.status_code == 200
        assert r.json() == {"handle": "alice", "fingerprint": pinned_peer_fp}


class TestFieldTamperingDetected:
    def test_tampered_method_rejected(self, client, keypair_a, keypair_b, pinned_peer_fp):
        headers = _sign(keypair_sender=keypair_a, destination_fp=_fp(keypair_b))
        tampered = _tamper_payload(headers, method="DELETE")
        r = client.post(ECHO_PATH, content=ECHO_BODY, headers=tampered)
        assert r.status_code == 401

    def test_tampered_path_rejected(self, client, keypair_a, keypair_b, pinned_peer_fp):
        headers = _sign(keypair_sender=keypair_a, destination_fp=_fp(keypair_b))
        tampered = _tamper_payload(headers, path="/federation/other")
        r = client.post(ECHO_PATH, content=ECHO_BODY, headers=tampered)
        assert r.status_code == 401

    def test_tampered_destination_rejected(self, client, keypair_a, keypair_b, pinned_peer_fp):
        headers = _sign(keypair_sender=keypair_a, destination_fp=_fp(keypair_b))
        tampered = _tamper_payload(headers, destination="not-my-fingerprint")
        r = client.post(ECHO_PATH, content=ECHO_BODY, headers=tampered)
        assert r.status_code == 401

    def test_tampered_body_mismatches_content_sha256(
        self, client, keypair_a, keypair_b, pinned_peer_fp
    ):
        headers = _sign(keypair_sender=keypair_a, destination_fp=_fp(keypair_b))
        # payload はそのまま、実際に送る body だけ変える（content_sha256 不一致）。
        r = client.post(ECHO_PATH, content=b'{"ok": false}', headers=headers)
        assert r.status_code == 401

    def test_tampered_signature_itself_rejected(self, client, keypair_a, keypair_b, pinned_peer_fp):
        headers = _sign(keypair_sender=keypair_a, destination_fp=_fp(keypair_b))
        auth = headers[federation_auth.AUTHORIZATION_HEADER]
        protected, _sep, signature = auth.removeprefix("Relay-JWS ").partition("..")
        tampered = dict(headers)
        tampered[federation_auth.AUTHORIZATION_HEADER] = f"Relay-JWS {protected}..{signature[:-4]}xxxx"
        r = client.post(ECHO_PATH, content=ECHO_BODY, headers=tampered)
        assert r.status_code == 401


class TestNonceReplay:
    def test_replayed_request_rejected(self, client, keypair_a, keypair_b, pinned_peer_fp):
        headers = _sign(keypair_sender=keypair_a, destination_fp=_fp(keypair_b))
        first = client.post(ECHO_PATH, content=ECHO_BODY, headers=headers)
        assert first.status_code == 200
        second = client.post(ECHO_PATH, content=ECHO_BODY, headers=headers)
        assert second.status_code == 401


class TestClockSkew:
    def test_stale_ts_rejected_with_skew_info(self, client, keypair_a, keypair_b, pinned_peer_fp):
        headers = _sign(
            keypair_sender=keypair_a,
            destination_fp=_fp(keypair_b),
            ts=federation_auth._now_unix() - 1000,
        )
        r = client.post(ECHO_PATH, content=ECHO_BODY, headers=headers)
        assert r.status_code == 401
        body = r.json()
        assert "ts_skew" in body
        assert "server_time" in body

    def test_corrected_ts_retry_succeeds(self, client, keypair_a, keypair_b, pinned_peer_fp):
        stale_headers = _sign(
            keypair_sender=keypair_a,
            destination_fp=_fp(keypair_b),
            ts=federation_auth._now_unix() - 1000,
        )
        stale = client.post(ECHO_PATH, content=ECHO_BODY, headers=stale_headers)
        assert stale.status_code == 401
        server_time = stale.json()["server_time"]

        # 送信側は応答の server_time を基準に ts を補正して 1 回だけ再試行する
        # （nonce は毎回新規生成されるため、再送はリプレイ扱いにならない）。
        corrected_headers = _sign(
            keypair_sender=keypair_a, destination_fp=_fp(keypair_b), ts=server_time
        )
        retried = client.post(ECHO_PATH, content=ECHO_BODY, headers=corrected_headers)
        assert retried.status_code == 200


class TestUnknownAndRevokedPeer:
    def test_unknown_kid_returns_401(self, client, keypair_b):
        unknown_keypair = _generate_keypair()  # pin していない鍵
        headers = _sign(keypair_sender=unknown_keypair, destination_fp=_fp(keypair_b))
        r = client.post(ECHO_PATH, content=ECHO_BODY, headers=headers)
        assert r.status_code == 401

    def test_revoked_peer_returns_401_immediately(
        self, client, keypair_a, keypair_b, settings, pinned_peer_fp
    ):
        headers_before = _sign(keypair_sender=keypair_a, destination_fp=_fp(keypair_b))
        before = client.post(ECHO_PATH, content=ECHO_BODY, headers=headers_before)
        assert before.status_code == 200

        federation_peers.revoke_peer(settings.db_path, handle="alice")

        headers_after = _sign(keypair_sender=keypair_a, destination_fp=_fp(keypair_b))
        after = client.post(ECHO_PATH, content=ECHO_BODY, headers=headers_after)
        assert after.status_code == 401


class TestTypConfusionRejected:
    def test_redeem_signature_typ_is_rejected_as_request_auth(
        self, client, keypair_a, keypair_b, pinned_peer_fp
    ):
        """redemption 用署名（typ: relay-fed-redeem）を Relay-JWS リクエスト認証に流用できない。"""
        fp_a = _fp(keypair_a)
        fp_b = _fp(keypair_b)
        payload = {
            "typ": "relay-fed-redeem",
            "v": 1,
            "method": "POST",
            "path": ECHO_PATH,
            "origin": fp_a,
            "destination": fp_b,
            "content_sha256": federation_auth._sha256_b64url(ECHO_BODY),
            "ts": federation_auth._now_unix(),
            "nonce": "reused-typ-nonce",
        }
        sig = federation_peers.sign_detached(
            payload, private_key_pem=keypair_a["private_pem"], kid=fp_a
        )
        headers = {
            federation_auth.AUTHORIZATION_HEADER: f"Relay-JWS {sig['protected']}..{sig['signature']}",
            federation_auth.PAYLOAD_HEADER: federation_auth._b64url_encode(
                rfc8785.dumps(payload)
            ),
        }
        r = client.post(ECHO_PATH, content=ECHO_BODY, headers=headers)
        assert r.status_code == 401


class TestMalformedAuthorizationHeader:
    def test_missing_authorization_header(self, client):
        r = client.post(ECHO_PATH, content=ECHO_BODY)
        assert r.status_code == 401

    def test_wrong_scheme_rejected(self, client, keypair_a, keypair_b, pinned_peer_fp):
        headers = _sign(keypair_sender=keypair_a, destination_fp=_fp(keypair_b))
        bad = dict(headers)
        bad[federation_auth.AUTHORIZATION_HEADER] = bad[
            federation_auth.AUTHORIZATION_HEADER
        ].replace("Relay-JWS", "Bearer")
        r = client.post(ECHO_PATH, content=ECHO_BODY, headers=bad)
        assert r.status_code == 401

    def test_missing_payload_header_rejected(self, client, keypair_a, keypair_b, pinned_peer_fp):
        headers = _sign(keypair_sender=keypair_a, destination_fp=_fp(keypair_b))
        del headers[federation_auth.PAYLOAD_HEADER]
        r = client.post(ECHO_PATH, content=ECHO_BODY, headers=headers)
        assert r.status_code == 401
