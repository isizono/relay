"""relay.federation_inbound テストスイート（`POST /federation/streams/{id}/messages`）。

federation v1 設計確定版「受信(replica側B)」節のエッジケース #1-#12 を検証する: replica
自動生成・dedup・owner stream への返信受け入れ・publisher_identity 強制刻印・local member
限定 fan-out（echo 防止）・envelope 必須フィールド検証・body size cap・per-peer rate limit・
StreamRecord の origin_peer/origin_stream_id/creator_identity 設定・membership の add-only
self-healing を扱う。put_member 自体の peer namespace 検証（#6/#7）は
`tests/test_streams.py::TestMembersPeerNamespaceValidation` を参照。
"""
from __future__ import annotations

import json
import sqlite3

import pytest
from joserfc.jwk import ECKey
from starlette.testclient import TestClient

from relay import db, federation_auth, federation_peers, streams
from relay.app import create_app
from relay.config import Settings
from relay.ratelimit import RateLimiter


def _generate_keypair() -> dict:
    key = ECKey.generate_key("P-256", private=True)
    return {
        "private_pem": key.as_pem(private=True).decode("ascii"),
        "public_jwk": key.as_dict(private=False),
    }


def _fp(keypair: dict) -> str:
    return federation_peers.compute_fingerprint(keypair["public_jwk"])


def _own_fp(settings: Settings) -> str:
    return federation_peers.compute_fingerprint(
        federation_peers.public_jwk_from_pem(settings.jws_private_key_pem)
    )


def _envelope(
    *,
    origin_stream_id: str,
    origin_publish_id: int,
    from_sub: str = "orch",
    to_members: list[str] | None = None,
    body: str = "hello",
) -> dict:
    return {
        "origin_stream_id": origin_stream_id,
        "origin_publish_id": origin_publish_id,
        "from_sub": from_sub,
        "to_members": to_members if to_members is not None else ["orch"],
        "body": body,
    }


def _post_message(client, settings_b, keypair_sender, *, path, envelope=None, raw_body=None):
    body_bytes = raw_body if raw_body is not None else json.dumps(envelope).encode("utf-8")
    headers = federation_auth.sign_federation_request(
        method="POST",
        path=path,
        body=body_bytes,
        origin_fp=_fp(keypair_sender),
        destination_fp=_own_fp(settings_b),
        private_key_pem=keypair_sender["private_pem"],
    )
    return client.post(path, content=body_bytes, headers=headers)


def _outbox_rows(settings: Settings, stream_id: str) -> list[sqlite3.Row]:
    conn = sqlite3.connect(settings.db_path)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(
            "SELECT member_identity, publish_id FROM outbox"
            " WHERE target_type = 'stream' AND stream_id = ?",
            (stream_id,),
        ).fetchall()
    finally:
        conn.close()


def _publish_log_row(settings: Settings, publish_id: int) -> sqlite3.Row | None:
    conn = sqlite3.connect(settings.db_path)
    conn.row_factory = sqlite3.Row
    try:
        return conn.execute(
            "SELECT * FROM publish_log WHERE publish_id = ?", (publish_id,)
        ).fetchone()
    finally:
        conn.close()


def _dedup_count(settings: Settings) -> int:
    conn = sqlite3.connect(settings.db_path)
    try:
        return conn.execute("SELECT COUNT(*) FROM federation_inbound_dedup").fetchone()[0]
    finally:
        conn.close()


MESSAGES_PATH = "/federation/streams/orch:collab/messages"


@pytest.fixture()
def keypair_a():
    return _generate_keypair()


@pytest.fixture()
def keypair_b():
    return _generate_keypair()


@pytest.fixture()
def enc_keypair_b():
    """B（受信側）の envelope 復号鍵。署名鍵（keypair_b）とは無関係の別鍵。"""
    key = ECKey.generate_key("P-256", private=True)
    return {
        "private_pem": key.as_pem(private=True).decode("ascii"),
        "public_jwk": key.as_dict(private=False),
    }


@pytest.fixture()
def settings_b(tmp_path, keypair_b):
    """受信側 B の Settings。`pinned_peer_a` 等が app 起動前に db を触るため、ここで
    migration を適用しておく（`test_federation_auth.py` の settings fixture と同型）。"""
    db_path = str(tmp_path / "b.db")
    db.init_db(db_path)
    return Settings(
        db_path=db_path,
        server_log_path=str(tmp_path / "b.jsonl"),
        dispatcher_lock_path=str(tmp_path / "b.lock"),
        jws_private_key_pem=keypair_b["private_pem"],
        federation_base_url="https://relay-b.example",
        auth_tokens={"tok-orch": "orch"},
    )


@pytest.fixture()
def app_b(settings_b):
    return create_app(settings_b)


@pytest.fixture()
def client_b(app_b):
    with TestClient(app_b) as c:
        yield c


@pytest.fixture()
def pinned_peer_a(settings_b, keypair_a):
    """A を B 側 peers に handle="alice" として pin 済みにする。"""
    fp_a = _fp(keypair_a)
    federation_peers.add_peer(
        settings_b.db_path,
        handle="alice",
        fingerprint=fp_a,
        key_jwk=keypair_a["public_jwk"],
        locator="https://relay-a.example",
    )
    return fp_a


def _local_auth() -> dict:
    return {"Authorization": "Bearer tok-orch"}


class TestReplicaAutoGeneration:
    """edge case #1 / #11: 未知 stream 宛メッセージの replica 自動生成。"""

    def test_unknown_stream_creates_replica_and_delivers(
        self, client_b, settings_b, app_b, keypair_a, pinned_peer_a
    ):
        envelope = _envelope(origin_stream_id="orch:collab", origin_publish_id=1)
        r = _post_message(client_b, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)
        assert r.status_code == 202
        publish_id = r.json()["publish_id"]

        registry = streams.get_registry_from_state(app_b.state)
        replica = registry.get("orch@alice:collab")
        assert replica is not None
        assert replica.creator_identity == "@alice"
        assert replica.origin_peer == "alice"
        assert replica.origin_stream_id == "orch:collab"

        rows = _outbox_rows(settings_b, "orch@alice:collab")
        assert (rows[0]["member_identity"], rows[0]["publish_id"]) == ("orch", publish_id)

    def test_replica_fields_reconstructed_identically_after_restart(
        self, settings_b, keypair_a, pinned_peer_a
    ):
        """restart 相当（新規 app インスタンス = in-memory registry リセット）でも同じ
        origin_peer/origin_stream_id/creator_identity で replica が再構築される。"""
        app1 = create_app(settings_b)
        with TestClient(app1) as c1:
            r1 = _post_message(
                c1,
                settings_b,
                keypair_a,
                path=MESSAGES_PATH,
                envelope=_envelope(origin_stream_id="orch:collab", origin_publish_id=1),
            )
            assert r1.status_code == 202

        # 新規 app インスタンス（同一 db_path、in-memory registry は空）= restart 相当。
        # 未処理の origin_publish_id（=2）を送るので dedup には引っかからない。
        app2 = create_app(settings_b)
        with TestClient(app2) as c2:
            r2 = _post_message(
                c2,
                settings_b,
                keypair_a,
                path=MESSAGES_PATH,
                envelope=_envelope(origin_stream_id="orch:collab", origin_publish_id=2),
            )
            assert r2.status_code == 202
            registry2 = streams.get_registry_from_state(app2.state)
            replica2 = registry2.get("orch@alice:collab")
            assert replica2 is not None
            assert replica2.creator_identity == "@alice"
            assert replica2.origin_peer == "alice"
            assert replica2.origin_stream_id == "orch:collab"


class TestDedup:
    """edge case #2: (origin_fingerprint, origin_publish_id) の disk 永続 dedup。"""

    def test_duplicate_origin_publish_id_returns_same_202_without_double_write(
        self, client_b, settings_b, keypair_a, pinned_peer_a
    ):
        envelope = _envelope(origin_stream_id="orch:collab", origin_publish_id=42)
        r1 = _post_message(client_b, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)
        r2 = _post_message(client_b, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)

        assert r1.status_code == 202
        assert r2.status_code == 202
        assert r1.json()["publish_id"] == r2.json()["publish_id"]
        assert _dedup_count(settings_b) == 1
        assert len(_outbox_rows(settings_b, "orch@alice:collab")) == 1

    def test_different_origin_publish_id_is_not_deduped(
        self, client_b, settings_b, keypair_a, pinned_peer_a
    ):
        r1 = _post_message(
            client_b,
            settings_b,
            keypair_a,
            path=MESSAGES_PATH,
            envelope=_envelope(origin_stream_id="orch:collab", origin_publish_id=1),
        )
        r2 = _post_message(
            client_b,
            settings_b,
            keypair_a,
            path=MESSAGES_PATH,
            envelope=_envelope(origin_stream_id="orch:collab", origin_publish_id=2),
        )
        assert r1.json()["publish_id"] != r2.json()["publish_id"]
        assert _dedup_count(settings_b) == 2


class TestReplyToOwnerStream:
    """edge case #3: owner stream への返信受け入れ（origin peer namespace の write member 確認）。"""

    def _create_owner_stream(self, client_b, name: str) -> str:
        r = client_b.post("/streams", json={"name": name}, headers=_local_auth())
        assert r.status_code == 201
        return r.json()["stream_id"]

    def test_write_member_of_origin_peer_namespace_accepts_reply(
        self, client_b, settings_b, keypair_a, pinned_peer_a
    ):
        stream_id = self._create_owner_stream(client_b, "s1")
        r_member = client_b.put(
            f"/streams/{stream_id}/members",
            json={"identity": "orch@alice", "access": "read_write"},
            headers=_local_auth(),
        )
        assert r_member.status_code == 200

        envelope = _envelope(origin_stream_id=stream_id, origin_publish_id=1)
        r = _post_message(
            client_b,
            settings_b,
            keypair_a,
            path=f"/federation/streams/{stream_id}/messages",
            envelope=envelope,
        )
        assert r.status_code == 202
        row = _publish_log_row(settings_b, r.json()["publish_id"])
        assert row["publisher_identity"] == "orch@alice"
        assert row["stream_id"] == stream_id

    def test_no_write_member_of_origin_peer_namespace_returns_404(
        self, client_b, settings_b, keypair_a, pinned_peer_a
    ):
        stream_id = self._create_owner_stream(client_b, "s2")
        # "@alice" の write member を一切追加しない。

        envelope = _envelope(origin_stream_id=stream_id, origin_publish_id=1)
        r = _post_message(
            client_b,
            settings_b,
            keypair_a,
            path=f"/federation/streams/{stream_id}/messages",
            envelope=envelope,
        )
        assert r.status_code == 404

    def test_read_only_member_of_origin_peer_namespace_returns_404(
        self, client_b, settings_b, keypair_a, pinned_peer_a
    ):
        """write 権限を持たない（read のみの）peer namespace member は返信を受け入れない。"""
        stream_id = self._create_owner_stream(client_b, "s3")
        client_b.put(
            f"/streams/{stream_id}/members",
            json={"identity": "orch@alice", "access": "read"},
            headers=_local_auth(),
        )
        envelope = _envelope(origin_stream_id=stream_id, origin_publish_id=1)
        r = _post_message(
            client_b,
            settings_b,
            keypair_a,
            path=f"/federation/streams/{stream_id}/messages",
            envelope=envelope,
        )
        assert r.status_code == 404


class TestPublisherIdentityForced:
    """edge case #4: publisher_identity は relay が {from_sub}@{handle} に強制刻印する。"""

    def test_self_reported_publisher_identity_field_is_ignored(
        self, client_b, settings_b, keypair_a, pinned_peer_a
    ):
        envelope = _envelope(origin_stream_id="orch:collab", origin_publish_id=1, from_sub="orch")
        envelope["publisher_identity"] = "totally-spoofed@nobody"  # envelope スキーマ外のフィールド
        r = _post_message(client_b, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)
        assert r.status_code == 202
        row = _publish_log_row(settings_b, r.json()["publish_id"])
        assert row["publisher_identity"] == "orch@alice"

    def test_different_from_sub_reflected_in_publisher_identity(
        self, client_b, settings_b, keypair_a, pinned_peer_a
    ):
        envelope = _envelope(
            origin_stream_id="orch:collab",
            origin_publish_id=1,
            from_sub="researcher",
            to_members=["researcher"],
        )
        r = _post_message(client_b, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)
        assert r.status_code == 202
        row = _publish_log_row(settings_b, r.json()["publish_id"])
        assert row["publisher_identity"] == "researcher@alice"


class TestLocalOnlyFanOut:
    """edge case #5: fan-out は local member のみ（peer member への echo 禁止）。"""

    def test_peer_member_does_not_receive_fanout(
        self, client_b, settings_b, keypair_a, pinned_peer_a
    ):
        envelope = _envelope(origin_stream_id="orch:collab", origin_publish_id=1, to_members=["orch"])
        r = _post_message(client_b, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)
        assert r.status_code == 202
        member_identities = {row["member_identity"] for row in _outbox_rows(settings_b, "orch@alice:collab")}
        assert member_identities == {"orch"}


class TestEnvelopeValidation:
    """edge case #8: envelope 必須フィールドの欠落・型不正は 400。"""

    def test_missing_origin_stream_id_returns_400(self, client_b, settings_b, keypair_a, pinned_peer_a):
        envelope = _envelope(origin_stream_id="orch:collab", origin_publish_id=1)
        del envelope["origin_stream_id"]
        r = _post_message(client_b, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)
        assert r.status_code == 400
        assert r.json()["code"] == "InvalidRequestError"

    def test_origin_stream_id_mismatching_path_returns_400(
        self, client_b, settings_b, keypair_a, pinned_peer_a
    ):
        envelope = _envelope(origin_stream_id="orch:other", origin_publish_id=1)
        r = _post_message(client_b, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)
        assert r.status_code == 400

    def test_non_integer_origin_publish_id_returns_400(
        self, client_b, settings_b, keypair_a, pinned_peer_a
    ):
        envelope = _envelope(origin_stream_id="orch:collab", origin_publish_id=1)
        envelope["origin_publish_id"] = "not-an-int"
        r = _post_message(client_b, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)
        assert r.status_code == 400

    def test_missing_from_sub_returns_400(self, client_b, settings_b, keypair_a, pinned_peer_a):
        envelope = _envelope(origin_stream_id="orch:collab", origin_publish_id=1)
        del envelope["from_sub"]
        r = _post_message(client_b, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)
        assert r.status_code == 400

    def test_to_members_containing_at_sign_returns_400(
        self, client_b, settings_b, keypair_a, pinned_peer_a
    ):
        envelope = _envelope(
            origin_stream_id="orch:collab", origin_publish_id=1, to_members=["orch@bob"]
        )
        r = _post_message(client_b, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)
        assert r.status_code == 400

    def test_empty_to_members_returns_400(self, client_b, settings_b, keypair_a, pinned_peer_a):
        envelope = _envelope(origin_stream_id="orch:collab", origin_publish_id=1, to_members=[])
        r = _post_message(client_b, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)
        assert r.status_code == 400

    def test_empty_body_returns_400(self, client_b, settings_b, keypair_a, pinned_peer_a):
        envelope = _envelope(origin_stream_id="orch:collab", origin_publish_id=1, body="")
        r = _post_message(client_b, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)
        assert r.status_code == 400

    def test_non_object_body_returns_400(self, client_b, settings_b, keypair_a, pinned_peer_a):
        r = _post_message(
            client_b, settings_b, keypair_a, path=MESSAGES_PATH, raw_body=b"[1, 2, 3]"
        )
        assert r.status_code == 400

    def test_malformed_json_returns_400(self, client_b, settings_b, keypair_a, pinned_peer_a):
        r = _post_message(
            client_b, settings_b, keypair_a, path=MESSAGES_PATH, raw_body=b"not json"
        )
        assert r.status_code == 400

    def test_body_and_body_jwe_both_present_returns_400(
        self, client_b, settings_b, keypair_a, pinned_peer_a
    ):
        envelope = _envelope(origin_stream_id="orch:collab", origin_publish_id=1)
        envelope["body_jwe"] = "irrelevant-for-this-check"
        r = _post_message(client_b, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)
        assert r.status_code == 400

    def test_empty_body_jwe_returns_400(self, client_b, settings_b, keypair_a, pinned_peer_a):
        envelope = _envelope(origin_stream_id="orch:collab", origin_publish_id=1)
        del envelope["body"]
        envelope["body_jwe"] = ""
        r = _post_message(client_b, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)
        assert r.status_code == 400


class TestBodyDecryption:
    """`body_jwe` の復号（ECDH-ES + A256GCM 固定）。alg/enc 不一致・zip 使用は復号を
    試みず拒否する（algorithm confusion 対策、relay.federation_peers 参照）。
    """

    def _envelope_with_body_jwe(self, *, origin_publish_id: int, body_jwe: str, **kwargs) -> dict:
        envelope = _envelope(
            origin_stream_id="orch:collab", origin_publish_id=origin_publish_id, **kwargs
        )
        del envelope["body"]
        envelope["body_jwe"] = body_jwe
        return envelope

    def test_decrypts_and_stores_plaintext_in_outbox(
        self, client_b, settings_b, keypair_a, enc_keypair_b
    ):
        import dataclasses

        settings_b_enc = dataclasses.replace(
            settings_b, jwe_private_key_pem=enc_keypair_b["private_pem"]
        )
        app = create_app(settings_b_enc)
        with TestClient(app) as c:
            federation_peers.add_peer(
                settings_b_enc.db_path,
                handle="alice",
                fingerprint=_fp(keypair_a),
                key_jwk=keypair_a["public_jwk"],
                locator="https://relay-a.example",
            )
            body_jwe = federation_peers.encrypt_envelope_body(
                "top secret payload", public_key_jwk=enc_keypair_b["public_jwk"]
            )
            envelope = self._envelope_with_body_jwe(origin_publish_id=1, body_jwe=body_jwe)
            r = _post_message(c, settings_b_enc, keypair_a, path=MESSAGES_PATH, envelope=envelope)
            assert r.status_code == 202

            conn = sqlite3.connect(settings_b_enc.db_path)
            conn.row_factory = sqlite3.Row
            try:
                row = conn.execute(
                    "SELECT payload FROM outbox WHERE stream_id = 'orch@alice:collab'"
                ).fetchone()
            finally:
                conn.close()
            assert bytes(row["payload"]).decode("utf-8") == "top secret payload"

    def test_missing_decryption_key_returns_400(
        self, client_b, settings_b, keypair_a, pinned_peer_a, enc_keypair_b
    ):
        """`body_jwe` が来ているのに自分の復号鍵（jwe_private_key_pem）が未設定。"""
        body_jwe = federation_peers.encrypt_envelope_body(
            "secret", public_key_jwk=enc_keypair_b["public_jwk"]
        )
        envelope = self._envelope_with_body_jwe(origin_publish_id=1, body_jwe=body_jwe)
        r = _post_message(client_b, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)
        assert r.status_code == 400
        assert r.json()["code"] == "FederationEnvelopeDecryptError"

    def test_unexpected_enc_is_rejected(
        self, client_b, settings_b, keypair_a, enc_keypair_b
    ):
        import dataclasses

        from joserfc import jwe as jwe_module
        from joserfc.jwk import ECKey

        settings_b_enc = dataclasses.replace(
            settings_b, jwe_private_key_pem=enc_keypair_b["private_pem"]
        )
        app = create_app(settings_b_enc)
        with TestClient(app) as c:
            federation_peers.add_peer(
                settings_b_enc.db_path,
                handle="alice",
                fingerprint=_fp(keypair_a),
                key_jwk=keypair_a["public_jwk"],
                locator="https://relay-a.example",
            )
            crafted = jwe_module.encrypt_compact(
                {"alg": "ECDH-ES", "enc": "A128GCM"},
                "secret",
                ECKey.import_key(enc_keypair_b["public_jwk"]),
                algorithms=["ECDH-ES", "A128GCM"],
            )
            envelope = self._envelope_with_body_jwe(origin_publish_id=1, body_jwe=crafted)
            r = _post_message(c, settings_b_enc, keypair_a, path=MESSAGES_PATH, envelope=envelope)
            assert r.status_code == 400
            assert r.json()["code"] == "FederationEnvelopeDecryptError"

    def test_zip_header_is_rejected(
        self, client_b, settings_b, keypair_a, enc_keypair_b
    ):
        """圧縮(zip)付き JWE は復号を試みず拒否する（サイドチャネル対策）。"""
        import dataclasses

        from joserfc import jwe as jwe_module
        from joserfc.jwk import ECKey

        settings_b_enc = dataclasses.replace(
            settings_b, jwe_private_key_pem=enc_keypair_b["private_pem"]
        )
        app = create_app(settings_b_enc)
        with TestClient(app) as c:
            federation_peers.add_peer(
                settings_b_enc.db_path,
                handle="alice",
                fingerprint=_fp(keypair_a),
                key_jwk=keypair_a["public_jwk"],
                locator="https://relay-a.example",
            )
            crafted = jwe_module.encrypt_compact(
                {"alg": "ECDH-ES", "enc": "A256GCM", "zip": "DEF"},
                "secret " * 20,
                ECKey.import_key(enc_keypair_b["public_jwk"]),
                algorithms=["ECDH-ES", "A256GCM", "DEF"],
            )
            envelope = self._envelope_with_body_jwe(origin_publish_id=1, body_jwe=crafted)
            r = _post_message(c, settings_b_enc, keypair_a, path=MESSAGES_PATH, envelope=envelope)
            assert r.status_code == 400
            assert r.json()["code"] == "FederationEnvelopeDecryptError"

    def test_tampered_ciphertext_is_rejected(
        self, client_b, settings_b, keypair_a, enc_keypair_b
    ):
        import dataclasses

        settings_b_enc = dataclasses.replace(
            settings_b, jwe_private_key_pem=enc_keypair_b["private_pem"]
        )
        app = create_app(settings_b_enc)
        with TestClient(app) as c:
            federation_peers.add_peer(
                settings_b_enc.db_path,
                handle="alice",
                fingerprint=_fp(keypair_a),
                key_jwk=keypair_a["public_jwk"],
                locator="https://relay-a.example",
            )
            body_jwe = federation_peers.encrypt_envelope_body(
                "secret", public_key_jwk=enc_keypair_b["public_jwk"]
            )
            parts = body_jwe.split(".")
            parts[3] = parts[3][:-1] + ("A" if parts[3][-1] != "A" else "B")
            tampered = ".".join(parts)
            envelope = self._envelope_with_body_jwe(origin_publish_id=1, body_jwe=tampered)
            r = _post_message(c, settings_b_enc, keypair_a, path=MESSAGES_PATH, envelope=envelope)
            assert r.status_code == 400
            assert r.json()["code"] == "FederationEnvelopeDecryptError"


class TestBodySizeCap:
    """edge case #9: request body が上限サイズを超えるとき 413。"""

    def test_oversized_body_returns_413(self, tmp_path, keypair_a, keypair_b):
        settings_b = Settings(
            db_path=str(tmp_path / "cap.db"),
            server_log_path=str(tmp_path / "cap.jsonl"),
            dispatcher_lock_path=str(tmp_path / "cap.lock"),
            jws_private_key_pem=keypair_b["private_pem"],
            federation_base_url="https://relay-b.example",
            max_payload_bytes=80,
        )
        app = create_app(settings_b)
        with TestClient(app) as c:
            federation_peers.add_peer(
                settings_b.db_path,
                handle="alice",
                fingerprint=_fp(keypair_a),
                key_jwk=keypair_a["public_jwk"],
                locator="https://relay-a.example",
            )
            envelope = _envelope(
                origin_stream_id="orch:collab", origin_publish_id=1, body="x" * 200
            )
            r = _post_message(c, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)
        assert r.status_code == 413
        assert r.json()["code"] == "PayloadTooLargeError"

    def test_body_within_cap_is_accepted(self, tmp_path, keypair_a, keypair_b):
        settings_b = Settings(
            db_path=str(tmp_path / "cap2.db"),
            server_log_path=str(tmp_path / "cap2.jsonl"),
            dispatcher_lock_path=str(tmp_path / "cap2.lock"),
            jws_private_key_pem=keypair_b["private_pem"],
            federation_base_url="https://relay-b.example",
            max_payload_bytes=1000,
        )
        app = create_app(settings_b)
        with TestClient(app) as c:
            federation_peers.add_peer(
                settings_b.db_path,
                handle="alice",
                fingerprint=_fp(keypair_a),
                key_jwk=keypair_a["public_jwk"],
                locator="https://relay-a.example",
            )
            envelope = _envelope(origin_stream_id="orch:collab", origin_publish_id=1, body="hi")
            r = _post_message(c, settings_b, keypair_a, path=MESSAGES_PATH, envelope=envelope)
        assert r.status_code == 202


class TestRateLimit:
    """edge case #10: per-peer rate limit を超えるとき 429。"""

    def test_over_limit_returns_429(self, client_b, app_b, settings_b, keypair_a, pinned_peer_a):
        # `receive_message` 専用の per-peer rate limiter を、テストで決定的に 429 境界へ
        # 到達させるため厳しい値へ差し替える（未初期化状態への先出し代入、遅延初期化を防ぐ）。
        app_b.state.federation_inbound_rate_limiter = RateLimiter(1)

        r1 = _post_message(
            client_b,
            settings_b,
            keypair_a,
            path=MESSAGES_PATH,
            envelope=_envelope(origin_stream_id="orch:collab", origin_publish_id=1),
        )
        assert r1.status_code == 202

        r2 = _post_message(
            client_b,
            settings_b,
            keypair_a,
            path=MESSAGES_PATH,
            envelope=_envelope(origin_stream_id="orch:collab", origin_publish_id=2),
        )
        assert r2.status_code == 429


class TestMembershipAddOnlySelfHealing:
    """edge case #12: membership upsert は add-only、毎転送で再主張し operator の
    誤操作/B 再起動から自己修復する。"""

    def test_local_downgrade_is_reasserted_by_next_federation_message(
        self, client_b, settings_b, app_b, keypair_a, pinned_peer_a
    ):
        r1 = _post_message(
            client_b,
            settings_b,
            keypair_a,
            path=MESSAGES_PATH,
            envelope=_envelope(origin_stream_id="orch:collab", origin_publish_id=1, to_members=["orch"]),
        )
        assert r1.status_code == 202

        registry = streams.get_registry_from_state(app_b.state)
        assert registry.has_write_access("orch@alice:collab", "orch")

        # ローカル orch が自分自身を read へ降格（operator の誤操作を模す）。
        r_downgrade = client_b.put(
            "/streams/orch@alice:collab/members",
            json={"identity": "orch", "access": "read"},
            headers=_local_auth(),
        )
        assert r_downgrade.status_code == 200
        assert registry.has_read_access("orch@alice:collab", "orch")
        assert not registry.has_write_access("orch@alice:collab", "orch")

        r2 = _post_message(
            client_b,
            settings_b,
            keypair_a,
            path=MESSAGES_PATH,
            envelope=_envelope(origin_stream_id="orch:collab", origin_publish_id=2, to_members=["orch"]),
        )
        assert r2.status_code == 202
        # 次の federation メッセージで read_write に再主張される（自己修復）。
        assert registry.has_write_access("orch@alice:collab", "orch")

    def test_existing_write_member_is_not_downgraded_by_upsert(
        self, client_b, settings_b, app_b, keypair_a, pinned_peer_a
    ):
        """add-only: to_members upsert は既存の read_write member を格下げしない。"""
        r1 = _post_message(
            client_b,
            settings_b,
            keypair_a,
            path=MESSAGES_PATH,
            envelope=_envelope(origin_stream_id="orch:collab", origin_publish_id=1, to_members=["orch"]),
        )
        assert r1.status_code == 202
        registry = streams.get_registry_from_state(app_b.state)
        assert registry.has_write_access("orch@alice:collab", "orch")

        r2 = _post_message(
            client_b,
            settings_b,
            keypair_a,
            path=MESSAGES_PATH,
            envelope=_envelope(origin_stream_id="orch:collab", origin_publish_id=2, to_members=["orch"]),
        )
        assert r2.status_code == 202
        assert registry.has_write_access("orch@alice:collab", "orch")
