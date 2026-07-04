"""relay.streams テストスイート。

stream CRUD・membership CRUD・stream レーン publish・stream レーン ack と、それぞれの
structural authZ（write 権限 membership 照合、identity-authz.md §2.2）を検証する。

`TestStreamRegistry` は in-memory registry の単体テスト、それ以外は実際に Starlette
アプリを起動して HTTP リクエストを投げる統合テスト（既存 test_app.py のパターンを踏襲）。

stream_id は作成者 identity でスコープ化された canonical id（`{作成者 identity}:{name}`、
wire-api.md §3.1）。registry 単体テストは endpoint 層のスコープ化を経由しないため stream_id を
opaque key（"s1" 等）として直接扱い、HTTP 統合テストは create 応答が返す canonical id で
以後の操作をアドレスする。
"""
import pytest
from starlette.testclient import TestClient

from datetime import datetime, timedelta, timezone

from relay.app import create_app
from relay.config import DEFAULT_MAX_STREAM_NAME_LENGTH, Settings
from relay.errors import ResourceLimitExceeded
from relay.streams import StreamRegistry, canonical_stream_id


# ---------------------------------------------------------------------------
# StreamRegistry 単体テスト
# ---------------------------------------------------------------------------


class TestStreamRegistry:
    def test_create_returns_record_with_bootstrap_write_member(self):
        registry = StreamRegistry()
        record = registry.create("s1", "agent-a", None)
        assert record is not None
        assert record.stream_id == "s1"
        assert record.state == "open"
        assert record.members == {"agent-a": "write"}

    def test_create_duplicate_returns_none(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        assert registry.create("s1", "agent-b", None) is None

    def test_get_missing_returns_none(self):
        registry = StreamRegistry()
        assert registry.get("nope") is None

    def test_close_sets_state_closed(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        registry.close("s1")
        assert registry.get("s1").state == "closed"

    def test_close_missing_stream_is_noop(self):
        registry = StreamRegistry()
        registry.close("nope")  # 例外を出さない

    def test_put_member_adds_access(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        registry.put_member("s1", "agent-b", "read")
        assert registry.get("s1").members["agent-b"] == "read"

    def test_put_member_overwrites_existing_access(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        registry.put_member("s1", "agent-a", "read_write")
        assert registry.get("s1").members["agent-a"] == "read_write"

    def test_put_member_checked_applies_when_write_member_remains(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        registry.put_member("s1", "agent-b", "write")
        # agent-a を read に落としても agent-b が write なので許可される。
        assert registry.put_member_checked("s1", "agent-a", "read") == "ok"
        assert registry.get("s1").members["agent-a"] == "read"

    def test_put_member_checked_rejects_last_write_member(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)  # agent-a が唯一の write member
        assert registry.put_member_checked("s1", "agent-a", "read") == "last_write_member"
        # 拒否された変更は適用されない（agent-a は write のまま）。
        assert registry.get("s1").members["agent-a"] == "write"

    def test_put_member_checked_read_write_keeps_write(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        assert registry.put_member_checked("s1", "agent-a", "read_write") == "ok"
        assert registry.get("s1").members["agent-a"] == "read_write"

    def test_put_member_checked_missing_stream(self):
        registry = StreamRegistry()
        assert registry.put_member_checked("nope", "agent-a", "read") == "not_found"

    def test_delete_member_removes_access(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        registry.put_member("s1", "agent-b", "read")
        registry.delete_member("s1", "agent-b")
        assert "agent-b" not in registry.get("s1").members

    def test_delete_missing_member_is_noop(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        registry.delete_member("s1", "nope")  # 例外を出さない

    def test_list_members_missing_stream_returns_none(self):
        registry = StreamRegistry()
        assert registry.list_members("nope") is None

    def test_list_members_returns_all(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        registry.put_member("s1", "agent-b", "read")
        members = {m["identity"]: m["access"] for m in registry.list_members("s1")}
        assert members == {"agent-a": "write", "agent-b": "read"}

    @pytest.mark.parametrize(
        "access,expected_write,expected_read",
        [("write", True, False), ("read", False, True), ("read_write", True, True)],
    )
    def test_access_helpers(self, access, expected_write, expected_read):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        registry.put_member("s1", "agent-b", access)
        assert registry.has_write_access("s1", "agent-b") is expected_write
        assert registry.has_read_access("s1", "agent-b") is expected_read

    def test_access_helpers_false_for_non_member(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        assert registry.has_write_access("s1", "stranger") is False
        assert registry.has_read_access("s1", "stranger") is False

    def test_access_helpers_false_for_missing_stream(self):
        registry = StreamRegistry()
        assert registry.has_write_access("nope", "agent-a") is False
        assert registry.has_read_access("nope", "agent-a") is False

    def test_read_members_includes_read_and_read_write_only(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)  # write のみ
        registry.put_member("s1", "agent-b", "read")
        registry.put_member("s1", "agent-c", "read_write")
        registry.put_member("s1", "agent-d", "write")
        assert set(registry.read_members("s1")) == {"agent-b", "agent-c"}

    def test_read_members_missing_stream_returns_empty(self):
        registry = StreamRegistry()
        assert registry.read_members("nope") == []

    def test_is_member_true_for_any_access_including_write_only(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)  # 作成者は write のみ
        registry.put_member("s1", "agent-b", "read")
        registry.put_member("s1", "agent-c", "read_write")
        assert registry.is_member("s1", "agent-a") is True
        assert registry.is_member("s1", "agent-b") is True
        assert registry.is_member("s1", "agent-c") is True

    def test_is_member_false_for_non_member(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        assert registry.is_member("s1", "stranger") is False

    def test_is_member_false_for_missing_stream(self):
        registry = StreamRegistry()
        assert registry.is_member("nope", "agent-a") is False


def _past_iso(seconds: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )


class TestStreamRegistryResourceLimits:
    def test_total_limit_rejects_create_beyond_cap(self):
        registry = StreamRegistry(max_total=2, max_per_identity=100)
        registry.create("s1", "agent-a", None)
        registry.create("s2", "agent-b", None)
        with pytest.raises(ResourceLimitExceeded) as exc:
            registry.create("s3", "agent-c", None)
        assert exc.value.scope == "total"

    def test_per_identity_limit_rejects_third_from_same_creator(self):
        registry = StreamRegistry(max_total=100, max_per_identity=2)
        registry.create("s1", "agent-a", None)
        registry.create("s2", "agent-a", None)
        with pytest.raises(ResourceLimitExceeded) as exc:
            registry.create("s3", "agent-a", None)
        assert exc.value.scope == "per_identity"

    def test_per_identity_limit_is_counted_per_creator(self):
        registry = StreamRegistry(max_total=100, max_per_identity=1)
        registry.create("s1", "agent-a", None)
        # agent-a は上限だが agent-b は自分の枠を消費して作成できる。
        assert registry.create("s2", "agent-b", None) is not None

    def test_duplicate_stream_id_returns_none_before_total_limit_check(self):
        registry = StreamRegistry(max_total=1, max_per_identity=100)
        registry.create("s1", "agent-a", None)
        # 既存 stream_id の再作成は新規スロットを消費しないため上限例外ではなく None(409)。
        assert registry.create("s1", "agent-b", None) is None

    def test_close_alone_does_not_free_per_identity_slot(self):
        registry = StreamRegistry(max_total=100, max_per_identity=1)
        registry.create("s1", "agent-a", None)
        registry.close("s1")
        with pytest.raises(ResourceLimitExceeded):
            registry.create("s2", "agent-a", None)

    def test_evict_frees_per_identity_slot(self):
        registry = StreamRegistry(max_total=100, max_per_identity=1)
        registry.create("s1", "agent-a", None)
        registry.close("s1")
        assert registry.evict("s1") is True
        # evict で record が消えたので agent-a は再び作成できる。
        assert registry.create("s2", "agent-a", None) is not None


class TestStreamRegistryIdleEviction:
    def test_closed_at_is_set_only_on_open_to_closed_transition(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        assert registry.get("s1").closed_at is None
        registry.close("s1")
        first_closed_at = registry.get("s1").closed_at
        assert first_closed_at is not None
        registry.close("s1")  # 冪等な再 close は closed_at を上書きしない。
        assert registry.get("s1").closed_at == first_closed_at

    def test_idle_closed_ids_returns_only_closed_past_grace(self):
        registry = StreamRegistry()
        registry.create("still-open", "agent-a", None)  # open のまま
        registry.create("recently-closed", "agent-b", None)
        registry.close("recently-closed")  # 猶予内の close
        registry.create("long-closed", "agent-c", None)
        registry.close("long-closed")
        registry.get("long-closed").closed_at = _past_iso(7200)  # 2h 前に close
        assert registry.idle_closed_ids(older_than_seconds=3600) == ["long-closed"]

    def test_idle_closed_ids_empty_when_nothing_past_grace(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        registry.close("s1")
        assert registry.idle_closed_ids(older_than_seconds=3600) == []

    def test_evict_removes_closed_stream(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        registry.close("s1")
        assert registry.evict("s1") is True
        assert registry.get("s1") is None

    def test_evict_refuses_open_stream(self):
        registry = StreamRegistry()
        registry.create("s1", "agent-a", None)
        assert registry.evict("s1") is False
        assert registry.get("s1") is not None

    def test_evict_missing_stream_returns_false(self):
        registry = StreamRegistry()
        assert registry.evict("nope") is False


# ---------------------------------------------------------------------------
# canonical_stream_id / name スコープ化の単体テスト
# ---------------------------------------------------------------------------


class TestCanonicalStreamId:
    def test_prefixes_creator_identity(self):
        assert canonical_stream_id("agent-a", "s1") == "agent-a:s1"

    def test_same_name_different_creator_differs(self):
        # squatting 不成立の核: 同名 name でも creator が違えば canonical は別物。
        assert canonical_stream_id("agent-a", "room") != canonical_stream_id(
            "agent-b", "room"
        )


# ---------------------------------------------------------------------------
# HTTP 統合テスト
# ---------------------------------------------------------------------------


# tok-a=agent-a が name="s1" で作成したときの canonical stream_id。
SID = "agent-a:s1"


@pytest.fixture()
def settings(tmp_path):
    return Settings(
        db_path=str(tmp_path / "test_relay.db"),
        server_log_path=str(tmp_path / "test_relay.jsonl"),
        auth_tokens={"tok-a": "agent-a", "tok-b": "agent-b", "tok-c": "agent-c"},
    )


@pytest.fixture()
def client(settings):
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def _auth(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


class TestCreateStream:
    def test_creates_stream_and_returns_canonical_id(self, client):
        r = client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        assert r.status_code == 201
        body = r.json()
        # 応答は作成者 identity でスコープ化された canonical stream_id。
        assert body["stream_id"] == "agent-a:s1"
        assert "created_at" in body

    def test_creator_becomes_write_member(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        # membership の key は identity（canonical id ではなく生の identity）。
        r = client.get(f"/streams/{SID}/members", headers=_auth("tok-a"))
        assert r.json() == {"members": [{"identity": "agent-a", "access": "write"}]}

    def test_duplicate_name_same_creator_returns_409(self, client):
        # 同一作成者の名前空間内での同名再作成は 409（既存 create 意味論の踏襲）。
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        r = client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        assert r.status_code == 409
        assert r.json()["code"] == "StreamAlreadyExistsError"

    def test_same_name_different_identity_no_conflict(self, client):
        # squatting 不成立: 別 identity が同名 name を使っても canonical が別物なので衝突しない。
        r_a = client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        r_b = client.post("/streams", json={"name": "s1"}, headers=_auth("tok-b"))
        assert r_a.status_code == 201
        assert r_b.status_code == 201
        assert r_a.json()["stream_id"] == "agent-a:s1"
        assert r_b.json()["stream_id"] == "agent-b:s1"
        assert r_a.json()["stream_id"] != r_b.json()["stream_id"]

    def test_squatter_cannot_occupy_victim_namespace(self, client):
        # 攻撃者(agent-b)が被害者(agent-a)の使いそうな name を先取りしても、canonical は
        # attacker の名前空間(agent-b:*)に閉じ、被害者の agent-a:* を締め出せない。
        squat = client.post("/streams", json={"name": "s1"}, headers=_auth("tok-b"))
        assert squat.json()["stream_id"] == "agent-b:s1"
        victim = client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        # 被害者は 409 で締め出されず自分の名前空間で作成できる。
        assert victim.status_code == 201
        assert victim.json()["stream_id"] == "agent-a:s1"
        # 攻撃者は被害者の stream の member ではない。
        members = client.get(f"/streams/{SID}/members", headers=_auth("tok-a")).json()[
            "members"
        ]
        assert all(m["identity"] != "agent-b" for m in members)

    def test_missing_name_returns_400(self, client):
        r = client.post("/streams", json={}, headers=_auth("tok-a"))
        assert r.status_code == 400
        assert r.json()["code"] == "InvalidRequestError"

    def test_empty_name_returns_400(self, client):
        r = client.post("/streams", json={"name": ""}, headers=_auth("tok-a"))
        assert r.status_code == 400

    def test_name_with_separator_returns_400(self, client):
        # name に区切り文字 ":" を許すと別 identity の canonical を詐称する余地が生じるため拒否。
        r = client.post(
            "/streams", json={"name": "agent-b:evil"}, headers=_auth("tok-a")
        )
        assert r.status_code == 400
        assert r.json()["code"] == "InvalidRequestError"

    def test_name_with_slash_returns_400(self, client):
        # "/" は URL パス区切りと衝突するため name に許さない。
        r = client.post("/streams", json={"name": "a/b"}, headers=_auth("tok-a"))
        assert r.status_code == 400

    def test_name_over_length_cap_returns_400(self, client):
        r = client.post(
            "/streams",
            json={"name": "x" * (DEFAULT_MAX_STREAM_NAME_LENGTH + 1)},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 400
        assert r.json()["code"] == "InvalidRequestError"

    def test_name_at_length_cap_accepted(self, client):
        r = client.post(
            "/streams",
            json={"name": "x" * DEFAULT_MAX_STREAM_NAME_LENGTH},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 201

    def test_default_ttl_out_of_range_returns_400(self, client):
        r = client.post(
            "/streams", json={"name": "s1", "default_ttl": 10}, headers=_auth("tok-a")
        )
        assert r.status_code == 400

    def test_default_ttl_within_range_accepted(self, client):
        r = client.post(
            "/streams", json={"name": "s1", "default_ttl": 3600}, headers=_auth("tok-a")
        )
        assert r.status_code == 201

    def test_requires_auth(self, client):
        r = client.post("/streams", json={"name": "s1"})
        assert r.status_code == 401


class TestCreateStreamResourceLimits:
    @pytest.fixture()
    def limited_client(self, tmp_path):
        settings = Settings(
            db_path=str(tmp_path / "limited.db"),
            server_log_path=str(tmp_path / "limited.jsonl"),
            auth_tokens={"tok-a": "agent-a", "tok-b": "agent-b"},
            max_streams_total=3,
            max_streams_per_identity=2,
        )
        app = create_app(settings)
        with TestClient(app) as c:
            yield c

    def test_within_limits_returns_201(self, limited_client):
        r = limited_client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        assert r.status_code == 201

    def test_per_identity_limit_returns_429(self, limited_client):
        limited_client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        limited_client.post("/streams", json={"name": "s2"}, headers=_auth("tok-a"))
        # agent-a の 3 件目は per-identity 上限(2)超過で拒否される。
        r = limited_client.post("/streams", json={"name": "s3"}, headers=_auth("tok-a"))
        assert r.status_code == 429
        assert r.json()["code"] == "ResourceLimitExceededError"

    def test_total_limit_returns_429(self, limited_client):
        # agent-a 2 件 + agent-b 1 件で total 上限(3)に到達させる。
        limited_client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        limited_client.post("/streams", json={"name": "s2"}, headers=_auth("tok-a"))
        limited_client.post("/streams", json={"name": "s3"}, headers=_auth("tok-b"))
        # agent-b は per-identity 枠に空きがあるが total 上限で拒否される。
        r = limited_client.post("/streams", json={"name": "s4"}, headers=_auth("tok-b"))
        assert r.status_code == 429
        assert r.json()["code"] == "ResourceLimitExceededError"


class TestGetStream:
    def test_returns_stream_meta(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        r = client.get(f"/streams/{SID}", headers=_auth("tok-a"))
        assert r.status_code == 200
        body = r.json()
        assert body["stream_id"] == "agent-a:s1"
        assert body["state"] == "open"

    def test_missing_stream_returns_404(self, client):
        r = client.get("/streams/nope", headers=_auth("tok-a"))
        assert r.status_code == 404
        assert r.json()["code"] == "StreamNotFoundError"

    def test_write_only_creator_can_view(self, client):
        """作成者は write 単独権限（bootstrap の access="write"）でも自身の stream を参照できる。"""
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        r = client.get(f"/streams/{SID}", headers=_auth("tok-a"))
        assert r.status_code == 200

    def test_read_member_can_view(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        r = client.get(f"/streams/{SID}", headers=_auth("tok-b"))
        assert r.status_code == 200
        assert r.json()["stream_id"] == "agent-a:s1"

    def test_read_write_member_can_view(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "read_write"},
            headers=_auth("tok-a"),
        )
        r = client.get(f"/streams/{SID}", headers=_auth("tok-b"))
        assert r.status_code == 200

    def test_non_member_gets_404_indistinguishable_from_missing(self, client):
        # 存在する stream への非メンバーアクセスと不在 stream_id へのアクセスが、存在を判別できる
        # 信号（status_code / error code）で区別できないこと。message は要求 id を echo するだけで
        # 呼び出し元が既知の値のため露呈にならない。
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        existing = client.get(f"/streams/{SID}", headers=_auth("tok-b"))
        missing = client.get("/streams/does-not-exist", headers=_auth("tok-b"))
        assert existing.status_code == missing.status_code == 404
        assert existing.json()["code"] == missing.json()["code"] == "StreamNotFoundError"


class TestCloseStream:
    def test_write_member_can_close(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        r = client.delete(f"/streams/{SID}", headers=_auth("tok-a"))
        assert r.status_code == 204
        assert client.get(f"/streams/{SID}", headers=_auth("tok-a")).json()["state"] == "closed"

    def test_non_member_gets_404_indistinguishable_from_missing(self, client):
        # 完全非メンバーの close 試行は不在 stream_id と同一の 404。GET 側の存在秘匿を
        # DELETE での probe でバイパスできないこと。
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        existing = client.delete(f"/streams/{SID}", headers=_auth("tok-b"))
        missing = client.delete("/streams/does-not-exist", headers=_auth("tok-b"))
        assert existing.status_code == missing.status_code == 404
        assert existing.json()["code"] == missing.json()["code"] == "StreamNotFoundError"
        # 拒否された close は実際に効いていない
        assert client.get(f"/streams/{SID}", headers=_auth("tok-a")).json()["state"] == "open"

    def test_read_only_member_forbidden(self, client):
        # 権限不足の正規 member は stream の存在を正当に知っているため 403 で区別してよい。
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        r = client.delete(f"/streams/{SID}", headers=_auth("tok-b"))
        assert r.status_code == 403
        assert r.json()["code"] == "MembershipRequiredError"

    def test_missing_stream_returns_404(self, client):
        r = client.delete("/streams/nope", headers=_auth("tok-a"))
        assert r.status_code == 404

    def test_close_is_idempotent(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.delete(f"/streams/{SID}", headers=_auth("tok-a"))
        r = client.delete(f"/streams/{SID}", headers=_auth("tok-a"))
        assert r.status_code == 204


class TestMembers:
    def test_write_member_can_add_member(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        r = client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 200
        members = client.get(f"/streams/{SID}/members", headers=_auth("tok-a")).json()["members"]
        assert {"identity": "agent-b", "access": "read"} in members

    def test_read_member_cannot_add_member(self, client):
        # 権限不足の正規 member（read 単独）による membership 変更は 403。
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        r = client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-c", "access": "read"},
            headers=_auth("tok-b"),
        )
        assert r.status_code == 403
        assert r.json()["code"] == "MembershipRequiredError"

    def test_non_member_put_member_404_indistinguishable_from_missing(self, client):
        # 完全非メンバーの membership 変更試行は不在 stream_id と同一の 404（存在 probe 防止）。
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        payload = {"identity": "agent-c", "access": "read"}
        existing = client.put(f"/streams/{SID}/members", json=payload, headers=_auth("tok-b"))
        missing = client.put(
            "/streams/does-not-exist/members", json=payload, headers=_auth("tok-b")
        )
        assert existing.status_code == missing.status_code == 404
        assert existing.json()["code"] == missing.json()["code"] == "StreamNotFoundError"
        # 拒否された追加は実際に効いていない
        members = client.get(f"/streams/{SID}/members", headers=_auth("tok-a")).json()["members"]
        assert all(m["identity"] != "agent-c" for m in members)

    def test_invalid_access_value_returns_400(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        r = client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "owner"},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 400

    def test_put_member_missing_stream_returns_404(self, client):
        r = client.put(
            "/streams/nope/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 404

    def test_demoting_last_write_member_returns_400(self, client):
        """唯一の write member を read に落とす操作は 400 で拒否する（write member 0 人ガード）。"""
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        r = client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-a", "access": "read"},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 400
        assert r.json()["code"] == "InvalidRequestError"
        # 拒否後も agent-a は write のままで、stream は操作可能。
        members = client.get(f"/streams/{SID}/members", headers=_auth("tok-a")).json()["members"]
        assert {"identity": "agent-a", "access": "write"} in members

    def test_demoting_write_member_allowed_when_another_write_exists(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "write"},
            headers=_auth("tok-a"),
        )
        r = client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-a", "access": "read"},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 200

    def test_write_member_can_remove_other_member(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        r = client.delete(
            f"/streams/{SID}/members", params={"identity": "agent-b"}, headers=_auth("tok-a")
        )
        assert r.status_code == 204
        members = client.get(f"/streams/{SID}/members", headers=_auth("tok-a")).json()["members"]
        assert all(m["identity"] != "agent-b" for m in members)

    def test_self_removal_always_allowed(self, client):
        """離脱（自分自身の membership 削除）は write 権限がなくても本人なら常に許可（identity-authz.md §2.2）。"""
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        r = client.delete(
            f"/streams/{SID}/members", params={"identity": "agent-b"}, headers=_auth("tok-b")
        )
        assert r.status_code == 204

    def test_non_write_member_cannot_remove_others(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-c", "access": "read"},
            headers=_auth("tok-a"),
        )
        r = client.delete(
            f"/streams/{SID}/members", params={"identity": "agent-c"}, headers=_auth("tok-b")
        )
        assert r.status_code == 403
        assert r.json()["code"] == "MembershipRequiredError"

    def test_non_member_delete_member_404_indistinguishable_from_missing(self, client):
        # 完全非メンバーの member 削除試行は不在 stream_id と同一の 404（存在 probe 防止）。
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        existing = client.delete(
            f"/streams/{SID}/members", params={"identity": "agent-a"}, headers=_auth("tok-b")
        )
        missing = client.delete(
            "/streams/does-not-exist/members",
            params={"identity": "agent-a"},
            headers=_auth("tok-b"),
        )
        assert existing.status_code == missing.status_code == 404
        assert existing.json()["code"] == missing.json()["code"] == "StreamNotFoundError"
        # 拒否された削除は実際に効いていない
        members = client.get(f"/streams/{SID}/members", headers=_auth("tok-a")).json()["members"]
        assert any(m["identity"] == "agent-a" for m in members)

    def test_non_member_self_removal_returns_404(self, client):
        # 非メンバーの自己離脱試行も 404。自己離脱パスが membership 判定より先に成立して
        # 204 を返すと stream の存在露呈になる。
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        r = client.delete(
            f"/streams/{SID}/members", params={"identity": "agent-b"}, headers=_auth("tok-b")
        )
        assert r.status_code == 404
        assert r.json()["code"] == "StreamNotFoundError"

    def test_delete_member_missing_stream_returns_404(self, client):
        r = client.delete(
            "/streams/nope/members", params={"identity": "agent-a"}, headers=_auth("tok-a")
        )
        assert r.status_code == 404

    def _stream_outbox_dlq_counts(self, settings, stream_id, member_identity):
        import sqlite3

        conn = sqlite3.connect(settings.db_path)
        try:
            outbox = conn.execute(
                "SELECT COUNT(*) FROM outbox"
                " WHERE target_type = 'stream' AND stream_id = ? AND member_identity = ?",
                (stream_id, member_identity),
            ).fetchone()[0]
            dlq = conn.execute(
                "SELECT COUNT(*) FROM dlq WHERE stream_id = ? AND member_identity = ?",
                (stream_id, member_identity),
            ).fetchone()[0]
        finally:
            conn.close()
        return outbox, dlq

    def test_self_departure_immediately_deletes_outbox_and_skips_dlq(self, client, settings):
        """自己離脱（本人による DELETE self）は未 ack outbox を即時削除し DLQ を経由しない。

        subscription レーンの unsubscribe と同型（wire-api.md §5.3 / §6.6）。明示的な関心放棄なので
        DLQ・warn ログを汚さず、エントリは痕跡なく消える（outbox 0 件・dlq 0 件）。
        """
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        client.post(f"/streams/{SID}/messages", json={"body": "m1"}, headers=_auth("tok-a"))

        # 送信直後は agent-b（read member）宛 outbox エントリが 1 件ある。
        outbox_before, _ = self._stream_outbox_dlq_counts(settings, SID, "agent-b")
        assert outbox_before == 1

        r = client.delete(
            f"/streams/{SID}/members", params={"identity": "agent-b"}, headers=_auth("tok-b")
        )
        assert r.status_code == 204

        # 即時削除で outbox からも消え、DLQ にも入らない（痕跡なし）。
        outbox_after, dlq_after = self._stream_outbox_dlq_counts(settings, SID, "agent-b")
        assert outbox_after == 0
        assert dlq_after == 0

    def test_removal_by_other_member_preserves_entry_for_sweep(self, client, settings):
        """他 member による除去は即時削除せず、エントリを DLQ sweep 経路に残す（wire-api.md §6.6）。

        自己離脱と対照的に、involuntary な read 権限喪失は観測対象として保全される。エントリは除去直後
        なら outbox に、dispatcher の DLQ sweep が走った後なら dlq に居る（どちらでも合計 1 件は保たれ、
        自己離脱のように痕跡なく消えることはない）。この合計での検証は背景 dispatcher の sweep timing に
        依存しない（レース耐性）。
        """
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        client.post(f"/streams/{SID}/messages", json={"body": "m1"}, headers=_auth("tok-a"))

        # agent-a（write member）が agent-b を除去（本人以外による解除）。
        r = client.delete(
            f"/streams/{SID}/members", params={"identity": "agent-b"}, headers=_auth("tok-a")
        )
        assert r.status_code == 204

        outbox_after, dlq_after = self._stream_outbox_dlq_counts(settings, SID, "agent-b")
        assert outbox_after + dlq_after == 1

    def test_list_members_member_can_view(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        r = client.get(f"/streams/{SID}/members", headers=_auth("tok-b"))
        assert r.status_code == 200
        assert {m["identity"] for m in r.json()["members"]} == {"agent-a", "agent-b"}

    def test_list_members_non_member_gets_404_hiding_composition(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        existing = client.get(f"/streams/{SID}/members", headers=_auth("tok-b"))
        missing = client.get("/streams/does-not-exist/members", headers=_auth("tok-b"))
        assert existing.status_code == missing.status_code == 404
        assert existing.json()["code"] == missing.json()["code"] == "StreamNotFoundError"

    def test_list_members_missing_stream_returns_404(self, client):
        r = client.get("/streams/nope/members", headers=_auth("tok-a"))
        assert r.status_code == 404


class TestPostStreamMessage:
    def test_write_member_can_publish(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-a", "access": "read_write"},
            headers=_auth("tok-a"),
        )
        r = client.post(
            f"/streams/{SID}/messages", json={"body": "hello"}, headers=_auth("tok-a")
        )
        assert r.status_code == 202
        body = r.json()
        assert isinstance(body["publish_id"], int)
        assert body["matched_members"] == 1  # 自分自身が read_write

    def test_matched_members_counts_only_read_access(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-c", "access": "write"},
            headers=_auth("tok-a"),
        )
        r = client.post(
            f"/streams/{SID}/messages", json={"body": "hello"}, headers=_auth("tok-a")
        )
        assert r.json()["matched_members"] == 1  # agent-b のみ（agent-a, agent-c は write のみ）

    def test_creates_outbox_entry_per_read_member(self, client, settings):
        import sqlite3

        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        r = client.post(
            f"/streams/{SID}/messages", json={"body": "hello"}, headers=_auth("tok-a")
        )
        publish_id = r.json()["publish_id"]

        conn = sqlite3.connect(settings.db_path)
        try:
            rows = conn.execute(
                "SELECT stream_id, member_identity, publish_id, payload FROM outbox"
                " WHERE target_type = 'stream'"
            ).fetchall()
        finally:
            conn.close()
        # outbox の stream_id には canonical id が入る。
        assert rows == [("agent-a:s1", "agent-b", publish_id, b"hello")]

    def test_read_only_member_forbidden(self, client):
        # 権限不足の正規 member（read 単独）による投函は 403。
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        r = client.post(
            f"/streams/{SID}/messages", json={"body": "hello"}, headers=_auth("tok-b")
        )
        assert r.status_code == 403
        assert r.json()["code"] == "MembershipRequiredError"

    def test_non_member_gets_404_indistinguishable_from_missing(self, client):
        # 完全非メンバーの投函試行は不在 stream_id と同一の 404（POST での存在 probe 防止）。
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        existing = client.post(
            f"/streams/{SID}/messages", json={"body": "hello"}, headers=_auth("tok-b")
        )
        missing = client.post(
            "/streams/does-not-exist/messages", json={"body": "hello"}, headers=_auth("tok-b")
        )
        assert existing.status_code == missing.status_code == 404
        assert existing.json()["code"] == missing.json()["code"] == "StreamNotFoundError"

    def test_missing_stream_returns_404(self, client):
        r = client.post(
            "/streams/nope/messages", json={"body": "hello"}, headers=_auth("tok-a")
        )
        assert r.status_code == 404

    def test_closed_stream_returns_410(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.delete(f"/streams/{SID}", headers=_auth("tok-a"))
        r = client.post(
            f"/streams/{SID}/messages", json={"body": "hello"}, headers=_auth("tok-a")
        )
        assert r.status_code == 410
        assert r.json()["code"] == "StreamGoneError"

    def test_body_exceeding_default_cap_returns_413(self, client):
        """既定の payload 上限（256 KiB）を超える body は 413 で拒否される。"""
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        r = client.post(
            f"/streams/{SID}/messages",
            json={"body": "x" * 300_000},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 413
        assert r.json()["code"] == "PayloadTooLargeError"


class TestPostStreamMessagePayloadCapConfigurable:
    """`Settings.max_payload_bytes` を明示的に小さくして 413 境界を決定的に検証する。"""

    def _client(self, tmp_path, max_payload_bytes: int) -> TestClient:
        settings = Settings(
            db_path=str(tmp_path / "test_relay.db"),
            server_log_path=str(tmp_path / "test_relay.jsonl"),
            auth_tokens={"tok-a": "agent-a"},
            max_payload_bytes=max_payload_bytes,
        )
        return TestClient(create_app(settings))

    def test_body_over_configured_cap_returns_413(self, tmp_path):
        with self._client(tmp_path, max_payload_bytes=50) as client:
            client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
            r = client.post(
                f"/streams/{SID}/messages",
                json={"body": "x" * 100},
                headers=_auth("tok-a"),
            )
        assert r.status_code == 413
        assert r.json()["code"] == "PayloadTooLargeError"

    def test_body_within_configured_cap_is_accepted(self, tmp_path):
        with self._client(tmp_path, max_payload_bytes=1000) as client:
            client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
            r = client.post(
                f"/streams/{SID}/messages", json={"body": "hello"}, headers=_auth("tok-a")
            )
        assert r.status_code == 202

    def test_missing_body_returns_400(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        r = client.post(f"/streams/{SID}/messages", json={}, headers=_auth("tok-a"))
        assert r.status_code == 400

    def test_ttl_out_of_range_returns_400(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        r = client.post(
            f"/streams/{SID}/messages",
            json={"body": "hello", "ttl": 5},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 400

    def test_zero_read_members_still_returns_202(self, client):
        """write のみの stream に自分だけ属する場合、matched_members=0 でも 202。"""
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        r = client.post(
            f"/streams/{SID}/messages", json={"body": "hello"}, headers=_auth("tok-a")
        )
        assert r.status_code == 202
        assert r.json()["matched_members"] == 0


class TestPostStreamMessageRateLimit:
    """stream レーン投函（POST /streams/{id}/messages）の per-identity rate limit。"""

    @pytest.fixture()
    def limited_client(self, tmp_path):
        settings = Settings(
            db_path=str(tmp_path / "srl.db"),
            server_log_path=str(tmp_path / "srl.jsonl"),
            dispatcher_lock_path=str(tmp_path / "srl.lock"),
            auth_tokens={"tok-a": "agent-a"},
            publish_rate_limit_per_second=1,
        )
        app = create_app(settings)
        with TestClient(app) as c:
            yield c

    def test_within_limit_returns_202(self, limited_client):
        limited_client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        r = limited_client.post(
            f"/streams/{SID}/messages", json={"body": "m1"}, headers=_auth("tok-a")
        )
        assert r.status_code == 202

    def test_over_limit_returns_429_with_retry_after(self, limited_client):
        limited_client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        ok = limited_client.post(
            f"/streams/{SID}/messages", json={"body": "m1"}, headers=_auth("tok-a")
        )
        assert ok.status_code == 202
        limited = limited_client.post(
            f"/streams/{SID}/messages", json={"body": "m2"}, headers=_auth("tok-a")
        )
        assert limited.status_code == 429
        assert limited.json()["code"] == "RateLimitExceededError"
        assert "Retry-After" in limited.headers


class TestAckStream:
    def test_read_member_can_ack_and_deletes_outbox_entries(self, client, settings):
        import sqlite3

        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        r1 = client.post(
            f"/streams/{SID}/messages", json={"body": "m1"}, headers=_auth("tok-a")
        )
        r2 = client.post(
            f"/streams/{SID}/messages", json={"body": "m2"}, headers=_auth("tok-a")
        )
        publish_id_2 = r2.json()["publish_id"]

        r = client.post(
            f"/streams/{SID}/ack",
            json={"up_to_publish_id": publish_id_2},
            headers=_auth("tok-b"),
        )
        assert r.status_code == 200

        conn = sqlite3.connect(settings.db_path)
        try:
            rows = conn.execute(
                "SELECT * FROM outbox WHERE target_type = 'stream' AND member_identity = 'agent-b'"
            ).fetchall()
        finally:
            conn.close()
        assert rows == []

    def test_cumulative_ack_leaves_later_entries(self, client, settings):
        import sqlite3

        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        r1 = client.post(
            f"/streams/{SID}/messages", json={"body": "m1"}, headers=_auth("tok-a")
        )
        r2 = client.post(
            f"/streams/{SID}/messages", json={"body": "m2"}, headers=_auth("tok-a")
        )
        publish_id_1 = r1.json()["publish_id"]

        client.post(
            f"/streams/{SID}/ack",
            json={"up_to_publish_id": publish_id_1},
            headers=_auth("tok-b"),
        )

        conn = sqlite3.connect(settings.db_path)
        try:
            rows = conn.execute(
                "SELECT publish_id FROM outbox"
                " WHERE target_type = 'stream' AND member_identity = 'agent-b'"
            ).fetchall()
        finally:
            conn.close()
        assert [row[0] for row in rows] == [r2.json()["publish_id"]]

    def test_ack_is_idempotent(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-a", "access": "read_write"},
            headers=_auth("tok-a"),
        )
        client.post(f"/streams/{SID}/messages", json={"body": "m1"}, headers=_auth("tok-a"))
        r1 = client.post(
            f"/streams/{SID}/ack", json={"up_to_publish_id": 999}, headers=_auth("tok-a")
        )
        r2 = client.post(
            f"/streams/{SID}/ack", json={"up_to_publish_id": 999}, headers=_auth("tok-a")
        )
        assert r1.status_code == 200
        assert r2.status_code == 200

    def test_write_only_member_gets_404(self, client):
        """write 権限だけでは ack できない（配達対象は read 権限を持つ member のみ）。"""
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-b", "access": "write"},
            headers=_auth("tok-a"),
        )
        r = client.post(
            f"/streams/{SID}/ack", json={"up_to_publish_id": 1}, headers=_auth("tok-b")
        )
        assert r.status_code == 404
        assert r.json()["code"] == "StreamNotFoundError"

    def test_non_member_gets_404_not_403(self, client):
        """存在と非所有を同一 404 に隠す（wire-api.md §5.6 / §5.7）。"""
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        r = client.post(
            f"/streams/{SID}/ack", json={"up_to_publish_id": 1}, headers=_auth("tok-b")
        )
        assert r.status_code == 404

    def test_missing_stream_returns_404(self, client):
        r = client.post(
            "/streams/nope/ack", json={"up_to_publish_id": 1}, headers=_auth("tok-a")
        )
        assert r.status_code == 404

    def test_invalid_up_to_publish_id_returns_400(self, client):
        client.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
        # 唯一の write member を read に落とすと write member 0 人ガードで拒否されるため、
        # read 権限（ack に必要）は read_write で付与する（write は維持）。
        client.put(
            f"/streams/{SID}/members",
            json={"identity": "agent-a", "access": "read_write"},
            headers=_auth("tok-a"),
        )
        r = client.post(
            f"/streams/{SID}/ack", json={"up_to_publish_id": "abc"}, headers=_auth("tok-a")
        )
        assert r.status_code == 400


class TestStreamRegistryIsolatedPerApp:
    def test_two_app_instances_do_not_share_state(self, settings, tmp_path):
        settings_2 = Settings(
            db_path=str(tmp_path / "test_relay_2.db"), auth_tokens={"tok-a": "agent-a"}
        )
        app1 = create_app(settings)
        app2 = create_app(settings_2)
        with TestClient(app1) as c1, TestClient(app2) as c2:
            c1.post("/streams", json={"name": "s1"}, headers=_auth("tok-a"))
            r = c2.get(f"/streams/{SID}", headers=_auth("tok-a"))
            assert r.status_code == 404
