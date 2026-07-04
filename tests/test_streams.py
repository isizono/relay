"""relay.streams テストスイート。

stream CRUD・membership CRUD・stream レーン publish・stream レーン ack と、それぞれの
structural authZ（write 権限 membership 照合、identity-authz.md §2.2）を検証する。

`TestStreamRegistry` は in-memory registry の単体テスト、それ以外は実際に Starlette
アプリを起動して HTTP リクエストを投げる統合テスト（既存 test_app.py のパターンを踏襲）。
"""
import pytest
from starlette.testclient import TestClient

from datetime import datetime, timedelta, timezone

from relay.app import create_app
from relay.config import Settings
from relay.errors import ResourceLimitExceeded
from relay.streams import StreamRegistry


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
# HTTP 統合テスト
# ---------------------------------------------------------------------------


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
    def test_creates_stream_and_returns_201(self, client):
        r = client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        assert r.status_code == 201
        body = r.json()
        assert body["stream_id"] == "s1"
        assert "created_at" in body

    def test_creator_becomes_write_member(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        r = client.get("/streams/s1/members", headers=_auth("tok-a"))
        assert r.json() == {"members": [{"identity": "agent-a", "access": "write"}]}

    def test_duplicate_stream_id_returns_409(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        r = client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-b"))
        assert r.status_code == 409
        assert r.json()["code"] == "StreamAlreadyExistsError"

    def test_missing_stream_id_returns_400(self, client):
        r = client.post("/streams", json={}, headers=_auth("tok-a"))
        assert r.status_code == 400

    def test_default_ttl_out_of_range_returns_400(self, client):
        r = client.post(
            "/streams", json={"stream_id": "s1", "default_ttl": 10}, headers=_auth("tok-a")
        )
        assert r.status_code == 400

    def test_default_ttl_within_range_accepted(self, client):
        r = client.post(
            "/streams", json={"stream_id": "s1", "default_ttl": 3600}, headers=_auth("tok-a")
        )
        assert r.status_code == 201

    def test_requires_auth(self, client):
        r = client.post("/streams", json={"stream_id": "s1"})
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
        r = limited_client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        assert r.status_code == 201

    def test_per_identity_limit_returns_429(self, limited_client):
        limited_client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        limited_client.post("/streams", json={"stream_id": "s2"}, headers=_auth("tok-a"))
        # agent-a の 3 件目は per-identity 上限(2)超過で拒否される。
        r = limited_client.post("/streams", json={"stream_id": "s3"}, headers=_auth("tok-a"))
        assert r.status_code == 429
        assert r.json()["code"] == "ResourceLimitExceededError"

    def test_total_limit_returns_429(self, limited_client):
        # agent-a 2 件 + agent-b 1 件で total 上限(3)に到達させる。
        limited_client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        limited_client.post("/streams", json={"stream_id": "s2"}, headers=_auth("tok-a"))
        limited_client.post("/streams", json={"stream_id": "s3"}, headers=_auth("tok-b"))
        # agent-b は per-identity 枠に空きがあるが total 上限で拒否される。
        r = limited_client.post("/streams", json={"stream_id": "s4"}, headers=_auth("tok-b"))
        assert r.status_code == 429
        assert r.json()["code"] == "ResourceLimitExceededError"


class TestGetStream:
    def test_returns_stream_meta(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        r = client.get("/streams/s1", headers=_auth("tok-a"))
        assert r.status_code == 200
        body = r.json()
        assert body["stream_id"] == "s1"
        assert body["state"] == "open"

    def test_missing_stream_returns_404(self, client):
        r = client.get("/streams/nope", headers=_auth("tok-a"))
        assert r.status_code == 404
        assert r.json()["code"] == "StreamNotFoundError"

    def test_read_is_open_to_non_members(self, client):
        """read は全許可（identity-authz.md §2.1）。membership 不要。"""
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        r = client.get("/streams/s1", headers=_auth("tok-b"))
        assert r.status_code == 200


class TestCloseStream:
    def test_write_member_can_close(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        r = client.delete("/streams/s1", headers=_auth("tok-a"))
        assert r.status_code == 204
        assert client.get("/streams/s1", headers=_auth("tok-a")).json()["state"] == "closed"

    def test_non_member_forbidden(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        r = client.delete("/streams/s1", headers=_auth("tok-b"))
        assert r.status_code == 403
        assert r.json()["code"] == "MembershipRequiredError"

    def test_read_only_member_forbidden(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        r = client.delete("/streams/s1", headers=_auth("tok-b"))
        assert r.status_code == 403

    def test_missing_stream_returns_404(self, client):
        r = client.delete("/streams/nope", headers=_auth("tok-a"))
        assert r.status_code == 404

    def test_close_is_idempotent(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.delete("/streams/s1", headers=_auth("tok-a"))
        r = client.delete("/streams/s1", headers=_auth("tok-a"))
        assert r.status_code == 204


class TestMembers:
    def test_write_member_can_add_member(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        r = client.put(
            "/streams/s1/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 200
        members = client.get("/streams/s1/members", headers=_auth("tok-a")).json()["members"]
        assert {"identity": "agent-b", "access": "read"} in members

    def test_non_write_member_cannot_add_member(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        r = client.put(
            "/streams/s1/members",
            json={"identity": "agent-c", "access": "read"},
            headers=_auth("tok-b"),
        )
        assert r.status_code == 403

    def test_invalid_access_value_returns_400(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        r = client.put(
            "/streams/s1/members",
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
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        r = client.put(
            "/streams/s1/members",
            json={"identity": "agent-a", "access": "read"},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 400
        assert r.json()["code"] == "InvalidRequestError"
        # 拒否後も agent-a は write のままで、stream は操作可能。
        members = client.get("/streams/s1/members", headers=_auth("tok-a")).json()["members"]
        assert {"identity": "agent-a", "access": "write"} in members

    def test_demoting_write_member_allowed_when_another_write_exists(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-b", "access": "write"},
            headers=_auth("tok-a"),
        )
        r = client.put(
            "/streams/s1/members",
            json={"identity": "agent-a", "access": "read"},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 200

    def test_write_member_can_remove_other_member(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        r = client.delete(
            "/streams/s1/members", params={"identity": "agent-b"}, headers=_auth("tok-a")
        )
        assert r.status_code == 204
        members = client.get("/streams/s1/members", headers=_auth("tok-a")).json()["members"]
        assert all(m["identity"] != "agent-b" for m in members)

    def test_self_removal_always_allowed(self, client):
        """離脱（自分自身の membership 削除）は write 権限がなくても本人なら常に許可（identity-authz.md §2.2）。"""
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        r = client.delete(
            "/streams/s1/members", params={"identity": "agent-b"}, headers=_auth("tok-b")
        )
        assert r.status_code == 204

    def test_non_write_member_cannot_remove_others(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-c", "access": "read"},
            headers=_auth("tok-a"),
        )
        r = client.delete(
            "/streams/s1/members", params={"identity": "agent-c"}, headers=_auth("tok-b")
        )
        assert r.status_code == 403

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
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        client.post("/streams/s1/messages", json={"body": "m1"}, headers=_auth("tok-a"))

        # 送信直後は agent-b（read member）宛 outbox エントリが 1 件ある。
        outbox_before, _ = self._stream_outbox_dlq_counts(settings, "s1", "agent-b")
        assert outbox_before == 1

        r = client.delete(
            "/streams/s1/members", params={"identity": "agent-b"}, headers=_auth("tok-b")
        )
        assert r.status_code == 204

        # 即時削除で outbox からも消え、DLQ にも入らない（痕跡なし）。
        outbox_after, dlq_after = self._stream_outbox_dlq_counts(settings, "s1", "agent-b")
        assert outbox_after == 0
        assert dlq_after == 0

    def test_removal_by_other_member_preserves_entry_for_sweep(self, client, settings):
        """他 member による除去は即時削除せず、エントリを DLQ sweep 経路に残す（wire-api.md §6.6）。

        自己離脱と対照的に、involuntary な read 権限喪失は観測対象として保全される。エントリは除去直後
        なら outbox に、dispatcher の DLQ sweep が走った後なら dlq に居る（どちらでも合計 1 件は保たれ、
        自己離脱のように痕跡なく消えることはない）。この合計での検証は背景 dispatcher の sweep timing に
        依存しない（レース耐性）。
        """
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        client.post("/streams/s1/messages", json={"body": "m1"}, headers=_auth("tok-a"))

        # agent-a（write member）が agent-b を除去（本人以外による解除）。
        r = client.delete(
            "/streams/s1/members", params={"identity": "agent-b"}, headers=_auth("tok-a")
        )
        assert r.status_code == 204

        outbox_after, dlq_after = self._stream_outbox_dlq_counts(settings, "s1", "agent-b")
        assert outbox_after + dlq_after == 1

    def test_list_members_open_to_non_members(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        r = client.get("/streams/s1/members", headers=_auth("tok-b"))
        assert r.status_code == 200

    def test_list_members_missing_stream_returns_404(self, client):
        r = client.get("/streams/nope/members", headers=_auth("tok-a"))
        assert r.status_code == 404


class TestPostStreamMessage:
    def test_write_member_can_publish(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-a", "access": "read_write"},
            headers=_auth("tok-a"),
        )
        r = client.post(
            "/streams/s1/messages", json={"body": "hello"}, headers=_auth("tok-a")
        )
        assert r.status_code == 202
        body = r.json()
        assert isinstance(body["publish_id"], int)
        assert body["matched_members"] == 1  # 自分自身が read_write

    def test_matched_members_counts_only_read_access(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-c", "access": "write"},
            headers=_auth("tok-a"),
        )
        r = client.post(
            "/streams/s1/messages", json={"body": "hello"}, headers=_auth("tok-a")
        )
        assert r.json()["matched_members"] == 1  # agent-b のみ（agent-a, agent-c は write のみ）

    def test_creates_outbox_entry_per_read_member(self, client, settings):
        import sqlite3

        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        r = client.post(
            "/streams/s1/messages", json={"body": "hello"}, headers=_auth("tok-a")
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
        assert rows == [("s1", "agent-b", publish_id, b"hello")]

    def test_non_write_member_forbidden(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        r = client.post(
            "/streams/s1/messages", json={"body": "hello"}, headers=_auth("tok-b")
        )
        assert r.status_code == 403
        assert r.json()["code"] == "MembershipRequiredError"

    def test_missing_stream_returns_404(self, client):
        r = client.post(
            "/streams/nope/messages", json={"body": "hello"}, headers=_auth("tok-a")
        )
        assert r.status_code == 404

    def test_closed_stream_returns_410(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.delete("/streams/s1", headers=_auth("tok-a"))
        r = client.post(
            "/streams/s1/messages", json={"body": "hello"}, headers=_auth("tok-a")
        )
        assert r.status_code == 410
        assert r.json()["code"] == "StreamGoneError"

    def test_missing_body_returns_400(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        r = client.post("/streams/s1/messages", json={}, headers=_auth("tok-a"))
        assert r.status_code == 400

    def test_ttl_out_of_range_returns_400(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        r = client.post(
            "/streams/s1/messages",
            json={"body": "hello", "ttl": 5},
            headers=_auth("tok-a"),
        )
        assert r.status_code == 400

    def test_zero_read_members_still_returns_202(self, client):
        """write のみの stream に自分だけ属する場合、matched_members=0 でも 202。"""
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        r = client.post(
            "/streams/s1/messages", json={"body": "hello"}, headers=_auth("tok-a")
        )
        assert r.status_code == 202
        assert r.json()["matched_members"] == 0


class TestAckStream:
    def test_read_member_can_ack_and_deletes_outbox_entries(self, client, settings):
        import sqlite3

        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        r1 = client.post(
            "/streams/s1/messages", json={"body": "m1"}, headers=_auth("tok-a")
        )
        r2 = client.post(
            "/streams/s1/messages", json={"body": "m2"}, headers=_auth("tok-a")
        )
        publish_id_2 = r2.json()["publish_id"]

        r = client.post(
            "/streams/s1/ack",
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

        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-b", "access": "read"},
            headers=_auth("tok-a"),
        )
        r1 = client.post(
            "/streams/s1/messages", json={"body": "m1"}, headers=_auth("tok-a")
        )
        r2 = client.post(
            "/streams/s1/messages", json={"body": "m2"}, headers=_auth("tok-a")
        )
        publish_id_1 = r1.json()["publish_id"]

        client.post(
            "/streams/s1/ack",
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
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-a", "access": "read_write"},
            headers=_auth("tok-a"),
        )
        client.post("/streams/s1/messages", json={"body": "m1"}, headers=_auth("tok-a"))
        r1 = client.post(
            "/streams/s1/ack", json={"up_to_publish_id": 999}, headers=_auth("tok-a")
        )
        r2 = client.post(
            "/streams/s1/ack", json={"up_to_publish_id": 999}, headers=_auth("tok-a")
        )
        assert r1.status_code == 200
        assert r2.status_code == 200

    def test_write_only_member_gets_404(self, client):
        """write 権限だけでは ack できない（配達対象は read 権限を持つ member のみ）。"""
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-b", "access": "write"},
            headers=_auth("tok-a"),
        )
        r = client.post(
            "/streams/s1/ack", json={"up_to_publish_id": 1}, headers=_auth("tok-b")
        )
        assert r.status_code == 404
        assert r.json()["code"] == "StreamNotFoundError"

    def test_non_member_gets_404_not_403(self, client):
        """存在と非所有を同一 404 に隠す（wire-api.md §5.6 / §5.7）。"""
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        r = client.post(
            "/streams/s1/ack", json={"up_to_publish_id": 1}, headers=_auth("tok-b")
        )
        assert r.status_code == 404

    def test_missing_stream_returns_404(self, client):
        r = client.post(
            "/streams/nope/ack", json={"up_to_publish_id": 1}, headers=_auth("tok-a")
        )
        assert r.status_code == 404

    def test_invalid_up_to_publish_id_returns_400(self, client):
        client.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
        # 唯一の write member を read に落とすと write member 0 人ガードで拒否されるため、
        # read 権限（ack に必要）は read_write で付与する（write は維持）。
        client.put(
            "/streams/s1/members",
            json={"identity": "agent-a", "access": "read_write"},
            headers=_auth("tok-a"),
        )
        r = client.post(
            "/streams/s1/ack", json={"up_to_publish_id": "abc"}, headers=_auth("tok-a")
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
            c1.post("/streams", json={"stream_id": "s1"}, headers=_auth("tok-a"))
            r = c2.get("/streams/s1", headers=_auth("tok-a"))
            assert r.status_code == 404
