"""relay.agent_cards テストスイート。

外部 agent の AgentCard 取得（HTTP はスタブ注入）・agent_cards table の読み書き・TTL 判定・
JWS 署名検証（identity-authz.md §1.2.3）を検証する。
"""
import json
from datetime import datetime, timedelta, timezone

import pytest
from joserfc.jwk import ECKey

from relay import agent_cards, db, identity
from relay.config import Settings


@pytest.fixture()
def settings(tmp_path):
    return Settings(db_path=str(tmp_path / "test_relay.db"))


@pytest.fixture()
def conn(settings):
    db.init_db(settings.db_path)
    c = db.get_connection(settings.db_path)
    yield c
    c.close()


def _stub_http_get(status: int, body):
    """`(url, timeout) -> (status, bytes)` を返すスタブ。body は dict/list/str/bytes を受ける。"""

    def _get(url: str, timeout: float):
        if isinstance(body, (bytes, bytearray)):
            return status, bytes(body)
        if isinstance(body, str):
            return status, body.encode("utf-8")
        return status, json.dumps(body).encode("utf-8")

    return _get


def _raising_http_get(url: str, timeout: float):
    raise AssertionError("HTTP fetch は呼ばれてはいけません（cache hit のはず）")


# ---------------------------------------------------------------------------
# URL 構築 / fetch
# ---------------------------------------------------------------------------


class TestFetch:
    def test_agent_card_url_appends_well_known_path(self):
        assert agent_cards.agent_card_url("https://ex.test") == (
            "https://ex.test/.well-known/agent-card.json"
        )
        assert agent_cards.agent_card_url("https://ex.test/") == (
            "https://ex.test/.well-known/agent-card.json"
        )

    def test_fetch_agent_card_returns_dict(self):
        card = agent_cards.fetch_agent_card(
            "https://ex.test", http_get=_stub_http_get(200, {"name": "ext", "version": "1.0"})
        )
        assert card == {"name": "ext", "version": "1.0"}

    def test_fetch_non_200_raises(self):
        with pytest.raises(agent_cards.AgentCardFetchError):
            agent_cards.fetch_agent_card(
                "https://ex.test", http_get=_stub_http_get(404, {"x": 1})
            )

    def test_fetch_invalid_json_raises(self):
        with pytest.raises(agent_cards.AgentCardFetchError):
            agent_cards.fetch_agent_card(
                "https://ex.test", http_get=_stub_http_get(200, b"not-json")
            )

    def test_fetch_non_object_raises(self):
        with pytest.raises(agent_cards.AgentCardFetchError):
            agent_cards.fetch_agent_card(
                "https://ex.test", http_get=_stub_http_get(200, [1, 2, 3])
            )

    def test_fetch_network_error_raises(self):
        def _boom(url, timeout):
            raise OSError("connection refused")

        with pytest.raises(agent_cards.AgentCardFetchError):
            agent_cards.fetch_agent_card("https://ex.test", http_get=_boom)

    def test_fetch_jwks_requires_keys_array(self):
        with pytest.raises(agent_cards.AgentCardFetchError):
            agent_cards.fetch_jwks(
                "https://ex.test/jwks", http_get=_stub_http_get(200, {"no": "keys"})
            )
        jwks = agent_cards.fetch_jwks(
            "https://ex.test/jwks", http_get=_stub_http_get(200, {"keys": []})
        )
        assert jwks == {"keys": []}


# ---------------------------------------------------------------------------
# store / get（TTL 判定込み）
# ---------------------------------------------------------------------------


class TestCache:
    def test_store_and_get_roundtrip(self, conn):
        card = {"name": "ext", "version": "1.0"}
        agent_cards.store_agent_card(conn, "agent-x", card, ttl_seconds=3600)
        conn.commit()
        assert agent_cards.get_cached_agent_card(conn, "agent-x") == card

    def test_get_missing_returns_none(self, conn):
        assert agent_cards.get_cached_agent_card(conn, "nobody") is None

    def test_expired_entry_is_cache_miss(self, conn):
        past = datetime.now(timezone.utc) - timedelta(seconds=10)
        agent_cards.store_agent_card(
            conn, "agent-x", {"name": "ext"}, ttl_seconds=1, fetched_at=past
        )
        conn.commit()
        assert agent_cards.get_cached_agent_card(conn, "agent-x") is None

    def test_ttl_none_never_expires(self, conn):
        very_old = datetime.now(timezone.utc) - timedelta(days=3650)
        agent_cards.store_agent_card(
            conn, "agent-x", {"name": "ext"}, ttl_seconds=None, fetched_at=very_old
        )
        conn.commit()
        assert agent_cards.get_cached_agent_card(conn, "agent-x") == {"name": "ext"}

    def test_upsert_overwrites_existing(self, conn):
        agent_cards.store_agent_card(conn, "agent-x", {"v": 1}, ttl_seconds=3600)
        agent_cards.store_agent_card(conn, "agent-x", {"v": 2}, ttl_seconds=3600)
        conn.commit()
        assert agent_cards.get_cached_agent_card(conn, "agent-x") == {"v": 2}
        # PRIMARY KEY なので行は 1 つ。
        count = conn.execute(
            "SELECT COUNT(*) FROM agent_cards WHERE identity = 'agent-x'"
        ).fetchone()[0]
        assert count == 1

    def test_store_and_get_jwks(self, conn):
        jwks = {"keys": [{"kty": "EC", "crv": "P-256", "kid": "k1", "x": "a", "y": "b"}]}
        agent_cards.store_agent_card(conn, "agent-x", {"name": "ext"}, jwks=jwks, ttl_seconds=3600)
        conn.commit()
        assert agent_cards.get_cached_jwks(conn, "agent-x") == jwks

    def test_get_jwks_none_when_not_stored(self, conn):
        agent_cards.store_agent_card(conn, "agent-x", {"name": "ext"}, ttl_seconds=3600)
        conn.commit()
        assert agent_cards.get_cached_jwks(conn, "agent-x") is None


# ---------------------------------------------------------------------------
# get_or_fetch（cache hit / miss）
# ---------------------------------------------------------------------------


class TestGetOrFetch:
    def test_cache_hit_does_not_fetch(self, conn):
        agent_cards.store_agent_card(conn, "agent-x", {"name": "cached"}, ttl_seconds=3600)
        conn.commit()
        card = agent_cards.get_or_fetch_agent_card(
            conn, "agent-x", "https://ex.test", http_get=_raising_http_get
        )
        assert card == {"name": "cached"}

    def test_cache_miss_fetches_and_stores(self, conn):
        card = agent_cards.get_or_fetch_agent_card(
            conn,
            "agent-x",
            "https://ex.test",
            http_get=_stub_http_get(200, {"name": "fresh"}),
            ttl_seconds=3600,
        )
        assert card == {"name": "fresh"}
        # 2 回目は fetch されない（キャッシュに載っている）。
        again = agent_cards.get_or_fetch_agent_card(
            conn, "agent-x", "https://ex.test", http_get=_raising_http_get
        )
        assert again == {"name": "fresh"}

    def test_expired_cache_triggers_refetch(self, conn):
        past = datetime.now(timezone.utc) - timedelta(seconds=10)
        agent_cards.store_agent_card(
            conn, "agent-x", {"name": "stale"}, ttl_seconds=1, fetched_at=past
        )
        conn.commit()
        card = agent_cards.get_or_fetch_agent_card(
            conn,
            "agent-x",
            "https://ex.test",
            http_get=_stub_http_get(200, {"name": "refreshed"}),
            ttl_seconds=3600,
        )
        assert card == {"name": "refreshed"}


# ---------------------------------------------------------------------------
# JWS 署名検証（identity-authz.md §1.2.3）
# ---------------------------------------------------------------------------


@pytest.fixture()
def signed_card():
    key = ECKey.generate_key("P-256")
    priv_pem = key.as_pem(private=True).decode()
    pub_pem = key.as_pem(private=False).decode()
    card = identity.sign_agent_card(
        {"name": "ext", "version": "1.0"},
        private_key_pem=priv_pem,
        kid="k1",
        jku="https://ex.test/jwks",
    )
    jwks = {"keys": [dict(key.as_dict(private=False), kid="k1")]}
    return card, pub_pem, jwks


class TestVerifyCardSignature:
    def test_verify_with_pem(self, signed_card):
        card, pub_pem, _jwks = signed_card
        assert agent_cards.verify_card_signature(card, public_key_pem=pub_pem) is True

    def test_verify_with_jwks(self, signed_card):
        card, _pub_pem, jwks = signed_card
        assert agent_cards.verify_card_signature(card, jwks=jwks) is True

    def test_tampered_card_fails_pem(self, signed_card):
        card, pub_pem, _jwks = signed_card
        tampered = dict(card)
        tampered["name"] = "evil"
        assert agent_cards.verify_card_signature(tampered, public_key_pem=pub_pem) is False

    def test_tampered_card_fails_jwks(self, signed_card):
        card, _pub_pem, jwks = signed_card
        tampered = dict(card)
        tampered["name"] = "evil"
        assert agent_cards.verify_card_signature(tampered, jwks=jwks) is False

    def test_wrong_key_fails(self, signed_card):
        card, _pub_pem, _jwks = signed_card
        other = ECKey.generate_key("P-256")
        wrong_jwks = {"keys": [dict(other.as_dict(private=False), kid="k1")]}
        assert agent_cards.verify_card_signature(card, jwks=wrong_jwks) is False

    def test_no_key_material_returns_false(self, signed_card):
        card, _pub_pem, _jwks = signed_card
        assert agent_cards.verify_card_signature(card) is False

    def test_unsigned_card_fails(self):
        assert agent_cards.verify_card_signature({"name": "ext"}, jwks={"keys": []}) is False

    @pytest.mark.parametrize(
        "malformed_signatures",
        [
            [{"foo": "bar"}],  # dict だが protected/signature キー欠落
            {"protected": "x", "signature": "y"},  # list でなく dict（[0] が KeyError）
            ["garbage"],  # 要素が dict でなく文字列（['protected'] が TypeError）
        ],
        ids=["missing_keys", "signatures_is_dict", "element_is_string"],
    )
    def test_malformed_signatures_returns_false_not_crash_pem(
        self, signed_card, malformed_signatures
    ):
        """外部 agent から受け取る非信頼 AgentCard の malformed signatures 構造で crash せず
        fail-closed（False）を返すことを検証する（PEM 経路、回帰: KeyError / TypeError が
        生の例外として get_or_fetch_agent_card の呼び出し元に漏れていた）。"""
        _card, pub_pem, _jwks = signed_card
        malformed_card = {"name": "ext", "signatures": malformed_signatures}
        assert agent_cards.verify_card_signature(malformed_card, public_key_pem=pub_pem) is False

    @pytest.mark.parametrize(
        "malformed_signatures",
        [
            [{"foo": "bar"}],
            {"protected": "x", "signature": "y"},
            ["garbage"],
        ],
        ids=["missing_keys", "signatures_is_dict", "element_is_string"],
    )
    def test_malformed_signatures_returns_false_not_crash_jwks(
        self, signed_card, malformed_signatures
    ):
        """PEM 経路と同じ malformed input を JWKS 経路（`verify_card_signature` 自前の
        signatures パース）でも検証する。"""
        _card, _pub_pem, jwks = signed_card
        malformed_card = {"name": "ext", "signatures": malformed_signatures}
        assert agent_cards.verify_card_signature(malformed_card, jwks=jwks) is False


class TestGetOrFetchWithVerification:
    def test_valid_signature_is_stored(self, conn, signed_card):
        card, _pub_pem, jwks = signed_card
        result = agent_cards.get_or_fetch_agent_card(
            conn,
            "agent-x",
            "https://ex.test",
            http_get=_stub_http_get(200, card),
            ttl_seconds=3600,
            verify_jwks=jwks,
        )
        assert result["name"] == "ext"
        assert agent_cards.get_cached_agent_card(conn, "agent-x")["name"] == "ext"
        # 検証に使った JWKS も保存される。
        assert agent_cards.get_cached_jwks(conn, "agent-x") == jwks

    def test_invalid_signature_raises_and_not_cached(self, conn, signed_card):
        card, _pub_pem, jwks = signed_card
        tampered = dict(card)
        tampered["name"] = "evil"
        with pytest.raises(agent_cards.AgentCardFetchError):
            agent_cards.get_or_fetch_agent_card(
                conn,
                "agent-x",
                "https://ex.test",
                http_get=_stub_http_get(200, tampered),
                ttl_seconds=3600,
                verify_jwks=jwks,
            )
        assert agent_cards.get_cached_agent_card(conn, "agent-x") is None


# ---------------------------------------------------------------------------
# ttl_seconds 省略時の既定値配線（Settings.agent_card_cache_ttl_seconds）
# ---------------------------------------------------------------------------


class TestTtlDefaultWiring:
    """`ttl_seconds` を明示的に渡さなかったとき、実際に `Settings.agent_card_cache_ttl_seconds`
    （settings 省略時は `DEFAULT_AGENT_CARD_CACHE_TTL_SECONDS`）が使われることを検証する
    （回帰: config 値が定義されているだけで一切参照されておらず、実質「既定は無期限キャッシュ」に
    なっていた）。
    """

    def test_no_settings_no_ttl_falls_back_to_module_default_and_expires(self, conn):
        past = datetime.now(timezone.utc) - timedelta(
            seconds=agent_cards.DEFAULT_AGENT_CARD_CACHE_TTL_SECONDS + 1
        )
        agent_cards.get_or_fetch_agent_card(
            conn,
            "agent-x",
            "https://ex.test",
            http_get=_stub_http_get(200, {"name": "v1"}),
            now=past,
        )
        # モジュール既定 TTL（1h）を過ぎているので cache miss になり、再 fetch が走って
        # 新しい内容に更新される。ttl_seconds を明示していないのに無期限キャッシュのままなら
        # ここで "v1" のままになってしまう（回帰対象そのもの）。
        refreshed = agent_cards.get_or_fetch_agent_card(
            conn, "agent-x", "https://ex.test", http_get=_stub_http_get(200, {"name": "v2"})
        )
        assert refreshed["name"] == "v2"

    def test_settings_ttl_is_honored_when_no_explicit_ttl_seconds(self, conn):
        """`Settings.agent_card_cache_ttl_seconds` を短く設定すると、その値どおり早く失効する。"""
        short_ttl_settings = Settings(db_path=":memory:", agent_card_cache_ttl_seconds=1)
        past = datetime.now(timezone.utc) - timedelta(seconds=2)

        agent_cards.get_or_fetch_agent_card(
            conn,
            "agent-x",
            "https://ex.test",
            http_get=_stub_http_get(200, {"name": "v1"}),
            settings=short_ttl_settings,
            now=past,
        )
        # settings.agent_card_cache_ttl_seconds=1 秒はとうに過ぎているので再 fetch が走る。
        refreshed = agent_cards.get_or_fetch_agent_card(
            conn,
            "agent-x",
            "https://ex.test",
            http_get=_stub_http_get(200, {"name": "v2"}),
            settings=short_ttl_settings,
        )
        assert refreshed["name"] == "v2"

    def test_explicit_ttl_seconds_none_still_means_infinite(self, conn):
        """`ttl_seconds=None` を明示的に渡した場合は「省略」と区別され、無期限キャッシュのまま。"""
        very_old = datetime.now(timezone.utc) - timedelta(days=3650)
        agent_cards.get_or_fetch_agent_card(
            conn,
            "agent-x",
            "https://ex.test",
            http_get=_stub_http_get(200, {"name": "v1"}),
            ttl_seconds=None,
            now=very_old,
        )
        still_cached = agent_cards.get_or_fetch_agent_card(
            conn, "agent-x", "https://ex.test", http_get=_raising_http_get
        )
        assert still_cached["name"] == "v1"

    def test_explicit_ttl_seconds_overrides_settings(self, conn):
        """`ttl_seconds` を明示的に渡した場合は `settings` より優先される。"""
        long_ttl_settings = Settings(db_path=":memory:", agent_card_cache_ttl_seconds=1)
        agent_cards.get_or_fetch_agent_card(
            conn,
            "agent-x",
            "https://ex.test",
            http_get=_stub_http_get(200, {"name": "v1"}),
            ttl_seconds=3600,
            settings=long_ttl_settings,
        )
        # settings 側は 1 秒だが明示 ttl_seconds=3600 が勝つので、まだ cache hit のまま。
        still_cached = agent_cards.get_or_fetch_agent_card(
            conn,
            "agent-x",
            "https://ex.test",
            http_get=_raising_http_get,
            settings=long_ttl_settings,
        )
        assert still_cached["name"] == "v1"
