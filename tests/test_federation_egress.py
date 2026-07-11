"""relay.federation_egress テストスイート。

egress dispatcher step の応答コード別処理（202/404/410/429/413/401/5xx/接続不能/タイムアウト）・
next_attempt_at/attempt_count によるリトライバックオフ・順序保証（per-(stream,peer)直列
+ 先頭失敗でレーン停止）・revoked peer sweep・restart-safe 性を httpx.MockTransport で
精密に検証する（tests/test_sdk_dispatcher.py と同じ手法）。末尾に実際の
`require_federation_authn` 検証を通す統合テストも 1 本持つ（署名配線そのものの健全性確認）。
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest
from joserfc.jwk import ECKey
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from relay import db, federation_auth, federation_egress, federation_peers, observability
from relay.config import Settings
from relay.ratelimit import RateLimiter
from relay.streams import StreamRegistry


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
def settings(tmp_path, keypair_a):
    db_path = str(tmp_path / "egress.db")
    db.init_db(db_path)
    return Settings(
        db_path=db_path,
        server_log_path=str(tmp_path / "egress.jsonl"),
        dispatcher_lock_path=str(tmp_path / "egress.lock"),
        jws_private_key_pem=keypair_a["private_pem"],
        federation_allow_private_locators=True,
    )


@pytest.fixture()
def pinned_bob(settings, keypair_b):
    """B（handle=bob）を pin 済みにしておく。"""
    federation_peers.add_peer(
        settings.db_path,
        handle="bob",
        fingerprint=_fp(keypair_b),
        key_jwk=keypair_b["public_jwk"],
        locator="https://8.8.8.8",
    )
    return _fp(keypair_b)


@pytest.fixture()
def app_state(settings):
    state = SimpleNamespace()
    state.settings = settings
    return state


def _future_iso(seconds: int = 3600) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _past_iso(seconds: int = 60) -> str:
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def _insert_publish_log(settings, stream_id: str, publisher_identity: str = "orch") -> int:
    conn = db.get_connection(settings.db_path)
    try:
        cur = conn.execute(
            "INSERT INTO publish_log (lane, stream_id, publisher_identity, enqueued_at)"
            " VALUES ('stream', ?, ?, ?)",
            (stream_id, publisher_identity, federation_egress._now_iso()),
        )
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def _insert_outbox_row(
    settings,
    stream_id: str,
    member_identity: str,
    publish_id: int,
    *,
    body: str = "hello",
    expires_at: str | None = None,
) -> None:
    conn = db.get_connection(settings.db_path)
    try:
        conn.execute(
            "INSERT INTO outbox"
            " (target_type, stream_id, member_identity, publish_id, payload, enqueued_at,"
            " expires_at)"
            " VALUES ('stream', ?, ?, ?, ?, ?, ?)",
            (
                stream_id,
                member_identity,
                publish_id,
                body.encode("utf-8"),
                federation_egress._now_iso(),
                expires_at if expires_at is not None else _future_iso(),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def _publish(settings, stream_id: str, member_identity: str, *, publisher_identity="orch", body="hello") -> int:
    """publish_log + outbox 行を 1 件、実際の post_stream_message 相当の対応関係で挿入する。"""
    publish_id = _insert_publish_log(settings, stream_id, publisher_identity)
    _insert_outbox_row(settings, stream_id, member_identity, publish_id, body=body)
    return publish_id


def _outbox_rows(settings, stream_id: str, member_identity: str | None = None):
    conn = db.get_connection(settings.db_path)
    try:
        if member_identity is None:
            return conn.execute(
                "SELECT * FROM outbox WHERE stream_id = ? ORDER BY publish_id", (stream_id,)
            ).fetchall()
        return conn.execute(
            "SELECT * FROM outbox WHERE stream_id = ? AND member_identity = ? ORDER BY publish_id",
            (stream_id, member_identity),
        ).fetchall()
    finally:
        conn.close()


def _dlq_rows(settings, stream_id: str, member_identity: str | None = None):
    conn = db.get_connection(settings.db_path)
    try:
        if member_identity is None:
            return conn.execute(
                "SELECT * FROM dlq WHERE stream_id = ? ORDER BY publish_id", (stream_id,)
            ).fetchall()
        return conn.execute(
            "SELECT * FROM dlq WHERE stream_id = ? AND member_identity = ? ORDER BY publish_id",
            (stream_id, member_identity),
        ).fetchall()
    finally:
        conn.close()


def _patch_transport(monkeypatch, handler):
    """`federation_net.build_async_client` を `httpx.MockTransport` 差し替え版に monkeypatch する。"""

    def _build(**kwargs):
        return httpx.AsyncClient(transport=httpx.MockTransport(handler))

    monkeypatch.setattr(federation_egress.federation_net, "build_async_client", _build)


async def _run_egress(app_state, settings, stream_registry) -> None:
    db_conn = db.get_connection(settings.db_path)
    try:
        await federation_egress.dispatch_federation_egress(
            app_state, db_conn, settings, stream_registry
        )
        db_conn.commit()
    finally:
        db_conn.close()


def _run(coro):
    return asyncio.run(coro)


def _decode_envelope(request: httpx.Request) -> dict:
    return json.loads(request.content)


# ---------------------------------------------------------------------------
# 応答コード別処理（エッジケース #1-#9）
# ---------------------------------------------------------------------------


class TestResponseCodeHandling:
    def test_202_deletes_outbox_row(self, monkeypatch, settings, app_state, pinned_bob):
        """#1: 202 応答で outbox 行が DELETE される。"""
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        publish_id = _publish(settings, "orch:collab", "orch@bob")

        def handler(request):
            return httpx.Response(202)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        assert _outbox_rows(settings, "orch:collab", "orch@bob") == []
        assert _dlq_rows(settings, "orch:collab", "orch@bob") == []

    def test_404_retries_without_deleting(self, monkeypatch, settings, app_state, pinned_bob):
        """#2: 404 応答で outbox 行を削除せずリトライ対象として残す。"""
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        _publish(settings, "orch:collab", "orch@bob")

        def handler(request):
            return httpx.Response(404)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        rows = _outbox_rows(settings, "orch:collab", "orch@bob")
        assert len(rows) == 1
        assert rows[0]["attempt_count"] == 1
        assert rows[0]["next_attempt_at"] is not None
        assert _dlq_rows(settings, "orch:collab", "orch@bob") == []

    def test_410_moves_to_dlq_immediately(self, monkeypatch, settings, app_state, pinned_bob):
        """#3: 410 応答で即座に DLQ（PeerStreamGone）へ移動する。"""
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        _publish(settings, "orch:collab", "orch@bob")

        def handler(request):
            return httpx.Response(410)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        assert _outbox_rows(settings, "orch:collab", "orch@bob") == []
        dlq = _dlq_rows(settings, "orch:collab", "orch@bob")
        assert len(dlq) == 1
        assert dlq[0]["error_code"] == federation_egress.DLQ_ERROR_PEER_STREAM_GONE

    def test_429_respects_retry_after(self, monkeypatch, settings, app_state, pinned_bob):
        """#4: 429 応答で Retry-After ヘッダに従い再送する。"""
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        _publish(settings, "orch:collab", "orch@bob")

        def handler(request):
            return httpx.Response(429, headers={"Retry-After": "37"})

        _patch_transport(monkeypatch, handler)
        before = datetime.now(timezone.utc)
        _run(_run_egress(app_state, settings, registry))

        rows = _outbox_rows(settings, "orch:collab", "orch@bob")
        assert len(rows) == 1
        next_attempt = datetime.strptime(
            rows[0]["next_attempt_at"], "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=timezone.utc)
        assert (next_attempt - before).total_seconds() >= 36.0

    def test_413_moves_to_dlq_permanently(self, monkeypatch, settings, app_state, pinned_bob):
        """#5: 413 応答で恒久的に DLQ（PeerRejectedTooLarge）へ移動する。"""
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        _publish(settings, "orch:collab", "orch@bob")

        def handler(request):
            return httpx.Response(413)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        assert _outbox_rows(settings, "orch:collab", "orch@bob") == []
        dlq = _dlq_rows(settings, "orch:collab", "orch@bob")
        assert len(dlq) == 1
        assert dlq[0]["error_code"] == federation_egress.DLQ_ERROR_PEER_REJECTED_TOO_LARGE

    def test_401_is_retryable_regardless_of_reason(
        self, monkeypatch, settings, app_state, pinned_bob
    ):
        """#6: 401 応答は原因（時刻ずれ・署名不正等）を問わず一律リトライ対象として残す。"""
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        _publish(settings, "orch:collab", "orch@bob")

        def handler(request):
            return httpx.Response(401, json={"error": "ts_skew or bad signature"})

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        rows = _outbox_rows(settings, "orch:collab", "orch@bob")
        assert len(rows) == 1
        assert rows[0]["attempt_count"] == 1
        assert _dlq_rows(settings, "orch:collab", "orch@bob") == []

    def test_connection_error_is_retryable(self, monkeypatch, settings, app_state, pinned_bob):
        """#7: 接続不達（ConnectError）はリトライ対象として残す。"""
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        _publish(settings, "orch:collab", "orch@bob")

        def handler(request):
            raise httpx.ConnectError("connection refused", request=request)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        rows = _outbox_rows(settings, "orch:collab", "orch@bob")
        assert len(rows) == 1
        assert rows[0]["attempt_count"] == 1

    def test_5xx_is_retryable(self, monkeypatch, settings, app_state, pinned_bob):
        """#8: 5xx（サーバーエラー）はリトライ対象として残す。"""
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        _publish(settings, "orch:collab", "orch@bob")

        def handler(request):
            return httpx.Response(503)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        rows = _outbox_rows(settings, "orch:collab", "orch@bob")
        assert len(rows) == 1
        assert rows[0]["attempt_count"] == 1
        assert _dlq_rows(settings, "orch:collab", "orch@bob") == []

    def test_timeout_is_retryable(self, monkeypatch, settings, app_state, pinned_bob):
        """#9: 接続/読み取りタイムアウトはリトライ対象として残す。"""
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        _publish(settings, "orch:collab", "orch@bob")

        def handler(request):
            raise httpx.ReadTimeout("timed out", request=request)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        rows = _outbox_rows(settings, "orch:collab", "orch@bob")
        assert len(rows) == 1
        assert rows[0]["attempt_count"] == 1

    def test_locator_rejected_is_retryable(self, monkeypatch, settings, app_state):
        """SSRF ガード違反（LocatorRejected、scheme 不正等）は接続不達と同様に
        リトライ対象として残す（HTTP リクエスト自体は発行されない）。
        """
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@carol", "read")
        _publish(settings, "orch:collab", "orch@carol")

        carol_keypair = _generate_keypair()
        federation_peers.add_peer(
            settings.db_path,
            handle="carol",
            fingerprint=_fp(carol_keypair),
            key_jwk=carol_keypair["public_jwk"],
            locator="ftp://8.8.8.8",
        )

        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            return httpx.Response(202)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        assert calls["n"] == 0
        rows = _outbox_rows(settings, "orch:collab", "orch@carol")
        assert len(rows) == 1
        assert rows[0]["attempt_count"] == 1
        assert _dlq_rows(settings, "orch:collab", "orch@carol") == []


# ---------------------------------------------------------------------------
# リトライバックオフ計算・backoff gating
# ---------------------------------------------------------------------------


class TestRetryBackoff:
    def test_backoff_uses_full_jitter_with_attempt_count(
        self, monkeypatch, settings, app_state, pinned_bob
    ):
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        _publish(settings, "orch:collab", "orch@bob")

        monkeypatch.setattr(federation_egress, "full_jitter", lambda base, cap, attempt: 42.0)

        def handler(request):
            return httpx.Response(503)

        _patch_transport(monkeypatch, handler)
        before = datetime.now(timezone.utc)
        _run(_run_egress(app_state, settings, registry))

        rows = _outbox_rows(settings, "orch:collab", "orch@bob")
        next_attempt = datetime.strptime(
            rows[0]["next_attempt_at"], "%Y-%m-%dT%H:%M:%SZ"
        ).replace(tzinfo=timezone.utc)
        assert 41.0 <= (next_attempt - before).total_seconds() <= 43.0

    def test_row_within_backoff_window_is_not_resent(
        self, monkeypatch, settings, app_state, pinned_bob
    ):
        """next_attempt_at が未来のうちは再送しない（同一 cycle 内の gating）。"""
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        publish_id = _publish(settings, "orch:collab", "orch@bob")
        conn = db.get_connection(settings.db_path)
        try:
            conn.execute(
                "UPDATE outbox SET attempt_count = 1, next_attempt_at = ?"
                " WHERE stream_id = ? AND publish_id = ?",
                (_future_iso(3600), "orch:collab", publish_id),
            )
            conn.commit()
        finally:
            conn.close()

        calls = {"n": 0}

        def handler(request):
            calls["n"] += 1
            return httpx.Response(202)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        assert calls["n"] == 0
        rows = _outbox_rows(settings, "orch:collab", "orch@bob")
        assert len(rows) == 1  # 削除されていない、再送もされていない

    def test_row_past_backoff_window_is_resent(self, monkeypatch, settings, app_state, pinned_bob):
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        publish_id = _publish(settings, "orch:collab", "orch@bob")
        conn = db.get_connection(settings.db_path)
        try:
            conn.execute(
                "UPDATE outbox SET attempt_count = 1, next_attempt_at = ?"
                " WHERE stream_id = ? AND publish_id = ?",
                (_past_iso(5), "orch:collab", publish_id),
            )
            conn.commit()
        finally:
            conn.close()

        def handler(request):
            return httpx.Response(202)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        assert _outbox_rows(settings, "orch:collab", "orch@bob") == []


# ---------------------------------------------------------------------------
# retain 委譲（エッジケース #10）: egress は attempt_count 上限を設けない
# ---------------------------------------------------------------------------


class TestRetainDelegation:
    def test_high_attempt_count_is_not_dlqd_by_egress_itself(
        self, monkeypatch, settings, app_state, pinned_bob
    ):
        """#10: attempt_count がどれだけ大きくても egress 自体は DLQ 化しない
        （federation egress 専用の上限は無く、既存 retain 機構に委ねる）。"""
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        publish_id = _publish(settings, "orch:collab", "orch@bob")
        conn = db.get_connection(settings.db_path)
        try:
            conn.execute(
                "UPDATE outbox SET attempt_count = 500 WHERE stream_id = ? AND publish_id = ?",
                ("orch:collab", publish_id),
            )
            conn.commit()
        finally:
            conn.close()

        def handler(request):
            return httpx.Response(503)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        rows = _outbox_rows(settings, "orch:collab", "orch@bob")
        assert len(rows) == 1
        assert rows[0]["attempt_count"] == 501
        assert _dlq_rows(settings, "orch:collab", "orch@bob") == []

    def test_retain_exceeded_row_is_dlqd_by_existing_sweep(self, settings):
        """expires_at 超過行は既存 `_sweep_retain_exceeded`（federation egress 無改変）が DLQ 化する。"""
        from relay import delivery

        publish_id = _insert_publish_log(settings, "orch:collab")
        _insert_outbox_row(
            settings, "orch:collab", "orch@bob", publish_id, expires_at=_past_iso(5)
        )

        conn = db.get_connection(settings.db_path)
        try:
            delivery._sweep_retain_exceeded(conn)
            conn.commit()
        finally:
            conn.close()

        assert _outbox_rows(settings, "orch:collab", "orch@bob") == []
        dlq = _dlq_rows(settings, "orch:collab", "orch@bob")
        assert dlq[0]["error_code"] == delivery.DLQ_ERROR_RETAIN_EXCEEDED


# ---------------------------------------------------------------------------
# 順序保証（エッジケース #11）: per-(stream, peer) 直列 + 先頭失敗でレーン停止
# ---------------------------------------------------------------------------


class TestOrderingAndLaneHalt:
    def test_second_publish_id_not_sent_while_head_fails(
        self, monkeypatch, settings, app_state, pinned_bob
    ):
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        pid1 = _publish(settings, "orch:collab", "orch@bob", body="first")
        pid2 = _publish(settings, "orch:collab", "orch@bob", body="second")

        calls = []

        def handler(request):
            calls.append(_decode_envelope(request)["origin_publish_id"])
            return httpx.Response(404)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        assert calls == [pid1]  # pid2 は一切送信されていない
        rows = {r["publish_id"]: r for r in _outbox_rows(settings, "orch:collab", "orch@bob")}
        assert rows[pid1]["attempt_count"] == 1
        assert rows[pid2]["attempt_count"] == 0
        assert rows[pid2]["next_attempt_at"] is None

    def test_continues_within_same_cycle_while_succeeding(
        self, monkeypatch, settings, app_state, pinned_bob
    ):
        """先頭が成功し続ける限り、同一 cycle 内で複数 publish_id を連続配達する。"""
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        pid1 = _publish(settings, "orch:collab", "orch@bob", body="first")
        pid2 = _publish(settings, "orch:collab", "orch@bob", body="second")

        calls = []

        def handler(request):
            calls.append(_decode_envelope(request)["origin_publish_id"])
            return httpx.Response(202)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        assert calls == [pid1, pid2]
        assert _outbox_rows(settings, "orch:collab", "orch@bob") == []


# ---------------------------------------------------------------------------
# revoked peer sweep（エッジケース #12）
# ---------------------------------------------------------------------------


class TestRevokedPeerSweep:
    def test_revoked_peer_outbox_moves_to_dlq_without_sending(
        self, monkeypatch, settings, app_state, pinned_bob
    ):
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        _publish(settings, "orch:collab", "orch@bob")
        _publish(settings, "orch:collab", "orch@bob")  # 複数行が残っていても全て対象

        federation_peers.revoke_peer(settings.db_path, handle="bob")

        called = {"n": 0}

        def handler(request):
            called["n"] += 1
            return httpx.Response(202)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        assert called["n"] == 0  # revoked peer へは HTTP を発行しない
        assert _outbox_rows(settings, "orch:collab", "orch@bob") == []
        dlq = _dlq_rows(settings, "orch:collab", "orch@bob")
        assert len(dlq) == 2
        assert all(r["error_code"] == federation_egress.DLQ_ERROR_PEER_REVOKED for r in dlq)


# ---------------------------------------------------------------------------
# restart-safe 性: stream が registry に不在なら送信も revoked sweep もスキップ
# ---------------------------------------------------------------------------


class TestRestartSafety:
    def test_unregistered_stream_is_skipped_entirely(self, monkeypatch, settings, app_state, pinned_bob):
        """再起動直後（stream_registry が空）を模す: 送信を試みず、行にも触れない。"""
        _publish(settings, "orch:collab", "orch@bob")
        empty_registry = StreamRegistry()  # "orch:collab" は登録されていない

        called = {"n": 0}

        def handler(request):
            called["n"] += 1
            return httpx.Response(202)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, empty_registry))

        assert called["n"] == 0
        rows = _outbox_rows(settings, "orch:collab", "orch@bob")
        assert len(rows) == 1
        assert rows[0]["attempt_count"] == 0

    def test_unregistered_stream_with_revoked_peer_is_also_skipped(
        self, monkeypatch, settings, app_state, pinned_bob
    ):
        """revoked peer 宛でも、stream が registry に不在なら今 cycle は DLQ 化しない
        （_sweep_stream_permanent_errors と同じ restart-safe パターン）。"""
        _publish(settings, "orch:collab", "orch@bob")
        federation_peers.revoke_peer(settings.db_path, handle="bob")
        empty_registry = StreamRegistry()

        def handler(request):
            return httpx.Response(202)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, empty_registry))

        rows = _outbox_rows(settings, "orch:collab", "orch@bob")
        assert len(rows) == 1  # DLQ 化されていない
        assert _dlq_rows(settings, "orch:collab", "orch@bob") == []


# ---------------------------------------------------------------------------
# envelope 構築（owner 側送信 / reply 方向の origin_stream_id 解決）
# ---------------------------------------------------------------------------


class TestEnvelopeConstruction:
    def test_owner_side_envelope_fields(self, monkeypatch, settings, app_state, pinned_bob):
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        publish_id = _publish(
            settings, "orch:collab", "orch@bob", publisher_identity="orch", body="hi bob"
        )

        captured = {}

        def handler(request):
            captured["envelope"] = _decode_envelope(request)
            captured["path"] = request.url.path
            return httpx.Response(202)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        assert captured["path"] == "/federation/streams/orch:collab/messages"
        envelope = captured["envelope"]
        assert envelope["origin_stream_id"] == "orch:collab"
        assert envelope["origin_publish_id"] == publish_id
        assert envelope["from_sub"] == "orch"
        assert envelope["to_members"] == ["orch"]
        assert envelope["body"] == "hi bob"

    def test_reply_direction_uses_origin_stream_id_from_record(
        self, monkeypatch, settings, app_state
    ):
        """reply 方向: replica の StreamRecord に origin_peer/origin_stream_id が
        設定されている場合、envelope の origin_stream_id はローカル replica id ではなく
        owner 側の元 stream_id を使う（inbound 側が設定する属性、ここでは手動で模す）。
        """
        registry = StreamRegistry()
        registry.create("orch@alice:collab", "@alice", None)
        registry.put_member("orch@alice:collab", "@alice", "read")
        record = registry.get("orch@alice:collab")
        # inbound（plan-a）が replica 生成時に設定する属性を手動で模す。
        record.origin_peer = "alice"
        record.origin_stream_id = "orch:collab"

        publish_id = _publish(
            settings, "orch@alice:collab", "@alice", publisher_identity="orch", body="reply"
        )
        # このテストでは "alice"（owner peer）宛に送るため専用の鍵で pin する。
        alice_keypair = _generate_keypair()
        federation_peers.add_peer(
            settings.db_path,
            handle="alice",
            fingerprint=_fp(alice_keypair),
            key_jwk=alice_keypair["public_jwk"],
            locator="https://8.8.4.4",
        )

        captured = {}

        def handler(request):
            captured["envelope"] = _decode_envelope(request)
            captured["path"] = request.url.path
            return httpx.Response(202)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        assert captured["path"] == "/federation/streams/orch:collab/messages"
        envelope = captured["envelope"]
        assert envelope["origin_stream_id"] == "orch:collab"
        assert envelope["origin_publish_id"] == publish_id
        assert envelope["from_sub"] == "orch"
        # reply 方向の member_identity は "@alice"（sub 部分は空文字列）。
        assert envelope["to_members"] == [""]

    def test_multiple_subs_same_publish_id_combined_into_single_request(
        self, monkeypatch, settings, app_state, pinned_bob
    ):
        """同一 publish_id・同一 peer 宛の複数 sub は 1 回の POST に集約される。"""
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        registry.put_member("orch:collab", "helper@bob", "read")

        publish_id = _insert_publish_log(settings, "orch:collab", "orch")
        _insert_outbox_row(settings, "orch:collab", "orch@bob", publish_id)
        _insert_outbox_row(settings, "orch:collab", "helper@bob", publish_id)

        calls = {"n": 0}
        captured = {}

        def handler(request):
            calls["n"] += 1
            captured["envelope"] = _decode_envelope(request)
            return httpx.Response(202)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        assert calls["n"] == 1
        assert sorted(captured["envelope"]["to_members"]) == ["helper", "orch"]
        assert _outbox_rows(settings, "orch:collab", "orch@bob") == []
        assert _outbox_rows(settings, "orch:collab", "helper@bob") == []

    def test_from_sub_is_none_when_publish_log_row_missing(
        self, monkeypatch, settings, app_state, pinned_bob
    ):
        """publish_log 行が無い outbox 行（防御的ケース、通常は起きない）でも
        envelope 構築はクラッシュせず、from_sub は null として送出される。
        """
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        _insert_outbox_row(settings, "orch:collab", "orch@bob", 999)

        captured = {}

        def handler(request):
            captured["envelope"] = _decode_envelope(request)
            return httpx.Response(202)

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        assert captured["envelope"]["from_sub"] is None
        assert _outbox_rows(settings, "orch:collab", "orch@bob") == []


# ---------------------------------------------------------------------------
# 既存 SSE dispatch との非干渉
# ---------------------------------------------------------------------------


class TestNonFederationNoop:
    def test_local_member_rows_are_untouched(self, monkeypatch, settings, app_state):
        """member_identity に '@' を含まない（ローカル）行は federation egress の対象外。"""
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "helper", "read")
        _publish(settings, "orch:collab", "helper")

        def handler(request):
            raise AssertionError("federation egress は非 federation 行に対して HTTP を発行しない")

        _patch_transport(monkeypatch, handler)
        _run(_run_egress(app_state, settings, registry))

        rows = _outbox_rows(settings, "orch:collab", "helper")
        assert len(rows) == 1
        assert rows[0]["attempt_count"] == 0

    def test_no_op_when_federation_disabled(self, monkeypatch, settings, app_state):
        """jws_private_key_pem 未設定なら federation egress 全体が no-op（fail-closed）。"""
        import dataclasses

        disabled_settings = dataclasses.replace(settings, jws_private_key_pem=None)
        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        _publish(settings, "orch:collab", "orch@bob")

        def handler(request):
            raise AssertionError("federation 無効時は HTTP を発行しない")

        _patch_transport(monkeypatch, handler)

        async def _run_disabled():
            db_conn = db.get_connection(disabled_settings.db_path)
            try:
                await federation_egress.dispatch_federation_egress(
                    app_state, db_conn, disabled_settings, registry
                )
                db_conn.commit()
            finally:
                db_conn.close()

        _run(_run_disabled())

        rows = _outbox_rows(settings, "orch:collab", "orch@bob")
        assert len(rows) == 1
        assert rows[0]["attempt_count"] == 0


# ---------------------------------------------------------------------------
# dispatch_once への配線確認
# ---------------------------------------------------------------------------


class TestDispatchOnceWiring:
    def test_dispatch_once_invokes_federation_egress(self, monkeypatch, settings, pinned_bob):
        from relay import delivery
        from relay.app import create_app

        app = create_app(settings)
        stream_registry = StreamRegistry()
        app.state.stream_registry = stream_registry
        stream_registry.create("orch:collab", "orch", None)
        stream_registry.put_member("orch:collab", "orch@bob", "read")
        _publish(settings, "orch:collab", "orch@bob")

        def handler(request):
            return httpx.Response(202)

        _patch_transport(monkeypatch, handler)
        _run(delivery.dispatch_once(app))

        assert _outbox_rows(settings, "orch:collab", "orch@bob") == []


# ---------------------------------------------------------------------------
# 実際の require_federation_authn 検証を通す統合テスト
# ---------------------------------------------------------------------------


class TestSignedRequestAcceptedByRealVerifier:
    def test_egress_produces_headers_accepted_by_receiver_verifier(
        self, monkeypatch, tmp_path, keypair_a, keypair_b
    ):
        """egress が発行する署名ヘッダーが、実際の `require_federation_authn` を通過することを確認する。"""
        # B（受信側）の settings と、A を pin 済みの peers。
        b_db_path = str(tmp_path / "b.db")
        db.init_db(b_db_path)
        b_settings = Settings(
            db_path=b_db_path,
            server_log_path=str(tmp_path / "b.jsonl"),
            dispatcher_lock_path=str(tmp_path / "b.lock"),
            jws_private_key_pem=keypair_b["private_pem"],
        )
        federation_peers.add_peer(
            b_db_path,
            handle="alice",
            fingerprint=_fp(keypair_a),
            key_jwk=keypair_a["public_jwk"],
            locator="https://8.8.4.4",
        )

        received = {}

        async def _echo_handler(request: Request) -> Response:
            peer = request.state.peer_identity
            received["handle"] = peer.handle
            received["fingerprint"] = peer.fingerprint
            return Response(status_code=202)

        b_app = Starlette(
            routes=[
                Route(
                    "/federation/streams/{stream_id}/messages",
                    federation_auth.require_federation_authn(_echo_handler),
                    methods=["POST"],
                )
            ]
        )
        b_app.state.settings = b_settings
        b_app.state.federation_nonce_cache = federation_auth.NonceCache()
        b_app.state.federation_request_rate_limiter = RateLimiter(1000)

        # A（送信側）の settings。bob を B として pin。
        a_db_path = str(tmp_path / "a.db")
        db.init_db(a_db_path)
        a_settings = Settings(
            db_path=a_db_path,
            server_log_path=str(tmp_path / "a.jsonl"),
            dispatcher_lock_path=str(tmp_path / "a.lock"),
            jws_private_key_pem=keypair_a["private_pem"],
            federation_allow_private_locators=True,
        )
        federation_peers.add_peer(
            a_db_path,
            handle="bob",
            fingerprint=_fp(keypair_b),
            key_jwk=keypair_b["public_jwk"],
            locator="https://8.8.8.8",
        )

        registry = StreamRegistry()
        registry.create("orch:collab", "orch", None)
        registry.put_member("orch:collab", "orch@bob", "read")
        _publish(a_settings, "orch:collab", "orch@bob")

        def _build(**kwargs):
            return httpx.AsyncClient(transport=httpx.ASGITransport(app=b_app))

        monkeypatch.setattr(federation_egress.federation_net, "build_async_client", _build)

        app_state = SimpleNamespace()
        app_state.settings = a_settings
        _run(_run_egress(app_state, a_settings, registry))

        assert received["handle"] == "alice"
        assert received["fingerprint"] == _fp(keypair_a)
        # 202 応答を実際に受理し outbox 行が削除されている。
        assert _outbox_rows(a_settings, "orch:collab", "orch@bob") == []
