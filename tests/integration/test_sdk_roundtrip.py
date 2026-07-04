"""SDK ↔ 実 relay（relay/app.py）の integration test（relay-v2-sdk.md §7.2）。

`tests/test_delivery.py` の `LiveServer` パターンを流用し、uvicorn を実 TCP port で
起動した本物の relay に対して SDK の publisher / dispatcher / subscriber を通す。

カバーする最小観点:
- publisher の publish() → dispatcher → 実 relay → subscriber receive() の往復
- dispatcher 停止中に outbox 蓄積 → 起動で配達再開

認証: 実 relay は `Authorization: Bearer <token>` の静的照合（relay/identity.py）。
SDK は `RELAY_BEARER_TOKEN` 環境変数から token を解決する（http/auth.py）。relay の
`Settings.auth_tokens` と揃える。publisher と subscriber は同一 identity（agent-x）で
問題ない（publish は authN のみ、subscribe は subscriber == 認証 identity を要求）。
"""
from __future__ import annotations

import json
import socket
import sqlite3
import threading
import time
from pathlib import Path

import httpx
import pytest
import uvicorn

from relay.app import create_app
from relay.config import Settings
from relay_sdk.client import subscribe
from relay_sdk.outbox import create_outbox_table, publish, run_dispatcher


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


@pytest.fixture()
def agent_card_path(tmp_path) -> str:
    path = tmp_path / "agent-card.json"
    path.write_text(
        json.dumps({"name": "agent-x", "version": "1.0.0"}), encoding="utf-8"
    )
    return str(path)


@pytest.fixture()
def relay_settings(tmp_path) -> Settings:
    return Settings(
        db_path=str(tmp_path / "relay.db"),
        server_log_path=str(tmp_path / "relay.jsonl"),
        dispatcher_lock_path=str(tmp_path / "relay.lock"),
        auth_tokens={"tok-x": "agent-x"},
        dispatcher_poll_interval_seconds=0.02,
    )


@pytest.fixture(autouse=True)
def _bearer(monkeypatch):
    monkeypatch.setenv("RELAY_BEARER_TOKEN", "tok-x")


def _run_sdk_dispatcher(db_path: str, base_url: str, card: str, stop: threading.Event):
    thread = threading.Thread(
        target=run_dispatcher,
        kwargs=dict(
            db_path=db_path,
            relay_base_url=base_url,
            agent_card_path=card,
            poll_interval_seconds=0.02,
            retry_backoff_base_seconds=0.02,
            stop_event=stop,
        ),
        daemon=True,
    )
    thread.start()
    return thread


class TestRoundTrip:
    def test_publish_dispatcher_relay_subscriber(self, relay_settings, agent_card_path, tmp_path):
        app = create_app(relay_settings)
        outbox_db = str(tmp_path / "publisher.db")
        conn = sqlite3.connect(outbox_db)
        create_outbox_table(conn)

        with LiveServer(app) as base_url:
            stop = threading.Event()
            dispatcher_thread = None
            try:
                with subscribe(
                    relay_base_url=base_url,
                    subscriber_identity="agent-x",
                    labels=["topic:sdk-it"],
                    agent_card_path=agent_card_path,
                ) as sub:
                    # 業務 write + outbox INSERT（同一 tx）→ commit。
                    publish(
                        conn,
                        ref_type="decision",
                        ref_id=1,
                        labels=["topic:sdk-it", "event:created"],
                        title="hello",
                    )
                    conn.commit()

                    dispatcher_thread = _run_sdk_dispatcher(
                        outbox_db, base_url, agent_card_path, stop
                    )

                    got = []
                    for event in sub.receive():
                        got.append(event)
                        break

                    assert got, "event を受信できなかった"
                    assert got[0].ref_type == "decision"
                    assert got[0].ref_id == "1"  # dispatcher 経路は TEXT 保存 → 文字列
                    assert "topic:sdk-it" in got[0].labels
                    assert got[0].publish_id > 0
            finally:
                stop.set()
                if dispatcher_thread is not None:
                    dispatcher_thread.join(timeout=3)
                conn.close()

        # 配達済み outbox 行は dispatcher が processed_at を打っている。
        check = sqlite3.connect(outbox_db)
        try:
            processed = check.execute(
                "SELECT processed_at FROM relay_outbox ORDER BY id LIMIT 1"
            ).fetchone()[0]
        finally:
            check.close()
        assert processed is not None

    def test_dispatcher_restart_resumes_delivery(self, relay_settings, agent_card_path, tmp_path):
        """dispatcher 停止中に outbox 蓄積 → 起動で配達再開（§7.2）。"""
        app = create_app(relay_settings)
        outbox_db = str(tmp_path / "publisher.db")
        conn = sqlite3.connect(outbox_db)
        create_outbox_table(conn)

        with LiveServer(app) as base_url:
            with subscribe(
                relay_base_url=base_url,
                subscriber_identity="agent-x",
                labels=["topic:sdk-it"],
                agent_card_path=agent_card_path,
            ) as sub:
                # dispatcher を起動しないまま 2 件 outbox に蓄積。
                publish(conn, ref_type="log", ref_id=1, labels=["topic:sdk-it"])
                publish(conn, ref_type="log", ref_id=2, labels=["topic:sdk-it"])
                conn.commit()

                # まだ配達されていない（dispatcher 未起動）。
                time.sleep(0.2)

                stop = threading.Event()
                dispatcher_thread = _run_sdk_dispatcher(
                    outbox_db, base_url, agent_card_path, stop
                )
                try:
                    got = []
                    for event in sub.receive():
                        got.append(event)
                        if len(got) == 2:
                            break
                    assert {e.ref_id for e in got} == {"1", "2"}
                finally:
                    stop.set()
                    dispatcher_thread.join(timeout=3)
        conn.close()
