"""tests/contract/ 共通 fixture（relay-v2-sdk.md §7.3）。

`relay_sdk` が生成する HTTP リクエストと実 relay（`relay.app.create_app`）が返す
レスポンス / SSE フレームが `docs/design/relay-v2-wire-api.md` に書かれた形に従うかを、
`relay_sdk.http` の公開関数（`post_subscription` 等）を実 relay に対して呼ぶことで検証する
契約テスト群の共通土台。

`relay_sdk.http.request` の各関数は dispatcher / `Subscription` が実際に使う唯一の
リクエスト構築経路なので、ここでは高レベル API（`subscribe()` / `Subscription`）ではなく
`relay_sdk.http` の関数を直接呼ぶ。高レベル API を挟むと、SSE 受信のような一部の経路で
relay からの生レスポンス（`id:` 行や error envelope の生 JSON）が SDK 側でパースされて
消えてしまい、ワイヤ形状そのものの検証にならないため。

`GET /events` の真のストリーミング検証には実 TCP ソケット越しの uvicorn が要る
（`tests/test_delivery.py` の `LiveServer` と同じ理由: Starlette `TestClient` /
`httpx.ASGITransport` はレスポンス完了を待ってしまい終端しない SSE stream を hang させる）。
非 SSE の contract テストも同じ `LiveServer` で統一し、harness を 1 本にする。
"""
from __future__ import annotations

import socket
import threading
import time
from contextlib import contextmanager

import httpx
import pytest
import uvicorn

from relay.app import create_app
from relay.config import Settings
from relay_sdk.http import make_client


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


@pytest.fixture(autouse=True)
def _no_ambient_bearer_token(monkeypatch):
    """`RELAY_BEARER_TOKEN` が開発者のシェルに export されていても 401 系テストが
    その値を拾わないようにする（`resolve_bearer_token` の優先順位 2 位、http/auth.py）。
    """
    monkeypatch.delenv("RELAY_BEARER_TOKEN", raising=False)


@pytest.fixture()
def relay_settings(tmp_path) -> Settings:
    return Settings(
        db_path=str(tmp_path / "relay.db"),
        server_log_path=str(tmp_path / "relay.jsonl"),
        dispatcher_lock_path=str(tmp_path / "relay.lock"),
        auth_tokens={"tok-a": "agent-a", "tok-b": "agent-b"},
        # SSE contract テストが polling dispatcher の周期を待たされないよう短くする
        # （既定 0.2 秒、tests/integration/test_sdk_roundtrip.py と同じ調整）。
        dispatcher_poll_interval_seconds=0.02,
    )


@pytest.fixture()
def relay_base_url(relay_settings):
    app = create_app(relay_settings)
    with LiveServer(app) as base_url:
        yield base_url


def make_recording_client(base_url: str, token: str) -> httpx.Client:
    """`relay_sdk.http.make_client` が返す client に `last_response` を生やして返す。

    `relay_sdk.http.request.*` の公開関数はパース済みの dict や例外しか返さないため、
    relay が返す生の status code / JSON body（error envelope の `code` / `message` 等）を
    直接検証したいテストは `client.last_response`（直前に送ったリクエストの生
    `httpx.Response`）を見る。

    `last_response` は直前の 1 リクエスト分しか保持しない（同一 client で複数 request を
    送ると上書きされる）。検証したい呼び出しの直後に読むこと。
    """
    client = make_client(base_url, bearer_token=token)
    client.last_response = None

    def _record(response: httpx.Response, *, _client: httpx.Client = client) -> None:
        _client.last_response = response

    client.event_hooks = {"response": [_record]}
    return client


@pytest.fixture()
def sdk_client_factory(relay_base_url):
    """identity ごとの認証済み client（`make_recording_client`）を作る factory。

    既定 `relay_settings` で起動した実 relay に対して発行する。`max_payload_bytes` の
    上書き等、既定と異なる `Settings` が要るテストは `make_relay_app_client_factory` を使う。
    """
    clients: list[httpx.Client] = []

    def _make(token: str) -> httpx.Client:
        client = make_recording_client(relay_base_url, token)
        clients.append(client)
        return client

    yield _make

    for client in clients:
        client.close()


@pytest.fixture()
def make_relay_app_client_factory():
    """任意の `Settings` で実 relay を起動し、identity ごとの認証済み client を返す
    factory を context manager として返す。既定の `relay_settings` / `sdk_client_factory`
    と異なる設定（例: `max_payload_bytes` を絞った 413 テスト）が要る場合に使う。

    使い方::

        with make_relay_app_client_factory(settings) as make_client_fn:
            client = make_client_fn("tok-a")
            ...
    """

    @contextmanager
    def _factory(settings: Settings):
        app = create_app(settings)
        clients: list[httpx.Client] = []
        with LiveServer(app) as base_url:

            def _make(token: str) -> httpx.Client:
                client = make_recording_client(base_url, token)
                clients.append(client)
                return client

            try:
                yield _make
            finally:
                for client in clients:
                    client.close()

    return _factory
