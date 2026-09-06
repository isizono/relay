"""2 relay を実 TCP port で起動し、federation 招待〜配達の往復を検証する test harness。

以降の federation 関連 PR（セキュリティ残・handshake・NAT 越え等）が「実際に 2 relay 間で
配達が通る」ことを機械的に裏取りできるよう、`tests/integration/test_federation_roundtrip.py`
から利用する想定で切り出した。新しい federation 統合テストを書く場合は、ここの
`LiveRelay` / `FederationPair` / `read_data_event` をそのまま再利用できる。

## 構成

`LiveRelay` が 1 relay インスタンスを実 TCP port（uvicorn + daemon thread、
tests/test_delivery.py・tests/integration/test_sdk_roundtrip.py の `LiveServer` と同型）で
起動する。`FederationPair` が A/B 2 つの `LiveRelay` を束ね、`start()` 内で
招待発行 → redeem → enc-key 交換までを完了させる（`python -m relay.invite peer ...` を
実際に呼ぶ、CLI 経由の E2E。tests/integration/test_federation_cli_roundtrip.py と同じ
アプローチ）。以降は `create_stream` / `add_federation_member` / `publish` /
`open_sse` を組み合わせて往復を検証する。

## dispatcher はテスト側で起動不要

federation egress（`relay.federation_egress.dispatch_federation_egress`）も local push
（`GET /events` への配達、`relay.delivery`）も、`relay/app.py` の lifespan が起動する
in-process の asyncio task（`run_dispatcher_loop`）が両方担う。`Settings` の
`dispatcher_poll_interval_seconds` を短くしてテストの待ち時間を縮めている以外、
dispatcher についてテスト側で特別な配線は要らない。

## SSE の読み方

`GET /events` は終端しないストリームのため、ASGI transport や Starlette `TestClient` では
読めない（tests/test_delivery.py 冒頭 docstring 参照）。本 harness も同じ理由で実ソケット
越しの `httpx.Client.stream()` を使う（`FederationPair.open_sse`）。
"""
from __future__ import annotations

import contextlib
import dataclasses
import io
import json
import socket
import threading
import time
from typing import Any, Iterator

import httpx
import pytest
import uvicorn
from joserfc.jwk import ECKey

from relay import invite
from relay.app import create_app
from relay.config import Settings

STARTUP_TIMEOUT_SECONDS = 5.0
SSE_READ_TIMEOUT_SECONDS = 10.0


def generate_private_pem() -> str:
    """ES256 (P-256) 秘密鍵を PEM で生成する（署名鍵・暗号化鍵のどちらにも使える形式）。"""
    key = ECKey.generate_key("P-256", private=True)
    return key.as_pem(private=True).decode("ascii")


def _run_invite(argv: list[str]) -> tuple[int, str]:
    """`relay.invite.main` を呼び、stdout を（capsys に頼らず）文字列で回収する。"""
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = invite.main(argv)
    return rc, buf.getvalue().strip()


def read_data_event(resp: httpx.Response, timeout: float = SSE_READ_TIMEOUT_SECONDS) -> dict:
    """SSE レスポンスから最初の `data:` 行を読み JSON decode する。

    keepalive コメント行は読み飛ばす（tests/test_delivery.py の `_read_until_data_line`
    と同型）。timeout 内に `data:` 行が来なければ `AssertionError` にする（無限 hang しない）。
    """
    deadline = time.time() + timeout
    for line in resp.iter_lines():
        if line.startswith("data:"):
            return json.loads(line[len("data:") :].strip())
        if time.time() > deadline:
            break
    raise AssertionError(f"data: 行が {timeout}s 以内に観測できませんでした")


def capture_egress_envelope(monkeypatch: pytest.MonkeyPatch) -> dict[str, Any]:
    """`relay.federation_net.build_async_client` を差し替え、egress dispatcher が実際に
    POST する envelope JSON を横取りする。

    `tests/test_federation_egress.py` の `_patch_transport`（`httpx.MockTransport` で
    差し替え）と異なり、こちらは実 `httpx.AsyncClient`（実ソケット）に `event_hooks` を
    足すだけなので、実際の配送は素通りする（相手 relay への到達を壊さずに wire を覗く）。
    戻り値の dict は呼び出し側の publish 後に `captured["envelope"]` で読む
    （非同期の egress cycle がいつ走るか分からないため、辞書を先に渡して後から埋める）。
    """
    captured: dict[str, Any] = {}

    async def _on_request(request: httpx.Request) -> None:
        if request.method == "POST" and "envelope" not in captured:
            with contextlib.suppress(ValueError):
                captured["envelope"] = json.loads(request.content)

    def _build_async_client(*, timeout: float = 10.0) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            follow_redirects=False, timeout=timeout, event_hooks={"request": [_on_request]}
        )

    monkeypatch.setattr("relay.federation_net.build_async_client", _build_async_client)
    return captured


class LiveRelay:
    """1 relay インスタンスを実 TCP port（uvicorn + daemon thread）で起動する。

    `federation_base_url` はポート確定後でないと定まらないため、`start()` 内で
    `Settings` を差し替える（実運用で `RELAY_BASE_URL` を起動前に固定値として渡すのと
    同じ役割を、動的ポートに合わせて事後にやっている）。
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self.app = create_app(settings)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(("127.0.0.1", 0))
        self.port = sock.getsockname()[1]
        sock.close()
        config = uvicorn.Config(self.app, host="127.0.0.1", port=self.port, log_level="warning")
        self.server = uvicorn.Server(config)
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.client: httpx.Client | None = None

    def start(self) -> None:
        self.thread.start()
        deadline = time.time() + STARTUP_TIMEOUT_SECONDS
        while not self.server.started and time.time() < deadline:
            time.sleep(0.02)
        if not self.server.started:
            raise RuntimeError(f"relay が {STARTUP_TIMEOUT_SECONDS}s 以内に起動しませんでした")
        self.settings = dataclasses.replace(self.settings, federation_base_url=self.base_url)
        self.app.state.settings = self.settings
        self.client = httpx.Client(base_url=self.base_url, timeout=10.0)

    def stop(self) -> None:
        if self.client is not None:
            self.client.close()
        self.server.should_exit = True
        self.thread.join(timeout=5)

    def auth_header(self, token: str) -> dict[str, str]:
        return {"Authorization": f"Bearer {token}"}


class FederationPair:
    """招待〜enc-key 交換まで完了させた A/B 2 relay のペア。

    - A: identity `IDENTITY_A`（token `TOKEN_A`）。B を handle `HANDLE_B_ON_A` として pin する。
    - B: identity `IDENTITY_B`（token `TOKEN_B`）。A を handle `HANDLE_A_ON_B` として pin する。

    handle は「相手を自分がどう呼ぶか」（`peer new`/`peer redeem` の `--handle`）であり、
    identity（Bearer token が解決するローカル identity）とは独立した命名空間である
    （federation member は `"{identity}@{handle}"` 形式、`relay.streams` 参照）。
    """

    IDENTITY_A = "alice"
    IDENTITY_B = "carol"
    TOKEN_A = "tok-a-alice"
    TOKEN_B = "tok-b-carol"
    HANDLE_B_ON_A = "relay-b"
    HANDLE_A_ON_B = "relay-a"

    def __init__(self, relay_a: LiveRelay, relay_b: LiveRelay):
        self.relay_a = relay_a
        self.relay_b = relay_b

    def start(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.relay_a.start()
        self.relay_b.start()
        self._pin_peers(monkeypatch)

    def stop(self) -> None:
        self.relay_a.stop()
        self.relay_b.stop()

    def _pin_peers(self, monkeypatch: pytest.MonkeyPatch) -> None:
        key_a_pem = self.relay_a.settings.jws_private_key_pem
        key_b_pem = self.relay_b.settings.jws_private_key_pem
        assert key_a_pem and key_b_pem, "A/B とも jws_private_key_pem が設定済みである必要がある"

        # (a) A: peer new --handle relay-b（B からの redeem を受け付ける招待を発行する）。
        monkeypatch.setenv("RELAY_JWS_PRIVATE_KEY_PEM", key_a_pem)
        rc, invite_url = _run_invite(
            [
                "peer",
                "new",
                "--handle",
                self.HANDLE_B_ON_A,
                "--db",
                self.relay_a.settings.db_path,
                "--base-url",
                self.relay_a.base_url,
            ]
        )
        assert rc == 0, invite_url

        # (b) B: peer redeem <URL> --handle relay-a（B が A を "relay-a" として pin する）。
        monkeypatch.setenv("RELAY_JWS_PRIVATE_KEY_PEM", key_b_pem)
        rc, out = _run_invite(
            [
                "peer",
                "redeem",
                invite_url,
                "--handle",
                self.HANDLE_A_ON_B,
                "--db",
                self.relay_b.settings.db_path,
                "--base-url",
                self.relay_b.base_url,
            ]
        )
        assert rc == 0, out

        # (c) A → B: peer enc-key。応答に B の enc_key がエコーバックされるため、
        # 1 回の呼び出しで双方向に envelope 暗号化鍵が揃う
        # （relay.invite._cmd_peer_enc_key docstring、
        # tests/integration/test_federation_cli_roundtrip.py の同型テスト参照）。
        enc_key_a_pem = self.relay_a.settings.jwe_private_key_pem
        assert enc_key_a_pem, "A の jwe_private_key_pem が設定済みである必要がある"
        monkeypatch.setenv("RELAY_JWS_PRIVATE_KEY_PEM", key_a_pem)
        monkeypatch.setenv("RELAY_JWE_PRIVATE_KEY_PEM", enc_key_a_pem)
        rc, out = _run_invite(
            [
                "peer",
                "enc-key",
                "--handle",
                self.HANDLE_B_ON_A,
                "--db",
                self.relay_a.settings.db_path,
            ]
        )
        assert rc == 0, out

    # -- ドメイン操作（stream / member / publish） ---------------------------------

    def create_stream(self, relay: LiveRelay, token: str, name: str) -> str:
        assert relay.client is not None
        resp = relay.client.post(
            "/streams", json={"name": name}, headers=relay.auth_header(token)
        )
        assert resp.status_code == 201, resp.text
        return resp.json()["stream_id"]

    def add_federation_member(
        self,
        relay: LiveRelay,
        token: str,
        stream_id: str,
        member_identity: str,
        *,
        access: str = "read",
    ) -> httpx.Response:
        assert relay.client is not None
        return relay.client.put(
            f"/streams/{stream_id}/members",
            json={"identity": member_identity, "access": access},
            headers=relay.auth_header(token),
        )

    def publish(self, relay: LiveRelay, token: str, stream_id: str, body: str) -> int:
        assert relay.client is not None
        resp = relay.client.post(
            f"/streams/{stream_id}/messages",
            json={"body": body},
            headers=relay.auth_header(token),
        )
        assert resp.status_code == 202, resp.text
        return resp.json()["publish_id"]

    @contextlib.contextmanager
    def open_sse(self, relay: LiveRelay, token: str) -> Iterator[httpx.Response]:
        assert relay.client is not None
        with relay.client.stream(
            "GET", "/events", headers=relay.auth_header(token)
        ) as resp:
            assert resp.status_code == 200, resp.headers
            yield resp


def _build_settings(tmp_path, *, name: str, token: str, identity: str) -> Settings:
    return Settings(
        db_path=str(tmp_path / f"{name}.db"),
        server_log_path=str(tmp_path / f"{name}.jsonl"),
        dispatcher_lock_path=str(tmp_path / f"{name}.lock"),
        auth_tokens={token: identity},
        jws_private_key_pem=generate_private_pem(),
        jwe_private_key_pem=generate_private_pem(),
        dispatcher_poll_interval_seconds=0.05,
        federation_allow_private_locators=True,
    )


@pytest.fixture()
def federation_pair(tmp_path, monkeypatch) -> Iterator[FederationPair]:
    """招待〜enc-key 交換まで完了させた A/B 2 relay のペアを渡す pytest fixture。

    実サーバーは 127.0.0.1 の動的ポートで listen するため、outbound ガードの private
    レンジ既定拒否を opt-in で解除する（同一ホスト E2E 用、本番既定は false のまま。
    tests/integration/test_federation_cli_roundtrip.py の同名フィクスチャと同じ理由）。
    """
    monkeypatch.setenv("RELAY_FEDERATION_ALLOW_PRIVATE_LOCATORS", "true")

    settings_a = _build_settings(
        tmp_path, name="a", token=FederationPair.TOKEN_A, identity=FederationPair.IDENTITY_A
    )
    settings_b = _build_settings(
        tmp_path, name="b", token=FederationPair.TOKEN_B, identity=FederationPair.IDENTITY_B
    )
    pair = FederationPair(LiveRelay(settings_a), LiveRelay(settings_b))
    pair.start(monkeypatch)
    try:
        yield pair
    finally:
        pair.stop()
