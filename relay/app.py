"""relay v2 Starlette アプリの組み立て。

各モジュール（streams / subscriptions / delivery / observability）が公開する
`routes: list[Route]` をここでまとめて登録する。個別 endpoint の実装は各モジュール側の
責務で、本ファイルは配線とアプリ起動ライフサイクル（DB migration 適用 + dispatcher の
起動/停止）のみを持つ。
"""
from __future__ import annotations

import asyncio
import contextlib
import sqlite3
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from relay import (
    credentials,
    db,
    delivery,
    federation,
    federation_auth,
    federation_inbound,
    invitations,
    observability,
    streams,
    subscriptions,
)
from relay.config import Settings, get_settings
from relay.errors import OUTBOX_UNAVAILABLE, error_response
from relay.identity import MEDIA_TYPE_AGENT_CARD, build_public_agent_card
from relay.ratelimit import RateLimiter

# 招待 redeem のレート制限（5 req/s、IP キー）。既存 RateLimiter の下限が 1/s のため
# 表現できる最小値をそのまま採る。localhost では全 127.0.0.1 で単一 bucket となり
# 実効はほぼ DoS/spam ガードのみ。
REDEEM_RATE_LIMIT_PER_SECOND = 5

# peer redeem のレート制限（invitations.redeem と同値、IP キー、federation namespace 別 bucket）。
FEDERATION_REDEEM_RATE_LIMIT_PER_SECOND = 5


async def handle_outbox_unavailable(request: Request, exc: Exception) -> Response:
    """outbox（SQLite）への読み書きが失敗した場合の共通ハンドラ。

    disk full / DB corrupt 等で `sqlite3.Error` が送出された場合、relay-v2-wire-api.md
    §8 の規約どおり `503 Service Unavailable` を返す（未処理のまま Starlette デフォルトの
    `500 Internal Server Error` に落ちるのを防ぐ）。個々の endpoint 実装が SQLite 操作の
    たびに try/except するのではなく、Starlette の `exception_handlers` で一箇所に集約する。
    """
    observability.record_event(
        request.app.state, "outbox_error", level="warning", reason=str(exc)
    )
    return error_response(
        503, OUTBOX_UNAVAILABLE, "outbox が一時的に利用できません（disk full / DB corrupt 等）"
    )


async def health(request: Request) -> Response:
    """疎通確認用の最小限の endpoint。認証を要求しない。"""
    return JSONResponse({"status": "ok", "service": "relay"})


async def agent_card(request: Request) -> Response:
    """`GET /.well-known/agent-card.json`（identity-authz.md §1.1.1）。

    公開 AgentCard の取得自体は認証を要求しない。
    """
    settings: Settings = request.app.state.settings
    card = build_public_agent_card(settings)
    return JSONResponse(card, media_type=MEDIA_TYPE_AGENT_CARD)


def create_app(settings: Settings | None = None) -> Starlette:
    """`Settings` から Starlette アプリを組み立てる。

    `settings` 省略時は `relay.config.get_settings()`（環境変数から解決）を使う。
    テストでは一時 DB path を指定した `Settings` を渡して DI する。
    """
    resolved_settings = settings if settings is not None else get_settings()

    @asynccontextmanager
    async def lifespan(app: Starlette) -> AsyncIterator[None]:
        db.init_db(resolved_settings.db_path)

        # 起動時ロード: disk 永続の credential を in-memory auth_tokens へ merge する
        # （relay 再起動を跨いだ credential の生存）。now は delivery._now_iso() で取得する
        # （`_now_iso` は delivery.py の module-private だが、app.py は独自の時刻ヘルパーを
        # 持たないためここでのみ無修飾アクセスする）。
        now = delivery._now_iso()
        resolved_settings.auth_tokens.update(
            credentials.load_bearers(resolved_settings.db_path, now)
        )
        app.state.redeem_rate_limiter = RateLimiter(REDEEM_RATE_LIMIT_PER_SECOND)
        app.state.federation_redeem_rate_limiter = RateLimiter(
            FEDERATION_REDEEM_RATE_LIMIT_PER_SECOND
        )
        # `require_federation_authn`（/federation/* 全 endpoint 共通）が参照する
        # per-peer nonce cache / rate limiter。未初期化のままだと federation サーフェスへの
        # リクエストが AttributeError で 500 になる（起動時に必ず用意する）。
        app.state.federation_nonce_cache = federation_auth.NonceCache()
        app.state.federation_request_rate_limiter = RateLimiter(
            federation_auth.DEFAULT_PEER_REQUEST_RATE_LIMIT_PER_SECOND
        )

        # dispatcher はプロセス内シングルトン（file lock で enforce、
        # relay-v2-wire-api.md §6.2）。lock を取れなかった場合はこのプロセスでは
        # dispatcher を起動しない（他プロセスが既に担っている）。
        lock_fd = delivery.try_acquire_dispatcher_lock(resolved_settings.dispatcher_lock_path)
        dispatcher_task: asyncio.Task | None = None
        if lock_fd is not None:
            dispatcher_task = asyncio.create_task(delivery.run_dispatcher_loop(app))

        try:
            yield
        finally:
            if dispatcher_task is not None:
                dispatcher_task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await dispatcher_task
            if lock_fd is not None:
                delivery.release_dispatcher_lock(lock_fd)

    routes: list[Route] = [
        Route("/", health, methods=["GET"]),
        Route("/.well-known/agent-card.json", agent_card, methods=["GET"]),
        *streams.routes,
        *subscriptions.routes,
        *delivery.routes,
        *observability.routes,
        *invitations.routes,
        *federation.routes,
        *federation_inbound.routes,
    ]

    app = Starlette(
        routes=routes,
        lifespan=lifespan,
        exception_handlers={sqlite3.Error: handle_outbox_unavailable},
    )
    app.state.settings = resolved_settings
    # `GET /status` の `uptime_seconds` 用（wire-api.md §7.1）。壁時計のずれに影響されない
    # `time.monotonic()` を使う。
    app.state.started_at = time.monotonic()
    return app


# uvicorn relay.app:app での起動用（`RELAY_DB_PATH` 等は環境変数で指定）。
app = create_app()
