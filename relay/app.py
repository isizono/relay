"""relay v2 Starlette アプリの組み立て。

各モジュール（streams / subscriptions / delivery / observability）が公開する
`routes: list[Route]` をここでまとめて登録する。個別 endpoint の実装は各モジュール側の
責務で、本ファイルは配線とアプリ起動ライフサイクル（DB migration 適用 + dispatcher の
起動/停止）のみを持つ。
"""
from __future__ import annotations

import asyncio
import contextlib
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from relay import db, delivery, observability, streams, subscriptions
from relay.config import Settings, get_settings
from relay.identity import MEDIA_TYPE_AGENT_CARD, build_public_agent_card


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
    ]

    app = Starlette(routes=routes, lifespan=lifespan)
    app.state.settings = resolved_settings
    # `GET /status` の `uptime_seconds` 用（wire-api.md §7.1）。壁時計のずれに影響されない
    # `time.monotonic()` を使う。
    app.state.started_at = time.monotonic()
    return app


# uvicorn relay.app:app での起動用（`RELAY_DB_PATH` 等は環境変数で指定）。
app = create_app()
