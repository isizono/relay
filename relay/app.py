"""relay v2 Starlette アプリの組み立て。

各モジュール（streams / subscriptions / delivery / observability）が公開する
`routes: list[Route]` をここでまとめて登録する。個別 endpoint の実装は各モジュール側の
責務で、本ファイルは配線とアプリ起動ライフサイクル（DB migration 適用）のみを持つ。
"""
from __future__ import annotations

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
        yield

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
    return app


# uvicorn relay.app:app での起動用（`RELAY_DB_PATH` 等は環境変数で指定）。
app = create_app()
