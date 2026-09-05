"""keepalive 付きサーバー起動エントリポイント（`python -m relay.serve`）。

`uv run uvicorn relay.app:app` で直接起動すると listen socket は OS 既定の TCP
keepalive のまま（Linux は既定 idle 2 時間程度）になり、NAT / ロードバランサ越しの
長時間アイドル接続がサイレントに切断されうる。ここでは listen socket を自前で作って
`SO_KEEPALIVE` と idle/interval/probe 回数を明示設定してから uvicorn に渡す。
"""
from __future__ import annotations

import argparse
import logging
import os
import socket
import sys

import uvicorn

from relay.app import app as relay_app

logger = logging.getLogger("relay.serve")

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
DEFAULT_LISTEN_BACKLOG = 2048

DEFAULT_TCP_KEEPIDLE_SECONDS = 60
DEFAULT_TCP_KEEPINTVL_SECONDS = 10
DEFAULT_TCP_KEEPCNT = 3


def _env_int(env_var: str, default: int) -> int:
    """整数の環境変数を読む。未設定・空文字なら `default` を返す。"""
    raw = os.environ.get(env_var)
    if not raw:
        return default
    return int(raw)


def resolve_keepalive_settings() -> tuple[int, int, int]:
    """`RELAY_TCP_KEEPIDLE` / `RELAY_TCP_KEEPINTVL` / `RELAY_TCP_KEEPCNT` を解決する。

    戻り値は `(idle_seconds, interval_seconds, probe_count)`。
    """
    return (
        _env_int("RELAY_TCP_KEEPIDLE", DEFAULT_TCP_KEEPIDLE_SECONDS),
        _env_int("RELAY_TCP_KEEPINTVL", DEFAULT_TCP_KEEPINTVL_SECONDS),
        _env_int("RELAY_TCP_KEEPCNT", DEFAULT_TCP_KEEPCNT),
    )


def configure_keepalive(sock: socket.socket, *, idle: int, interval: int, count: int) -> None:
    """listen socket に `SO_KEEPALIVE` とプラットフォーム別 keepalive オプションを設定する。

    `accept()` で生まれる各接続 socket は listen socket のオプションを継承するため、
    接続ごとに設定し直す必要は無い。プラットフォームに存在しない定数（例: Linux 専用の
    `TCP_KEEPIDLE` が無い環境）は設定をスキップして警告ログを出すだけに留め、例外は
    送出しない（keepalive はベストエフォートの延命策であり、無くてもサーバー自体の起動・
    リクエスト処理は成立する必要があるため）。
    """
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)

    # idle: 最後の送受信から最初の keepalive probe を送るまでの秒数。
    # Linux は `TCP_KEEPIDLE`、macOS は同じ意味の `TCP_KEEPALIVE` という別名の定数を使う。
    if hasattr(socket, "TCP_KEEPIDLE"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, idle)
    elif hasattr(socket, "TCP_KEEPALIVE"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPALIVE, idle)
    else:
        logger.warning(
            "platform=%s には TCP_KEEPIDLE も TCP_KEEPALIVE も無いため"
            " keepalive の idle 秒数は設定しない",
            sys.platform,
        )

    if hasattr(socket, "TCP_KEEPINTVL"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, interval)
    else:
        logger.warning(
            "platform=%s には TCP_KEEPINTVL が無いため keepalive の probe 間隔は設定しない",
            sys.platform,
        )

    if hasattr(socket, "TCP_KEEPCNT"):
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, count)
    else:
        logger.warning(
            "platform=%s には TCP_KEEPCNT が無いため keepalive の probe 回数は設定しない",
            sys.platform,
        )


def create_listen_socket(
    host: str, port: int, *, idle: int, interval: int, count: int
) -> socket.socket:
    """keepalive 設定済みの listen socket を作る（uvicorn の `Server.run(sockets=...)` に渡す）。"""
    family = socket.AF_INET6 if ":" in host else socket.AF_INET
    sock = socket.socket(family, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    configure_keepalive(sock, idle=idle, interval=interval, count=count)
    sock.bind((host, port))
    sock.listen(DEFAULT_LISTEN_BACKLOG)
    return sock


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m relay.serve")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--log-level", default="info")
    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO)
    args = build_parser().parse_args(argv)

    idle, interval, count = resolve_keepalive_settings()
    sock = create_listen_socket(args.host, args.port, idle=idle, interval=interval, count=count)
    logger.info(
        "relay.serve: listening on %s:%s (keepalive idle=%ss interval=%ss count=%s)",
        args.host,
        args.port,
        idle,
        interval,
        count,
    )

    config = uvicorn.Config(relay_app, log_level=args.log_level)
    server = uvicorn.Server(config)
    try:
        server.run(sockets=[sock])
    finally:
        sock.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
