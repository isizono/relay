"""relay v2 の永続層（SQLite）接続・migration 適用。

disk に永続化するのは outbox / dlq / publish_log / agent_cards の 4 table のみ。
streams / memberships / subscriptions は relay-v2-wire-api.md §0 の R1 原則
（substrate は outbox のみ disk 永続化、それ以外は in-memory）により、本モジュールの
schema には含めない。設計判断の詳細は docs/ARCHITECTURE.md を参照。
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

from yoyo import get_backend, read_migrations

MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "migrations"


def get_connection(db_path: str) -> sqlite3.Connection:
    """スレッドごとに新しい SQLite 接続を開く。

    WAL モード + busy_timeout で並行書き込みの database-is-locked を吸収する
    （旧 server.py の踏襲）。PRAGMA synchronous=NORMAL は WAL と組み合わせて
    fsync コストを抑える定石設定。
    """
    conn = sqlite3.connect(db_path, check_same_thread=False, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def _migrations_url(db_path: str) -> str:
    """yoyo の `get_backend` に渡す sqlite URL を組み立てる。

    yoyo は `sqlite:///relative/path`（3 スラッシュ = 相対）/
    `sqlite:////absolute/path`（4 スラッシュ = 絶対）を区別するため、
    ここでは常に絶対パスへ解決してから 4 スラッシュ形式に統一する。
    `:memory:` はテスト用の特別扱い。
    """
    if db_path == ":memory:":
        return "sqlite:///:memory:"
    abs_path = Path(db_path).resolve()
    return f"sqlite:///{abs_path}"


def apply_migrations(db_path: str, migrations_dir: Path | str = MIGRATIONS_DIR) -> None:
    """`migrations_dir` 配下の未適用 migration を `db_path` に適用する。

    big-bang cut-over 確定（旧 relay.db のデータは破棄してよい）につき、初期実装は
    `migrations/0001-initial-schema.sql` の単一 migration のみを持つ。以後の schema
    変更はここに新しい migration ファイルを追加する形で運用する。
    """
    backend = get_backend(_migrations_url(db_path))
    migrations = read_migrations(str(migrations_dir))
    with backend.lock():
        backend.apply_migrations(backend.to_apply(migrations))


def init_db(db_path: str) -> None:
    """起動時に呼ぶ schema 初期化のエントリポイント。"""
    apply_migrations(db_path)
