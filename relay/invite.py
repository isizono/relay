"""招待 URL 発行 CLI（`python -m relay.invite`）。

relay DB へ直接 INSERT するローカル CLI。発行操作をネットワークに晒さないための
D1 の実現手段であり、HTTP 発行 endpoint は存在しない。

DB パスの解決順は `--db` 明示 → env `RELAY_DB_PATH` → canonical 絶対パス
`~/.local/state/relay/relay.db`（cwd 相対 fallback は持たない）。launchd が export する
env は対話 shell に伝播しないため、CLI の既定を env 頼みにすると、対話 shell から叩いた
invite が server 側 config の cwd 相対既定（`relay.db`）相当の別 DB に書かれ、
「発行は成功するのに redeem が常に 404」という原因追跡困難な破綻を生む。CLI 側の
絶対パス既定でこれを構造的に排除する。
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from relay import credentials, db

CANONICAL_DB_PATH = str(Path.home() / ".local" / "state" / "relay" / "relay.db")

_TTL_UNITS: dict[str, int] = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def _now_iso() -> str:
    return credentials._now_iso()


def _resolve_db_path(explicit: str | None) -> str:
    """`--db` → env `RELAY_DB_PATH` → canonical 絶対パスの順で DB パスを解決する。

    cwd 相対の fallback は持たない（E9: launchd env は対話 shell に伝播しないため、
    env 頼みの既定は CLI と server の DB 不一致を防げない）。
    """
    if explicit:
        return explicit
    env = os.environ.get("RELAY_DB_PATH")
    if env:
        return env
    return CANONICAL_DB_PATH


def _parse_ttl(value: str) -> int | None:
    """`15m` / `90d` / `none` 形式を秒数（`none` は `None`）に変換する。"""
    if value == "none":
        return None
    if len(value) < 2:
        raise ValueError(f"不正な TTL 形式です: {value!r}（例: 15m, 90d, none）")
    unit = value[-1]
    if unit not in _TTL_UNITS:
        raise ValueError(
            f"TTL の単位は s/m/h/d のいずれかです（none も可）: {value!r}"
        )
    try:
        amount = int(value[:-1])
    except ValueError as exc:
        raise ValueError(f"不正な TTL 形式です: {value!r}（例: 15m, 90d, none）") from exc
    return amount * _TTL_UNITS[unit]


def _mask_token(token: str) -> str:
    return token[:8] + "…" if len(token) > 8 else token


def _cmd_new(args: argparse.Namespace) -> int:
    db_path = _resolve_db_path(args.db)
    db.init_db(db_path)

    try:
        invite_ttl_seconds = _parse_ttl(args.ttl)
    except ValueError as exc:
        print(f"--ttl: {exc}", file=sys.stderr)
        return 2
    if invite_ttl_seconds is None:
        print(
            "--ttl に none は指定できません（招待 token は必ず失効時刻を持ちます）",
            file=sys.stderr,
        )
        return 2

    try:
        credential_ttl_seconds = _parse_ttl(args.credential_ttl)
    except ValueError as exc:
        print(f"--credential-ttl: {exc}", file=sys.stderr)
        return 2

    token = credentials.issue_invite(
        db_path,
        identity=args.identity,
        invite_ttl_seconds=invite_ttl_seconds,
        credential_ttl_seconds=credential_ttl_seconds,
    )
    print(f"{args.base_url}/invitations/redeem#v=1&t={token}")
    return 0


def _cmd_revoke(args: argparse.Namespace) -> int:
    db_path = _resolve_db_path(args.db)
    db.init_db(db_path)
    count = credentials.revoke(
        db_path, identity=args.identity, credential_id=args.credential_id, now=_now_iso()
    )
    if count == 0:
        print("該当する有効な credential が見つかりませんでした", file=sys.stderr)
        return 1
    print(f"{count} 件の credential を失効しました（relay 再起動まで有効なまま残ります）")
    return 0


def _cmd_list(args: argparse.Namespace) -> int:
    db_path = _resolve_db_path(args.db)
    db.init_db(db_path)
    now = _now_iso()

    conn = db.get_connection(db_path)
    try:
        invitation_rows = conn.execute(
            "SELECT token, identity, created_at, expires_at, redeemed_at"
            " FROM invitations ORDER BY id"
        ).fetchall()
        credential_rows = conn.execute(
            "SELECT token, identity, created_at, expires_at, revoked_at"
            " FROM credentials ORDER BY id"
        ).fetchall()
    finally:
        conn.close()

    print("invitations:")
    for row in invitation_rows:
        if row["redeemed_at"] is not None:
            state = "redeemed"
        elif row["expires_at"] < now:
            state = "expired"
        else:
            state = "pending"
        print(
            f"  {_mask_token(row['token'])} identity={row['identity']}"
            f" created_at={row['created_at']} expires_at={row['expires_at']}"
            f" state={state}"
        )

    print("credentials:")
    for row in credential_rows:
        if row["revoked_at"] is not None:
            state = "revoked"
        elif row["expires_at"] is not None and row["expires_at"] < now:
            state = "expired"
        else:
            state = "active"
        print(
            f"  {_mask_token(row['token'])} identity={row['identity']}"
            f" created_at={row['created_at']} expires_at={row['expires_at']}"
            f" state={state}"
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m relay.invite")
    sub = parser.add_subparsers(dest="command", required=True)

    new_parser = sub.add_parser("new", help="招待 URL を新規発行する")
    new_parser.add_argument("--identity", required=True)
    new_parser.add_argument("--ttl", default="15m")
    new_parser.add_argument("--credential-ttl", default="none")
    new_parser.add_argument("--base-url", default="http://127.0.0.1:8770")
    new_parser.add_argument("--db", default=None)
    new_parser.set_defaults(func=_cmd_new)

    revoke_parser = sub.add_parser("revoke", help="credential を失効する")
    selector = revoke_parser.add_mutually_exclusive_group(required=True)
    selector.add_argument("--identity")
    selector.add_argument("--credential-id", type=int)
    revoke_parser.add_argument("--db", default=None)
    revoke_parser.set_defaults(func=_cmd_revoke)

    list_parser = sub.add_parser("list", help="invitations / credentials を一覧表示する")
    list_parser.add_argument("--db", default=None)
    list_parser.set_defaults(func=_cmd_list)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
