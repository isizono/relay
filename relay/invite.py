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
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import parse_qs, urlparse, urlunparse

import httpx

from relay import config, credentials, db, federation_net, federation_peers

CANONICAL_DB_PATH = str(Path.home() / ".local" / "state" / "relay" / "relay.db")

_TTL_UNITS: dict[str, int] = {"s": 1, "m": 60, "h": 3600, "d": 86400}

DEFAULT_FEDERATION_BASE_URL = "http://127.0.0.1:8770"


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


def _resolve_federation_private_key_pem() -> str | None:
    """federation マシン鍵（`RELAY_JWS_PRIVATE_KEY_PEM`、AgentCard 署名鍵と共用）を読む。"""
    return os.environ.get("RELAY_JWS_PRIVATE_KEY_PEM")


def _resolve_federation_base_url(explicit: str | None) -> str:
    if explicit:
        return explicit
    return os.environ.get("RELAY_BASE_URL", DEFAULT_FEDERATION_BASE_URL)


def _resolve_allow_private_locators() -> bool:
    raw = os.environ.get("RELAY_FEDERATION_ALLOW_PRIVATE_LOCATORS")
    if raw is None:
        return False
    return raw.strip().lower() in ("1", "true")


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
        config.validate_local_identity(args.identity)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

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


# ---------------------------------------------------------------------------
# peer サブコマンド（federation 招待ベース鍵交換）
# ---------------------------------------------------------------------------


def _cmd_peer_new(args: argparse.Namespace) -> int:
    db_path = _resolve_db_path(args.db)
    db.init_db(db_path)

    private_key_pem = _resolve_federation_private_key_pem()
    if not private_key_pem:
        print(
            "federation マシン鍵が未設定です（RELAY_JWS_PRIVATE_KEY_PEM）",
            file=sys.stderr,
        )
        return 2

    try:
        federation_peers.validate_peer_handle(args.handle)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

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

    own_fingerprint = federation_peers.compute_fingerprint(
        federation_peers.public_jwk_from_pem(private_key_pem)
    )
    token = federation_peers.issue_peer_invite(
        db_path, handle=args.handle, invite_ttl_seconds=invite_ttl_seconds
    )
    base_url = _resolve_federation_base_url(args.base_url)
    print(f"{base_url}/federation/peers/redeem#v=1&t={token}&fp={own_fingerprint}")
    return 0


def _cmd_peer_redeem(args: argparse.Namespace) -> int:
    db_path = _resolve_db_path(args.db)
    db.init_db(db_path)

    private_key_pem = _resolve_federation_private_key_pem()
    if not private_key_pem:
        print(
            "federation マシン鍵が未設定です（RELAY_JWS_PRIVATE_KEY_PEM）",
            file=sys.stderr,
        )
        return 2

    try:
        federation_peers.validate_peer_handle(args.handle)
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 2

    parsed = urlparse(args.url)
    fragment_params = parse_qs(parsed.fragment)
    token = fragment_params.get("t", [None])[0]
    a_fingerprint = fragment_params.get("fp", [None])[0]
    if not token or not a_fingerprint:
        print(
            "招待 URL の形式が不正です（fragment に t / fp が必要です）", file=sys.stderr
        )
        return 2
    redeem_endpoint = urlunparse(parsed._replace(fragment=""))

    allow_private = _resolve_allow_private_locators()
    try:
        federation_net.validate_locator(redeem_endpoint, allow_private=allow_private)
    except federation_net.LocatorRejected as exc:
        print(f"招待 URL の locator が拒否されました: {exc}", file=sys.stderr)
        return 1

    own_jwk = federation_peers.public_jwk_from_pem(private_key_pem)
    ts = int(time.time())
    sig_payload = {
        "typ": "relay-fed-redeem",
        "token": token,
        "ts": ts,
        "a_fp": a_fingerprint,
    }
    sig = federation_peers.sign_detached(sig_payload, private_key_pem=private_key_pem)
    own_base_url = _resolve_federation_base_url(args.base_url)
    body = {
        "invite_token": token,
        "ts": ts,
        "a_fp": a_fingerprint,
        "card": {"key": own_jwk, "locator": own_base_url},
        "sig": sig,
    }

    try:
        with federation_net.build_client() as client:
            response = client.post(redeem_endpoint, json=body)
    except httpx.HTTPError as exc:
        print(f"redeem リクエストに失敗しました: {exc}", file=sys.stderr)
        return 1

    try:
        resp_bytes = federation_net.read_body_capped(response)
    except federation_net.LocatorRejected as exc:
        print(f"応答が拒否されました: {exc}", file=sys.stderr)
        return 1

    if response.status_code != 200:
        print(
            f"redeem に失敗しました（status={response.status_code}）:"
            f" {resp_bytes.decode('utf-8', errors='replace')}",
            file=sys.stderr,
        )
        return 1

    try:
        resp_body = json.loads(resp_bytes)
    except json.JSONDecodeError:
        print("応答が不正な JSON です。pin しません。", file=sys.stderr)
        return 1
    if not isinstance(resp_body, dict):
        print("応答が JSON object ではありません。pin しません。", file=sys.stderr)
        return 1
    resp_handle = resp_body.get("handle")
    resp_card = resp_body.get("card")
    resp_sig = resp_body.get("sig")
    if not isinstance(resp_card, dict) or not isinstance(resp_sig, dict):
        print("応答の形式が不正です。pin しません。", file=sys.stderr)
        return 1
    resp_key = resp_card.get("key")
    resp_locator = resp_card.get("locator")

    verify_payload = {
        "typ": "relay-fed-redeem-resp",
        "handle": resp_handle,
        "card": resp_card,
    }
    if not federation_peers.verify_detached(verify_payload, resp_sig, public_key=resp_key):
        print("応答の署名検証に失敗しました。pin しません。", file=sys.stderr)
        return 1

    # チャネルバインディング: 応答の鍵 fingerprint が招待 URL の fp と一致しない場合は
    # 中断して pin しない（不一致は攻撃者による応答詐称の可能性、警報として扱う）。
    resp_fingerprint = federation_peers.compute_fingerprint(resp_key)
    if resp_fingerprint != a_fingerprint:
        print(
            "WARNING: 応答の鍵 fingerprint が招待 URL の fp と一致しません"
            f"（期待={a_fingerprint}, 実際={resp_fingerprint}）。中断し pin しません。",
            file=sys.stderr,
        )
        return 1

    try:
        federation_net.validate_locator(resp_locator, allow_private=allow_private)
    except federation_net.LocatorRejected as exc:
        print(f"応答の locator が拒否されました: {exc}", file=sys.stderr)
        return 1

    try:
        federation_peers.add_peer(
            db_path,
            handle=args.handle,
            fingerprint=resp_fingerprint,
            key_jwk=resp_key,
            locator=resp_locator,
        )
    except federation_peers.PeerAlreadyRegisteredError as exc:
        print(f"pin に失敗しました: {exc}", file=sys.stderr)
        return 1

    print(f"peer '{args.handle}' を pin しました（fingerprint={resp_fingerprint}）")
    return 0


def _cmd_peer_list(args: argparse.Namespace) -> int:
    db_path = _resolve_db_path(args.db)
    db.init_db(db_path)
    peers = federation_peers.list_peers(db_path)
    print("peers:")
    for p in peers:
        state = "revoked" if p["revoked_at"] is not None else "active"
        print(
            f"  handle={p['handle']} fingerprint={p['fingerprint']}"
            f" locator={p['locator']} created_at={p['created_at']} state={state}"
        )
    return 0


def _cmd_peer_revoke(args: argparse.Namespace) -> int:
    db_path = _resolve_db_path(args.db)
    db.init_db(db_path)
    count = federation_peers.revoke_peer(db_path, handle=args.handle)
    if count == 0:
        print("該当する有効な peer が見つかりませんでした", file=sys.stderr)
        return 1
    print(f"peer '{args.handle}' を revoke しました")
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

    peer_parser = sub.add_parser("peer", help="federation peer 操作")
    peer_sub = peer_parser.add_subparsers(dest="peer_command", required=True)

    peer_new_parser = peer_sub.add_parser("new", help="peer 招待 URL を新規発行する")
    peer_new_parser.add_argument("--handle", required=True)
    peer_new_parser.add_argument("--ttl", default="15m")
    peer_new_parser.add_argument("--base-url", default=None)
    peer_new_parser.add_argument("--db", default=None)
    peer_new_parser.set_defaults(func=_cmd_peer_new)

    peer_redeem_parser = peer_sub.add_parser("redeem", help="peer 招待 URL を redeem する")
    peer_redeem_parser.add_argument("url")
    peer_redeem_parser.add_argument("--handle", required=True)
    peer_redeem_parser.add_argument("--base-url", default=None)
    peer_redeem_parser.add_argument("--db", default=None)
    peer_redeem_parser.set_defaults(func=_cmd_peer_redeem)

    peer_list_parser = peer_sub.add_parser("list", help="peer 一覧を表示する")
    peer_list_parser.add_argument("--db", default=None)
    peer_list_parser.set_defaults(func=_cmd_peer_list)

    peer_revoke_parser = peer_sub.add_parser("revoke", help="peer を revoke する")
    peer_revoke_parser.add_argument("--handle", required=True)
    peer_revoke_parser.add_argument("--db", default=None)
    peer_revoke_parser.set_defaults(func=_cmd_peer_revoke)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
