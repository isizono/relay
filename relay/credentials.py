"""招待 token / credential の永続化と認証への接続。

招待 token（短命・一回性、`it_` 接頭辞）と redeem 済み credential（長命、`bt_` 接頭辞）を
`invitations` / `credentials` table（`migrations/0003-invitations-credentials.sql`）で
管理する。redeem は単一トランザクション内の atomic UPDATE + rowcount 判定で
exactly-once を保証する（並行 redeem・再送・失効はすべて rowcount != 1 として一律に扱う。
未知 / 失効 / 既 redeem を区別しない HTTP 応答は呼び出し側 `relay.invitations` の責務）。

credential は平文で永続化する。起動時に `load_bearers` を `Settings.auth_tokens` へ merge、
redeem 成功時は同じ dict へ in-process 追加する（`relay.identity.authenticate_request` の
平文 dict 照合はここでは変更しない）。
"""
from __future__ import annotations

import secrets
import sqlite3
from datetime import datetime, timedelta, timezone

from relay import db

INVITE_TOKEN_PREFIX = "it_"
BEARER_TOKEN_PREFIX = "bt_"

# entropy: invite 128bit / bearer 256bit（federation 仕様 parity）。
_INVITE_TOKEN_BYTES = 16
_BEARER_TOKEN_BYTES = 32

DEFAULT_GC_RETENTION_SECONDS = 604800  # 7日


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _add_seconds_iso(base_iso: str, seconds: int) -> str:
    base = datetime.strptime(base_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return (base + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


def issue_invite(
    db_path: str,
    *,
    identity: str,
    invite_ttl_seconds: int,
    credential_ttl_seconds: int | None,
) -> str:
    """招待 token を発行して `invitations` に1行 INSERT し、招待 token 文字列を返す。"""
    now = _now_iso()
    token = INVITE_TOKEN_PREFIX + secrets.token_urlsafe(_INVITE_TOKEN_BYTES)
    expires_at = _add_seconds_iso(now, invite_ttl_seconds)
    conn = db.get_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO invitations"
            " (token, identity, credential_ttl_seconds, created_at, expires_at)"
            " VALUES (?, ?, ?, ?, ?)",
            (token, identity, credential_ttl_seconds, now, expires_at),
        )
        conn.commit()
    finally:
        conn.close()
    return token


def redeem_invite(
    conn: sqlite3.Connection, invite_token: str, now: str
) -> tuple[str, str, str | None] | None:
    """招待 token を消費し bearer credential を発行する。

    単一トランザクション内で invitations の atomic UPDATE（rowcount 判定）→ credentials
    INSERT → invitations.redeemed_credential_id 反映まで行う。未知 / 失効 / 既 redeem の
    token はすべて rowcount != 1 として区別せず `None` を返す（呼び出し側で 404 に変換、
    存在秘匿 regime）。

    Returns:
        成功時 `(bearer_token, identity, expires_at)`。失敗時 `None`。
    """
    cur = conn.execute(
        "UPDATE invitations SET redeemed_at = ?"
        " WHERE token = ? AND redeemed_at IS NULL AND expires_at > ?",
        (now, invite_token, now),
    )
    if cur.rowcount != 1:
        conn.rollback()
        return None

    row = conn.execute(
        "SELECT id, identity, credential_ttl_seconds FROM invitations WHERE token = ?",
        (invite_token,),
    ).fetchone()
    invitation_id = row["id"]
    identity = row["identity"]
    credential_ttl_seconds = row["credential_ttl_seconds"]

    bearer_token = BEARER_TOKEN_PREFIX + secrets.token_urlsafe(_BEARER_TOKEN_BYTES)
    expires_at = (
        _add_seconds_iso(now, credential_ttl_seconds)
        if credential_ttl_seconds is not None
        else None
    )

    cred_cur = conn.execute(
        "INSERT INTO credentials"
        " (token, identity, created_at, expires_at, source_invitation_id)"
        " VALUES (?, ?, ?, ?, ?)",
        (bearer_token, identity, now, expires_at, invitation_id),
    )
    credential_id = cred_cur.lastrowid
    conn.execute(
        "UPDATE invitations SET redeemed_credential_id = ? WHERE id = ?",
        (credential_id, invitation_id),
    )
    conn.commit()
    return bearer_token, identity, expires_at


def was_already_redeemed(conn: sqlite3.Connection, invite_token: str) -> bool:
    """`invite_token` が存在し、既に redeem 済みかどうかを返す。

    `redeem_invite` が `None` を返した後の監査ログ（`invite_reredeem`）を「既 redeem の
    再送」と「未知 / 失効」で区別するためだけに使う。HTTP 応答はこの区別をしない
    （未知/失効/既 redeem を一律 404、存在秘匿）。
    """
    row = conn.execute(
        "SELECT redeemed_at FROM invitations WHERE token = ?", (invite_token,)
    ).fetchone()
    return row is not None and row["redeemed_at"] is not None


def load_bearers(db_path: str, now: str) -> dict[str, str]:
    """現行有効な（未失効・未 revoke）credential の `{token: identity}` 対応表を返す。

    relay 起動時に `Settings.auth_tokens` へ merge するために使う（relay 再起動を
    跨いだ credential の生存）。
    """
    conn = db.get_connection(db_path)
    try:
        rows = conn.execute(
            "SELECT token, identity FROM credentials"
            " WHERE revoked_at IS NULL AND (expires_at IS NULL OR expires_at > ?)",
            (now,),
        ).fetchall()
    finally:
        conn.close()
    return {row["token"]: row["identity"] for row in rows}


def revoke(
    db_path: str,
    *,
    identity: str | None = None,
    credential_id: int | None = None,
    now: str,
) -> int:
    """該当 credential の `revoked_at` をセットする（soft revoke）。

    `identity` または `credential_id` のどちらか一方を指定する。反映は relay 再起動時の
    `load_bearers` 再構築を待つ（in-memory `auth_tokens` は即時には変わらない）。

    Returns:
        更新した行数。
    """
    if (identity is None) == (credential_id is None):
        raise ValueError("identity または credential_id のどちらか一方を指定してください")
    conn = db.get_connection(db_path)
    try:
        if credential_id is not None:
            cur = conn.execute(
                "UPDATE credentials SET revoked_at = ? WHERE id = ? AND revoked_at IS NULL",
                (now, credential_id),
            )
        else:
            cur = conn.execute(
                "UPDATE credentials SET revoked_at = ? WHERE identity = ? AND revoked_at IS NULL",
                (now, identity),
            )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def gc(db_path: str, now: str, retention_seconds: int = DEFAULT_GC_RETENTION_SECONDS) -> None:
    """保持期間を過ぎた消費済み / 失効 invitations と revoke 済み credentials を物理削除する。

    v1 では呼び出し必須ではない（registry の無制限成長を防ぐ prune の SHOULD）。
    """
    cutoff = _add_seconds_iso(now, -retention_seconds)
    conn = db.get_connection(db_path)
    try:
        conn.execute(
            "DELETE FROM invitations"
            " WHERE (redeemed_at IS NOT NULL AND redeemed_at < ?)"
            " OR (redeemed_at IS NULL AND expires_at < ?)",
            (cutoff, cutoff),
        )
        # invitations.redeemed_credential_id は credentials.id への FK 参照。削除対象の
        # credential を指す行が残っていると FK 違反になるため、削除前に参照を外す
        # （invitation 自体の行は残ってよい。redeemed_at が新しく上の DELETE 対象に
        # ならない invitation でも、redeem 先の credential だけ先に失効・GC されうる）。
        conn.execute(
            "UPDATE invitations SET redeemed_credential_id = NULL"
            " WHERE redeemed_credential_id IN ("
            "   SELECT id FROM credentials WHERE revoked_at IS NOT NULL AND revoked_at < ?"
            " )",
            (cutoff,),
        )
        conn.execute(
            "DELETE FROM credentials WHERE revoked_at IS NOT NULL AND revoked_at < ?",
            (cutoff,),
        )
        conn.commit()
    finally:
        conn.close()
