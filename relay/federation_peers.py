"""peer レジストリ（pin CRUD）と招待ベース鍵ピン留めの永続化。

`peers` / `peer_invitations`（`migrations/0004-federation-peers.sql`）を既存
invitations / credentials と同一パターン（atomic 消費マーク・一律 404 存在秘匿）で
別テーブルとして持つ。local 招待の産物が Bearer token であるのに対し、peer 招待の
産物は pin 済み公開鍵（federation レーンの authN 素材）であり、信頼の産物が異なる
ため別物として並置する（table・CLI サブコマンドを分離）。

RFC 7638 JWK thumbprint 計算、redemption 署名（detached JWS, JCS canonical payload、
identity.py の AgentCard 署名パターンと同型）もここで提供する。

## envelope 暗号化（JWE）

federation envelope の body（メッセージ本文）を ECDH-ES + A256GCM の compact JWE で
暗号化・復号する（`encrypt_envelope_body` / `decrypt_envelope_body`）。署名鍵（ES256、
peer 認証用）とは別の鍵ペアを使う（`peers.enc_key_jwk` / `Settings.jwe_private_key_pem`、
1 つの鍵を署名と暗号化の 2 用途に流用しない）。alg/enc は常にこの組に固定し、`zip`
圧縮は使わない（algorithm confusion・圧縮サイドチャネル対策、`decrypt_envelope_body`
docstring 参照）。
"""
from __future__ import annotations

import base64
import json
import secrets
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any

import rfc8785
from joserfc import jwe, jws
from joserfc.jwk import ECKey, JWKRegistry

from relay import db

# envelope body 暗号化の alg/enc 固定値（鍵分離: peers.key_jwk / jws_private_key_pem
# とは別の鍵ペアを使う。zip 圧縮ヘッダは使わない）。
JWE_ALG = "ECDH-ES"
JWE_ENC = "A256GCM"
_JWE_ALLOWED_ALGORITHMS = (JWE_ALG, JWE_ENC)

PEER_INVITE_TOKEN_PREFIX = "pi_"
_PEER_INVITE_TOKEN_BYTES = 16  # 128bit（招待 token 仕様、federation 仕様 parity）

_FORBIDDEN_HANDLE_CHARS = ("@", ":", "/")


class PeerAlreadyRegisteredError(Exception):
    """redeem 時に fingerprint または handle が既存 peer と衝突した（呼び出し側で 400 に変換）。"""


def validate_peer_handle(handle: str) -> None:
    """peer handle が `@` `:` `/` を含まない非空文字列であることを検証する。

    `@` を含む local identity と対称の制約。handle は各 relay がローカルに付ける相手の
    あだ名であり、両側で一致する保証はない。
    """
    if not handle or any(ch in handle for ch in _FORBIDDEN_HANDLE_CHARS):
        raise ValueError(f"peer handle に '@' ':' '/' は使用できません: {handle!r}")


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().strftime("%Y-%m-%dT%H:%M:%SZ")


def _add_seconds_iso(base_iso: str, seconds: int) -> str:
    base = datetime.strptime(base_iso, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return (base + timedelta(seconds=seconds)).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# RFC 7638 JWK thumbprint / 公開鍵抽出
# ---------------------------------------------------------------------------


def compute_fingerprint(key_jwk: dict[str, Any]) -> str:
    """公開鍵 JWK の RFC 7638 thumbprint（SHA-256, b64url）を計算する。

    RFC 7638 の thumbprint は `kty` ごとの required field 集合に基づく汎用アルゴリズムの
    ため `JWKRegistry.import_key` で `kty` 別に自動ディスパッチする（federation の署名鍵は
    ES256/EC 固定だが、fingerprint 計算自体を EC に決め打ちしない）。
    """
    key = JWKRegistry.import_key(key_jwk)
    return key.thumbprint()


def public_jwk_from_pem(private_key_pem: str) -> dict[str, Any]:
    """PEM 鍵（秘密鍵可）から対応する公開鍵 JWK dict を取り出す。"""
    key = ECKey.import_key(private_key_pem)
    return key.as_dict(private=False)


def public_enc_jwk_from_pem(private_key_pem: str) -> dict[str, Any]:
    """PEM 鍵（秘密鍵可）から envelope 暗号化用の公開鍵 JWK dict を取り出す。

    `public_jwk_from_pem` と同じ抽出だが、`use: "enc"` を明示する点が異なる。署名鍵の
    JWK には `use` を付与していないため、暗号化鍵とは JWK 単体を見ても区別できる
    （joserfc の `Key.check_use` が `use` 不一致を検出する。取り違え防止の多層目、
    一次防御は鍵ペア自体を分離すること）。
    """
    key = ECKey.import_key(private_key_pem)
    jwk = key.as_dict(private=False)
    jwk["use"] = "enc"
    return jwk


def validate_enc_key_jwk(jwk: Any) -> None:
    """peer から届いた envelope 暗号化用公開鍵 JWK の構造を検証する（fail-closed）。

    ECDH-ES は仕様上複数曲線を扱えるが、federation の署名鍵（ES256/P-256）と揃えて
    P-256 に固定する（曲線 confusion の余地を減らす）。秘密鍵成分 `d` を含む場合は
    送信側の実装ミスで秘密鍵そのものが漏洩した可能性があるため拒否する（相手の秘密鍵を
    自分の DB に保存してしまう事故を未然に防ぐ）。
    """
    if not isinstance(jwk, dict):
        raise ValueError("enc_key は JSON object でなければなりません")
    if jwk.get("kty") != "EC":
        raise ValueError("enc_key.kty は 'EC' でなければなりません")
    if jwk.get("crv") != "P-256":
        raise ValueError("enc_key.crv は 'P-256' でなければなりません")
    if not isinstance(jwk.get("x"), str) or not jwk["x"]:
        raise ValueError("enc_key.x は必須の非空文字列です")
    if not isinstance(jwk.get("y"), str) or not jwk["y"]:
        raise ValueError("enc_key.y は必須の非空文字列です")
    if "d" in jwk:
        raise ValueError("enc_key に秘密鍵成分 'd' を含めることはできません")


class EnvelopeDecryptionError(Exception):
    """envelope body の JWE 復号に失敗した（alg/enc 不一致・鍵不一致・改竄・zip 使用等）。

    理由は区別せず一様にこの例外に畳む（`federation_auth.FederationAuthenticationError`
    と同じ fail-closed 方針）。呼び出し側で 400 相当に変換すること。
    """


def encrypt_envelope_body(plaintext: str, *, public_key_jwk: dict[str, Any]) -> str:
    """envelope body を compact JWE（ECDH-ES + A256GCM、zip 圧縮なし）で暗号化する。

    `algorithms` を `_JWE_ALLOWED_ALGORITHMS` に固定して渡すため、生成される JWE は
    常にこの alg/enc の組になる（`zip` header を含む protected header を渡さない限り
    圧縮は使われない。本関数は明示的に `zip` を指定しないため常に無効）。
    """
    key = ECKey.import_key(public_key_jwk)
    protected = {"alg": JWE_ALG, "enc": JWE_ENC}
    return jwe.encrypt_compact(
        protected, plaintext, key, algorithms=list(_JWE_ALLOWED_ALGORITHMS)
    )


def decrypt_envelope_body(compact_jwe: str, *, private_key_pem: str) -> str:
    """`encrypt_envelope_body` が生成した compact JWE を復号する。

    `algorithms=_JWE_ALLOWED_ALGORITHMS` を渡すことで、joserfc は protected header の
    `enc` を最初にこの許可集合と照合し（`_rfc7516.message._perform_decrypt`）、
    一致しなければ実際の復号（鍵合意・ciphertext 復号）を一切試みず
    `UnsupportedAlgorithmError` を送出する。`alg` も同様に recipient ごとの許可集合
    照合を通る。`zip` header が付与されていても、許可集合に `zip` の値（例: `DEF`）が
    含まれないため展開時に拒否される（受信側で JWE ヘッダの alg を信用してディスパッチ
    しない、algorithm confusion 対策）。理由の区別はせず、いずれの失敗も
    `EnvelopeDecryptionError` に畳んで fail-closed にする。
    """
    key = ECKey.import_key(private_key_pem)
    try:
        result = jwe.decrypt_compact(
            compact_jwe, key, algorithms=list(_JWE_ALLOWED_ALGORITHMS)
        )
    except Exception as exc:
        raise EnvelopeDecryptionError(str(exc)) from exc
    if result.plaintext is None:
        raise EnvelopeDecryptionError("復号結果が空です")
    return result.plaintext.decode("utf-8")


# ---------------------------------------------------------------------------
# detached JWS（JCS canonical payload、typ: relay-fed-redeem / relay-fed-redeem-resp）
# ---------------------------------------------------------------------------


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode("ascii")


def sign_detached(
    payload: dict[str, Any], *, private_key_pem: str, kid: str | None = None
) -> dict[str, str]:
    """`payload` を JCS 正規化し detached JWS（ES256）で署名する。

    identity.py の `sign_agent_card` と同一パターン（payload は署名対象 dict から
    再計算できるため JWS 本体には格納しない、`{protected, signature}` の detached 形）。
    `payload["typ"]` があれば protected header の `typ` に写す（異なる用途の署名対象を
    型で区別し、他コンテキストの署名を誤って通用させない）。`kid` を渡すと protected
    header に含める（検証側が verify 前に「どの peer の鍵で検証すべきか」を判定する経路
    として使う。redemption 署名は peer 未 pin の段階で使うため kid を持たない）。
    """
    canonical = rfc8785.dumps(payload)
    key = ECKey.import_key(private_key_pem)
    protected: dict[str, Any] = {"alg": "ES256"}
    typ = payload.get("typ")
    if typ is not None:
        protected["typ"] = typ
    if kid is not None:
        protected["kid"] = kid
    compact = jws.serialize_compact(protected, canonical, key)
    protected_b64, _payload_b64, signature_b64 = compact.split(".")
    return {"protected": protected_b64, "signature": signature_b64}


def verify_detached(
    payload: dict[str, Any], sig: Any, *, public_key: str | dict[str, Any]
) -> bool:
    """`sign_detached` が生成した signature を検証する。

    `sig` / `payload` は非信頼入力（外部 peer からの POST body）であり得るため、
    identity.py の `verify_agent_card_signature` と同様に構造不正（`sig` が dict でない・
    必須キー欠落等）まで含めて単一の try で fail-closed に畳む。

    Returns:
        署名が正しく、かつ payload（JCS 正規化した `payload`）と一致する場合に True。
        それ以外（構造不正・鍵不一致・署名不一致）はすべて False。
    """
    try:
        canonical = rfc8785.dumps(payload)
        compact = f"{sig['protected']}.{_b64url_encode(canonical)}.{sig['signature']}"
        key = ECKey.import_key(public_key)
        result = jws.deserialize_compact(compact, key)
    except Exception:
        return False
    return result.payload == canonical


# ---------------------------------------------------------------------------
# peers（pin）CRUD
# ---------------------------------------------------------------------------


def _peer_row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
    enc_key_jwk = row["enc_key_jwk"]
    return {
        "id": row["id"],
        "handle": row["handle"],
        "fingerprint": row["fingerprint"],
        "key_jwk": json.loads(row["key_jwk"]),
        "locator": row["locator"],
        "created_at": row["created_at"],
        "revoked_at": row["revoked_at"],
        "disclosure_level": row["disclosure_level"],
        "enc_key_jwk": json.loads(enc_key_jwk) if enc_key_jwk is not None else None,
    }


def _insert_peer(
    conn: sqlite3.Connection,
    *,
    handle: str,
    fingerprint: str,
    key_jwk: dict[str, Any],
    locator: str,
    now: str,
    enc_key_jwk: dict[str, Any] | None = None,
) -> int:
    """`peers` に1行 INSERT する（同一トランザクション内での利用を想定、commit しない）。

    Raises:
        PeerAlreadyRegisteredError: handle または fingerprint が既存 peer と重複する場合。
    """
    validate_peer_handle(handle)
    try:
        cur = conn.execute(
            "INSERT INTO peers (handle, fingerprint, key_jwk, locator, created_at, enc_key_jwk)"
            " VALUES (?, ?, ?, ?, ?, ?)",
            (
                handle,
                fingerprint,
                json.dumps(key_jwk),
                locator,
                now,
                json.dumps(enc_key_jwk) if enc_key_jwk is not None else None,
            ),
        )
    except sqlite3.IntegrityError as exc:
        raise PeerAlreadyRegisteredError(
            f"handle または fingerprint が既存 peer と重複しています: {exc}"
        ) from exc
    return cur.lastrowid


def add_peer(
    db_path: str,
    *,
    handle: str,
    fingerprint: str,
    key_jwk: dict[str, Any],
    locator: str,
    enc_key_jwk: dict[str, Any] | None = None,
) -> int:
    """peer を pin する（`peers` に INSERT、単独トランザクション）。

    B 側 CLI（`peer redeem` の応答照合成功後）と A 側 endpoint（`peer redeem` の署名検証・
    a_fp 照合成功後）の双方から呼ぶ。`enc_key_jwk`（envelope 暗号化用公開鍵）は招待側が
    その時点で暗号化鍵を持っていれば同じ 1 往復で渡せる任意項目で、無くても pin 自体は
    成立する（後から `set_peer_enc_key` / `POST /federation/peers/enc-key` で追加できる）。
    """
    now = _now_iso()
    conn = db.get_connection(db_path)
    try:
        peer_id = _insert_peer(
            conn,
            handle=handle,
            fingerprint=fingerprint,
            key_jwk=key_jwk,
            locator=locator,
            now=now,
            enc_key_jwk=enc_key_jwk,
        )
        conn.commit()
        return peer_id
    except PeerAlreadyRegisteredError:
        conn.rollback()
        raise
    finally:
        conn.close()


def set_peer_enc_key(db_path: str, *, fingerprint: str, enc_key_jwk: dict[str, Any]) -> bool:
    """既存 pin 済み peer の envelope 暗号化用公開鍵を追加/更新する。

    招待・redeem フローをやり直さず、既に確立した peer 関係へ鍵だけ追加する経路
    （`POST /federation/peers/enc-key` / `python -m relay.invite peer enc-key` から呼ぶ）。

    Returns:
        対象 fingerprint の peer が存在し更新できたかどうか。
    """
    conn = db.get_connection(db_path)
    try:
        cur = conn.execute(
            "UPDATE peers SET enc_key_jwk = ? WHERE fingerprint = ?",
            (json.dumps(enc_key_jwk), fingerprint),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


def get_peer_by_fingerprint(db_path: str, fingerprint: str) -> dict[str, Any] | None:
    conn = db.get_connection(db_path)
    try:
        row = conn.execute(
            "SELECT * FROM peers WHERE fingerprint = ?", (fingerprint,)
        ).fetchone()
    finally:
        conn.close()
    return _peer_row_to_dict(row) if row is not None else None


def get_peer_by_handle(db_path: str, handle: str) -> dict[str, Any] | None:
    conn = db.get_connection(db_path)
    try:
        row = conn.execute("SELECT * FROM peers WHERE handle = ?", (handle,)).fetchone()
    finally:
        conn.close()
    return _peer_row_to_dict(row) if row is not None else None


def list_peers(db_path: str) -> list[dict[str, Any]]:
    conn = db.get_connection(db_path)
    try:
        rows = conn.execute("SELECT * FROM peers ORDER BY id").fetchall()
    finally:
        conn.close()
    return [_peer_row_to_dict(row) for row in rows]


def revoke_peer(db_path: str, *, handle: str, now: str | None = None) -> int:
    """該当 peer の `revoked_at` をセットする（unpin）。

    verifier は毎リクエスト `peers` table を直接引く前提（キャッシュしない）のため、
    この呼び出し直後から当該 peer の federation リクエストは 401 になる。

    Returns:
        更新行数（0 または 1）。
    """
    now = now if now is not None else _now_iso()
    conn = db.get_connection(db_path)
    try:
        cur = conn.execute(
            "UPDATE peers SET revoked_at = ? WHERE handle = ? AND revoked_at IS NULL",
            (now, handle),
        )
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# peer_invitations（招待 token、一回性）
# ---------------------------------------------------------------------------


def issue_peer_invite(db_path: str, *, handle: str, invite_ttl_seconds: int) -> str:
    """peer 招待 token を発行して `peer_invitations` に1行 INSERT し、token を返す。"""
    validate_peer_handle(handle)
    now = _now_iso()
    token = PEER_INVITE_TOKEN_PREFIX + secrets.token_urlsafe(_PEER_INVITE_TOKEN_BYTES)
    expires_at = _add_seconds_iso(now, invite_ttl_seconds)
    conn = db.get_connection(db_path)
    try:
        conn.execute(
            "INSERT INTO peer_invitations (token, handle, created_at, expires_at)"
            " VALUES (?, ?, ?, ?)",
            (token, handle, now, expires_at),
        )
        conn.commit()
    finally:
        conn.close()
    return token


def redeem_peer_invite(
    conn: sqlite3.Connection, invite_token: str, now: str
) -> tuple[int, str] | None:
    """peer 招待 token を atomic に消費する（`credentials.redeem_invite` と同一パターン）。

    未知 / 失効 / 既 redeem の token はすべて rowcount != 1 として区別せず `None` を返す
    （呼び出し側で 404 に変換、存在秘匿）。

    Returns:
        成功時 `(invitation_id, handle)`。失敗時 `None`。
    """
    cur = conn.execute(
        "UPDATE peer_invitations SET redeemed_at = ?"
        " WHERE token = ? AND redeemed_at IS NULL AND expires_at > ?",
        (now, invite_token, now),
    )
    if cur.rowcount != 1:
        conn.rollback()
        return None
    row = conn.execute(
        "SELECT id, handle FROM peer_invitations WHERE token = ?", (invite_token,)
    ).fetchone()
    conn.commit()
    return row["id"], row["handle"]


def was_peer_invite_already_redeemed(conn: sqlite3.Connection, invite_token: str) -> bool:
    """`invite_token` が存在し、既に redeem 済みかどうかを返す（漏洩検知ログの区別用）。"""
    row = conn.execute(
        "SELECT redeemed_at FROM peer_invitations WHERE token = ?", (invite_token,)
    ).fetchone()
    return row is not None and row["redeemed_at"] is not None


def was_peer_invite_already_redeemed_db(db_path: str, invite_token: str) -> bool:
    """`was_peer_invite_already_redeemed` の db_path 版。"""
    conn = db.get_connection(db_path)
    try:
        return was_peer_invite_already_redeemed(conn, invite_token)
    finally:
        conn.close()


def consume_peer_invite(db_path: str, invite_token: str, now: str) -> tuple[int, str] | None:
    """peer 招待 token を単独トランザクションで消費する（`redeem_peer_invite` の db_path 版）。

    endpoint 側の呼び出し順は「token 消費 → 署名検証 → a_fp 照合 → peer pin」。
    token 消費はここで確定し、後続の署名検証に失敗しても巻き戻さない（招待 URL の
    総当たり・誤所持による再試行を token 一回性で必ず消尽させ、漏洩検知シグナル
    （再 redeem の warning ログ）と一貫させるための意図的な設計）。

    Returns:
        成功時 `(invitation_id, handle)`。未知 / 失効 / 既 redeem は区別せず `None`。
    """
    conn = db.get_connection(db_path)
    try:
        return redeem_peer_invite(conn, invite_token, now)
    finally:
        conn.close()


def mark_peer_invite_redeemed(db_path: str, *, invitation_id: int, peer_id: int) -> None:
    """redeem 成功後、`peer_invitations.redeemed_peer_id` を反映する（追跡用、独立トランザクション）。"""
    conn = db.get_connection(db_path)
    try:
        conn.execute(
            "UPDATE peer_invitations SET redeemed_peer_id = ? WHERE id = ?",
            (peer_id, invitation_id),
        )
        conn.commit()
    finally:
        conn.close()
