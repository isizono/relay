"""stream（場）関連 endpoint（relay-v2-wire-api.md §3）。

`POST /streams` / `GET /streams`（一覧） / `GET /streams/{stream_id}` /
`DELETE /streams/{stream_id}` / `POST /streams/{stream_id}/messages` /
`PUT`・`DELETE`・`GET /streams/{stream_id}/members` / `POST /streams/{stream_id}/ack`
を実装する。

stream の状態（membership 含む）は relay-v2-wire-api.md §0 の R1 原則により in-memory
実装とする（`StreamRegistry`）。SQLite には streams / memberships table を持たない。
registry は `request.app.state.stream_registry` に app インスタンスごとに遅延生成され、
同一 app を共有する他モジュール（例: delivery.py の `GET /events` が「識別 identity の
member 場を自動含む」判定をする際）からも `request.app.state.stream_registry` として
参照できる。

`relay.identity.require_authn` を各 handler に適用し、`request.state.identity` から
呼び出し元 identity を得る。structural authZ（write 権限 membership の照合、
identity-authz.md §2.2）もここで行う。

投函（`POST /streams/{stream_id}/messages`）は「outbox 永続化完了 = 202」（wire-api.md
§3.2, §6.1）の条件を満たすため、read 権限を持つ member 宛の outbox エントリ作成を
`relay.db` 経由で直接 SQLite に書く（transactional outbox）。publish_id の採番は
`publish_log` への INSERT 1 件で行う。

`idempotency_key` の 15 分 dedup（wire-api.md §6.3）は `relay.idempotency` の共通
ヘルパーを使う（subscription レーンの `POST /publish` と同じ dedup store を app 単位で
共有する）。`ttl`（メッセージ単位の retain 上書き）・`default_ttl`（stream 単位の retain
default）は `migrations/0002-outbox-expires-at.sql` で追加した `outbox.expires_at` 列に
enqueue 時点で計算した期限を書き込み、DLQ sweep（`relay.delivery`）がこれを見て retain
超過を検出する。
"""
from __future__ import annotations

import json
import sqlite3
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Literal

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from relay import db, federation_peers, idempotency, observability
from relay.config import (
    DEFAULT_MAX_STREAMS_PER_IDENTITY,
    DEFAULT_MAX_STREAMS_TOTAL,
    DEFAULT_RETAIN_SECONDS,
    MAX_RETAIN_SECONDS,
    MIN_RETAIN_SECONDS,
    Settings,
)
from relay.errors import (
    INVALID_REQUEST,
    MEMBERSHIP_REQUIRED,
    PAYLOAD_TOO_LARGE,
    RATE_LIMIT_EXCEEDED,
    STREAM_ALREADY_EXISTS,
    STREAM_GONE,
    STREAM_NOT_FOUND,
    ResourceLimitExceeded,
    error_response,
    resource_limit_response,
)
from relay.identity import Identity, require_authn
from relay.ratelimit import get_publish_rate_limiter

Access = Literal["read", "write", "read_write"]
_VALID_ACCESS: frozenset[str] = frozenset({"read", "write", "read_write"})

# canonical stream_id は "{creator_identity}{SEP}{name}" 形式で、creator identity を構造的に
# 前置する。これにより stream_id 名前空間が identity 単位で分割され、ある identity が別 identity
# の名前空間で stream を作成することが構造的に不可能になる（同名の name を別 identity が使っても
# canonical が別物になり衝突しない）。
#
# 区切り文字 ":" の選定理由:
# - URL パスの単一セグメント（`[^/]+`）に収まるため、"/" と違い `/streams/{id}/members` 等の
#   サブリソース経路とルーティング上衝突しない。
# - name から ":" と "/" を除外する（`_validate_stream_name`）ことで canonical 文字列の区切り
#   構造が一意に保たれ、delivery target key `stream:{stream_id}:{member_identity}` の
#   （":" 区切りに依存する）injectivity も維持される。
# creator identity 自体が ":" / "/" を含まないことは authN 側（管理者管理の識別子）の前提。
STREAM_ID_SEPARATOR = ":"
_FORBIDDEN_NAME_CHARS = (STREAM_ID_SEPARATOR, "/")


def canonical_stream_id(creator_identity: str, name: str) -> str:
    """creator identity でスコープ化した canonical stream_id を構築する。"""
    return f"{creator_identity}{STREAM_ID_SEPARATOR}{name}"


def _validate_stream_name(
    value: object, settings: Settings
) -> tuple[str | None, Response | None]:
    """`POST /streams` の `name`（作成者名前空間内の stream 名）を検証する。

    Returns:
        (検証済み name または None, エラー Response または None) のタプル。
    """
    if not isinstance(value, str) or not value:
        return None, error_response(400, INVALID_REQUEST, "name は必須の非空文字列です")
    if any(ch in value for ch in _FORBIDDEN_NAME_CHARS):
        return None, error_response(
            400,
            INVALID_REQUEST,
            "name に ':' および '/' は使用できません",
        )
    if len(value) > settings.max_stream_name_length:
        return None, error_response(
            400,
            INVALID_REQUEST,
            f"name は最大 {settings.max_stream_name_length} 文字までです",
        )
    return value, None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# in-memory stream / membership registry
# ---------------------------------------------------------------------------


@dataclass
class StreamRecord:
    stream_id: str
    created_at: str
    default_ttl: int | None
    creator_identity: str
    state: Literal["open", "closed"] = "open"
    # open→closed へ遷移した時刻（ISO8601 UTC、秒精度）。open のうちは None。
    # idle-GC の猶予計算に使う。冪等な再 close では更新しない（最初の close 時刻を保つ）。
    closed_at: str | None = None
    members: dict[str, Access] = field(default_factory=dict)
    # federation replica stream（`relay.federation_inbound` が自動生成する）でのみ設定する。
    # origin_peer は生成元 peer の handle、origin_stream_id は生成元 peer 上での
    # owner-relative stream_id（egress が返信の送り先 path を組み立てる際に使う）。
    # 通常の（local が owner の）stream では両方 None。
    origin_peer: str | None = None
    origin_stream_id: str | None = None


class StreamRegistry:
    """stream 本体と membership を保持する in-memory registry。

    `threading.Lock` で単純に排他する。relay-v2-wire-api.md §0 の R1 原則により disk
    永続化しない（relay 再起動で消える。docs/ARCHITECTURE.md 参照）。

    資源上限（`max_total` / `max_per_identity`）は DoS 防御。`create` 時に registry 全体
    の登録数と作成者 identity の登録数を検査し、超過すると `ResourceLimitExceeded` を送出
    する（判定と挿入は同一 lock 下で atomic に行い、並行 create による上限すり抜けを防ぐ）。
    """

    def __init__(
        self,
        max_total: int = DEFAULT_MAX_STREAMS_TOTAL,
        max_per_identity: int = DEFAULT_MAX_STREAMS_PER_IDENTITY,
    ) -> None:
        self._lock = threading.Lock()
        self._streams: dict[str, StreamRecord] = {}
        self._max_total = max_total
        self._max_per_identity = max_per_identity
        # creator_identity ごとの現存 stream 数。上限判定を O(1) にするため create/evict で
        # 増減させる。0 になった identity は key を落とす（identity 空間での無制限成長を防ぐ）。
        # 不変条件: sum(_per_creator_count.values()) == len(_streams)。
        self._per_creator_count: dict[str, int] = {}

    def create(
        self,
        stream_id: str,
        creator_identity: str,
        default_ttl: int | None,
        *,
        origin_peer: str | None = None,
        origin_stream_id: str | None = None,
    ) -> StreamRecord | None:
        """新規 stream を作成する。既存なら None を返す（呼び出し側で 409 にする）。

        作成者は bootstrap として write 権限を持つ member として自動登録される
        （wire-api.md §3.1）。

        `origin_peer` / `origin_stream_id` は `relay.federation_inbound` が replica
        stream を自動生成する際にのみ渡す（生成元 peer の handle と、生成元での
        owner-relative stream_id）。省略時（local が owner の通常 stream）は両方 None。

        Raises:
            ResourceLimitExceeded: registry 総数または作成者 identity の登録数が上限に
                達している場合（既存 stream_id の再作成は新規スロットを消費しないため
                この検査より前に None を返す）。
        """
        with self._lock:
            if stream_id in self._streams:
                return None
            if len(self._streams) >= self._max_total:
                raise ResourceLimitExceeded("total")
            if self._per_creator_count.get(creator_identity, 0) >= self._max_per_identity:
                raise ResourceLimitExceeded("per_identity")
            record = StreamRecord(
                stream_id=stream_id,
                created_at=_now_iso(),
                default_ttl=default_ttl,
                creator_identity=creator_identity,
                origin_peer=origin_peer,
                origin_stream_id=origin_stream_id,
            )
            record.members[creator_identity] = "write"
            self._streams[stream_id] = record
            self._per_creator_count[creator_identity] = (
                self._per_creator_count.get(creator_identity, 0) + 1
            )
            return record

    def _decr_creator(self, creator_identity: str) -> None:
        """`creator_identity` の現存 stream 数を 1 減らす（lock 保持下で呼ぶこと）。"""
        remaining = self._per_creator_count.get(creator_identity, 0) - 1
        if remaining <= 0:
            self._per_creator_count.pop(creator_identity, None)
        else:
            self._per_creator_count[creator_identity] = remaining

    def get(self, stream_id: str) -> StreamRecord | None:
        with self._lock:
            return self._streams.get(stream_id)

    def close(self, stream_id: str) -> None:
        """新規投函を止める（close）。存在しない stream_id は no-op（呼び出し側で 404 判定済み前提）。

        close は冪等: 既に closed な stream への再 close は状態を変えず成功扱いにする。
        `closed_at` は open→closed への遷移時のみ記録し、再 close では上書きしない
        （idle-GC の猶予起点を最初の close 時刻に固定する）。
        """
        with self._lock:
            record = self._streams.get(stream_id)
            if record is not None and record.state != "closed":
                record.state = "closed"
                record.closed_at = _now_iso()

    def idle_closed_ids(self, older_than_seconds: float) -> list[str]:
        """close から `older_than_seconds` 秒以上経過した close 済み stream_id の一覧。

        idle-GC の除去候補選定。ここでは「close 済み かつ 猶予経過」だけを判定し、実際の
        除去は未配達 outbox エントリが drain し切ったことを確認してから `evict` で行う
        （`relay.delivery._sweep_idle_streams`）。`closed_at` は秒精度の固定幅 ISO8601 UTC
        文字列なので辞書順比較が時刻順比較に一致する。
        """
        cutoff = (_now() - timedelta(seconds=older_than_seconds)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        with self._lock:
            return [
                stream_id
                for stream_id, record in self._streams.items()
                if record.state == "closed"
                and record.closed_at is not None
                and record.closed_at <= cutoff
            ]

    def evict(self, stream_id: str) -> bool:
        """close 済み stream を registry から除去する。除去できたら True。

        open な stream（配達継続中で live）や不在 stream は除去せず False を返す。
        `idle_closed_ids` で候補選定 → outbox drain 確認 → 本メソッドで除去、の順で使う。
        """
        with self._lock:
            record = self._streams.get(stream_id)
            if record is None or record.state != "closed":
                return False
            del self._streams[stream_id]
            self._decr_creator(record.creator_identity)
            return True

    def put_member(self, stream_id: str, identity: str, access: Access) -> None:
        with self._lock:
            record = self._streams.get(stream_id)
            if record is not None:
                record.members[identity] = access

    def put_member_checked(self, stream_id: str, identity: str, access: Access) -> str:
        """membership 変更を「write 権限を持つ member が 0 人になる操作」を拒否しつつ適用する。

        write 権限（`write` / `read_write`）を持つ member が 1 人も居ない stream は、以後
        write を要求する全操作（投函 / close / membership 変更）を実行できる identity が
        存在しなくなり恒久的に操作不能になる（membership は in-memory なので relay 再起動で
        しか解消しない）。この lockout を防ぐため、適用後に write member が 0 人になる変更を
        拒否する。判定と適用は同一 lock 下で atomic に行う。

        Returns:
            "ok"                -- 適用済み
            "not_found"         -- stream 不在
            "last_write_member" -- この変更で write member が 0 人になるため未適用
        """
        with self._lock:
            record = self._streams.get(stream_id)
            if record is None:
                return "not_found"
            prospective = dict(record.members)
            prospective[identity] = access
            if not any(a in ("write", "read_write") for a in prospective.values()):
                return "last_write_member"
            record.members[identity] = access
            return "ok"

    def delete_member(self, stream_id: str, identity: str) -> None:
        with self._lock:
            record = self._streams.get(stream_id)
            if record is not None:
                record.members.pop(identity, None)

    def list_members(self, stream_id: str) -> list[dict[str, str]] | None:
        with self._lock:
            record = self._streams.get(stream_id)
            if record is None:
                return None
            return [{"identity": i, "access": a} for i, a in record.members.items()]

    def has_write_access(self, stream_id: str, identity: str) -> bool:
        with self._lock:
            record = self._streams.get(stream_id)
            if record is None:
                return False
            return record.members.get(identity) in ("write", "read_write")

    def has_read_access(self, stream_id: str, identity: str) -> bool:
        with self._lock:
            record = self._streams.get(stream_id)
            if record is None:
                return False
            return record.members.get(identity) in ("read", "read_write")

    def has_peer_write_member(self, stream_id: str, handle: str) -> bool:
        """`stream_id` に `@{handle}` suffix を持つ write 権限 member が存在するかを返す。

        federation 受信（`relay.federation_inbound`）が「owner stream への返信」を受け入れる際、
        送信元 peer の namespace（`*@{handle}`、sub 部分は問わない）が write 権限を持つ member
        として当該 stream に居ることを確認するのに使う（居なければ 404）。
        """
        suffix = f"@{handle}"
        with self._lock:
            record = self._streams.get(stream_id)
            if record is None:
                return False
            return any(
                identity.endswith(suffix) and access in ("write", "read_write")
                for identity, access in record.members.items()
            )

    def is_member(self, stream_id: str, identity: str) -> bool:
        """`identity` が当該 stream の member かを access 種別を問わず返す。

        参照系（メタ取得 / member 一覧）の structural authZ に使う。write 単独権限の member
        （作成者 bootstrap は `access: "write"` で登録される）も member として扱い、自身が
        属する stream を参照できる。不在 stream は非メンバーと同じく False。
        """
        with self._lock:
            record = self._streams.get(stream_id)
            if record is None:
                return False
            return identity in record.members

    def read_members(self, stream_id: str) -> list[str]:
        with self._lock:
            record = self._streams.get(stream_id)
            if record is None:
                return []
            return [i for i, a in record.members.items() if a in ("read", "read_write")]

    def count(self) -> int:
        """現在の stream 数（`GET /status` の `streams_count` 用）。"""
        with self._lock:
            return len(self._streams)

    def read_streams_for_identity(self, identity: str) -> list[str]:
        """`identity` が read 権限を持つ stream_id の一覧を返す。

        `relay.delivery` の dispatcher が「接続した identity が read 権限を持つ member
        である stream のメッセージも同じ SSE 接続に流す」（wire-api.md §5.5）を実装する
        際に使う。
        """
        with self._lock:
            return [
                stream_id
                for stream_id, record in self._streams.items()
                if record.members.get(identity) in ("read", "read_write")
            ]

    def list_readable_meta(self, identity: str) -> list[dict[str, str]]:
        """`identity` が read 権限を持つ stream の一覧を、`GET /streams/{stream_id}`
        （`get_stream`）と同じメタ形状（stream_id/state/created_at）で返す（wire-api.md §3.5）。

        write 単独権限の member（作成者 bootstrap の既定）は含めない。`GET /streams/{id}`
        の単体参照は `is_member`（access 種別を問わない）を基準にするのに対し、一覧は
        受信可能な場だけを見せる意図で read 権限（`read` / `read_write`）を基準にする —
        両者は意図的に異なる基準であり、write 単独の作成者は自分の stream を一覧に見るには
        明示的に read（または read_write）を自身に付与する必要がある。
        """
        with self._lock:
            return [
                {
                    "stream_id": record.stream_id,
                    "state": record.state,
                    "created_at": record.created_at,
                }
                for record in self._streams.values()
                if record.members.get(identity) in ("read", "read_write")
            ]


def get_registry_from_state(app_state) -> StreamRegistry:
    """`request.app.state.stream_registry` を遅延初期化して返す。

    app インスタンス（テストでは `create_app(settings)` 呼び出しごと）にスコープされ、
    テスト間の状態リークを防ぐ。`request` を持たない呼び出し元（dispatcher 等）からも
    `app.state` を直接渡して呼べる。
    """
    registry = getattr(app_state, "stream_registry", None)
    if registry is None:
        settings: Settings | None = getattr(app_state, "settings", None)
        if settings is not None:
            registry = StreamRegistry(
                max_total=settings.max_streams_total,
                max_per_identity=settings.max_streams_per_identity,
            )
        else:
            registry = StreamRegistry()
        app_state.stream_registry = registry
    return registry


def get_registry(request: Request) -> StreamRegistry:
    """`request` 経由で呼ぶ場合の `get_registry_from_state` の薄いラッパー。"""
    return get_registry_from_state(request.app.state)


def _get_registry(request: Request) -> StreamRegistry:
    return get_registry_from_state(request.app.state)


def _get_connection(request: Request) -> sqlite3.Connection:
    settings: Settings = request.app.state.settings
    return db.get_connection(settings.db_path)


# ---------------------------------------------------------------------------
# バリデーションヘルパ
# ---------------------------------------------------------------------------


def _validate_retain_seconds(value: object, *, field_name: str) -> tuple[int | None, Response | None]:
    """`default_ttl` / `ttl` の値検証（min 60 / max 86400、wire-api.md §6.4）。

    Returns:
        (検証済みの値 または None, エラー Response または None) のタプル。
    """
    if value is None:
        return None, None
    if isinstance(value, bool) or not isinstance(value, int):
        return None, error_response(
            400, INVALID_REQUEST, f"{field_name} は整数（秒）で指定してください"
        )
    if not (MIN_RETAIN_SECONDS <= value <= MAX_RETAIN_SECONDS):
        return None, error_response(
            400,
            INVALID_REQUEST,
            f"{field_name} は {MIN_RETAIN_SECONDS}〜{MAX_RETAIN_SECONDS} 秒の範囲で指定してください",
        )
    return value, None


async def _read_capped_body(request: Request) -> tuple[bytes, Response | None]:
    """`Settings.max_payload_bytes` を超える request body を 413 で拒否する。

    `Content-Length` ヘッダで早期に拒否できる場合は body を読まずに拒否する。ヘッダが
    無い/信頼できない（chunked transfer 等）場合に備え、`request.stream()` を読み進める
    間も上限超過を検知し、上限に達した時点で残りを読み切る前に打ち切る（全体をメモリに
    読み切ってからサイズ判定すると、判定自体が DoS の踏み台になる。セキュリティ監査
    finding H-4/F2 の「`await request.json()` が全体メモリ読込」という指摘への対応）。
    """
    settings: Settings = request.app.state.settings
    max_bytes = settings.max_payload_bytes

    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > max_bytes:
                return b"", error_response(
                    413,
                    PAYLOAD_TOO_LARGE,
                    f"リクエストボディが上限（{max_bytes} bytes）を超えています",
                )
        except ValueError:
            pass  # 不正な Content-Length は実読み込み側の検証に委ねる

    chunks: list[bytes] = []
    total = 0
    async for chunk in request.stream():
        total += len(chunk)
        if total > max_bytes:
            return b"", error_response(
                413, PAYLOAD_TOO_LARGE, f"リクエストボディが上限（{max_bytes} bytes）を超えています"
            )
        chunks.append(chunk)
    return b"".join(chunks), None


async def _read_json_body(request: Request) -> tuple[dict, Response | None]:
    raw, err = await _read_capped_body(request)
    if err is not None:
        return {}, err
    try:
        body = json.loads(raw) if raw else None
    except Exception:
        return {}, error_response(400, INVALID_REQUEST, "リクエストボディが不正な JSON です")
    if not isinstance(body, dict):
        return {}, error_response(400, INVALID_REQUEST, "リクエストボディは JSON object でなければなりません")
    return body, None


# ---------------------------------------------------------------------------
# endpoint: POST /streams
# ---------------------------------------------------------------------------


@require_authn
async def create_stream(request: Request) -> Response:
    identity: Identity = request.state.identity
    body, err = await _read_json_body(request)
    if err is not None:
        return err

    name, err = _validate_stream_name(body.get("name"), request.app.state.settings)
    if err is not None:
        return err

    default_ttl, err = _validate_retain_seconds(body.get("default_ttl"), field_name="default_ttl")
    if err is not None:
        return err

    stream_id = canonical_stream_id(identity.id, name)
    registry = _get_registry(request)
    try:
        record = registry.create(stream_id, identity.id, default_ttl)
    except ResourceLimitExceeded as exc:
        return resource_limit_response("stream", exc.scope)
    if record is None:
        return error_response(
            409, STREAM_ALREADY_EXISTS, f"stream '{stream_id}' は既に存在します"
        )
    return JSONResponse(
        {"stream_id": record.stream_id, "created_at": record.created_at}, status_code=201
    )


# ---------------------------------------------------------------------------
# endpoint: GET /streams（一覧）
# ---------------------------------------------------------------------------


@require_authn
async def list_streams(request: Request) -> Response:
    """呼び出し元 identity が read 権限を持つ member である stream の一覧を返す
    （wire-api.md §3.5）。member でない stream は結果に含めない（存在を露呈しない）。
    """
    identity: Identity = request.state.identity
    registry = _get_registry(request)
    streams = registry.list_readable_meta(identity.id)
    return JSONResponse({"streams": streams})


# ---------------------------------------------------------------------------
# endpoint: GET /streams/{stream_id}
# ---------------------------------------------------------------------------


@require_authn
async def get_stream(request: Request) -> Response:
    identity: Identity = request.state.identity
    stream_id = request.path_params["stream_id"]
    registry = _get_registry(request)
    record = registry.get(stream_id)
    # 非メンバーには存在しない stream_id と同一の 404 を返し、stream の存在を悟らせない
    # （identity-authz.md §2.1, A2A 1.0 §7.5）。403 での拒否は resource 存在の露呈になる。
    if record is None or not registry.is_member(stream_id, identity.id):
        return error_response(404, STREAM_NOT_FOUND, f"stream '{stream_id}' が見つかりません")
    return JSONResponse(
        {"stream_id": record.stream_id, "state": record.state, "created_at": record.created_at}
    )


# ---------------------------------------------------------------------------
# endpoint: DELETE /streams/{stream_id}（close）
# ---------------------------------------------------------------------------


@require_authn
async def close_stream(request: Request) -> Response:
    identity: Identity = request.state.identity
    stream_id = request.path_params["stream_id"]
    registry = _get_registry(request)
    record = registry.get(stream_id)
    # 完全非メンバーには不在の stream_id と同一の 404 を返し、stream の存在を悟らせない
    # （identity-authz.md §2.2）。403 を返してよいのは、stream の存在を正当に知っている
    # 権限不足の member のみ。
    if record is None or not registry.is_member(stream_id, identity.id):
        return error_response(404, STREAM_NOT_FOUND, f"stream '{stream_id}' が見つかりません")
    if not registry.has_write_access(stream_id, identity.id):
        return error_response(
            403, MEMBERSHIP_REQUIRED, f"stream '{stream_id}' の write 権限がありません"
        )
    registry.close(stream_id)
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# endpoint: POST /streams/{stream_id}/messages
# ---------------------------------------------------------------------------


@require_authn
async def post_stream_message(request: Request) -> Response:
    identity: Identity = request.state.identity
    stream_id = request.path_params["stream_id"]
    registry = _get_registry(request)

    limiter = get_publish_rate_limiter(request.app.state)
    allowed, retry_after = limiter.allow(identity.id)
    if not allowed:
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="rate_limited"
        )
        response = error_response(
            429, RATE_LIMIT_EXCEEDED, "投函のレート制限を超過しました"
        )
        response.headers["Retry-After"] = str(retry_after)
        return response

    record = registry.get(stream_id)
    if record is None:
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="stream_not_found"
        )
        return error_response(404, STREAM_NOT_FOUND, f"stream '{stream_id}' が見つかりません")
    if not registry.is_member(stream_id, identity.id):
        # 完全非メンバーには不在の stream_id と同一の 404 を返し、stream の存在を悟らせない
        # （identity-authz.md §2.2）。metric は内部観測用のため真の理由を残す。
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="membership_required"
        )
        return error_response(404, STREAM_NOT_FOUND, f"stream '{stream_id}' が見つかりません")
    if not registry.has_write_access(stream_id, identity.id):
        # 権限不足の member は stream の存在を正当に知っているため 403 で区別してよい。
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="membership_required"
        )
        return error_response(
            403, MEMBERSHIP_REQUIRED, f"stream '{stream_id}' の write 権限がありません"
        )
    if record.state == "closed":
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="stream_gone"
        )
        return error_response(410, STREAM_GONE, f"stream '{stream_id}' は close 済みです")

    body, err = await _read_json_body(request)
    if err is not None:
        observability.inc_metric(
            request.app.state,
            "relay_publish_failed_total",
            failure_reason="payload_too_large" if err.status_code == 413 else "invalid_request",
        )
        return err

    message_body = body.get("body")
    if not isinstance(message_body, str) or message_body == "":
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="invalid_request"
        )
        return error_response(400, INVALID_REQUEST, "body は必須の非空文字列です")

    _ttl, err = _validate_retain_seconds(body.get("ttl"), field_name="ttl")
    if err is not None:
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="invalid_request"
        )
        return err

    idempotency_key = body.get("idempotency_key")
    if idempotency_key is not None and not isinstance(idempotency_key, str):
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="invalid_request"
        )
        return error_response(400, INVALID_REQUEST, "idempotency_key は文字列で指定してください")

    dedup_key = idempotency.build_key(
        lane="stream",
        publisher_identity=identity.id,
        explicit_key=idempotency_key,
        scope=stream_id,
        body=message_body,
    )
    store = idempotency.get_store(request.app.state)
    existing_publish_id = await idempotency.resolve_or_reserve(store, dedup_key)
    if existing_publish_id is not None:
        return JSONResponse(
            {"publish_id": existing_publish_id, "matched_members": 0}, status_code=202
        )

    # 予約獲得後は finalize / release のどちらかで必ず予約を解消する。CancelledError
    # でも解放が要るため BaseException で受ける。
    try:
        retain_seconds = (
            _ttl if _ttl is not None else (record.default_ttl or DEFAULT_RETAIN_SECONDS)
        )
        expires_at = (datetime.now(timezone.utc) + timedelta(seconds=retain_seconds)).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )

        read_members = registry.read_members(stream_id)
        payload = message_body.encode("utf-8")
        now = _now_iso()

        conn = _get_connection(request)
        try:
            cur = conn.execute(
                "INSERT INTO publish_log (lane, stream_id, publisher_identity, enqueued_at)"
                " VALUES ('stream', ?, ?, ?)",
                (stream_id, identity.id, now),
            )
            publish_id = cur.lastrowid
            for member_identity in read_members:
                conn.execute(
                    "INSERT INTO outbox"
                    " (target_type, stream_id, member_identity, publish_id, payload, enqueued_at,"
                    " expires_at)"
                    " VALUES ('stream', ?, ?, ?, ?, ?, ?)",
                    (stream_id, member_identity, publish_id, payload, now, expires_at),
                )
            conn.commit()
        finally:
            conn.close()
    except BaseException:
        store.release(dedup_key)
        raise
    store.finalize(dedup_key, publish_id)

    observability.record_event(
        request.app.state,
        "publish_received",
        lane="stream",
        publish_id=publish_id,
        publisher_identity=identity.id,
        stream_id=stream_id,
        matched_members=len(read_members),
    )
    observability.inc_metric(request.app.state, "relay_publish_received_total")
    return JSONResponse(
        {"publish_id": publish_id, "matched_members": len(read_members)}, status_code=202
    )


# ---------------------------------------------------------------------------
# endpoint: membership API
# ---------------------------------------------------------------------------


def _validate_peer_member_identity(
    db_path: str, registry: "StreamRegistry", stream_id: str, target_identity: str
) -> Response | None:
    """`@` を含む member identity（peer namespace）に federation 命名規約の検証を課す。

    v1 で 2 検証（federation v1 設計確定版「命名規約」節）:
    (1) suffix（最後の `@` より後）が active な（未 revoke の）peer handle であること。
    (2) 1 stream に同居できる peer namespace は 1 つまで（複数 peer 同居は構造禁止、
        group 会話は v2 送り）。

    `target_identity` に `@` が含まれない場合は検証対象外（None を返す）。既存 member 一覧は
    `registry.list_members`（lock 保護下のスナップショット）経由で読み、`StreamRecord.members`
    を lock 外から直接反復しない。
    """
    if "@" not in target_identity:
        return None
    sub_part, _, handle_part = target_identity.rpartition("@")
    if not sub_part or not handle_part:
        return error_response(
            400, INVALID_REQUEST, f"identity '{target_identity}' の '@' 形式が不正です"
        )
    peer = federation_peers.get_peer_by_handle(db_path, handle_part)
    if peer is None or peer["revoked_at"] is not None:
        return error_response(
            400,
            INVALID_REQUEST,
            f"'{handle_part}' は有効な（active な）peer handle ではありません",
        )
    for member in registry.list_members(stream_id) or []:
        member_identity = member["identity"]
        if member_identity == target_identity or "@" not in member_identity:
            continue
        _, _, existing_handle = member_identity.rpartition("@")
        if existing_handle != handle_part:
            return error_response(
                400,
                INVALID_REQUEST,
                "1 つの stream に同居できる peer namespace は 1 つまでです"
                f"（既に '{existing_handle}' の member が存在します）",
            )
    return None


@require_authn
async def put_member(request: Request) -> Response:
    identity: Identity = request.state.identity
    stream_id = request.path_params["stream_id"]
    registry = _get_registry(request)

    record = registry.get(stream_id)
    # 完全非メンバーには不在の stream_id と同一の 404 を返し、stream の存在を悟らせない
    # （identity-authz.md §2.2）。403 は存在を正当に知っている権限不足 member 専用。
    if record is None or not registry.is_member(stream_id, identity.id):
        return error_response(404, STREAM_NOT_FOUND, f"stream '{stream_id}' が見つかりません")
    if not registry.has_write_access(stream_id, identity.id):
        return error_response(
            403, MEMBERSHIP_REQUIRED, f"stream '{stream_id}' の write 権限がありません"
        )

    body, err = await _read_json_body(request)
    if err is not None:
        return err

    target_identity = body.get("identity")
    if not isinstance(target_identity, str) or not target_identity:
        return error_response(400, INVALID_REQUEST, "identity は必須の非空文字列です")
    access = body.get("access")
    if access not in _VALID_ACCESS:
        return error_response(
            400, INVALID_REQUEST, "access は read / write / read_write のいずれかです"
        )

    settings: Settings = request.app.state.settings
    err = _validate_peer_member_identity(settings.db_path, registry, stream_id, target_identity)
    if err is not None:
        return err

    result = registry.put_member_checked(stream_id, target_identity, access)
    if result == "not_found":
        return error_response(404, STREAM_NOT_FOUND, f"stream '{stream_id}' が見つかりません")
    if result == "last_write_member":
        return error_response(
            400,
            INVALID_REQUEST,
            f"stream '{stream_id}' の write 権限を持つ member が 0 人になる membership 変更は許可されません",
        )
    return JSONResponse({}, status_code=200)


@require_authn
async def delete_member(request: Request) -> Response:
    identity: Identity = request.state.identity
    stream_id = request.path_params["stream_id"]
    registry = _get_registry(request)

    record = registry.get(stream_id)
    # 完全非メンバーには不在の stream_id と同一の 404 を返し、stream の存在を悟らせない
    # （identity-authz.md §2.2）。自己離脱パスより先に判定するため、非メンバーの自己離脱
    # 試行も 404 になる（204 を返すと存在の露呈になる）。
    if record is None or not registry.is_member(stream_id, identity.id):
        return error_response(404, STREAM_NOT_FOUND, f"stream '{stream_id}' が見つかりません")

    target_identity = request.query_params.get("identity")
    if not target_identity:
        return error_response(400, INVALID_REQUEST, "identity クエリパラメータは必須です")

    # 自分自身の membership 削除（離脱）は本人であれば常に許可する（identity-authz.md §2.2）。
    if target_identity != identity.id and not registry.has_write_access(stream_id, identity.id):
        return error_response(
            403, MEMBERSHIP_REQUIRED, f"stream '{stream_id}' の write 権限がありません"
        )

    # 自己離脱（本人による membership 解除）は subscription レーンの unsubscribe と同型に扱い、
    # 未 ack outbox エントリを同一 transaction で即時削除する（DLQ を経由しない、wire-api.md
    # §5.3 / §6.6）。明示的な関心放棄は事故ではないため DLQ・warn ログを汚さない。他 member に
    # よる除去（involuntary な read 権限喪失）は outbox を残し、dispatcher の DLQ sweep
    # （`relay.delivery._sweep_stream_permanent_errors`）が dead 化して観測対象に残す。
    if target_identity == identity.id:
        conn = _get_connection(request)
        try:
            conn.execute(
                "DELETE FROM outbox"
                " WHERE target_type = 'stream' AND stream_id = ? AND member_identity = ?",
                (stream_id, identity.id),
            )
            conn.commit()
        finally:
            conn.close()

    registry.delete_member(stream_id, target_identity)
    return Response(status_code=204)


@require_authn
async def list_members(request: Request) -> Response:
    identity: Identity = request.state.identity
    stream_id = request.path_params["stream_id"]
    registry = _get_registry(request)
    # 非メンバーには存在しない stream_id と同一の 404 を返し、member 構成を露呈しない
    # （identity-authz.md §2.1, A2A 1.0 §7.5）。
    if not registry.is_member(stream_id, identity.id):
        return error_response(404, STREAM_NOT_FOUND, f"stream '{stream_id}' が見つかりません")
    members = registry.list_members(stream_id)
    if members is None:
        return error_response(404, STREAM_NOT_FOUND, f"stream '{stream_id}' が見つかりません")
    return JSONResponse({"members": members})


# ---------------------------------------------------------------------------
# endpoint: POST /streams/{stream_id}/ack
# ---------------------------------------------------------------------------


@require_authn
async def ack_stream(request: Request) -> Response:
    identity: Identity = request.state.identity
    stream_id = request.path_params["stream_id"]
    registry = _get_registry(request)

    # 場が不在、または呼び出し元が read 権限を持つ member でない場合は同一の 404
    # （wire-api.md §5.6 / §5.7 の存在露呈回避）。
    if not registry.has_read_access(stream_id, identity.id):
        return error_response(
            404, STREAM_NOT_FOUND, f"stream '{stream_id}' が見つからないか read 権限がありません"
        )

    body, err = await _read_json_body(request)
    if err is not None:
        return err

    up_to_publish_id = body.get("up_to_publish_id")
    if isinstance(up_to_publish_id, bool) or not isinstance(up_to_publish_id, int):
        return error_response(400, INVALID_REQUEST, "up_to_publish_id は整数で指定してください")

    conn = _get_connection(request)
    try:
        conn.execute(
            "DELETE FROM outbox"
            " WHERE target_type = 'stream' AND stream_id = ? AND member_identity = ?"
            " AND publish_id <= ?",
            (stream_id, identity.id, up_to_publish_id),
        )
        conn.commit()
    finally:
        conn.close()

    observability.record_event(
        request.app.state,
        "ack_received",
        lane="stream",
        stream_id=stream_id,
        member_identity=identity.id,
        up_to_publish_id=up_to_publish_id,
    )
    observability.inc_metric(request.app.state, "relay_ack_received_total")
    return JSONResponse({}, status_code=200)


routes: list[Route] = [
    Route("/streams", create_stream, methods=["POST"]),
    Route("/streams", list_streams, methods=["GET"]),
    Route("/streams/{stream_id}", get_stream, methods=["GET"]),
    Route("/streams/{stream_id}", close_stream, methods=["DELETE"]),
    Route("/streams/{stream_id}/messages", post_stream_message, methods=["POST"]),
    Route("/streams/{stream_id}/members", put_member, methods=["PUT"]),
    Route("/streams/{stream_id}/members", delete_member, methods=["DELETE"]),
    Route("/streams/{stream_id}/members", list_members, methods=["GET"]),
    Route("/streams/{stream_id}/ack", ack_stream, methods=["POST"]),
]
