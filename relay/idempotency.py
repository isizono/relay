"""publisher 再送の idempotency_key dedup（relay-v2-wire-api.md §6.3）。

stream レーン（`POST /streams/{id}/messages`）と subscription レーン
（`POST /publish`）の両方で使う共通ヘルパー。`idempotency_key` を publisher が
指定した場合は `(lane, publisher_identity, idempotency_key)` で 15 分 dedup し、
省略時は `(publisher_identity, ref/stream, labels 正規化 hash, body 正規化 hash,
受信秒精度 ts)` から relay 側で擬似キーを補完する（wire-api.md §6.3）。

in-memory store（`app.state.idempotency_store`）に保持する。relay 再起動で
消えるが、dedup window が 15 分と短いため、他の in-memory state（subscription
registry 等）と同じ liveness クラスの扱いで問題ない。

dedup 判定と publish 実行（DB insert + publish_id 採番）は同一ロック区間に
収められない（ロックが publish 全体を跨ぐと全 publish が直列化される）ため、
二相で運用する: `check_and_reserve()` が単一ロック区間で「既存 entry の返却」と
「予約の獲得」を atomic に行い、予約の勝者だけが publish を実行して
`finalize()`（成功）/ `release()`（失敗）で予約を解消する。予約中に同一キーで
到着したリクエストは `PendingReservation.wait()` で勝者の結果を待つ。判定と
予約が atomic なので、判定後に制御を手放しても同一キーの publish は二重実行
されない。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

DEDUP_WINDOW_SECONDS = 900  # 15 分

# 勝者の publish はローカル SQLite への同期書き込み（busy timeout 5 秒）で完了する。
# 待機がこの上限へ達するのは勝者側の異常時のみ。
PENDING_WAIT_TIMEOUT_SECONDS = 10.0
_MAX_RESERVE_ATTEMPTS = 3


class PendingReservation:
    """予約中キーの待ち合わせ点。勝者の `finalize()` / `release()` で解決される。"""

    __slots__ = ("_event", "_publish_id")

    def __init__(self) -> None:
        self._event = threading.Event()
        self._publish_id: int | None = None

    def _resolve(self, publish_id: int | None) -> None:
        # `_publish_id` の代入を `set()` より先に行うこと。`wait()` 側は
        # `Event.wait()` の happens-before に依存して `_publish_id` を読む。
        self._publish_id = publish_id
        self._event.set()

    async def wait(self, timeout: float) -> int | None:
        """勝者の結果を待つ。

        publish 成功なら勝者の `publish_id`、失敗（`release()`）または timeout なら
        None を返す。`threading.Event` の待機を threadpool に逃がすため、event loop
        はブロックしない。
        """
        await asyncio.to_thread(self._event.wait, timeout)
        return self._publish_id


@dataclass(frozen=True)
class ReserveOutcome:
    """`IdempotencyStore.check_and_reserve()` の結果。3 状態のうち必ず 1 つになる。

    - `publish_id` が非 None: dedup ヒット。既存 publish の id をそのまま応答してよい。
    - `reserved` が True: 呼び出し側が予約の勝者。publish 完了後に `finalize()`、
      失敗時は `release()` を必ず呼ぶこと（怠ると同一キーの後続リクエストが
      勝者の結果を待ち続ける）。
    - `pending` が非 None: 別リクエストが同一キーで publish 実行中。
      `pending.wait()` で勝者の結果を待てる。
    """

    publish_id: int | None = None
    reserved: bool = False
    pending: PendingReservation | None = None


class IdempotencyStore:
    """`key -> (publish_id, expires_at)` と予約中キーを保持する in-memory dedup store。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, tuple[int, datetime]] = {}
        self._pending: dict[str, PendingReservation] = {}

    def _purge_expired_locked(self, now: datetime) -> None:
        expired = [k for k, (_, expires_at) in self._entries.items() if expires_at <= now]
        for k in expired:
            del self._entries[k]

    def check_and_reserve(self, key: str) -> ReserveOutcome:
        """既存 entry の返却と予約の獲得を単一ロック区間で atomic に行う。

        判定と予約を別ロック区間に分けると、判定後・予約前に同一キーの並行
        リクエストがすり抜けて publish が二重実行されるため、分割してはならない。
        """
        now = datetime.now(timezone.utc)
        with self._lock:
            self._purge_expired_locked(now)
            entry = self._entries.get(key)
            if entry is not None:
                return ReserveOutcome(publish_id=entry[0])
            pending = self._pending.get(key)
            if pending is not None:
                return ReserveOutcome(pending=pending)
            self._pending[key] = PendingReservation()
            return ReserveOutcome(reserved=True)

    def finalize(self, key: str, publish_id: int) -> None:
        """予約を確定し、`DEDUP_WINDOW_SECONDS` の間 dedup entry として公開する。

        同一キーで待機中のリクエストには `publish_id` を通知する。
        """
        now = datetime.now(timezone.utc)
        with self._lock:
            self._entries[key] = (publish_id, now + timedelta(seconds=DEDUP_WINDOW_SECONDS))
            pending = self._pending.pop(key, None)
        if pending is not None:
            pending._resolve(publish_id)

    def release(self, key: str) -> None:
        """publish に失敗した予約を破棄する。同一キーは再び予約可能になる。

        同一キーで待機中のリクエストには失敗（None）を通知し、予約の取り直しを促す。
        """
        with self._lock:
            pending = self._pending.pop(key, None)
        if pending is not None:
            pending._resolve(None)


async def resolve_or_reserve(store: IdempotencyStore, key: str) -> int | None:
    """dedup 判定を行い、既存 `publish_id`（dedup ヒット）か None（予約獲得）を返す。

    None を返した場合、呼び出し側が予約の勝者であり、publish 完了後に
    `store.finalize(key, publish_id)`、失敗時に `store.release(key)` を必ず呼ぶこと。

    同一キーが予約中（別リクエストが publish 実行中）の場合は勝者の結果を待ち、
    勝者が成功すればその `publish_id` を返す。勝者が失敗した場合は予約を取り直す。
    予約が繰り返し解決しない場合（勝者が finalize も release もしないまま滞留する
    状態は publish フローの契約違反）は RuntimeError を送出する。
    """
    for _ in range(_MAX_RESERVE_ATTEMPTS):
        outcome = store.check_and_reserve(key)
        if outcome.publish_id is not None:
            return outcome.publish_id
        if outcome.reserved:
            return None
        assert outcome.pending is not None
        publish_id = await outcome.pending.wait(PENDING_WAIT_TIMEOUT_SECONDS)
        if publish_id is not None:
            return publish_id
    raise RuntimeError(
        f"idempotency 予約が {_MAX_RESERVE_ATTEMPTS} 回の試行で解決しませんでした: {key}"
    )


def get_store(app_state: Any) -> IdempotencyStore:
    """`app.state.idempotency_store` を遅延初期化して返す。"""
    store = getattr(app_state, "idempotency_store", None)
    if store is None:
        store = IdempotencyStore()
        app_state.idempotency_store = store
    return store


def build_key(
    *,
    lane: str,
    publisher_identity: str,
    explicit_key: str | None,
    scope: str,
    labels: Iterable[str] = (),
    body: str | bytes | None = None,
) -> str:
    """dedup キーを組み立てる。

    `explicit_key` 指定時は `(lane, publisher_identity, explicit_key)` で決まる
    素直なキー。省略時は wire-api.md §6.3 の擬似キー補完式
    `(publisher_identity, ref/stream, labels 正規化 hash, body 正規化 hash,
    受信秒精度 ts)` を使う。`scope` は stream レーンなら `stream_id`、
    subscription レーンなら `ref` の正規化文字列を渡す。

    `publisher_identity` は認証済み identity を渡すこと。リクエストボディ由来の
    値を渡すと、他 identity のキーを詐称して dedup を横取りできてしまう。
    """
    if explicit_key:
        return f"{lane}:{publisher_identity}:explicit:{explicit_key}"

    if isinstance(body, bytes):
        body_bytes = body
    elif isinstance(body, str):
        body_bytes = body.encode("utf-8")
    else:
        body_bytes = b""
    body_hash = hashlib.sha256(body_bytes).hexdigest()

    now_second = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    pseudo_parts = {
        "scope": scope,
        "labels": sorted(labels),
        "body_hash": body_hash,
        "ts": now_second,
    }
    digest = hashlib.sha256(
        json.dumps(pseudo_parts, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    return f"{lane}:{publisher_identity}:pseudo:{digest}"
