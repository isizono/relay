"""publisher 再送の idempotency_key dedup（relay-v2-wire-api.md §6.3）。

stream レーン（`POST /streams/{id}/messages`）と subscription レーン
（`POST /publish`）の両方で使う共通ヘルパー。`idempotency_key` を publisher が
指定した場合は `(lane, publisher_identity, idempotency_key)` で 15 分 dedup し、
省略時は `(publisher_identity, ref/stream, labels 正規化 hash, body 正規化 hash,
受信秒精度 ts)` から relay 側で擬似キーを補完する（wire-api.md §6.3）。

in-memory store（`app.state.idempotency_store`）に保持する。relay 再起動で
消えるが、dedup window が 15 分と短いため、他の in-memory state（subscription
registry 等）と同じ liveness クラスの扱いで問題ない。
"""
from __future__ import annotations

import hashlib
import json
import threading
from datetime import datetime, timedelta, timezone
from typing import Any, Iterable

DEDUP_WINDOW_SECONDS = 900  # 15 分


class IdempotencyStore:
    """`key -> (publish_id, expires_at)` を保持する in-memory dedup store。"""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: dict[str, tuple[int, datetime]] = {}

    def _purge_expired_locked(self, now: datetime) -> None:
        expired = [k for k, (_, expires_at) in self._entries.items() if expires_at <= now]
        for k in expired:
            del self._entries[k]

    def check(self, key: str) -> int | None:
        """既存 entry があれば既存の `publish_id` を返す（dedup ヒット）。"""
        now = datetime.now(timezone.utc)
        with self._lock:
            self._purge_expired_locked(now)
            entry = self._entries.get(key)
            return entry[0] if entry is not None else None

    def register(self, key: str, publish_id: int) -> None:
        now = datetime.now(timezone.utc)
        with self._lock:
            self._entries[key] = (publish_id, now + timedelta(seconds=DEDUP_WINDOW_SECONDS))


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
