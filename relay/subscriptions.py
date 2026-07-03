"""subscription 関連 endpoint（relay-v2-wire-api.md §5.1〜§5.4, §5.6, §5.7）。

`POST /subscriptions` / `PUT /subscriptions/{id}/lease` / `DELETE /subscriptions/{id}` /
`POST /subscriptions/{id}/ack` / `POST /publish` を実装する。

subscription registry（lease 含む）は relay-v2-wire-api.md §0 の R1 原則により in-memory
実装とする（`SubscriptionRegistry`、`StreamRegistry` と対称の設計）。SQLite には
subscriptions table を持たない（設計判断の詳細は docs/ARCHITECTURE.md §DB schema を参照）。
registry は `request.app.state.subscription_registry` に app インスタンスごとに遅延生成され、
`relay.delivery`（`GET /events` の ownership 検証、dispatcher の DLQ permanent error 判定）
からも同じ属性名で参照できる。

subscription を名指しする操作は ownership 検証（呼び出し元 identity == subscriber 本人）を
structural authZ として行う（identity-authz.md §2.2, wire-api.md §5.7）。非所有・不明な
subscription_id は 404（存在露呈回避）、所有者本人の lease 切れ済みは 410。

`relay.identity.require_authn` を各 handler に適用し、`request.state.identity` から
呼び出し元 identity を得る。labels の subset マッチングは Python in-memory で行う。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from relay import db, idempotency, observability
from relay.config import (
    DEFAULT_LEASE_TTL_SECONDS,
    DEFAULT_RETAIN_SECONDS,
    MAX_LEASE_TTL_SECONDS,
    MAX_RETAIN_SECONDS,
    MIN_LEASE_TTL_SECONDS,
    MIN_RETAIN_SECONDS,
    Settings,
)
from relay.errors import (
    INVALID_REQUEST,
    LABEL_VALIDATION,
    RATE_LIMIT_EXCEEDED,
    SUBSCRIBER_MISMATCH,
    SUBSCRIPTION_GONE,
    SUBSCRIPTION_NOT_FOUND,
    error_response,
)
from relay.identity import Identity, require_authn


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------------------
# in-memory subscription registry
# ---------------------------------------------------------------------------


@dataclass
class SubscriptionRecord:
    subscription_id: str
    subscriber: str
    labels: frozenset[str]
    lease_ttl: int
    lease_expires_at: datetime
    retain_seconds: int


class SubscriptionRegistry:
    """subscription 本体と lease を保持する in-memory registry。

    `StreamRegistry`（`relay/streams.py`）と対称の設計。relay-v2-wire-api.md §0 の
    R1 原則により disk 永続化しない（relay 再起動で消える。subscriber は re-subscribe
    で自己修復する）。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._subs: dict[str, SubscriptionRecord] = {}

    def create(
        self,
        subscriber: str,
        labels: frozenset[str],
        lease_ttl: int,
        retain_seconds: int,
    ) -> SubscriptionRecord:
        with self._lock:
            subscription_id = str(uuid.uuid4())
            record = SubscriptionRecord(
                subscription_id=subscription_id,
                subscriber=subscriber,
                labels=labels,
                lease_ttl=lease_ttl,
                lease_expires_at=_now() + timedelta(seconds=lease_ttl),
                retain_seconds=retain_seconds,
            )
            self._subs[subscription_id] = record
            return record

    def get(self, subscription_id: str) -> SubscriptionRecord | None:
        with self._lock:
            return self._subs.get(subscription_id)

    def is_owner(self, subscription_id: str, identity: str) -> bool:
        record = self.get(subscription_id)
        return record is not None and record.subscriber == identity

    def is_lease_expired(self, subscription_id: str) -> bool:
        """subscription が registry に存在し、かつ lease が切れているかを返す。

        不存在の場合は False を返す（「不存在」と「lease 切れ」は呼び出し側で
        `get()` / `is_owner()` と組み合わせて区別すること。wire-api.md §5.7）。
        """
        record = self.get(subscription_id)
        if record is None:
            return False
        return record.lease_expires_at <= _now()

    def renew(self, subscription_id: str, lease_ttl: int | None) -> SubscriptionRecord | None:
        with self._lock:
            record = self._subs.get(subscription_id)
            if record is None:
                return None
            ttl = lease_ttl if lease_ttl is not None else record.lease_ttl
            record.lease_ttl = ttl
            record.lease_expires_at = _now() + timedelta(seconds=ttl)
            return record

    def delete(self, subscription_id: str) -> None:
        with self._lock:
            self._subs.pop(subscription_id, None)

    def evict_expired(self, older_than_seconds: float) -> list[str]:
        """lease が `older_than_seconds` 秒より前に切れた subscription を registry から除去する。

        unsubscribe されないまま放置された subscription（クラッシュした subscriber 等）が
        registry に無期限に残り続けるのを防ぐ（`_subs` の無制限成長を回避）。除去対象は
        「所有者本人への 410 ヒント」（wire-api.md §5.7）を提供する目的の猶予期間を過ぎた
        ものに限る。除去されると以後は非所有者と同じ `404 Not Found` になるが、subscriber は
        `404` / `410` のどちらも re-subscribe のシグナルとして同一に扱うため機能上の影響はない
        （§5.7）。除去した `subscription_id` の一覧を返す（呼び出し側のログ用）。
        """
        cutoff = _now() - timedelta(seconds=older_than_seconds)
        with self._lock:
            expired_ids = [
                subscription_id
                for subscription_id, record in self._subs.items()
                if record.lease_expires_at <= cutoff
            ]
            for subscription_id in expired_ids:
                del self._subs[subscription_id]
            return expired_ids

    def count(self) -> int:
        """現在の subscription 数（`GET /status` の `subscriptions_count` 用）。"""
        with self._lock:
            return len(self._subs)

    def matching(self, publish_labels: frozenset[str]) -> list[SubscriptionRecord]:
        """`publish_labels` の superset となる labels を持つ、lease 生存中の subscription 一覧。

        subset マッチング（subscribe.labels が publish.labels の subset なら match、
        wire-api.md §5.2）。lease 切れの subscription は fan-out 対象から除外する。
        """
        now = _now()
        with self._lock:
            return [
                record
                for record in self._subs.values()
                if record.labels.issubset(publish_labels) and record.lease_expires_at > now
            ]


def get_registry_from_state(app_state) -> SubscriptionRegistry:
    """`request.app.state.subscription_registry` を遅延初期化して返す。

    `request` を持たない呼び出し元（dispatcher 等）からも `app.state` を直接渡して呼べる。
    """
    registry = getattr(app_state, "subscription_registry", None)
    if registry is None:
        registry = SubscriptionRegistry()
        app_state.subscription_registry = registry
    return registry


def get_registry(request: Request) -> SubscriptionRegistry:
    """`request` 経由で呼ぶ場合の `get_registry_from_state` の薄いラッパー。"""
    return get_registry_from_state(request.app.state)


def _get_connection(request: Request) -> sqlite3.Connection:
    settings: Settings = request.app.state.settings
    return db.get_connection(settings.db_path)


# ---------------------------------------------------------------------------
# publish レート制限（relay-v2-wire-api.md §5.4、publisher ごと token bucket）
# ---------------------------------------------------------------------------


class RateLimiter:
    """publisher identity ごとの token bucket rate limiter。"""

    def __init__(self, rate_per_second: int) -> None:
        self._rate = max(1, rate_per_second)
        self._lock = threading.Lock()
        self._buckets: dict[str, tuple[float, float]] = {}

    def allow(self, identity: str) -> tuple[bool, int]:
        """許可なら `(True, 0)`、拒否なら `(False, retry_after_seconds)`。"""
        now = time.monotonic()
        with self._lock:
            tokens, last = self._buckets.get(identity, (float(self._rate), now))
            tokens = min(float(self._rate), tokens + (now - last) * self._rate)
            if tokens >= 1.0:
                self._buckets[identity] = (tokens - 1.0, now)
                return True, 0
            self._buckets[identity] = (tokens, now)
            retry_after = max(1, int((1.0 - tokens) / self._rate) + 1)
            return False, retry_after


def _get_rate_limiter(request: Request) -> RateLimiter:
    limiter = getattr(request.app.state, "publish_rate_limiter", None)
    if limiter is None:
        settings: Settings = request.app.state.settings
        limiter = RateLimiter(settings.publish_rate_limit_per_second)
        request.app.state.publish_rate_limiter = limiter
    return limiter


# ---------------------------------------------------------------------------
# バリデーションヘルパ
# ---------------------------------------------------------------------------


async def _read_json_body(request: Request) -> tuple[dict, Response | None]:
    try:
        body = await request.json()
    except Exception:
        return {}, error_response(400, INVALID_REQUEST, "リクエストボディが不正な JSON です")
    if not isinstance(body, dict):
        return {}, error_response(400, INVALID_REQUEST, "リクエストボディは JSON object でなければなりません")
    return body, None


async def _read_json_body_optional(request: Request) -> tuple[dict, Response | None]:
    """body が空でもよい endpoint 用（PUT /lease は body 省略可）。"""
    raw = await request.body()
    if not raw:
        return {}, None
    return await _read_json_body(request)


def _validate_lease_ttl(value: object) -> tuple[int | None, Response | None]:
    if value is None:
        return None, None
    if isinstance(value, bool) or not isinstance(value, int):
        return None, error_response(400, INVALID_REQUEST, "lease_ttl は整数（秒）で指定してください")
    if not (MIN_LEASE_TTL_SECONDS <= value <= MAX_LEASE_TTL_SECONDS):
        return None, error_response(
            400,
            INVALID_REQUEST,
            f"lease_ttl は {MIN_LEASE_TTL_SECONDS}〜{MAX_LEASE_TTL_SECONDS} 秒の範囲で指定してください",
        )
    return value, None


def _validate_retain_seconds(value: object) -> tuple[int | None, Response | None]:
    if value is None:
        return None, None
    if isinstance(value, bool) or not isinstance(value, int):
        return None, error_response(
            400, INVALID_REQUEST, "delivery_options.retain_seconds は整数（秒）で指定してください"
        )
    if not (MIN_RETAIN_SECONDS <= value <= MAX_RETAIN_SECONDS):
        return None, error_response(
            400,
            INVALID_REQUEST,
            f"delivery_options.retain_seconds は {MIN_RETAIN_SECONDS}〜{MAX_RETAIN_SECONDS} 秒の範囲で指定してください",
        )
    return value, None


# ---------------------------------------------------------------------------
# endpoint: POST /subscriptions
# ---------------------------------------------------------------------------


@require_authn
async def create_subscription(request: Request) -> Response:
    identity: Identity = request.state.identity
    body, err = await _read_json_body(request)
    if err is not None:
        return err

    subscriber = body.get("subscriber")
    if not isinstance(subscriber, str) or not subscriber:
        return error_response(400, INVALID_REQUEST, "subscriber は必須の非空文字列です")
    if subscriber != identity.id:
        return error_response(
            403,
            SUBSCRIBER_MISMATCH,
            "subscriber は認証済み identity と一致していなければなりません（代理 subscribe 不可）",
        )

    labels = body.get("labels")
    if not isinstance(labels, list) or not labels:
        return error_response(400, LABEL_VALIDATION, "labels は非空配列で指定してください（firehose 防止）")
    if not all(isinstance(label, str) for label in labels):
        return error_response(400, LABEL_VALIDATION, "labels は文字列の配列で指定してください")

    lease_ttl, err = _validate_lease_ttl(body.get("lease_ttl"))
    if err is not None:
        return err
    if lease_ttl is None:
        lease_ttl = DEFAULT_LEASE_TTL_SECONDS

    delivery_options = body.get("delivery_options")
    if delivery_options is not None and not isinstance(delivery_options, dict):
        return error_response(400, INVALID_REQUEST, "delivery_options は JSON object で指定してください")
    retain_seconds, err = _validate_retain_seconds(
        (delivery_options or {}).get("retain_seconds")
    )
    if err is not None:
        return err
    if retain_seconds is None:
        retain_seconds = DEFAULT_RETAIN_SECONDS

    registry = get_registry(request)
    record = registry.create(identity.id, frozenset(labels), lease_ttl, retain_seconds)
    observability.record_event(
        request.app.state,
        "subscribe",
        subscription_id=record.subscription_id,
        subscriber=identity.id,
        labels=sorted(labels),
    )
    return JSONResponse(
        {
            "subscription_id": record.subscription_id,
            "lease_expires_at": _iso(record.lease_expires_at),
        },
        status_code=201,
    )


# ---------------------------------------------------------------------------
# endpoint: PUT /subscriptions/{subscription_id}/lease
# ---------------------------------------------------------------------------


@require_authn
async def renew_lease(request: Request) -> Response:
    identity: Identity = request.state.identity
    subscription_id = request.path_params["subscription_id"]
    registry = get_registry(request)

    if not registry.is_owner(subscription_id, identity.id):
        return error_response(
            404, SUBSCRIPTION_NOT_FOUND, f"subscription '{subscription_id}' が見つかりません"
        )
    if registry.is_lease_expired(subscription_id):
        return error_response(
            410, SUBSCRIPTION_GONE, f"subscription '{subscription_id}' の lease は切れています"
        )

    body, err = await _read_json_body_optional(request)
    if err is not None:
        return err
    lease_ttl, err = _validate_lease_ttl(body.get("lease_ttl"))
    if err is not None:
        return err

    record = registry.renew(subscription_id, lease_ttl)
    assert record is not None  # is_owner で存在確認済み
    return JSONResponse({"lease_expires_at": _iso(record.lease_expires_at)})


# ---------------------------------------------------------------------------
# endpoint: DELETE /subscriptions/{subscription_id}（unsubscribe）
# ---------------------------------------------------------------------------


@require_authn
async def delete_subscription(request: Request) -> Response:
    identity: Identity = request.state.identity
    subscription_id = request.path_params["subscription_id"]
    registry = get_registry(request)

    if not registry.is_owner(subscription_id, identity.id):
        return error_response(
            404, SUBSCRIPTION_NOT_FOUND, f"subscription '{subscription_id}' が見つかりません"
        )

    # 明示的な関心放棄は DLQ を通らず、未 ack エントリを同一 transaction で即時削除する
    # （wire-api.md §5.3）。lease 切れ済みでも unsubscribe 自体は許可する（§5.3 に 410 の
    # 規定がないため、DELETE は ownership のみを見る）。
    conn = _get_connection(request)
    try:
        conn.execute(
            "DELETE FROM outbox WHERE target_type = 'subscription' AND subscription_id = ?",
            (subscription_id,),
        )
        conn.commit()
    finally:
        conn.close()

    registry.delete(subscription_id)
    observability.record_event(
        request.app.state, "unsubscribe", subscription_id=subscription_id, subscriber=identity.id
    )
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# endpoint: POST /subscriptions/{subscription_id}/ack
# ---------------------------------------------------------------------------


@require_authn
async def ack_subscription(request: Request) -> Response:
    identity: Identity = request.state.identity
    subscription_id = request.path_params["subscription_id"]
    registry = get_registry(request)

    if not registry.is_owner(subscription_id, identity.id):
        return error_response(
            404, SUBSCRIPTION_NOT_FOUND, f"subscription '{subscription_id}' が見つかりません"
        )
    if registry.is_lease_expired(subscription_id):
        return error_response(
            410, SUBSCRIPTION_GONE, f"subscription '{subscription_id}' の lease は切れています"
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
            " WHERE target_type = 'subscription' AND subscription_id = ? AND publish_id <= ?",
            (subscription_id, up_to_publish_id),
        )
        conn.commit()
    finally:
        conn.close()

    observability.record_event(
        request.app.state,
        "ack_received",
        lane="subscription",
        subscription_id=subscription_id,
        up_to_publish_id=up_to_publish_id,
    )
    observability.inc_metric(request.app.state, "relay_ack_received_total")
    return JSONResponse({}, status_code=200)


# ---------------------------------------------------------------------------
# endpoint: POST /publish（subscription レーン publish）
# ---------------------------------------------------------------------------


@require_authn
async def publish(request: Request) -> Response:
    identity: Identity = request.state.identity
    settings: Settings = request.app.state.settings

    limiter = _get_rate_limiter(request)
    allowed, retry_after = limiter.allow(identity.id)
    if not allowed:
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="rate_limited"
        )
        response = error_response(
            429, RATE_LIMIT_EXCEEDED, "publish のレート制限を超過しました"
        )
        response.headers["Retry-After"] = str(retry_after)
        return response

    body, err = await _read_json_body(request)
    if err is not None:
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="invalid_request"
        )
        return err

    ref = body.get("ref")
    if not isinstance(ref, dict) or not isinstance(ref.get("type"), str) or "id" not in ref:
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="invalid_request"
        )
        return error_response(400, INVALID_REQUEST, "ref は { type, id } object で指定してください")

    labels = body.get("labels")
    if not isinstance(labels, list) or not all(isinstance(label, str) for label in labels):
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="invalid_request"
        )
        return error_response(400, INVALID_REQUEST, "labels は文字列の配列で指定してください")

    title = body.get("title")
    if title is not None and not isinstance(title, str):
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="invalid_request"
        )
        return error_response(400, INVALID_REQUEST, "title は文字列で指定してください")

    idempotency_key = body.get("idempotency_key")
    if idempotency_key is not None and not isinstance(idempotency_key, str):
        observability.inc_metric(
            request.app.state, "relay_publish_failed_total", failure_reason="invalid_request"
        )
        return error_response(400, INVALID_REQUEST, "idempotency_key は文字列で指定してください")

    labels_set = frozenset(labels)
    dedup_scope = json.dumps(ref, sort_keys=True, ensure_ascii=False)
    dedup_key = idempotency.build_key(
        lane="subscription",
        publisher_identity=identity.id,
        explicit_key=idempotency_key,
        scope=dedup_scope,
        labels=labels_set,
        body=title,
    )
    store = idempotency.get_store(request.app.state)
    existing_publish_id = store.check(dedup_key)
    if existing_publish_id is not None:
        return JSONResponse(
            {"publish_id": existing_publish_id, "matched_subscriptions": 0}, status_code=202
        )

    registry = get_registry(request)
    matches = registry.matching(labels_set)

    payload = json.dumps({"ref": ref, "title": title}, ensure_ascii=False).encode("utf-8")
    labels_json = json.dumps(sorted(labels_set), ensure_ascii=False)
    now_dt = _now()
    now = _iso(now_dt)

    conn = _get_connection(request)
    try:
        cur = conn.execute(
            "INSERT INTO publish_log (lane, stream_id, publisher_identity, enqueued_at)"
            " VALUES ('subscription', NULL, ?, ?)",
            (identity.id, now),
        )
        publish_id = cur.lastrowid
        for record in matches:
            expires_at = _iso(now_dt + timedelta(seconds=record.retain_seconds))
            conn.execute(
                "INSERT INTO outbox"
                " (target_type, subscription_id, publish_id, payload, labels, enqueued_at,"
                " expires_at)"
                " VALUES ('subscription', ?, ?, ?, ?, ?, ?)",
                (record.subscription_id, publish_id, payload, labels_json, now, expires_at),
            )
        conn.commit()
    finally:
        conn.close()
    store.register(dedup_key, publish_id)

    observability.record_event(
        request.app.state,
        "publish_received",
        lane="subscription",
        publish_id=publish_id,
        publisher_identity=identity.id,
        matched_subscriptions=len(matches),
    )
    observability.inc_metric(
        request.app.state, "relay_publish_received_total", publisher_identity=identity.id
    )
    return JSONResponse(
        {"publish_id": publish_id, "matched_subscriptions": len(matches)}, status_code=202
    )


routes: list[Route] = [
    Route("/subscriptions", create_subscription, methods=["POST"]),
    Route("/subscriptions/{subscription_id}/lease", renew_lease, methods=["PUT"]),
    Route("/subscriptions/{subscription_id}", delete_subscription, methods=["DELETE"]),
    Route("/subscriptions/{subscription_id}/ack", ack_subscription, methods=["POST"]),
    Route("/publish", publish, methods=["POST"]),
]
