"""observability（relay-v2-wire-api.md §7）。

`GET /status`（§7.1 運用スナップショット）/ `GET /metrics`（§7.2 Prometheus 互換）/
構造化ログ + サーバーログ sink（§7.3）を実装する。

`GET /status` / `GET /metrics` は他の GET 系 endpoint と同様 `relay.identity.require_authn`
のみを通す（authN のみで authZ なし、identity-authz.md §2.1）。

## 構造化ログとサーバーログの統合

Foundation の元 docstring は「構造化ログ」（`publish_id` で trace する短期ログ）と
「サーバーログ」（payload 込み・TTL 90 日の長期デバッグ sink）を 2 つの独立した sink として
想定していたが、本実装ではこれを単一の JSON Lines append-only sink
（`Settings.server_log_path`、既定 `relay-server.jsonl`）に統合している（`event` フィールドで
種別を判別できるため、実用上 2 sink に分離する必然性が薄いと判断した）。
**購読者向け読み取り endpoint は一切持たない**（wire-api.md §7.3 が明示的に禁止する
「`since=N` 型 pull の裏口化」を避けるため）。

`record_event(app_state, event_type, level="info"|"warning", **fields)` の `level="warning"`
event は、サーバーログへの追記に加えて `app_state` 上の in-memory リングバッファ
（既定 50 件、`RECENT_WARNINGS_MAXLEN`）にも積まれる。`GET /status` の `recent_warnings` は
このバッファを返すが、任意の認証済み client に露出するため、バッファには warning の構造的な
識別子（`_STATUS_WARNING_SAFE_FIELDS`）のみを載せ、free-form な reason / peer identity /
payload・title 本文は落とす（full な entry はサーバーログ側にのみ残る）。warning 対象は
DLQ 移動（`outbox_dead`）・subscription registry からの lease 切れ除去
（`subscription_registry_evicted`）・dispatcher 内部エラー（`dispatcher_error`）・
SSE slow consumer 強制切断（`sse_slow_consumer_disconnect`）・認証失敗（`authn_failed`）。

## metrics（Prometheus 互換、§7.2）

カウンタ系（`_total` で終わる 7 metric）は `MetricsRegistry`（`app_state.metrics_registry`）に
in-memory で積算し、呼び出し側（`streams.py` / `subscriptions.py` / `delivery.py` /
`identity.py` は関与しない）が `inc_metric(app_state, name, **labels)` を該当箇所で呼ぶ。
gauge 系（`relay_outbox_depth` / `relay_sse_connections`）は積算せず、`GET /metrics` の
スクレイプ時点で DB / `ConnectionManager` から実測して都度計算する（カウンタの drift を防ぐため）。

metric の label には peer identity（`publisher_identity`）・`subscription_id`・
`delivery_target` を使わない（wire-api.md §7.2）。`GET /metrics` は任意の認証済み client が
読めるため、他 peer の identity 列挙・subscription_id 露出・label cardinality 爆発を避ける。
publisher identity の trace が要る場合は構造化ログ（`record_event`）側にのみ載せる。
"""
from __future__ import annotations

import json
import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse, PlainTextResponse, Response
from starlette.routing import Route

from relay import db
from relay.config import Settings
from relay.identity import require_authn

SERVER_LOG_TTL_DAYS = 90
_GC_MIN_INTERVAL_SECONDS = 3600  # 期限切れ行の間引きは 1 時間に 1 回まで

# `GET /status` の `recent_warnings` が保持する件数（in-memory リングバッファ、app 単位）。
RECENT_WARNINGS_MAXLEN = 50

# recent_warnings は `GET /status` で任意の認証済み client に返るため、warning event の
# フィールドのうち構造的な resource 識別子だけをこの allowlist で通す。free-form な reason /
# peer identity / payload・title 本文はここに含めない（含めると cross-tenant のユーザーデータ
# 漏洩になる）。未知フィールドは default-deny で落とすので、新しい warning event が安全でない
# フィールドを足しても /status からは漏れない。full な entry はサーバーログ sink 側に残る。
_STATUS_WARNING_SAFE_FIELDS = frozenset(
    {
        "lane",
        "target_type",
        "publish_id",
        "stream_id",
        "subscription_id",
        "error_code",
        "oldest_unacked_publish_id",
    }
)

_write_lock = threading.Lock()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ts_format(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


# ---------------------------------------------------------------------------
# 構造化ログ + サーバーログ sink
# ---------------------------------------------------------------------------


def _get_recent_warnings(app_state: Any) -> deque:
    warnings = getattr(app_state, "recent_warnings", None)
    if warnings is None:
        warnings = deque(maxlen=RECENT_WARNINGS_MAXLEN)
        app_state.recent_warnings = warnings
    return warnings


def record_event(app_state: Any, event_type: str, *, level: str = "info", **fields: Any) -> None:
    """構造化ログ 1 件をサーバーログ sink に append する。

    `level="warning"`（既定は `"info"`）の event は、`app_state` 上の in-memory
    `recent_warnings` リングバッファにも同時に積む（`GET /status` 用）。バッファには
    `_STATUS_WARNING_SAFE_FIELDS` に載る識別子のみを積み、reason / identity / payload 等は
    サーバーログ側の full な entry にのみ残す。

    `settings` が取れない呼び出し（テストの最小 fixture 等）ではファイル書き込みのみ
    無視して no-op にする（observability は best-effort、配達経路そのものには一切関与しない）。
    """
    entry = {"ts": _ts_format(_now()), "event": event_type, "level": level, **fields}

    if level == "warning":
        safe_view = {
            "ts": entry["ts"],
            "event": event_type,
            "level": level,
            **{k: v for k, v in fields.items() if k in _STATUS_WARNING_SAFE_FIELDS},
        }
        with _write_lock:
            _get_recent_warnings(app_state).append(safe_view)

    settings: Settings | None = getattr(app_state, "settings", None)
    if settings is None:
        return
    line = json.dumps(entry, ensure_ascii=False, default=str)
    with _write_lock:
        with open(settings.server_log_path, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    _maybe_gc(app_state, settings)


def _maybe_gc(app_state: Any, settings: Settings) -> None:
    last = getattr(app_state, "_server_log_last_gc", None)
    now = _now()
    if last is not None and (now - last).total_seconds() < _GC_MIN_INTERVAL_SECONDS:
        return
    app_state._server_log_last_gc = now
    purge_expired_server_log(settings)


def purge_expired_server_log(settings: Settings) -> None:
    """TTL（既定 90 日）を過ぎた行をサーバーログから取り除く。

    パース不能な行（壊れた書き込み等）は安全側に倒して残す。
    """
    path = Path(settings.server_log_path)
    if not path.exists():
        return
    cutoff = _now() - timedelta(days=SERVER_LOG_TTL_DAYS)
    with _write_lock:
        with open(path, encoding="utf-8") as f:
            lines = f.readlines()
        kept = []
        for line in lines:
            try:
                entry = json.loads(line)
                ts = datetime.strptime(entry["ts"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(
                    tzinfo=timezone.utc
                )
            except (json.JSONDecodeError, KeyError, ValueError):
                kept.append(line)
                continue
            if ts >= cutoff:
                kept.append(line)
        with open(path, "w", encoding="utf-8") as f:
            f.writelines(kept)


# ---------------------------------------------------------------------------
# Prometheus 互換カウンタ（wire-api.md §7.2）
# ---------------------------------------------------------------------------


class MetricsRegistry:
    """counter 系 metric を保持する in-memory registry。

    label の組み合わせ（`dict` を `sorted` した `tuple[tuple[str, str], ...]`）ごとに
    単調増加のカウンタを持つ。gauge 相当の値はここでは保持しない（`render_prometheus_metrics`
    がスクレイプ時点の実測値を都度計算する）。
    """

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: dict[str, dict[tuple[tuple[str, str], ...], float]] = {}

    def inc(self, name: str, amount: float = 1.0, **labels: str) -> None:
        key = tuple(sorted(labels.items()))
        with self._lock:
            bucket = self._counters.setdefault(name, {})
            bucket[key] = bucket.get(key, 0.0) + amount

    def snapshot(self) -> dict[str, dict[tuple[tuple[str, str], ...], float]]:
        with self._lock:
            return {name: dict(values) for name, values in self._counters.items()}


def get_metrics_registry(app_state: Any) -> MetricsRegistry:
    """`app_state.metrics_registry` を遅延初期化して返す。"""
    registry = getattr(app_state, "metrics_registry", None)
    if registry is None:
        registry = MetricsRegistry()
        app_state.metrics_registry = registry
    return registry


def inc_metric(app_state: Any, name: str, amount: float = 1.0, **labels: str) -> None:
    """`streams.py` / `subscriptions.py` / `delivery.py` から呼ぶカウンタ加算の薄いラッパー。

    registry の生成・保持は本モジュールに閉じる（呼び出し側は import 循環を気にしなくてよい。
    `relay.streams` / `relay.subscriptions` / `relay.delivery` はいずれも本モジュールを import
    済みだが、本モジュールはそれらを import しない — この非対称性で循環 import を避けている）。
    """
    get_metrics_registry(app_state).inc(name, amount, **labels)


# gauge かどうかは `GET /metrics` 側の実測値取得ロジックで区別するため、ここでは
# 名前 → (type, help) の対応表のみ持つ。counter は起動直後（未 increment）は出力されない
# （典型的な Prometheus client の挙動に合わせた。cardinality が未使用のまま無限に積み上がらない）。
_METRIC_HELP: dict[str, tuple[str, str]] = {
    "relay_publish_received_total": (
        "counter",
        "Total number of publishes accepted into the outbox (stream + subscription lanes).",
    ),
    "relay_publish_failed_total": (
        "counter",
        "Total number of publish attempts rejected before reaching the outbox, by failure_reason.",
    ),
    "relay_push_delivered_total": (
        "counter",
        "Total number of outbox entries successfully pushed over an SSE connection, by lane.",
    ),
    "relay_ack_received_total": (
        "counter",
        "Total number of cumulative ack requests received (stream + subscription lanes).",
    ),
    "relay_outbox_dead_total": (
        "counter",
        "Total number of outbox entries moved to the dead letter queue.",
    ),
    "relay_subscription_lease_expirations_total": (
        "counter",
        "Total number of subscriptions reaped from the in-memory registry after lease expiry.",
    ),
    "relay_sse_slow_consumer_disconnects_total": (
        "counter",
        "Total number of SSE connections force-disconnected as slow consumers.",
    ),
    "relay_federation_plaintext_fallback_total": (
        "counter",
        "Total number of federation envelopes whose first send attempt used an unencrypted"
        " body because an encryption key was missing on either side, by reason. Counted once"
        " per outbox row regardless of whether that attempt ultimately succeeds.",
    ),
    "relay_outbox_depth": (
        "gauge",
        "Current number of pending (undelivered) outbox entries across both lanes.",
    ),
    "relay_sse_connections": (
        "gauge",
        "Current number of active SSE connections.",
    ),
}


def _escape_label_value(value: str) -> str:
    return str(value).replace("\\", "\\\\").replace('"', '\\"').replace("\n", "\\n")


def _format_labels(label_tuple: tuple[tuple[str, str], ...]) -> str:
    if not label_tuple:
        return ""
    parts = ",".join(f'{k}="{_escape_label_value(v)}"' for k, v in label_tuple)
    return "{" + parts + "}"


def _active_sse_connection_count(app_state: Any) -> int:
    manager = getattr(app_state, "connection_manager", None)
    return manager.count() if manager is not None else 0


def _streams_count(app_state: Any) -> int:
    registry = getattr(app_state, "stream_registry", None)
    return registry.count() if registry is not None else 0


def _subscriptions_count(app_state: Any) -> int:
    registry = getattr(app_state, "subscription_registry", None)
    return registry.count() if registry is not None else 0


def _outbox_counts(settings: Settings) -> tuple[int, int]:
    """`(outbox_pending_count, outbox_dead_count)` を返す。"""
    conn = db.get_connection(settings.db_path)
    try:
        pending = conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0]
        dead = conn.execute("SELECT COUNT(*) FROM dlq").fetchone()[0]
        return pending, dead
    finally:
        conn.close()


def render_prometheus_metrics(app_state: Any) -> str:
    """`GET /metrics` の本文（Prometheus text exposition format）を組み立てる。"""
    settings: Settings = app_state.settings
    counters = get_metrics_registry(app_state).snapshot()
    outbox_depth, _dead = _outbox_counts(settings)

    gauge_values: dict[str, dict[tuple[tuple[str, str], ...], float]] = {
        "relay_outbox_depth": {(): outbox_depth},
        "relay_sse_connections": {(): _active_sse_connection_count(app_state)},
    }

    lines: list[str] = []
    for name, (metric_type, help_text) in _METRIC_HELP.items():
        values = gauge_values.get(name, {}) if metric_type == "gauge" else counters.get(name, {})
        lines.append(f"# HELP {name} {help_text}")
        lines.append(f"# TYPE {name} {metric_type}")
        for label_tuple, value in sorted(values.items()):
            lines.append(f"{name}{_format_labels(label_tuple)} {value}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# endpoint: GET /status（wire-api.md §7.1）
# ---------------------------------------------------------------------------


@require_authn
async def get_status(request: Request) -> Response:
    app_state = request.app.state
    settings: Settings = app_state.settings

    outbox_pending_count, outbox_dead_count = _outbox_counts(settings)

    cutoff = (_now() - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn = db.get_connection(settings.db_path)
    try:
        publishes_5min = conn.execute(
            "SELECT COUNT(*) FROM publish_log WHERE enqueued_at >= ?", (cutoff,)
        ).fetchone()[0]
    finally:
        conn.close()

    started_at = getattr(app_state, "started_at", None)
    uptime_seconds = (time.monotonic() - started_at) if started_at is not None else 0.0

    body = {
        "uptime_seconds": round(uptime_seconds, 3),
        "subscriptions_count": _subscriptions_count(app_state),
        "active_sse_connections": _active_sse_connection_count(app_state),
        "streams_count": _streams_count(app_state),
        "outbox_pending_count": outbox_pending_count,
        "outbox_dead_count": outbox_dead_count,
        # 5 分間の publish 件数を秒あたりレート（Prometheus rate() 相当）に換算する。
        "publish_rate_5min": round(publishes_5min / 300.0, 4),
        "recent_warnings": list(_get_recent_warnings(app_state)),
    }
    return JSONResponse(body)


# ---------------------------------------------------------------------------
# endpoint: GET /metrics（wire-api.md §7.2）
# ---------------------------------------------------------------------------


@require_authn
async def get_metrics(request: Request) -> Response:
    body = render_prometheus_metrics(request.app.state)
    return PlainTextResponse(body, media_type="text/plain; version=0.0.4; charset=utf-8")


routes: list[Route] = [
    Route("/status", get_status, methods=["GET"]),
    Route("/metrics", get_metrics, methods=["GET"]),
]
