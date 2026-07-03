"""observability（relay-v2-wire-api.md §7）。

`GET /status`（運用スナップショット）/ `GET /metrics`（Prometheus 互換）は未実装
（後続タスクの担当分）。本モジュールは構造化ログ + サーバーログ sink
（`record_event`）を実装する（relay-v2-wire-api.md §7.3、Delivery タスクの担当分）。

サーバーログの既定パスは `relay.config.Settings.server_log_path`
（既定値 `relay-server.jsonl`）を参照。payload 込みの append-only JSON Lines で、
**購読者向け読み取り endpoint は一切持たない**（`since=N` 型 pull の裏口化を防ぐため、
wire-api.md §7.3 で明示的に禁止されている）。TTL は 90 日で、期限切れの行は定期的に
間引かれる（`purge_expired_server_log`）。

構造化ログ（publish / push / subscribe / unsubscribe / ack / 認証失敗 / outbox エラー /
DLQ 移動を `publish_id` で trace する短期ログ）とサーバーログ（デバッグ用長期 payload 込み
sink）は本来 2 つの sink だが、本実装では単一の JSON Lines sink に統合している
（`event` フィールドで種別を判別）。将来 2 sink に分離する場合はこのモジュールが起点になる。
"""
from __future__ import annotations

import json
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from starlette.routing import Route

from relay.config import Settings

SERVER_LOG_TTL_DAYS = 90
_GC_MIN_INTERVAL_SECONDS = 3600  # 期限切れ行の間引きは 1 時間に 1 回まで

_write_lock = threading.Lock()


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _ts_format(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def record_event(app_state: Any, event_type: str, **fields: Any) -> None:
    """構造化ログ 1 件をサーバーログ sink に append する。

    `settings` が取れない呼び出し（テストの最小 fixture 等）では無視して no-op にする
    （observability は best-effort、配達経路そのものには一切関与しない）。
    """
    settings: Settings | None = getattr(app_state, "settings", None)
    if settings is None:
        return
    entry = {"ts": _ts_format(_now()), "event": event_type, **fields}
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


routes: list[Route] = []
