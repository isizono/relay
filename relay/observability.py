"""observability endpoint（relay-v2-wire-api.md §7）。

`GET /status`（運用スナップショット）/ `GET /metrics`（Prometheus 互換）と、構造化ログ・
サーバーログ（append-only sink、disk 上は SQLite と物理分離した別ファイル、TTL 90 日）は
ここに実装する（後続タスクの担当分）。

サーバーログの既定パスは `relay.config.Settings.server_log_path`
（既定値 `relay-server.jsonl`）を参照。
"""
from __future__ import annotations

from starlette.routing import Route

routes: list[Route] = []
