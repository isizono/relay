"""配達基盤（outbox / SSE / DLQ、relay-v2-wire-api.md §5.5, §6）。

`GET /events`（SSE 多重化購読）、polling dispatcher（未 ack outbox の SELECT → push →
retry → DLQ 化ループ）、DLQ sweep（dead_at から 7 日後の物理 DELETE）はここに実装する
（後続タスクの担当分）。

outbox / dlq の物理 schema は `migrations/0001-initial-schema.sql` を参照
（`relay.db` モジュールで migration 適用）。

`GET /events` の `subscription_ids=` には ownership 検証（structural authZ、
wire-api.md §5.7）が掛かる。slow consumer 強制切断（retry 累積超過 / keepalive write
失敗）もこのモジュールの責務（wire-api.md §6.4）。
"""
from __future__ import annotations

from starlette.routing import Route

routes: list[Route] = []
