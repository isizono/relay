-- outbox エントリの retain 期限を保持する列を追加する。
--
-- relay-v2-wire-api.md §6.4 は「subscription outbox の retain default = 24h」
-- 「場 outbox の retain default = 場の default_ttl」と規定するが、0001 の outbox
-- schema には enqueue 時点で期限を計算して保持する列が無かった（詳細は
-- docs/ARCHITECTURE.md の Delivery 実装セクションを参照）。
--
-- expires_at は enqueue 時点で `enqueued_at + retain_seconds` を計算して INSERT 時に
-- 埋める（アプリケーション側の責務）。dispatcher の DLQ sweep（relay-v2-wire-api.md
-- §6.6）はこの列を使って retain 超過エントリを検出する。

-- depends: 0001-initial-schema

ALTER TABLE outbox ADD COLUMN expires_at TEXT;

CREATE INDEX idx_outbox_expires_at ON outbox(expires_at);
