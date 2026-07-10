-- federation inbound メッセージの dedup（(origin_fingerprint, origin_publish_id) → local_publish_id）。
--
-- egress の再送（B 未達 / タイムアウト等による store-and-forward retry）が同一
-- (origin_fingerprint, origin_publish_id) で再到達したとき、二重に publish_log/outbox へ
-- 書き込まず同一 202 を返すための disk 永続化 dedup。relay 再起動を跨いだ redelivery
-- （store-and-forward retention 24h の間）を捕捉する必要があるため、15 分 window の
-- in-memory idempotency store（relay.idempotency）とは別に disk table として持つ。

-- depends: 0004-federation-peers

CREATE TABLE federation_inbound_dedup (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    origin_fingerprint  TEXT NOT NULL,
    origin_publish_id   INTEGER NOT NULL,
    local_publish_id    INTEGER NOT NULL,
    created_at          TEXT NOT NULL
);

CREATE UNIQUE INDEX idx_federation_inbound_dedup_origin
    ON federation_inbound_dedup(origin_fingerprint, origin_publish_id);
