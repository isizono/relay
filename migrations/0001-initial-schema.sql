-- relay v2 初期 schema
--
-- disk (SQLite) に永続化するのは outbox / dlq / publish_log / agent_cards の 4 table のみ。
-- streams / memberships / subscriptions は relay-v2-wire-api.md §0 の R1 原則により
-- in-memory（liveness クラス）として実装する。relay 再起動で消え、re-subscribe /
-- re-register で自己修復する設計であり、SQLite には作らない。
-- 詳細な設計判断の根拠は docs/ARCHITECTURE.md を参照。

-- depends:

-- publish_id の採番専用 table。1 行 INSERT して AUTOINCREMENT された rowid を
-- publish_id として採用する（同一 transaction で outbox 展開まで行う）。
CREATE TABLE publish_log (
    publish_id          INTEGER PRIMARY KEY AUTOINCREMENT,
    lane                TEXT NOT NULL CHECK (lane IN ('stream', 'subscription')),
    stream_id           TEXT,
    publisher_identity  TEXT NOT NULL,
    enqueued_at         TEXT NOT NULL
);

-- transactional outbox。publish 受領 → マッチング → 各 delivery target への
-- エントリ作成を単一 transaction で行う（relay-v2-wire-api.md §6.1）。
--
-- delivery target は 2 種類:
--   - subscription レーン: target_type='subscription', subscription_id で識別
--   - stream レーン:       target_type='stream',       (stream_id, member_identity) で識別
--                           （場レーンの配達先は「stream × 呼び出し元 identity」に解決される。
--                            relay-v2-wire-api.md §5.6 / §5.7）
CREATE TABLE outbox (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    target_type       TEXT NOT NULL CHECK (target_type IN ('subscription', 'stream')),
    subscription_id   TEXT,
    stream_id         TEXT,
    member_identity   TEXT,
    publish_id        INTEGER NOT NULL,
    payload           BLOB NOT NULL,
    labels            TEXT,
    enqueued_at       TEXT NOT NULL,
    next_attempt_at   TEXT,
    attempt_count     INTEGER NOT NULL DEFAULT 0,
    CHECK (
        (target_type = 'subscription' AND subscription_id IS NOT NULL AND stream_id IS NULL AND member_identity IS NULL)
        OR
        (target_type = 'stream' AND stream_id IS NOT NULL AND member_identity IS NOT NULL AND subscription_id IS NULL)
    )
);

-- subscription レーンの ack / 重複排除は (subscription_id, publish_id) の組で行う。
CREATE UNIQUE INDEX idx_outbox_subscription_target
    ON outbox(subscription_id, publish_id)
    WHERE target_type = 'subscription';

-- stream レーンの ack / 重複排除は (stream_id, member_identity, publish_id) の組で行う。
CREATE UNIQUE INDEX idx_outbox_stream_target
    ON outbox(stream_id, member_identity, publish_id)
    WHERE target_type = 'stream';

-- dispatcher の polling 走査用（未 ack かつ未 dead のエントリを順に SELECT）。
CREATE INDEX idx_outbox_next_attempt_at ON outbox(next_attempt_at);

-- DLQ（dead letter）。retain 超過 / permanent error で dead 化した outbox エントリの退避先。
-- outbox と同じ delivery target 構造を持つ。
CREATE TABLE dlq (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    target_type       TEXT NOT NULL CHECK (target_type IN ('subscription', 'stream')),
    subscription_id   TEXT,
    stream_id         TEXT,
    member_identity   TEXT,
    publish_id        INTEGER NOT NULL,
    payload           BLOB NOT NULL,
    labels            TEXT,
    error_code        TEXT NOT NULL,
    dead_at           TEXT NOT NULL,
    CHECK (
        (target_type = 'subscription' AND subscription_id IS NOT NULL AND stream_id IS NULL AND member_identity IS NULL)
        OR
        (target_type = 'stream' AND stream_id IS NOT NULL AND member_identity IS NOT NULL AND subscription_id IS NULL)
    )
);

-- dead_at から 7 日後の物理 DELETE sweep 用。
CREATE INDEX idx_dlq_dead_at ON dlq(dead_at);

-- 外部 agent の AgentCard キャッシュ（identity/authZ 仕様 §1.3, §4.2）。
-- relay 自身が発行する AgentCard の disk-persisted 設定用途にも使う。
CREATE TABLE agent_cards (
    identity            TEXT PRIMARY KEY,
    card_json           TEXT NOT NULL,
    public_keys_jwks    TEXT,
    fetched_at          TEXT NOT NULL,
    expires_at          TEXT
);

CREATE INDEX idx_agent_cards_expires_at ON agent_cards(expires_at);
