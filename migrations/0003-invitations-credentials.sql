-- 招待 token（短命・一回性）と、redeem 済み credential（長命）を管理する。
--
-- 平文保存: relay の authN は Settings.auth_tokens の平文 dict.get であり、DB を
-- ハッシュ化すると authenticate_request の改造が必須になる。fate-sharing 前提の
-- localhost では consumer 側が平文保存する以上ハッシュ化の限界価値が小さいため平文とする。

-- depends: 0002-outbox-expires-at

CREATE TABLE credentials (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    token       TEXT NOT NULL UNIQUE,   -- bt_<base64url> 平文
    identity    TEXT NOT NULL,
    created_at  TEXT NOT NULL,          -- ISO8601 UTC "%Y-%m-%dT%H:%M:%SZ"
    expires_at  TEXT,                   -- NULL=無期限（既定）。非 NULL=期限付き
    revoked_at  TEXT,                   -- NULL=有効
    source_invitation_id INTEGER
);
CREATE INDEX idx_credentials_token ON credentials(token);

CREATE TABLE invitations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    token       TEXT NOT NULL UNIQUE,   -- it_<base64url> 平文
    identity    TEXT NOT NULL,          -- redeem で credential に写像される認証主体
    credential_ttl_seconds INTEGER,     -- NULL=無期限 credential を発行
    created_at  TEXT NOT NULL,
    expires_at  TEXT NOT NULL,          -- 招待 token 失効時刻
    redeemed_at TEXT,                   -- NULL=未使用（atomic 消費マーク）
    redeemed_credential_id INTEGER REFERENCES credentials(id)
);
CREATE INDEX idx_invitations_token ON invitations(token);
