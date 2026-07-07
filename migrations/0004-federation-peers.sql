-- federation の peer レジストリと招待ベース鍵ピン留め（招待 token・redeem 済み peer pin）。
--
-- 既存 invitations / credentials と同一パターン（atomic 消費マーク・一律 404 存在秘匿）を
-- 別テーブルとして持つ。local 招待の産物が Bearer token であるのに対し、peer 招待の産物は
-- pin 済み公開鍵（federation レーンの authN 素材）であり、信頼の産物が異なるため別物とする。

-- depends: 0003-invitations-credentials

CREATE TABLE peers (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    handle       TEXT NOT NULL UNIQUE,      -- ローカルでの相手の呼び名
    fingerprint  TEXT NOT NULL UNIQUE,      -- RFC 7638 JWK thumbprint (SHA-256, b64url)
    key_jwk      TEXT NOT NULL,             -- pin した公開鍵 (JWK JSON)
    locator      TEXT NOT NULL,             -- 相手 relay の base URL（identity と分離、可変）
    created_at   TEXT NOT NULL,
    revoked_at   TEXT,                      -- NULL=active。unpin は即時（verifier が毎回 DB を引く）
    disclosure_level TEXT                   -- per-peer 開示段階の予約列。書き込みコードは未実装
);
CREATE INDEX idx_peers_fingerprint ON peers(fingerprint);
CREATE INDEX idx_peers_handle ON peers(handle);

CREATE TABLE peer_invitations (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    token        TEXT NOT NULL UNIQUE,      -- pi_<128bit b64url>
    handle       TEXT NOT NULL,             -- redeem 成功時にこの handle で pin する
    created_at   TEXT NOT NULL,
    expires_at   TEXT NOT NULL,
    redeemed_at  TEXT,                      -- atomic 消費マーク（credentials.redeem_invite と同型）
    redeemed_peer_id INTEGER REFERENCES peers(id)
);
CREATE INDEX idx_peer_invitations_token ON peer_invitations(token);
