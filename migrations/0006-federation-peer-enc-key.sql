-- peer の envelope 暗号化用公開鍵（ECDH-ES, P-256 JWK）を追加で pin できるようにする。
--
-- 署名鍵（peers.key_jwk, ES256 / federation 認証用）とは別の鍵ペアであり、値も別カラムに
-- 持つ（1 つの鍵を署名と暗号化の 2 用途に流用しない）。NULL 許容: 既存 pin 済み peer は
-- 未設定のまま残り、envelope の body は互換のため平文で送る（relay.federation_egress の
-- 暗号化フォールバック判定 = 自分の暗号化鍵設定 AND このカラムが非 NULL）。

-- depends: 0005-federation-inbound-dedup

ALTER TABLE peers ADD COLUMN enc_key_jwk TEXT;
