-- peer 単位で envelope 暗号化を必須化するフラグを追加する。
--
-- 既定 0（false）: 従来通り、鍵が揃わなければ平文 body にフォールバックする。1（true）に
-- すると、この peer 宛の配達は暗号化鍵が双方揃わない限り送信せず permanent error として
-- DLQ に回す（`relay.federation_egress` 参照）。全体設定
-- `RELAY_FEDERATION_REQUIRE_ENCRYPTION` との OR で判定する（どちらか一方が真なら必須）。

-- depends: 0006-federation-peer-enc-key

ALTER TABLE peers ADD COLUMN require_encryption INTEGER NOT NULL DEFAULT 0;
