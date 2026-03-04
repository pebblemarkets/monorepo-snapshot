ALTER TABLE orders
ADD COLUMN IF NOT EXISTS wal_offset BIGINT;

CREATE INDEX IF NOT EXISTS orders_account_nonce_wal_offset_idx
ON orders (account_id, nonce, wal_offset);
