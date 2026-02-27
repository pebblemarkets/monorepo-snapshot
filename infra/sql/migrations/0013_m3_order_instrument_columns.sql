-- M3: bind off-ledger orders/fills to instrument identity for clearing aggregation.

ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS instrument_admin TEXT;

ALTER TABLE orders
  ADD COLUMN IF NOT EXISTS instrument_id TEXT;

WITH latest_account_ref AS (
  SELECT
    ar.account_id,
    ar.instrument_admin,
    ar.instrument_id
  FROM account_refs ar
  JOIN account_ref_latest l
    ON l.account_id = ar.account_id
   AND l.contract_id = ar.contract_id
  WHERE ar.active = TRUE
)
UPDATE orders o
SET
  instrument_admin = r.instrument_admin,
  instrument_id = r.instrument_id
FROM latest_account_ref r
WHERE o.account_id = r.account_id
  AND (o.instrument_admin IS NULL OR o.instrument_id IS NULL);

ALTER TABLE orders
  ALTER COLUMN instrument_admin SET NOT NULL;

ALTER TABLE orders
  ALTER COLUMN instrument_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS orders_instrument_idx
  ON orders(instrument_admin, instrument_id, market_id, status, submitted_at);

ALTER TABLE fills
  ADD COLUMN IF NOT EXISTS instrument_admin TEXT;

ALTER TABLE fills
  ADD COLUMN IF NOT EXISTS instrument_id TEXT;

UPDATE fills f
SET
  instrument_admin = o.instrument_admin,
  instrument_id = o.instrument_id
FROM orders o
WHERE f.taker_order_id = o.order_id
  AND (f.instrument_admin IS NULL OR f.instrument_id IS NULL);

ALTER TABLE fills
  ALTER COLUMN instrument_admin SET NOT NULL;

ALTER TABLE fills
  ALTER COLUMN instrument_id SET NOT NULL;

CREATE INDEX IF NOT EXISTS fills_instrument_unsettled_idx
  ON fills(instrument_admin, instrument_id, clearing_epoch, fill_sequence, fill_id);
