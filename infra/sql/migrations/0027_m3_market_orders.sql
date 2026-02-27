-- M3: market-order support for off-ledger trading.
--
-- Extend orders constraints to support:
-- - order_type: Limit, Market
-- - tif: GTC, IOC
-- - price semantics:
--   - Limit orders require price_ticks > 0
--   - Market orders allow price_ticks >= 0 (canonicalized to 0 by API)

ALTER TABLE orders
  DROP CONSTRAINT IF EXISTS orders_order_type_check;

ALTER TABLE orders
  DROP CONSTRAINT IF EXISTS orders_tif_check;

ALTER TABLE orders
  DROP CONSTRAINT IF EXISTS orders_price_ticks_check;

ALTER TABLE orders
  ADD CONSTRAINT orders_order_type_check
  CHECK (order_type IN ('Limit', 'Market'));

ALTER TABLE orders
  ADD CONSTRAINT orders_tif_check
  CHECK (tif IN ('GTC', 'IOC'));

ALTER TABLE orders
  ADD CONSTRAINT orders_price_ticks_check
  CHECK (
    (order_type = 'Limit' AND price_ticks > 0)
    OR
    (order_type = 'Market' AND price_ticks >= 0)
  );
