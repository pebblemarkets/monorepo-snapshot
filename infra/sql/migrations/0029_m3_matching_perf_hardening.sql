-- M3: matching throughput hardening.
--
-- 1) Replace per-order MAX() scans for fill/event sequencing with O(1) counters.
-- 2) Add partial, side-specific open-book indexes aligned with matching query order.

CREATE TABLE IF NOT EXISTS market_sequences (
  market_id TEXT PRIMARY KEY,
  next_fill_sequence BIGINT NOT NULL DEFAULT 0,
  next_event_sequence BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK (next_fill_sequence >= 0),
  CHECK (next_event_sequence >= 0)
);

WITH known_markets AS (
  SELECT market_id FROM orders
  UNION
  SELECT market_id FROM fills
  UNION
  SELECT aggregate_id AS market_id
  FROM trading_events
  WHERE aggregate_type = 'market'
),
fill_max AS (
  SELECT market_id, MAX(fill_sequence) AS max_fill_sequence
  FROM fills
  GROUP BY market_id
),
event_max AS (
  SELECT aggregate_id AS market_id, MAX(sequence) AS max_event_sequence
  FROM trading_events
  WHERE aggregate_type = 'market'
  GROUP BY aggregate_id
)
INSERT INTO market_sequences (
  market_id,
  next_fill_sequence,
  next_event_sequence,
  updated_at
)
SELECT
  km.market_id,
  COALESCE(fm.max_fill_sequence, -1) + 1,
  COALESCE(em.max_event_sequence, -1) + 1,
  now()
FROM known_markets km
LEFT JOIN fill_max fm
  ON fm.market_id = km.market_id
LEFT JOIN event_max em
  ON em.market_id = km.market_id
ON CONFLICT (market_id) DO UPDATE
SET
  next_fill_sequence = GREATEST(
    market_sequences.next_fill_sequence,
    EXCLUDED.next_fill_sequence
  ),
  next_event_sequence = GREATEST(
    market_sequences.next_event_sequence,
    EXCLUDED.next_event_sequence
  ),
  updated_at = now();

CREATE INDEX IF NOT EXISTS orders_open_book_asks_idx
  ON orders(
    instrument_admin,
    instrument_id,
    market_id,
    outcome,
    price_ticks ASC,
    submitted_at ASC,
    order_id ASC
  )
  INCLUDE (account_id, side, remaining_minor, locked_minor)
  WHERE side = 'Sell'
    AND status IN ('Open', 'PartiallyFilled')
    AND remaining_minor > 0;

CREATE INDEX IF NOT EXISTS orders_open_book_bids_idx
  ON orders(
    instrument_admin,
    instrument_id,
    market_id,
    outcome,
    price_ticks DESC,
    submitted_at ASC,
    order_id ASC
  )
  INCLUDE (account_id, side, remaining_minor, locked_minor)
  WHERE side = 'Buy'
    AND status IN ('Open', 'PartiallyFilled')
    AND remaining_minor > 0;
