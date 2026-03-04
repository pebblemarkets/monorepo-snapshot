-- M3 Stage C: per-party fill attribution and market max-price metadata.

ALTER TABLE fills
  ADD COLUMN IF NOT EXISTS canonical_price_ticks BIGINT;

ALTER TABLE fills
  ADD COLUMN IF NOT EXISTS maker_outcome TEXT;

ALTER TABLE fills
  ADD COLUMN IF NOT EXISTS maker_side TEXT;

ALTER TABLE fills
  ADD COLUMN IF NOT EXISTS maker_price_ticks BIGINT;

ALTER TABLE fills
  ADD COLUMN IF NOT EXISTS taker_outcome TEXT;

ALTER TABLE fills
  ADD COLUMN IF NOT EXISTS taker_side TEXT;

ALTER TABLE fills
  ADD COLUMN IF NOT EXISTS taker_price_ticks BIGINT;

UPDATE fills f
SET
  canonical_price_ticks = f.price_ticks,
  maker_outcome = maker.outcome,
  maker_side = maker.side,
  maker_price_ticks = f.price_ticks,
  taker_outcome = taker.outcome,
  taker_side = taker.side,
  taker_price_ticks = f.price_ticks
FROM orders maker,
     orders taker
WHERE maker.order_id = f.maker_order_id
  AND taker.order_id = f.taker_order_id
  AND (
    f.canonical_price_ticks IS NULL
    OR f.maker_outcome IS NULL
    OR f.maker_side IS NULL
    OR f.maker_price_ticks IS NULL
    OR f.taker_outcome IS NULL
    OR f.taker_side IS NULL
    OR f.taker_price_ticks IS NULL
  );

ALTER TABLE fills
  ALTER COLUMN canonical_price_ticks SET NOT NULL;

ALTER TABLE fills
  ALTER COLUMN maker_outcome SET NOT NULL;

ALTER TABLE fills
  ALTER COLUMN maker_side SET NOT NULL;

ALTER TABLE fills
  ALTER COLUMN maker_price_ticks SET NOT NULL;

ALTER TABLE fills
  ALTER COLUMN taker_outcome SET NOT NULL;

ALTER TABLE fills
  ALTER COLUMN taker_side SET NOT NULL;

ALTER TABLE fills
  ALTER COLUMN taker_price_ticks SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fills_maker_side_check'
      AND conrelid = 'fills'::regclass
  ) THEN
    ALTER TABLE fills
      ADD CONSTRAINT fills_maker_side_check
      CHECK (maker_side IN ('Buy', 'Sell'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fills_taker_side_check'
      AND conrelid = 'fills'::regclass
  ) THEN
    ALTER TABLE fills
      ADD CONSTRAINT fills_taker_side_check
      CHECK (taker_side IN ('Buy', 'Sell'));
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fills_canonical_price_ticks_check'
      AND conrelid = 'fills'::regclass
  ) THEN
    ALTER TABLE fills
      ADD CONSTRAINT fills_canonical_price_ticks_check
      CHECK (canonical_price_ticks > 0);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fills_maker_price_ticks_check'
      AND conrelid = 'fills'::regclass
  ) THEN
    ALTER TABLE fills
      ADD CONSTRAINT fills_maker_price_ticks_check
      CHECK (maker_price_ticks > 0);
  END IF;

  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fills_taker_price_ticks_check'
      AND conrelid = 'fills'::regclass
  ) THEN
    ALTER TABLE fills
      ADD CONSTRAINT fills_taker_price_ticks_check
      CHECK (taker_price_ticks > 0);
  END IF;
END
$$;

CREATE INDEX IF NOT EXISTS fills_market_taker_outcome_idx
  ON fills(market_id, taker_outcome, matched_at DESC, fill_sequence DESC);

CREATE INDEX IF NOT EXISTS fills_market_maker_outcome_idx
  ON fills(market_id, maker_outcome, matched_at DESC, fill_sequence DESC);

ALTER TABLE markets
  ADD COLUMN IF NOT EXISTS max_price_ticks BIGINT;

ALTER TABLE markets
  ALTER COLUMN max_price_ticks SET DEFAULT 10000;

UPDATE markets
SET max_price_ticks = 10000
WHERE max_price_ticks IS NULL;

ALTER TABLE markets
  ALTER COLUMN max_price_ticks SET NOT NULL;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'markets_max_price_ticks_check'
      AND conrelid = 'markets'::regclass
  ) THEN
    ALTER TABLE markets
      ADD CONSTRAINT markets_max_price_ticks_check
      CHECK (max_price_ticks > 1);
  END IF;
END
$$;

-- Re-run pending-trade reconciliation with per-party fill attribution.
WITH latest_account_state AS (
  SELECT
    st.account_id,
    st.last_applied_epoch
  FROM account_states st
  JOIN account_state_latest asl
    ON asl.contract_id = st.contract_id
  WHERE st.active = TRUE
),
pending_fill_deltas AS (
  SELECT
    account_id,
    COALESCE(SUM(delta_minor), 0)::BIGINT AS pending_delta_minor
  FROM (
    SELECT
      f.taker_account_id AS account_id,
      CASE
        WHEN f.taker_side = 'Buy' THEN -(f.taker_price_ticks * f.quantity_minor)
        ELSE f.taker_price_ticks * f.quantity_minor
      END AS delta_minor
    FROM fills f
    JOIN latest_account_state las
      ON las.account_id = f.taker_account_id
    WHERE f.clearing_epoch IS NULL OR f.clearing_epoch > las.last_applied_epoch

    UNION ALL

    SELECT
      f.maker_account_id AS account_id,
      CASE
        WHEN f.maker_side = 'Buy' THEN -(f.maker_price_ticks * f.quantity_minor)
        ELSE f.maker_price_ticks * f.quantity_minor
      END AS delta_minor
    FROM fills f
    JOIN latest_account_state las
      ON las.account_id = f.maker_account_id
    WHERE f.clearing_epoch IS NULL OR f.clearing_epoch > las.last_applied_epoch
  ) contributions
  GROUP BY account_id
)
UPDATE account_risk_state rs
SET
  delta_pending_trades_minor = COALESCE(pfd.pending_delta_minor, 0),
  updated_at = now()
FROM latest_account_state las
LEFT JOIN pending_fill_deltas pfd
  ON pfd.account_id = las.account_id
WHERE rs.account_id = las.account_id
  AND rs.delta_pending_trades_minor <> COALESCE(pfd.pending_delta_minor, 0);
