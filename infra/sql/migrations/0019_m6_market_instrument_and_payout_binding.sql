-- M6: explicit market-level instrument + payout policy binding.

ALTER TABLE markets
  ADD COLUMN IF NOT EXISTS instrument_admin TEXT;

ALTER TABLE markets
  ADD COLUMN IF NOT EXISTS instrument_id TEXT;

ALTER TABLE markets
  ADD COLUMN IF NOT EXISTS payout_per_share_minor BIGINT;

-- Keep successor lifecycle rows consistent with predecessor rows by market_id.
WITH per_market AS (
  SELECT DISTINCT ON (market_id)
    market_id,
    instrument_admin,
    instrument_id,
    payout_per_share_minor
  FROM markets
  WHERE instrument_admin IS NOT NULL
    AND instrument_id IS NOT NULL
    AND payout_per_share_minor IS NOT NULL
  ORDER BY market_id, created_at DESC, contract_id DESC
)
UPDATE markets m
SET
  instrument_admin = COALESCE(m.instrument_admin, pm.instrument_admin),
  instrument_id = COALESCE(m.instrument_id, pm.instrument_id),
  payout_per_share_minor = COALESCE(m.payout_per_share_minor, pm.payout_per_share_minor)
FROM per_market pm
WHERE m.market_id = pm.market_id
  AND (
    m.instrument_admin IS NULL
    OR m.instrument_id IS NULL
    OR m.payout_per_share_minor IS NULL
  );

-- For first-generation market rows, derive instrument from latest active token policy.
WITH default_token AS (
  SELECT instrument_admin, instrument_id
  FROM token_configs
  WHERE active = TRUE
  ORDER BY created_at DESC, contract_id DESC
  LIMIT 1
)
UPDATE markets m
SET
  instrument_admin = COALESCE(m.instrument_admin, dt.instrument_admin),
  instrument_id = COALESCE(m.instrument_id, dt.instrument_id),
  payout_per_share_minor = COALESCE(m.payout_per_share_minor, 100)
FROM default_token dt
WHERE m.instrument_admin IS NULL
  OR m.instrument_id IS NULL
  OR m.payout_per_share_minor IS NULL;

-- Payout default for any row already carrying instrument identity.
UPDATE markets
SET payout_per_share_minor = 100
WHERE payout_per_share_minor IS NULL
  AND instrument_admin IS NOT NULL
  AND instrument_id IS NOT NULL;

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM markets
    WHERE instrument_admin IS NULL
       OR instrument_id IS NULL
       OR payout_per_share_minor IS NULL
  ) THEN
    RAISE EXCEPTION
      'markets backfill failed: instrument/payout fields remain NULL; ensure token_configs exists or prefill markets before this migration';
  END IF;
END $$;

ALTER TABLE markets
  ALTER COLUMN instrument_admin SET NOT NULL;

ALTER TABLE markets
  ALTER COLUMN instrument_id SET NOT NULL;

ALTER TABLE markets
  ALTER COLUMN payout_per_share_minor SET NOT NULL;

DO $$
DECLARE
  c RECORD;
BEGIN
  ALTER TABLE public.markets
    DROP CONSTRAINT IF EXISTS markets_payout_per_share_minor_positive_check;

  FOR c IN
    SELECT pc.conname
    FROM pg_constraint pc
    JOIN pg_class t ON t.oid = pc.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'public'
      AND t.relname = 'markets'
      AND pc.contype = 'c'
      AND pg_get_constraintdef(pc.oid) ~* '\mpayout_per_share_minor\M'
  LOOP
    EXECUTE format(
      'ALTER TABLE public.markets DROP CONSTRAINT IF EXISTS %I',
      c.conname
    );
  END LOOP;
END $$;

ALTER TABLE markets
  ADD CONSTRAINT markets_payout_per_share_minor_positive_check
  CHECK (payout_per_share_minor > 0);

CREATE INDEX IF NOT EXISTS markets_instrument_market_idx
  ON markets(instrument_admin, instrument_id, market_id);
