ALTER TABLE market_metadata
  ADD COLUMN IF NOT EXISTS resolution_time TIMESTAMPTZ;
