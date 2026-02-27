-- M6: terminalized position bookkeeping for resolved markets.

ALTER TABLE positions
  ADD COLUMN IF NOT EXISTS settled BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE positions
  ADD COLUMN IF NOT EXISTS settled_outcome TEXT;

ALTER TABLE positions
  ADD COLUMN IF NOT EXISTS settled_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS positions_market_settled_idx
  ON positions(market_id, settled, outcome);

CREATE TABLE IF NOT EXISTS position_settlement_snapshots (
  id BIGSERIAL PRIMARY KEY,
  market_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  outcome TEXT NOT NULL,
  net_quantity_minor_before BIGINT NOT NULL,
  avg_entry_price_ticks_before BIGINT,
  realized_pnl_minor_before BIGINT NOT NULL,
  resolved_outcome TEXT NOT NULL,
  settlement_delta_minor BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (market_id, account_id, outcome)
);

CREATE INDEX IF NOT EXISTS position_settlement_snapshots_market_idx
  ON position_settlement_snapshots(market_id, created_at DESC, id DESC);
