-- M6: durable market-resolution settlement jobs and computed account deltas.

CREATE TABLE IF NOT EXISTS market_settlement_jobs (
  market_id TEXT PRIMARY KEY,
  market_contract_id TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  resolved_outcome TEXT NOT NULL,
  payout_per_share_minor BIGINT NOT NULL,
  state TEXT NOT NULL,
  target_epoch BIGINT,
  error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK (payout_per_share_minor > 0),
  CHECK (
    state IN (
      'Pending',
      'Computing',
      'QueuedForClearing',
      'AwaitingApply',
      'Applied',
      'Finalized',
      'Failed'
    )
  )
);

CREATE INDEX IF NOT EXISTS market_settlement_jobs_state_idx
  ON market_settlement_jobs(state, updated_at ASC, created_at ASC);

CREATE INDEX IF NOT EXISTS market_settlement_jobs_target_epoch_idx
  ON market_settlement_jobs(target_epoch)
  WHERE target_epoch IS NOT NULL;

CREATE TABLE IF NOT EXISTS market_settlement_deltas (
  market_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  delta_minor BIGINT NOT NULL,
  source_position_count INT NOT NULL DEFAULT 0,
  assigned_epoch BIGINT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (market_id, account_id),
  FOREIGN KEY (market_id) REFERENCES market_settlement_jobs(market_id) ON DELETE CASCADE,
  CHECK (source_position_count >= 0)
);

CREATE INDEX IF NOT EXISTS market_settlement_deltas_unassigned_idx
  ON market_settlement_deltas(instrument_admin, instrument_id, assigned_epoch, market_id, account_id);

CREATE INDEX IF NOT EXISTS market_settlement_deltas_market_idx
  ON market_settlement_deltas(market_id, account_id);

CREATE TABLE IF NOT EXISTS market_settlement_events (
  id BIGSERIAL PRIMARY KEY,
  market_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  FOREIGN KEY (market_id) REFERENCES market_settlement_jobs(market_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS market_settlement_events_market_created_idx
  ON market_settlement_events(market_id, created_at DESC, id DESC);
