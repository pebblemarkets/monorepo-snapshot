-- M3: off-ledger trading and epoch-clearing durable state.
--
-- These are application-owned tables (not ledger projections). They are designed
-- for deterministic matching, idempotent clearing application, and replayability.

CREATE TABLE IF NOT EXISTS orders (
  order_id TEXT PRIMARY KEY,
  market_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  owner_party TEXT NOT NULL,
  outcome TEXT NOT NULL,
  side TEXT NOT NULL,
  order_type TEXT NOT NULL,
  tif TEXT NOT NULL,
  nonce BIGINT NOT NULL,
  price_ticks BIGINT NOT NULL,
  quantity_minor BIGINT NOT NULL,
  remaining_minor BIGINT NOT NULL,
  locked_minor BIGINT NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  engine_version TEXT NOT NULL,
  submitted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK (side IN ('Buy', 'Sell')),
  CHECK (order_type IN ('Limit')),
  CHECK (tif IN ('GTC')),
  CHECK (nonce >= 0),
  CHECK (price_ticks > 0),
  CHECK (quantity_minor > 0),
  CHECK (remaining_minor >= 0),
  CHECK (locked_minor >= 0),
  CHECK (status IN ('Open', 'PartiallyFilled', 'Filled', 'Cancelled', 'Rejected'))
);

CREATE UNIQUE INDEX IF NOT EXISTS orders_account_nonce_uniq
  ON orders(account_id, nonce);

CREATE INDEX IF NOT EXISTS orders_open_book_idx
  ON orders(market_id, outcome, side, status, price_ticks, submitted_at, order_id);

CREATE INDEX IF NOT EXISTS orders_account_idx
  ON orders(account_id, submitted_at DESC);

CREATE TABLE IF NOT EXISTS fills (
  fill_id TEXT PRIMARY KEY,
  market_id TEXT NOT NULL,
  outcome TEXT NOT NULL,
  maker_order_id TEXT NOT NULL REFERENCES orders(order_id),
  taker_order_id TEXT NOT NULL REFERENCES orders(order_id),
  maker_account_id TEXT NOT NULL,
  taker_account_id TEXT NOT NULL,
  price_ticks BIGINT NOT NULL,
  quantity_minor BIGINT NOT NULL,
  engine_version TEXT NOT NULL,
  matched_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  clearing_epoch BIGINT,

  CHECK (price_ticks > 0),
  CHECK (quantity_minor > 0)
);

CREATE INDEX IF NOT EXISTS fills_market_idx
  ON fills(market_id, outcome, matched_at DESC, fill_id);

CREATE INDEX IF NOT EXISTS fills_maker_account_idx
  ON fills(maker_account_id, matched_at DESC);

CREATE INDEX IF NOT EXISTS fills_taker_account_idx
  ON fills(taker_account_id, matched_at DESC);

CREATE INDEX IF NOT EXISTS fills_clearing_epoch_idx
  ON fills(clearing_epoch, matched_at ASC);

CREATE TABLE IF NOT EXISTS positions (
  account_id TEXT NOT NULL,
  market_id TEXT NOT NULL,
  outcome TEXT NOT NULL,
  net_quantity_minor BIGINT NOT NULL DEFAULT 0,
  avg_entry_price_ticks BIGINT,
  realized_pnl_minor BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (account_id, market_id, outcome)
);

CREATE INDEX IF NOT EXISTS positions_market_idx
  ON positions(market_id, outcome);

CREATE TABLE IF NOT EXISTS account_risk_state (
  account_id TEXT PRIMARY KEY,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  delta_pending_trades_minor BIGINT NOT NULL DEFAULT 0,
  locked_open_orders_minor BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK (locked_open_orders_minor >= 0)
);

CREATE INDEX IF NOT EXISTS account_risk_state_instrument_idx
  ON account_risk_state(instrument_admin, instrument_id);

CREATE TABLE IF NOT EXISTS account_nonces (
  account_id TEXT PRIMARY KEY,
  last_nonce BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK (last_nonce >= 0)
);

CREATE TABLE IF NOT EXISTS clearing_epochs (
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  epoch BIGINT NOT NULL,
  prev_batch_hash TEXT NOT NULL,
  batch_hash TEXT NOT NULL,
  engine_version TEXT NOT NULL,
  anchor_contract_id TEXT,
  status TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finalized_at TIMESTAMPTZ,
  details JSONB NOT NULL DEFAULT '{}'::jsonb,

  PRIMARY KEY (instrument_admin, instrument_id, epoch),
  UNIQUE (anchor_contract_id),
  CHECK (epoch > 0),
  CHECK (status IN ('Anchoring', 'Anchored', 'Applying', 'Applied', 'Failed'))
);

CREATE INDEX IF NOT EXISTS clearing_epochs_status_idx
  ON clearing_epochs(status, started_at ASC);

CREATE TABLE IF NOT EXISTS clearing_epoch_deltas (
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  epoch BIGINT NOT NULL,
  account_id TEXT NOT NULL,
  delta_minor BIGINT NOT NULL,
  source_fill_count INT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (instrument_admin, instrument_id, epoch, account_id),
  CHECK (source_fill_count >= 0)
);

CREATE INDEX IF NOT EXISTS clearing_epoch_deltas_account_idx
  ON clearing_epoch_deltas(account_id, epoch DESC);

CREATE TABLE IF NOT EXISTS clearing_applies (
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  epoch BIGINT NOT NULL,
  account_id TEXT NOT NULL,
  batch_anchor_cid TEXT NOT NULL,
  account_state_before_cid TEXT,
  account_state_after_cid TEXT,
  delta_minor BIGINT NOT NULL,
  status TEXT NOT NULL,
  applied_offset BIGINT,
  applied_update_id TEXT,
  applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  error TEXT,

  PRIMARY KEY (instrument_admin, instrument_id, epoch, account_id),
  CHECK (status IN ('Applied', 'Skipped', 'Failed'))
);

CREATE INDEX IF NOT EXISTS clearing_applies_status_idx
  ON clearing_applies(status, applied_at DESC);

CREATE INDEX IF NOT EXISTS clearing_applies_account_idx
  ON clearing_applies(account_id, applied_at DESC);

CREATE TABLE IF NOT EXISTS trading_events (
  id BIGSERIAL PRIMARY KEY,
  aggregate_type TEXT NOT NULL,
  aggregate_id TEXT NOT NULL,
  sequence BIGINT NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  UNIQUE (aggregate_type, aggregate_id, sequence)
);

CREATE INDEX IF NOT EXISTS trading_events_created_idx
  ON trading_events(created_at DESC, id DESC);

