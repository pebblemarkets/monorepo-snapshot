-- M7: account liquidity reservations for withdrawal admission.

CREATE TABLE IF NOT EXISTS account_liquidity_state (
  account_id TEXT PRIMARY KEY,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  pending_withdrawals_reserved_minor BIGINT NOT NULL DEFAULT 0,
  version BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK (pending_withdrawals_reserved_minor >= 0),
  CHECK (version >= 0)
);

CREATE INDEX IF NOT EXISTS account_liquidity_state_instrument_idx
  ON account_liquidity_state(instrument_admin, instrument_id);
