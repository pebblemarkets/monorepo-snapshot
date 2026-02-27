-- M2: off-ledger treasury durable state (op-log + inventory reservations).
--
-- These tables are derived/operational state owned by the treasury service. They are safe to wipe
-- against a fresh local sandbox, but are intended to be durable in a long-running environment.

CREATE TABLE IF NOT EXISTS treasury_ops (
  op_id TEXT PRIMARY KEY,
  op_type TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  owner_party TEXT,
  account_id TEXT,
  amount_minor BIGINT,
  state TEXT NOT NULL,
  step_seq BIGINT NOT NULL DEFAULT 0,
  command_id TEXT NOT NULL,

  -- Correlation / evidence
  observed_offset BIGINT,
  observed_update_id TEXT,
  terminal_observed_offset BIGINT,
  terminal_update_id TEXT,
  lineage_root_instruction_cid TEXT,
  current_instruction_cid TEXT,
  input_holding_cids JSONB NOT NULL DEFAULT '[]',
  receiver_holding_cids JSONB NOT NULL DEFAULT '[]',
  extra JSONB NOT NULL DEFAULT '{}'::jsonb,

  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS treasury_ops_state_idx ON treasury_ops(state);
CREATE INDEX IF NOT EXISTS treasury_ops_type_idx ON treasury_ops(op_type);
CREATE INDEX IF NOT EXISTS treasury_ops_account_idx ON treasury_ops(account_id);

CREATE TABLE IF NOT EXISTS treasury_op_events (
  id BIGSERIAL PRIMARY KEY,
  op_id TEXT NOT NULL,
  state TEXT NOT NULL,
  detail JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS treasury_op_events_op_idx ON treasury_op_events(op_id);

-- Per-holding reservations (enforced with row-level locking + unique constraint).
-- This is the minimal state required to safely select UTXOs deterministically.
CREATE TABLE IF NOT EXISTS treasury_holding_reservations (
  holding_cid TEXT NOT NULL,
  op_id TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  state TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (holding_cid, op_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS treasury_holding_reservations_one_active_idx
  ON treasury_holding_reservations(holding_cid)
  WHERE state IN ('Reserved', 'Submitting');

CREATE INDEX IF NOT EXISTS treasury_holding_reservations_op_idx
  ON treasury_holding_reservations(op_id);

