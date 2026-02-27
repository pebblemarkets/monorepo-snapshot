-- M1: core on-ledger state projections (refs, accounts, clearing).

-- Reference configs
CREATE TABLE IF NOT EXISTS token_configs (
  contract_id TEXT PRIMARY KEY,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  decimals INT NOT NULL,
  symbol TEXT NOT NULL,
  deposit_mode TEXT NOT NULL,
  inbound_requires_acceptance BOOLEAN NOT NULL,
  hard_max_inputs_per_transfer BIGINT NOT NULL,
  operational_max_inputs_per_transfer BIGINT NOT NULL,
  requires_deposit_id_metadata BOOLEAN NOT NULL,
  requires_original_instruction_cid BOOLEAN NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS token_configs_instrument_idx
  ON token_configs(instrument_admin, instrument_id);

CREATE TABLE IF NOT EXISTS fee_schedules (
  contract_id TEXT PRIMARY KEY,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  deposit_policy TEXT NOT NULL,
  withdrawal_policy TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS fee_schedules_instrument_idx
  ON fee_schedules(instrument_admin, instrument_id);

CREATE TABLE IF NOT EXISTS oracle_configs (
  contract_id TEXT PRIMARY KEY,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  mode TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS oracle_configs_instrument_idx
  ON oracle_configs(instrument_admin, instrument_id);

-- Accounts (no contract keys)
CREATE TABLE IF NOT EXISTS account_refs (
  contract_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  owner_party TEXT NOT NULL,
  committee_party TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  status TEXT NOT NULL,
  finalized_epoch BIGINT,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS account_refs_account_id_idx ON account_refs(account_id);
CREATE INDEX IF NOT EXISTS account_refs_owner_idx ON account_refs(owner_party);
CREATE INDEX IF NOT EXISTS account_refs_active_idx ON account_refs(active);

CREATE TABLE IF NOT EXISTS account_ref_latest (
  account_id TEXT PRIMARY KEY,
  contract_id TEXT NOT NULL,
  last_offset BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS account_states (
  contract_id TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  owner_party TEXT NOT NULL,
  committee_party TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  cleared_cash_minor BIGINT NOT NULL,
  last_applied_epoch BIGINT NOT NULL,
  last_applied_batch_anchor TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS account_states_account_id_idx ON account_states(account_id);
CREATE INDEX IF NOT EXISTS account_states_owner_idx ON account_states(owner_party);
CREATE INDEX IF NOT EXISTS account_states_active_idx ON account_states(active);

CREATE TABLE IF NOT EXISTS account_state_latest (
  account_id TEXT PRIMARY KEY,
  contract_id TEXT NOT NULL,
  last_offset BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Clearing chain
CREATE TABLE IF NOT EXISTS clearing_heads (
  contract_id TEXT PRIMARY KEY,
  committee_party TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  next_epoch BIGINT NOT NULL,
  prev_batch_hash TEXT NOT NULL,
  engine_version TEXT NOT NULL,
  last_batch_anchor TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS clearing_heads_instrument_idx
  ON clearing_heads(instrument_admin, instrument_id);
CREATE INDEX IF NOT EXISTS clearing_heads_active_idx
  ON clearing_heads(active);

CREATE TABLE IF NOT EXISTS clearing_head_latest (
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  contract_id TEXT NOT NULL,
  last_offset BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (instrument_admin, instrument_id)
);

CREATE TABLE IF NOT EXISTS batch_anchors (
  contract_id TEXT PRIMARY KEY,
  committee_party TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  epoch BIGINT NOT NULL,
  prev_batch_hash TEXT NOT NULL,
  batch_hash TEXT NOT NULL,
  engine_version TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL,
  UNIQUE (instrument_admin, instrument_id, epoch)
);

CREATE INDEX IF NOT EXISTS batch_anchors_instrument_epoch_idx
  ON batch_anchors(instrument_admin, instrument_id, epoch);

