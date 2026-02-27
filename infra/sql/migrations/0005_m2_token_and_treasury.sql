-- M2: mock CIP-56 token projections + treasury deposit/withdraw projections.

-- Expand TokenConfig projection to include the full on-ledger contract fields used by M2.
ALTER TABLE token_configs
  ADD COLUMN IF NOT EXISTS committee_party TEXT,
  ADD COLUMN IF NOT EXISTS registry_url TEXT,
  ADD COLUMN IF NOT EXISTS target_utxo_count_min BIGINT,
  ADD COLUMN IF NOT EXISTS target_utxo_count_max BIGINT,
  ADD COLUMN IF NOT EXISTS dust_threshold_minor BIGINT,
  ADD COLUMN IF NOT EXISTS withdrawal_fee_headroom_minor BIGINT,
  ADD COLUMN IF NOT EXISTS unexpected_fee_buffer_minor BIGINT,
  ADD COLUMN IF NOT EXISTS allowed_deposit_pending_status_classes JSONB,
  ADD COLUMN IF NOT EXISTS allowed_withdrawal_pending_status_classes JSONB,
  ADD COLUMN IF NOT EXISTS allowed_cancel_pending_status_classes JSONB,
  ADD COLUMN IF NOT EXISTS auditor_party TEXT;

-- --------------------
-- Mock token standard (wizardcat-token-standard)
-- --------------------

CREATE TABLE IF NOT EXISTS token_holdings (
  contract_id TEXT PRIMARY KEY,
  owner_party TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  amount_minor BIGINT NOT NULL,
  lock_status_class TEXT,
  lock_expires_at TIMESTAMPTZ,
  lock_owner_party TEXT,
  origin_instruction_cid TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS token_holdings_owner_instrument_active_idx
  ON token_holdings(owner_party, instrument_admin, instrument_id)
  WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS token_holdings_origin_idx
  ON token_holdings(origin_instruction_cid);

CREATE TABLE IF NOT EXISTS token_transfer_instructions (
  contract_id TEXT PRIMARY KEY,
  lineage_root_instruction_cid TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  sender_party TEXT NOT NULL,
  receiver_party TEXT NOT NULL,
  amount_minor BIGINT NOT NULL,
  metadata JSONB NOT NULL,
  reason TEXT NOT NULL,
  output TEXT NOT NULL,
  status_class TEXT,
  original_instruction_cid TEXT,
  step_seq BIGINT NOT NULL,
  pending_actions JSONB NOT NULL,
  locked_holding_cid TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS token_transfer_instructions_root_active_idx
  ON token_transfer_instructions(lineage_root_instruction_cid)
  WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS token_transfer_instructions_parties_idx
  ON token_transfer_instructions(sender_party, receiver_party);

CREATE INDEX IF NOT EXISTS token_transfer_instructions_original_idx
  ON token_transfer_instructions(original_instruction_cid);

CREATE TABLE IF NOT EXISTS token_transfer_instruction_latest (
  lineage_root_instruction_cid TEXT PRIMARY KEY,
  contract_id TEXT NOT NULL UNIQUE,
  last_offset BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- --------------------
-- Treasury (pebble: Pebble.Treasury)
-- --------------------

CREATE TABLE IF NOT EXISTS deposit_requests (
  contract_id TEXT PRIMARY KEY,
  committee_party TEXT NOT NULL,
  owner_party TEXT NOT NULL,
  account_id TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  deposit_id TEXT NOT NULL,
  token_config_cid TEXT NOT NULL,
  account_state_cid TEXT NOT NULL,
  offer_instruction_cid TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS deposit_requests_account_idx ON deposit_requests(account_id);
CREATE INDEX IF NOT EXISTS deposit_requests_owner_idx ON deposit_requests(owner_party);

CREATE TABLE IF NOT EXISTS deposit_pendings (
  contract_id TEXT PRIMARY KEY,
  committee_party TEXT NOT NULL,
  owner_party TEXT NOT NULL,
  account_id TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  deposit_id TEXT NOT NULL,
  token_config_cid TEXT NOT NULL,
  lineage_root_instruction_cid TEXT NOT NULL,
  current_instruction_cid TEXT NOT NULL,
  step_seq BIGINT NOT NULL,
  expected_amount_minor BIGINT NOT NULL,
  metadata JSONB NOT NULL,
  reason TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS deposit_pendings_account_active_idx
  ON deposit_pendings(account_id)
  WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS deposit_pendings_owner_deposit_active_idx
  ON deposit_pendings(owner_party, instrument_admin, instrument_id, deposit_id)
  WHERE active = TRUE;

CREATE TABLE IF NOT EXISTS deposit_pending_latest (
  owner_party TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  deposit_id TEXT NOT NULL,
  contract_id TEXT NOT NULL,
  last_offset BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (owner_party, instrument_admin, instrument_id, deposit_id)
);

CREATE TABLE IF NOT EXISTS deposit_receipts (
  contract_id TEXT PRIMARY KEY,
  committee_party TEXT NOT NULL,
  owner_party TEXT NOT NULL,
  account_id TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  deposit_id TEXT NOT NULL,
  lineage_root_instruction_cid TEXT NOT NULL,
  terminal_instruction_cid TEXT NOT NULL,
  credited_minor BIGINT NOT NULL,
  receiver_holding_cids JSONB NOT NULL,
  metadata JSONB NOT NULL,
  reason TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS deposit_receipts_account_idx ON deposit_receipts(account_id);
CREATE INDEX IF NOT EXISTS deposit_receipts_owner_idx ON deposit_receipts(owner_party);

CREATE TABLE IF NOT EXISTS withdrawal_requests (
  contract_id TEXT PRIMARY KEY,
  committee_party TEXT NOT NULL,
  owner_party TEXT NOT NULL,
  account_id TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  withdrawal_id TEXT NOT NULL,
  amount_minor BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS withdrawal_requests_account_idx ON withdrawal_requests(account_id);
CREATE INDEX IF NOT EXISTS withdrawal_requests_owner_idx ON withdrawal_requests(owner_party);

CREATE TABLE IF NOT EXISTS withdrawal_pendings (
  contract_id TEXT PRIMARY KEY,
  committee_party TEXT NOT NULL,
  owner_party TEXT NOT NULL,
  account_id TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  withdrawal_id TEXT NOT NULL,
  amount_minor BIGINT NOT NULL,
  token_config_cid TEXT NOT NULL,
  lineage_root_instruction_cid TEXT NOT NULL,
  current_instruction_cid TEXT NOT NULL,
  step_seq BIGINT NOT NULL,
  pending_state TEXT NOT NULL,
  pending_actions JSONB NOT NULL,
  metadata JSONB NOT NULL,
  reason TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS withdrawal_pendings_account_active_idx
  ON withdrawal_pendings(account_id)
  WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS withdrawal_pendings_owner_withdrawal_active_idx
  ON withdrawal_pendings(owner_party, instrument_admin, instrument_id, withdrawal_id)
  WHERE active = TRUE;

CREATE TABLE IF NOT EXISTS withdrawal_pending_latest (
  owner_party TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  withdrawal_id TEXT NOT NULL,
  contract_id TEXT NOT NULL,
  last_offset BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (owner_party, instrument_admin, instrument_id, withdrawal_id)
);

CREATE TABLE IF NOT EXISTS withdrawal_receipts (
  contract_id TEXT PRIMARY KEY,
  committee_party TEXT NOT NULL,
  owner_party TEXT NOT NULL,
  account_id TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  withdrawal_id TEXT NOT NULL,
  amount_minor BIGINT NOT NULL,
  status TEXT NOT NULL,
  lineage_root_instruction_cid TEXT NOT NULL,
  terminal_instruction_cid TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS withdrawal_receipts_account_idx ON withdrawal_receipts(account_id);
CREATE INDEX IF NOT EXISTS withdrawal_receipts_owner_idx ON withdrawal_receipts(owner_party);

CREATE TABLE IF NOT EXISTS quarantined_holdings (
  contract_id TEXT PRIMARY KEY,
  committee_party TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  holding_cid TEXT NOT NULL,
  discovered_at TIMESTAMPTZ NOT NULL,
  reason TEXT NOT NULL,
  related_account_id TEXT,
  related_deposit_id TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS quarantined_holdings_instrument_active_idx
  ON quarantined_holdings(instrument_admin, instrument_id)
  WHERE active = TRUE;

