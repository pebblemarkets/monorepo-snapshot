-- M2: additional mock token standard projections required by the off-ledger treasury service.

-- --------------------
-- Mock token standard (wizardcat-token-standard)
-- --------------------

CREATE TABLE IF NOT EXISTS token_holding_claims (
  contract_id TEXT PRIMARY KEY,
  issuer_party TEXT NOT NULL,
  owner_party TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  amount_minor BIGINT NOT NULL,
  origin_instruction_cid TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS token_holding_claims_owner_instrument_active_idx
  ON token_holding_claims(owner_party, instrument_admin, instrument_id)
  WHERE active = TRUE;

CREATE INDEX IF NOT EXISTS token_holding_claims_origin_idx
  ON token_holding_claims(origin_instruction_cid);

CREATE TABLE IF NOT EXISTS token_wallets (
  contract_id TEXT PRIMARY KEY,
  owner_party TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS token_wallets_owner_instrument_active_idx
  ON token_wallets(owner_party, instrument_admin, instrument_id)
  WHERE active = TRUE;

