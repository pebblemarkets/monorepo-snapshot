-- M7: projection for wizardcat operator-accept policies used by one-shot withdrawals.

CREATE TABLE IF NOT EXISTS operator_accept_policies (
  contract_id TEXT PRIMARY KEY,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  authorizer_party TEXT NOT NULL,
  operator_party TEXT NOT NULL,
  mode TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS operator_accept_policies_lookup_idx
  ON operator_accept_policies(instrument_admin, instrument_id, authorizer_party, operator_party, mode)
  WHERE active = TRUE;
