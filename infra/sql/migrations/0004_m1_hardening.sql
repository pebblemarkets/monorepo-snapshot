-- M1 hardening: audit metadata + uniqueness constraints.

-- Audit metadata (correlation only; do not use for correctness gating).
CREATE TABLE IF NOT EXISTS ledger_updates (
  ledger_offset BIGINT PRIMARY KEY,
  update_id TEXT NOT NULL,
  record_time TIMESTAMPTZ NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS ledger_updates_record_time_idx ON ledger_updates(record_time);
CREATE INDEX IF NOT EXISTS ledger_updates_update_id_idx ON ledger_updates(update_id);

-- Uniqueness constraints

-- Accounts: at most one non-Closed active account per (owner, instrument).
CREATE UNIQUE INDEX IF NOT EXISTS account_refs_active_owner_instrument_uniq
  ON account_refs(owner_party, instrument_admin, instrument_id)
  WHERE active = TRUE AND status <> 'Closed';

-- Reference configs: at most one active config per instrument.
CREATE UNIQUE INDEX IF NOT EXISTS token_configs_active_instrument_uniq
  ON token_configs(instrument_admin, instrument_id)
  WHERE active = TRUE;

CREATE UNIQUE INDEX IF NOT EXISTS fee_schedules_active_instrument_uniq
  ON fee_schedules(instrument_admin, instrument_id)
  WHERE active = TRUE;

CREATE UNIQUE INDEX IF NOT EXISTS oracle_configs_active_instrument_uniq
  ON oracle_configs(instrument_admin, instrument_id)
  WHERE active = TRUE;

-- Clearing head: singleton per instrument.
CREATE UNIQUE INDEX IF NOT EXISTS clearing_heads_active_instrument_uniq
  ON clearing_heads(instrument_admin, instrument_id)
  WHERE active = TRUE;
