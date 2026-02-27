-- M7: optional unified writer lane for per-account serialized mutations.

CREATE TABLE IF NOT EXISTS account_write_intents (
  intent_id UUID PRIMARY KEY,
  account_id TEXT NOT NULL,
  intent_type TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  payload JSONB NOT NULL,
  state TEXT NOT NULL,
  attempt_count INT NOT NULL DEFAULT 0,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK (
    intent_type IN (
      'ClearingDelta',
      'TreasuryDebit',
      'TreasuryCredit',
      'WithdrawalRefund'
    )
  ),
  CHECK (state IN ('Queued', 'Submitting', 'Committed', 'Failed')),
  CHECK (attempt_count >= 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS account_write_intents_account_idempotency_uniq
  ON account_write_intents(account_id, idempotency_key);

CREATE INDEX IF NOT EXISTS account_write_intents_state_created_idx
  ON account_write_intents(state, created_at);
