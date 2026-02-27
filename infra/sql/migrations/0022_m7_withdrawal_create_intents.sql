-- M7: idempotent, recoverable withdrawal create submission saga state.

CREATE TABLE IF NOT EXISTS withdrawal_create_intents (
  intent_id UUID PRIMARY KEY,
  idempotency_key TEXT NOT NULL,
  account_id TEXT NOT NULL,
  instrument_admin TEXT NOT NULL,
  instrument_id TEXT NOT NULL,
  withdrawal_id TEXT NOT NULL,
  amount_minor BIGINT NOT NULL,
  command_id TEXT,
  request_contract_id TEXT,
  state TEXT NOT NULL,
  last_error TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK (amount_minor > 0),
  CHECK (
    state IN (
      'Prepared',
      'Submitted',
      'Committed',
      'RejectedIneligible',
      'FailedSubmitUnknown',
      'Failed'
    )
  )
);

CREATE UNIQUE INDEX IF NOT EXISTS withdrawal_create_intents_idempotency_uniq
  ON withdrawal_create_intents(idempotency_key);

CREATE UNIQUE INDEX IF NOT EXISTS withdrawal_create_intents_account_withdrawal_uniq
  ON withdrawal_create_intents(account_id, withdrawal_id);

CREATE UNIQUE INDEX IF NOT EXISTS withdrawal_create_intents_command_uniq
  ON withdrawal_create_intents(command_id)
  WHERE command_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS withdrawal_create_intents_state_created_idx
  ON withdrawal_create_intents(state, created_at);
