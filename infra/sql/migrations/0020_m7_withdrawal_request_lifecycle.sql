-- M7: withdrawal request lifecycle state to prevent silent queue starvation.

ALTER TABLE withdrawal_requests
  ADD COLUMN IF NOT EXISTS request_state TEXT NOT NULL DEFAULT 'Queued',
  ADD COLUMN IF NOT EXISTS request_state_reason TEXT,
  ADD COLUMN IF NOT EXISTS eligibility_snapshot JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS last_evaluated_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'withdrawal_requests_request_state_check'
  ) THEN
    ALTER TABLE withdrawal_requests
      ADD CONSTRAINT withdrawal_requests_request_state_check
      CHECK (request_state IN (
        'Queued',
        'RejectedIneligible',
        'Processing',
        'Processed',
        'Failed',
        'Superseded'
      ));
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS withdrawal_requests_state_created_idx
  ON withdrawal_requests(request_state, created_at);

CREATE UNIQUE INDEX IF NOT EXISTS withdrawal_requests_active_pipeline_uniq
  ON withdrawal_requests(account_id, withdrawal_id)
  WHERE active = TRUE
    AND request_state IN ('Queued', 'Processing');

CREATE TABLE IF NOT EXISTS withdrawal_request_events (
  id BIGSERIAL PRIMARY KEY,
  contract_id TEXT,
  withdrawal_id TEXT NOT NULL,
  account_id TEXT NOT NULL,
  from_state TEXT,
  to_state TEXT NOT NULL,
  reason TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS withdrawal_request_events_withdrawal_idx
  ON withdrawal_request_events(account_id, withdrawal_id, created_at DESC, id DESC);
