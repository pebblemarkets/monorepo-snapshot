-- M7: shared account advisory lock namespace capability registry.

CREATE TABLE IF NOT EXISTS account_lock_capabilities (
  service_name TEXT PRIMARY KEY,
  lock_namespace INT NOT NULL,
  active_withdrawals_enabled BOOLEAN NOT NULL DEFAULT FALSE,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS account_lock_capabilities_namespace_idx
  ON account_lock_capabilities(lock_namespace);
