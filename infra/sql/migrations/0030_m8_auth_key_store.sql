-- M8: runtime API key store (DB-backed) for user/admin auth.
--
-- This enables key registration without API process restarts while keeping
-- lookups hot via in-memory cache in the API service.

CREATE TABLE IF NOT EXISTS user_api_keys (
  api_key TEXT PRIMARY KEY,
  account_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK (length(trim(api_key)) > 0),
  CHECK (length(trim(account_id)) > 0)
);

CREATE INDEX IF NOT EXISTS user_api_keys_account_id_idx
  ON user_api_keys (account_id);

CREATE TABLE IF NOT EXISTS admin_api_keys (
  admin_key TEXT PRIMARY KEY,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK (length(trim(admin_key)) > 0)
);
