-- M8: user registration metadata for self-serve username -> account/party mapping.

CREATE TABLE IF NOT EXISTS user_registrations (
  username TEXT PRIMARY KEY,
  account_id TEXT NOT NULL UNIQUE,
  owner_party TEXT NOT NULL UNIQUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK (length(trim(username)) > 0),
  CHECK (length(trim(account_id)) > 0),
  CHECK (length(trim(owner_party)) > 0)
);
