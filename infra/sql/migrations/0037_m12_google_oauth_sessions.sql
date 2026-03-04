-- M12: Google OAuth identity mapping and user session storage.

CREATE TABLE IF NOT EXISTS user_google_identities (
  google_subject TEXT PRIMARY KEY,
  username TEXT NOT NULL,
  account_id TEXT NOT NULL,
  email TEXT NOT NULL,
  email_verified BOOLEAN NOT NULL DEFAULT FALSE,
  full_name TEXT,
  avatar_url TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_login_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  CHECK (length(trim(google_subject)) > 0),
  CHECK (length(trim(username)) > 0),
  CHECK (length(trim(account_id)) > 0),
  CHECK (length(trim(email)) > 0)
);

CREATE INDEX IF NOT EXISTS user_google_identities_account_id_idx
  ON user_google_identities (account_id);

CREATE INDEX IF NOT EXISTS user_google_identities_username_idx
  ON user_google_identities (username);

CREATE TABLE IF NOT EXISTS user_sessions (
  session_id TEXT PRIMARY KEY,
  session_token_hash TEXT NOT NULL UNIQUE,
  account_id TEXT NOT NULL,
  auth_source TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at TIMESTAMPTZ NOT NULL,
  last_seen_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ,
  user_agent TEXT,
  remote_addr TEXT,

  CHECK (length(trim(session_id)) > 0),
  CHECK (length(trim(session_token_hash)) > 0),
  CHECK (length(trim(account_id)) > 0),
  CHECK (length(trim(auth_source)) > 0)
);

CREATE INDEX IF NOT EXISTS user_sessions_account_active_created_idx
  ON user_sessions (account_id, created_at DESC)
  WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS user_sessions_expires_at_idx
  ON user_sessions (expires_at);

CREATE INDEX IF NOT EXISTS user_sessions_revoked_at_idx
  ON user_sessions (revoked_at);
