-- M12 follow-up: track one-time bootstrap API key reveal on OAuth user sessions.

ALTER TABLE user_sessions
  ADD COLUMN IF NOT EXISTS bootstrap_key_id TEXT,
  ADD COLUMN IF NOT EXISTS bootstrap_key_revealed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS user_sessions_bootstrap_key_idx
  ON user_sessions (bootstrap_key_id)
  WHERE bootstrap_key_id IS NOT NULL;
