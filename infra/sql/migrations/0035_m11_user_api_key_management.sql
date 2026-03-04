-- M8: managed user API keys (metadata + revocation) for self-serve key lifecycle.

ALTER TABLE user_api_keys
  ADD COLUMN IF NOT EXISTS key_id TEXT;

UPDATE user_api_keys
SET key_id = 'uak-' || md5(api_key)
WHERE key_id IS NULL;

ALTER TABLE user_api_keys
  ALTER COLUMN key_id SET NOT NULL;

ALTER TABLE user_api_keys
  ADD COLUMN IF NOT EXISTS label TEXT,
  ADD COLUMN IF NOT EXISTS last_used_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ;

ALTER TABLE user_api_keys
  DROP CONSTRAINT IF EXISTS user_api_keys_key_id_nonempty;

ALTER TABLE user_api_keys
  ADD CONSTRAINT user_api_keys_key_id_nonempty CHECK (length(trim(key_id)) > 0);

ALTER TABLE user_api_keys
  DROP CONSTRAINT IF EXISTS user_api_keys_label_len;

ALTER TABLE user_api_keys
  ADD CONSTRAINT user_api_keys_label_len CHECK (label IS NULL OR length(label) <= 64);

CREATE UNIQUE INDEX IF NOT EXISTS user_api_keys_key_id_idx
  ON user_api_keys (key_id);

CREATE INDEX IF NOT EXISTS user_api_keys_account_active_created_idx
  ON user_api_keys (account_id, created_at DESC)
  WHERE revoked_at IS NULL;

CREATE INDEX IF NOT EXISTS user_api_keys_revoked_at_idx
  ON user_api_keys (revoked_at);
