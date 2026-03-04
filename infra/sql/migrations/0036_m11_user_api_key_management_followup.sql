-- M11 follow-up: normalize user API key ids and preserve forward migration compatibility.
-- Note: 0035 backfilled with a `uak-` prefix; runtime and API contract use `uak_`.

UPDATE user_api_keys
SET key_id = 'uak_' || substring(key_id from 5)
WHERE key_id LIKE 'uak-%';
