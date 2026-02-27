-- M7: idempotency marker for exactly-once withdrawal reservation release.

CREATE TABLE IF NOT EXISTS withdrawal_reservation_releases (
  account_id TEXT NOT NULL,
  withdrawal_id TEXT NOT NULL,
  terminal_ref TEXT NOT NULL,
  release_reason TEXT NOT NULL,
  amount_minor BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

  PRIMARY KEY (account_id, withdrawal_id),
  CHECK (amount_minor > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS withdrawal_reservation_releases_terminal_ref_uniq
  ON withdrawal_reservation_releases(terminal_ref);
