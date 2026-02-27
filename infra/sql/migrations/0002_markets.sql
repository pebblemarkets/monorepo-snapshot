CREATE TABLE IF NOT EXISTS markets (
  contract_id TEXT PRIMARY KEY,
  market_id TEXT NOT NULL,
  committee_party TEXT NOT NULL,
  creator_party TEXT NOT NULL,
  question TEXT NOT NULL,
  outcomes JSONB NOT NULL,
  status TEXT NOT NULL,
  resolved_outcome TEXT,
  created_at TIMESTAMPTZ NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  last_offset BIGINT NOT NULL
);

CREATE INDEX IF NOT EXISTS markets_active_idx ON markets(active);
CREATE INDEX IF NOT EXISTS markets_market_id_idx ON markets(market_id);

