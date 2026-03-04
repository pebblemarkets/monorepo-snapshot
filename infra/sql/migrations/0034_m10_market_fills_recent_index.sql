-- Improve /markets/:market_id/fills performance under sustained polling.
-- Existing fills_market_idx starts with (market_id, outcome, ...), which is
-- suboptimal for queries that do not filter by outcome.

CREATE INDEX IF NOT EXISTS fills_market_recent_idx
  ON fills(market_id, matched_at DESC, fill_sequence DESC);
