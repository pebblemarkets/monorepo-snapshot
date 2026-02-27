-- M3: deterministic fill sequencing for replay and canonical ordering.

ALTER TABLE fills
  ADD COLUMN IF NOT EXISTS fill_sequence BIGINT;

WITH ranked AS (
  SELECT
    fill_id,
    ROW_NUMBER() OVER (
      PARTITION BY market_id
      ORDER BY matched_at ASC, fill_id ASC
    ) - 1 AS fill_sequence
  FROM fills
  WHERE fill_sequence IS NULL
)
UPDATE fills f
SET fill_sequence = ranked.fill_sequence
FROM ranked
WHERE f.fill_id = ranked.fill_id;

ALTER TABLE fills
  ALTER COLUMN fill_sequence SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS fills_market_sequence_uniq
  ON fills(market_id, fill_sequence);
