-- M2: indexer-maintained ingestion watermark(s) used for treasury spend-safety gating.

CREATE TABLE IF NOT EXISTS submission_path_watermarks (
  submission_path TEXT PRIMARY KEY,
  watermark_offset BIGINT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

