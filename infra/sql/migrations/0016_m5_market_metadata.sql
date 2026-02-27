CREATE TABLE IF NOT EXISTS market_metadata (
  market_id TEXT PRIMARY KEY,
  category TEXT NOT NULL DEFAULT 'General',
  tags JSONB NOT NULL DEFAULT '[]'::jsonb,
  featured BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS market_metadata_category_idx
  ON market_metadata(category);

CREATE INDEX IF NOT EXISTS market_metadata_featured_idx
  ON market_metadata(featured);
