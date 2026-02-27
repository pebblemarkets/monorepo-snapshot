ALTER TABLE market_metadata
  ADD COLUMN IF NOT EXISTS card_background_image_url TEXT,
  ADD COLUMN IF NOT EXISTS hero_background_image_url TEXT,
  ADD COLUMN IF NOT EXISTS thumbnail_image_url TEXT;
