-- Align ledger_offsets.ledger_offset with the indexer's BIGINT usage.
--
-- Historical local DBs may still have this column as TEXT from early scaffolding.
-- Convert safely and fail fast if any non-numeric value exists.

DO $$
DECLARE
  v_data_type TEXT;
BEGIN
  SELECT data_type
    INTO v_data_type
  FROM information_schema.columns
  WHERE table_schema = 'public'
    AND table_name = 'ledger_offsets'
    AND column_name = 'ledger_offset';

  IF v_data_type = 'text' THEN
    IF EXISTS (
      SELECT 1
      FROM ledger_offsets
      WHERE ledger_offset !~ '^-?[0-9]+$'
      LIMIT 1
    ) THEN
      RAISE EXCEPTION
        'ledger_offsets.ledger_offset contains non-numeric values and cannot be cast to BIGINT';
    END IF;

    ALTER TABLE ledger_offsets
      ALTER COLUMN ledger_offset TYPE BIGINT
      USING ledger_offset::BIGINT;
  END IF;
END $$;
