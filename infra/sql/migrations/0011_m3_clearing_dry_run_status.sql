-- M3: introduce a distinct terminal status for dry-run clearing rows.
--
-- We keep apply-path semantics unchanged; this only affects read-only dry-run markers.

DO $$
DECLARE
  c RECORD;
BEGIN
  -- Canonical name created by 0010 on most databases.
  ALTER TABLE public.clearing_epochs
    DROP CONSTRAINT IF EXISTS clearing_epochs_status_check;

  -- Safety net for drifted/legacy names so re-runs stay idempotent.
  FOR c IN
    SELECT pc.conname
    FROM pg_constraint pc
    JOIN pg_class t ON t.oid = pc.conrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'public'
      AND t.relname = 'clearing_epochs'
      AND pc.contype = 'c'
      AND pg_get_constraintdef(pc.oid) ~* '\mstatus\M'
  LOOP
    EXECUTE format(
      'ALTER TABLE public.clearing_epochs DROP CONSTRAINT IF EXISTS %I',
      c.conname
    );
  END LOOP;
END $$;

ALTER TABLE clearing_epochs
  ADD CONSTRAINT clearing_epochs_status_check
  CHECK (
    status IN (
      'Anchoring',
      'Anchored',
      'DryRunReady',
      'Applying',
      'Applied',
      'Failed'
    )
  );

UPDATE clearing_epochs
SET status = 'DryRunReady'
WHERE status = 'Anchored'
  AND details->>'mode' = 'dry-run';
