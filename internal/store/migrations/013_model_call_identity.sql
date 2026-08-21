-- Additive first-class request identity fields. The original ledger and its
-- timing/metadata extension remain checksum-stable.
ALTER TABLE fornix.model_calls
  ADD COLUMN IF NOT EXISTS schema_version INTEGER NOT NULL DEFAULT 1;

ALTER TABLE fornix.model_calls
  ADD COLUMN IF NOT EXISTS causation_id TEXT NOT NULL DEFAULT '';

ALTER TABLE fornix.model_calls
  ADD COLUMN IF NOT EXISTS correlation_id TEXT NOT NULL DEFAULT '';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'fornix.model_calls'::regclass
      AND conname = 'model_calls_schema_version_positive'
  ) THEN
    ALTER TABLE fornix.model_calls
      ADD CONSTRAINT model_calls_schema_version_positive CHECK (schema_version > 0);
  END IF;
END $$;
