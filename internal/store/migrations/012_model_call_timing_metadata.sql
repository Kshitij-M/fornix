-- Additive model-call observability fields. Kept separate so migration 011
-- remains checksum-stable for databases that already applied it.
ALTER TABLE fornix.model_calls
  ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE fornix.model_calls
  ADD COLUMN IF NOT EXISTS duration_ms BIGINT NOT NULL DEFAULT 0;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conrelid = 'fornix.model_calls'::regclass
      AND conname = 'model_calls_duration_nonnegative'
  ) THEN
    ALTER TABLE fornix.model_calls
      ADD CONSTRAINT model_calls_duration_nonnegative CHECK (duration_ms >= 0);
  END IF;
END $$;
