-- Persist the exact next eligibility time for rate-limited admission
-- decisions. Historical rows remain NULL: their past rolling-window state
-- cannot be reconstructed safely.
ALTER TABLE fornix.operation_admission_decisions
  ADD COLUMN IF NOT EXISTS retry_at TIMESTAMPTZ;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'operation_admission_retry_at_valid'
      AND conrelid = 'fornix.operation_admission_decisions'::regclass
  ) THEN
    ALTER TABLE fornix.operation_admission_decisions
      ADD CONSTRAINT operation_admission_retry_at_valid
      CHECK (retry_at IS NULL OR (status = 'denied' AND reason_code = 'rate_limit_exceeded')) NOT VALID;
  END IF;
END $$;
