-- 043: preserve verification intent as part of the immutable effect identity.
-- Older rows default to false; new reservations persist the normalized value so
-- duplicate identity checks cannot confuse a required verification effect with
-- an observational effect that happens to have the same status label.

ALTER TABLE fornix.operation_effects
  ADD COLUMN IF NOT EXISTS verification_required BOOLEAN NOT NULL DEFAULT FALSE;
