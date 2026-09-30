-- 042: harden external-effect recovery authority and idempotency.

ALTER TABLE fornix.operation_effect_transitions
  ADD COLUMN IF NOT EXISTS lease_kind TEXT NOT NULL DEFAULT 'operation';

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'operation_effect_transition_lease_kind_valid'
      AND conrelid = 'fornix.operation_effect_transitions'::regclass
  ) THEN
    ALTER TABLE fornix.operation_effect_transitions
      ADD CONSTRAINT operation_effect_transition_lease_kind_valid
      CHECK (lease_kind IN ('operation','effect'));
  END IF;
END $$;

ALTER TABLE fornix.operation_effect_state
  DROP CONSTRAINT IF EXISTS operation_effect_state_status_valid;
ALTER TABLE fornix.operation_effect_state
  ADD CONSTRAINT operation_effect_state_status_valid CHECK (
    state IN ('reserved','dispatching','dispatched','acknowledged',
      'verification_pending','verified','verification_failed',
      'compensation_pending','compensated','recovery_required')
  );

CREATE UNIQUE INDEX IF NOT EXISTS operation_effect_transition_idempotency_idx
  ON fornix.operation_effect_transitions(workspace_id, effect_id, idempotency_key);

CREATE INDEX IF NOT EXISTS operation_effect_recovery_queue_idx
  ON fornix.operation_effect_state(workspace_id, updated_at, effect_id)
  WHERE state NOT IN ('verified','compensated');

CREATE INDEX IF NOT EXISTS operation_effect_leases_active_expiry_idx
  ON fornix.operation_effect_leases(workspace_id, lease_until, effect_id)
  WHERE released_at IS NULL;
