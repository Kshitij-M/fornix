-- 036: universal operation admission, approval, and external-effect state.
-- Decisions and histories are append-only; small current tables make recovery
-- and bounded operator inspection efficient without replacing the history.

CREATE TABLE IF NOT EXISTS fornix.operation_admission_decisions (
  workspace_id TEXT NOT NULL,
  decision_id TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  operation_hash TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  actor JSONB NOT NULL,
  capability JSONB NOT NULL,
  target JSONB NOT NULL,
  effect_class TEXT NOT NULL,
  policy_id TEXT NOT NULL,
  policy_version TEXT NOT NULL,
  policy_hash TEXT NOT NULL,
  policy_snapshot JSONB NOT NULL,
  input_hash TEXT NOT NULL,
  decision_hash TEXT NOT NULL,
  status TEXT NOT NULL,
  reason_code TEXT NOT NULL,
  approval_id TEXT NOT NULL DEFAULT '',
  cost_micros BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, decision_id),
  UNIQUE (workspace_id, idempotency_key),
  UNIQUE (workspace_id, operation_id, decision_hash),
  CONSTRAINT operation_admission_operation_fk FOREIGN KEY (workspace_id, operation_id) REFERENCES fornix.operations(workspace_id, id),
  CONSTRAINT operation_admission_hash_shape CHECK (
    operation_hash ~ '^[0-9a-f]{64}$' AND policy_hash ~ '^[0-9a-f]{64}$' AND
    input_hash ~ '^[0-9a-f]{64}$' AND decision_hash ~ '^[0-9a-f]{64}$'
  ),
  CONSTRAINT operation_admission_status_valid CHECK (status IN ('allowed','awaiting_approval','denied','abstained')),
  CONSTRAINT operation_admission_effect_valid CHECK (effect_class IN ('read_only','observation','reversible_write','approval_required_write','irreversible_write','external_communication')),
  CONSTRAINT operation_admission_json_bounded CHECK (
    octet_length(actor::text) <= 16384 AND octet_length(capability::text) <= 16384 AND
    octet_length(target::text) <= 16384 AND octet_length(policy_snapshot::text) <= 131072
  ),
  CONSTRAINT operation_admission_cost_nonnegative CHECK (cost_micros >= 0)
);
CREATE INDEX IF NOT EXISTS operation_admission_actor_idx ON fornix.operation_admission_decisions(workspace_id, (actor->>'id'), created_at DESC, decision_id);

CREATE TABLE IF NOT EXISTS fornix.operation_approvals (
  workspace_id TEXT NOT NULL,
  approval_id TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  decision_id TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  operation_hash TEXT NOT NULL,
  decision_hash TEXT NOT NULL,
  capability_hash TEXT NOT NULL,
  target_hash TEXT NOT NULL,
  input_hash TEXT NOT NULL,
  policy_hash TEXT NOT NULL,
  effect_class TEXT NOT NULL,
  requested_by JSONB NOT NULL,
  status TEXT NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  decided_at TIMESTAMPTZ,
  decided_by JSONB,
  decision_reason_hash TEXT NOT NULL DEFAULT '',
  PRIMARY KEY (workspace_id, approval_id),
  UNIQUE (workspace_id, decision_id),
  CONSTRAINT operation_approval_decision_fk FOREIGN KEY (workspace_id, decision_id) REFERENCES fornix.operation_admission_decisions(workspace_id, decision_id),
  CONSTRAINT operation_approval_operation_fk FOREIGN KEY (workspace_id, operation_id) REFERENCES fornix.operations(workspace_id, id),
  CONSTRAINT operation_approval_hash_shape CHECK (
    operation_hash ~ '^[0-9a-f]{64}$' AND decision_hash ~ '^[0-9a-f]{64}$' AND
    capability_hash ~ '^[0-9a-f]{64}$' AND target_hash ~ '^[0-9a-f]{64}$' AND
    input_hash ~ '^[0-9a-f]{64}$' AND policy_hash ~ '^[0-9a-f]{64}$' AND
    (decision_reason_hash = '' OR decision_reason_hash ~ '^[0-9a-f]{64}$')
  ),
  CONSTRAINT operation_approval_status_valid CHECK (status IN ('pending','approved','denied','expired')),
  CONSTRAINT operation_approval_effect_valid CHECK (effect_class IN ('reversible_write','approval_required_write','irreversible_write','external_communication')),
  CONSTRAINT operation_approval_json_bounded CHECK (octet_length(requested_by::text) <= 16384 AND octet_length(COALESCE(decided_by, '{}'::jsonb)::text) <= 16384)
);

CREATE TABLE IF NOT EXISTS fornix.operation_approval_transitions (
  workspace_id TEXT NOT NULL,
  approval_id TEXT NOT NULL,
  version BIGINT NOT NULL,
  from_status TEXT NOT NULL,
  to_status TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  actor JSONB NOT NULL,
  command_hash TEXT NOT NULL,
  reason_hash TEXT NOT NULL DEFAULT '',
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, approval_id, version),
  CONSTRAINT operation_approval_transition_fk FOREIGN KEY (workspace_id, approval_id) REFERENCES fornix.operation_approvals(workspace_id, approval_id),
  CONSTRAINT operation_approval_transition_status_valid CHECK (from_status IN ('pending','approved','denied','expired') AND to_status IN ('pending','approved','denied','expired')),
  CONSTRAINT operation_approval_transition_hash_shape CHECK (command_hash ~ '^[0-9a-f]{64}$' AND (reason_hash = '' OR reason_hash ~ '^[0-9a-f]{64}$')),
  CONSTRAINT operation_approval_transition_json_bounded CHECK (octet_length(actor::text) <= 16384)
);
CREATE UNIQUE INDEX IF NOT EXISTS operation_approval_transition_idempotency_idx ON fornix.operation_approval_transitions(workspace_id, approval_id, idempotency_key);

CREATE TABLE IF NOT EXISTS fornix.operation_effect_state (
  workspace_id TEXT NOT NULL,
  effect_id TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  state TEXT NOT NULL,
  version BIGINT NOT NULL DEFAULT 1,
  provider_request_id TEXT NOT NULL DEFAULT '',
  response_hash TEXT NOT NULL DEFAULT '',
  verification_hash TEXT NOT NULL DEFAULT '',
  compensation_hash TEXT NOT NULL DEFAULT '',
  failure_code TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, effect_id),
  CONSTRAINT operation_effect_state_effect_fk FOREIGN KEY (workspace_id, effect_id) REFERENCES fornix.operation_effects(workspace_id, effect_id),
  CONSTRAINT operation_effect_state_operation_fk FOREIGN KEY (workspace_id, operation_id) REFERENCES fornix.operations(workspace_id, id),
  CONSTRAINT operation_effect_state_version_valid CHECK (version > 0),
  CONSTRAINT operation_effect_state_status_valid CHECK (state IN ('reserved','dispatched','acknowledged','verification_pending','verified','verification_failed','compensation_pending','compensated','recovery_required')),
  CONSTRAINT operation_effect_state_hash_shape CHECK (
    (response_hash = '' OR response_hash ~ '^[0-9a-f]{64}$') AND
    (verification_hash = '' OR verification_hash ~ '^[0-9a-f]{64}$') AND
    (compensation_hash = '' OR compensation_hash ~ '^[0-9a-f]{64}$')
  )
);

CREATE TABLE IF NOT EXISTS fornix.operation_effect_transitions (
  workspace_id TEXT NOT NULL,
  effect_id TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  version BIGINT NOT NULL,
  from_state TEXT NOT NULL,
  to_state TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  fence BIGINT NOT NULL,
  command_hash TEXT NOT NULL,
  provider_request_id TEXT NOT NULL DEFAULT '',
  response_hash TEXT NOT NULL DEFAULT '',
  verification_hash TEXT NOT NULL DEFAULT '',
  compensation_hash TEXT NOT NULL DEFAULT '',
  failure_code TEXT NOT NULL DEFAULT '',
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, effect_id, version),
  CONSTRAINT operation_effect_transition_effect_fk FOREIGN KEY (workspace_id, effect_id) REFERENCES fornix.operation_effects(workspace_id, effect_id),
  CONSTRAINT operation_effect_transition_operation_fk FOREIGN KEY (workspace_id, operation_id) REFERENCES fornix.operations(workspace_id, id),
  CONSTRAINT operation_effect_transition_version_valid CHECK (version > 0 AND fence > 0),
  CONSTRAINT operation_effect_transition_hash_shape CHECK (
    command_hash ~ '^[0-9a-f]{64}$' AND
    (response_hash = '' OR response_hash ~ '^[0-9a-f]{64}$') AND
    (verification_hash = '' OR verification_hash ~ '^[0-9a-f]{64}$') AND
    (compensation_hash = '' OR compensation_hash ~ '^[0-9a-f]{64}$')
  ),
  CONSTRAINT operation_effect_transition_text_bounded CHECK (length(owner_id) BETWEEN 1 AND 128 AND length(request_id) BETWEEN 1 AND 128 AND length(idempotency_key) BETWEEN 1 AND 256)
);
CREATE INDEX IF NOT EXISTS operation_effect_transition_operation_idx ON fornix.operation_effect_transitions(workspace_id, operation_id, occurred_at, effect_id, version);

CREATE OR REPLACE FUNCTION fornix.reject_operation_admission_history_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix operation admission/effect history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS operation_admission_decisions_append_only ON fornix.operation_admission_decisions;
CREATE TRIGGER operation_admission_decisions_append_only BEFORE UPDATE OR DELETE ON fornix.operation_admission_decisions FOR EACH ROW EXECUTE FUNCTION fornix.reject_operation_admission_history_mutation();
DROP TRIGGER IF EXISTS operation_approval_transitions_append_only ON fornix.operation_approval_transitions;
CREATE TRIGGER operation_approval_transitions_append_only BEFORE UPDATE OR DELETE ON fornix.operation_approval_transitions FOR EACH ROW EXECUTE FUNCTION fornix.reject_operation_admission_history_mutation();
DROP TRIGGER IF EXISTS operation_effect_transitions_append_only ON fornix.operation_effect_transitions;
CREATE TRIGGER operation_effect_transitions_append_only BEFORE UPDATE OR DELETE ON fornix.operation_effect_transitions FOR EACH ROW EXECUTE FUNCTION fornix.reject_operation_admission_history_mutation();
