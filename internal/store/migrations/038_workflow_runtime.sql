-- 038: durable, workspace-scoped multi-step workflow state.
-- The generic operation authority remains the root identity. These tables are
-- a bounded workflow projection and append-only checkpoint history.

CREATE TABLE IF NOT EXISTS fornix.workflow_runs (
  workspace_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  operation_hash TEXT NOT NULL,
  plan_hash TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  actor JSONB NOT NULL,
  task_ref JSONB,
  session_ref JSONB,
  status TEXT NOT NULL,
  budget JSONB NOT NULL,
  wait JSONB,
  failure JSONB,
  terminal_reason TEXT NOT NULL DEFAULT '',
  state_version BIGINT NOT NULL DEFAULT 0,
  state_hash TEXT NOT NULL,
  output_bytes BIGINT NOT NULL DEFAULT 0,
  tokens BIGINT NOT NULL DEFAULT 0,
  cost_micros BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, run_id),
  UNIQUE (workspace_id, operation_id),
  CONSTRAINT workflow_run_operation_fk FOREIGN KEY (workspace_id, operation_id) REFERENCES fornix.operations(workspace_id, id),
  CONSTRAINT workflow_run_identity CHECK (length(workspace_id) > 0 AND length(run_id) > 0 AND length(operation_id) > 0),
  CONSTRAINT workflow_run_hash_shape CHECK (operation_hash ~ '^[0-9a-f]{64}$' AND plan_hash ~ '^[0-9a-f]{64}$' AND state_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT workflow_run_status_valid CHECK (status IN ('created','running','awaiting_approval','awaiting_human_input','awaiting_callback','awaiting_retry','awaiting_external','recovery_required','succeeded','failed','cancelled','dead_letter')),
  CONSTRAINT workflow_run_versions_valid CHECK (state_version >= 0 AND output_bytes >= 0 AND tokens >= 0 AND cost_micros >= 0),
  CONSTRAINT workflow_run_json_bounded CHECK (
    octet_length(actor::text) <= 16384 AND octet_length(COALESCE(task_ref, '{}'::jsonb)::text) <= 4096 AND
    octet_length(COALESCE(session_ref, '{}'::jsonb)::text) <= 4096 AND octet_length(budget::text) <= 16384 AND
    octet_length(COALESCE(wait, '{}'::jsonb)::text) <= 8192 AND octet_length(COALESCE(failure, '{}'::jsonb)::text) <= 16384 AND length(terminal_reason) <= 256
  )
);
CREATE INDEX IF NOT EXISTS workflow_runs_queue_idx ON fornix.workflow_runs(workspace_id, status, updated_at, run_id);

CREATE TABLE IF NOT EXISTS fornix.workflow_step_states (
  workspace_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  step_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 0,
  idempotency_key TEXT NOT NULL DEFAULT '',
  output_hash TEXT NOT NULL DEFAULT '',
  evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
  artifacts JSONB NOT NULL DEFAULT '[]'::jsonb,
  effect JSONB,
  wait JSONB,
  failure JSONB,
  next_retry_at TIMESTAMPTZ,
  state_version BIGINT NOT NULL DEFAULT 0,
  state_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, run_id, step_id),
  UNIQUE (workspace_id, run_id, ordinal),
  CONSTRAINT workflow_step_run_fk FOREIGN KEY (workspace_id, run_id) REFERENCES fornix.workflow_runs(workspace_id, run_id),
  CONSTRAINT workflow_step_identity CHECK (length(step_id) > 0 AND length(kind) > 0 AND ordinal BETWEEN 0 AND 63),
  CONSTRAINT workflow_step_status_valid CHECK (status IN ('planned','ready','running','awaiting_approval','awaiting_human_input','awaiting_callback','awaiting_retry','awaiting_external','succeeded','failed','cancelled','recovery_required','compensating','compensated')),
  CONSTRAINT workflow_step_attempt_valid CHECK (attempt >= 0 AND attempt <= 1024 AND state_version >= 0),
  CONSTRAINT workflow_step_hash_shape CHECK ((output_hash = '' OR output_hash ~ '^[0-9a-f]{64}$') AND state_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT workflow_step_json_bounded CHECK (
    octet_length(evidence::text) <= 65536 AND octet_length(artifacts::text) <= 65536 AND octet_length(COALESCE(effect, '{}'::jsonb)::text) <= 32768 AND
    octet_length(COALESCE(wait, '{}'::jsonb)::text) <= 8192 AND octet_length(COALESCE(failure, '{}'::jsonb)::text) <= 16384
  )
);

CREATE TABLE IF NOT EXISTS fornix.workflow_transitions (
  workspace_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  version BIGINT NOT NULL,
  step_id TEXT NOT NULL DEFAULT '',
  from_run_status TEXT NOT NULL,
  to_run_status TEXT NOT NULL,
  from_step_status TEXT NOT NULL DEFAULT '',
  to_step_status TEXT NOT NULL DEFAULT '',
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  command_hash TEXT NOT NULL,
  actor JSONB NOT NULL,
  owner_id TEXT NOT NULL DEFAULT '',
  fence BIGINT NOT NULL DEFAULT 0,
  task_owner_id TEXT NOT NULL DEFAULT '',
  task_fence BIGINT NOT NULL DEFAULT 0,
  previous_state_hash TEXT NOT NULL,
  state_hash TEXT NOT NULL,
  state JSONB NOT NULL,
  event_sequence BIGINT,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, run_id, version),
  CONSTRAINT workflow_transition_run_fk FOREIGN KEY (workspace_id, run_id) REFERENCES fornix.workflow_runs(workspace_id, run_id),
  CONSTRAINT workflow_transition_version_valid CHECK (version > 0 AND fence >= 0 AND task_fence >= 0),
  CONSTRAINT workflow_transition_hash_shape CHECK (command_hash ~ '^[0-9a-f]{64}$' AND previous_state_hash ~ '^[0-9a-f]{64}$' AND state_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT workflow_transition_json_bounded CHECK (octet_length(actor::text) <= 16384 AND octet_length(state::text) <= 131072)
);
CREATE INDEX IF NOT EXISTS workflow_transitions_event_idx ON fornix.workflow_transitions(workspace_id, event_sequence) WHERE event_sequence IS NOT NULL;

CREATE TABLE IF NOT EXISTS fornix.workflow_idempotency (
  workspace_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  step_id TEXT NOT NULL DEFAULT '',
  idempotency_key TEXT NOT NULL,
  command_hash TEXT NOT NULL,
  transition_version BIGINT,
  outcome_hash TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, run_id, step_id, idempotency_key),
  CONSTRAINT workflow_idempotency_run_fk FOREIGN KEY (workspace_id, run_id) REFERENCES fornix.workflow_runs(workspace_id, run_id),
  CONSTRAINT workflow_idempotency_hash_shape CHECK (command_hash ~ '^[0-9a-f]{64}$' AND (outcome_hash = '' OR outcome_hash ~ '^[0-9a-f]{64}$'))
);

CREATE OR REPLACE FUNCTION fornix.reject_workflow_history_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix workflow transition history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS workflow_transitions_append_only ON fornix.workflow_transitions;
CREATE TRIGGER workflow_transitions_append_only BEFORE UPDATE OR DELETE ON fornix.workflow_transitions FOR EACH ROW EXECUTE FUNCTION fornix.reject_workflow_history_mutation();
