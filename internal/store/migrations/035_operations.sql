-- 035: generic, workspace-scoped operation authority.
-- Operations coordinate existing authorities; they do not replace them.

CREATE TABLE IF NOT EXISTS fornix.operations (
  workspace_id TEXT NOT NULL,
  id TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  operation_hash TEXT NOT NULL,
  status TEXT NOT NULL,
  actor JSONB NOT NULL,
  task_ref JSONB,
  session_ref JSONB,
  request JSONB NOT NULL,
  plan JSONB,
  plan_hash TEXT NOT NULL DEFAULT '',
  result_hash TEXT NOT NULL DEFAULT '',
  report_hash TEXT NOT NULL DEFAULT '',
  failure JSONB,
  next_retry_at TIMESTAMPTZ,
  state_version BIGINT NOT NULL DEFAULT 0,
  state_hash TEXT NOT NULL,
  task_owner_id TEXT NOT NULL DEFAULT '',
  task_fence BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  PRIMARY KEY (workspace_id, id),
  CONSTRAINT operations_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT operations_id_nonempty CHECK (length(id) > 0),
  CONSTRAINT operations_request_identity_nonempty CHECK (length(request_id) > 0 AND length(idempotency_key) > 0),
  CONSTRAINT operations_hash_shape CHECK (
    request_hash ~ '^[0-9a-f]{64}$' AND operation_hash ~ '^[0-9a-f]{64}$' AND
    (plan_hash = '' OR plan_hash ~ '^[0-9a-f]{64}$') AND
    (result_hash = '' OR result_hash ~ '^[0-9a-f]{64}$') AND
    (report_hash = '' OR report_hash ~ '^[0-9a-f]{64}$') AND state_hash ~ '^[0-9a-f]{64}$'
  ),
  CONSTRAINT operations_status_valid CHECK (status IN ('created','planned','admitted','awaiting_approval','running','awaiting_retry','awaiting_external','verifying','succeeded','failed','cancelled','recovery_required','dead_letter','abstained')),
  CONSTRAINT operations_versions_nonnegative CHECK (state_version >= 0 AND task_fence >= 0),
  CONSTRAINT operations_json_size CHECK (
    octet_length(actor::text) <= 16384 AND octet_length(COALESCE(task_ref, '{}'::jsonb)::text) <= 4096 AND
    octet_length(COALESCE(session_ref, '{}'::jsonb)::text) <= 4096 AND octet_length(request::text) <= 131072 AND
    octet_length(COALESCE(plan, '{}'::jsonb)::text) <= 262144 AND octet_length(COALESCE(failure, '{}'::jsonb)::text) <= 16384
  ),
  UNIQUE (workspace_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS operations_queue_idx ON fornix.operations(workspace_id, status, next_retry_at, created_at, id);
CREATE INDEX IF NOT EXISTS operations_actor_idx ON fornix.operations(workspace_id, (actor->>'id'), created_at DESC);

CREATE TABLE IF NOT EXISTS fornix.operation_idempotency (
  workspace_id TEXT NOT NULL,
  command TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  transition_version BIGINT,
  outcome_hash TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, command, idempotency_key),
  CONSTRAINT operation_idempotency_hash_shape CHECK (request_hash ~ '^[0-9a-f]{64}$' AND (outcome_hash = '' OR outcome_hash ~ '^[0-9a-f]{64}$')),
  CONSTRAINT operation_idempotency_operation_fk FOREIGN KEY (workspace_id, operation_id) REFERENCES fornix.operations(workspace_id, id)
);

CREATE TABLE IF NOT EXISTS fornix.operation_transitions (
  workspace_id TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  state_version BIGINT NOT NULL,
  from_status TEXT NOT NULL,
  to_status TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  actor JSONB NOT NULL,
  task_ref JSONB,
  session_ref JSONB,
  task_owner_id TEXT NOT NULL DEFAULT '',
  task_fence BIGINT NOT NULL DEFAULT 0,
  operation_owner_id TEXT NOT NULL DEFAULT '',
  operation_fence BIGINT NOT NULL DEFAULT 0,
  causation_id TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL DEFAULT '',
  reason_code TEXT NOT NULL DEFAULT '',
  state JSONB NOT NULL,
  state_hash TEXT NOT NULL,
  previous_state_hash TEXT NOT NULL,
  event_sequence BIGINT,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, operation_id, state_version),
  CONSTRAINT operation_transitions_operation_fk FOREIGN KEY (workspace_id, operation_id) REFERENCES fornix.operations(workspace_id, id),
  CONSTRAINT operation_transitions_version_valid CHECK (state_version > 0),
  CONSTRAINT operation_transitions_status_valid CHECK (
    from_status IN ('created','planned','admitted','awaiting_approval','running','awaiting_retry','awaiting_external','verifying','succeeded','failed','cancelled','recovery_required','dead_letter','abstained') AND
    to_status IN ('created','planned','admitted','awaiting_approval','running','awaiting_retry','awaiting_external','verifying','succeeded','failed','cancelled','recovery_required','dead_letter','abstained')
  ),
  CONSTRAINT operation_transitions_fence_nonnegative CHECK (task_fence >= 0 AND operation_fence >= 0),
  CONSTRAINT operation_transitions_hash_shape CHECK (state_hash ~ '^[0-9a-f]{64}$' AND previous_state_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT operation_transitions_json_size CHECK (octet_length(actor::text) <= 16384 AND octet_length(state::text) <= 32768)
);
CREATE INDEX IF NOT EXISTS operation_transitions_event_idx ON fornix.operation_transitions(workspace_id, event_sequence) WHERE event_sequence IS NOT NULL;

CREATE TABLE IF NOT EXISTS fornix.operation_resources (
  workspace_id TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  resource_kind TEXT NOT NULL,
  resource_id TEXT NOT NULL,
  resource_hash TEXT NOT NULL DEFAULT '',
  role TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, operation_id, ordinal),
  CONSTRAINT operation_resources_operation_fk FOREIGN KEY (workspace_id, operation_id) REFERENCES fornix.operations(workspace_id, id),
  CONSTRAINT operation_resources_identity_nonempty CHECK (length(resource_kind) > 0 AND length(resource_id) > 0),
  CONSTRAINT operation_resources_hash_shape CHECK (resource_hash = '' OR resource_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT operation_resources_ordinal_valid CHECK (ordinal BETWEEN 0 AND 127)
);

CREATE TABLE IF NOT EXISTS fornix.operation_steps (
  workspace_id TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  step_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  kind TEXT NOT NULL,
  capability JSONB NOT NULL,
  target JSONB NOT NULL,
  depends_on JSONB NOT NULL DEFAULT '[]'::jsonb,
  effect_class TEXT NOT NULL,
  profile JSONB NOT NULL,
  input_hash TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'planned',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  output_hash TEXT NOT NULL DEFAULT '',
  report_hash TEXT NOT NULL DEFAULT '',
  state_hash TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, operation_id, step_id),
  UNIQUE (workspace_id, operation_id, ordinal),
  CONSTRAINT operation_steps_operation_fk FOREIGN KEY (workspace_id, operation_id) REFERENCES fornix.operations(workspace_id, id),
  CONSTRAINT operation_steps_identity_nonempty CHECK (length(step_id) > 0 AND length(kind) > 0),
  CONSTRAINT operation_steps_ordinal_valid CHECK (ordinal BETWEEN 0 AND 1023),
  CONSTRAINT operation_steps_hash_shape CHECK (input_hash ~ '^[0-9a-f]{64}$' AND (output_hash = '' OR output_hash ~ '^[0-9a-f]{64}$') AND (report_hash = '' OR report_hash ~ '^[0-9a-f]{64}$') AND (state_hash = '' OR state_hash ~ '^[0-9a-f]{64}$')),
  CONSTRAINT operation_steps_json_size CHECK (octet_length(capability::text) <= 16384 AND octet_length(target::text) <= 16384 AND octet_length(depends_on::text) <= 16384 AND octet_length(profile::text) <= 16384)
);

CREATE TABLE IF NOT EXISTS fornix.operation_attempts (
  workspace_id TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  step_id TEXT NOT NULL,
  attempt INTEGER NOT NULL,
  attempt_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  operation_owner_id TEXT NOT NULL DEFAULT '',
  operation_fence BIGINT NOT NULL DEFAULT 0,
  status TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  response_hash TEXT NOT NULL DEFAULT '',
  failure JSONB,
  external_effect_status TEXT NOT NULL DEFAULT 'not_started',
  started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  completed_at TIMESTAMPTZ,
  PRIMARY KEY (workspace_id, operation_id, step_id, attempt),
  UNIQUE (workspace_id, attempt_id),
  UNIQUE (workspace_id, operation_id, step_id, attempt_id),
  UNIQUE (workspace_id, operation_id, step_id, idempotency_key),
  CONSTRAINT operation_attempts_step_fk FOREIGN KEY (workspace_id, operation_id, step_id) REFERENCES fornix.operation_steps(workspace_id, operation_id, step_id),
  CONSTRAINT operation_attempts_identity_nonempty CHECK (length(idempotency_key) > 0),
  CONSTRAINT operation_attempts_attempt_valid CHECK (attempt >= 1 AND operation_fence >= 0),
  CONSTRAINT operation_attempts_hash_shape CHECK (request_hash ~ '^[0-9a-f]{64}$' AND (response_hash = '' OR response_hash ~ '^[0-9a-f]{64}$')),
  CONSTRAINT operation_attempts_json_size CHECK (octet_length(COALESCE(failure, '{}'::jsonb)::text) <= 16384)
);

CREATE TABLE IF NOT EXISTS fornix.operation_effects (
  workspace_id TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  step_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  effect_id TEXT NOT NULL,
  effect_class TEXT NOT NULL,
  boundary TEXT NOT NULL,
  idempotency_key TEXT NOT NULL DEFAULT '',
  provider_request_id TEXT NOT NULL DEFAULT '',
  provider_idempotency_supported BOOLEAN NOT NULL DEFAULT FALSE,
  delivery_semantics TEXT NOT NULL,
  verification_status TEXT NOT NULL,
  compensation_status TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  response_hash TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  verified_at TIMESTAMPTZ,
  PRIMARY KEY (workspace_id, effect_id),
  UNIQUE (workspace_id, attempt_id),
  CONSTRAINT operation_effects_attempt_fk FOREIGN KEY (workspace_id, operation_id, step_id, attempt_id) REFERENCES fornix.operation_attempts(workspace_id, operation_id, step_id, attempt_id),
  CONSTRAINT operation_effects_identity_nonempty CHECK (length(boundary) > 0),
  CONSTRAINT operation_effects_delivery_valid CHECK (delivery_semantics IN ('not_started','at_most_once','at_least_once','unknown')),
  CONSTRAINT operation_effects_verification_valid CHECK (verification_status IN ('not_required','pending','verified','failed','unknown')),
  CONSTRAINT operation_effects_compensation_valid CHECK (compensation_status IN ('unavailable','available','pending','completed','failed','unknown')),
  CONSTRAINT operation_effects_hash_shape CHECK (request_hash ~ '^[0-9a-f]{64}$' AND (response_hash = '' OR response_hash ~ '^[0-9a-f]{64}$'))
);

CREATE TABLE IF NOT EXISTS fornix.operation_callbacks (
  workspace_id TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  callback_id TEXT NOT NULL,
  callback_kind TEXT NOT NULL,
  external_id TEXT NOT NULL DEFAULT '',
  request_hash TEXT NOT NULL,
  response_hash TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, callback_id),
  CONSTRAINT operation_callbacks_operation_fk FOREIGN KEY (workspace_id, operation_id) REFERENCES fornix.operations(workspace_id, id),
  CONSTRAINT operation_callbacks_hash_shape CHECK (request_hash ~ '^[0-9a-f]{64}$' AND (response_hash = '' OR response_hash ~ '^[0-9a-f]{64}$')),
  CONSTRAINT operation_callbacks_status_bounded CHECK (length(status) BETWEEN 1 AND 128),
  CONSTRAINT operation_callbacks_text_bounded CHECK (length(callback_kind) BETWEEN 1 AND 128 AND length(external_id) <= 256)
);

CREATE TABLE IF NOT EXISTS fornix.operation_leases (
  workspace_id TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  fence BIGINT NOT NULL,
  lease_until TIMESTAMPTZ NOT NULL,
  acquired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  renewed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  released_at TIMESTAMPTZ,
  PRIMARY KEY (workspace_id, operation_id),
  CONSTRAINT operation_leases_operation_fk FOREIGN KEY (workspace_id, operation_id) REFERENCES fornix.operations(workspace_id, id),
  CONSTRAINT operation_leases_identity_nonempty CHECK (length(owner_id) > 0 AND fence > 0)
);

CREATE TABLE IF NOT EXISTS fornix.operation_links (
  workspace_id TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  link_kind TEXT NOT NULL,
  source_id TEXT NOT NULL,
  source_hash TEXT NOT NULL DEFAULT '',
  role TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, operation_id, link_kind, source_id, role),
  CONSTRAINT operation_links_operation_fk FOREIGN KEY (workspace_id, operation_id) REFERENCES fornix.operations(workspace_id, id),
  CONSTRAINT operation_links_identity_nonempty CHECK (length(link_kind) > 0 AND length(source_id) > 0),
  CONSTRAINT operation_links_hash_shape CHECK (source_hash = '' OR source_hash ~ '^[0-9a-f]{64}$')
);
CREATE INDEX IF NOT EXISTS operation_links_source_idx ON fornix.operation_links(workspace_id, link_kind, source_id);

CREATE OR REPLACE FUNCTION fornix.reject_operation_history_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix operation history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS operation_transitions_append_only ON fornix.operation_transitions;
CREATE TRIGGER operation_transitions_append_only BEFORE UPDATE OR DELETE ON fornix.operation_transitions FOR EACH ROW EXECUTE FUNCTION fornix.reject_operation_history_mutation();
DROP TRIGGER IF EXISTS operation_attempts_append_only ON fornix.operation_attempts;
CREATE TRIGGER operation_attempts_append_only BEFORE UPDATE OR DELETE ON fornix.operation_attempts FOR EACH ROW EXECUTE FUNCTION fornix.reject_operation_history_mutation();
DROP TRIGGER IF EXISTS operation_effects_append_only ON fornix.operation_effects;
CREATE TRIGGER operation_effects_append_only BEFORE UPDATE OR DELETE ON fornix.operation_effects FOR EACH ROW EXECUTE FUNCTION fornix.reject_operation_history_mutation();
DROP TRIGGER IF EXISTS operation_callbacks_append_only ON fornix.operation_callbacks;
CREATE TRIGGER operation_callbacks_append_only BEFORE UPDATE OR DELETE ON fornix.operation_callbacks FOR EACH ROW EXECUTE FUNCTION fornix.reject_operation_history_mutation();
DROP TRIGGER IF EXISTS operation_links_append_only ON fornix.operation_links;
CREATE TRIGGER operation_links_append_only BEFORE UPDATE OR DELETE ON fornix.operation_links FOR EACH ROW EXECUTE FUNCTION fornix.reject_operation_history_mutation();
