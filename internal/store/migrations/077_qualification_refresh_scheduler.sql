-- 077: durable deployment-owned qualification refresh scheduling.
-- This is a fenced handoff projection. It never executes deployment work or
-- stores credentials/raw evidence; attempts and events are append-only.

CREATE TABLE IF NOT EXISTS fornix.qualification_refresh_schedules (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  release_id TEXT NOT NULL,
  config_hash TEXT NOT NULL,
  required_evidence_kinds JSONB NOT NULL,
  required_recovery_drills JSONB NOT NULL DEFAULT '[]'::jsonb,
  interval_ms BIGINT NOT NULL,
  freshness_ms BIGINT NOT NULL,
  max_attempts INTEGER NOT NULL,
  backoff_base_ms BIGINT NOT NULL,
  backoff_max_ms BIGINT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  next_due_at TIMESTAMPTZ,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  retry_count INTEGER NOT NULL DEFAULT 0,
  last_as_of TIMESTAMPTZ,
  last_attempt_id TEXT,
  last_report_id TEXT,
  last_report_hash TEXT,
  last_outcome TEXT,
  last_error_code TEXT,
  lease_owner_id TEXT,
  lease_fence BIGINT NOT NULL DEFAULT 0,
  lease_until TIMESTAMPTZ,
  request_id TEXT,
  idempotency_key TEXT NOT NULL,
  causation_id TEXT,
  correlation_id TEXT,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_refresh_schedules_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_refresh_schedules_deployment_nonempty CHECK (length(deployment_id) BETWEEN 1 AND 128),
  CONSTRAINT qualification_refresh_schedules_release_nonempty CHECK (length(release_id) > 0),
  CONSTRAINT qualification_refresh_schedules_hash_valid CHECK (config_hash ~ '^[0-9a-f]{64}$' AND (last_report_hash IS NULL OR last_report_hash ~ '^[0-9a-f]{64}$')),
  CONSTRAINT qualification_refresh_schedules_kinds_bounded CHECK (jsonb_typeof(required_evidence_kinds) = 'array' AND jsonb_array_length(required_evidence_kinds) BETWEEN 1 AND 6 AND jsonb_typeof(required_recovery_drills) = 'array' AND jsonb_array_length(required_recovery_drills) <= 5),
  CONSTRAINT qualification_refresh_schedules_intervals_bounded CHECK (interval_ms BETWEEN 60000 AND 31536000000 AND freshness_ms BETWEEN 60000 AND 31536000000),
  CONSTRAINT qualification_refresh_schedules_retry_bounded CHECK (max_attempts BETWEEN 1 AND 32 AND backoff_base_ms BETWEEN 1 AND 86400000 AND backoff_max_ms BETWEEN backoff_base_ms AND 86400000),
  CONSTRAINT qualification_refresh_schedules_status_valid CHECK (status IN ('active','paused','cancelled','dead_letter')),
  CONSTRAINT qualification_refresh_schedules_attempts_nonnegative CHECK (attempt_count >= 0),
  CONSTRAINT qualification_refresh_schedules_retries_bounded CHECK (retry_count BETWEEN 0 AND 32),
  CONSTRAINT qualification_refresh_schedules_fence_nonnegative CHECK (lease_fence >= 0),
  CONSTRAINT qualification_refresh_schedules_actor_bounded CHECK (octet_length(actor::text) <= 4096),
  CONSTRAINT qualification_refresh_schedules_idempotency_nonempty CHECK (length(idempotency_key) BETWEEN 1 AND 256),
  CONSTRAINT qualification_refresh_schedules_scope_unique UNIQUE (workspace_id, deployment_id, release_id),
  CONSTRAINT qualification_refresh_schedules_idempotency_unique UNIQUE (workspace_id, deployment_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS qualification_refresh_schedules_due_idx
  ON fornix.qualification_refresh_schedules(workspace_id, next_due_at, created_at, id)
  WHERE status = 'active';
CREATE INDEX IF NOT EXISTS qualification_refresh_schedules_lease_idx
  ON fornix.qualification_refresh_schedules(workspace_id, lease_until, id)
  WHERE lease_owner_id IS NOT NULL;

CREATE TABLE IF NOT EXISTS fornix.qualification_refresh_schedule_attempts (
  id TEXT PRIMARY KEY,
  schedule_id TEXT NOT NULL REFERENCES fornix.qualification_refresh_schedules(id),
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  release_id TEXT NOT NULL,
  attempt_number INTEGER NOT NULL,
  owner_id TEXT NOT NULL,
  fence BIGINT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  plan_hash TEXT NOT NULL,
  refresh_id TEXT,
  refresh_hash TEXT,
  outcome TEXT NOT NULL,
  retryable BOOLEAN NOT NULL DEFAULT false,
  error_code TEXT,
  retry_at TIMESTAMPTZ,
  idempotency_key TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_refresh_schedule_attempts_scope_nonempty CHECK (length(workspace_id) > 0 AND length(deployment_id) > 0 AND length(release_id) > 0),
  CONSTRAINT qualification_refresh_schedule_attempts_number_bounded CHECK (attempt_number BETWEEN 1 AND 32),
  CONSTRAINT qualification_refresh_schedule_attempts_fence_positive CHECK (fence > 0),
  CONSTRAINT qualification_refresh_schedule_attempts_hashes_valid CHECK (plan_hash ~ '^[0-9a-f]{64}$' AND (refresh_hash IS NULL OR refresh_hash ~ '^[0-9a-f]{64}$')),
  CONSTRAINT qualification_refresh_schedule_attempts_outcome_valid CHECK (outcome IN ('succeeded','retryable','failed')),
  CONSTRAINT qualification_refresh_schedule_attempts_retry_consistent CHECK ((outcome = 'retryable' AND retryable = true) OR (outcome <> 'retryable' AND retryable = false)),
  CONSTRAINT qualification_refresh_schedule_attempts_idempotency_nonempty CHECK (length(idempotency_key) BETWEEN 1 AND 256),
  CONSTRAINT qualification_refresh_schedule_attempts_actor_bounded CHECK (octet_length(actor::text) <= 4096),
  CONSTRAINT qualification_refresh_schedule_attempts_identity_unique UNIQUE (workspace_id, schedule_id, idempotency_key),
  CONSTRAINT qualification_refresh_schedule_attempts_number_unique UNIQUE (schedule_id, attempt_number)
);

CREATE INDEX IF NOT EXISTS qualification_refresh_schedule_attempts_lookup_idx
  ON fornix.qualification_refresh_schedule_attempts(workspace_id, schedule_id, created_at, id);

CREATE TABLE IF NOT EXISTS fornix.qualification_refresh_schedule_events (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  schedule_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  release_id TEXT NOT NULL,
  attempt_id TEXT,
  event TEXT NOT NULL,
  owner_id TEXT,
  fence BIGINT,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT,
  idempotency_key TEXT,
  causation_id TEXT,
  correlation_id TEXT,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_refresh_schedule_events_workspace_nonempty CHECK (length(workspace_id) > 0 AND length(schedule_id) > 0 AND length(deployment_id) > 0 AND length(release_id) > 0),
  CONSTRAINT qualification_refresh_schedule_events_event_valid CHECK (event IN ('registered','claimed','taken_over','renewed','released','completed','retry_scheduled','paused','resumed','cancelled','dead_lettered')),
  CONSTRAINT qualification_refresh_schedule_events_fence_valid CHECK (fence IS NULL OR fence > 0),
  CONSTRAINT qualification_refresh_schedule_events_json_bounded CHECK (octet_length(actor::text) <= 4096 AND octet_length(metadata::text) <= 8192)
);

CREATE INDEX IF NOT EXISTS qualification_refresh_schedule_events_lookup_idx
  ON fornix.qualification_refresh_schedule_events(workspace_id, schedule_id, occurred_at, id);

CREATE OR REPLACE FUNCTION fornix.reject_qualification_refresh_schedule_history_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix qualification refresh schedule history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS qualification_refresh_schedule_attempts_append_only ON fornix.qualification_refresh_schedule_attempts;
CREATE TRIGGER qualification_refresh_schedule_attempts_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_refresh_schedule_attempts
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_refresh_schedule_history_mutation();
DROP TRIGGER IF EXISTS qualification_refresh_schedule_events_append_only ON fornix.qualification_refresh_schedule_events;
CREATE TRIGGER qualification_refresh_schedule_events_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_refresh_schedule_events
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_refresh_schedule_history_mutation();

ALTER TABLE fornix.qualification_refresh_schedules ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_refresh_schedules;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_refresh_schedules
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
ALTER TABLE fornix.qualification_refresh_schedule_attempts ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_refresh_schedule_attempts;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_refresh_schedule_attempts
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
ALTER TABLE fornix.qualification_refresh_schedule_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_refresh_schedule_events;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_refresh_schedule_events
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
