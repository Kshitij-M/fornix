-- Durable post-finalization cleanup authority for separately managed sandbox
-- runtimes. Tool result finalization and intent creation share one transaction.

CREATE UNIQUE INDEX IF NOT EXISTS tool_runs_workspace_id_uq
  ON fornix.tool_runs(workspace_id, id);

CREATE TABLE IF NOT EXISTS fornix.sandbox_cleanup_jobs (
  workspace_id TEXT NOT NULL,
  id TEXT NOT NULL,
  tool_run_id TEXT NOT NULL,
  tool_attempt INTEGER NOT NULL,
  domain_link_id TEXT NOT NULL,
  domain_kind TEXT NOT NULL DEFAULT 'tool_run',
  domain_id TEXT NOT NULL,
  backend TEXT NOT NULL,
  execution_identity JSONB NOT NULL,
  execution_identity_hash TEXT NOT NULL,
  intent_hash TEXT NOT NULL,
  tool_request_hash TEXT NOT NULL,
  result_hash TEXT NOT NULL,
  actor JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  owner_id TEXT NOT NULL DEFAULT '',
  fence BIGINT NOT NULL DEFAULT 0,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  retry_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  lease_until TIMESTAMPTZ,
  failure_code TEXT NOT NULL DEFAULT '',
  completion_state TEXT NOT NULL DEFAULT '',
  completion_owner_id TEXT NOT NULL DEFAULT '',
  version BIGINT NOT NULL DEFAULT 1,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  completed_at TIMESTAMPTZ,
  PRIMARY KEY (workspace_id, id),
  UNIQUE (workspace_id, tool_run_id, tool_attempt),
  FOREIGN KEY (workspace_id, tool_run_id)
    REFERENCES fornix.tool_runs(workspace_id, id),
  FOREIGN KEY (workspace_id, domain_link_id)
    REFERENCES fornix.domain_effect_links(workspace_id, link_id),
  CONSTRAINT sandbox_cleanup_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT sandbox_cleanup_attempt_valid CHECK (tool_attempt > 0 AND attempt_count BETWEEN 0 AND 12),
  CONSTRAINT sandbox_cleanup_domain_identity_valid CHECK (domain_kind = 'tool_run' AND domain_id = tool_run_id),
  CONSTRAINT sandbox_cleanup_backend_valid CHECK (backend IN ('oci-container', 'gvisor', 'microvm')),
  CONSTRAINT sandbox_cleanup_hashes_valid CHECK (
    execution_identity_hash ~ '^[0-9a-f]{64}$' AND intent_hash ~ '^[0-9a-f]{64}$' AND
    tool_request_hash ~ '^[0-9a-f]{64}$' AND result_hash ~ '^[0-9a-f]{64}$'
  ),
  CONSTRAINT sandbox_cleanup_identity_scope_valid CHECK (
    execution_identity->>'workspace_id' = workspace_id AND
    execution_identity->>'tool_run_id' = tool_run_id AND
    execution_identity->>'tool_request_hash' = tool_request_hash AND
    execution_identity->>'backend' = backend AND
    actor->>'workspace_id' = workspace_id
  ),
  CONSTRAINT sandbox_cleanup_status_valid CHECK (status IN ('pending','leased','retry_wait','completed','dead_letter')),
  CONSTRAINT sandbox_cleanup_failure_code_valid CHECK (failure_code = '' OR failure_code ~ '^[a-z0-9_]{1,64}$'),
  CONSTRAINT sandbox_cleanup_fence_valid CHECK (fence >= 0 AND version > 0),
  CONSTRAINT sandbox_cleanup_lease_valid CHECK (
    (status = 'leased' AND owner_id <> '' AND lease_until IS NOT NULL) OR
    (status <> 'leased' AND owner_id = '' AND lease_until IS NULL)
  ),
  CONSTRAINT sandbox_cleanup_completion_valid CHECK (
    (status = 'completed' AND completed_at IS NOT NULL AND completion_state IN ('removed','already_absent') AND completion_owner_id <> '') OR
    (status <> 'completed' AND completed_at IS NULL AND completion_state = '' AND completion_owner_id = '')
  ),
  CONSTRAINT sandbox_cleanup_actor_bounded CHECK (octet_length(actor::text) <= 16384),
  CONSTRAINT sandbox_cleanup_identity_bounded CHECK (octet_length(execution_identity::text) <= 8192)
);

CREATE INDEX IF NOT EXISTS sandbox_cleanup_due_idx
  ON fornix.sandbox_cleanup_jobs(workspace_id, retry_at, created_at, id)
  WHERE status IN ('pending','retry_wait');
CREATE INDEX IF NOT EXISTS sandbox_cleanup_lease_expiry_idx
  ON fornix.sandbox_cleanup_jobs(workspace_id, lease_until, created_at, id)
  WHERE status = 'leased';

CREATE TABLE IF NOT EXISTS fornix.sandbox_cleanup_events (
  workspace_id TEXT NOT NULL,
  job_id TEXT NOT NULL,
  version BIGINT NOT NULL,
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  worker_id TEXT NOT NULL DEFAULT '',
  fence BIGINT NOT NULL DEFAULT 0,
  failure_code TEXT NOT NULL DEFAULT '',
  observation_state TEXT NOT NULL DEFAULT '',
  retry_at TIMESTAMPTZ,
  actor JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, job_id, version),
  FOREIGN KEY (workspace_id, job_id)
    REFERENCES fornix.sandbox_cleanup_jobs(workspace_id, id),
  CONSTRAINT sandbox_cleanup_event_kind_valid CHECK (kind IN ('created','claimed','renewed','retried','completed','dead_letter')),
  CONSTRAINT sandbox_cleanup_event_status_valid CHECK (status IN ('pending','leased','retry_wait','completed','dead_letter')),
  CONSTRAINT sandbox_cleanup_event_failure_code_valid CHECK (failure_code = '' OR failure_code ~ '^[a-z0-9_]{1,64}$'),
  CONSTRAINT sandbox_cleanup_event_observation_state_valid CHECK (observation_state IN ('','removed','already_absent','unknown','identity_mismatch')),
  CONSTRAINT sandbox_cleanup_event_fence_valid CHECK (fence >= 0 AND version > 0),
  CONSTRAINT sandbox_cleanup_event_actor_bounded CHECK (octet_length(actor::text) <= 16384)
);

CREATE INDEX IF NOT EXISTS sandbox_cleanup_events_order_idx
  ON fornix.sandbox_cleanup_events(workspace_id, job_id, version);

CREATE OR REPLACE FUNCTION fornix.reject_sandbox_cleanup_event_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'sandbox cleanup event history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS sandbox_cleanup_events_append_only ON fornix.sandbox_cleanup_events;
CREATE TRIGGER sandbox_cleanup_events_append_only
  BEFORE UPDATE OR DELETE ON fornix.sandbox_cleanup_events
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_sandbox_cleanup_event_mutation();

CREATE OR REPLACE FUNCTION fornix.reject_sandbox_cleanup_identity_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'DELETE' THEN
    RAISE EXCEPTION 'sandbox cleanup intent history cannot be deleted';
  END IF;
  IF NEW.workspace_id IS DISTINCT FROM OLD.workspace_id OR
     NEW.id IS DISTINCT FROM OLD.id OR
     NEW.tool_run_id IS DISTINCT FROM OLD.tool_run_id OR
     NEW.tool_attempt IS DISTINCT FROM OLD.tool_attempt OR
     NEW.domain_link_id IS DISTINCT FROM OLD.domain_link_id OR
     NEW.backend IS DISTINCT FROM OLD.backend OR
     NEW.execution_identity IS DISTINCT FROM OLD.execution_identity OR
     NEW.execution_identity_hash IS DISTINCT FROM OLD.execution_identity_hash OR
     NEW.intent_hash IS DISTINCT FROM OLD.intent_hash OR
     NEW.tool_request_hash IS DISTINCT FROM OLD.tool_request_hash OR
     NEW.result_hash IS DISTINCT FROM OLD.result_hash OR
     NEW.actor IS DISTINCT FROM OLD.actor OR
     NEW.created_at IS DISTINCT FROM OLD.created_at THEN
    RAISE EXCEPTION 'sandbox cleanup intent identity is immutable';
  END IF;
  IF OLD.completion_owner_id <> '' AND NEW.completion_owner_id IS DISTINCT FROM OLD.completion_owner_id THEN
    RAISE EXCEPTION 'sandbox cleanup completion owner is immutable';
  END IF;
  RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION fornix.validate_sandbox_cleanup_domain_link()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  source_kind TEXT;
  source_id TEXT;
BEGIN
  SELECT domain_kind, domain_id INTO source_kind, source_id
  FROM fornix.domain_effect_links
  WHERE workspace_id = NEW.workspace_id AND link_id = NEW.domain_link_id;
  IF NOT FOUND OR source_kind <> NEW.domain_kind OR source_id <> NEW.domain_id THEN
    RAISE EXCEPTION 'sandbox cleanup link does not identify the same workspace tool run';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS sandbox_cleanup_jobs_link_scope ON fornix.sandbox_cleanup_jobs;
CREATE TRIGGER sandbox_cleanup_jobs_link_scope
  BEFORE INSERT ON fornix.sandbox_cleanup_jobs
  FOR EACH ROW EXECUTE FUNCTION fornix.validate_sandbox_cleanup_domain_link();

DROP TRIGGER IF EXISTS sandbox_cleanup_jobs_identity_immutable ON fornix.sandbox_cleanup_jobs;
CREATE TRIGGER sandbox_cleanup_jobs_identity_immutable
  BEFORE UPDATE OR DELETE ON fornix.sandbox_cleanup_jobs
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_sandbox_cleanup_identity_mutation();

ALTER TABLE fornix.sandbox_cleanup_jobs ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.sandbox_cleanup_jobs;
CREATE POLICY workspace_scope_isolation ON fornix.sandbox_cleanup_jobs
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.sandbox_cleanup_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.sandbox_cleanup_events;
CREATE POLICY workspace_scope_isolation ON fornix.sandbox_cleanup_events
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
