-- 074: immutable workspace/deployment readiness freshness policies.
-- Policies govern operator review freshness only; they never grant release
-- admission or extend signed evidence expiry.

CREATE TABLE IF NOT EXISTS fornix.qualification_readiness_policies (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  revision BIGINT NOT NULL,
  max_age_seconds BIGINT NOT NULL,
  require_ready BOOLEAN NOT NULL DEFAULT false,
  policy_hash TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  causation_id TEXT,
  correlation_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_readiness_policy_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_readiness_policy_deployment_nonempty CHECK (length(deployment_id) BETWEEN 1 AND 128),
  CONSTRAINT qualification_readiness_policy_revision_positive CHECK (revision > 0),
  CONSTRAINT qualification_readiness_policy_age_valid CHECK (max_age_seconds BETWEEN 1 AND 2592000),
  CONSTRAINT qualification_readiness_policy_hashes_valid CHECK (policy_hash ~ '^[0-9a-f]{64}$' AND request_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT qualification_readiness_policy_actor_bounded CHECK (octet_length(actor::text) <= 4096),
  CONSTRAINT qualification_readiness_policy_idempotency_nonempty CHECK (length(idempotency_key) BETWEEN 1 AND 256),
  CONSTRAINT qualification_readiness_policy_revision_unique UNIQUE (workspace_id, deployment_id, revision),
  CONSTRAINT qualification_readiness_policy_hash_unique UNIQUE (workspace_id, deployment_id, policy_hash),
  CONSTRAINT qualification_readiness_policy_idempotency_unique UNIQUE (workspace_id, deployment_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS qualification_readiness_policy_lookup_idx
  ON fornix.qualification_readiness_policies(workspace_id, deployment_id, revision DESC, id DESC);

CREATE TABLE IF NOT EXISTS fornix.qualification_readiness_policy_events (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  policy_id TEXT NOT NULL,
  revision BIGINT NOT NULL,
  event TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT,
  idempotency_key TEXT,
  causation_id TEXT,
  correlation_id TEXT,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_readiness_policy_events_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_readiness_policy_events_deployment_nonempty CHECK (length(deployment_id) > 0),
  CONSTRAINT qualification_readiness_policy_events_policy_nonempty CHECK (length(policy_id) > 0),
  CONSTRAINT qualification_readiness_policy_events_revision_positive CHECK (revision > 0),
  CONSTRAINT qualification_readiness_policy_events_event_valid CHECK (event IN ('published')),
  CONSTRAINT qualification_readiness_policy_events_json_bounded CHECK (octet_length(actor::text) <= 4096 AND octet_length(metadata::text) <= 8192)
);

CREATE INDEX IF NOT EXISTS qualification_readiness_policy_events_lookup_idx
  ON fornix.qualification_readiness_policy_events(workspace_id, deployment_id, occurred_at, id);

CREATE OR REPLACE FUNCTION fornix.reject_qualification_readiness_policy_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix readiness freshness policy history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS qualification_readiness_policies_append_only ON fornix.qualification_readiness_policies;
CREATE TRIGGER qualification_readiness_policies_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_readiness_policies
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_readiness_policy_mutation();

DROP TRIGGER IF EXISTS qualification_readiness_policy_events_append_only ON fornix.qualification_readiness_policy_events;
CREATE TRIGGER qualification_readiness_policy_events_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_readiness_policy_events
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_readiness_policy_mutation();

ALTER TABLE fornix.qualification_readiness_policies ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_readiness_policies;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_readiness_policies
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.qualification_readiness_policy_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_readiness_policy_events;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_readiness_policy_events
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
