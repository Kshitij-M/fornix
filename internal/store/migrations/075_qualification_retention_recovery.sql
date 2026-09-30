-- 075: qualification retention metadata and read-only recovery evidence.
-- These tables are operational overlays. Readiness, incident, and freshness
-- policy history remains authoritative and append-only in migrations 073/074.

CREATE TABLE IF NOT EXISTS fornix.qualification_retention_policies (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  revision BIGINT NOT NULL,
  snapshot_retention_seconds BIGINT NOT NULL,
  incident_retention_seconds BIGINT NOT NULL,
  policy_retention_seconds BIGINT NOT NULL,
  keep_latest_snapshots INTEGER NOT NULL,
  keep_latest_incidents INTEGER NOT NULL,
  protect_incidents BOOLEAN NOT NULL DEFAULT true,
  policy_hash TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  causation_id TEXT,
  correlation_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_retention_policy_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_retention_policy_deployment_nonempty CHECK (length(deployment_id) BETWEEN 1 AND 128),
  CONSTRAINT qualification_retention_policy_revision_positive CHECK (revision > 0),
  CONSTRAINT qualification_retention_policy_durations_valid CHECK (
    snapshot_retention_seconds BETWEEN 86400 AND 315360000 AND
    incident_retention_seconds BETWEEN 86400 AND 315360000 AND
    policy_retention_seconds BETWEEN 86400 AND 315360000
  ),
  CONSTRAINT qualification_retention_policy_counts_valid CHECK (
    keep_latest_snapshots BETWEEN 1 AND 1000 AND keep_latest_incidents BETWEEN 1 AND 1000
  ),
  CONSTRAINT qualification_retention_policy_hashes_valid CHECK (
    policy_hash ~ '^[0-9a-f]{64}$' AND request_hash ~ '^[0-9a-f]{64}$'
  ),
  CONSTRAINT qualification_retention_policy_actor_bounded CHECK (octet_length(actor::text) <= 4096),
  CONSTRAINT qualification_retention_policy_idempotency_nonempty CHECK (length(idempotency_key) BETWEEN 1 AND 256),
  CONSTRAINT qualification_retention_policy_revision_unique UNIQUE (workspace_id, deployment_id, revision),
  CONSTRAINT qualification_retention_policy_hash_unique UNIQUE (workspace_id, deployment_id, policy_hash),
  CONSTRAINT qualification_retention_policy_idempotency_unique UNIQUE (workspace_id, deployment_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS qualification_retention_policy_lookup_idx
  ON fornix.qualification_retention_policies(workspace_id, deployment_id, revision DESC, id DESC);

CREATE TABLE IF NOT EXISTS fornix.qualification_retention_metadata (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  record_kind TEXT NOT NULL,
  record_id TEXT NOT NULL,
  record_hash TEXT NOT NULL,
  policy_id TEXT,
  policy_revision BIGINT NOT NULL DEFAULT 0,
  retain_until TIMESTAMPTZ NOT NULL,
  metadata_hash TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT,
  idempotency_key TEXT NOT NULL,
  causation_id TEXT,
  correlation_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_retention_metadata_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_retention_metadata_deployment_nonempty CHECK (length(deployment_id) BETWEEN 1 AND 128),
  CONSTRAINT qualification_retention_metadata_kind_valid CHECK (record_kind IN ('readiness_snapshot','incident_annotation','freshness_policy')),
  CONSTRAINT qualification_retention_metadata_record_nonempty CHECK (length(record_id) BETWEEN 1 AND 256),
  CONSTRAINT qualification_retention_metadata_hashes_valid CHECK (
    record_hash ~ '^[0-9a-f]{64}$' AND metadata_hash ~ '^[0-9a-f]{64}$'
  ),
  CONSTRAINT qualification_retention_metadata_policy_valid CHECK ((policy_id IS NULL AND policy_revision = 0) OR (policy_id IS NOT NULL AND policy_revision > 0)),
  CONSTRAINT qualification_retention_metadata_actor_bounded CHECK (octet_length(actor::text) <= 4096),
  CONSTRAINT qualification_retention_metadata_idempotency_nonempty CHECK (length(idempotency_key) BETWEEN 1 AND 256),
  CONSTRAINT qualification_retention_metadata_identity_unique UNIQUE (workspace_id, deployment_id, record_kind, record_id),
  CONSTRAINT qualification_retention_metadata_hash_unique UNIQUE (workspace_id, deployment_id, record_kind, metadata_hash)
);

CREATE INDEX IF NOT EXISTS qualification_retention_metadata_due_idx
  ON fornix.qualification_retention_metadata(workspace_id, deployment_id, retain_until, record_kind, record_id);
CREATE INDEX IF NOT EXISTS qualification_retention_metadata_record_idx
  ON fornix.qualification_retention_metadata(workspace_id, deployment_id, record_kind, record_id);

CREATE TABLE IF NOT EXISTS fornix.qualification_retention_events (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  record_kind TEXT NOT NULL,
  record_id TEXT NOT NULL,
  event TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT,
  idempotency_key TEXT,
  causation_id TEXT,
  correlation_id TEXT,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_retention_events_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_retention_events_deployment_nonempty CHECK (length(deployment_id) > 0),
  CONSTRAINT qualification_retention_events_kind_valid CHECK (record_kind IN ('readiness_snapshot','incident_annotation','freshness_policy','retention_policy')),
  CONSTRAINT qualification_retention_events_record_nonempty CHECK (length(record_id) > 0),
  CONSTRAINT qualification_retention_events_event_valid CHECK (event IN ('policy_published','metadata_registered')),
  CONSTRAINT qualification_retention_events_json_bounded CHECK (octet_length(actor::text) <= 4096 AND octet_length(metadata::text) <= 8192)
);

CREATE INDEX IF NOT EXISTS qualification_retention_events_lookup_idx
  ON fornix.qualification_retention_events(workspace_id, deployment_id, occurred_at, id);

CREATE OR REPLACE FUNCTION fornix.reject_qualification_retention_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix qualification retention history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS qualification_retention_policies_append_only ON fornix.qualification_retention_policies;
CREATE TRIGGER qualification_retention_policies_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_retention_policies
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_retention_mutation();

DROP TRIGGER IF EXISTS qualification_retention_metadata_append_only ON fornix.qualification_retention_metadata;
CREATE TRIGGER qualification_retention_metadata_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_retention_metadata
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_retention_mutation();

DROP TRIGGER IF EXISTS qualification_retention_events_append_only ON fornix.qualification_retention_events;
CREATE TRIGGER qualification_retention_events_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_retention_events
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_retention_mutation();

ALTER TABLE fornix.qualification_retention_policies ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_retention_policies;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_retention_policies
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.qualification_retention_metadata ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_retention_metadata;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_retention_metadata
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.qualification_retention_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_retention_events;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_retention_events
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
