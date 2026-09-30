-- 073: advisory, hash-only readiness snapshots and bounded incident evidence.
-- These tables preserve operator observations without copying deployment
-- payloads or changing release admission authority.

CREATE TABLE IF NOT EXISTS fornix.qualification_readiness_snapshots (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  release_id TEXT NOT NULL,
  release_hash TEXT NOT NULL,
  trust_snapshot_revision BIGINT NOT NULL,
  trust_snapshot_hash TEXT NOT NULL,
  required_kinds JSONB NOT NULL DEFAULT '[]'::jsonb,
  active_evidence_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  missing_kinds JSONB NOT NULL DEFAULT '[]'::jsonb,
  blocked_reasons JSONB NOT NULL DEFAULT '[]'::jsonb,
  gate_hash TEXT NOT NULL,
  ready BOOLEAN NOT NULL,
  snapshot_hash TEXT NOT NULL,
  evaluated_at TIMESTAMPTZ NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  causation_id TEXT,
  correlation_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_readiness_snapshot_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_readiness_snapshot_deployment_nonempty CHECK (length(deployment_id) BETWEEN 1 AND 128),
  CONSTRAINT qualification_readiness_snapshot_release_nonempty CHECK (length(release_id) > 0),
  CONSTRAINT qualification_readiness_snapshot_hashes_valid CHECK (
    release_hash ~ '^[0-9a-f]{64}$' AND trust_snapshot_hash ~ '^[0-9a-f]{64}$' AND
    gate_hash ~ '^[0-9a-f]{64}$' AND snapshot_hash ~ '^[0-9a-f]{64}$' AND request_hash ~ '^[0-9a-f]{64}$'
  ),
  CONSTRAINT qualification_readiness_snapshot_revision_positive CHECK (trust_snapshot_revision > 0),
  CONSTRAINT qualification_readiness_snapshot_arrays_bounded CHECK (
    jsonb_typeof(required_kinds) = 'array' AND jsonb_array_length(required_kinds) BETWEEN 1 AND 8 AND
    jsonb_typeof(active_evidence_ids) = 'array' AND jsonb_array_length(active_evidence_ids) <= 16 AND
    jsonb_typeof(missing_kinds) = 'array' AND jsonb_array_length(missing_kinds) <= 8 AND
    jsonb_typeof(blocked_reasons) = 'array' AND jsonb_array_length(blocked_reasons) <= 16
  ),
  CONSTRAINT qualification_readiness_snapshot_actor_bounded CHECK (octet_length(actor::text) <= 4096),
  CONSTRAINT qualification_readiness_snapshot_idempotency_nonempty CHECK (length(idempotency_key) BETWEEN 1 AND 256),
  CONSTRAINT qualification_readiness_snapshot_request_hash_valid CHECK (request_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT qualification_readiness_snapshot_identity_unique UNIQUE (workspace_id, deployment_id, release_id, snapshot_hash),
  CONSTRAINT qualification_readiness_snapshot_idempotency_unique UNIQUE (workspace_id, deployment_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS qualification_readiness_snapshot_lookup_idx
  ON fornix.qualification_readiness_snapshots(workspace_id, deployment_id, release_id, id DESC);

CREATE TABLE IF NOT EXISTS fornix.qualification_readiness_snapshot_events (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  release_id TEXT NOT NULL,
  snapshot_id TEXT NOT NULL,
  annotation_id TEXT,
  event TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT,
  idempotency_key TEXT,
  causation_id TEXT,
  correlation_id TEXT,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_readiness_events_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_readiness_events_deployment_nonempty CHECK (length(deployment_id) > 0),
  CONSTRAINT qualification_readiness_events_release_nonempty CHECK (length(release_id) > 0),
  CONSTRAINT qualification_readiness_events_snapshot_nonempty CHECK (length(snapshot_id) > 0),
  CONSTRAINT qualification_readiness_events_event_valid CHECK (event IN ('captured','annotated')),
  CONSTRAINT qualification_readiness_events_json_bounded CHECK (octet_length(actor::text) <= 4096 AND octet_length(metadata::text) <= 8192)
);

CREATE INDEX IF NOT EXISTS qualification_readiness_events_lookup_idx
  ON fornix.qualification_readiness_snapshot_events(workspace_id, deployment_id, release_id, occurred_at, id);

CREATE TABLE IF NOT EXISTS fornix.qualification_incident_annotations (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  release_id TEXT NOT NULL,
  snapshot_id TEXT NOT NULL,
  snapshot_hash TEXT NOT NULL,
  code TEXT NOT NULL,
  disposition TEXT NOT NULL,
  reference_hash TEXT,
  annotation_hash TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  causation_id TEXT,
  correlation_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_incident_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_incident_deployment_nonempty CHECK (length(deployment_id) > 0),
  CONSTRAINT qualification_incident_release_nonempty CHECK (length(release_id) > 0),
  CONSTRAINT qualification_incident_snapshot_nonempty CHECK (length(snapshot_id) > 0),
  CONSTRAINT qualification_incident_hashes_valid CHECK (
    snapshot_hash ~ '^[0-9a-f]{64}$' AND annotation_hash ~ '^[0-9a-f]{64}$' AND
    (reference_hash IS NULL OR reference_hash ~ '^[0-9a-f]{64}$') AND request_hash ~ '^[0-9a-f]{64}$'
  ),
  CONSTRAINT qualification_incident_code_valid CHECK (length(code) BETWEEN 1 AND 128 AND code !~ '[\x00\r\n]'),
  CONSTRAINT qualification_incident_disposition_valid CHECK (disposition IN ('observed','acknowledged','mitigated','escalated')),
  CONSTRAINT qualification_incident_actor_bounded CHECK (octet_length(actor::text) <= 4096),
  CONSTRAINT qualification_incident_idempotency_nonempty CHECK (length(idempotency_key) BETWEEN 1 AND 256),
  CONSTRAINT qualification_incident_identity_unique UNIQUE (workspace_id, deployment_id, release_id, annotation_hash),
  CONSTRAINT qualification_incident_idempotency_unique UNIQUE (workspace_id, deployment_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS qualification_incident_lookup_idx
  ON fornix.qualification_incident_annotations(workspace_id, deployment_id, release_id, id DESC);
CREATE INDEX IF NOT EXISTS qualification_incident_snapshot_idx
  ON fornix.qualification_incident_annotations(workspace_id, deployment_id, snapshot_id, id DESC);

CREATE OR REPLACE FUNCTION fornix.reject_qualification_readiness_history_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix readiness history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS qualification_readiness_snapshots_append_only ON fornix.qualification_readiness_snapshots;
CREATE TRIGGER qualification_readiness_snapshots_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_readiness_snapshots
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_readiness_history_mutation();

DROP TRIGGER IF EXISTS qualification_readiness_events_append_only ON fornix.qualification_readiness_snapshot_events;
CREATE TRIGGER qualification_readiness_events_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_readiness_snapshot_events
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_readiness_history_mutation();

DROP TRIGGER IF EXISTS qualification_incident_annotations_append_only ON fornix.qualification_incident_annotations;
CREATE TRIGGER qualification_incident_annotations_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_incident_annotations
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_readiness_history_mutation();

ALTER TABLE fornix.qualification_readiness_snapshots ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_readiness_snapshots;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_readiness_snapshots
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.qualification_readiness_snapshot_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_readiness_snapshot_events;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_readiness_snapshot_events
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.qualification_incident_annotations ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_incident_annotations;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_incident_annotations
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
