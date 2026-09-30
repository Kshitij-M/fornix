-- 068: durable deployment release/evidence references and qualification gate.
-- Raw signed evidence remains owned by qualification_imports; these rows only
-- index immutable identities and redacted hashes for deterministic gating.

CREATE TABLE IF NOT EXISTS fornix.qualification_deployment_releases (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  release_hash TEXT NOT NULL,
  target_hash TEXT NOT NULL,
  version TEXT NOT NULL,
  commit_hash TEXT,
  trust_snapshot_revision BIGINT NOT NULL,
  trust_snapshot_hash TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT,
  idempotency_key TEXT NOT NULL,
  causation_id TEXT,
  correlation_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_release_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_release_deployment_nonempty CHECK (length(deployment_id) BETWEEN 1 AND 128),
  CONSTRAINT qualification_release_hashes_valid CHECK (release_hash ~ '^[0-9a-f]{64}$' AND target_hash ~ '^[0-9a-f]{64}$' AND trust_snapshot_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT qualification_release_version_nonempty CHECK (length(version) BETWEEN 1 AND 128),
  CONSTRAINT qualification_release_revision_positive CHECK (trust_snapshot_revision > 0),
  CONSTRAINT qualification_release_status_valid CHECK (status IN ('active','retired')),
  CONSTRAINT qualification_release_actor_bounded CHECK (octet_length(actor::text) <= 4096),
  CONSTRAINT qualification_release_idempotency_nonempty CHECK (length(idempotency_key) BETWEEN 1 AND 256),
  CONSTRAINT qualification_release_identity_unique UNIQUE (workspace_id, deployment_id, release_hash),
  CONSTRAINT qualification_release_idempotency_unique UNIQUE (workspace_id, deployment_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS qualification_release_lookup_idx
  ON fornix.qualification_deployment_releases(workspace_id, deployment_id, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS fornix.qualification_deployment_release_events (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  release_id TEXT NOT NULL,
  event TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_release_events_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_release_events_deployment_nonempty CHECK (length(deployment_id) > 0),
  CONSTRAINT qualification_release_events_release_nonempty CHECK (length(release_id) > 0),
  CONSTRAINT qualification_release_events_event_valid CHECK (event IN ('registered','evidence_linked')),
  CONSTRAINT qualification_release_events_json_bounded CHECK (octet_length(actor::text) <= 4096 AND octet_length(metadata::text) <= 8192)
);

CREATE INDEX IF NOT EXISTS qualification_release_events_lookup_idx
  ON fornix.qualification_deployment_release_events(workspace_id, deployment_id, occurred_at, id);

CREATE TABLE IF NOT EXISTS fornix.qualification_deployment_evidence_links (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  release_id TEXT NOT NULL,
  kind TEXT NOT NULL,
  import_id TEXT NOT NULL,
  target_hash TEXT NOT NULL,
  signed_hash TEXT NOT NULL,
  observation_hash TEXT NOT NULL,
  report_hash TEXT NOT NULL,
  manifest_hash TEXT NOT NULL,
  source_hash TEXT NOT NULL,
  trust_snapshot_revision BIGINT NOT NULL,
  trust_snapshot_hash TEXT NOT NULL,
  outcome TEXT NOT NULL,
  recovery_state TEXT NOT NULL DEFAULT 'not_applicable',
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT,
  idempotency_key TEXT NOT NULL,
  causation_id TEXT,
  correlation_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_evidence_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_evidence_deployment_nonempty CHECK (length(deployment_id) BETWEEN 1 AND 128),
  CONSTRAINT qualification_evidence_release_nonempty CHECK (length(release_id) > 0),
  CONSTRAINT qualification_evidence_kind_valid CHECK (kind IN ('release','migration','backup_restore','topology','provider','external_effect')),
  CONSTRAINT qualification_evidence_import_nonempty CHECK (length(import_id) > 0),
  CONSTRAINT qualification_evidence_hashes_valid CHECK (target_hash ~ '^[0-9a-f]{64}$' AND signed_hash ~ '^[0-9a-f]{64}$' AND observation_hash ~ '^[0-9a-f]{64}$' AND report_hash ~ '^[0-9a-f]{64}$' AND manifest_hash ~ '^[0-9a-f]{64}$' AND source_hash ~ '^[0-9a-f]{64}$' AND trust_snapshot_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT qualification_evidence_revision_positive CHECK (trust_snapshot_revision > 0),
  CONSTRAINT qualification_evidence_outcome_valid CHECK (outcome IN ('passed','failed','blocked','skipped')),
  CONSTRAINT qualification_evidence_recovery_valid CHECK (recovery_state IN ('resolved','recovery_required','unknown','not_applicable')),
  CONSTRAINT qualification_evidence_actor_bounded CHECK (octet_length(actor::text) <= 4096),
  CONSTRAINT qualification_evidence_idempotency_nonempty CHECK (length(idempotency_key) BETWEEN 1 AND 256),
  CONSTRAINT qualification_evidence_kind_unique UNIQUE (workspace_id, deployment_id, release_id, kind),
  CONSTRAINT qualification_evidence_idempotency_unique UNIQUE (workspace_id, deployment_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS qualification_evidence_release_lookup_idx
  ON fornix.qualification_deployment_evidence_links(workspace_id, deployment_id, release_id, kind);
CREATE INDEX IF NOT EXISTS qualification_evidence_import_lookup_idx
  ON fornix.qualification_deployment_evidence_links(workspace_id, deployment_id, import_id);

CREATE TABLE IF NOT EXISTS fornix.qualification_deployment_evidence_events (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  release_id TEXT NOT NULL,
  evidence_id TEXT NOT NULL,
  event TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_evidence_events_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_evidence_events_deployment_nonempty CHECK (length(deployment_id) > 0),
  CONSTRAINT qualification_evidence_events_release_nonempty CHECK (length(release_id) > 0),
  CONSTRAINT qualification_evidence_events_evidence_nonempty CHECK (length(evidence_id) > 0),
  CONSTRAINT qualification_evidence_events_event_valid CHECK (event IN ('linked')),
  CONSTRAINT qualification_evidence_events_json_bounded CHECK (octet_length(actor::text) <= 4096 AND octet_length(metadata::text) <= 8192)
);

CREATE INDEX IF NOT EXISTS qualification_evidence_events_lookup_idx
  ON fornix.qualification_deployment_evidence_events(workspace_id, deployment_id, occurred_at, id);

CREATE OR REPLACE FUNCTION fornix.reject_qualification_deployment_evidence_history_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix deployment qualification history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS qualification_release_events_append_only ON fornix.qualification_deployment_release_events;
CREATE TRIGGER qualification_release_events_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_deployment_release_events
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_deployment_evidence_history_mutation();

DROP TRIGGER IF EXISTS qualification_evidence_events_append_only ON fornix.qualification_deployment_evidence_events;
CREATE TRIGGER qualification_evidence_events_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_deployment_evidence_events
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_deployment_evidence_history_mutation();

ALTER TABLE fornix.qualification_deployment_releases ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_deployment_releases;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_deployment_releases
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.qualification_deployment_release_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_deployment_release_events;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_deployment_release_events
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.qualification_deployment_evidence_links ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_deployment_evidence_links;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_deployment_evidence_links
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.qualification_deployment_evidence_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_deployment_evidence_events;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_deployment_evidence_events
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
