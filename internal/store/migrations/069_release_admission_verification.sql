-- 069: hash-only release verification and startup/admission binding.
-- Raw manifests, signatures, image layers, and credentials stay outside
-- Fornix. These rows bind deployment-owned verification facts to one release
-- and one deterministic qualification gate.

CREATE TABLE IF NOT EXISTS fornix.qualification_deployment_release_verifications (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  release_id TEXT NOT NULL,
  release_hash TEXT NOT NULL,
  target_hash TEXT NOT NULL,
  artifact_kind TEXT NOT NULL,
  artifact_hash TEXT NOT NULL,
  attestation_hash TEXT NOT NULL,
  gate_hash TEXT NOT NULL,
  trust_snapshot_revision BIGINT NOT NULL,
  trust_snapshot_hash TEXT NOT NULL,
  source_reference TEXT,
  status TEXT NOT NULL DEFAULT 'verified',
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT,
  idempotency_key TEXT NOT NULL,
  causation_id TEXT,
  correlation_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  expires_at TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ,
  CONSTRAINT qualification_verification_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_verification_deployment_nonempty CHECK (length(deployment_id) BETWEEN 1 AND 128),
  CONSTRAINT qualification_verification_release_nonempty CHECK (length(release_id) > 0),
  CONSTRAINT qualification_verification_hashes_valid CHECK (release_hash ~ '^[0-9a-f]{64}$' AND target_hash ~ '^[0-9a-f]{64}$' AND artifact_hash ~ '^[0-9a-f]{64}$' AND attestation_hash ~ '^[0-9a-f]{64}$' AND gate_hash ~ '^[0-9a-f]{64}$' AND trust_snapshot_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT qualification_verification_kind_valid CHECK (artifact_kind IN ('release','image','binary','manifest')),
  CONSTRAINT qualification_verification_status_valid CHECK (status IN ('verified','failed','revoked','expired')),
  CONSTRAINT qualification_verification_times_valid CHECK (expires_at > created_at),
  CONSTRAINT qualification_verification_actor_bounded CHECK (octet_length(actor::text) <= 4096),
  CONSTRAINT qualification_verification_source_bounded CHECK (source_reference IS NULL OR (length(source_reference) <= 256 AND source_reference !~ '[\x00\r\n]')),
  CONSTRAINT qualification_verification_idempotency_nonempty CHECK (length(idempotency_key) BETWEEN 1 AND 256),
  CONSTRAINT qualification_verification_identity_unique UNIQUE (workspace_id, deployment_id, release_id, artifact_kind),
  CONSTRAINT qualification_verification_idempotency_unique UNIQUE (workspace_id, deployment_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS qualification_verification_lookup_idx
  ON fornix.qualification_deployment_release_verifications(workspace_id, deployment_id, release_id, artifact_kind);
CREATE INDEX IF NOT EXISTS qualification_verification_expiry_idx
  ON fornix.qualification_deployment_release_verifications(workspace_id, deployment_id, expires_at);

CREATE TABLE IF NOT EXISTS fornix.qualification_deployment_release_verification_events (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  release_id TEXT NOT NULL,
  verification_id TEXT NOT NULL,
  event TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_verification_events_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_verification_events_deployment_nonempty CHECK (length(deployment_id) > 0),
  CONSTRAINT qualification_verification_events_release_nonempty CHECK (length(release_id) > 0),
  CONSTRAINT qualification_verification_events_verification_nonempty CHECK (length(verification_id) > 0),
  CONSTRAINT qualification_verification_events_event_valid CHECK (event IN ('registered','revoked')),
  CONSTRAINT qualification_verification_events_json_bounded CHECK (octet_length(actor::text) <= 4096 AND octet_length(metadata::text) <= 8192)
);

CREATE INDEX IF NOT EXISTS qualification_verification_events_lookup_idx
  ON fornix.qualification_deployment_release_verification_events(workspace_id, deployment_id, release_id, occurred_at, id);

CREATE OR REPLACE FUNCTION fornix.reject_qualification_release_verification_history_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix release verification history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS qualification_release_verification_events_append_only ON fornix.qualification_deployment_release_verification_events;
CREATE TRIGGER qualification_release_verification_events_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_deployment_release_verification_events
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_release_verification_history_mutation();

ALTER TABLE fornix.qualification_deployment_release_verifications ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_deployment_release_verifications;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_deployment_release_verifications
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.qualification_deployment_release_verification_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_deployment_release_verification_events;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_deployment_release_verification_events
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
