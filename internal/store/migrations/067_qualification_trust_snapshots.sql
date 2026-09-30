-- 067: deployment qualification trust distribution and import binding.
--
-- Snapshot publication is separately auditable from the Task 79 signer
-- catalog. The catalog authorizes snapshot publishers; the snapshot is the
-- bounded signer set used by new qualification imports.

CREATE TABLE IF NOT EXISTS fornix.qualification_trust_snapshots (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  revision BIGINT NOT NULL,
  snapshot_hash TEXT NOT NULL,
  source_hash TEXT NOT NULL,
  source_reference TEXT,
  signer_key_id TEXT NOT NULL,
  signature_scheme TEXT NOT NULL DEFAULT 'ed25519',
  signer_public_key BYTEA NOT NULL,
  signature TEXT NOT NULL,
  issued_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  signed_bytes BYTEA NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT,
  idempotency_key TEXT NOT NULL,
  causation_id TEXT,
  correlation_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_snapshot_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_snapshot_deployment_nonempty CHECK (length(deployment_id) BETWEEN 1 AND 128),
  CONSTRAINT qualification_snapshot_revision_positive CHECK (revision > 0),
  CONSTRAINT qualification_snapshot_hashes_valid CHECK (snapshot_hash ~ '^[0-9a-f]{64}$' AND source_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT qualification_snapshot_signer_nonempty CHECK (length(signer_key_id) BETWEEN 1 AND 128),
  CONSTRAINT qualification_snapshot_scheme_valid CHECK (signature_scheme = 'ed25519'),
  CONSTRAINT qualification_snapshot_public_key_length CHECK (octet_length(signer_public_key) = 32),
  CONSTRAINT qualification_snapshot_signature_valid CHECK (signature ~ '^[0-9a-f]{128}$'),
  CONSTRAINT qualification_snapshot_window_valid CHECK (expires_at > issued_at),
  CONSTRAINT qualification_snapshot_status_valid CHECK (status IN ('active','revoked')),
  CONSTRAINT qualification_snapshot_bytes_bounded CHECK (octet_length(signed_bytes) BETWEEN 1 AND 131072),
  CONSTRAINT qualification_snapshot_idempotency_nonempty CHECK (length(idempotency_key) BETWEEN 1 AND 256),
  CONSTRAINT qualification_snapshot_actor_bounded CHECK (octet_length(actor::text) <= 4096),
  CONSTRAINT qualification_snapshot_revision_unique UNIQUE (workspace_id, deployment_id, revision),
  CONSTRAINT qualification_snapshot_hash_unique UNIQUE (workspace_id, deployment_id, snapshot_hash),
  CONSTRAINT qualification_snapshot_idempotency_unique UNIQUE (workspace_id, deployment_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS qualification_snapshot_current_idx
  ON fornix.qualification_trust_snapshots(workspace_id, deployment_id, status, expires_at, revision DESC);

CREATE TABLE IF NOT EXISTS fornix.qualification_trust_snapshot_events (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  snapshot_id TEXT NOT NULL,
  revision BIGINT NOT NULL,
  event TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_snapshot_events_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_snapshot_events_deployment_nonempty CHECK (length(deployment_id) > 0),
  CONSTRAINT qualification_snapshot_events_snapshot_nonempty CHECK (length(snapshot_id) > 0),
  CONSTRAINT qualification_snapshot_events_revision_positive CHECK (revision > 0),
  CONSTRAINT qualification_snapshot_events_event_valid CHECK (event IN ('published','revoked')),
  CONSTRAINT qualification_snapshot_events_json_bounded CHECK (octet_length(actor::text) <= 4096 AND octet_length(metadata::text) <= 8192)
);

CREATE INDEX IF NOT EXISTS qualification_snapshot_events_lookup_idx
  ON fornix.qualification_trust_snapshot_events(workspace_id, deployment_id, occurred_at, id);

CREATE OR REPLACE FUNCTION fornix.reject_qualification_snapshot_event_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix qualification snapshot history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS qualification_snapshot_events_append_only ON fornix.qualification_trust_snapshot_events;
CREATE TRIGGER qualification_snapshot_events_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_trust_snapshot_events
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_snapshot_event_mutation();

ALTER TABLE fornix.qualification_imports
  ADD COLUMN IF NOT EXISTS trust_snapshot_revision BIGINT,
  ADD COLUMN IF NOT EXISTS trust_snapshot_hash TEXT;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname = 'qualification_imports_snapshot_binding_valid'
      AND conrelid = 'fornix.qualification_imports'::regclass
  ) THEN
    ALTER TABLE fornix.qualification_imports
      ADD CONSTRAINT qualification_imports_snapshot_binding_valid
      CHECK (
        (trust_snapshot_revision IS NULL AND trust_snapshot_hash IS NULL)
        OR (trust_snapshot_revision > 0 AND trust_snapshot_hash ~ '^[0-9a-f]{64}$')
      );
  END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS qualification_imports_snapshot_lookup_idx
  ON fornix.qualification_imports(workspace_id, deployment_id, trust_snapshot_revision, trust_snapshot_hash);

ALTER TABLE fornix.qualification_trust_snapshots ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_trust_snapshots;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_trust_snapshots
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.qualification_trust_snapshot_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_trust_snapshot_events;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_trust_snapshot_events
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
