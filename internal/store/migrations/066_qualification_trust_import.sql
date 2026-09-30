-- 066: deployment-owned trust lifecycle and authorized qualification imports.
--
-- This catalog is separate from connector/capability trust. It stores public
-- verification material, bounded evidence bytes, and audit metadata only;
-- private signing keys and deployment secrets never enter PostgreSQL.

CREATE TABLE IF NOT EXISTS fornix.qualification_trusted_signers (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  key_id TEXT NOT NULL,
  signature_scheme TEXT NOT NULL DEFAULT 'ed25519',
  public_key BYTEA NOT NULL,
  public_key_hash TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  supersedes_key_id TEXT,
  superseded_by_key_id TEXT,
  valid_from TIMESTAMPTZ NOT NULL,
  valid_until TIMESTAMPTZ NOT NULL,
  revoked_at TIMESTAMPTZ,
  superseded_at TIMESTAMPTZ,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_signers_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_signers_deployment_nonempty CHECK (length(deployment_id) BETWEEN 1 AND 128),
  CONSTRAINT qualification_signers_key_nonempty CHECK (length(key_id) BETWEEN 1 AND 128),
  CONSTRAINT qualification_signers_scheme_valid CHECK (signature_scheme = 'ed25519'),
  CONSTRAINT qualification_signers_public_key_length CHECK (octet_length(public_key) = 32),
  CONSTRAINT qualification_signers_public_hash_valid CHECK (public_key_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT qualification_signers_status_valid CHECK (status IN ('active','revoked','superseded')),
  CONSTRAINT qualification_signers_window_valid CHECK (valid_until > valid_from),
  CONSTRAINT qualification_signers_actor_bounded CHECK (octet_length(actor::text) <= 4096),
  CONSTRAINT qualification_signers_scope_key_unique UNIQUE (workspace_id, deployment_id, key_id),
  CONSTRAINT qualification_signers_scope_public_unique UNIQUE (workspace_id, deployment_id, public_key_hash)
);

CREATE INDEX IF NOT EXISTS qualification_signers_active_lookup_idx
  ON fornix.qualification_trusted_signers(workspace_id, deployment_id, key_id, status, valid_from, valid_until);

CREATE TABLE IF NOT EXISTS fornix.qualification_trusted_signer_events (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  signer_id TEXT NOT NULL,
  key_id TEXT NOT NULL,
  event TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_signer_events_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_signer_events_deployment_nonempty CHECK (length(deployment_id) > 0),
  CONSTRAINT qualification_signer_events_signer_nonempty CHECK (length(signer_id) > 0),
  CONSTRAINT qualification_signer_events_key_nonempty CHECK (length(key_id) > 0),
  CONSTRAINT qualification_signer_events_event_valid CHECK (event IN ('registered','superseded','revoked')),
  CONSTRAINT qualification_signer_events_json_bounded CHECK (octet_length(actor::text) <= 4096 AND octet_length(metadata::text) <= 8192)
);

CREATE INDEX IF NOT EXISTS qualification_signer_events_lookup_idx
  ON fornix.qualification_trusted_signer_events(workspace_id, deployment_id, occurred_at, id);

CREATE TABLE IF NOT EXISTS fornix.qualification_imports (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  target_hash TEXT NOT NULL,
  key_id TEXT NOT NULL,
  signer_record_id TEXT NOT NULL,
  signed_hash TEXT NOT NULL,
  observation_hash TEXT NOT NULL,
  source_hash TEXT NOT NULL,
  source_reference TEXT,
  request_id TEXT,
  idempotency_key TEXT NOT NULL,
  causation_id TEXT,
  correlation_id TEXT,
  status TEXT NOT NULL DEFAULT 'accepted',
  signed_bytes BYTEA NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_imports_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_imports_deployment_nonempty CHECK (length(deployment_id) > 0),
  CONSTRAINT qualification_imports_hash_valid CHECK (target_hash ~ '^[0-9a-f]{64}$' AND signed_hash ~ '^[0-9a-f]{64}$' AND observation_hash ~ '^[0-9a-f]{64}$' AND source_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT qualification_imports_key_nonempty CHECK (length(key_id) BETWEEN 1 AND 128),
  CONSTRAINT qualification_imports_signer_nonempty CHECK (length(signer_record_id) BETWEEN 1 AND 128),
  CONSTRAINT qualification_imports_idempotency_nonempty CHECK (length(idempotency_key) BETWEEN 1 AND 256),
  CONSTRAINT qualification_imports_status_valid CHECK (status = 'accepted'),
  CONSTRAINT qualification_imports_bytes_bounded CHECK (octet_length(signed_bytes) BETWEEN 1 AND 131072),
  CONSTRAINT qualification_imports_actor_bounded CHECK (octet_length(actor::text) <= 4096),
  CONSTRAINT qualification_imports_scope_signed_unique UNIQUE (workspace_id, deployment_id, signed_hash),
  CONSTRAINT qualification_imports_scope_idempotency_unique UNIQUE (workspace_id, deployment_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS qualification_imports_lookup_idx
  ON fornix.qualification_imports(workspace_id, deployment_id, created_at DESC, id DESC);

CREATE TABLE IF NOT EXISTS fornix.qualification_import_events (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  import_id TEXT NOT NULL,
  event TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_import_events_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_import_events_deployment_nonempty CHECK (length(deployment_id) > 0),
  CONSTRAINT qualification_import_events_import_nonempty CHECK (length(import_id) > 0),
  CONSTRAINT qualification_import_events_event_valid CHECK (event IN ('accepted')),
  CONSTRAINT qualification_import_events_json_bounded CHECK (octet_length(actor::text) <= 4096 AND octet_length(metadata::text) <= 8192)
);

CREATE INDEX IF NOT EXISTS qualification_import_events_lookup_idx
  ON fornix.qualification_import_events(workspace_id, deployment_id, occurred_at, id);

CREATE OR REPLACE FUNCTION fornix.reject_qualification_import_history_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix qualification import history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS qualification_signer_events_append_only ON fornix.qualification_trusted_signer_events;
CREATE TRIGGER qualification_signer_events_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_trusted_signer_events
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_import_history_mutation();

DROP TRIGGER IF EXISTS qualification_imports_append_only ON fornix.qualification_imports;
CREATE TRIGGER qualification_imports_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_imports
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_import_history_mutation();

DROP TRIGGER IF EXISTS qualification_import_events_append_only ON fornix.qualification_import_events;
CREATE TRIGGER qualification_import_events_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_import_events
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_import_history_mutation();

ALTER TABLE fornix.qualification_trusted_signers ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_trusted_signers;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_trusted_signers
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.qualification_trusted_signer_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_trusted_signer_events;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_trusted_signer_events
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.qualification_imports ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_imports;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_imports
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.qualification_import_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_import_events;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_import_events
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
