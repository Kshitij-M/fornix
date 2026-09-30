-- 051: durable signed capability-schema catalogs.
--
-- Catalogs contain only schema fingerprints, versions, signatures, and audit
-- metadata. They do not contain executable validators, prompts, credentials,
-- or raw schema documents.

CREATE TABLE IF NOT EXISTS fornix.trust_schema_catalogs (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  revision BIGINT NOT NULL,
  catalog_hash TEXT NOT NULL,
  entries JSONB NOT NULL,
  signature_scheme TEXT NOT NULL,
  signer_id TEXT NOT NULL,
  signature TEXT NOT NULL,
  issued_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT trust_schema_catalogs_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT trust_schema_catalogs_revision_positive CHECK (revision > 0),
  CONSTRAINT trust_schema_catalogs_hash_valid CHECK (catalog_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT trust_schema_catalogs_entries_array CHECK (jsonb_typeof(entries) = 'array'),
  CONSTRAINT trust_schema_catalogs_scheme_valid CHECK (signature_scheme = 'ed25519'),
  CONSTRAINT trust_schema_catalogs_signer_nonempty CHECK (length(signer_id) BETWEEN 1 AND 128),
  CONSTRAINT trust_schema_catalogs_signature_nonempty CHECK (length(signature) > 0 AND length(signature) <= 512),
  CONSTRAINT trust_schema_catalogs_window_valid CHECK (expires_at > issued_at),
  CONSTRAINT trust_schema_catalogs_json_bounded CHECK (octet_length(entries::text) <= 131072 AND octet_length(actor::text) <= 4096),
  CONSTRAINT trust_schema_catalogs_revision_unique UNIQUE (workspace_id, revision),
  CONSTRAINT trust_schema_catalogs_hash_unique UNIQUE (workspace_id, catalog_hash)
);

CREATE INDEX IF NOT EXISTS trust_schema_catalogs_current_idx
  ON fornix.trust_schema_catalogs(workspace_id, revision DESC, expires_at DESC);

CREATE OR REPLACE FUNCTION fornix.reject_trust_schema_catalog_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix signed schema catalogs are append-only';
END;
$$;

DROP TRIGGER IF EXISTS trust_schema_catalogs_append_only ON fornix.trust_schema_catalogs;
CREATE TRIGGER trust_schema_catalogs_append_only
  BEFORE UPDATE OR DELETE ON fornix.trust_schema_catalogs
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_trust_schema_catalog_mutation();

ALTER TABLE fornix.trust_schema_catalogs ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.trust_schema_catalogs;
CREATE POLICY workspace_scope_isolation ON fornix.trust_schema_catalogs
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
