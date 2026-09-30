-- 048: durable workspace-scoped signer and signed connector/capability catalog.
--
-- The catalog stores public verification material and signed policy metadata;
-- it never stores private signing keys or provider credentials. Rows are
-- append-only so a deployment can audit exactly which trust snapshot was
-- published, even after a signer is revoked.

CREATE TABLE IF NOT EXISTS fornix.trust_signers (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  signer_id TEXT NOT NULL,
  signature_scheme TEXT NOT NULL,
  public_key BYTEA NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  revoked_at TIMESTAMPTZ,
  CONSTRAINT trust_signers_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT trust_signers_id_nonempty CHECK (length(signer_id) BETWEEN 1 AND 128),
  CONSTRAINT trust_signers_scheme_valid CHECK (signature_scheme = 'ed25519'),
  CONSTRAINT trust_signers_key_length CHECK (octet_length(public_key) = 32),
  CONSTRAINT trust_signers_status_valid CHECK (status IN ('active','revoked')),
  CONSTRAINT trust_signers_unique_key UNIQUE (workspace_id, signer_id, public_key)
);

CREATE UNIQUE INDEX IF NOT EXISTS trust_signers_active_id_uq
  ON fornix.trust_signers(workspace_id, signer_id)
  WHERE status = 'active';

CREATE TABLE IF NOT EXISTS fornix.trust_signer_events (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  signer_id TEXT NOT NULL,
  event TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT trust_signer_events_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT trust_signer_events_signer_nonempty CHECK (length(signer_id) > 0),
  CONSTRAINT trust_signer_events_event_valid CHECK (event IN ('registered','revoked')),
  CONSTRAINT trust_signer_events_json_bounded CHECK (octet_length(actor::text) <= 4096 AND octet_length(metadata::text) <= 8192)
);

CREATE INDEX IF NOT EXISTS trust_signer_events_lookup_idx
  ON fornix.trust_signer_events(workspace_id, signer_id, occurred_at, id);

CREATE TABLE IF NOT EXISTS fornix.trust_policies (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  revision BIGINT NOT NULL,
  policy_hash TEXT NOT NULL,
  entries JSONB NOT NULL,
  signature_scheme TEXT NOT NULL,
  signer_id TEXT NOT NULL,
  signature TEXT NOT NULL,
  issued_at TIMESTAMPTZ NOT NULL,
  expires_at TIMESTAMPTZ NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT trust_policies_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT trust_policies_revision_positive CHECK (revision > 0),
  CONSTRAINT trust_policies_hash_valid CHECK (policy_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT trust_policies_entries_array CHECK (jsonb_typeof(entries) = 'array'),
  CONSTRAINT trust_policies_scheme_valid CHECK (signature_scheme = 'ed25519'),
  CONSTRAINT trust_policies_signer_nonempty CHECK (length(signer_id) BETWEEN 1 AND 128),
  CONSTRAINT trust_policies_signature_nonempty CHECK (length(signature) > 0 AND length(signature) <= 512),
  CONSTRAINT trust_policies_window_valid CHECK (expires_at > issued_at),
  CONSTRAINT trust_policies_json_bounded CHECK (octet_length(entries::text) <= 1048576 AND octet_length(actor::text) <= 4096),
  CONSTRAINT trust_policies_workspace_revision_unique UNIQUE (workspace_id, revision),
  CONSTRAINT trust_policies_workspace_hash_unique UNIQUE (workspace_id, policy_hash)
);

CREATE INDEX IF NOT EXISTS trust_policies_current_idx
  ON fornix.trust_policies(workspace_id, revision DESC, expires_at DESC);

CREATE OR REPLACE FUNCTION fornix.reject_trust_catalog_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix trust catalog history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS trust_policies_append_only ON fornix.trust_policies;
CREATE TRIGGER trust_policies_append_only
  BEFORE UPDATE OR DELETE ON fornix.trust_policies
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_trust_catalog_mutation();

DROP TRIGGER IF EXISTS trust_signer_events_append_only ON fornix.trust_signer_events;
CREATE TRIGGER trust_signer_events_append_only
  BEFORE UPDATE OR DELETE ON fornix.trust_signer_events
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_trust_catalog_mutation();

ALTER TABLE fornix.trust_signers ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.trust_signers;
CREATE POLICY workspace_scope_isolation ON fornix.trust_signers
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.trust_signer_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.trust_signer_events;
CREATE POLICY workspace_scope_isolation ON fornix.trust_signer_events
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.trust_policies ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.trust_policies;
CREATE POLICY workspace_scope_isolation ON fornix.trust_policies
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
