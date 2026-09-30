-- 052: bind schema-catalog and managed-credential source facts to effect
-- reservations and cross-authority operation links. Empty defaults preserve
-- legacy rows and their pre-052 canonical hashes.

ALTER TABLE fornix.operation_authority_links
  ADD COLUMN IF NOT EXISTS schema_catalog_hash TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS schema_catalog_revision TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS credential_source_version TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS credential_source_expires_at TIMESTAMPTZ;

ALTER TABLE fornix.operation_authority_links
  DROP CONSTRAINT IF EXISTS operation_authority_links_schema_catalog_pair_valid,
  DROP CONSTRAINT IF EXISTS operation_authority_links_source_facts_valid;

ALTER TABLE fornix.operation_authority_links
  ADD CONSTRAINT operation_authority_links_schema_catalog_pair_valid CHECK (
    (schema_catalog_hash = '' AND schema_catalog_revision = '') OR
    (schema_catalog_hash ~ '^[0-9a-f]{64}$' AND length(schema_catalog_revision) BETWEEN 1 AND 64)
  ),
  ADD CONSTRAINT operation_authority_links_source_facts_valid CHECK (
    (credential_source_version = '' AND credential_source_expires_at IS NULL) OR
    (credential_source_version <> '' AND length(credential_source_version) BETWEEN 1 AND 128)
  );

CREATE INDEX IF NOT EXISTS operation_authority_links_schema_catalog_idx
  ON fornix.operation_authority_links(workspace_id, schema_catalog_hash, schema_catalog_revision)
  WHERE schema_catalog_hash <> '';

ALTER TABLE fornix.operation_effects
  ADD COLUMN IF NOT EXISTS schema_catalog_hash TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS schema_catalog_revision TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS credential_lease_id TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS credential_lease_fence BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS credential_revocation_epoch BIGINT NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS credential_source_version TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS credential_source_expires_at TIMESTAMPTZ;

ALTER TABLE fornix.operation_effects
  DROP CONSTRAINT IF EXISTS operation_effects_authority_schema_pair_valid,
  DROP CONSTRAINT IF EXISTS operation_effects_authority_credential_valid;

ALTER TABLE fornix.operation_effects
  ADD CONSTRAINT operation_effects_authority_schema_pair_valid CHECK (
    (schema_catalog_hash = '' AND schema_catalog_revision = '') OR
    (schema_catalog_hash ~ '^[0-9a-f]{64}$' AND length(schema_catalog_revision) BETWEEN 1 AND 64)
  ),
  ADD CONSTRAINT operation_effects_authority_credential_valid CHECK (
    (credential_lease_id = '' AND credential_lease_fence = 0 AND credential_revocation_epoch = 0 AND credential_source_version = '' AND credential_source_expires_at IS NULL) OR
    (credential_lease_id <> '' AND credential_lease_fence > 0 AND credential_revocation_epoch > 0 AND length(credential_source_version) BETWEEN 1 AND 128)
  );

CREATE INDEX IF NOT EXISTS operation_effects_authority_idx
  ON fornix.operation_effects(workspace_id, schema_catalog_hash, credential_lease_id)
  WHERE schema_catalog_hash <> '' OR credential_lease_id <> '';
