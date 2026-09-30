-- 050: bind credential leases to the opaque version returned by the
-- external secret authority. The value is metadata only; secret bytes never
-- enter PostgreSQL.

ALTER TABLE fornix.credential_leases
  ADD COLUMN IF NOT EXISTS source_version TEXT NOT NULL DEFAULT 'local-unversioned';

ALTER TABLE fornix.credential_leases
  ADD COLUMN IF NOT EXISTS source_expires_at TIMESTAMPTZ;

ALTER TABLE fornix.credential_leases
  DROP CONSTRAINT IF EXISTS credential_leases_source_version_valid;

ALTER TABLE fornix.credential_leases
  ADD CONSTRAINT credential_leases_source_version_valid
  CHECK (length(source_version) BETWEEN 1 AND 128);

CREATE INDEX IF NOT EXISTS credential_leases_source_version_idx
  ON fornix.credential_leases(workspace_id, credential_ref_id, source_version, fence DESC);
