-- Migration 064: bounded retention for operational federation history.
-- Authoritative peer commands and control events remain append-only. Expiring
-- a poll/quarantine row first records an immutable redacted tombstone.

ALTER TABLE fornix.workspace_federation_poll_attempts
  ADD COLUMN IF NOT EXISTS retention_class TEXT NOT NULL DEFAULT 'operational',
  ADD COLUMN IF NOT EXISTS retention_state TEXT NOT NULL DEFAULT 'active',
  ADD COLUMN IF NOT EXISTS retention_deadline TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS tombstone_hash TEXT NOT NULL DEFAULT '';

UPDATE fornix.workspace_federation_poll_attempts
SET retention_deadline = COALESCE(completed_at, updated_at) + INTERVAL '30 days'
WHERE retention_deadline IS NULL AND state IN ('succeeded','failed');

ALTER TABLE fornix.workspace_federation_poll_attempts
  DROP CONSTRAINT IF EXISTS federation_poll_retention_shape;
ALTER TABLE fornix.workspace_federation_poll_attempts
  ADD CONSTRAINT federation_poll_retention_shape CHECK (
    retention_class IN ('authoritative','operational') AND
    retention_state IN ('active','expired') AND
    (retention_state = 'active' OR (expired_at IS NOT NULL AND tombstone_hash ~ '^[0-9a-f]{64}$')) AND
    (tombstone_hash = '' OR tombstone_hash ~ '^[0-9a-f]{64}$')
  );

CREATE INDEX IF NOT EXISTS workspace_federation_poll_retention_idx
  ON fornix.workspace_federation_poll_attempts(workspace_id, retention_class, retention_state, retention_deadline, id);

ALTER TABLE fornix.federation_legacy_quarantine
  ADD COLUMN IF NOT EXISTS retention_class TEXT NOT NULL DEFAULT 'operational',
  ADD COLUMN IF NOT EXISTS retention_state TEXT NOT NULL DEFAULT 'active',
  ADD COLUMN IF NOT EXISTS retention_deadline TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS tombstone_hash TEXT NOT NULL DEFAULT '';

UPDATE fornix.federation_legacy_quarantine
SET retention_deadline = created_at + INTERVAL '30 days'
WHERE retention_deadline IS NULL;

ALTER TABLE fornix.federation_legacy_quarantine
  DROP CONSTRAINT IF EXISTS federation_legacy_quarantine_retention_shape;
ALTER TABLE fornix.federation_legacy_quarantine
  ADD CONSTRAINT federation_legacy_quarantine_retention_shape CHECK (
    retention_class IN ('authoritative','operational') AND
    retention_state IN ('active','expired') AND
    (retention_state = 'active' OR (expired_at IS NOT NULL AND tombstone_hash ~ '^[0-9a-f]{64}$')) AND
    (tombstone_hash = '' OR tombstone_hash ~ '^[0-9a-f]{64}$')
  );

CREATE INDEX IF NOT EXISTS federation_legacy_quarantine_retention_idx
  ON fornix.federation_legacy_quarantine(audit_workspace_id, retention_class, retention_state, retention_deadline, id);

CREATE TABLE IF NOT EXISTS fornix.federation_retention_tombstones (
  id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  source_table TEXT NOT NULL,
  source_id TEXT NOT NULL,
  source_hash TEXT NOT NULL,
  reason TEXT NOT NULL,
  expired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  CONSTRAINT federation_retention_tombstone_identity CHECK (
    length(id) BETWEEN 1 AND 128 AND length(workspace_id) BETWEEN 1 AND 256 AND
    source_table IN ('workspace_federation_poll_attempts','federation_legacy_quarantine') AND
    length(source_id) BETWEEN 1 AND 256 AND source_hash ~ '^[0-9a-f]{64}$' AND
    length(reason) BETWEEN 1 AND 512 AND octet_length(actor::text) <= 16384
  ),
  PRIMARY KEY (id, expired_at)
) PARTITION BY RANGE (expired_at);

CREATE TABLE IF NOT EXISTS fornix.federation_retention_tombstones_default
  PARTITION OF fornix.federation_retention_tombstones DEFAULT;

CREATE INDEX IF NOT EXISTS federation_retention_tombstones_page_idx
  ON fornix.federation_retention_tombstones(workspace_id, expired_at, id);

ALTER TABLE fornix.federation_retention_tombstones ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.federation_retention_tombstones;
CREATE POLICY workspace_scope_isolation ON fornix.federation_retention_tombstones
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.federation_retention_tombstones_default ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.federation_retention_tombstones_default;
CREATE POLICY workspace_scope_isolation ON fornix.federation_retention_tombstones_default
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
