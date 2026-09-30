-- Migration 063: explicit, redacted disposition for historical global
-- federation rows. No legacy bearer token, URL, or body is copied.

CREATE TABLE IF NOT EXISTS fornix.federation_legacy_quarantine (
  id TEXT PRIMARY KEY,
  audit_workspace_id TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  legacy_table TEXT NOT NULL,
  legacy_peer_id TEXT NOT NULL,
  source_url_hash TEXT NOT NULL,
  row_hash TEXT NOT NULL,
  disposition TEXT NOT NULL DEFAULT 'quarantined',
  reason TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (audit_workspace_id, legacy_table, legacy_peer_id),
  CONSTRAINT federation_legacy_quarantine_identity CHECK (
    length(id) BETWEEN 1 AND 128 AND length(audit_workspace_id) BETWEEN 1 AND 256 AND
    length(legacy_table) BETWEEN 1 AND 128 AND length(legacy_peer_id) BETWEEN 1 AND 256 AND
    source_url_hash ~ '^[0-9a-f]{64}$' AND row_hash ~ '^[0-9a-f]{64}$' AND
    disposition = 'quarantined' AND length(reason) BETWEEN 1 AND 512 AND
    length(request_id) BETWEEN 1 AND 256 AND length(idempotency_key) BETWEEN 1 AND 256 AND
    octet_length(actor::text) <= 16384
  )
);

CREATE INDEX IF NOT EXISTS federation_legacy_quarantine_page_idx
  ON fornix.federation_legacy_quarantine (audit_workspace_id, id);
CREATE INDEX IF NOT EXISTS federation_legacy_quarantine_request_idx
  ON fornix.federation_legacy_quarantine (audit_workspace_id, idempotency_key, id);

ALTER TABLE fornix.federation_legacy_quarantine ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.federation_legacy_quarantine;
CREATE POLICY workspace_scope_isolation ON fornix.federation_legacy_quarantine
  USING (audit_workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (audit_workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
