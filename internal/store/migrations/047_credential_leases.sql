-- 047: durable, workspace-scoped credential lease authority.
--
-- Lease rows contain references and fencing metadata only. Secret material is
-- resolved by an injected SecretResolver and never stored in PostgreSQL.
-- The lease table is coordination authority: it bounds and revokes use, but
-- it cannot make an external provider call exactly once.

CREATE UNIQUE INDEX IF NOT EXISTS credential_references_workspace_id_uq
  ON fornix.credential_references(workspace_id, id);

CREATE TABLE IF NOT EXISTS fornix.credential_leases (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  credential_ref_id TEXT NOT NULL,
  provider TEXT NOT NULL,
  purpose TEXT NOT NULL,
  fence BIGINT NOT NULL,
  revocation_epoch BIGINT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  expires_at TIMESTAMPTZ NOT NULL,
  acquired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  renewed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  released_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ,
  CONSTRAINT credential_leases_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT credential_leases_id_nonempty CHECK (length(id) > 0),
  CONSTRAINT credential_leases_ref_nonempty CHECK (length(credential_ref_id) > 0),
  CONSTRAINT credential_leases_provider_nonempty CHECK (length(provider) > 0),
  CONSTRAINT credential_leases_purpose_nonempty CHECK (length(purpose) BETWEEN 1 AND 128),
  CONSTRAINT credential_leases_fence_positive CHECK (fence > 0 AND revocation_epoch > 0),
  CONSTRAINT credential_leases_status_valid CHECK (status IN ('active','released','revoked','expired')),
  CONSTRAINT credential_leases_ref_fk
    FOREIGN KEY (workspace_id, credential_ref_id)
    REFERENCES fornix.credential_references(workspace_id, id)
);

CREATE INDEX IF NOT EXISTS credential_leases_active_lookup_idx
  ON fornix.credential_leases(workspace_id, credential_ref_id, status, fence DESC);

CREATE INDEX IF NOT EXISTS credential_leases_expiry_idx
  ON fornix.credential_leases(status, expires_at, workspace_id);

CREATE TABLE IF NOT EXISTS fornix.credential_lease_events (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  lease_id TEXT NOT NULL,
  credential_ref_id TEXT NOT NULL,
  event TEXT NOT NULL,
  fence BIGINT NOT NULL,
  revocation_epoch BIGINT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT credential_lease_events_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT credential_lease_events_event_valid CHECK (event IN ('acquired','renewed','released','revoked','expired')),
  CONSTRAINT credential_lease_events_fence_positive CHECK (fence > 0 AND revocation_epoch > 0),
  CONSTRAINT credential_lease_events_lease_fk
    FOREIGN KEY (lease_id) REFERENCES fornix.credential_leases(id) ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS credential_lease_events_lookup_idx
  ON fornix.credential_lease_events(workspace_id, lease_id, occurred_at, id);

CREATE OR REPLACE FUNCTION fornix.reject_credential_lease_event_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix credential lease events are append-only';
END;
$$;

DROP TRIGGER IF EXISTS credential_lease_events_append_only ON fornix.credential_lease_events;
CREATE TRIGGER credential_lease_events_append_only
  BEFORE UPDATE OR DELETE ON fornix.credential_lease_events
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_credential_lease_event_mutation();

ALTER TABLE fornix.credential_leases ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.credential_leases;
CREATE POLICY workspace_scope_isolation ON fornix.credential_leases
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.credential_lease_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.credential_lease_events;
CREATE POLICY workspace_scope_isolation ON fornix.credential_lease_events
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
