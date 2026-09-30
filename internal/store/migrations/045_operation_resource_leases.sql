-- 045: durable workspace-scoped resource serialization for generic workers.
-- Resource leases are coordination state. Their append-only history records
-- ownership changes without becoming a second operation authority.

CREATE TABLE IF NOT EXISTS fornix.operation_resource_leases (
  workspace_id TEXT NOT NULL,
  resource_key TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  operation_fence BIGINT NOT NULL,
  fence BIGINT NOT NULL,
  lease_until TIMESTAMPTZ NOT NULL,
  acquired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  renewed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  released_at TIMESTAMPTZ,
  PRIMARY KEY (workspace_id, resource_key),
  CONSTRAINT operation_resource_leases_operation_fk
    FOREIGN KEY (workspace_id, operation_id) REFERENCES fornix.operations(workspace_id, id) ON DELETE CASCADE,
  CONSTRAINT operation_resource_leases_identity_check
    CHECK (length(workspace_id) > 0 AND length(resource_key) BETWEEN 1 AND 512 AND length(owner_id) > 0),
  CONSTRAINT operation_resource_leases_fence_check CHECK (operation_fence > 0 AND fence > 0)
);

CREATE INDEX IF NOT EXISTS operation_resource_leases_operation_idx
  ON fornix.operation_resource_leases(workspace_id, operation_id, released_at, lease_until);

CREATE TABLE IF NOT EXISTS fornix.operation_resource_lease_history (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  resource_key TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  operation_fence BIGINT NOT NULL,
  fence BIGINT NOT NULL,
  action TEXT NOT NULL,
  lease_until TIMESTAMPTZ,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT operation_resource_lease_history_identity_check
    CHECK (length(workspace_id) > 0 AND length(resource_key) BETWEEN 1 AND 512 AND length(owner_id) > 0),
  CONSTRAINT operation_resource_lease_history_action_check
    CHECK (action IN ('acquired', 'renewed', 'released', 'takeover')),
  CONSTRAINT operation_resource_lease_history_fence_check CHECK (operation_fence > 0 AND fence > 0)
);

CREATE INDEX IF NOT EXISTS operation_resource_lease_history_lookup_idx
  ON fornix.operation_resource_lease_history(workspace_id, resource_key, occurred_at, id);

CREATE OR REPLACE FUNCTION fornix.reject_operation_resource_lease_history_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix operation resource lease history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS operation_resource_lease_history_append_only ON fornix.operation_resource_lease_history;
CREATE TRIGGER operation_resource_lease_history_append_only
  BEFORE UPDATE OR DELETE ON fornix.operation_resource_lease_history
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_operation_resource_lease_history_mutation();

ALTER TABLE fornix.operation_resource_leases ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.operation_resource_leases;
CREATE POLICY workspace_scope_isolation ON fornix.operation_resource_leases
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.operation_resource_lease_history ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.operation_resource_lease_history;
CREATE POLICY workspace_scope_isolation ON fornix.operation_resource_lease_history
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
