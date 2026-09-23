-- 037: immutable, workspace-scoped connector binding identities.
-- Runtime adapters remain process-local; this table stores only redacted
-- configuration and references needed to reconstruct an admitted binding.

CREATE TABLE IF NOT EXISTS fornix.connector_bindings (
  workspace_id TEXT NOT NULL,
  binding_id TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  connector JSONB NOT NULL,
  binding_kind TEXT NOT NULL,
  binding_version INTEGER NOT NULL,
  config_hash TEXT NOT NULL,
  configuration JSONB NOT NULL,
  credential_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  status TEXT NOT NULL DEFAULT 'active',
  created_by JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, binding_id),
  UNIQUE (workspace_id, binding_kind, binding_version, config_hash),
  CONSTRAINT connector_binding_hash_shape CHECK (config_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT connector_binding_kind_valid CHECK (binding_kind IN ('http_api','sql_readonly')),
  CONSTRAINT connector_binding_version_valid CHECK (binding_version BETWEEN 1 AND 1024),
  CONSTRAINT connector_binding_status_valid CHECK (status IN ('active','disabled')),
  CONSTRAINT connector_binding_json_bounded CHECK (
    octet_length(connector::text) <= 16384 AND octet_length(configuration::text) <= 65536 AND
    octet_length(credential_refs::text) <= 16384 AND octet_length(created_by::text) <= 16384
  )
);
CREATE INDEX IF NOT EXISTS connector_bindings_connector_idx ON fornix.connector_bindings(workspace_id, binding_kind, created_at DESC, binding_id);

CREATE TABLE IF NOT EXISTS fornix.connector_binding_idempotency (
  workspace_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  binding_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, idempotency_key),
  CONSTRAINT connector_binding_idempotency_fk FOREIGN KEY (workspace_id, binding_id) REFERENCES fornix.connector_bindings(workspace_id, binding_id),
  CONSTRAINT connector_binding_idempotency_hash_shape CHECK (request_hash ~ '^[0-9a-f]{64}$')
);

CREATE OR REPLACE FUNCTION fornix.reject_connector_binding_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix connector bindings are immutable; register a new version';
END;
$$;

DROP TRIGGER IF EXISTS connector_bindings_append_only ON fornix.connector_bindings;
CREATE TRIGGER connector_bindings_append_only BEFORE UPDATE OR DELETE ON fornix.connector_bindings FOR EACH ROW EXECUTE FUNCTION fornix.reject_connector_binding_mutation();
