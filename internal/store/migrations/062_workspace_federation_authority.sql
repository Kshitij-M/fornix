-- Migration 062: workspace-scoped federation peer and poll authority.
-- Historical fabric federation rows and raw bearer tokens remain quarantined;
-- this migration never assigns ownership by inference.

CREATE TABLE IF NOT EXISTS fornix.workspace_federation_peers (
  workspace_id TEXT NOT NULL,
  peer_id TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  remote_workspace_id TEXT NOT NULL,
  endpoint_url TEXT NOT NULL,
  credential_ref TEXT NOT NULL,
  allow_private_networks BOOLEAN NOT NULL DEFAULT FALSE,
  max_messages INTEGER NOT NULL DEFAULT 500,
  max_response_bytes BIGINT NOT NULL DEFAULT 1048576,
  timeout_ms BIGINT NOT NULL DEFAULT 10000,
  status TEXT NOT NULL DEFAULT 'active',
  revision BIGINT NOT NULL DEFAULT 1,
  config_hash TEXT NOT NULL,
  created_by JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, peer_id),
  CONSTRAINT workspace_federation_peer_identity CHECK (
    length(workspace_id) BETWEEN 1 AND 256 AND length(peer_id) BETWEEN 1 AND 128 AND
    length(remote_workspace_id) BETWEEN 1 AND 256 AND length(endpoint_url) BETWEEN 1 AND 2048 AND
    length(credential_ref) BETWEEN 3 AND 128 AND config_hash ~ '^[0-9a-f]{64}$' AND
    status IN ('active','disabled') AND revision >= 1 AND max_messages BETWEEN 1 AND 1000 AND
    max_response_bytes BETWEEN 1 AND 16777216 AND timeout_ms BETWEEN 1 AND 120000 AND
    octet_length(created_by::text) <= 16384
  )
);

CREATE TABLE IF NOT EXISTS fornix.workspace_federation_peer_commands (
  sequence BIGSERIAL PRIMARY KEY,
  id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  peer_id TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  revision BIGINT NOT NULL,
  peer JSONB NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  causation_id TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (workspace_id, id),
  UNIQUE (workspace_id, idempotency_key),
  CONSTRAINT workspace_federation_peer_command_identity CHECK (
    length(id) BETWEEN 1 AND 128 AND length(workspace_id) BETWEEN 1 AND 256 AND
    length(peer_id) BETWEEN 1 AND 128 AND length(request_id) BETWEEN 1 AND 256 AND
    length(idempotency_key) BETWEEN 1 AND 256 AND request_hash ~ '^[0-9a-f]{64}$' AND
    revision >= 1 AND octet_length(peer::text) <= 16384 AND octet_length(actor::text) <= 16384
  )
);

CREATE INDEX IF NOT EXISTS workspace_federation_peer_commands_read_idx
  ON fornix.workspace_federation_peer_commands (workspace_id, peer_id, sequence DESC);

CREATE TABLE IF NOT EXISTS fornix.workspace_federation_peer_leases (
  workspace_id TEXT NOT NULL,
  peer_id TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  fence BIGINT NOT NULL DEFAULT 1,
  expires_at TIMESTAMPTZ NOT NULL,
  acquired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  renewed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, peer_id),
  CONSTRAINT workspace_federation_peer_lease_identity CHECK (
    length(workspace_id) BETWEEN 1 AND 256 AND length(peer_id) BETWEEN 1 AND 128 AND
    length(owner_id) BETWEEN 1 AND 128 AND fence > 0
  )
);

CREATE TABLE IF NOT EXISTS fornix.workspace_federation_poll_attempts (
  id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  peer_id TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  from_sequence BIGINT NOT NULL DEFAULT 0,
  state TEXT NOT NULL DEFAULT 'reserved',
  owner_id TEXT NOT NULL,
  fence BIGINT NOT NULL,
  credential_lease_id TEXT NOT NULL DEFAULT '',
  credential_lease_fence BIGINT NOT NULL DEFAULT 0,
  credential_revocation_epoch BIGINT NOT NULL DEFAULT 0,
  credential_source_version TEXT NOT NULL DEFAULT '',
  provider_request_id TEXT NOT NULL DEFAULT '',
  response_hash TEXT NOT NULL DEFAULT '',
  imported_count INTEGER NOT NULL DEFAULT 0,
  failure_code TEXT NOT NULL DEFAULT '',
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  causation_id TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL DEFAULT '',
  started_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  completed_at TIMESTAMPTZ,
  PRIMARY KEY (workspace_id, id),
  UNIQUE (workspace_id, idempotency_key),
  CONSTRAINT workspace_federation_poll_identity CHECK (
    length(id) BETWEEN 1 AND 128 AND length(workspace_id) BETWEEN 1 AND 256 AND
    length(peer_id) BETWEEN 1 AND 128 AND length(request_id) BETWEEN 1 AND 256 AND
    length(idempotency_key) BETWEEN 1 AND 256 AND request_hash ~ '^[0-9a-f]{64}$' AND
    from_sequence >= 0 AND state IN ('reserved','dispatching','succeeded','failed','recovery_required') AND
    length(owner_id) BETWEEN 1 AND 128 AND fence > 0 AND credential_lease_fence >= 0 AND
    credential_revocation_epoch >= 0 AND
    (response_hash = '' OR response_hash ~ '^[0-9a-f]{64}$') AND imported_count BETWEEN 0 AND 1000 AND
    octet_length(actor::text) <= 16384
  )
);

CREATE INDEX IF NOT EXISTS workspace_federation_poll_recovery_idx
  ON fornix.workspace_federation_poll_attempts (workspace_id, state, updated_at);
CREATE INDEX IF NOT EXISTS workspace_federation_poll_peer_idx
  ON fornix.workspace_federation_poll_attempts (workspace_id, peer_id, started_at DESC);

ALTER TABLE fornix.workspace_federation_peers ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.workspace_federation_peers;
CREATE POLICY workspace_scope_isolation ON fornix.workspace_federation_peers
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.workspace_federation_peer_commands ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.workspace_federation_peer_commands;
CREATE POLICY workspace_scope_isolation ON fornix.workspace_federation_peer_commands
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.workspace_federation_peer_leases ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.workspace_federation_peer_leases;
CREATE POLICY workspace_scope_isolation ON fornix.workspace_federation_peer_leases
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.workspace_federation_poll_attempts ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.workspace_federation_poll_attempts;
CREATE POLICY workspace_scope_isolation ON fornix.workspace_federation_poll_attempts
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
