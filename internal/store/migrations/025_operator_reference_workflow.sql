-- Workspace/operator metadata and bounded repository-ingest bookkeeping.
-- Existing control tables intentionally do not gain foreign keys here: older
-- installations contain logical workspace IDs in many historical tables.
CREATE TABLE IF NOT EXISTS fornix.workspaces (
  id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  default_provider TEXT NOT NULL DEFAULT 'fake',
  tool_root TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT workspaces_id_nonempty CHECK (length(id) > 0),
  CONSTRAINT workspaces_status_valid CHECK (status IN ('active','disabled')),
  CONSTRAINT workspaces_provider_nonempty CHECK (length(default_provider) > 0)
);

CREATE INDEX IF NOT EXISTS workspaces_status_id_idx
  ON fornix.workspaces(status, id);

CREATE TABLE IF NOT EXISTS fornix.repository_ingests (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  repository TEXT NOT NULL,
  source_root TEXT NOT NULL DEFAULT '',
  manifest_hash TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'pending',
  file_count INTEGER NOT NULL DEFAULT 0,
  chunk_count INTEGER NOT NULL DEFAULT 0,
  symbol_count INTEGER NOT NULL DEFAULT 0,
  byte_count BIGINT NOT NULL DEFAULT 0,
  last_error TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT repository_ingests_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT repository_ingests_repository_nonempty CHECK (length(repository) > 0),
  CONSTRAINT repository_ingests_manifest_hash_shape CHECK (manifest_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT repository_ingests_request_hash_shape CHECK (request_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT repository_ingests_status_valid CHECK (status IN ('pending','running','succeeded','failed')),
  CONSTRAINT repository_ingests_counts_nonnegative CHECK (file_count >= 0 AND chunk_count >= 0 AND symbol_count >= 0 AND byte_count >= 0),
  UNIQUE (workspace_id, idempotency_key),
  UNIQUE (workspace_id, repository, manifest_hash)
);

CREATE INDEX IF NOT EXISTS repository_ingests_workspace_time_idx
  ON fornix.repository_ingests(workspace_id, updated_at DESC, id);

CREATE TABLE IF NOT EXISTS fornix.operator_audit (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL DEFAULT '',
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  operation TEXT NOT NULL,
  resource TEXT NOT NULL DEFAULT '',
  outcome TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT operator_audit_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT operator_audit_request_nonempty CHECK (length(request_id) > 0),
  CONSTRAINT operator_audit_operation_nonempty CHECK (length(operation) > 0),
  CONSTRAINT operator_audit_outcome_valid CHECK (outcome IN ('accepted','deduped','rejected','failed')),
  UNIQUE (workspace_id, request_id, operation, resource)
);

CREATE INDEX IF NOT EXISTS operator_audit_workspace_time_idx
  ON fornix.operator_audit(workspace_id, created_at DESC, id);

INSERT INTO fornix.workspaces(id, display_name, status, default_provider)
VALUES ('default', 'Default workspace', 'active', 'fake')
ON CONFLICT (id) DO NOTHING;
