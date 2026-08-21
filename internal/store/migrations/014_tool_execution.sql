CREATE TABLE IF NOT EXISTS fornix.tool_runs (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  causation_id TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL DEFAULT '',
  tool_id TEXT NOT NULL,
  capability TEXT NOT NULL DEFAULT '',
  mode TEXT NOT NULL,
  status TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  session_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_owner_id TEXT NOT NULL DEFAULT '',
  task_fence BIGINT NOT NULL DEFAULT 0,
  approval_id TEXT NOT NULL DEFAULT '',
  attempt INTEGER NOT NULL DEFAULT 0,
  request_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  response_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  result JSONB,
  failure JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  duration_ms BIGINT NOT NULL DEFAULT 0,
  CONSTRAINT tool_runs_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT tool_runs_idempotency_nonempty CHECK (length(idempotency_key) > 0),
  CONSTRAINT tool_runs_hash_nonempty CHECK (length(request_hash) > 0),
  CONSTRAINT tool_runs_fence_nonnegative CHECK (task_fence >= 0),
  CONSTRAINT tool_runs_status_valid CHECK (status IN ('pending','awaiting_approval','running','succeeded','failed','denied','cancelled')),
  CONSTRAINT tool_runs_mode_valid CHECK (mode IN ('automatic','pre_approved','interactive','denied')),
  UNIQUE (workspace_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS tool_runs_workspace_status_idx
  ON fornix.tool_runs(workspace_id, status, created_at, id);
CREATE INDEX IF NOT EXISTS tool_runs_workspace_request_idx
  ON fornix.tool_runs(workspace_id, request_id);

CREATE TABLE IF NOT EXISTS fornix.tool_approvals (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  tool_id TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  session_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'pending',
  reason TEXT NOT NULL DEFAULT '',
  decided_by JSONB NOT NULL DEFAULT '{}'::jsonb,
  decision_reason TEXT NOT NULL DEFAULT '',
  expires_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  decided_at TIMESTAMPTZ,
  CONSTRAINT tool_approvals_status_valid CHECK (status IN ('pending','approved','denied','expired')),
  CONSTRAINT tool_approvals_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT tool_approvals_run_fk FOREIGN KEY (run_id) REFERENCES fornix.tool_runs(id) ON DELETE CASCADE,
  UNIQUE (run_id)
);

CREATE INDEX IF NOT EXISTS tool_approvals_workspace_status_idx
  ON fornix.tool_approvals(workspace_id, status, expires_at, created_at);
