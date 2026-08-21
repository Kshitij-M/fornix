CREATE TABLE IF NOT EXISTS fornix.agent_runs (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  causation_id TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL DEFAULT '',
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  session_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_owner_id TEXT NOT NULL DEFAULT '',
  task_fence BIGINT NOT NULL DEFAULT 0,
  goal TEXT NOT NULL,
  provider JSONB NOT NULL DEFAULT '{}'::jsonb,
  tools JSONB NOT NULL DEFAULT '[]'::jsonb,
  budget JSONB NOT NULL DEFAULT '{}'::jsonb,
  retrieval_request JSONB NOT NULL DEFAULT '{}'::jsonb,
  context_hash TEXT NOT NULL DEFAULT '',
  state TEXT NOT NULL,
  phase TEXT NOT NULL,
  turn INTEGER NOT NULL DEFAULT 0,
  step INTEGER NOT NULL DEFAULT 0,
  model_attempts INTEGER NOT NULL DEFAULT 0,
  model_calls INTEGER NOT NULL DEFAULT 0,
  tool_calls INTEGER NOT NULL DEFAULT 0,
  input_tokens INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0,
  total_tokens INTEGER NOT NULL DEFAULT 0,
  context_bytes INTEGER NOT NULL DEFAULT 0,
  cost JSONB NOT NULL DEFAULT '{}'::jsonb,
  history JSONB NOT NULL DEFAULT '[]'::jsonb,
  pending_tools JSONB NOT NULL DEFAULT '[]'::jsonb,
  last_output TEXT NOT NULL DEFAULT '',
  last_failure JSONB,
  termination TEXT NOT NULL DEFAULT '',
  next_retry_at TIMESTAMPTZ,
  state_version BIGINT NOT NULL DEFAULT 1,
  event_sequence BIGINT NOT NULL DEFAULT 0,
  state_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  CONSTRAINT agent_runs_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT agent_runs_idempotency_nonempty CHECK (length(idempotency_key) > 0),
  CONSTRAINT agent_runs_request_hash_nonempty CHECK (length(request_hash) > 0),
  CONSTRAINT agent_runs_goal_nonempty CHECK (length(goal) > 0),
  CONSTRAINT agent_runs_context_hash_bounded CHECK (length(context_hash) <= 128),
  CONSTRAINT agent_runs_fence_nonnegative CHECK (task_fence >= 0),
  CONSTRAINT agent_runs_state_valid CHECK (state IN ('pending','running','awaiting_approval','awaiting_retry','awaiting_external','succeeded','failed','cancelled','deadletter')),
  CONSTRAINT agent_runs_phase_valid CHECK (phase IN ('model','tool')),
  CONSTRAINT agent_runs_version_positive CHECK (state_version > 0),
  UNIQUE (workspace_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS agent_runs_workspace_state_idx
  ON fornix.agent_runs(workspace_id, state, updated_at, id);
CREATE INDEX IF NOT EXISTS agent_runs_workspace_task_idx
  ON fornix.agent_runs(workspace_id, (task_ref->>'id'), state);
CREATE INDEX IF NOT EXISTS agent_runs_retry_idx
  ON fornix.agent_runs(workspace_id, next_retry_at)
  WHERE state = 'awaiting_retry';

ALTER TABLE fornix.agent_runs ADD COLUMN IF NOT EXISTS retrieval_request JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE fornix.agent_runs ADD COLUMN IF NOT EXISTS context_hash TEXT NOT NULL DEFAULT '';
