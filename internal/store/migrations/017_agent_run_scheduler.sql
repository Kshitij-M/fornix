ALTER TABLE fornix.agent_runs
  ADD COLUMN IF NOT EXISTS scheduler_priority INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS next_scheduled_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  ADD COLUMN IF NOT EXISTS schedule_attempts INTEGER NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS last_worker_id TEXT NOT NULL DEFAULT '';

UPDATE fornix.agent_runs
SET next_scheduled_at = COALESCE(next_retry_at, created_at)
WHERE next_scheduled_at IS NULL;

ALTER TABLE fornix.agent_runs
  ADD CONSTRAINT agent_runs_scheduler_priority_bounded
    CHECK (scheduler_priority BETWEEN -1000000 AND 1000000),
  ADD CONSTRAINT agent_runs_schedule_attempts_nonnegative
    CHECK (schedule_attempts >= 0);

CREATE INDEX IF NOT EXISTS agent_runs_scheduler_queue_idx
  ON fornix.agent_runs(
    workspace_id,
    scheduler_priority DESC,
    next_scheduled_at,
    created_at,
    id
  )
  WHERE state IN ('pending','running','awaiting_retry','awaiting_approval');

CREATE TABLE IF NOT EXISTS fornix.agent_run_worker_leases (
  workspace_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  fence BIGINT NOT NULL DEFAULT 1,
  lease_until TIMESTAMPTZ NOT NULL,
  acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  renewed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  released_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (workspace_id, run_id),
  CONSTRAINT agent_run_worker_leases_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT agent_run_worker_leases_run_nonempty CHECK (length(run_id) > 0),
  CONSTRAINT agent_run_worker_leases_owner_nonempty CHECK (length(owner_id) > 0),
  CONSTRAINT agent_run_worker_leases_fence_positive CHECK (fence > 0)
);

CREATE INDEX IF NOT EXISTS agent_run_worker_leases_expiry_idx
  ON fornix.agent_run_worker_leases(workspace_id, lease_until, run_id)
  WHERE released_at IS NULL;

CREATE INDEX IF NOT EXISTS agent_run_worker_leases_owner_idx
  ON fornix.agent_run_worker_leases(workspace_id, owner_id, updated_at DESC);
