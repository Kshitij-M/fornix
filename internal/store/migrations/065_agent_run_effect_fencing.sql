-- Migration 065: bind model and tool effects to the owning agent-run lease.
--
-- These are nullable in the logical sense (empty values preserve legacy
-- standalone calls), but the columns remain non-null so old rows are easy to
-- query and cannot contain a partial ownership tuple. Postgres remains the
-- authority: the stores validate owner/fence against the live lease row in the
-- same transaction that mutates the effect.
ALTER TABLE fornix.model_calls
  ADD COLUMN IF NOT EXISTS agent_run_id TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS agent_run_owner_id TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS agent_run_fence BIGINT NOT NULL DEFAULT 0;

ALTER TABLE fornix.tool_runs
  ADD COLUMN IF NOT EXISTS agent_run_id TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS agent_run_owner_id TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS agent_run_fence BIGINT NOT NULL DEFAULT 0;

ALTER TABLE fornix.model_calls
  ADD CONSTRAINT model_calls_agent_run_fence_nonnegative
    CHECK (agent_run_fence >= 0),
  ADD CONSTRAINT model_calls_agent_run_binding_complete
    CHECK (
      (agent_run_id = '' AND agent_run_owner_id = '' AND agent_run_fence = 0)
      OR (agent_run_id <> '' AND agent_run_owner_id <> '' AND agent_run_fence > 0)
    );

ALTER TABLE fornix.tool_runs
  ADD CONSTRAINT tool_runs_agent_run_fence_nonnegative
    CHECK (agent_run_fence >= 0),
  ADD CONSTRAINT tool_runs_agent_run_binding_complete
    CHECK (
      (agent_run_id = '' AND agent_run_owner_id = '' AND agent_run_fence = 0)
      OR (agent_run_id <> '' AND agent_run_owner_id <> '' AND agent_run_fence > 0)
    );

CREATE INDEX IF NOT EXISTS model_calls_workspace_agent_run_idx
  ON fornix.model_calls(workspace_id, agent_run_id, agent_run_fence)
  WHERE agent_run_id <> '';

CREATE INDEX IF NOT EXISTS tool_runs_workspace_agent_run_idx
  ON fornix.tool_runs(workspace_id, agent_run_id, agent_run_fence)
  WHERE agent_run_id <> '';
