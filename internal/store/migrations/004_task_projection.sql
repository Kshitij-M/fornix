-- Rebuildable derived task view. Event history remains authoritative.
CREATE TABLE IF NOT EXISTS fabric.task_state_projections (
  workspace_id TEXT NOT NULL,
  task_id TEXT NOT NULL,
  status TEXT NOT NULL,
  result TEXT NOT NULL DEFAULT '',
  assigned_session TEXT NOT NULL DEFAULT '',
  last_event_id TEXT NOT NULL,
  last_event_sequence BIGINT NOT NULL,
  applied_event_count BIGINT NOT NULL DEFAULT 1,
  state_hash TEXT NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (workspace_id, task_id),
  CONSTRAINT task_projection_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT task_projection_task_nonempty CHECK (length(task_id) > 0),
  CONSTRAINT task_projection_sequence_positive CHECK (last_event_sequence > 0),
  CONSTRAINT task_projection_count_positive CHECK (applied_event_count > 0)
);

CREATE INDEX IF NOT EXISTS task_state_projections_workspace_sequence_idx
  ON fabric.task_state_projections (workspace_id, last_event_sequence);
CREATE INDEX IF NOT EXISTS task_state_projections_status_idx
  ON fabric.task_state_projections (workspace_id, status, updated_at DESC);
