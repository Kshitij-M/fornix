ALTER TABLE fornix.sessions ADD COLUMN IF NOT EXISTS workspace_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE fornix.tasks ADD COLUMN IF NOT EXISTS workspace_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE fornix.tasks ADD COLUMN IF NOT EXISTS attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE fornix.tasks ADD COLUMN IF NOT EXISTS max_attempts INTEGER NOT NULL DEFAULT 3;
ALTER TABLE fornix.tasks ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE fornix.tasks ADD COLUMN IF NOT EXISTS last_error TEXT NOT NULL DEFAULT '';
ALTER TABLE fornix.tasks ADD COLUMN IF NOT EXISTS failure_class TEXT NOT NULL DEFAULT '';
ALTER TABLE fornix.tasks ADD COLUMN IF NOT EXISTS retryable BOOLEAN;
ALTER TABLE fornix.tasks ADD COLUMN IF NOT EXISTS execution_fence BIGINT NOT NULL DEFAULT 0;
ALTER TABLE fornix.tasks ADD COLUMN IF NOT EXISTS cancelled_at TIMESTAMPTZ;

ALTER TABLE fornix.tasks
  ADD CONSTRAINT tasks_workspace_id_unique UNIQUE (workspace_id, id);

CREATE INDEX IF NOT EXISTS tasks_workspace_ready_idx
  ON fornix.tasks (workspace_id, status, next_attempt_at, created_at, id);
CREATE INDEX IF NOT EXISTS tasks_workspace_assigned_idx
  ON fornix.tasks (workspace_id, assigned_session)
  WHERE status IN ('claimed', 'in_progress');

CREATE TABLE IF NOT EXISTS fornix.task_dependencies (
  workspace_id TEXT NOT NULL,
  task_id BIGINT NOT NULL,
  depends_on_task_id BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (workspace_id, task_id, depends_on_task_id),
  CONSTRAINT task_dependencies_no_self CHECK (task_id <> depends_on_task_id),
  CONSTRAINT task_dependencies_task_fk
    FOREIGN KEY (workspace_id, task_id)
    REFERENCES fornix.tasks(workspace_id, id) ON DELETE CASCADE,
  CONSTRAINT task_dependencies_dependency_fk
    FOREIGN KEY (workspace_id, depends_on_task_id)
    REFERENCES fornix.tasks(workspace_id, id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS task_dependencies_dependency_idx
  ON fornix.task_dependencies (workspace_id, depends_on_task_id, task_id);

CREATE TABLE IF NOT EXISTS fornix.task_execution_leases (
  workspace_id TEXT NOT NULL,
  task_id BIGINT NOT NULL,
  owner_id TEXT NOT NULL,
  fence BIGINT NOT NULL DEFAULT 1,
  lease_until TIMESTAMPTZ NOT NULL,
  acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  renewed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  released_at TIMESTAMPTZ,
  PRIMARY KEY (workspace_id, task_id),
  CONSTRAINT task_execution_lease_task_fk
    FOREIGN KEY (workspace_id, task_id)
    REFERENCES fornix.tasks(workspace_id, id) ON DELETE CASCADE,
  CONSTRAINT task_execution_lease_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT task_execution_lease_owner_nonempty CHECK (length(owner_id) > 0),
  CONSTRAINT task_execution_lease_fence_positive CHECK (fence > 0)
);

CREATE INDEX IF NOT EXISTS task_execution_leases_expiry_idx
  ON fornix.task_execution_leases (workspace_id, lease_until)
  WHERE released_at IS NULL;
