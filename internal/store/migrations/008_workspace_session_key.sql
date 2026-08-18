-- Session IDs are workspace-local. Replace the legacy global session key and
-- task foreign key so PostgreSQL enforces the same boundary as the handlers.
ALTER TABLE fornix.tasks DROP CONSTRAINT IF EXISTS tasks_assigned_session_fkey;
ALTER TABLE fornix.sessions DROP CONSTRAINT IF EXISTS sessions_pkey;

ALTER TABLE fornix.sessions
  ADD CONSTRAINT sessions_pkey PRIMARY KEY (workspace_id, id);

ALTER TABLE fornix.tasks
  ADD CONSTRAINT tasks_assigned_session_fkey
  FOREIGN KEY (workspace_id, assigned_session)
  REFERENCES fornix.sessions(workspace_id, id)
  ON DELETE SET NULL (assigned_session);
