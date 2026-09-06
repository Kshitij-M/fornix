-- 033: make task creation idempotent at the task boundary.
-- The event idempotency record remains the authoritative transition record;
-- these columns let the task create API identify and compare the logical
-- request before allocating a second compatibility row.
ALTER TABLE fornix.tasks
  ADD COLUMN IF NOT EXISTS create_idempotency_key TEXT,
  ADD COLUMN IF NOT EXISTS create_request_hash TEXT;

CREATE UNIQUE INDEX IF NOT EXISTS tasks_workspace_create_idempotency_idx
  ON fornix.tasks (workspace_id, create_idempotency_key)
  WHERE create_idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS tasks_workspace_create_request_hash_idx
  ON fornix.tasks (workspace_id, create_request_hash)
  WHERE create_request_hash IS NOT NULL;
