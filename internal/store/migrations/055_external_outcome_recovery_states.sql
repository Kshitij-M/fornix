-- Preserve uncertainty at the owning model/tool ledger when an external
-- boundary may have accepted a request but the process cannot prove the
-- outcome. Recovery is deliberately non-terminal: a duplicate request must
-- fail closed until a domain-specific reconciler resolves it.

ALTER TABLE fornix.model_calls
  DROP CONSTRAINT IF EXISTS model_calls_status_valid;

ALTER TABLE fornix.model_calls
  ADD CONSTRAINT model_calls_status_valid
  CHECK (status IN ('pending', 'running', 'succeeded', 'failed', 'recovery_required'));

ALTER TABLE fornix.tool_runs
  DROP CONSTRAINT IF EXISTS tool_runs_status_valid;

ALTER TABLE fornix.tool_runs
  ADD CONSTRAINT tool_runs_status_valid
  CHECK (status IN ('pending','awaiting_approval','running','succeeded','failed','recovery_required','denied','cancelled'));
