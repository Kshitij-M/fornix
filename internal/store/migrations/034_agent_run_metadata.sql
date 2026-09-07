-- 034: persist bounded, non-secret agent metadata used to reproduce provider
-- routing details such as the local reference-workflow mount. Existing rows
-- receive an empty object and retain their prior replay/state semantics.
ALTER TABLE fornix.agent_runs
  ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;
