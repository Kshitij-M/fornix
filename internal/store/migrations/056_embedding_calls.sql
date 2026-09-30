-- 056: workspace-scoped embedding-call authority.
--
-- Embedding vectors remain in Postgres so a successful call can be replayed
-- without contacting a provider. The row is a specialized execution ledger;
-- generic operation/effect authority is linked through domain_kind
-- embedding_call by the runtime adapter.

CREATE TABLE IF NOT EXISTS fornix.embedding_calls (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  causation_id TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL DEFAULT '',
  source_kind TEXT NOT NULL,
  source_id TEXT NOT NULL DEFAULT '',
  source_hash TEXT NOT NULL,
  provider TEXT NOT NULL,
  endpoint TEXT NOT NULL DEFAULT '',
  model TEXT NOT NULL,
  actor JSONB NOT NULL,
  task_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  session_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_owner_id TEXT NOT NULL DEFAULT '',
  task_fence BIGINT NOT NULL DEFAULT 0,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'pending',
  attempt_count INTEGER NOT NULL DEFAULT 0,
  provider_request_id TEXT NOT NULL DEFAULT '',
  input_bytes BIGINT NOT NULL DEFAULT 0,
  budget_max_input_bytes INTEGER NOT NULL,
  budget_dimension INTEGER NOT NULL,
  budget_timeout_ms INTEGER NOT NULL,
  max_cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  usage JSONB NOT NULL DEFAULT '{}'::jsonb,
  failure JSONB,
  request_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  response_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  vector vector(768),
  vector_hash TEXT NOT NULL DEFAULT '',
  vector_dimension INTEGER NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  duration_ms BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (workspace_id, request_id),
  UNIQUE (workspace_id, idempotency_key),
  CONSTRAINT embedding_calls_identity_nonempty CHECK (
    length(workspace_id) > 0 AND length(request_id) > 0 AND
    length(idempotency_key) > 0 AND length(source_kind) > 0 AND
    length(source_hash) = 64 AND length(provider) > 0 AND length(model) > 0
  ),
  CONSTRAINT embedding_calls_hash_shape CHECK (
    request_hash ~ '^[0-9a-f]{64}$' AND source_hash ~ '^[0-9a-f]{64}$' AND
    (vector_hash = '' OR vector_hash ~ '^[0-9a-f]{64}$')
  ),
  CONSTRAINT embedding_calls_status_valid CHECK (status IN ('pending','running','succeeded','failed','recovery_required','cancelled')),
  CONSTRAINT embedding_calls_attempt_valid CHECK (attempt_count >= 0 AND input_bytes >= 0 AND duration_ms >= 0),
  CONSTRAINT embedding_calls_budget_valid CHECK (
    budget_max_input_bytes BETWEEN 1 AND 2000 AND
    budget_dimension = 768 AND budget_timeout_ms BETWEEN 1 AND 600000 AND max_cost_usd >= 0
  ),
  CONSTRAINT embedding_calls_task_fence_pair CHECK (
    (task_owner_id = '' AND task_fence = 0) OR (task_owner_id <> '' AND task_fence > 0)
  ),
  CONSTRAINT embedding_calls_vector_shape CHECK (
    (vector IS NULL AND vector_hash = '' AND vector_dimension = 0) OR
    (vector IS NOT NULL AND vector_hash ~ '^[0-9a-f]{64}$' AND vector_dimension = 768)
  ),
  CONSTRAINT embedding_calls_success_shape CHECK (
    status <> 'succeeded' OR (vector IS NOT NULL AND vector_hash <> '' AND vector_dimension = 768)
  ),
  CONSTRAINT embedding_calls_json_bounded CHECK (
    octet_length(actor::text) <= 16384 AND octet_length(task_ref::text) <= 8192 AND
    octet_length(session_ref::text) <= 8192 AND octet_length(metadata::text) <= 16384 AND
    octet_length(request_evidence::text) <= 16384 AND octet_length(response_evidence::text) <= 16384 AND
    (failure IS NULL OR octet_length(failure::text) <= 16384)
  )
);

CREATE INDEX IF NOT EXISTS embedding_calls_status_idx
  ON fornix.embedding_calls(workspace_id, status, created_at, id);
CREATE INDEX IF NOT EXISTS embedding_calls_provider_idx
  ON fornix.embedding_calls(workspace_id, provider, model, created_at DESC, id);
CREATE INDEX IF NOT EXISTS embedding_calls_source_idx
  ON fornix.embedding_calls(workspace_id, source_hash, provider, model, vector_dimension);
CREATE INDEX IF NOT EXISTS embedding_calls_recovery_idx
  ON fornix.embedding_calls(workspace_id, updated_at, id)
  WHERE status = 'recovery_required';

ALTER TABLE fornix.embedding_calls ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.embedding_calls;
CREATE POLICY workspace_scope_isolation ON fornix.embedding_calls
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

CREATE OR REPLACE FUNCTION fornix.touch_embedding_call_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  NEW.updated_at = clock_timestamp();
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS embedding_calls_updated_at ON fornix.embedding_calls;
CREATE TRIGGER embedding_calls_updated_at
  BEFORE UPDATE ON fornix.embedding_calls
  FOR EACH ROW EXECUTE FUNCTION fornix.touch_embedding_call_updated_at();
