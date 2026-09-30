-- 058: provider-specific embedding reconciliation and query-vector retention.
--
-- Recovery never blindly retries generation. Query vectors may be expired
-- without removing their source/vector hashes or audit history.

ALTER TABLE fornix.embedding_calls
  ADD COLUMN IF NOT EXISTS retention_class TEXT NOT NULL DEFAULT 'authoritative',
  ADD COLUMN IF NOT EXISTS retention_deadline TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS expired_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS tombstone_hash TEXT NOT NULL DEFAULT '';

UPDATE fornix.embedding_calls
SET retention_class = 'query',
    retention_deadline = COALESCE(retention_deadline, created_at + INTERVAL '24 hours')
WHERE source_kind IN ('memo_query', 'symbol_query', 'rag_query')
  AND retention_class = 'authoritative';

ALTER TABLE fornix.embedding_calls
  DROP CONSTRAINT IF EXISTS embedding_calls_status_valid;
ALTER TABLE fornix.embedding_calls
  ADD CONSTRAINT embedding_calls_status_valid CHECK (status IN ('pending','running','succeeded','failed','recovery_required','cancelled','expired'));

ALTER TABLE fornix.embedding_calls
  DROP CONSTRAINT IF EXISTS embedding_calls_vector_shape;
ALTER TABLE fornix.embedding_calls
  ADD CONSTRAINT embedding_calls_vector_shape CHECK (
    (vector IS NULL AND vector_hash = '' AND vector_dimension = 0 AND status <> 'expired') OR
    (vector IS NULL AND vector_hash ~ '^[0-9a-f]{64}$' AND vector_dimension = 0 AND status = 'expired' AND expired_at IS NOT NULL AND tombstone_hash ~ '^[0-9a-f]{64}$') OR
    (vector IS NOT NULL AND vector_hash ~ '^[0-9a-f]{64}$' AND vector_dimension = 768 AND status <> 'expired')
  );

ALTER TABLE fornix.embedding_calls
  DROP CONSTRAINT IF EXISTS embedding_calls_success_shape;
ALTER TABLE fornix.embedding_calls
  ADD CONSTRAINT embedding_calls_success_shape CHECK (
    status <> 'succeeded' OR (vector IS NOT NULL AND vector_hash <> '' AND vector_dimension = 768)
  );

ALTER TABLE fornix.embedding_calls
  DROP CONSTRAINT IF EXISTS embedding_calls_retention_shape;
ALTER TABLE fornix.embedding_calls
  ADD CONSTRAINT embedding_calls_retention_shape CHECK (
    (retention_class = 'authoritative' AND retention_deadline IS NULL AND expired_at IS NULL AND tombstone_hash = '') OR
    (retention_class = 'query' AND retention_deadline IS NOT NULL)
  );

CREATE INDEX IF NOT EXISTS embedding_calls_query_retention_idx
  ON fornix.embedding_calls(workspace_id, retention_deadline, id)
  WHERE retention_class = 'query' AND status = 'succeeded';

CREATE TABLE IF NOT EXISTS fornix.embedding_call_reconciliations (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  attempt_key TEXT NOT NULL,
  provider TEXT NOT NULL,
  provider_request_id TEXT NOT NULL,
  source_hash TEXT NOT NULL,
  vector_hash TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  result_hash TEXT NOT NULL,
  response_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(workspace_id, attempt_key),
  UNIQUE(workspace_id, request_id, provider_request_id),
  CONSTRAINT embedding_reconciliation_identity CHECK (
    length(workspace_id) > 0 AND length(request_id) > 0 AND length(attempt_key) > 0 AND
    length(provider) > 0 AND length(provider_request_id) > 0 AND
    source_hash ~ '^[0-9a-f]{64}$' AND result_hash ~ '^[0-9a-f]{64}$'
  ),
  CONSTRAINT embedding_reconciliation_status CHECK (status IN ('succeeded','failed')),
  CONSTRAINT embedding_reconciliation_json_bounded CHECK (octet_length(response_evidence::text) <= 16384 AND octet_length(actor::text) <= 16384)
);

ALTER TABLE fornix.embedding_call_reconciliations
  ADD CONSTRAINT embedding_reconciliation_call_fk
  FOREIGN KEY (workspace_id, request_id)
  REFERENCES fornix.embedding_calls(workspace_id, request_id)
  ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS embedding_reconciliations_request_idx
  ON fornix.embedding_call_reconciliations(workspace_id, request_id, created_at, id);

ALTER TABLE fornix.embedding_call_reconciliations ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.embedding_call_reconciliations;
CREATE POLICY workspace_scope_isolation ON fornix.embedding_call_reconciliations
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

CREATE OR REPLACE FUNCTION fornix.prevent_embedding_reconciliation_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix embedding reconciliation history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS embedding_reconciliation_append_only ON fornix.embedding_call_reconciliations;
CREATE TRIGGER embedding_reconciliation_append_only
  BEFORE UPDATE OR DELETE ON fornix.embedding_call_reconciliations
  FOR EACH ROW EXECUTE FUNCTION fornix.prevent_embedding_reconciliation_mutation();
