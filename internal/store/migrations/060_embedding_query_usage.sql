-- Migration 060: bounded, hash-only query-embedding use attribution.

CREATE TABLE IF NOT EXISTS fornix.embedding_query_uses (
  id BIGSERIAL PRIMARY KEY,
  schema_version INTEGER NOT NULL DEFAULT 1,
  workspace_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_id TEXT NOT NULL,
  embedding_request_id TEXT NOT NULL,
  source_hash TEXT NOT NULL,
  provider JSONB NOT NULL,
  actor JSONB NOT NULL,
  route TEXT NOT NULL,
  gate_reason TEXT NOT NULL,
  cache_hit BOOLEAN NOT NULL DEFAULT FALSE,
  duplicate_work BOOLEAN NOT NULL DEFAULT FALSE,
  usage JSONB NOT NULL DEFAULT '{}'::jsonb,
  cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  cost_known BOOLEAN NOT NULL DEFAULT FALSE,
  usage_measured BOOLEAN NOT NULL DEFAULT FALSE,
  usage_estimated BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE(workspace_id, idempotency_key),
  CONSTRAINT embedding_query_use_identity CHECK (
    length(workspace_id) > 0 AND length(idempotency_key) > 0 AND length(request_id) > 0 AND
    length(embedding_request_id) > 0 AND source_hash ~ '^[0-9a-f]{64}$' AND
    length(route) BETWEEN 1 AND 128 AND length(gate_reason) BETWEEN 1 AND 128
  ),
  CONSTRAINT embedding_query_use_cost CHECK (cost_usd >= 0),
  CONSTRAINT embedding_query_use_measurement CHECK (NOT (usage_measured AND usage_estimated)),
  CONSTRAINT embedding_query_use_json_bounded CHECK (octet_length(provider::text) <= 4096 AND octet_length(actor::text) <= 16384 AND octet_length(usage::text) <= 4096)
);

ALTER TABLE fornix.embedding_query_uses
  ADD CONSTRAINT embedding_query_use_call_fk
  FOREIGN KEY (workspace_id, embedding_request_id)
  REFERENCES fornix.embedding_calls(workspace_id, request_id)
  ON DELETE RESTRICT;

CREATE INDEX IF NOT EXISTS embedding_query_uses_workspace_created_idx
  ON fornix.embedding_query_uses(workspace_id, created_at, id);
CREATE INDEX IF NOT EXISTS embedding_query_uses_cache_idx
  ON fornix.embedding_query_uses(workspace_id, source_hash, provider, created_at);

ALTER TABLE fornix.embedding_query_uses ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.embedding_query_uses;
CREATE POLICY workspace_scope_isolation ON fornix.embedding_query_uses
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
