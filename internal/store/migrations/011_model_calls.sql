-- Durable model execution ledger. This is execution metadata, not an
-- alternative source of truth for append-only control events.
CREATE TABLE IF NOT EXISTS fornix.model_calls (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  provider TEXT NOT NULL,
  endpoint TEXT NOT NULL DEFAULT '',
  model TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  session_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  content_emitted BOOLEAN NOT NULL DEFAULT FALSE,
  provider_request_id TEXT NOT NULL DEFAULT '',
  usage JSONB NOT NULL DEFAULT '{}'::jsonb,
  cost JSONB NOT NULL DEFAULT '{}'::jsonb,
  failure JSONB,
  response JSONB,
  request_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  response_evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  CONSTRAINT model_calls_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT model_calls_request_nonempty CHECK (length(request_id) > 0),
  CONSTRAINT model_calls_idempotency_nonempty CHECK (length(idempotency_key) > 0),
  CONSTRAINT model_calls_hash_shape CHECK (request_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT model_calls_provider_nonempty CHECK (length(provider) > 0),
  CONSTRAINT model_calls_model_nonempty CHECK (length(model) > 0),
  CONSTRAINT model_calls_status_valid CHECK (status IN ('pending', 'running', 'succeeded', 'failed')),
  CONSTRAINT model_calls_attempt_nonnegative CHECK (attempt_count >= 0),
  CONSTRAINT model_calls_request_evidence_size CHECK (octet_length(request_evidence::text) <= 1048576),
  CONSTRAINT model_calls_response_evidence_size CHECK (octet_length(response_evidence::text) <= 1048576)
);

ALTER TABLE fornix.model_calls ADD COLUMN IF NOT EXISTS endpoint TEXT NOT NULL DEFAULT '';
ALTER TABLE fornix.model_calls ADD COLUMN IF NOT EXISTS response_evidence JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE UNIQUE INDEX IF NOT EXISTS model_calls_workspace_request_idx
  ON fornix.model_calls (workspace_id, request_id);
CREATE UNIQUE INDEX IF NOT EXISTS model_calls_workspace_idempotency_idx
  ON fornix.model_calls (workspace_id, idempotency_key);
CREATE INDEX IF NOT EXISTS model_calls_workspace_status_idx
  ON fornix.model_calls (workspace_id, status, created_at, id);
CREATE INDEX IF NOT EXISTS model_calls_workspace_provider_idx
  ON fornix.model_calls (workspace_id, provider, model, created_at DESC);
