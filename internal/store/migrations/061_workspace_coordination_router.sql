-- Migration 061: workspace-scoped coordination and router authorities.
-- Historical global tables are intentionally preserved and are not backfilled
-- because ownership cannot be inferred safely.

CREATE TABLE IF NOT EXISTS fornix.workspace_coordination_messages (
  sequence BIGSERIAL PRIMARY KEY,
  id TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  workspace_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  sender TEXT NOT NULL,
  recipient TEXT NOT NULL,
  subject TEXT NOT NULL,
  body TEXT NOT NULL DEFAULT '',
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  causation_id TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL DEFAULT '',
  origin_host TEXT NOT NULL DEFAULT '',
  occurred_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (workspace_id, id),
  UNIQUE (workspace_id, idempotency_key),
  CONSTRAINT workspace_coordination_identity CHECK (
    length(id) BETWEEN 1 AND 128 AND length(workspace_id) BETWEEN 1 AND 256 AND
    length(request_id) BETWEEN 1 AND 256 AND length(idempotency_key) BETWEEN 1 AND 256 AND
    request_hash ~ '^[0-9a-f]{64}$' AND length(sender) BETWEEN 1 AND 256 AND
    length(recipient) BETWEEN 1 AND 256 AND length(subject) BETWEEN 1 AND 512 AND
    octet_length(body) <= 65536 AND octet_length(actor::text) <= 16384
  )
);

CREATE INDEX IF NOT EXISTS workspace_coordination_read_idx
  ON fornix.workspace_coordination_messages (workspace_id, sequence DESC);
CREATE INDEX IF NOT EXISTS workspace_coordination_recipient_idx
  ON fornix.workspace_coordination_messages (workspace_id, recipient, sequence DESC);

CREATE TABLE IF NOT EXISTS fornix.workspace_router_observations (
  id BIGSERIAL PRIMARY KEY,
  schema_version INTEGER NOT NULL DEFAULT 1,
  workspace_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  task_category TEXT NOT NULL,
  model_id TEXT NOT NULL,
  cost_usd DOUBLE PRECISION NOT NULL DEFAULT 0,
  latency_ms BIGINT NOT NULL DEFAULT 0,
  outcome TEXT NOT NULL DEFAULT 'unknown',
  outcome_score DOUBLE PRECISION,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  causation_id TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL DEFAULT '',
  observed_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (workspace_id, idempotency_key),
  CONSTRAINT workspace_router_observation_identity CHECK (
    length(workspace_id) BETWEEN 1 AND 256 AND length(request_id) BETWEEN 1 AND 256 AND
    length(idempotency_key) BETWEEN 1 AND 256 AND request_hash ~ '^[0-9a-f]{64}$' AND
    length(task_category) BETWEEN 1 AND 256 AND length(model_id) BETWEEN 1 AND 256 AND
    cost_usd >= 0 AND latency_ms >= 0 AND octet_length(actor::text) <= 16384
  ),
  CONSTRAINT workspace_router_observation_score CHECK (
    outcome_score IS NULL OR (outcome_score >= 0 AND outcome_score <= 1)
  )
);

CREATE INDEX IF NOT EXISTS workspace_router_observation_category_idx
  ON fornix.workspace_router_observations (workspace_id, task_category, observed_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS workspace_router_observation_model_idx
  ON fornix.workspace_router_observations (workspace_id, task_category, model_id, observed_at DESC);

ALTER TABLE fornix.workspace_coordination_messages ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.workspace_coordination_messages;
CREATE POLICY workspace_scope_isolation ON fornix.workspace_coordination_messages
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.workspace_router_observations ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.workspace_router_observations;
CREATE POLICY workspace_scope_isolation ON fornix.workspace_router_observations
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
