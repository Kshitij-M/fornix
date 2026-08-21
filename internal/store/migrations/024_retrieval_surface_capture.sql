-- 024: append-only redacted retrieval-surface captures for offline evaluation.
-- Retrieval/evidence rows remain authoritative; this table stores only hashes,
-- references, bounded traces, and operational measurements.

CREATE TABLE IF NOT EXISTS fornix.retrieval_surfaces (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  plan_hash TEXT NOT NULL,
  context_hash TEXT NOT NULL,
  budget JSONB NOT NULL,
  trace JSONB NOT NULL,
  evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  duration_ms BIGINT NOT NULL DEFAULT 0,
  sql_queries INTEGER NOT NULL DEFAULT 0,
  cost_usd NUMERIC(24,12) NOT NULL DEFAULT 0,
  cost_known BOOLEAN NOT NULL DEFAULT FALSE,
  cost_estimated BOOLEAN NOT NULL DEFAULT FALSE,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  causation_id TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL DEFAULT '',
  captured_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT retrieval_surfaces_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT retrieval_surfaces_identity_nonempty CHECK (length(request_id) > 0 AND length(idempotency_key) > 0),
  CONSTRAINT retrieval_surfaces_hash_shape CHECK (
    payload_hash ~ '^[0-9a-f]{64}$' AND request_hash ~ '^[0-9a-f]{64}$' AND
    plan_hash ~ '^[0-9a-f]{64}$' AND context_hash ~ '^[0-9a-f]{64}$'
  ),
  CONSTRAINT retrieval_surfaces_measurements_nonnegative CHECK (duration_ms >= 0 AND sql_queries >= 0 AND cost_usd >= 0),
  CONSTRAINT retrieval_surfaces_cost_flags CHECK (NOT (cost_known AND cost_estimated)),
  CONSTRAINT retrieval_surfaces_payload_size CHECK (
    octet_length(budget::text) <= 8192 AND octet_length(trace::text) <= 65536 AND octet_length(evidence_refs::text) <= 65536
  ),
  UNIQUE (workspace_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS retrieval_surfaces_workspace_time_idx
  ON fornix.retrieval_surfaces(workspace_id, captured_at, id);
CREATE INDEX IF NOT EXISTS retrieval_surfaces_workspace_request_idx
  ON fornix.retrieval_surfaces(workspace_id, request_hash, captured_at, id);
CREATE INDEX IF NOT EXISTS retrieval_surfaces_workspace_context_idx
  ON fornix.retrieval_surfaces(workspace_id, context_hash, captured_at, id);

CREATE OR REPLACE FUNCTION fornix.reject_retrieval_surface_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix retrieval surfaces are append-only';
END;
$$;

DROP TRIGGER IF EXISTS retrieval_surfaces_append_only ON fornix.retrieval_surfaces;
CREATE TRIGGER retrieval_surfaces_append_only
BEFORE UPDATE OR DELETE ON fornix.retrieval_surfaces
FOR EACH ROW EXECUTE FUNCTION fornix.reject_retrieval_surface_mutation();
