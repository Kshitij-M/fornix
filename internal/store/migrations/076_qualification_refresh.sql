-- 076: atomic deployment qualification evidence refresh history.
-- Refreshes replace hash-only links; accepted signed imports remain the raw
-- evidence authority and are never copied or mutated here.

CREATE TABLE IF NOT EXISTS fornix.qualification_refresh_runs (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  release_id TEXT NOT NULL,
  as_of TIMESTAMPTZ NOT NULL,
  request_hash TEXT NOT NULL,
  before_gate_hash TEXT NOT NULL,
  after_gate_hash TEXT NOT NULL,
  outcome TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT,
  idempotency_key TEXT NOT NULL,
  causation_id TEXT,
  correlation_id TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_refresh_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_refresh_deployment_nonempty CHECK (length(deployment_id) BETWEEN 1 AND 128),
  CONSTRAINT qualification_refresh_release_nonempty CHECK (length(release_id) > 0),
  CONSTRAINT qualification_refresh_hashes_valid CHECK (request_hash ~ '^[0-9a-f]{64}$' AND before_gate_hash ~ '^[0-9a-f]{64}$' AND after_gate_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT qualification_refresh_outcome_valid CHECK (outcome IN ('passed','failed','blocked','skipped')),
  CONSTRAINT qualification_refresh_actor_bounded CHECK (octet_length(actor::text) <= 4096),
  CONSTRAINT qualification_refresh_idempotency_nonempty CHECK (length(idempotency_key) BETWEEN 1 AND 256),
  CONSTRAINT qualification_refresh_identity_unique UNIQUE (workspace_id, deployment_id, release_id, request_hash),
  CONSTRAINT qualification_refresh_idempotency_unique UNIQUE (workspace_id, deployment_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS qualification_refresh_lookup_idx
  ON fornix.qualification_refresh_runs(workspace_id, deployment_id, release_id, id DESC);

CREATE TABLE IF NOT EXISTS fornix.qualification_refresh_items (
  run_id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  release_id TEXT NOT NULL,
  item_index SMALLINT NOT NULL,
  kind TEXT NOT NULL,
  import_id TEXT NOT NULL,
  previous_link_id TEXT,
  link_id TEXT NOT NULL,
  signed_hash TEXT NOT NULL,
  observation_hash TEXT NOT NULL,
  report_hash TEXT NOT NULL,
  manifest_hash TEXT NOT NULL,
  source_hash TEXT NOT NULL,
  boundary_evidence_hash TEXT,
  boundary_evidence_until TIMESTAMPTZ,
  outcome TEXT NOT NULL,
  reason TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (run_id, item_index),
  CONSTRAINT qualification_refresh_item_scope_fk FOREIGN KEY (run_id) REFERENCES fornix.qualification_refresh_runs(id),
  CONSTRAINT qualification_refresh_item_scope_nonempty CHECK (length(workspace_id) > 0 AND length(deployment_id) > 0 AND length(release_id) > 0),
  CONSTRAINT qualification_refresh_item_index_valid CHECK (item_index BETWEEN 0 AND 5),
  CONSTRAINT qualification_refresh_item_kind_valid CHECK (kind IN ('release','migration','backup_restore','topology','provider','external_effect')),
  CONSTRAINT qualification_refresh_item_ids_nonempty CHECK (length(import_id) > 0 AND length(link_id) > 0),
  CONSTRAINT qualification_refresh_item_hashes_valid CHECK (signed_hash ~ '^[0-9a-f]{64}$' AND observation_hash ~ '^[0-9a-f]{64}$' AND report_hash ~ '^[0-9a-f]{64}$' AND manifest_hash ~ '^[0-9a-f]{64}$' AND source_hash ~ '^[0-9a-f]{64}$' AND (boundary_evidence_hash IS NULL OR boundary_evidence_hash ~ '^[0-9a-f]{64}$')),
  CONSTRAINT qualification_refresh_item_boundary_valid CHECK ((boundary_evidence_hash IS NULL AND boundary_evidence_until IS NULL) OR (boundary_evidence_hash IS NOT NULL AND boundary_evidence_until IS NOT NULL)),
  CONSTRAINT qualification_refresh_item_outcome_valid CHECK (outcome IN ('passed','failed','blocked','skipped')),
  CONSTRAINT qualification_refresh_item_reason_bounded CHECK (reason IS NULL OR (length(reason) BETWEEN 1 AND 256 AND reason !~ '[\x00\r\n]'))
);

CREATE INDEX IF NOT EXISTS qualification_refresh_items_lookup_idx
  ON fornix.qualification_refresh_items(workspace_id, deployment_id, release_id, run_id, item_index);

CREATE TABLE IF NOT EXISTS fornix.qualification_refresh_events (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  deployment_id TEXT NOT NULL,
  release_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  event TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT,
  idempotency_key TEXT,
  causation_id TEXT,
  correlation_id TEXT,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT qualification_refresh_events_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT qualification_refresh_events_deployment_nonempty CHECK (length(deployment_id) > 0),
  CONSTRAINT qualification_refresh_events_release_nonempty CHECK (length(release_id) > 0),
  CONSTRAINT qualification_refresh_events_run_nonempty CHECK (length(run_id) > 0),
  CONSTRAINT qualification_refresh_events_event_valid CHECK (event IN ('refreshed')),
  CONSTRAINT qualification_refresh_events_json_bounded CHECK (octet_length(actor::text) <= 4096 AND octet_length(metadata::text) <= 8192)
);

CREATE INDEX IF NOT EXISTS qualification_refresh_events_lookup_idx
  ON fornix.qualification_refresh_events(workspace_id, deployment_id, release_id, occurred_at, id);

CREATE OR REPLACE FUNCTION fornix.reject_qualification_refresh_history_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix qualification refresh history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS qualification_refresh_runs_append_only ON fornix.qualification_refresh_runs;
CREATE TRIGGER qualification_refresh_runs_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_refresh_runs
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_refresh_history_mutation();

DROP TRIGGER IF EXISTS qualification_refresh_items_append_only ON fornix.qualification_refresh_items;
CREATE TRIGGER qualification_refresh_items_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_refresh_items
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_refresh_history_mutation();

DROP TRIGGER IF EXISTS qualification_refresh_events_append_only ON fornix.qualification_refresh_events;
CREATE TRIGGER qualification_refresh_events_append_only
  BEFORE UPDATE OR DELETE ON fornix.qualification_refresh_events
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_qualification_refresh_history_mutation();

ALTER TABLE fornix.qualification_refresh_runs ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_refresh_runs;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_refresh_runs
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.qualification_refresh_items ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_refresh_items;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_refresh_items
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.qualification_refresh_events ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.qualification_refresh_events;
CREATE POLICY workspace_scope_isolation ON fornix.qualification_refresh_events
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
