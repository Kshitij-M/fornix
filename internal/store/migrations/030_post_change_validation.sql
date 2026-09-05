-- 030: deterministic post-change validation and re-index handoffs.
-- Postgres owns validation identity, immutable check history, handoff state,
-- and audit metadata. Repository inspection and ingestion remain explicit
-- bounded boundaries outside this transaction.

CREATE TABLE IF NOT EXISTS fornix.validation_runs (
  id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  change_application_id TEXT NOT NULL,
  proposal_id TEXT NOT NULL,
  packet_hash TEXT NOT NULL,
  expected_tree_hash TEXT NOT NULL,
  observed_tree_hash TEXT NOT NULL DEFAULT '',
  source_manifest_hash TEXT NOT NULL DEFAULT '',
  repository TEXT NOT NULL,
  source_root TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  session_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  agent_run_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_owner_id TEXT NOT NULL DEFAULT '',
  task_fence BIGINT NOT NULL DEFAULT 0,
  plan JSONB NOT NULL DEFAULT '{}'::jsonb,
  budget JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'pending',
  outcome TEXT NOT NULL DEFAULT '',
  dry_run BOOLEAN NOT NULL DEFAULT FALSE,
  result_count INTEGER NOT NULL DEFAULT 0,
  passed_count INTEGER NOT NULL DEFAULT 0,
  failed_count INTEGER NOT NULL DEFAULT 0,
  abstained_count INTEGER NOT NULL DEFAULT 0,
  report JSONB NOT NULL DEFAULT '{}'::jsonb,
  report_hash TEXT NOT NULL DEFAULT '',
  report_artifact_id BIGINT,
  replay_hash TEXT NOT NULL DEFAULT '',
  last_error TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  started_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  PRIMARY KEY (workspace_id, id),
  CONSTRAINT validation_runs_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT validation_runs_identity_nonempty CHECK (length(id) > 0 AND length(request_id) > 0 AND length(idempotency_key) > 0),
  CONSTRAINT validation_runs_hash_shape CHECK (
    request_hash ~ '^[0-9a-f]{64}$' AND packet_hash ~ '^[0-9a-f]{64}$' AND
    expected_tree_hash ~ '^[0-9a-f]{64}$' AND
    (observed_tree_hash = '' OR observed_tree_hash ~ '^[0-9a-f]{64}$') AND
    (source_manifest_hash = '' OR source_manifest_hash ~ '^[0-9a-f]{64}$') AND
    (report_hash = '' OR report_hash ~ '^[0-9a-f]{64}$') AND
    (replay_hash = '' OR replay_hash ~ '^[0-9a-f]{64}$')
  ),
  CONSTRAINT validation_runs_status_valid CHECK (status IN ('pending','running','passed','failed','abstained','cancelled','recovery_required')),
  CONSTRAINT validation_runs_outcome_valid CHECK (outcome IN ('','passed','failed','abstained','skipped')),
  CONSTRAINT validation_runs_counts_valid CHECK (result_count >= 0 AND passed_count >= 0 AND failed_count >= 0 AND abstained_count >= 0 AND task_fence >= 0),
  CONSTRAINT validation_runs_json_bounded CHECK (
    octet_length(actor::text) <= 16384 AND octet_length(task_ref::text) <= 4096 AND
    octet_length(session_ref::text) <= 4096 AND octet_length(agent_run_ref::text) <= 4096 AND
    octet_length(plan::text) <= 65536 AND octet_length(budget::text) <= 16384 AND
    octet_length(report::text) <= 1048576 AND octet_length(last_error) <= 8192
  ),
  CONSTRAINT validation_runs_application_fk FOREIGN KEY (workspace_id, change_application_id)
    REFERENCES fornix.change_applications(workspace_id, id),
  CONSTRAINT validation_runs_proposal_fk FOREIGN KEY (workspace_id, proposal_id)
    REFERENCES fornix.change_proposals(workspace_id, id),
  UNIQUE (workspace_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS validation_runs_workspace_status_idx
  ON fornix.validation_runs(workspace_id, status, updated_at DESC, id);
CREATE INDEX IF NOT EXISTS validation_runs_workspace_change_idx
  ON fornix.validation_runs(workspace_id, change_application_id, created_at DESC, id);

CREATE TABLE IF NOT EXISTS fornix.validation_check_results (
  id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  validation_run_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  validator_id TEXT NOT NULL,
  validator_version TEXT NOT NULL,
  attempt INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL,
  outcome TEXT NOT NULL,
  input_hash TEXT NOT NULL,
  result_hash TEXT NOT NULL,
  summary TEXT NOT NULL DEFAULT '',
  failure JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
  output_artifact_id BIGINT,
  files INTEGER NOT NULL DEFAULT 0,
  bytes BIGINT NOT NULL DEFAULT 0,
  sql_queries INTEGER NOT NULL DEFAULT 0,
  duration_ms BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, id),
  CONSTRAINT validation_check_results_run_fk FOREIGN KEY (workspace_id, validation_run_id)
    REFERENCES fornix.validation_runs(workspace_id, id),
  CONSTRAINT validation_check_results_ordinal_valid CHECK (ordinal BETWEEN 0 AND 63 AND attempt >= 1),
  CONSTRAINT validation_check_results_identity_nonempty CHECK (length(id) > 0 AND length(validator_id) > 0 AND length(validator_version) > 0),
  CONSTRAINT validation_check_results_hash_shape CHECK (input_hash ~ '^[0-9a-f]{64}$' AND result_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT validation_check_results_status_valid CHECK (status IN ('passed','failed','abstained','skipped')),
  CONSTRAINT validation_check_results_outcome_valid CHECK (outcome IN ('passed','failed','abstained','skipped')),
  CONSTRAINT validation_check_results_measurements_valid CHECK (files >= 0 AND bytes >= 0 AND sql_queries >= 0 AND duration_ms >= 0),
  CONSTRAINT validation_check_results_json_bounded CHECK (octet_length(failure::text) <= 32768 AND octet_length(evidence::text) <= 65536 AND octet_length(summary) <= 8192),
  UNIQUE (workspace_id, validation_run_id, ordinal, attempt),
  UNIQUE (workspace_id, validation_run_id, validator_id, validator_version, attempt)
);

CREATE INDEX IF NOT EXISTS validation_check_results_run_ordinal_idx
  ON fornix.validation_check_results(workspace_id, validation_run_id, ordinal, attempt);

CREATE TABLE IF NOT EXISTS fornix.validation_transitions (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  validation_run_id TEXT NOT NULL,
  from_status TEXT NOT NULL DEFAULT '',
  to_status TEXT NOT NULL,
  outcome TEXT NOT NULL DEFAULT '',
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT validation_transitions_run_fk FOREIGN KEY (workspace_id, validation_run_id)
    REFERENCES fornix.validation_runs(workspace_id, id),
  CONSTRAINT validation_transitions_identity_nonempty CHECK (length(workspace_id) > 0 AND length(validation_run_id) > 0 AND length(to_status) > 0 AND length(request_id) > 0),
  CONSTRAINT validation_transitions_json_bounded CHECK (octet_length(actor::text) <= 16384 AND octet_length(reason) <= 8192)
);

CREATE INDEX IF NOT EXISTS validation_transitions_run_idx
  ON fornix.validation_transitions(workspace_id, validation_run_id, id);

CREATE TABLE IF NOT EXISTS fornix.validation_artifact_links (
  workspace_id TEXT NOT NULL,
  validation_run_id TEXT NOT NULL,
  result_id TEXT NOT NULL DEFAULT '',
  artifact_id BIGINT NOT NULL,
  role TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, validation_run_id, result_id, artifact_id, role),
  CONSTRAINT validation_artifact_links_run_fk FOREIGN KEY (workspace_id, validation_run_id)
    REFERENCES fornix.validation_runs(workspace_id, id),
  CONSTRAINT validation_artifact_links_role_nonempty CHECK (length(role) > 0)
);

CREATE INDEX IF NOT EXISTS validation_artifact_links_artifact_idx
  ON fornix.validation_artifact_links(workspace_id, artifact_id, validation_run_id);

CREATE TABLE IF NOT EXISTS fornix.reindex_handoffs (
  id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  validation_run_id TEXT NOT NULL,
  change_application_id TEXT NOT NULL,
  repository TEXT NOT NULL,
  source_root TEXT NOT NULL,
  previous_manifest_hash TEXT NOT NULL DEFAULT '',
  expected_tree_hash TEXT NOT NULL,
  observed_tree_hash TEXT NOT NULL,
  manifest_hash TEXT NOT NULL DEFAULT '',
  ingest_job_id TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'pending',
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  session_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_owner_id TEXT NOT NULL DEFAULT '',
  task_fence BIGINT NOT NULL DEFAULT 0,
  failure JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  submitted_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  PRIMARY KEY (workspace_id, id),
  CONSTRAINT reindex_handoffs_run_fk FOREIGN KEY (workspace_id, validation_run_id)
    REFERENCES fornix.validation_runs(workspace_id, id),
  CONSTRAINT reindex_handoffs_application_fk FOREIGN KEY (workspace_id, change_application_id)
    REFERENCES fornix.change_applications(workspace_id, id),
  CONSTRAINT reindex_handoffs_identity_nonempty CHECK (length(id) > 0 AND length(request_id) > 0 AND length(idempotency_key) > 0 AND length(repository) > 0),
  CONSTRAINT reindex_handoffs_hash_shape CHECK (
    request_hash ~ '^[0-9a-f]{64}$' AND expected_tree_hash ~ '^[0-9a-f]{64}$' AND observed_tree_hash ~ '^[0-9a-f]{64}$' AND
    (previous_manifest_hash = '' OR previous_manifest_hash ~ '^[0-9a-f]{64}$') AND
    (manifest_hash = '' OR manifest_hash ~ '^[0-9a-f]{64}$')
  ),
  CONSTRAINT reindex_handoffs_status_valid CHECK (status IN ('pending','submitted','running','succeeded','failed','cancelled')),
  CONSTRAINT reindex_handoffs_fence_valid CHECK (task_fence >= 0),
  CONSTRAINT reindex_handoffs_json_bounded CHECK (octet_length(actor::text) <= 16384 AND octet_length(task_ref::text) <= 4096 AND octet_length(session_ref::text) <= 4096 AND octet_length(failure::text) <= 32768),
  UNIQUE (workspace_id, idempotency_key),
  UNIQUE (workspace_id, validation_run_id)
);

CREATE INDEX IF NOT EXISTS reindex_handoffs_workspace_status_idx
  ON fornix.reindex_handoffs(workspace_id, status, updated_at DESC, id);

CREATE OR REPLACE FUNCTION fornix.reject_validation_history_mutation()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix validation history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS validation_check_results_append_only ON fornix.validation_check_results;
CREATE TRIGGER validation_check_results_append_only
  BEFORE UPDATE OR DELETE ON fornix.validation_check_results
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_validation_history_mutation();
DROP TRIGGER IF EXISTS validation_transitions_append_only ON fornix.validation_transitions;
CREATE TRIGGER validation_transitions_append_only
  BEFORE UPDATE OR DELETE ON fornix.validation_transitions
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_validation_history_mutation();
DROP TRIGGER IF EXISTS validation_artifact_links_append_only ON fornix.validation_artifact_links;
CREATE TRIGGER validation_artifact_links_append_only
  BEFORE UPDATE OR DELETE ON fornix.validation_artifact_links
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_validation_history_mutation();
