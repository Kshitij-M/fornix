-- Durable workspace-scoped observations, cost accounting, metrics, and
-- offline replay evaluation. These tables are additive: existing authority
-- remains events, checkpoints, model/tool ledgers, retrieval, evidence, and
-- artifacts.

CREATE TABLE IF NOT EXISTS fornix.run_observations (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  kind TEXT NOT NULL,
  component TEXT NOT NULL,
  operation TEXT NOT NULL,
  outcome TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  session_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  causation_id TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL DEFAULT '',
  source_kind TEXT NOT NULL DEFAULT '',
  source_id TEXT NOT NULL DEFAULT '',
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  duration_ms BIGINT NOT NULL DEFAULT 0,
  db_queries INTEGER NOT NULL DEFAULT 0,
  db_rows BIGINT NOT NULL DEFAULT 0,
  input_bytes BIGINT NOT NULL DEFAULT 0,
  output_bytes BIGINT NOT NULL DEFAULT 0,
  input_tokens INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0,
  total_tokens INTEGER NOT NULL DEFAULT 0,
  usage_measured BOOLEAN NOT NULL DEFAULT FALSE,
  usage_estimated BOOLEAN NOT NULL DEFAULT FALSE,
  cost_usd NUMERIC(24,12) NOT NULL DEFAULT 0,
  cost_known BOOLEAN NOT NULL DEFAULT FALSE,
  retry_count INTEGER NOT NULL DEFAULT 0,
  duplicate_work BOOLEAN NOT NULL DEFAULT FALSE,
  artifact_bytes BIGINT NOT NULL DEFAULT 0,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  evidence JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT observations_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT observations_idempotency_nonempty CHECK (length(idempotency_key) > 0),
  CONSTRAINT observations_hash_shape CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT observations_counters_nonnegative CHECK (duration_ms >= 0 AND db_queries >= 0 AND db_rows >= 0 AND input_bytes >= 0 AND output_bytes >= 0 AND input_tokens >= 0 AND output_tokens >= 0 AND total_tokens >= 0 AND retry_count >= 0 AND artifact_bytes >= 0 AND cost_usd >= 0),
  CONSTRAINT observations_evidence_size CHECK (octet_length(evidence::text) <= 16384),
  CONSTRAINT observations_usage_flags CHECK (NOT (usage_measured AND usage_estimated)),
  UNIQUE (workspace_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS observations_workspace_time_idx ON fornix.run_observations(workspace_id, created_at, id);
CREATE INDEX IF NOT EXISTS observations_workspace_dimensions_idx ON fornix.run_observations(workspace_id, kind, component, operation, outcome, created_at);

CREATE TABLE IF NOT EXISTS fornix.trace_spans (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  trace_id TEXT NOT NULL,
  parent_id TEXT NOT NULL DEFAULT '',
  idempotency_key TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  component TEXT NOT NULL,
  operation TEXT NOT NULL,
  outcome TEXT NOT NULL,
  started_at TIMESTAMPTZ NOT NULL,
  finished_at TIMESTAMPTZ,
  duration_ms BIGINT NOT NULL DEFAULT 0,
  attributes JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT spans_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT spans_identity_nonempty CHECK (length(trace_id) > 0 AND length(idempotency_key) > 0),
  CONSTRAINT spans_counters_nonnegative CHECK (duration_ms >= 0),
  UNIQUE (workspace_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS spans_workspace_trace_idx ON fornix.trace_spans(workspace_id, trace_id, started_at, id);

CREATE TABLE IF NOT EXISTS fornix.cost_ledger (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  payload_hash TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  category TEXT NOT NULL,
  basis TEXT NOT NULL DEFAULT '',
  source_kind TEXT NOT NULL,
  source_id TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  session_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  causation_id TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL DEFAULT '',
  units NUMERIC(24,12) NOT NULL DEFAULT 0,
  unit_cost_usd NUMERIC(24,12) NOT NULL DEFAULT 0,
  amount_usd NUMERIC(24,12) NOT NULL DEFAULT 0,
  amount_known BOOLEAN NOT NULL DEFAULT FALSE,
  measured BOOLEAN NOT NULL DEFAULT FALSE,
  estimated BOOLEAN NOT NULL DEFAULT FALSE,
  input_tokens INTEGER NOT NULL DEFAULT 0,
  output_tokens INTEGER NOT NULL DEFAULT 0,
  duration_ms BIGINT NOT NULL DEFAULT 0,
  bytes BIGINT NOT NULL DEFAULT 0,
  duplicate_work BOOLEAN NOT NULL DEFAULT FALSE,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT costs_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT costs_identity_nonempty CHECK (length(idempotency_key) > 0 AND length(source_kind) > 0 AND length(source_id) > 0),
  CONSTRAINT costs_hash_shape CHECK (payload_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT costs_values_nonnegative CHECK (units >= 0 AND unit_cost_usd >= 0 AND amount_usd >= 0 AND input_tokens >= 0 AND output_tokens >= 0 AND duration_ms >= 0 AND bytes >= 0),
  CONSTRAINT costs_flags_valid CHECK (NOT (measured AND estimated)),
  UNIQUE (workspace_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS costs_workspace_category_idx ON fornix.cost_ledger(workspace_id, category, created_at, id);
CREATE INDEX IF NOT EXISTS costs_workspace_source_idx ON fornix.cost_ledger(workspace_id, source_kind, source_id, created_at);

CREATE TABLE IF NOT EXISTS fornix.metric_samples (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  name TEXT NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  sample_count BIGINT NOT NULL DEFAULT 0,
  observed_at TIMESTAMPTZ NOT NULL,
  dimensions JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT metrics_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT metrics_identity_nonempty CHECK (length(idempotency_key) > 0 AND length(name) > 0),
  CONSTRAINT metrics_count_nonnegative CHECK (sample_count >= 0),
  UNIQUE (workspace_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS metrics_workspace_time_idx ON fornix.metric_samples(workspace_id, name, observed_at, id);

CREATE TABLE IF NOT EXISTS fornix.eval_datasets (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  name TEXT NOT NULL,
  version INTEGER NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  dataset_hash TEXT NOT NULL,
  cases JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT eval_datasets_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT eval_datasets_name_nonempty CHECK (length(name) > 0),
  CONSTRAINT eval_datasets_version_positive CHECK (version > 0),
  CONSTRAINT eval_datasets_hash_shape CHECK (dataset_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT eval_datasets_cases_size CHECK (octet_length(cases::text) <= 1048576),
  UNIQUE (workspace_id, id),
  UNIQUE (workspace_id, name, version),
  UNIQUE (workspace_id, dataset_hash)
);

CREATE TABLE IF NOT EXISTS fornix.eval_runs (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  dataset_id TEXT NOT NULL,
  dataset_hash TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'pending',
  dry_run BOOLEAN NOT NULL DEFAULT FALSE,
  batch_limit INTEGER NOT NULL DEFAULT 100,
  cases_total INTEGER NOT NULL DEFAULT 0,
  cases_completed INTEGER NOT NULL DEFAULT 0,
  cases_passed INTEGER NOT NULL DEFAULT 0,
  cases_failed INTEGER NOT NULL DEFAULT 0,
  cost_usd NUMERIC(24,12) NOT NULL DEFAULT 0,
  cost_known BOOLEAN NOT NULL DEFAULT FALSE,
  replay_hash TEXT NOT NULL DEFAULT '',
  gates JSONB NOT NULL DEFAULT '[]'::jsonb,
  report JSONB NOT NULL DEFAULT '{}'::jsonb,
  report_artifact_id BIGINT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  CONSTRAINT eval_runs_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT eval_runs_identity_nonempty CHECK (length(dataset_id) > 0 AND length(dataset_hash) > 0 AND length(idempotency_key) > 0 AND length(request_hash) > 0),
  CONSTRAINT eval_runs_status_valid CHECK (status IN ('pending','running','succeeded','failed','cancelled')),
  CONSTRAINT eval_runs_batch_valid CHECK (batch_limit BETWEEN 1 AND 1000),
  CONSTRAINT eval_runs_counts_valid CHECK (cases_total >= 0 AND cases_completed >= 0 AND cases_passed >= 0 AND cases_failed >= 0 AND cost_usd >= 0),
  CONSTRAINT eval_runs_report_size CHECK (octet_length(report::text) <= 65536),
  UNIQUE (workspace_id, idempotency_key),
  UNIQUE (workspace_id, id),
  CONSTRAINT eval_runs_dataset_fk FOREIGN KEY (workspace_id, dataset_id) REFERENCES fornix.eval_datasets(workspace_id, id)
);
CREATE INDEX IF NOT EXISTS eval_runs_workspace_status_idx ON fornix.eval_runs(workspace_id, status, created_at, id);

CREATE TABLE IF NOT EXISTS fornix.eval_results (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  eval_run_id TEXT NOT NULL,
  case_id TEXT NOT NULL,
  replay_run_id TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  input_hash TEXT NOT NULL,
  context_hash TEXT NOT NULL DEFAULT '',
  termination TEXT NOT NULL DEFAULT '',
  observed_cost_usd NUMERIC(24,12) NOT NULL DEFAULT 0,
  cost_known BOOLEAN NOT NULL DEFAULT FALSE,
  replay_hash TEXT NOT NULL,
  passed BOOLEAN NOT NULL,
  abstained BOOLEAN NOT NULL DEFAULT FALSE,
  gates JSONB NOT NULL DEFAULT '[]'::jsonb,
  error TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT eval_results_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT eval_results_identity_nonempty CHECK (length(eval_run_id) > 0 AND length(case_id) > 0 AND length(replay_run_id) > 0 AND length(input_hash) > 0),
  CONSTRAINT eval_results_hash_shape CHECK (replay_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT eval_results_cost_nonnegative CHECK (observed_cost_usd >= 0),
  CONSTRAINT eval_results_run_fk FOREIGN KEY (workspace_id, eval_run_id) REFERENCES fornix.eval_runs(workspace_id, id),
  UNIQUE (workspace_id, eval_run_id, case_id)
);
CREATE INDEX IF NOT EXISTS eval_results_workspace_run_idx ON fornix.eval_results(workspace_id, eval_run_id, case_id);

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'eval_runs_report_artifact_fk') THEN
    ALTER TABLE fornix.eval_runs ADD CONSTRAINT eval_runs_report_artifact_fk
      FOREIGN KEY (workspace_id, report_artifact_id) REFERENCES fornix.artifacts(workspace_id, id);
  END IF;
END $$;

CREATE OR REPLACE FUNCTION fornix.reject_observability_history_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix observability history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS run_observations_append_only ON fornix.run_observations;
CREATE TRIGGER run_observations_append_only BEFORE UPDATE OR DELETE ON fornix.run_observations FOR EACH ROW EXECUTE FUNCTION fornix.reject_observability_history_mutation();
DROP TRIGGER IF EXISTS trace_spans_append_only ON fornix.trace_spans;
CREATE TRIGGER trace_spans_append_only BEFORE UPDATE OR DELETE ON fornix.trace_spans FOR EACH ROW EXECUTE FUNCTION fornix.reject_observability_history_mutation();
DROP TRIGGER IF EXISTS cost_ledger_append_only ON fornix.cost_ledger;
CREATE TRIGGER cost_ledger_append_only BEFORE UPDATE OR DELETE ON fornix.cost_ledger FOR EACH ROW EXECUTE FUNCTION fornix.reject_observability_history_mutation();
DROP TRIGGER IF EXISTS metric_samples_append_only ON fornix.metric_samples;
CREATE TRIGGER metric_samples_append_only BEFORE UPDATE OR DELETE ON fornix.metric_samples FOR EACH ROW EXECUTE FUNCTION fornix.reject_observability_history_mutation();
DROP TRIGGER IF EXISTS eval_datasets_append_only ON fornix.eval_datasets;
CREATE TRIGGER eval_datasets_append_only BEFORE UPDATE OR DELETE ON fornix.eval_datasets FOR EACH ROW EXECUTE FUNCTION fornix.reject_observability_history_mutation();
DROP TRIGGER IF EXISTS eval_results_append_only ON fornix.eval_results;
CREATE TRIGGER eval_results_append_only BEFORE UPDATE OR DELETE ON fornix.eval_results FOR EACH ROW EXECUTE FUNCTION fornix.reject_observability_history_mutation();
