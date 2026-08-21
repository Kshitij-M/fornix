-- 023: deterministic retrieval quality metrics and baseline regressions.
-- Additive only: authoritative evidence, retrieval history, and eval history
-- remain immutable; these fields enrich the existing append-only results.

ALTER TABLE fornix.eval_runs
  ADD COLUMN IF NOT EXISTS baseline_eval_run_id TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS retrieval_quality JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS regressions JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE fornix.eval_results
  ADD COLUMN IF NOT EXISTS retrieval_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
  ADD COLUMN IF NOT EXISTS resolved_evidence JSONB NOT NULL DEFAULT '[]'::jsonb,
  ADD COLUMN IF NOT EXISTS regressions JSONB NOT NULL DEFAULT '[]'::jsonb;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'eval_runs_quality_payload_size') THEN
    ALTER TABLE fornix.eval_runs ADD CONSTRAINT eval_runs_quality_payload_size
      CHECK (octet_length(retrieval_quality::text) <= 65536 AND octet_length(regressions::text) <= 65536);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'eval_results_quality_payload_size') THEN
    ALTER TABLE fornix.eval_results ADD CONSTRAINT eval_results_quality_payload_size
      CHECK (octet_length(retrieval_metrics::text) <= 65536 AND octet_length(resolved_evidence::text) <= 16384 AND octet_length(regressions::text) <= 65536);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS eval_runs_workspace_baseline_idx
  ON fornix.eval_runs(workspace_id, baseline_eval_run_id)
  WHERE baseline_eval_run_id <> '';
