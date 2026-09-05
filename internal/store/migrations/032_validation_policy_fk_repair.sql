-- 032: repair policy-pin foreign keys individually.
-- Migration 031 is immutable. Its compatibility guard handled duplicate
-- constraints around the whole loop, which could stop a partially prepared
-- database before later policy-bearing tables received their composite FK.
-- This forward-only repair is safe on fresh and existing databases.

DO $$
DECLARE
  table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'change_proposals','change_applications','change_approvals',
    'validation_runs','reindex_handoffs','work_receipts','run_observations',
    'trace_spans','cost_ledger','metric_samples','control_events'
  ] LOOP
    BEGIN
      EXECUTE format(
        'ALTER TABLE fornix.%I ADD CONSTRAINT %I FOREIGN KEY (workspace_id, policy_id, policy_version, policy_hash) REFERENCES fornix.validation_policy_versions(workspace_id, policy_id, version, policy_hash)',
        table_name, table_name || '_policy_fk'
      );
    EXCEPTION WHEN duplicate_object THEN
      NULL;
    END;
  END LOOP;
END $$;
