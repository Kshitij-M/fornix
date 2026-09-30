-- 040: durable hash-only results for generic connector execution.
-- Raw connector payloads remain in their domain evidence/artifact authorities.

CREATE TABLE IF NOT EXISTS fornix.operation_results (
  workspace_id TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  result_id TEXT NOT NULL,
  result_hash TEXT NOT NULL,
  result JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, operation_id),
  UNIQUE (workspace_id, result_id),
  CONSTRAINT operation_results_operation_fk FOREIGN KEY (workspace_id, operation_id)
    REFERENCES fornix.operations(workspace_id, id),
  CONSTRAINT operation_results_hash_shape CHECK (result_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT operation_results_json_bounded CHECK (octet_length(result::text) <= 262144)
);

CREATE INDEX IF NOT EXISTS operation_results_created_idx
  ON fornix.operation_results(workspace_id, created_at, operation_id);

CREATE OR REPLACE FUNCTION fornix.reject_operation_result_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix operation results are append-only';
END;
$$;

DROP TRIGGER IF EXISTS operation_results_append_only ON fornix.operation_results;
CREATE TRIGGER operation_results_append_only
  BEFORE UPDATE OR DELETE ON fornix.operation_results
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_operation_result_mutation();
