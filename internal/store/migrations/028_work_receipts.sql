-- 028: immutable, workspace-scoped Work Receipts.
-- Receipts are derived verification envelopes. Existing task, event, model,
-- tool, evidence, artifact, retrieval, cost, and replay tables remain the
-- authorities for their records.

CREATE TABLE IF NOT EXISTS fornix.work_receipts (
  id TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  work_kind TEXT NOT NULL,
  work_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  canonical_hash TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'verified',
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  session_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_owner_id TEXT NOT NULL DEFAULT '',
  task_fence BIGINT NOT NULL DEFAULT 0,
  source_manifest_hash TEXT NOT NULL DEFAULT '',
  replay_hash TEXT NOT NULL DEFAULT '',
  cost JSONB NOT NULL DEFAULT '{}'::jsonb,
  verification JSONB NOT NULL DEFAULT '{}'::jsonb,
  canonical_payload BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  verified_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, id),
  CONSTRAINT work_receipts_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT work_receipts_id_nonempty CHECK (length(id) > 0),
  CONSTRAINT work_receipts_work_identity_nonempty CHECK (length(work_kind) > 0 AND length(work_id) > 0),
  CONSTRAINT work_receipts_request_identity_nonempty CHECK (length(request_id) > 0 AND length(idempotency_key) > 0),
  CONSTRAINT work_receipts_hash_shape CHECK (request_hash ~ '^[0-9a-f]{64}$' AND canonical_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT work_receipts_status_valid CHECK (status IN ('verified', 'rejected')),
  CONSTRAINT work_receipts_fence_nonnegative CHECK (task_fence >= 0),
  CONSTRAINT work_receipts_manifest_hash_shape CHECK (source_manifest_hash = '' OR source_manifest_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT work_receipts_replay_hash_shape CHECK (replay_hash = '' OR replay_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT work_receipts_payload_size CHECK (octet_length(canonical_payload) BETWEEN 2 AND 1048576),
  CONSTRAINT work_receipts_json_size CHECK (
    octet_length(actor::text) <= 16384 AND octet_length(task_ref::text) <= 4096 AND
    octet_length(session_ref::text) <= 4096 AND octet_length(cost::text) <= 8192 AND
    octet_length(verification::text) <= 32768
  ),
  UNIQUE (workspace_id, work_kind, work_id),
  UNIQUE (workspace_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS work_receipts_workspace_time_idx
  ON fornix.work_receipts(workspace_id, created_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS work_receipts_workspace_work_idx
  ON fornix.work_receipts(workspace_id, work_kind, work_id);
CREATE INDEX IF NOT EXISTS work_receipts_workspace_hash_idx
  ON fornix.work_receipts(workspace_id, canonical_hash);

CREATE TABLE IF NOT EXISTS fornix.work_receipt_steps (
  workspace_id TEXT NOT NULL,
  receipt_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  step_id TEXT NOT NULL,
  name TEXT NOT NULL,
  kind TEXT NOT NULL,
  status TEXT NOT NULL,
  source_kind TEXT NOT NULL DEFAULT '',
  source_id TEXT NOT NULL DEFAULT '',
  source_hash TEXT NOT NULL DEFAULT '',
  input_hash TEXT NOT NULL DEFAULT '',
  output_hash TEXT NOT NULL DEFAULT '',
  reference_roles JSONB NOT NULL DEFAULT '[]'::jsonb,
  duration_ms BIGINT NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  retry_count INTEGER NOT NULL DEFAULT 0,
  duplicate_work BOOLEAN NOT NULL DEFAULT FALSE,
  external_effect BOOLEAN NOT NULL DEFAULT FALSE,
  external_boundary TEXT NOT NULL DEFAULT '',
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, receipt_id, ordinal),
  CONSTRAINT work_receipt_steps_receipt_fk FOREIGN KEY (workspace_id, receipt_id)
    REFERENCES fornix.work_receipts(workspace_id, id),
  CONSTRAINT work_receipt_steps_ordinal_valid CHECK (ordinal BETWEEN 0 AND 63),
  CONSTRAINT work_receipt_steps_identity_nonempty CHECK (length(step_id) > 0 AND length(name) > 0 AND length(kind) > 0 AND length(status) > 0),
  CONSTRAINT work_receipt_steps_measurements_nonnegative CHECK (duration_ms >= 0 AND attempts >= 0 AND retry_count >= 0),
  CONSTRAINT work_receipt_steps_hash_shape CHECK (
    (source_hash = '' OR source_hash ~ '^[0-9a-f]{64}$') AND
    (input_hash = '' OR input_hash ~ '^[0-9a-f]{64}$') AND
    (output_hash = '' OR output_hash ~ '^[0-9a-f]{64}$')
  ),
  CONSTRAINT work_receipt_steps_boundary_required CHECK (NOT external_effect OR length(external_boundary) > 0),
  CONSTRAINT work_receipt_steps_json_size CHECK (octet_length(reference_roles::text) <= 16384 AND octet_length(metadata::text) <= 16384)
);

CREATE INDEX IF NOT EXISTS work_receipt_steps_workspace_source_idx
  ON fornix.work_receipt_steps(workspace_id, source_kind, source_id, receipt_id, ordinal);

CREATE TABLE IF NOT EXISTS fornix.work_receipt_references (
  workspace_id TEXT NOT NULL,
  receipt_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  reference_kind TEXT NOT NULL,
  source_id TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT '',
  source_hash TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, receipt_id, ordinal),
  CONSTRAINT work_receipt_references_receipt_fk FOREIGN KEY (workspace_id, receipt_id)
    REFERENCES fornix.work_receipts(workspace_id, id),
  CONSTRAINT work_receipt_references_ordinal_valid CHECK (ordinal BETWEEN 0 AND 127),
  CONSTRAINT work_receipt_references_identity_nonempty CHECK (length(reference_kind) > 0 AND length(source_id) > 0),
  CONSTRAINT work_receipt_references_hash_shape CHECK (source_hash = '' OR source_hash ~ '^[0-9a-f]{64}$'),
  UNIQUE (workspace_id, receipt_id, reference_kind, source_id, role, source_hash)
);

CREATE INDEX IF NOT EXISTS work_receipt_references_workspace_source_idx
  ON fornix.work_receipt_references(workspace_id, reference_kind, source_id, receipt_id);

CREATE OR REPLACE FUNCTION fornix.reject_work_receipt_mutation()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix work receipt history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS work_receipts_append_only ON fornix.work_receipts;
CREATE TRIGGER work_receipts_append_only
  BEFORE UPDATE OR DELETE ON fornix.work_receipts
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_work_receipt_mutation();

DROP TRIGGER IF EXISTS work_receipt_steps_append_only ON fornix.work_receipt_steps;
CREATE TRIGGER work_receipt_steps_append_only
  BEFORE UPDATE OR DELETE ON fornix.work_receipt_steps
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_work_receipt_mutation();

DROP TRIGGER IF EXISTS work_receipt_references_append_only ON fornix.work_receipt_references;
CREATE TRIGGER work_receipt_references_append_only
  BEFORE UPDATE OR DELETE ON fornix.work_receipt_references
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_work_receipt_mutation();
