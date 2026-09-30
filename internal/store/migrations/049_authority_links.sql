-- 049: append-only cross-authority operation linkage.
--
-- This table is a bounded, hash-only join. The operation, admission,
-- credential, trust, effect, evidence, artifact, and receipt tables remain
-- authoritative for their own state and lifecycle.

CREATE TABLE IF NOT EXISTS fornix.operation_authority_links (
  workspace_id TEXT NOT NULL,
  link_id TEXT NOT NULL,
  stage TEXT NOT NULL,
  operation_id TEXT NOT NULL,
  operation_hash TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  admission_decision_id TEXT NOT NULL DEFAULT '',
  admission_input_hash TEXT NOT NULL DEFAULT '',
  trust_policy_hash TEXT NOT NULL DEFAULT '',
  trust_policy_revision TEXT NOT NULL DEFAULT '',
  connector_hash TEXT NOT NULL DEFAULT '',
  capability_hash TEXT NOT NULL DEFAULT '',
  policy_id TEXT NOT NULL DEFAULT '',
  policy_version TEXT NOT NULL DEFAULT '',
  policy_hash TEXT NOT NULL DEFAULT '',
  credential_lease_id TEXT NOT NULL DEFAULT '',
  credential_lease_fence BIGINT NOT NULL DEFAULT 0,
  credential_revocation_epoch BIGINT NOT NULL DEFAULT 0,
  operation_owner_id TEXT NOT NULL DEFAULT '',
  operation_fence BIGINT NOT NULL DEFAULT 0,
  task_owner_id TEXT NOT NULL DEFAULT '',
  task_fence BIGINT NOT NULL DEFAULT 0,
  effect_reservation_hash TEXT NOT NULL DEFAULT '',
  effect_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  result_id TEXT NOT NULL DEFAULT '',
  result_hash TEXT NOT NULL DEFAULT '',
  receipt_id TEXT NOT NULL DEFAULT '',
  receipt_hash TEXT NOT NULL DEFAULT '',
  evidence_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  artifact_refs JSONB NOT NULL DEFAULT '[]'::jsonb,
  actor JSONB NOT NULL,
  request_id TEXT NOT NULL,
  causation_id TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL DEFAULT '',
  link_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, link_id),
  CONSTRAINT operation_authority_links_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT operation_authority_links_id_nonempty CHECK (length(link_id) BETWEEN 1 AND 128),
  CONSTRAINT operation_authority_links_stage_valid CHECK (stage IN ('admission','result','receipt')),
  CONSTRAINT operation_authority_links_operation_nonempty CHECK (length(operation_id) BETWEEN 1 AND 128),
  CONSTRAINT operation_authority_links_identity_nonempty CHECK (length(idempotency_key) BETWEEN 1 AND 256 AND length(request_id) BETWEEN 1 AND 256),
  CONSTRAINT operation_authority_links_hash_shape CHECK (
    operation_hash ~ '^[0-9a-f]{64}$' AND link_hash ~ '^[0-9a-f]{64}$' AND
    (admission_input_hash = '' OR admission_input_hash ~ '^[0-9a-f]{64}$') AND
    (trust_policy_hash = '' OR trust_policy_hash ~ '^[0-9a-f]{64}$') AND
    (connector_hash = '' OR connector_hash ~ '^[0-9a-f]{64}$') AND
    (capability_hash = '' OR capability_hash ~ '^[0-9a-f]{64}$') AND
    (policy_hash = '' OR policy_hash ~ '^[0-9a-f]{64}$') AND
    (effect_reservation_hash = '' OR effect_reservation_hash ~ '^[0-9a-f]{64}$') AND
    (result_hash = '' OR result_hash ~ '^[0-9a-f]{64}$') AND
    (receipt_hash = '' OR receipt_hash ~ '^[0-9a-f]{64}$')
  ),
  CONSTRAINT operation_authority_links_fences_valid CHECK (
    (credential_lease_id = '' AND credential_lease_fence = 0 AND credential_revocation_epoch = 0) OR
    (credential_lease_id <> '' AND credential_lease_fence > 0 AND credential_revocation_epoch > 0)
  ),
  CONSTRAINT operation_authority_links_operation_fence_valid CHECK (
    (operation_owner_id = '' AND operation_fence = 0) OR
    (operation_owner_id <> '' AND operation_fence > 0)
  ),
  CONSTRAINT operation_authority_links_task_fence_valid CHECK (
    (task_owner_id = '' AND task_fence = 0) OR
    (task_owner_id <> '' AND task_fence > 0)
  ),
  CONSTRAINT operation_authority_links_result_pair_valid CHECK ((result_id = '') = (result_hash = '')),
  CONSTRAINT operation_authority_links_receipt_pair_valid CHECK ((receipt_id = '') = (receipt_hash = '')),
  CONSTRAINT operation_authority_links_json_bounded CHECK (
    jsonb_typeof(effect_ids) = 'array' AND jsonb_typeof(evidence_refs) = 'array' AND jsonb_typeof(artifact_refs) = 'array' AND
    jsonb_array_length(effect_ids) <= 128 AND jsonb_array_length(evidence_refs) <= 128 AND jsonb_array_length(artifact_refs) <= 128 AND
    octet_length(effect_ids::text) <= 16384 AND octet_length(evidence_refs::text) <= 65536 AND octet_length(artifact_refs::text) <= 65536 AND
    octet_length(actor::text) <= 16384
  ),
  CONSTRAINT operation_authority_links_operation_fk FOREIGN KEY (workspace_id, operation_id) REFERENCES fornix.operations(workspace_id, id)
);

CREATE UNIQUE INDEX IF NOT EXISTS operation_authority_links_delivery_uq
  ON fornix.operation_authority_links(workspace_id, operation_id, stage, idempotency_key);
CREATE INDEX IF NOT EXISTS operation_authority_links_operation_idx
  ON fornix.operation_authority_links(workspace_id, operation_id, created_at, link_id);
CREATE INDEX IF NOT EXISTS operation_authority_links_hash_idx
  ON fornix.operation_authority_links(workspace_id, link_hash);

CREATE OR REPLACE FUNCTION fornix.reject_operation_authority_link_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix operation authority links are append-only';
END;
$$;

DROP TRIGGER IF EXISTS operation_authority_links_append_only ON fornix.operation_authority_links;
CREATE TRIGGER operation_authority_links_append_only
  BEFORE UPDATE OR DELETE ON fornix.operation_authority_links
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_operation_authority_link_mutation();

ALTER TABLE fornix.operation_authority_links ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.operation_authority_links;
CREATE POLICY workspace_scope_isolation ON fornix.operation_authority_links
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
