-- 031: workspace-scoped declarative validation policy packs.
-- A policy version's body and rules are immutable. Lifecycle state and default
-- selection are operational views backed by append-only transition/audit rows.
-- Policy packs contain validator references and limits only; they never carry
-- executable SQL, shell, prompts, credentials, or external service URLs.

CREATE TABLE IF NOT EXISTS fornix.validation_policies (
  workspace_id TEXT NOT NULL,
  policy_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, policy_id),
  CONSTRAINT validation_policies_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT validation_policies_id_nonempty CHECK (length(policy_id) > 0 AND length(policy_id) <= 128)
);

CREATE TABLE IF NOT EXISTS fornix.validation_policy_versions (
  workspace_id TEXT NOT NULL,
  policy_id TEXT NOT NULL,
  version TEXT NOT NULL,
  policy_hash TEXT NOT NULL,
  pack JSONB NOT NULL,
  status TEXT NOT NULL DEFAULT 'draft',
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  activated_at TIMESTAMPTZ,
  retired_at TIMESTAMPTZ,
  PRIMARY KEY (workspace_id, policy_id, version),
  CONSTRAINT validation_policy_versions_policy_fk FOREIGN KEY (workspace_id, policy_id)
    REFERENCES fornix.validation_policies(workspace_id, policy_id),
  CONSTRAINT validation_policy_versions_version_nonempty CHECK (length(version) > 0 AND length(version) <= 64),
  CONSTRAINT validation_policy_versions_hash_shape CHECK (policy_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT validation_policy_versions_status_valid CHECK (status IN ('draft','active','retired')),
  CONSTRAINT validation_policy_versions_json_bounded CHECK (octet_length(pack::text) <= 65536 AND octet_length(actor::text) <= 16384),
  UNIQUE (workspace_id, policy_id, version, policy_hash)
);

CREATE INDEX IF NOT EXISTS validation_policy_versions_workspace_status_idx
  ON fornix.validation_policy_versions(workspace_id, status, updated_at DESC, policy_id, version);
CREATE UNIQUE INDEX IF NOT EXISTS validation_policy_versions_one_active_idx
  ON fornix.validation_policy_versions(workspace_id, policy_id)
  WHERE status = 'active';

CREATE OR REPLACE FUNCTION fornix.reject_validation_policy_body_mutation()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF NEW.workspace_id <> OLD.workspace_id
     OR NEW.policy_id <> OLD.policy_id
     OR NEW.version <> OLD.version
     OR NEW.policy_hash <> OLD.policy_hash
     OR NEW.pack <> OLD.pack
     OR NEW.actor <> OLD.actor
     OR NEW.created_at <> OLD.created_at THEN
    RAISE EXCEPTION 'fornix validation policy version body is immutable';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS validation_policy_versions_body_immutable ON fornix.validation_policy_versions;
CREATE TRIGGER validation_policy_versions_body_immutable
  BEFORE UPDATE ON fornix.validation_policy_versions
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_validation_policy_body_mutation();

CREATE TABLE IF NOT EXISTS fornix.validation_policy_rules (
  workspace_id TEXT NOT NULL,
  policy_id TEXT NOT NULL,
  version TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  validator_id TEXT NOT NULL,
  validator_version TEXT NOT NULL,
  required BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, policy_id, version, ordinal),
  CONSTRAINT validation_policy_rules_version_fk FOREIGN KEY (workspace_id, policy_id, version)
    REFERENCES fornix.validation_policy_versions(workspace_id, policy_id, version),
  CONSTRAINT validation_policy_rules_ordinal_valid CHECK (ordinal BETWEEN 0 AND 63),
  CONSTRAINT validation_policy_rules_identity_nonempty CHECK (length(validator_id) > 0 AND length(validator_id) <= 128 AND length(validator_version) > 0 AND length(validator_version) <= 64),
  UNIQUE (workspace_id, policy_id, version, validator_id, validator_version)
);

CREATE TABLE IF NOT EXISTS fornix.validation_policy_defaults (
  workspace_id TEXT PRIMARY KEY,
  policy_id TEXT NOT NULL,
  version TEXT NOT NULL,
  policy_hash TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT validation_policy_defaults_version_fk FOREIGN KEY (workspace_id, policy_id, version, policy_hash)
    REFERENCES fornix.validation_policy_versions(workspace_id, policy_id, version, policy_hash),
  CONSTRAINT validation_policy_defaults_hash_shape CHECK (policy_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT validation_policy_defaults_json_bounded CHECK (octet_length(actor::text) <= 16384)
);

CREATE TABLE IF NOT EXISTS fornix.validation_policy_transitions (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  policy_id TEXT NOT NULL,
  version TEXT NOT NULL,
  from_status TEXT NOT NULL DEFAULT '',
  to_status TEXT NOT NULL,
  operation TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL DEFAULT '',
  reason TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT validation_policy_transitions_version_fk FOREIGN KEY (workspace_id, policy_id, version)
    REFERENCES fornix.validation_policy_versions(workspace_id, policy_id, version),
  CONSTRAINT validation_policy_transitions_identity_nonempty CHECK (length(workspace_id) > 0 AND length(policy_id) > 0 AND length(version) > 0 AND length(to_status) > 0 AND length(operation) > 0 AND length(request_id) > 0),
  CONSTRAINT validation_policy_transitions_json_bounded CHECK (octet_length(actor::text) <= 16384 AND octet_length(reason) <= 8192)
);

CREATE INDEX IF NOT EXISTS validation_policy_transitions_lookup_idx
  ON fornix.validation_policy_transitions(workspace_id, policy_id, version, id);

CREATE TABLE IF NOT EXISTS fornix.validation_policy_audit (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  policy_id TEXT NOT NULL,
  version TEXT NOT NULL,
  policy_hash TEXT NOT NULL DEFAULT '',
  operation TEXT NOT NULL,
  from_status TEXT NOT NULL DEFAULT '',
  to_status TEXT NOT NULL DEFAULT '',
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL DEFAULT '',
  allowed BOOLEAN NOT NULL,
  reason TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT validation_policy_audit_version_fk FOREIGN KEY (workspace_id, policy_id, version)
    REFERENCES fornix.validation_policy_versions(workspace_id, policy_id, version),
  CONSTRAINT validation_policy_audit_hash_shape CHECK (policy_hash = '' OR policy_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT validation_policy_audit_identity_nonempty CHECK (length(workspace_id) > 0 AND length(operation) > 0 AND length(request_id) > 0),
  CONSTRAINT validation_policy_audit_json_bounded CHECK (octet_length(actor::text) <= 16384 AND octet_length(reason) <= 8192)
);

CREATE INDEX IF NOT EXISTS validation_policy_audit_workspace_time_idx
  ON fornix.validation_policy_audit(workspace_id, created_at DESC, id);

CREATE TABLE IF NOT EXISTS fornix.validation_policy_idempotency (
  workspace_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  operation TEXT NOT NULL,
  policy_id TEXT NOT NULL,
  version TEXT NOT NULL,
  policy_hash TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, idempotency_key),
  CONSTRAINT validation_policy_idempotency_hash_shape CHECK (request_hash ~ '^[0-9a-f]{64}$' AND (policy_hash = '' OR policy_hash ~ '^[0-9a-f]{64}$')),
  CONSTRAINT validation_policy_idempotency_identity_nonempty CHECK (length(operation) > 0 AND length(policy_id) > 0 AND length(version) > 0)
);

CREATE INDEX IF NOT EXISTS validation_policy_idempotency_policy_idx
  ON fornix.validation_policy_idempotency(workspace_id, policy_id, version);

CREATE OR REPLACE FUNCTION fornix.reject_validation_policy_append_only()
RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix validation policy history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS validation_policy_rules_append_only ON fornix.validation_policy_rules;
CREATE TRIGGER validation_policy_rules_append_only
  BEFORE UPDATE OR DELETE ON fornix.validation_policy_rules
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_validation_policy_append_only();
DROP TRIGGER IF EXISTS validation_policy_transitions_append_only ON fornix.validation_policy_transitions;
CREATE TRIGGER validation_policy_transitions_append_only
  BEFORE UPDATE OR DELETE ON fornix.validation_policy_transitions
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_validation_policy_append_only();
DROP TRIGGER IF EXISTS validation_policy_audit_append_only ON fornix.validation_policy_audit;
CREATE TRIGGER validation_policy_audit_append_only
  BEFORE UPDATE OR DELETE ON fornix.validation_policy_audit
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_validation_policy_append_only();
DROP TRIGGER IF EXISTS validation_policy_idempotency_append_only ON fornix.validation_policy_idempotency;
CREATE TRIGGER validation_policy_idempotency_append_only
  BEFORE UPDATE OR DELETE ON fornix.validation_policy_idempotency
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_validation_policy_append_only();

-- Policy pins are nullable so all existing pre-policy records remain valid.
-- New policy-bearing mutations write all three fields together.
ALTER TABLE fornix.change_proposals ADD COLUMN IF NOT EXISTS policy_id TEXT;
ALTER TABLE fornix.change_proposals ADD COLUMN IF NOT EXISTS policy_version TEXT;
ALTER TABLE fornix.change_proposals ADD COLUMN IF NOT EXISTS policy_hash TEXT;
ALTER TABLE fornix.change_applications ADD COLUMN IF NOT EXISTS policy_id TEXT;
ALTER TABLE fornix.change_applications ADD COLUMN IF NOT EXISTS policy_version TEXT;
ALTER TABLE fornix.change_applications ADD COLUMN IF NOT EXISTS policy_hash TEXT;
ALTER TABLE fornix.change_approvals ADD COLUMN IF NOT EXISTS policy_id TEXT;
ALTER TABLE fornix.change_approvals ADD COLUMN IF NOT EXISTS policy_version TEXT;
ALTER TABLE fornix.change_approvals ADD COLUMN IF NOT EXISTS policy_hash TEXT;
ALTER TABLE fornix.validation_runs ADD COLUMN IF NOT EXISTS policy_id TEXT;
ALTER TABLE fornix.validation_runs ADD COLUMN IF NOT EXISTS policy_version TEXT;
ALTER TABLE fornix.validation_runs ADD COLUMN IF NOT EXISTS policy_hash TEXT;
ALTER TABLE fornix.reindex_handoffs ADD COLUMN IF NOT EXISTS policy_id TEXT;
ALTER TABLE fornix.reindex_handoffs ADD COLUMN IF NOT EXISTS policy_version TEXT;
ALTER TABLE fornix.reindex_handoffs ADD COLUMN IF NOT EXISTS policy_hash TEXT;
ALTER TABLE fornix.work_receipts ADD COLUMN IF NOT EXISTS policy_id TEXT;
ALTER TABLE fornix.work_receipts ADD COLUMN IF NOT EXISTS policy_version TEXT;
ALTER TABLE fornix.work_receipts ADD COLUMN IF NOT EXISTS policy_hash TEXT;
ALTER TABLE fornix.run_observations ADD COLUMN IF NOT EXISTS policy_id TEXT;
ALTER TABLE fornix.run_observations ADD COLUMN IF NOT EXISTS policy_version TEXT;
ALTER TABLE fornix.run_observations ADD COLUMN IF NOT EXISTS policy_hash TEXT;
ALTER TABLE fornix.trace_spans ADD COLUMN IF NOT EXISTS policy_id TEXT;
ALTER TABLE fornix.trace_spans ADD COLUMN IF NOT EXISTS policy_version TEXT;
ALTER TABLE fornix.trace_spans ADD COLUMN IF NOT EXISTS policy_hash TEXT;
ALTER TABLE fornix.cost_ledger ADD COLUMN IF NOT EXISTS policy_id TEXT;
ALTER TABLE fornix.cost_ledger ADD COLUMN IF NOT EXISTS policy_version TEXT;
ALTER TABLE fornix.cost_ledger ADD COLUMN IF NOT EXISTS policy_hash TEXT;
ALTER TABLE fornix.metric_samples ADD COLUMN IF NOT EXISTS policy_id TEXT;
ALTER TABLE fornix.metric_samples ADD COLUMN IF NOT EXISTS policy_version TEXT;
ALTER TABLE fornix.metric_samples ADD COLUMN IF NOT EXISTS policy_hash TEXT;
ALTER TABLE fornix.control_events ADD COLUMN IF NOT EXISTS policy_id TEXT;
ALTER TABLE fornix.control_events ADD COLUMN IF NOT EXISTS policy_version TEXT;
ALTER TABLE fornix.control_events ADD COLUMN IF NOT EXISTS policy_hash TEXT;

DO $$
DECLARE
  table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY['change_proposals','change_applications','change_approvals','validation_runs','reindex_handoffs','work_receipts','run_observations','trace_spans','cost_ledger','metric_samples','control_events'] LOOP
    BEGIN
      EXECUTE format('ALTER TABLE fornix.%I ADD CONSTRAINT %I_policy_columns_all_or_none CHECK ((policy_id IS NULL AND policy_version IS NULL AND policy_hash IS NULL) OR (policy_id IS NOT NULL AND policy_version IS NOT NULL AND policy_hash IS NOT NULL))', table_name, table_name);
    EXCEPTION WHEN duplicate_object THEN
      NULL;
    END;
  END LOOP;
END $$;

DO $$
DECLARE
  table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY['change_proposals','change_applications','change_approvals','validation_runs','reindex_handoffs','work_receipts','run_observations','trace_spans','cost_ledger','metric_samples','control_events'] LOOP
    EXECUTE format('ALTER TABLE fornix.%I ADD CONSTRAINT %I FOREIGN KEY (workspace_id, policy_id, policy_version, policy_hash) REFERENCES fornix.validation_policy_versions(workspace_id, policy_id, version, policy_hash)', table_name, table_name || '_policy_fk');
  END LOOP;
EXCEPTION WHEN duplicate_object THEN
  NULL;
END $$;

CREATE INDEX IF NOT EXISTS validation_policy_pins_idx
  ON fornix.change_proposals(workspace_id, policy_id, policy_version, policy_hash)
  WHERE policy_id IS NOT NULL;
