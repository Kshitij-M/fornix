-- 054: immutable, workspace-scoped bindings between specialized domain
-- records and the generic operation/effect authority. This is a relationship
-- table, not a second effect ledger; dispatch state remains in 036 tables.

CREATE TABLE IF NOT EXISTS fornix.domain_effect_links (
  workspace_id TEXT NOT NULL,
  link_id TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  operation_id TEXT NOT NULL,
  operation_hash TEXT NOT NULL,
  step_id TEXT NOT NULL,
  attempt_id TEXT NOT NULL,
  effect_id TEXT NOT NULL,
  effect_reservation_hash TEXT NOT NULL,
  domain_kind TEXT NOT NULL,
  domain_id TEXT NOT NULL,
  domain_hash TEXT NOT NULL,
  link_role TEXT NOT NULL DEFAULT 'primary',
  request_hash TEXT NOT NULL,
  result_hash TEXT NOT NULL DEFAULT '',
  boundary TEXT NOT NULL,
  effect_class TEXT NOT NULL,
  delivery_guarantee TEXT NOT NULL,
  provider_idempotency_supported BOOLEAN NOT NULL DEFAULT FALSE,
  provider_request_id TEXT NOT NULL DEFAULT '',
  verification_status TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'linked',
  operation_owner_id TEXT NOT NULL DEFAULT '',
  operation_fence BIGINT NOT NULL DEFAULT 0,
  task_owner_id TEXT NOT NULL DEFAULT '',
  task_fence BIGINT NOT NULL DEFAULT 0,
  schema_catalog_hash TEXT NOT NULL DEFAULT '',
  schema_catalog_revision TEXT NOT NULL DEFAULT '',
  credential_lease_id TEXT NOT NULL DEFAULT '',
  credential_lease_fence BIGINT NOT NULL DEFAULT 0,
  credential_revocation_epoch BIGINT NOT NULL DEFAULT 0,
  credential_source_version TEXT NOT NULL DEFAULT '',
  credential_source_expires_at TIMESTAMPTZ,
  actor JSONB NOT NULL,
  request_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  causation_id TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL DEFAULT '',
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  link_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, link_id),
  UNIQUE (workspace_id, domain_kind, domain_id, link_role, attempt_id),
  UNIQUE (workspace_id, effect_id, link_role),
  UNIQUE (workspace_id, idempotency_key),
  CONSTRAINT domain_effect_links_operation_fk FOREIGN KEY (workspace_id, operation_id) REFERENCES fornix.operations(workspace_id, id),
  CONSTRAINT domain_effect_links_effect_fk FOREIGN KEY (workspace_id, effect_id) REFERENCES fornix.operation_effects(workspace_id, effect_id),
  CONSTRAINT domain_effect_links_hash_shape CHECK (
    operation_hash ~ '^[0-9a-f]{64}$' AND effect_reservation_hash ~ '^[0-9a-f]{64}$' AND
    domain_hash ~ '^[0-9a-f]{64}$' AND request_hash ~ '^[0-9a-f]{64}$' AND
    (result_hash = '' OR result_hash ~ '^[0-9a-f]{64}$') AND link_hash ~ '^[0-9a-f]{64}$'
  ),
  CONSTRAINT domain_effect_links_identity_nonempty CHECK (
    length(link_id) > 0 AND length(operation_id) > 0 AND length(step_id) > 0 AND
    length(attempt_id) > 0 AND length(effect_id) > 0 AND length(domain_kind) > 0 AND
    length(domain_id) > 0 AND length(request_id) > 0 AND length(idempotency_key) > 0
  ),
  CONSTRAINT domain_effect_links_class_valid CHECK (effect_class IN ('reversible_write','approval_required_write','irreversible_write','external_communication')),
  CONSTRAINT domain_effect_links_delivery_valid CHECK (delivery_guarantee IN ('at_least_once','unknown')),
  CONSTRAINT domain_effect_links_verification_valid CHECK (verification_status IN ('not_required','pending','verified','failed','unknown')),
  CONSTRAINT domain_effect_links_status_valid CHECK (status IN ('linked','reconciled','recovery_required')),
  CONSTRAINT domain_effect_links_catalog_pair_valid CHECK ((schema_catalog_hash = '' AND schema_catalog_revision = '') OR (schema_catalog_hash ~ '^[0-9a-f]{64}$' AND length(schema_catalog_revision) BETWEEN 1 AND 64)),
  CONSTRAINT domain_effect_links_fence_pairs CHECK (
    (operation_owner_id = '' AND operation_fence = 0 OR operation_owner_id <> '' AND operation_fence > 0) AND
    (task_owner_id = '' AND task_fence = 0 OR task_owner_id <> '' AND task_fence > 0) AND
    (credential_lease_id = '' AND credential_lease_fence = 0 AND credential_revocation_epoch = 0 OR credential_lease_id <> '' AND credential_lease_fence > 0 AND credential_revocation_epoch > 0)
  ),
  CONSTRAINT domain_effect_links_json_bounded CHECK (
    octet_length(actor::text) <= 16384 AND octet_length(metadata::text) <= 16384
  )
);

CREATE INDEX IF NOT EXISTS domain_effect_links_operation_idx
  ON fornix.domain_effect_links(workspace_id, operation_id, created_at, link_id);
CREATE INDEX IF NOT EXISTS domain_effect_links_effect_idx
  ON fornix.domain_effect_links(workspace_id, effect_id, created_at, link_id);
CREATE INDEX IF NOT EXISTS domain_effect_links_domain_idx
  ON fornix.domain_effect_links(workspace_id, domain_kind, domain_id, created_at, link_id);

CREATE TABLE IF NOT EXISTS fornix.domain_effect_link_transitions (
  workspace_id TEXT NOT NULL,
  link_id TEXT NOT NULL,
  version BIGINT NOT NULL,
  from_status TEXT NOT NULL,
  to_status TEXT NOT NULL,
  provider_request_id TEXT NOT NULL DEFAULT '',
  result_hash TEXT NOT NULL DEFAULT '',
  failure_code TEXT NOT NULL DEFAULT '',
  idempotency_key TEXT NOT NULL,
  actor JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, link_id, version),
  UNIQUE (workspace_id, link_id, idempotency_key),
  CONSTRAINT domain_effect_link_transition_fk FOREIGN KEY (workspace_id, link_id) REFERENCES fornix.domain_effect_links(workspace_id, link_id),
  CONSTRAINT domain_effect_link_transition_hash_shape CHECK (result_hash = '' OR result_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT domain_effect_link_transition_status_valid CHECK (from_status IN ('linked','reconciled','recovery_required') AND to_status IN ('linked','reconciled','recovery_required')),
  CONSTRAINT domain_effect_link_transition_json_bounded CHECK (octet_length(actor::text) <= 16384)
);

CREATE INDEX IF NOT EXISTS domain_effect_link_transitions_order_idx
  ON fornix.domain_effect_link_transitions(workspace_id, link_id, version);

-- Migration 046 protects the tables that existed at that point. New
-- workspace-scoped tables must install the same fail-closed policy in their
-- own forward migration so later databases and upgrades have identical
-- isolation guarantees.
ALTER TABLE fornix.domain_effect_links ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.domain_effect_links;
CREATE POLICY workspace_scope_isolation ON fornix.domain_effect_links
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

ALTER TABLE fornix.domain_effect_link_transitions ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.domain_effect_link_transitions;
CREATE POLICY workspace_scope_isolation ON fornix.domain_effect_link_transitions
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));

CREATE OR REPLACE FUNCTION fornix.reject_domain_effect_link_history_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix domain effect link history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS domain_effect_links_append_only ON fornix.domain_effect_links;
CREATE TRIGGER domain_effect_links_append_only
  BEFORE UPDATE OR DELETE ON fornix.domain_effect_links
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_domain_effect_link_history_mutation();
DROP TRIGGER IF EXISTS domain_effect_link_transitions_append_only ON fornix.domain_effect_link_transitions;
CREATE TRIGGER domain_effect_link_transitions_append_only
  BEFORE UPDATE OR DELETE ON fornix.domain_effect_link_transitions
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_domain_effect_link_history_mutation();
