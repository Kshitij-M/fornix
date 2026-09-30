-- 070: bind the exact redacted egress and network-boundary envelope to
-- generic effect reservations and authority links. Historical rows retain
-- empty compatibility values; new strict production dispatch must populate
-- the complete envelope for network communication effects.

ALTER TABLE fornix.operation_effects
  ADD COLUMN IF NOT EXISTS egress_policy_hash TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS destination_policy_hash TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS network_boundary TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS network_boundary_hash TEXT NOT NULL DEFAULT '';

ALTER TABLE fornix.operation_effects
  DROP CONSTRAINT IF EXISTS operation_effects_external_boundary_valid;

ALTER TABLE fornix.operation_effects
  ADD CONSTRAINT operation_effects_external_boundary_valid CHECK (
    (egress_policy_hash = '' AND destination_policy_hash = '' AND network_boundary = '' AND network_boundary_hash = '') OR
    (egress_policy_hash ~ '^[0-9a-f]{64}$' AND destination_policy_hash ~ '^[0-9a-f]{64}$' AND network_boundary IN ('controlled_transport','deployment_attested') AND network_boundary_hash ~ '^[0-9a-f]{64}$')
  );

ALTER TABLE fornix.operation_authority_links
  ADD COLUMN IF NOT EXISTS egress_policy_hash TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS destination_policy_hash TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS network_boundary TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS network_boundary_hash TEXT NOT NULL DEFAULT '';

ALTER TABLE fornix.operation_authority_links
  DROP CONSTRAINT IF EXISTS operation_authority_links_external_boundary_valid;

ALTER TABLE fornix.operation_authority_links
  ADD CONSTRAINT operation_authority_links_external_boundary_valid CHECK (
    (egress_policy_hash = '' AND destination_policy_hash = '' AND network_boundary = '' AND network_boundary_hash = '') OR
    (egress_policy_hash ~ '^[0-9a-f]{64}$' AND destination_policy_hash ~ '^[0-9a-f]{64}$' AND network_boundary IN ('controlled_transport','deployment_attested') AND network_boundary_hash ~ '^[0-9a-f]{64}$')
  );

CREATE INDEX IF NOT EXISTS operation_effects_external_boundary_idx
  ON fornix.operation_effects(workspace_id, egress_policy_hash, destination_policy_hash)
  WHERE egress_policy_hash <> '';

CREATE INDEX IF NOT EXISTS operation_authority_links_external_boundary_idx
  ON fornix.operation_authority_links(workspace_id, egress_policy_hash, destination_policy_hash)
  WHERE egress_policy_hash <> '';

ALTER TABLE fornix.domain_effect_links
  ADD COLUMN IF NOT EXISTS egress_policy_hash TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS destination_policy_hash TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS network_boundary TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS network_boundary_hash TEXT NOT NULL DEFAULT '';

ALTER TABLE fornix.domain_effect_links
  DROP CONSTRAINT IF EXISTS domain_effect_links_external_boundary_valid;

ALTER TABLE fornix.domain_effect_links
  ADD CONSTRAINT domain_effect_links_external_boundary_valid CHECK (
    (egress_policy_hash = '' AND destination_policy_hash = '' AND network_boundary = '' AND network_boundary_hash = '') OR
    (egress_policy_hash ~ '^[0-9a-f]{64}$' AND destination_policy_hash ~ '^[0-9a-f]{64}$' AND network_boundary IN ('controlled_transport','deployment_attested') AND network_boundary_hash ~ '^[0-9a-f]{64}$')
  );

CREATE INDEX IF NOT EXISTS domain_effect_links_external_boundary_idx
  ON fornix.domain_effect_links(workspace_id, egress_policy_hash, destination_policy_hash)
  WHERE egress_policy_hash <> '';

ALTER TABLE fornix.workspace_federation_poll_attempts
  ADD COLUMN IF NOT EXISTS egress_policy_hash TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS destination_policy_hash TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS network_boundary TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS network_boundary_hash TEXT NOT NULL DEFAULT '';

ALTER TABLE fornix.workspace_federation_poll_attempts
  DROP CONSTRAINT IF EXISTS workspace_federation_poll_external_boundary_valid;

ALTER TABLE fornix.workspace_federation_poll_attempts
  ADD CONSTRAINT workspace_federation_poll_external_boundary_valid CHECK (
    (egress_policy_hash = '' AND destination_policy_hash = '' AND network_boundary = '' AND network_boundary_hash = '') OR
    (egress_policy_hash ~ '^[0-9a-f]{64}$' AND destination_policy_hash ~ '^[0-9a-f]{64}$' AND network_boundary IN ('controlled_transport','deployment_attested') AND network_boundary_hash ~ '^[0-9a-f]{64}$')
  );

CREATE INDEX IF NOT EXISTS workspace_federation_poll_external_boundary_idx
  ON fornix.workspace_federation_poll_attempts(workspace_id, egress_policy_hash, destination_policy_hash)
  WHERE egress_policy_hash <> '';
