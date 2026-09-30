-- 041: independent workspace-scoped recovery ownership for uncertain effects.
-- The append-only effect transition history remains authoritative. This table
-- is only the mutable lease projection used by recovery workers.

CREATE TABLE IF NOT EXISTS fornix.operation_effect_leases (
  workspace_id TEXT NOT NULL,
  effect_id TEXT NOT NULL,
  owner_id TEXT NOT NULL,
  fence BIGINT NOT NULL,
  lease_until TIMESTAMPTZ NOT NULL,
  acquired_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  renewed_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  released_at TIMESTAMPTZ,
  PRIMARY KEY (workspace_id, effect_id),
  CONSTRAINT operation_effect_leases_effect_fk
    FOREIGN KEY (workspace_id, effect_id)
    REFERENCES fornix.operation_effects(workspace_id, effect_id),
  CONSTRAINT operation_effect_leases_identity_nonempty
    CHECK (length(owner_id) > 0 AND fence > 0)
);

CREATE INDEX IF NOT EXISTS operation_effect_leases_expiry_idx
  ON fornix.operation_effect_leases(workspace_id, lease_until, effect_id);
