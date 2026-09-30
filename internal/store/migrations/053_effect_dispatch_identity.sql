-- 053: make provider-facing effect identity unique within a workspace and
-- boundary. The operation effect row remains the local reservation authority;
-- this partial index prevents two independent attempts from claiming the
-- same non-empty provider idempotency identity.

CREATE UNIQUE INDEX IF NOT EXISTS operation_effects_provider_identity_uq
  ON fornix.operation_effects(workspace_id, boundary, idempotency_key)
  WHERE idempotency_key <> '';
