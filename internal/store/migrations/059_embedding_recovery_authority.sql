-- Migration 059: bind embedding reconciliation audit to the original local
-- operation/effect/link authority and its recovery fence. These fields are
-- references and hashes only; provider payloads and credentials remain out of
-- the audit table.

ALTER TABLE fornix.embedding_call_reconciliations
  ADD COLUMN IF NOT EXISTS operation_id text,
  ADD COLUMN IF NOT EXISTS effect_id text,
  ADD COLUMN IF NOT EXISTS link_id text,
  ADD COLUMN IF NOT EXISTS request_hash text,
  ADD COLUMN IF NOT EXISTS recovery_owner_id text,
  ADD COLUMN IF NOT EXISTS recovery_fence bigint,
  ADD COLUMN IF NOT EXISTS expected_effect_version bigint,
  ADD COLUMN IF NOT EXISTS expected_link_version bigint,
  ADD COLUMN IF NOT EXISTS causation_id text,
  ADD COLUMN IF NOT EXISTS correlation_id text;

ALTER TABLE fornix.embedding_call_reconciliations
  ADD CONSTRAINT embedding_reconciliation_recovery_fence_nonnegative
    CHECK (recovery_fence IS NULL OR recovery_fence > 0),
  ADD CONSTRAINT embedding_reconciliation_expected_effect_version_positive
    CHECK (expected_effect_version IS NULL OR expected_effect_version > 0),
  ADD CONSTRAINT embedding_reconciliation_expected_link_version_positive
    CHECK (expected_link_version IS NULL OR expected_link_version > 0);

CREATE INDEX IF NOT EXISTS embedding_reconciliation_authority_idx
  ON fornix.embedding_call_reconciliations(workspace_id, operation_id, effect_id, link_id, created_at, id);

CREATE INDEX IF NOT EXISTS embedding_reconciliation_request_hash_idx
  ON fornix.embedding_call_reconciliations(workspace_id, request_id, request_hash, created_at);
