-- Keep authorization decisions append-only when the same request identity is
-- evaluated after permissions, credential state, or route context changes.
-- Identical decision fingerprints still deduplicate; a prior allow is never a
-- cached grant and cannot suppress a later denial record.
ALTER TABLE fornix.authorization_audit
  DROP CONSTRAINT IF EXISTS authorization_audit_idempotent;

ALTER TABLE fornix.authorization_audit
  ADD CONSTRAINT authorization_audit_idempotent
  UNIQUE (workspace_id, request_id, identity_id, api_key_id, permission, resource, decision_hash);
