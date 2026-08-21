-- Migration 018 is immutable once applied. Scope authorization idempotency to
-- the authenticated identity and credential as well as the request tuple so a
-- decision cannot be replayed across principals.
ALTER TABLE fornix.authorization_audit
  DROP CONSTRAINT IF EXISTS authorization_audit_idempotent;

ALTER TABLE fornix.authorization_audit
  ADD CONSTRAINT authorization_audit_idempotent
  UNIQUE (workspace_id, request_id, identity_id, api_key_id, permission, resource);
