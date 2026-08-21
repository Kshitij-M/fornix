CREATE TABLE IF NOT EXISTS fornix.identities (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  subject TEXT NOT NULL,
  kind TEXT NOT NULL DEFAULT 'user',
  display_name TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL DEFAULT 'active',
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT identities_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT identities_subject_nonempty CHECK (length(subject) > 0),
  CONSTRAINT identities_status_valid CHECK (status IN ('active','revoked','disabled')),
  CONSTRAINT identities_workspace_subject_unique UNIQUE (workspace_id, subject)
);

CREATE INDEX IF NOT EXISTS identities_workspace_idx
  ON fornix.identities(workspace_id, status, subject);

CREATE TABLE IF NOT EXISTS fornix.roles (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  name TEXT NOT NULL,
  permissions JSONB NOT NULL DEFAULT '[]'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT roles_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT roles_name_nonempty CHECK (length(name) > 0),
  CONSTRAINT roles_permissions_array CHECK (jsonb_typeof(permissions) = 'array'),
  CONSTRAINT roles_workspace_name_unique UNIQUE (workspace_id, name)
);

CREATE TABLE IF NOT EXISTS fornix.identity_role_bindings (
  workspace_id TEXT NOT NULL,
  identity_id TEXT NOT NULL REFERENCES fornix.identities(id) ON DELETE CASCADE,
  role_id TEXT NOT NULL REFERENCES fornix.roles(id) ON DELETE CASCADE,
  granted_by TEXT NOT NULL DEFAULT '',
  expires_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, identity_id, role_id),
  CONSTRAINT identity_role_bindings_workspace_nonempty CHECK (length(workspace_id) > 0)
);

CREATE INDEX IF NOT EXISTS identity_role_bindings_lookup_idx
  ON fornix.identity_role_bindings(workspace_id, identity_id, expires_at);

CREATE TABLE IF NOT EXISTS fornix.api_keys (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  identity_id TEXT NOT NULL REFERENCES fornix.identities(id) ON DELETE CASCADE,
  prefix TEXT NOT NULL,
  token_hash BYTEA NOT NULL,
  status TEXT NOT NULL DEFAULT 'active',
  expires_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ,
  rotated_from TEXT REFERENCES fornix.api_keys(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  last_used_at TIMESTAMPTZ,
  CONSTRAINT api_keys_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT api_keys_prefix_nonempty CHECK (length(prefix) > 0),
  CONSTRAINT api_keys_hash_length CHECK (octet_length(token_hash) = 32),
  CONSTRAINT api_keys_status_valid CHECK (status IN ('active','revoked','expired')),
  CONSTRAINT api_keys_workspace_id_unique UNIQUE (workspace_id, id),
  CONSTRAINT api_keys_prefix_unique UNIQUE (prefix)
);

CREATE INDEX IF NOT EXISTS api_keys_auth_lookup_idx
  ON fornix.api_keys(id, workspace_id, status, expires_at);

CREATE TABLE IF NOT EXISTS fornix.credential_references (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  provider TEXT NOT NULL,
  name TEXT NOT NULL,
  reference TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT 1,
  status TEXT NOT NULL DEFAULT 'active',
  rotated_from TEXT REFERENCES fornix.credential_references(id) ON DELETE SET NULL,
  expires_at TIMESTAMPTZ,
  revoked_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT credential_references_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT credential_references_provider_nonempty CHECK (length(provider) > 0),
  CONSTRAINT credential_references_name_nonempty CHECK (length(name) > 0),
  CONSTRAINT credential_references_ref_nonempty CHECK (length(reference) > 0),
  CONSTRAINT credential_references_version_positive CHECK (version > 0),
  CONSTRAINT credential_references_status_valid CHECK (status IN ('active','revoked','rotated')),
  CONSTRAINT credential_references_unique_version UNIQUE (workspace_id, provider, name, version)
);

CREATE INDEX IF NOT EXISTS credential_references_lookup_idx
  ON fornix.credential_references(workspace_id, provider, name, status, version DESC);

CREATE TABLE IF NOT EXISTS fornix.authorization_audit (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  identity_id TEXT NOT NULL DEFAULT '',
  api_key_id TEXT NOT NULL DEFAULT '',
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  permission TEXT NOT NULL,
  resource TEXT NOT NULL DEFAULT '',
  decision BOOLEAN NOT NULL,
  reason TEXT NOT NULL,
  method TEXT NOT NULL DEFAULT '',
  path TEXT NOT NULL DEFAULT '',
  decision_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  CONSTRAINT authorization_audit_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT authorization_audit_request_nonempty CHECK (length(request_id) > 0),
  CONSTRAINT authorization_audit_permission_nonempty CHECK (length(permission) > 0),
  CONSTRAINT authorization_audit_hash_nonempty CHECK (length(decision_hash) > 0),
  CONSTRAINT authorization_audit_idempotent UNIQUE (workspace_id, request_id, permission, resource)
);

CREATE INDEX IF NOT EXISTS authorization_audit_workspace_time_idx
  ON fornix.authorization_audit(workspace_id, created_at DESC, id DESC);

CREATE OR REPLACE FUNCTION fornix.reject_authorization_audit_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'authorization_audit is append-only';
END;
$$;

DROP TRIGGER IF EXISTS authorization_audit_append_only ON fornix.authorization_audit;
CREATE TRIGGER authorization_audit_append_only
  BEFORE UPDATE OR DELETE ON fornix.authorization_audit
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_authorization_audit_mutation();
