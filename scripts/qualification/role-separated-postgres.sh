#!/bin/sh
set -eu

# Configure an already-migrated database for runtime role separation. Role
# creation and password rotation stay outside this script so secrets can be
# provisioned by the deployment system rather than passed on a command line.
# The script never prints DSNs, role credentials, or table payloads.

admin_dsn=${FORNIX_RLS_ADMIN_DSN:-}
app_dsn=${FORNIX_RLS_TEST_DSN:-}
app_role=${FORNIX_RLS_APP_ROLE:-}
migration_role=${FORNIX_RLS_MIGRATION_ROLE:-}

if [ -z "$admin_dsn" ] || [ -z "$app_dsn" ] || [ -z "$app_role" ] || [ -z "$migration_role" ]; then
	printf '%s\n' 'role-separated PostgreSQL qualification: FORNIX_RLS_ADMIN_DSN, FORNIX_RLS_TEST_DSN, FORNIX_RLS_APP_ROLE, and FORNIX_RLS_MIGRATION_ROLE are required' >&2
	exit 1
fi

case "$app_role" in
	*[!A-Za-z0-9_]*|'') printf '%s\n' 'FORNIX_RLS_APP_ROLE must be a simple PostgreSQL identifier' >&2; exit 1 ;;
esac
case "$migration_role" in
	*[!A-Za-z0-9_]*|'') printf '%s\n' 'FORNIX_RLS_MIGRATION_ROLE must be a simple PostgreSQL identifier' >&2; exit 1 ;;
esac
if [ "$app_role" = "$migration_role" ]; then
	printf '%s\n' 'runtime and migration roles must be different' >&2
	exit 1
fi

psql "$admin_dsn" -v ON_ERROR_STOP=1 -v app_role="$app_role" -v migration_role="$migration_role" <<'SQL'
SELECT set_config('fornix.qualification_app_role', :'app_role', false),
       set_config('fornix.qualification_migration_role', :'migration_role', false);

DO $do$
DECLARE
  app_role_name TEXT := current_setting('fornix.qualification_app_role');
  migration_role_name TEXT := current_setting('fornix.qualification_migration_role');
  table_name TEXT;
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = app_role_name) THEN
    RAISE EXCEPTION 'runtime role does not exist: %', app_role_name;
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = migration_role_name) THEN
    RAISE EXCEPTION 'migration role does not exist: %', migration_role_name;
  END IF;
  EXECUTE format('ALTER ROLE %I NOSUPERUSER NOBYPASSRLS', app_role_name);
  EXECUTE format('GRANT CONNECT ON DATABASE %I TO %I', current_database(), app_role_name);
  EXECUTE format('REVOKE CREATE ON SCHEMA fornix FROM %I', app_role_name);
  EXECUTE format('GRANT USAGE ON SCHEMA fornix TO %I', app_role_name);
  EXECUTE format('GRANT USAGE ON SCHEMA fornix TO %I', migration_role_name);

  FOR table_name IN
    SELECT c.relname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'fornix'
      AND c.relkind IN ('r', 'p')
      AND c.relname <> 'schema_migrations'
    ORDER BY c.relname
  LOOP
    EXECUTE format('ALTER TABLE fornix.%I OWNER TO %I', table_name, migration_role_name);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON fornix.%I TO %I', table_name, app_role_name);
  END LOOP;

  -- The migration catalog is authority for the migration role only. Granting
  -- it to the runtime role would let application code inspect or mutate
  -- deployment state and would make least-privilege qualification meaningless.
  EXECUTE format('REVOKE ALL ON TABLE fornix.schema_migrations FROM %I', app_role_name);

  FOR table_name IN
    SELECT c.relname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'fornix' AND c.relkind = 'S'
    ORDER BY c.relname
  LOOP
    EXECUTE format('ALTER SEQUENCE fornix.%I OWNER TO %I', table_name, migration_role_name);
    EXECUTE format('GRANT USAGE, SELECT, UPDATE ON SEQUENCE fornix.%I TO %I', table_name, app_role_name);
  END LOOP;

  EXECUTE format('GRANT EXECUTE ON FUNCTION fornix.set_workspace_context(TEXT) TO %I', app_role_name);
  EXECUTE format('ALTER FUNCTION fornix.authenticate_api_key(TEXT) OWNER TO %I', migration_role_name);
  EXECUTE format('REVOKE ALL ON FUNCTION fornix.authenticate_api_key(TEXT) FROM PUBLIC');
  EXECUTE format('GRANT EXECUTE ON FUNCTION fornix.authenticate_api_key(TEXT) TO %I', app_role_name);

  IF EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'fornix'
      AND c.relkind IN ('r', 'p')
      AND EXISTS (
        SELECT 1 FROM pg_attribute a
        WHERE a.attrelid = c.oid AND a.attname IN ('workspace_id', 'audit_workspace_id') AND NOT a.attisdropped
      )
      AND NOT c.relrowsecurity
  ) THEN
    RAISE EXCEPTION 'one or more workspace-scoped tables do not have RLS enabled';
  END IF;
END
$do$;

SELECT CASE
  WHEN r.rolsuper OR r.rolbypassrls THEN
    pg_catalog.format('runtime role %I is still privileged', r.rolname)
  ELSE
    pg_catalog.format('runtime role %I is NOBYPASSRLS and non-superuser', r.rolname)
END AS qualification
FROM pg_roles r
WHERE r.rolname = :'app_role';
SQL

printf '%s\n' 'role-separated PostgreSQL qualification passed: runtime ownership, grants, RLS coverage, and NOBYPASSRLS verified'

# Verify the runtime role can authenticate without direct cross-workspace reads.
# The fixture uses a synthetic hash and is deleted by the administrative role;
# no plaintext credential is ever passed to PostgreSQL or printed.
auth_workspace=qualification-auth-workspace
auth_identity=qualification-auth-identity
auth_role=qualification-auth-role
auth_key=qualification-auth-key
auth_secret=qualification-secret
auth_token="fornix_${auth_key}_${auth_secret}"
auth_hash=8119ad1a47f19d3c5d3d1ace4a65945c16c04f16ea1aca148ef537489d06c594
psql "$admin_dsn" -v ON_ERROR_STOP=1 -v auth_workspace="$auth_workspace" -v auth_identity="$auth_identity" -v auth_role="$auth_role" -v auth_key="$auth_key" -v auth_hash="$auth_hash" <<'SQL'
DELETE FROM fornix.api_keys WHERE id = :'auth_key';
DELETE FROM fornix.identity_role_bindings WHERE identity_id = :'auth_identity';
DELETE FROM fornix.roles WHERE id = :'auth_role';
DELETE FROM fornix.identities WHERE id = :'auth_identity';
INSERT INTO fornix.identities(id,workspace_id,subject,kind,status)
VALUES (:'auth_identity', :'auth_workspace', 'qualification', 'service', 'active');
INSERT INTO fornix.roles(id,workspace_id,name,permissions)
VALUES (:'auth_role', :'auth_workspace', 'qualification', '["model:invoke","retrieval:read","workspace:read","workspace:write"]'::jsonb);
INSERT INTO fornix.identity_role_bindings(workspace_id,identity_id,role_id)
VALUES (:'auth_workspace', :'auth_identity', :'auth_role');
INSERT INTO fornix.api_keys(id,workspace_id,identity_id,prefix,token_hash,status)
VALUES (:'auth_key', :'auth_workspace', :'auth_identity', 'qualification_', decode(:'auth_hash','hex'), 'active');
SQL
auth_result=$(psql "$app_dsn" -v ON_ERROR_STOP=1 -At -c "SELECT workspace_id || '|' || identity_id || '|' || octet_length(token_hash)::text FROM fornix.authenticate_api_key('qualification-auth-key')")
case "$auth_result" in
	"$auth_workspace|$auth_identity|32") : ;;
	*) printf '%s\n' 'runtime API-key authentication function did not return the expected scoped principal' >&2; exit 1 ;;
esac

# Qualify the Task 66 federation tables through the non-owner runtime role.
# The administrative fixture is synthetic and contains no credential material;
# the application session must see its own workspace and zero rows from a
# different workspace after the transaction-local context is installed.
federation_workspace=qualification-federation-workspace
federation_foreign=qualification-federation-foreign
psql "$admin_dsn" -v ON_ERROR_STOP=1 -v federation_workspace="$federation_workspace" -v federation_foreign="$federation_foreign" <<'SQL'
DELETE FROM fornix.workspace_federation_peers WHERE workspace_id IN (:'federation_workspace', :'federation_foreign');
INSERT INTO fornix.workspace_federation_peers(workspace_id,peer_id,remote_workspace_id,endpoint_url,credential_ref,config_hash)
VALUES (:'federation_workspace','qualification-peer','remote-a','https://peer.example.test','provider/federation',repeat('a',64)),
       (:'federation_foreign','qualification-peer','remote-b','https://peer.example.test','provider/federation',repeat('b',64));
SQL
cleanup_federation_fixture() {
	psql "$admin_dsn" -v ON_ERROR_STOP=1 -v federation_workspace="$federation_workspace" -v federation_foreign="$federation_foreign" >/dev/null <<'SQL'
DELETE FROM fornix.workspace_federation_peers WHERE workspace_id IN (:'federation_workspace', :'federation_foreign');
SQL
}
trap cleanup_federation_fixture EXIT
federation_scope_count=$(psql "$app_dsn" -v ON_ERROR_STOP=1 -At -c "SELECT fornix.set_workspace_context('$federation_workspace'); SELECT count(*) FROM fornix.workspace_federation_peers")
federation_foreign_count=$(psql "$app_dsn" -v ON_ERROR_STOP=1 -At -c "SELECT fornix.set_workspace_context('$federation_workspace'); SELECT count(*) FROM fornix.workspace_federation_peers WHERE workspace_id='$federation_foreign'")
case "$federation_scope_count:$federation_foreign_count" in
	1:0) : ;;
	*) printf '%s\n' 'runtime role federation RLS qualification failed' >&2; exit 1 ;;
esac
FORNIX_RLS_TEST_DSN="$app_dsn" FORNIX_RLS_APP_ROLE="$app_role" go test ./internal/store -run '^TestRuntimeRoleWorkspaceRLSPoolReuseFailsClosed$' -count=1 -v
psql "$admin_dsn" -v ON_ERROR_STOP=1 -v federation_workspace="$federation_workspace" -v federation_foreign="$federation_foreign" <<'SQL'
DELETE FROM fornix.workspace_federation_peers WHERE workspace_id IN (:'federation_workspace', :'federation_foreign');
SQL
trap - EXIT
FORNIX_RLS_TEST_DSN="$app_dsn" FORNIX_RLS_AUTH_TOKEN="$auth_token" go test ./internal/store -run '^TestAuthStoreAuthenticateWithRuntimeRole$' -count=1 -v
FORNIX_TEST_PG_DSN="$app_dsn" FORNIX_SKIP_MIGRATIONS=1 go test ./internal/store -run '^TestCredentialLeaseStore' -count=1 -v
FORNIX_RLS_TEST_DSN="$app_dsn" FORNIX_RLS_AUTH_TOKEN="$auth_token" go test ./internal/server -run '^TestWorkspaceScopedRetrievalRouteWithRuntimeRLS$' -count=1 -v
FORNIX_RLS_TEST_DSN="$app_dsn" FORNIX_RLS_AUTH_TOKEN="$auth_token" go test ./internal/server -run '^TestWorkspaceScopedCoordinationRouterRoutesWithRuntimeRLS$' -count=1 -v
psql "$admin_dsn" -v ON_ERROR_STOP=1 -v auth_identity="$auth_identity" -v auth_role="$auth_role" -v auth_key="$auth_key" <<'SQL'
DELETE FROM fornix.api_keys WHERE id = :'auth_key';
DELETE FROM fornix.identity_role_bindings WHERE identity_id = :'auth_identity';
DELETE FROM fornix.roles WHERE id = :'auth_role';
DELETE FROM fornix.identities WHERE id = :'auth_identity';
SQL
printf '%s\n' 'role-separated PostgreSQL qualification passed: runtime API-key authentication is scoped and redacted'
