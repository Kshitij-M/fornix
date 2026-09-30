-- 046: extend defense-in-depth workspace isolation to every workspace-scoped
-- table, including historical and future universal control-plane surfaces.
-- Role ownership and grants remain deployment-specific; see the qualification
-- script and docs/114-production-role-separated-isolation-foundation.md.

DO $$
DECLARE
  table_name TEXT;
BEGIN
  FOR table_name IN
    SELECT c.relname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'fornix'
      AND c.relkind IN ('r', 'p')
      AND EXISTS (
        SELECT 1
        FROM pg_attribute a
        WHERE a.attrelid = c.oid
          AND a.attname = 'workspace_id'
          AND NOT a.attisdropped
      )
      AND c.relname <> 'schema_migrations'
    ORDER BY c.relname
  LOOP
    EXECUTE format('ALTER TABLE fornix.%I ENABLE ROW LEVEL SECURITY', table_name);
    EXECUTE format('DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.%I', table_name);
    EXECUTE format(
      'CREATE POLICY workspace_scope_isolation ON fornix.%I '
      'USING (workspace_id = NULLIF(current_setting(''fornix.workspace_id'', true), '''')) '
      'WITH CHECK (workspace_id = NULLIF(current_setting(''fornix.workspace_id'', true), ''''))',
      table_name
    );
  END LOOP;
END;
$$;

-- Authentication is the one workspace lookup that cannot install a tenant
-- context before it knows which tenant the opaque API-key identifier belongs
-- to. Keep that lookup behind a narrowly scoped, SECURITY DEFINER function.
-- Deployment qualification transfers ownership to the migration role and
-- grants EXECUTE only to the runtime role; the runtime role never receives a
-- direct cross-workspace authentication read path.
CREATE OR REPLACE FUNCTION fornix.authenticate_api_key(p_key_id TEXT)
RETURNS TABLE(
  workspace_id TEXT,
  identity_id TEXT,
  subject TEXT,
  kind TEXT,
  display_name TEXT,
  token_hash BYTEA
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = fornix, pg_temp
AS $$
BEGIN
  RETURN QUERY
  WITH matched AS (
    SELECT k.id AS key_id, k.workspace_id, k.identity_id,
           i.subject, i.kind, i.display_name, k.token_hash
    FROM api_keys k
    JOIN identities i ON i.id = k.identity_id AND i.workspace_id = k.workspace_id
    WHERE k.id = p_key_id
      AND k.status = 'active'
      AND i.status = 'active'
      AND (k.expires_at IS NULL OR k.expires_at > clock_timestamp())
    FOR UPDATE OF k
  )
  SELECT m.workspace_id, m.identity_id, m.subject, m.kind, m.display_name,
         m.token_hash
  FROM matched m;
END;
$$;

REVOKE ALL ON FUNCTION fornix.authenticate_api_key(TEXT) FROM PUBLIC;
