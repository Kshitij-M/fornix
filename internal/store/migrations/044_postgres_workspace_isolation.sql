-- 044: defense-in-depth workspace isolation for the generic authority.
--
-- Policies intentionally are not FORCE RLS. Existing development databases use
-- one role that owns the tables; production must transfer ownership to a
-- migration role and run the service as a dedicated NOBYPASSRLS application
-- role before claiming database-enforced tenant isolation.

CREATE OR REPLACE FUNCTION fornix.set_workspace_context(p_workspace_id TEXT)
RETURNS VOID
LANGUAGE plpgsql
SECURITY INVOKER
AS $$
BEGIN
  IF p_workspace_id IS NULL OR btrim(p_workspace_id) = '' OR length(p_workspace_id) > 256 THEN
    RAISE EXCEPTION 'fornix workspace context is required and bounded';
  END IF;
  PERFORM set_config('fornix.workspace_id', btrim(p_workspace_id), true);
END;
$$;

DO $$
DECLARE
  table_name TEXT;
BEGIN
  FOREACH table_name IN ARRAY ARRAY[
    'operations',
    'operation_idempotency',
    'operation_transitions',
    'operation_resources',
    'operation_steps',
    'operation_attempts',
    'operation_effects',
    'operation_callbacks',
    'operation_leases',
    'operation_links',
    'operation_results',
    'operation_admission_decisions',
    'operation_approvals',
    'operation_approval_transitions',
    'operation_effect_state',
    'operation_effect_transitions',
    'operation_effect_leases'
  ] LOOP
    IF to_regclass(format('fornix.%I', table_name)) IS NULL THEN
      CONTINUE;
    END IF;
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
