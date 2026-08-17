-- Keep migration 002 immutable while making its append-only invariant
-- enforceable even if a future projection or maintenance job uses SQL.
CREATE OR REPLACE FUNCTION fabric.reject_control_event_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'fabric.control_events is append-only';
END;
$$;

DROP TRIGGER IF EXISTS control_events_append_only ON fabric.control_events;
CREATE TRIGGER control_events_append_only
  BEFORE UPDATE OR DELETE ON fabric.control_events
  FOR EACH ROW EXECUTE FUNCTION fabric.reject_control_event_mutation();
