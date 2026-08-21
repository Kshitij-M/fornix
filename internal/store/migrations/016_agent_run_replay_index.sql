CREATE INDEX IF NOT EXISTS control_events_agent_run_idx
  ON fornix.control_events(workspace_id, (payload->>'run_id'), sequence)
  WHERE event_type LIKE 'agent.%';
