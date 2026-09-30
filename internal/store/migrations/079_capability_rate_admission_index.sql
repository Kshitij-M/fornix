-- Support bounded rolling-window admission checks directly from the
-- authoritative append-only decision history. Denials do not consume rate
-- capacity; awaiting-approval decisions do.
CREATE INDEX IF NOT EXISTS operation_admission_capability_rate_idx
  ON fornix.operation_admission_decisions (
    workspace_id,
    (capability->'connector'->>'name'),
    (capability->>'name'),
    created_at DESC,
    decision_id
  )
  WHERE status <> 'denied';
