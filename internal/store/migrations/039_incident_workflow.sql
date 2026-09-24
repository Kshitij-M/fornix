-- 039: fake-first, workspace-scoped incident resources for the universal
-- multi-domain reference workflow. Raw event bytes remain in evidence.

CREATE TABLE IF NOT EXISTS fornix.incident_records (
  workspace_id TEXT NOT NULL,
  id TEXT NOT NULL,
  source_system TEXT NOT NULL,
  external_id TEXT NOT NULL,
  severity TEXT NOT NULL,
  summary_hash TEXT NOT NULL DEFAULT '',
  payload_hash TEXT NOT NULL,
  payload_evidence_id BIGINT NOT NULL,
  status TEXT NOT NULL,
  workflow_id TEXT NOT NULL DEFAULT '',
  receipt_id TEXT NOT NULL DEFAULT '',
  event_hash TEXT NOT NULL,
  actor JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, id),
  UNIQUE (workspace_id, source_system, external_id),
  CONSTRAINT incident_evidence_fk FOREIGN KEY (workspace_id, payload_evidence_id)
    REFERENCES fornix.evidence_records(workspace_id, id),
  CONSTRAINT incident_identity_nonempty CHECK (
    length(workspace_id) > 0 AND length(id) > 0 AND length(source_system) > 0 AND length(external_id) > 0
  ),
  CONSTRAINT incident_hash_shape CHECK (
    (summary_hash = '' OR summary_hash ~ '^[0-9a-f]{64}$') AND
    payload_hash ~ '^[0-9a-f]{64}$' AND event_hash ~ '^[0-9a-f]{64}$'
  ),
  CONSTRAINT incident_severity_valid CHECK (severity IN ('info','warning','critical')),
  CONSTRAINT incident_status_valid CHECK (status IN ('open','investigating','awaiting_approval','remediating','resolved','failed','cancelled','verification_failed')),
  CONSTRAINT incident_actor_bounded CHECK (octet_length(actor::text) <= 16384),
  CONSTRAINT incident_refs_bounded CHECK (length(workflow_id) <= 128 AND length(receipt_id) <= 128)
);
CREATE INDEX IF NOT EXISTS incident_records_status_idx
  ON fornix.incident_records(workspace_id, status, updated_at, id);

CREATE TABLE IF NOT EXISTS fornix.incident_event_deliveries (
  workspace_id TEXT NOT NULL,
  delivery_key TEXT NOT NULL,
  incident_id TEXT NOT NULL,
  event_hash TEXT NOT NULL,
  duplicate BOOLEAN NOT NULL DEFAULT FALSE,
  actor JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, delivery_key),
  CONSTRAINT incident_delivery_fk FOREIGN KEY (workspace_id, incident_id)
    REFERENCES fornix.incident_records(workspace_id, id),
  CONSTRAINT incident_delivery_hash CHECK (event_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT incident_delivery_actor_bounded CHECK (octet_length(actor::text) <= 16384)
);
CREATE INDEX IF NOT EXISTS incident_delivery_incident_idx
  ON fornix.incident_event_deliveries(workspace_id, incident_id, created_at, delivery_key);

CREATE TABLE IF NOT EXISTS fornix.incident_idempotency (
  workspace_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  incident_id TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, idempotency_key),
  CONSTRAINT incident_idempotency_fk FOREIGN KEY (workspace_id, incident_id)
    REFERENCES fornix.incident_records(workspace_id, id),
  CONSTRAINT incident_idempotency_hash CHECK (request_hash ~ '^[0-9a-f]{64}$')
);

CREATE TABLE IF NOT EXISTS fornix.incident_approvals (
  workspace_id TEXT NOT NULL,
  run_id TEXT NOT NULL,
  step_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  operation_hash TEXT NOT NULL,
  plan_hash TEXT NOT NULL,
  decision TEXT NOT NULL,
  decision_hash TEXT NOT NULL,
  actor JSONB NOT NULL,
  evidence_id BIGINT NOT NULL,
  evidence_hash TEXT NOT NULL,
  decided_at TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  PRIMARY KEY (workspace_id, run_id, step_id),
  UNIQUE (workspace_id, idempotency_key),
  CONSTRAINT incident_approval_run_fk FOREIGN KEY (workspace_id, run_id)
    REFERENCES fornix.workflow_runs(workspace_id, run_id),
  CONSTRAINT incident_approval_evidence_fk FOREIGN KEY (workspace_id, evidence_id)
    REFERENCES fornix.evidence_records(workspace_id, id),
  CONSTRAINT incident_approval_decision_valid CHECK (decision IN ('approve','reject')),
  CONSTRAINT incident_approval_hash_shape CHECK (
    operation_hash ~ '^[0-9a-f]{64}$' AND plan_hash ~ '^[0-9a-f]{64}$' AND
    decision_hash ~ '^[0-9a-f]{64}$' AND evidence_hash ~ '^[0-9a-f]{64}$'
  )
);
CREATE INDEX IF NOT EXISTS incident_approvals_key_idx
  ON fornix.incident_approvals(workspace_id, idempotency_key);

CREATE OR REPLACE FUNCTION fornix.reject_incident_history_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix incident history is append-only';
END;
$$;

DROP TRIGGER IF EXISTS incident_event_deliveries_append_only ON fornix.incident_event_deliveries;
CREATE TRIGGER incident_event_deliveries_append_only
  BEFORE UPDATE OR DELETE ON fornix.incident_event_deliveries
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_incident_history_mutation();
DROP TRIGGER IF EXISTS incident_idempotency_append_only ON fornix.incident_idempotency;
CREATE TRIGGER incident_idempotency_append_only
  BEFORE UPDATE OR DELETE ON fornix.incident_idempotency
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_incident_history_mutation();
DROP TRIGGER IF EXISTS incident_approvals_append_only ON fornix.incident_approvals;
CREATE TRIGGER incident_approvals_append_only
  BEFORE UPDATE OR DELETE ON fornix.incident_approvals
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_incident_history_mutation();
