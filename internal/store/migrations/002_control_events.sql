-- Durable control-plane history.  The event row is append-only: projections may
-- be rebuilt from it, but authoritative history is never updated in place.
CREATE TABLE IF NOT EXISTS fabric.control_events (
  sequence BIGSERIAL PRIMARY KEY,
  event_id TEXT NOT NULL UNIQUE,
  event_type TEXT NOT NULL,
  schema_version INT NOT NULL,
  workspace_id TEXT NOT NULL,
  scope JSONB NOT NULL DEFAULT '{}'::jsonb,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  session_ref JSONB NOT NULL DEFAULT '{}'::jsonb,
  causation_id TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL DEFAULT '',
  idempotency_key TEXT NOT NULL DEFAULT '',
  state_deltas JSONB NOT NULL DEFAULT '[]'::jsonb,
  artifacts JSONB NOT NULL DEFAULT '[]'::jsonb,
  provenance JSONB NOT NULL DEFAULT '{}'::jsonb,
  payload JSONB NOT NULL,
  raw_payload BYTEA NOT NULL,
  request_hash TEXT NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT control_events_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT control_events_schema_positive CHECK (schema_version > 0)
);

CREATE INDEX IF NOT EXISTS control_events_workspace_sequence_idx
  ON fabric.control_events (workspace_id, sequence);
CREATE INDEX IF NOT EXISTS control_events_type_sequence_idx
  ON fabric.control_events (workspace_id, event_type, sequence);
CREATE INDEX IF NOT EXISTS control_events_task_idx
  ON fabric.control_events ((task_ref->>'id'), sequence)
  WHERE task_ref ? 'id';
CREATE INDEX IF NOT EXISTS control_events_session_idx
  ON fabric.control_events ((session_ref->>'id'), sequence)
  WHERE session_ref ? 'id';

-- A key is reserved in the same transaction as its event.  event_sequence is
-- nullable only while that transaction is in flight; committed rows always
-- point to the event that owns the effect.
CREATE TABLE IF NOT EXISTS fabric.idempotency_records (
  workspace_id TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  event_sequence BIGINT,
  event_id TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (workspace_id, idempotency_key),
  CONSTRAINT idempotency_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT idempotency_key_nonempty CHECK (length(idempotency_key) > 0),
  CONSTRAINT idempotency_sequence_positive CHECK (event_sequence IS NULL OR event_sequence > 0)
);

CREATE INDEX IF NOT EXISTS idempotency_event_sequence_idx
  ON fabric.idempotency_records (workspace_id, event_sequence)
  WHERE event_sequence IS NOT NULL;

-- Checkpoints are monotonic consumer cursors.  They intentionally do not
-- delete or acknowledge the underlying event history.
CREATE TABLE IF NOT EXISTS fabric.control_checkpoints (
  workspace_id TEXT NOT NULL,
  consumer_id TEXT NOT NULL,
  sequence BIGINT NOT NULL DEFAULT 0,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (workspace_id, consumer_id),
  CONSTRAINT checkpoint_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT checkpoint_consumer_nonempty CHECK (length(consumer_id) > 0),
  CONSTRAINT checkpoint_sequence_nonnegative CHECK (sequence >= 0)
);
