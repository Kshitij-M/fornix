-- 072: current evidence-link lifecycle and append-only revocation/replacement
-- events. Imported signed bytes and prior lifecycle events remain immutable.

ALTER TABLE fornix.qualification_deployment_evidence_links
  ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active',
  ADD COLUMN IF NOT EXISTS supersedes_link_id TEXT,
  ADD COLUMN IF NOT EXISTS superseded_by_link_id TEXT,
  ADD COLUMN IF NOT EXISTS revoked_at TIMESTAMPTZ,
  ADD COLUMN IF NOT EXISTS revocation_reason TEXT;

ALTER TABLE fornix.qualification_deployment_evidence_links
  DROP CONSTRAINT IF EXISTS qualification_evidence_kind_unique;

ALTER TABLE fornix.qualification_deployment_evidence_links
  DROP CONSTRAINT IF EXISTS qualification_deployment_evidence_lifecycle_valid;

ALTER TABLE fornix.qualification_deployment_evidence_links
  ADD CONSTRAINT qualification_deployment_evidence_lifecycle_valid CHECK (
    status IN ('active','revoked','superseded') AND
    (supersedes_link_id IS NULL OR length(supersedes_link_id) BETWEEN 1 AND 128) AND
    (superseded_by_link_id IS NULL OR length(superseded_by_link_id) BETWEEN 1 AND 128) AND
    (revocation_reason IS NULL OR (length(revocation_reason) BETWEEN 1 AND 256 AND revocation_reason !~ '[\x00\r\n]')) AND
    ((status = 'active' AND superseded_by_link_id IS NULL AND revoked_at IS NULL AND revocation_reason IS NULL) OR
     (status = 'revoked' AND superseded_by_link_id IS NULL AND revoked_at IS NOT NULL AND revocation_reason IS NOT NULL) OR
     (status = 'superseded' AND superseded_by_link_id IS NOT NULL AND revoked_at IS NULL AND revocation_reason IS NULL))
  );

CREATE UNIQUE INDEX IF NOT EXISTS qualification_evidence_active_kind_unique
  ON fornix.qualification_deployment_evidence_links(workspace_id, deployment_id, release_id, kind)
  WHERE status = 'active';

CREATE INDEX IF NOT EXISTS qualification_evidence_lifecycle_lookup_idx
  ON fornix.qualification_deployment_evidence_links(workspace_id, deployment_id, release_id, status, id);

ALTER TABLE fornix.qualification_deployment_evidence_events
  ADD COLUMN IF NOT EXISTS request_id TEXT,
  ADD COLUMN IF NOT EXISTS idempotency_key TEXT,
  ADD COLUMN IF NOT EXISTS causation_id TEXT,
  ADD COLUMN IF NOT EXISTS correlation_id TEXT;

ALTER TABLE fornix.qualification_deployment_evidence_events
  DROP CONSTRAINT IF EXISTS qualification_evidence_events_event_valid;

ALTER TABLE fornix.qualification_deployment_evidence_events
  ADD CONSTRAINT qualification_evidence_events_event_valid CHECK (event IN ('linked','superseded','revoked'));

CREATE UNIQUE INDEX IF NOT EXISTS qualification_evidence_events_idempotency_unique
  ON fornix.qualification_deployment_evidence_events(workspace_id, deployment_id, idempotency_key, event)
  WHERE idempotency_key IS NOT NULL;
