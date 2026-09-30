-- 071: bind hash-only external-boundary observations to deployment evidence
-- and release admission. Historical rows remain readable with empty values;
-- new strict external-effect evidence must carry the complete pair.

ALTER TABLE fornix.qualification_deployment_evidence_links
  ADD COLUMN IF NOT EXISTS external_boundary_hash TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS boundary_evidence_hash TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS boundary_evidence_expires_at TIMESTAMPTZ;

ALTER TABLE fornix.qualification_deployment_evidence_links
  DROP CONSTRAINT IF EXISTS qualification_deployment_evidence_boundary_hashes_valid;

ALTER TABLE fornix.qualification_deployment_evidence_links
  ADD CONSTRAINT qualification_deployment_evidence_boundary_hashes_valid CHECK (
    (external_boundary_hash = '' AND boundary_evidence_hash = '' AND boundary_evidence_expires_at IS NULL) OR
    (external_boundary_hash ~ '^[0-9a-f]{64}$' AND boundary_evidence_hash ~ '^[0-9a-f]{64}$' AND boundary_evidence_expires_at IS NOT NULL)
  );

CREATE INDEX IF NOT EXISTS qualification_deployment_evidence_boundary_idx
  ON fornix.qualification_deployment_evidence_links(workspace_id, deployment_id, external_boundary_hash)
  WHERE external_boundary_hash <> '';

ALTER TABLE fornix.qualification_deployment_release_verifications
  ADD COLUMN IF NOT EXISTS external_boundary_hash TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS boundary_evidence_hash TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS boundary_evidence_expires_at TIMESTAMPTZ;

ALTER TABLE fornix.qualification_deployment_release_verifications
  DROP CONSTRAINT IF EXISTS qualification_verification_boundary_hashes_valid;

ALTER TABLE fornix.qualification_deployment_release_verifications
  ADD CONSTRAINT qualification_verification_boundary_hashes_valid CHECK (
    (external_boundary_hash = '' AND boundary_evidence_hash = '' AND boundary_evidence_expires_at IS NULL) OR
    (external_boundary_hash ~ '^[0-9a-f]{64}$' AND boundary_evidence_hash ~ '^[0-9a-f]{64}$' AND boundary_evidence_expires_at IS NOT NULL)
  );

CREATE INDEX IF NOT EXISTS qualification_verification_boundary_idx
  ON fornix.qualification_deployment_release_verifications(workspace_id, deployment_id, external_boundary_hash)
  WHERE external_boundary_hash <> '';
