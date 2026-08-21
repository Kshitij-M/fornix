-- Artifact-backed output links. This migration is additive: inline compatibility
-- fields remain readable and old rows remain valid without backfill.

ALTER TABLE fornix.artifact_refs
  ADD COLUMN IF NOT EXISTS causation_id TEXT NOT NULL DEFAULT '',
  ADD COLUMN IF NOT EXISTS correlation_id TEXT NOT NULL DEFAULT '';

ALTER TABLE fornix.tool_runs
  ADD COLUMN IF NOT EXISTS stdout_artifact_id BIGINT,
  ADD COLUMN IF NOT EXISTS stderr_artifact_id BIGINT,
  ADD COLUMN IF NOT EXISTS result_artifact_id BIGINT;

ALTER TABLE fornix.evidence_records
  ADD COLUMN IF NOT EXISTS raw_artifact_id BIGINT;

ALTER TABLE fornix.agent_runs
  ADD COLUMN IF NOT EXISTS last_output_artifact_id BIGINT,
  ADD COLUMN IF NOT EXISTS history_artifact_id BIGINT;

CREATE TABLE IF NOT EXISTS fornix.artifact_lifecycle_events (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  artifact_id BIGINT NOT NULL,
  action TEXT NOT NULL,
  previous_status TEXT NOT NULL,
  new_status TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  causation_id TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT artifact_lifecycle_workspace_fk FOREIGN KEY (workspace_id, artifact_id)
    REFERENCES fornix.artifacts(workspace_id, id),
  CONSTRAINT artifact_lifecycle_action_valid CHECK (action IN ('archive','delete','verify')),
  CONSTRAINT artifact_lifecycle_workspace_nonempty CHECK (length(workspace_id) > 0)
);
CREATE INDEX IF NOT EXISTS artifact_lifecycle_workspace_artifact_idx
  ON fornix.artifact_lifecycle_events(workspace_id, artifact_id, id);

CREATE OR REPLACE FUNCTION fornix.reject_artifact_lifecycle_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'fornix artifact lifecycle history is append-only';
END;
$$;
DROP TRIGGER IF EXISTS artifact_lifecycle_append_only ON fornix.artifact_lifecycle_events;
CREATE TRIGGER artifact_lifecycle_append_only
  BEFORE UPDATE OR DELETE ON fornix.artifact_lifecycle_events
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_artifact_lifecycle_mutation();

-- An artifact-backed source keeps a bounded compatibility marker inline while
-- raw_size_bytes continues to describe the authoritative raw bytes.
ALTER TABLE fornix.evidence_records DROP CONSTRAINT IF EXISTS evidence_raw_size;
ALTER TABLE fornix.evidence_records DROP CONSTRAINT IF EXISTS evidence_raw_length;
ALTER TABLE fornix.evidence_records
  ADD CONSTRAINT evidence_raw_stored_size CHECK (raw_size_bytes BETWEEN 1 AND 67108864),
  ADD CONSTRAINT evidence_raw_representation CHECK (
    (raw_artifact_id IS NULL AND raw_size_bytes = octet_length(raw_payload))
    OR raw_artifact_id IS NOT NULL
  );

CREATE OR REPLACE FUNCTION fornix.reject_provenance_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  -- One-way backfill is the only permitted evidence-row update: the original
  -- bytes are already represented by the new immutable artifact, and the
  -- compatibility column becomes its marker. Identity, hash, derived views,
  -- and supersession metadata remain immutable.
  IF TG_TABLE_NAME = 'evidence_records'
     AND OLD.raw_artifact_id IS NULL
     AND NEW.raw_artifact_id IS NOT NULL
     AND NEW.id IS NOT DISTINCT FROM OLD.id
     AND NEW.workspace_id IS NOT DISTINCT FROM OLD.workspace_id
     AND NEW.source_reference IS NOT DISTINCT FROM OLD.source_reference
     AND NEW.deduplication_key IS NOT DISTINCT FROM OLD.deduplication_key
     AND NEW.kind IS NOT DISTINCT FROM OLD.kind
     AND NEW.media_type IS NOT DISTINCT FROM OLD.media_type
     AND NEW.gist IS NOT DISTINCT FROM OLD.gist
     AND NEW.detail IS NOT DISTINCT FROM OLD.detail
     AND NEW.raw_size_bytes IS NOT DISTINCT FROM OLD.raw_size_bytes
     AND NEW.evidence_hash IS NOT DISTINCT FROM OLD.evidence_hash
     AND NEW.supersedes_id IS NOT DISTINCT FROM OLD.supersedes_id
  THEN
    RETURN NEW;
  END IF;
  RAISE EXCEPTION 'fornix provenance records are append-only';
END;
$$;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'tool_runs_stdout_artifact_fk') THEN
    ALTER TABLE fornix.tool_runs ADD CONSTRAINT tool_runs_stdout_artifact_fk
      FOREIGN KEY (workspace_id, stdout_artifact_id)
      REFERENCES fornix.artifacts(workspace_id, id);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'tool_runs_stderr_artifact_fk') THEN
    ALTER TABLE fornix.tool_runs ADD CONSTRAINT tool_runs_stderr_artifact_fk
      FOREIGN KEY (workspace_id, stderr_artifact_id)
      REFERENCES fornix.artifacts(workspace_id, id);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'tool_runs_result_artifact_fk') THEN
    ALTER TABLE fornix.tool_runs ADD CONSTRAINT tool_runs_result_artifact_fk
      FOREIGN KEY (workspace_id, result_artifact_id)
      REFERENCES fornix.artifacts(workspace_id, id);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'evidence_records_raw_artifact_fk') THEN
    ALTER TABLE fornix.evidence_records ADD CONSTRAINT evidence_records_raw_artifact_fk
      FOREIGN KEY (workspace_id, raw_artifact_id)
      REFERENCES fornix.artifacts(workspace_id, id);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'agent_runs_last_output_artifact_fk') THEN
    ALTER TABLE fornix.agent_runs ADD CONSTRAINT agent_runs_last_output_artifact_fk
      FOREIGN KEY (workspace_id, last_output_artifact_id)
      REFERENCES fornix.artifacts(workspace_id, id);
  END IF;
  IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'agent_runs_history_artifact_fk') THEN
    ALTER TABLE fornix.agent_runs ADD CONSTRAINT agent_runs_history_artifact_fk
      FOREIGN KEY (workspace_id, history_artifact_id)
      REFERENCES fornix.artifacts(workspace_id, id);
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS tool_runs_workspace_artifacts_idx
  ON fornix.tool_runs(workspace_id, stdout_artifact_id, stderr_artifact_id, result_artifact_id)
  WHERE stdout_artifact_id IS NOT NULL OR stderr_artifact_id IS NOT NULL OR result_artifact_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS evidence_records_workspace_artifact_idx
  ON fornix.evidence_records(workspace_id, raw_artifact_id)
  WHERE raw_artifact_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS agent_runs_workspace_artifacts_idx
  ON fornix.agent_runs(workspace_id, last_output_artifact_id, history_artifact_id)
  WHERE last_output_artifact_id IS NOT NULL OR history_artifact_id IS NOT NULL;
