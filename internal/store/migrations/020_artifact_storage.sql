-- Workspace-scoped immutable content-addressed artifacts. Postgres remains the
-- authority; chunks use BYTEA/TOAST and are bounded to make every write and
-- integrity check predictable without an object-store dependency.
CREATE TABLE IF NOT EXISTS fornix.artifacts (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  kind TEXT NOT NULL,
  media_type TEXT NOT NULL DEFAULT 'application/octet-stream',
  byte_size BIGINT NOT NULL,
  chunk_size INTEGER NOT NULL,
  chunk_count INTEGER NOT NULL,
  manifest JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'active',
  integrity_state TEXT NOT NULL DEFAULT 'unknown',
  retain_until TIMESTAMPTZ,
  archive_after TIMESTAMPTZ,
  delete_after TIMESTAMPTZ,
  allow_delete BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  archived_at TIMESTAMPTZ,
  deleted_at TIMESTAMPTZ,
  integrity_at TIMESTAMPTZ,
  CONSTRAINT artifacts_workspace_id_unique UNIQUE (workspace_id, id),
  CONSTRAINT artifacts_content_unique UNIQUE (workspace_id, content_hash),
  CONSTRAINT artifacts_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT artifacts_hash_shape CHECK (content_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT artifacts_kind_nonempty CHECK (length(kind) > 0),
  CONSTRAINT artifacts_media_type_nonempty CHECK (length(media_type) > 0),
  CONSTRAINT artifacts_byte_size_valid CHECK (byte_size BETWEEN 1 AND 67108864),
  CONSTRAINT artifacts_chunk_size_valid CHECK (chunk_size BETWEEN 1 AND 1048576),
  CONSTRAINT artifacts_chunk_count_valid CHECK (chunk_count BETWEEN 1 AND 256),
  CONSTRAINT artifacts_status_valid CHECK (status IN ('active','archived','deleted')),
  CONSTRAINT artifacts_integrity_valid CHECK (integrity_state IN ('unknown','valid','corrupt')),
  CONSTRAINT artifacts_deleted_state_valid CHECK (
    (status <> 'deleted' AND deleted_at IS NULL) OR
    (status = 'deleted' AND deleted_at IS NOT NULL)
  )
);

CREATE INDEX IF NOT EXISTS artifacts_workspace_status_idx
  ON fornix.artifacts(workspace_id, status, created_at, id);
CREATE INDEX IF NOT EXISTS artifacts_workspace_retention_idx
  ON fornix.artifacts(workspace_id, status, archive_after, delete_after, id);

CREATE TABLE IF NOT EXISTS fornix.artifact_chunks (
  workspace_id TEXT NOT NULL,
  artifact_id BIGINT NOT NULL,
  chunk_index INTEGER NOT NULL,
  content_hash TEXT NOT NULL,
  byte_size INTEGER NOT NULL,
  raw_bytes BYTEA NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (workspace_id, artifact_id, chunk_index),
  CONSTRAINT artifact_chunks_artifact_fk
    FOREIGN KEY (workspace_id, artifact_id)
    REFERENCES fornix.artifacts(workspace_id, id),
  CONSTRAINT artifact_chunks_index_valid CHECK (chunk_index >= 0),
  CONSTRAINT artifact_chunks_hash_shape CHECK (content_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT artifact_chunks_size_valid CHECK (byte_size BETWEEN 1 AND 1048576),
  CONSTRAINT artifact_chunks_length_valid CHECK (byte_size = octet_length(raw_bytes))
);

CREATE INDEX IF NOT EXISTS artifact_chunks_workspace_hash_idx
  ON fornix.artifact_chunks(workspace_id, content_hash);

CREATE TABLE IF NOT EXISTS fornix.artifact_refs (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  artifact_id BIGINT NOT NULL,
  source_kind TEXT NOT NULL,
  source_id TEXT NOT NULL,
  role TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  authoritative BOOLEAN NOT NULL DEFAULT TRUE,
  CONSTRAINT artifact_refs_workspace_id_unique UNIQUE (workspace_id, id),
  CONSTRAINT artifact_refs_artifact_fk
    FOREIGN KEY (workspace_id, artifact_id)
    REFERENCES fornix.artifacts(workspace_id, id),
  CONSTRAINT artifact_refs_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT artifact_refs_source_kind_nonempty CHECK (length(source_kind) > 0),
  CONSTRAINT artifact_refs_source_id_nonempty CHECK (length(source_id) > 0),
  CONSTRAINT artifact_refs_role_nonempty CHECK (length(role) > 0),
  CONSTRAINT artifact_refs_idempotency_nonempty CHECK (length(idempotency_key) > 0),
  CONSTRAINT artifact_refs_hash_shape CHECK (request_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT artifact_refs_identity_unique
    UNIQUE (workspace_id, source_kind, source_id, role),
  CONSTRAINT artifact_refs_idempotency_unique
    UNIQUE (workspace_id, idempotency_key)
);

CREATE INDEX IF NOT EXISTS artifact_refs_workspace_artifact_idx
  ON fornix.artifact_refs(workspace_id, artifact_id, authoritative, id);
CREATE INDEX IF NOT EXISTS artifact_refs_workspace_source_idx
  ON fornix.artifact_refs(workspace_id, source_kind, source_id, role, id);

CREATE TABLE IF NOT EXISTS fornix.artifact_provenance (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  from_artifact_id BIGINT NOT NULL,
  to_artifact_id BIGINT NOT NULL,
  relation TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT artifact_provenance_workspace_id_unique UNIQUE (workspace_id, id),
  CONSTRAINT artifact_provenance_from_fk
    FOREIGN KEY (workspace_id, from_artifact_id)
    REFERENCES fornix.artifacts(workspace_id, id),
  CONSTRAINT artifact_provenance_to_fk
    FOREIGN KEY (workspace_id, to_artifact_id)
    REFERENCES fornix.artifacts(workspace_id, id),
  CONSTRAINT artifact_provenance_distinct CHECK (from_artifact_id <> to_artifact_id),
  CONSTRAINT artifact_provenance_relation_valid CHECK (
    relation IN ('supports','derives','contains','supersedes','contradicts','related')
  ),
  CONSTRAINT artifact_provenance_unique
    UNIQUE (workspace_id, from_artifact_id, to_artifact_id, relation)
);

CREATE INDEX IF NOT EXISTS artifact_provenance_workspace_from_idx
  ON fornix.artifact_provenance(workspace_id, from_artifact_id, relation, to_artifact_id, id);
CREATE INDEX IF NOT EXISTS artifact_provenance_workspace_to_idx
  ON fornix.artifact_provenance(workspace_id, to_artifact_id, relation, from_artifact_id, id);

CREATE OR REPLACE FUNCTION fornix.reject_artifact_raw_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.workspace_id IS DISTINCT FROM OLD.workspace_id
     OR NEW.artifact_id IS DISTINCT FROM OLD.artifact_id
     OR NEW.chunk_index IS DISTINCT FROM OLD.chunk_index
     OR NEW.content_hash IS DISTINCT FROM OLD.content_hash
     OR NEW.byte_size IS DISTINCT FROM OLD.byte_size
     OR NEW.raw_bytes IS DISTINCT FROM OLD.raw_bytes THEN
    RAISE EXCEPTION 'fornix artifact raw bytes are immutable';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS artifact_chunks_raw_immutable ON fornix.artifact_chunks;
CREATE TRIGGER artifact_chunks_raw_immutable
  BEFORE UPDATE ON fornix.artifact_chunks
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_artifact_raw_update();

CREATE OR REPLACE FUNCTION fornix.reject_artifact_identity_update()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.workspace_id IS DISTINCT FROM OLD.workspace_id
     OR NEW.content_hash IS DISTINCT FROM OLD.content_hash
     OR NEW.kind IS DISTINCT FROM OLD.kind
     OR NEW.media_type IS DISTINCT FROM OLD.media_type
     OR NEW.byte_size IS DISTINCT FROM OLD.byte_size
     OR NEW.chunk_size IS DISTINCT FROM OLD.chunk_size
     OR NEW.chunk_count IS DISTINCT FROM OLD.chunk_count
     OR NEW.manifest IS DISTINCT FROM OLD.manifest THEN
    RAISE EXCEPTION 'fornix artifact identity and manifest are immutable';
  END IF;
  RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS artifacts_identity_immutable ON fornix.artifacts;
CREATE TRIGGER artifacts_identity_immutable
  BEFORE UPDATE ON fornix.artifacts
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_artifact_identity_update();

CREATE OR REPLACE FUNCTION fornix.reject_artifact_history_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'fornix artifact references and provenance are append-only';
END;
$$;

DROP TRIGGER IF EXISTS artifact_refs_append_only ON fornix.artifact_refs;
CREATE TRIGGER artifact_refs_append_only
  BEFORE UPDATE OR DELETE ON fornix.artifact_refs
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_artifact_history_mutation();

DROP TRIGGER IF EXISTS artifact_provenance_append_only ON fornix.artifact_provenance;
CREATE TRIGGER artifact_provenance_append_only
  BEFORE UPDATE OR DELETE ON fornix.artifact_provenance
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_artifact_history_mutation();

ALTER TABLE fornix.model_calls ADD COLUMN IF NOT EXISTS response_artifact_id BIGINT;
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint WHERE conname = 'model_calls_response_artifact_fk'
  ) THEN
    ALTER TABLE fornix.model_calls
      ADD CONSTRAINT model_calls_response_artifact_fk
      FOREIGN KEY (workspace_id, response_artifact_id)
      REFERENCES fornix.artifacts(workspace_id, id);
  END IF;
END $$;
CREATE INDEX IF NOT EXISTS model_calls_workspace_response_artifact_idx
  ON fornix.model_calls(workspace_id, response_artifact_id)
  WHERE response_artifact_id IS NOT NULL;
