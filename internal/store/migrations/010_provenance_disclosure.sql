-- Immutable, workspace-scoped evidence and typed provenance. The raw event
-- store remains authoritative; these rows are attributable evidence views and
-- graph metadata that may be replayed or disclosed at bounded detail levels.
CREATE TABLE IF NOT EXISTS fornix.evidence_records (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  source_reference TEXT NOT NULL,
  deduplication_key TEXT NOT NULL,
  kind TEXT NOT NULL,
  media_type TEXT NOT NULL DEFAULT 'application/octet-stream',
  gist TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT '',
  raw_payload BYTEA NOT NULL,
  raw_size_bytes BIGINT NOT NULL,
  evidence_hash TEXT NOT NULL,
  supersedes_id BIGINT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT evidence_workspace_id_unique UNIQUE (workspace_id, id),
  CONSTRAINT evidence_source_dedupe_unique
    UNIQUE (workspace_id, source_reference, deduplication_key),
  CONSTRAINT evidence_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT evidence_source_reference_nonempty CHECK (length(source_reference) > 0),
  CONSTRAINT evidence_dedupe_nonempty CHECK (length(deduplication_key) > 0),
  CONSTRAINT evidence_kind_nonempty CHECK (length(kind) > 0),
  CONSTRAINT evidence_media_type_nonempty CHECK (length(media_type) > 0),
  CONSTRAINT evidence_gist_size CHECK (octet_length(gist) <= 16384),
  CONSTRAINT evidence_detail_size CHECK (octet_length(detail) <= 4194304),
  CONSTRAINT evidence_raw_size CHECK (raw_size_bytes BETWEEN 1 AND 4194304),
  CONSTRAINT evidence_raw_length CHECK (raw_size_bytes = octet_length(raw_payload)),
  CONSTRAINT evidence_hash_shape CHECK (evidence_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT evidence_supersedes_fk
    FOREIGN KEY (workspace_id, supersedes_id)
    REFERENCES fornix.evidence_records(workspace_id, id)
    DEFERRABLE INITIALLY IMMEDIATE
);

CREATE INDEX IF NOT EXISTS evidence_records_workspace_hash_idx
  ON fornix.evidence_records (workspace_id, evidence_hash, id);
CREATE INDEX IF NOT EXISTS evidence_records_workspace_supersedes_idx
  ON fornix.evidence_records (workspace_id, supersedes_id, id)
  WHERE supersedes_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS evidence_records_workspace_created_idx
  ON fornix.evidence_records (workspace_id, created_at, id);

CREATE TABLE IF NOT EXISTS fornix.provenance_edges (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  from_evidence_id BIGINT NOT NULL,
  to_evidence_id BIGINT NOT NULL,
  relation TEXT NOT NULL,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT provenance_workspace_id_unique UNIQUE (workspace_id, id),
  CONSTRAINT provenance_endpoints_distinct CHECK (from_evidence_id <> to_evidence_id),
  CONSTRAINT provenance_workspace_nonempty CHECK (length(workspace_id) > 0),
  CONSTRAINT provenance_relation_nonempty CHECK (length(relation) > 0),
  CONSTRAINT provenance_edge_unique
    UNIQUE (workspace_id, from_evidence_id, to_evidence_id, relation),
  CONSTRAINT provenance_from_fk
    FOREIGN KEY (workspace_id, from_evidence_id)
    REFERENCES fornix.evidence_records(workspace_id, id),
  CONSTRAINT provenance_to_fk
    FOREIGN KEY (workspace_id, to_evidence_id)
    REFERENCES fornix.evidence_records(workspace_id, id)
);

CREATE INDEX IF NOT EXISTS provenance_edges_workspace_from_idx
  ON fornix.provenance_edges (workspace_id, from_evidence_id, relation, to_evidence_id, id);
CREATE INDEX IF NOT EXISTS provenance_edges_workspace_to_idx
  ON fornix.provenance_edges (workspace_id, to_evidence_id, relation, from_evidence_id, id);

CREATE OR REPLACE FUNCTION fornix.reject_provenance_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'fornix provenance records are append-only';
END;
$$;

DROP TRIGGER IF EXISTS evidence_records_append_only ON fornix.evidence_records;
CREATE TRIGGER evidence_records_append_only
  BEFORE UPDATE OR DELETE ON fornix.evidence_records
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_provenance_mutation();

DROP TRIGGER IF EXISTS provenance_edges_append_only ON fornix.provenance_edges;
CREATE TRIGGER provenance_edges_append_only
  BEFORE UPDATE OR DELETE ON fornix.provenance_edges
  FOR EACH ROW EXECUTE FUNCTION fornix.reject_provenance_mutation();
