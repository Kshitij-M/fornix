-- 057: immutable, workspace-scoped links from successful embedding calls to
-- derived target projections. The link is committed in the target's
-- transaction; the embedding call itself remains the external-effect ledger.

CREATE TABLE IF NOT EXISTS fornix.embedding_target_attachments (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  request_id TEXT NOT NULL,
  target_kind TEXT NOT NULL,
  target_id TEXT NOT NULL,
  source_hash TEXT NOT NULL,
  vector_hash TEXT NOT NULL,
  attached_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
  UNIQUE (workspace_id, request_id, target_kind, target_id),
  FOREIGN KEY (workspace_id, request_id)
    REFERENCES fornix.embedding_calls(workspace_id, request_id),
  CONSTRAINT embedding_target_attachment_identity CHECK (
    length(workspace_id) > 0 AND length(request_id) > 0 AND
    length(target_kind) BETWEEN 1 AND 64 AND length(target_id) BETWEEN 1 AND 512 AND
    source_hash ~ '^[0-9a-f]{64}$' AND vector_hash ~ '^[0-9a-f]{64}$'
  )
);

CREATE INDEX IF NOT EXISTS embedding_target_attachments_target_idx
  ON fornix.embedding_target_attachments(workspace_id, target_kind, target_id, attached_at, id);
CREATE INDEX IF NOT EXISTS embedding_target_attachments_request_idx
  ON fornix.embedding_target_attachments(workspace_id, request_id, attached_at, id);

ALTER TABLE fornix.embedding_target_attachments ENABLE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS workspace_scope_isolation ON fornix.embedding_target_attachments;
CREATE POLICY workspace_scope_isolation ON fornix.embedding_target_attachments
  USING (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''))
  WITH CHECK (workspace_id = NULLIF(current_setting('fornix.workspace_id', true), ''));
