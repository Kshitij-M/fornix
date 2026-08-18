-- Retrieval sources must be explicitly workspace scoped. Existing rows belong
-- to the default workspace; new uniqueness and indexes keep equal content from
-- different workspaces independent without copying or rewriting source data.

ALTER TABLE fornix.memos
  ADD COLUMN IF NOT EXISTS workspace_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE fornix.chunks
  ADD COLUMN IF NOT EXISTS workspace_id TEXT NOT NULL DEFAULT 'default';
ALTER TABLE fornix.symbols
  ADD COLUMN IF NOT EXISTS workspace_id TEXT NOT NULL DEFAULT 'default';

ALTER TABLE fornix.memos DROP CONSTRAINT IF EXISTS memos_sha256_uniq;
ALTER TABLE fornix.memos
  ADD CONSTRAINT memos_workspace_sha256_uniq UNIQUE (workspace_id, sha256);
ALTER TABLE fornix.memos
  ADD CONSTRAINT memos_workspace_nonempty CHECK (length(workspace_id) > 0);

ALTER TABLE fornix.chunks DROP CONSTRAINT IF EXISTS chunks_sha256_uniq;
ALTER TABLE fornix.chunks
  ADD CONSTRAINT chunks_workspace_sha256_uniq UNIQUE (workspace_id, content_sha256);
ALTER TABLE fornix.chunks
  ADD CONSTRAINT chunks_workspace_nonempty CHECK (length(workspace_id) > 0);

ALTER TABLE fornix.symbols DROP CONSTRAINT IF EXISTS symbols_uniq;
ALTER TABLE fornix.symbols
  ADD CONSTRAINT symbols_workspace_identity_uniq
  UNIQUE (workspace_id, repo, file_path, symbol_name, symbol_kind);
ALTER TABLE fornix.symbols
  ADD CONSTRAINT symbols_workspace_nonempty CHECK (length(workspace_id) > 0);

CREATE INDEX IF NOT EXISTS memos_workspace_tsv_idx
  ON fornix.memos (workspace_id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS memos_workspace_type_idx
  ON fornix.memos (workspace_id, type, id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS memos_workspace_embedding_idx
  ON fornix.memos (workspace_id, id) WHERE deleted_at IS NULL AND embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS chunks_workspace_tsv_idx
  ON fornix.chunks (workspace_id, id);
CREATE INDEX IF NOT EXISTS chunks_workspace_source_idx
  ON fornix.chunks (workspace_id, source_path, id);
CREATE INDEX IF NOT EXISTS chunks_workspace_embedding_idx
  ON fornix.chunks (workspace_id, id) WHERE embedding IS NOT NULL;

CREATE INDEX IF NOT EXISTS symbols_workspace_repo_idx
  ON fornix.symbols (workspace_id, repo, id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS symbols_workspace_name_idx
  ON fornix.symbols (workspace_id, symbol_name, id) WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS symbols_workspace_embedding_idx
  ON fornix.symbols (workspace_id, id) WHERE deleted_at IS NULL AND embedding IS NOT NULL;
