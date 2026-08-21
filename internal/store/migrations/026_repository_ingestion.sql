-- Durable, resumable repository snapshots. The existing chunks/symbols tables
-- remain compatibility read models; these tables retain ingest identity,
-- checkpoints, source metadata, and append-only lineage.
CREATE TABLE IF NOT EXISTS fornix.ingest_jobs (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  repository TEXT NOT NULL,
  source_root TEXT NOT NULL,
  mount_root TEXT NOT NULL,
  manifest_hash TEXT NOT NULL,
  request_hash TEXT NOT NULL,
  idempotency_key TEXT NOT NULL,
  schema_version INTEGER NOT NULL DEFAULT 1,
  actor JSONB NOT NULL DEFAULT '{}'::jsonb,
  task_ref JSONB,
  session_ref JSONB,
  task_owner_id TEXT NOT NULL DEFAULT '',
  task_fence BIGINT NOT NULL DEFAULT 0,
  causation_id TEXT NOT NULL DEFAULT '',
  correlation_id TEXT NOT NULL DEFAULT '',
  source_config JSONB NOT NULL DEFAULT '{}'::jsonb,
  status TEXT NOT NULL DEFAULT 'queued',
  batch_size INTEGER NOT NULL DEFAULT 32,
  file_count INTEGER NOT NULL DEFAULT 0,
  processed_files INTEGER NOT NULL DEFAULT 0,
  removed_files INTEGER NOT NULL DEFAULT 0,
  chunk_count INTEGER NOT NULL DEFAULT 0,
  symbol_count INTEGER NOT NULL DEFAULT 0,
  source_bytes BIGINT NOT NULL DEFAULT 0,
  indexed_bytes BIGINT NOT NULL DEFAULT 0,
  deduped_chunks INTEGER NOT NULL DEFAULT 0,
  embedding_attempts INTEGER NOT NULL DEFAULT 0,
  embedding_skipped INTEGER NOT NULL DEFAULT 0,
  batch_count INTEGER NOT NULL DEFAULT 0,
  report JSONB,
  report_hash TEXT NOT NULL DEFAULT '',
  report_artifact_id BIGINT,
  last_error TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  started_at TIMESTAMPTZ,
  completed_at TIMESTAMPTZ,
  CONSTRAINT ingest_jobs_status_valid CHECK (status IN ('queued','running','succeeded','failed','cancelled')),
  CONSTRAINT ingest_jobs_counts_valid CHECK (batch_size > 0 AND file_count >= 0 AND processed_files >= 0 AND removed_files >= 0 AND chunk_count >= 0 AND symbol_count >= 0 AND source_bytes >= 0 AND indexed_bytes >= 0 AND deduped_chunks >= 0 AND embedding_attempts >= 0 AND embedding_skipped >= 0 AND batch_count >= 0),
  CONSTRAINT ingest_jobs_hash_valid CHECK (manifest_hash ~ '^[0-9a-f]{64}$' AND request_hash ~ '^[0-9a-f]{64}$'),
  CONSTRAINT ingest_jobs_workspace_valid CHECK (length(workspace_id) > 0),
  UNIQUE (workspace_id, idempotency_key),
  UNIQUE (workspace_id, repository, manifest_hash)
);

CREATE UNIQUE INDEX IF NOT EXISTS ingest_jobs_active_workspace_repository_idx
  ON fornix.ingest_jobs(workspace_id, repository)
  WHERE status IN ('queued','running');
CREATE INDEX IF NOT EXISTS ingest_jobs_workspace_time_idx
  ON fornix.ingest_jobs(workspace_id, updated_at DESC, id);

CREATE TABLE IF NOT EXISTS fornix.ingest_checkpoints (
  job_id TEXT PRIMARY KEY REFERENCES fornix.ingest_jobs(id),
  workspace_id TEXT NOT NULL,
  next_ordinal INTEGER NOT NULL DEFAULT 0,
  processed_files INTEGER NOT NULL DEFAULT 0,
  removed_files INTEGER NOT NULL DEFAULT 0,
  chunk_count INTEGER NOT NULL DEFAULT 0,
  symbol_count INTEGER NOT NULL DEFAULT 0,
  source_bytes BIGINT NOT NULL DEFAULT 0,
  indexed_bytes BIGINT NOT NULL DEFAULT 0,
  deduped_chunks INTEGER NOT NULL DEFAULT 0,
  batch_count INTEGER NOT NULL DEFAULT 0,
  last_batch_hash TEXT NOT NULL DEFAULT '',
  state_hash TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (workspace_id, job_id)
);

CREATE TABLE IF NOT EXISTS fornix.ingest_files (
  id TEXT PRIMARY KEY,
  job_id TEXT NOT NULL REFERENCES fornix.ingest_jobs(id),
  workspace_id TEXT NOT NULL,
  ordinal INTEGER NOT NULL,
  path TEXT NOT NULL,
  mode INTEGER NOT NULL,
  byte_size BIGINT NOT NULL,
  content_hash TEXT NOT NULL,
  state TEXT NOT NULL,
  supersedes_file_id TEXT,
  chunk_count INTEGER NOT NULL DEFAULT 0,
  symbol_count INTEGER NOT NULL DEFAULT 0,
  indexed_bytes BIGINT NOT NULL DEFAULT 0,
  skipped_reason TEXT NOT NULL DEFAULT '',
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  indexed_at TIMESTAMPTZ,
  CONSTRAINT ingest_files_state_valid CHECK (state IN ('present','removed','skipped','indexed')),
  CONSTRAINT ingest_files_size_valid CHECK (byte_size >= 0 AND chunk_count >= 0 AND symbol_count >= 0 AND indexed_bytes >= 0),
  UNIQUE (job_id, path),
  UNIQUE (job_id, ordinal)
);
CREATE INDEX IF NOT EXISTS ingest_files_job_ordinal_idx ON fornix.ingest_files(job_id, ordinal);
CREATE INDEX IF NOT EXISTS ingest_files_workspace_path_idx ON fornix.ingest_files(workspace_id, path, created_at DESC);

CREATE TABLE IF NOT EXISTS fornix.ingest_symbols (
  id TEXT PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  job_id TEXT NOT NULL REFERENCES fornix.ingest_jobs(id),
  file_id TEXT NOT NULL REFERENCES fornix.ingest_files(id),
  repository TEXT NOT NULL,
  file_path TEXT NOT NULL,
  symbol_name TEXT NOT NULL,
  symbol_kind TEXT NOT NULL,
  language TEXT NOT NULL DEFAULT '',
  line_start INTEGER NOT NULL DEFAULT 0,
  line_end INTEGER NOT NULL DEFAULT 0,
  signature TEXT NOT NULL DEFAULT '',
  docstring TEXT NOT NULL DEFAULT '',
  content_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (job_id, file_id, symbol_name, symbol_kind, line_start, line_end)
);
CREATE INDEX IF NOT EXISTS ingest_symbols_workspace_path_idx ON fornix.ingest_symbols(workspace_id, file_path, line_start, id);

CREATE TABLE IF NOT EXISTS fornix.ingest_lineage (
  id BIGSERIAL PRIMARY KEY,
  workspace_id TEXT NOT NULL,
  job_id TEXT NOT NULL REFERENCES fornix.ingest_jobs(id),
  file_id TEXT NOT NULL REFERENCES fornix.ingest_files(id),
  source_kind TEXT NOT NULL,
  source_id TEXT NOT NULL,
  target_kind TEXT NOT NULL,
  target_id TEXT NOT NULL,
  relation TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (workspace_id, job_id, file_id, source_kind, source_id, target_kind, target_id, relation)
);
CREATE INDEX IF NOT EXISTS ingest_lineage_workspace_job_idx ON fornix.ingest_lineage(workspace_id, job_id, id);
