ALTER TABLE fornix.ingest_jobs
  ADD COLUMN IF NOT EXISTS skipped_files INTEGER NOT NULL DEFAULT 0;

ALTER TABLE fornix.ingest_jobs
  DROP CONSTRAINT IF EXISTS ingest_jobs_counts_valid;

ALTER TABLE fornix.ingest_jobs
  ADD CONSTRAINT ingest_jobs_counts_valid CHECK (
    batch_size > 0 AND file_count >= 0 AND processed_files >= 0 AND
    removed_files >= 0 AND skipped_files >= 0 AND chunk_count >= 0 AND
    symbol_count >= 0 AND source_bytes >= 0 AND indexed_bytes >= 0 AND
    deduped_chunks >= 0 AND embedding_attempts >= 0 AND
    embedding_skipped >= 0 AND batch_count >= 0
  );
