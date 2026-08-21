package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgvector "github.com/pgvector/pgvector-go"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/ingest"
)

var (
	ErrIngestJobNotFound = errors.New("ingest job not found")
	ErrIngestConflict    = errors.New("ingest job conflicts with existing identity")
	ErrIngestCheckpoint  = errors.New("ingest checkpoint changed; retry the batch")
	ErrIngestFence       = errors.New("ingest task fence is invalid")
	ErrIngestTerminal    = errors.New("ingest job is terminal")
	ErrIngestPathChanged = errors.New("ingest source changed after discovery")
)

type IngestStore struct {
	pool         *pgxpool.Pool
	events       *EventStore
	artifacts    *ArtifactStore
	embedder     func(context.Context, string) ([]float32, error)
	beforeCommit func() error
}

type IngestBatchResult struct {
	Job      contracts.IngestJob     `json:"job"`
	Report   *contracts.IngestReport `json:"report,omitempty"`
	Advanced bool                    `json:"advanced"`
	Deduped  bool                    `json:"deduped"`
}

func NewIngestStore(pool *pgxpool.Pool, events *EventStore, artifacts *ArtifactStore) *IngestStore {
	if events == nil {
		events = NewEventStore(pool)
	}
	return &IngestStore{pool: pool, events: events, artifacts: artifacts}
}

func (s *IngestStore) SetEmbedder(embedder func(context.Context, string) ([]float32, error)) {
	if s != nil {
		s.embedder = embedder
	}
}
func (s *IngestStore) SetFailureHook(hook func() error) {
	if s != nil {
		s.beforeCommit = hook
	}
}

func (s *IngestStore) Submit(ctx context.Context, request contracts.IngestJobRequest, discovered ingest.DiscoveryResult) (contracts.IngestJob, bool, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return contracts.IngestJob{}, false, fmt.Errorf("ingest store is not configured")
	}
	normalized, err := request.Normalize()
	if err != nil {
		return contracts.IngestJob{}, false, err
	}
	if discovered.ManifestHash == "" {
		return contracts.IngestJob{}, false, fmt.Errorf("discovery manifest is required")
	}
	manifestFiles := make([]contracts.IngestFile, len(discovered.Files))
	for i := range discovered.Files {
		manifestFiles[i] = discovered.Files[i].File
	}
	if contracts.ManifestHash(manifestFiles) != discovered.ManifestHash {
		return contracts.IngestJob{}, false, fmt.Errorf("discovery manifest is inconsistent")
	}

	requestHash := normalized.RequestHash()
	jobID := stableIngestID(normalized.WorkspaceID, normalized.Source.Repository, discovered.ManifestHash)
	sourceJSON, _ := json.Marshal(normalized.Source)
	actorJSON, _ := json.Marshal(normalized.Actor)
	taskJSON := nullableJSON(normalized.Task)
	sessionJSON := nullableJSON(normalized.Session)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.IngestJob{}, false, fmt.Errorf("begin ingest submit: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// Serialize submissions for the same workspace/repository. This makes the
	// active-job partial uniqueness constraint a deterministic API boundary.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtext($1))`, normalized.WorkspaceID+"|"+normalized.Source.Repository); err != nil {
		return contracts.IngestJob{}, false, fmt.Errorf("lock ingest identity: %w", err)
	}
	// Resolve existing identities before INSERT so the manifest uniqueness
	// constraint cannot surface as an untyped database error. A matching
	// manifest is only a dedupe when the processing policy is also identical;
	// otherwise the caller must receive a fail-closed conflict.
	if job, readErr := readIngestJobByKeyTx(ctx, tx, normalized.WorkspaceID, normalized.IdempotencyKey, true); readErr == nil {
		if job.RequestHash != requestHash {
			return contracts.IngestJob{}, false, fmt.Errorf("%w: idempotency key", ErrIngestConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.IngestJob{}, false, err
		}
		return job, false, nil
	} else if !errors.Is(readErr, ErrIngestJobNotFound) {
		return contracts.IngestJob{}, false, readErr
	}
	if job, readErr := readIngestJobByManifestTx(ctx, tx, normalized.WorkspaceID, normalized.Source.Repository, discovered.ManifestHash, true); readErr == nil {
		if job.RequestHash != requestHash {
			return contracts.IngestJob{}, false, fmt.Errorf("%w: manifest processing policy", ErrIngestConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.IngestJob{}, false, err
		}
		return job, false, nil
	} else if !errors.Is(readErr, ErrIngestJobNotFound) {
		return contracts.IngestJob{}, false, readErr
	}
	var created bool
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.ingest_jobs(
		 id,workspace_id,repository,source_root,mount_root,manifest_hash,request_hash,idempotency_key,schema_version,
		 actor,task_ref,session_ref,task_owner_id,task_fence,causation_id,correlation_id,source_config,status,batch_size,
		 file_count,skipped_files,source_bytes)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12::jsonb,$13,$14,$15,$16,$17::jsonb,'queued',$18,$19,$20,$21)
		ON CONFLICT (workspace_id,idempotency_key) DO NOTHING
		RETURNING id`, jobID, normalized.WorkspaceID, normalized.Source.Repository, normalized.Source.SourceRoot,
		normalized.Source.MountRoot, discovered.ManifestHash, requestHash, normalized.IdempotencyKey, normalized.SchemaVersion,
		actorJSON, taskJSON, sessionJSON, normalized.TaskOwnerID, int64(normalized.TaskFence), normalized.CausationID,
		normalized.CorrelationID, sourceJSON, normalized.BatchSize, len(discovered.Files), len(discovered.Skipped), discovered.TotalBytes).Scan(&jobID)
	if err == nil {
		created = true
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return contracts.IngestJob{}, false, fmt.Errorf("insert ingest job: %w", err)
	}
	if !created {
		job, readErr := readIngestJobByKeyTx(ctx, tx, normalized.WorkspaceID, normalized.IdempotencyKey, true)
		if readErr == nil {
			if job.RequestHash != requestHash {
				return contracts.IngestJob{}, false, fmt.Errorf("%w: idempotency key", ErrIngestConflict)
			}
			if err := tx.Commit(ctx); err != nil {
				return contracts.IngestJob{}, false, err
			}
			return job, false, nil
		}
		if !errors.Is(readErr, ErrIngestJobNotFound) {
			return contracts.IngestJob{}, false, readErr
		}
		job, readErr = readIngestJobByManifestTx(ctx, tx, normalized.WorkspaceID, normalized.Source.Repository, discovered.ManifestHash, true)
		if readErr != nil {
			return contracts.IngestJob{}, false, fmt.Errorf("read conflicting ingest identity: %w", readErr)
		}
		if job.RequestHash != requestHash {
			return contracts.IngestJob{}, false, fmt.Errorf("%w: manifest processing policy", ErrIngestConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.IngestJob{}, false, err
		}
		return job, false, nil
	}

	previous, err := previousIngestFilesTx(ctx, tx, normalized.WorkspaceID, normalized.Source.Repository)
	if err != nil {
		return contracts.IngestJob{}, false, err
	}
	seen := make(map[string]bool, len(discovered.Files))
	for index := range discovered.Files {
		file := discovered.Files[index].File
		file.ID = stableIngestFileID(jobID, file.Path, file.ContentHash)
		file.JobID, file.WorkspaceID, file.Ordinal = jobID, normalized.WorkspaceID, index
		if old := previous[file.Path]; old.ID != "" && old.ContentHash != file.ContentHash {
			file.SupersedesFileID = old.ID
		}
		if err := insertIngestFileTx(ctx, tx, file); err != nil {
			return contracts.IngestJob{}, false, err
		}
		seen[file.Path] = true
	}
	removed := 0
	removedPaths := make([]string, 0, len(previous))
	for path := range previous {
		if !seen[path] {
			removedPaths = append(removedPaths, path)
		}
	}
	sort.Strings(removedPaths)
	for _, path := range removedPaths {
		old := previous[path]
		removed++
		removedFile := contracts.IngestFile{ID: stableIngestFileID(jobID, path, old.ContentHash), JobID: jobID, WorkspaceID: normalized.WorkspaceID, Ordinal: len(discovered.Files) + removed - 1, Path: path, Mode: old.Mode, ByteSize: old.ByteSize, ContentHash: old.ContentHash, State: contracts.IngestFileRemoved, SupersedesFileID: old.ID}
		if err := insertIngestFileTx(ctx, tx, removedFile); err != nil {
			return contracts.IngestJob{}, false, err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.ingest_jobs SET file_count=$2, skipped_files=$3, removed_files=$4, updated_at=clock_timestamp() WHERE id=$1`, jobID, len(discovered.Files)+removed, len(discovered.Skipped), removed); err != nil {
		return contracts.IngestJob{}, false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.ingest_checkpoints(job_id,workspace_id,state_hash) VALUES($1,$2,$3)`, jobID, normalized.WorkspaceID, checkpointHash(contracts.IngestCheckpoint{JobID: jobID, WorkspaceID: normalized.WorkspaceID})); err != nil {
		return contracts.IngestJob{}, false, fmt.Errorf("insert ingest checkpoint: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.repository_ingests(id,workspace_id,repository,source_root,manifest_hash,request_hash,idempotency_key,status,file_count,byte_count) VALUES($1,$2,$3,$4,$5,$6,$7,'pending',$8,$9) ON CONFLICT (workspace_id,idempotency_key) DO UPDATE SET source_root=EXCLUDED.source_root,manifest_hash=EXCLUDED.manifest_hash,request_hash=EXCLUDED.request_hash,file_count=EXCLUDED.file_count,byte_count=EXCLUDED.byte_count,status='pending',updated_at=clock_timestamp()`, jobID, normalized.WorkspaceID, normalized.Source.Repository, normalized.Source.SourceRoot, discovered.ManifestHash, requestHash, normalized.IdempotencyKey, len(discovered.Files), discovered.TotalBytes); err != nil {
		return contracts.IngestJob{}, false, fmt.Errorf("write ingest compatibility row: %w", err)
	}
	event, err := contracts.NewEvent("ingest.job_submitted", map[string]any{"job_id": jobID, "repository": normalized.Source.Repository, "manifest_hash": discovered.ManifestHash, "file_count": len(discovered.Files), "source_bytes": discovered.TotalBytes})
	if err != nil {
		return contracts.IngestJob{}, false, err
	}
	event.Scope.WorkspaceID, event.Actor, event.Task, event.Session = normalized.WorkspaceID, normalized.Actor, normalized.Task, normalized.Session
	event.CausationID, event.CorrelationID, event.IdempotencyKey = normalized.CausationID, normalized.CorrelationID, "ingest-submit:"+jobID
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.IngestJob{}, false, fmt.Errorf("append ingest submission: %w", err)
	}
	if err := s.commit(ctx, tx); err != nil {
		return contracts.IngestJob{}, false, fmt.Errorf("commit ingest submission: %w", err)
	}
	return s.Get(ctx, normalized.WorkspaceID, jobID)
}

func (s *IngestStore) Get(ctx context.Context, workspaceID, id string) (contracts.IngestJob, bool, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.IngestJob{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	job, err := readIngestJobTx(ctx, tx, strings.TrimSpace(workspaceID), strings.TrimSpace(id), false)
	if err != nil {
		return contracts.IngestJob{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.IngestJob{}, false, err
	}
	return job, true, nil
}

func (s *IngestStore) List(ctx context.Context, workspaceID, cursor string, limit int) (contracts.IngestPage, error) {
	if limit <= 0 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	rows, err := s.pool.Query(ctx, `SELECT id FROM fornix.ingest_jobs WHERE workspace_id=$1 AND id>$2 ORDER BY id LIMIT $3`, strings.TrimSpace(workspaceID), strings.TrimSpace(cursor), limit+1)
	if err != nil {
		return contracts.IngestPage{}, err
	}
	defer rows.Close()
	page := contracts.IngestPage{Items: make([]contracts.IngestJob, 0, limit)}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return contracts.IngestPage{}, err
		}
		job, _, err := s.Get(ctx, workspaceID, id)
		if err != nil {
			return contracts.IngestPage{}, err
		}
		page.Items = append(page.Items, job)
	}
	if err := rows.Err(); err != nil {
		return contracts.IngestPage{}, err
	}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, nil
}

func (s *IngestStore) ProcessBatch(ctx context.Context, request contracts.IngestBatchRequest) (IngestBatchResult, error) {
	if s == nil || s.pool == nil {
		return IngestBatchResult{}, fmt.Errorf("ingest store is not configured")
	}
	if strings.TrimSpace(request.WorkspaceID) == "" || strings.TrimSpace(request.JobID) == "" {
		return IngestBatchResult{}, fmt.Errorf("workspace_id and job_id are required")
	}
	batchSize := request.BatchSize
	if batchSize <= 0 {
		batchSize = contracts.DefaultIngestBatchSize
	}
	if batchSize > contracts.MaxIngestBatchSize {
		batchSize = contracts.MaxIngestBatchSize
	}
	job, _, err := s.Get(ctx, request.WorkspaceID, request.JobID)
	if err != nil {
		return IngestBatchResult{}, err
	}
	if job.Task != nil && (strings.TrimSpace(request.TaskOwnerID) != job.TaskOwnerID || request.TaskFence != job.TaskFence) {
		return IngestBatchResult{}, ErrIngestFence
	}
	if job.Status == contracts.IngestSucceeded {
		return IngestBatchResult{Job: job, Report: job.Report, Deduped: true}, nil
	}
	if job.Status == contracts.IngestCancelled || job.Status == contracts.IngestFailed {
		return IngestBatchResult{Job: job, Report: job.Report}, ErrIngestTerminal
	}
	files, err := s.batchFiles(ctx, job, batchSize)
	if err != nil {
		return IngestBatchResult{}, err
	}
	if len(files) == 0 {
		return s.finalizeEmpty(ctx, job, request.Actor)
	}
	prepared, err := s.prepareFiles(ctx, job, files)
	if err != nil {
		return IngestBatchResult{}, fmt.Errorf("%w: %v", ErrIngestPathChanged, err)
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return IngestBatchResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	lockedJob, err := readIngestJobTx(ctx, tx, request.WorkspaceID, request.JobID, true)
	if err != nil {
		return IngestBatchResult{}, err
	}
	if lockedJob.Checkpoint.NextOrdinal != job.Checkpoint.NextOrdinal {
		return IngestBatchResult{}, ErrIngestCheckpoint
	}
	if lockedJob.Status == contracts.IngestCancelled || lockedJob.Status == contracts.IngestFailed {
		return IngestBatchResult{Job: lockedJob}, ErrIngestTerminal
	}
	if lockedJob.Task != nil && (strings.TrimSpace(request.TaskOwnerID) != lockedJob.TaskOwnerID || request.TaskFence != lockedJob.TaskFence) {
		return IngestBatchResult{}, ErrIngestFence
	}
	if err := authorizeIngestTaskTx(ctx, tx, lockedJob); err != nil {
		return IngestBatchResult{}, err
	}
	if lockedJob.Status == contracts.IngestQueued {
		if _, err := tx.Exec(ctx, `UPDATE fornix.ingest_jobs SET status='running',started_at=COALESCE(started_at,clock_timestamp()),updated_at=clock_timestamp() WHERE id=$1`, lockedJob.ID); err != nil {
			return IngestBatchResult{}, err
		}
	}

	batchHash := preparedHash(prepared)
	batchCounters := batchStats(prepared)
	for _, file := range prepared {
		deduped, err := s.writePreparedFileTx(ctx, tx, lockedJob, file)
		if err != nil {
			return IngestBatchResult{}, err
		}
		batchCounters.deduped += deduped
	}
	next := lockedJob.Checkpoint
	next.NextOrdinal += len(prepared)
	next.ProcessedFiles += batchCounters.processed
	next.RemovedFiles += batchCounters.removed
	next.ChunkCount += batchCounters.chunks
	next.SymbolCount += batchCounters.symbols
	next.SourceBytes += batchCounters.sourceBytes
	next.IndexedBytes += batchCounters.indexedBytes
	next.DedupedChunks += batchCounters.deduped
	next.BatchCount++
	next.LastBatchHash = batchHash
	next.StateHash = checkpointHash(next)
	if _, err := tx.Exec(ctx, `UPDATE fornix.ingest_checkpoints SET next_ordinal=$2,processed_files=$3,removed_files=$4,chunk_count=$5,symbol_count=$6,source_bytes=$7,indexed_bytes=$8,deduped_chunks=$9,batch_count=$10,last_batch_hash=$11,state_hash=$12,updated_at=clock_timestamp() WHERE job_id=$1`, lockedJob.ID, next.NextOrdinal, next.ProcessedFiles, next.RemovedFiles, next.ChunkCount, next.SymbolCount, next.SourceBytes, next.IndexedBytes, next.DedupedChunks, next.BatchCount, next.LastBatchHash, next.StateHash); err != nil {
		return IngestBatchResult{}, err
	}
	lockedJob.EmbeddingAttempts += batchCounters.embeddingAttempts
	lockedJob.EmbeddingSkipped += batchCounters.embeddingSkipped
	if _, err := tx.Exec(ctx, `UPDATE fornix.ingest_jobs SET status='running',processed_files=$2,removed_files=$3,chunk_count=$4,symbol_count=$5,source_bytes=$6,indexed_bytes=$7,deduped_chunks=$8,embedding_attempts=$9,embedding_skipped=$10,batch_count=$11,updated_at=clock_timestamp() WHERE id=$1`, lockedJob.ID, next.ProcessedFiles, next.RemovedFiles, next.ChunkCount, next.SymbolCount, next.SourceBytes, next.IndexedBytes, next.DedupedChunks, lockedJob.EmbeddingAttempts, lockedJob.EmbeddingSkipped, next.BatchCount); err != nil {
		return IngestBatchResult{}, err
	}
	event, err := contracts.NewEvent("ingest.batch_committed", map[string]any{"job_id": lockedJob.ID, "next_ordinal": next.NextOrdinal, "batch_hash": batchHash, "file_count": len(prepared), "chunk_count": batchCounters.chunks, "symbol_count": batchCounters.symbols})
	if err != nil {
		return IngestBatchResult{}, err
	}
	event.Scope.WorkspaceID, event.Actor, event.Task, event.Session = lockedJob.WorkspaceID, request.Actor, lockedJob.Task, lockedJob.Session
	event.IdempotencyKey = fmt.Sprintf("ingest-batch:%s:%d", lockedJob.ID, lockedJob.Checkpoint.NextOrdinal)
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return IngestBatchResult{}, err
	}
	final := next.NextOrdinal >= lockedJob.FileCount
	if final {
		report := reportFrom(lockedJob, next, contracts.IngestSucceeded, "")
		report.ReportHash = report.StableHash()
		raw, _ := json.Marshal(report)
		var artifactID *int64
		if len(raw) > contracts.MaxIngestReportBytes && s.artifacts != nil {
			put, putErr := s.artifacts.PutTx(ctx, tx, ArtifactPutInput{WorkspaceID: lockedJob.WorkspaceID, Kind: "ingest-report", MediaType: "application/json", Raw: raw, Manifest: contracts.ArtifactManifest{SchemaVersion: contracts.ArtifactSchemaVersion, Gist: "bounded repository ingestion report", Detail: "durable deterministic ingestion report", Metadata: map[string]string{"job_id": lockedJob.ID, "manifest_hash": lockedJob.ManifestHash}}, SourceKind: "ingest_job", SourceID: lockedJob.ID, Role: "report", IdempotencyKey: "ingest-report:" + lockedJob.ID, Actor: request.Actor})
			if putErr != nil {
				return IngestBatchResult{}, putErr
			}
			artifactID = &put.Artifact.ID
		}
		if _, err := tx.Exec(ctx, `UPDATE fornix.ingest_jobs SET status='succeeded',report=$2::jsonb,report_hash=$3,report_artifact_id=$4,completed_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=$1`, lockedJob.ID, raw, report.ReportHash, artifactID); err != nil {
			return IngestBatchResult{}, err
		}
		if _, err := tx.Exec(ctx, `UPDATE fornix.repository_ingests SET status='succeeded',file_count=$2,chunk_count=$3,symbol_count=$4,byte_count=$5,last_error='',updated_at=clock_timestamp() WHERE workspace_id=$1 AND id=$6`, lockedJob.WorkspaceID, next.ProcessedFiles, next.ChunkCount, next.SymbolCount, next.SourceBytes, lockedJob.ID); err != nil {
			return IngestBatchResult{}, err
		}
		event, err := contracts.NewEvent("ingest.job_completed", map[string]any{"job_id": lockedJob.ID, "manifest_hash": lockedJob.ManifestHash, "report_hash": report.ReportHash, "report_artifact_id": artifactID})
		if err != nil {
			return IngestBatchResult{}, err
		}
		event.Scope.WorkspaceID, event.Actor, event.IdempotencyKey = lockedJob.WorkspaceID, request.Actor, "ingest-complete:"+lockedJob.ID
		if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
			return IngestBatchResult{}, err
		}
	}
	if err := s.commit(ctx, tx); err != nil {
		return IngestBatchResult{}, err
	}
	resultJob, _, err := s.Get(ctx, request.WorkspaceID, request.JobID)
	if err != nil {
		return IngestBatchResult{}, err
	}
	return IngestBatchResult{Job: resultJob, Report: resultJob.Report, Advanced: true}, nil
}

func (s *IngestStore) Cancel(ctx context.Context, workspaceID, jobID string, actor contracts.ActorRef) (contracts.IngestJob, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.IngestJob{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	job, err := readIngestJobTx(ctx, tx, workspaceID, jobID, true)
	if err != nil {
		return contracts.IngestJob{}, err
	}
	if !contracts.IsIngestTerminal(job.Status) {
		if _, err := tx.Exec(ctx, `UPDATE fornix.ingest_jobs SET status='cancelled',last_error='cancelled by operator',completed_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=$1`, jobID); err != nil {
			return contracts.IngestJob{}, err
		}
		event, eventErr := contracts.NewEvent("ingest.job_cancelled", map[string]any{"job_id": jobID})
		if eventErr != nil {
			return contracts.IngestJob{}, eventErr
		}
		event.Scope.WorkspaceID, event.Actor, event.IdempotencyKey = workspaceID, actor, "ingest-cancel:"+jobID
		if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
			return contracts.IngestJob{}, err
		}
	}
	if err := s.commit(ctx, tx); err != nil {
		return contracts.IngestJob{}, err
	}
	result, _, err := s.Get(ctx, workspaceID, jobID)
	return result, err
}

func (s *IngestStore) commit(ctx context.Context, tx pgx.Tx) error {
	if s.beforeCommit != nil {
		if err := s.beforeCommit(); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}

type preparedFile struct {
	file              contracts.IngestFile
	chunks            []preparedChunk
	symbols           []contracts.IngestSymbol
	sourceBytes       int64
	embeddingAttempts int
	embeddingSkipped  int
}
type preparedChunk struct {
	path, sourceRange, content, hash string
	embedding                        []float32
}
type batchStatsValue struct {
	processed, removed, chunks, symbols int
	sourceBytes, indexedBytes           int64
	deduped                             int
	embeddingAttempts, embeddingSkipped int
}

func (s *IngestStore) batchFiles(ctx context.Context, job contracts.IngestJob, limit int) ([]contracts.IngestFile, error) {
	rows, err := s.pool.Query(ctx, `SELECT id,job_id,workspace_id,ordinal,path,mode,byte_size,content_hash,state,supersedes_file_id,chunk_count,symbol_count,indexed_bytes,skipped_reason,created_at,indexed_at FROM fornix.ingest_files WHERE job_id=$1 AND ordinal >= $2 ORDER BY ordinal LIMIT $3`, job.ID, job.Checkpoint.NextOrdinal, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	files := make([]contracts.IngestFile, 0, limit)
	for rows.Next() {
		file, err := scanIngestFile(rows)
		if err != nil {
			return nil, err
		}
		files = append(files, file)
	}
	return files, rows.Err()
}

func (s *IngestStore) prepareFiles(ctx context.Context, job contracts.IngestJob, files []contracts.IngestFile) ([]preparedFile, error) {
	if err := ingest.ValidateConfiguredRoot(job.SourceRoot, job.MountRoot); err != nil {
		return nil, err
	}
	prepared := make([]preparedFile, 0, len(files))
	embeddingChunks, embeddingBytes := 0, int64(0)
	for _, file := range files {
		item := preparedFile{file: file, sourceBytes: file.ByteSize}
		if file.State == contracts.IngestFileRemoved {
			prepared = append(prepared, item)
			continue
		}
		data, err := ingest.ReadAndVerify(job.SourceRoot, file)
		if err != nil {
			return nil, err
		}
		windows, err := ingest.Chunk(data, job.Source.ChunkBytes, job.Source.ChunkOverlap)
		if err != nil {
			return nil, err
		}
		for _, window := range windows {
			rangeValue := fmt.Sprintf("%d-%d", window.LineStart, window.LineEnd)
			h := sha256.Sum256([]byte(window.Text))
			chunk := preparedChunk{path: file.Path, sourceRange: rangeValue, content: window.Text, hash: hex.EncodeToString(h[:])}
			if job.Source.Embedding.Enabled {
				withinBudget := (job.Source.Embedding.MaxChunks <= 0 || embeddingChunks < job.Source.Embedding.MaxChunks) && (job.Source.Embedding.MaxBytes <= 0 || embeddingBytes+int64(len(window.Text)) <= job.Source.Embedding.MaxBytes)
				if s.embedder == nil || !withinBudget {
					item.embeddingSkipped++
				} else {
					item.embeddingAttempts++
					embeddingChunks++
					embeddingBytes += int64(len(window.Text))
					vector, embedErr := s.embedder(ctx, window.Text)
					if embedErr == nil && len(vector) == 768 {
						chunk.embedding = vector
					} else {
						item.embeddingSkipped++
					}
				}
			}
			item.chunks = append(item.chunks, chunk)
		}
		if job.Source.ExtractSymbols {
			item.symbols = ingest.Symbols(file.Path, data)
		}
		prepared = append(prepared, item)
	}
	return prepared, nil
}

func (s *IngestStore) writePreparedFileTx(ctx context.Context, tx pgx.Tx, job contracts.IngestJob, prepared preparedFile) (int, error) {
	file := prepared.file
	if _, err := tx.Exec(ctx, `UPDATE fornix.ingest_files SET state=$2,chunk_count=$3,symbol_count=$4,indexed_bytes=$5,indexed_at=clock_timestamp() WHERE job_id=$1 AND id=$6`, job.ID, map[bool]string{true: contracts.IngestFileRemoved, false: contracts.IngestFileIndexed}[file.State == contracts.IngestFileRemoved], len(prepared.chunks), len(prepared.symbols), prepared.sourceBytes, file.ID); err != nil {
		return 0, err
	}
	if file.State == contracts.IngestFileRemoved {
		_, err := tx.Exec(ctx, `UPDATE fornix.symbols SET deleted_at=clock_timestamp() WHERE workspace_id=$1 AND repo=$2 AND file_path=$3 AND deleted_at IS NULL`, job.WorkspaceID, job.Repository, file.Path)
		return 0, err
	}
	dedupedCount := 0
	for _, chunk := range prepared.chunks {
		metadata, _ := json.Marshal(map[string]string{"ingest_job_id": job.ID, "ingest_file_id": file.ID, "content_hash": file.ContentHash})
		var id int64
		var inserted bool
		if len(chunk.embedding) == 768 {
			err := tx.QueryRow(ctx, `INSERT INTO fornix.chunks(workspace_id,source_path,source_range,content,content_sha256,metadata,embedding) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7) ON CONFLICT(workspace_id,content_sha256) DO NOTHING RETURNING id,(xmax=0)`, job.WorkspaceID, chunk.path, chunk.sourceRange, chunk.content, chunk.hash, metadata, pgvector.NewVector(chunk.embedding)).Scan(&id, &inserted)
			if errors.Is(err, pgx.ErrNoRows) {
				dedupedCount++
				if err := tx.QueryRow(ctx, `SELECT id FROM fornix.chunks WHERE workspace_id=$1 AND content_sha256=$2`, job.WorkspaceID, chunk.hash).Scan(&id); err != nil {
					return 0, err
				}
			} else if err != nil {
				return 0, err
			}
		} else {
			err := tx.QueryRow(ctx, `INSERT INTO fornix.chunks(workspace_id,source_path,source_range,content,content_sha256,metadata) VALUES($1,$2,$3,$4,$5,$6::jsonb) ON CONFLICT(workspace_id,content_sha256) DO NOTHING RETURNING id,(xmax=0)`, job.WorkspaceID, chunk.path, chunk.sourceRange, chunk.content, chunk.hash, metadata).Scan(&id, &inserted)
			if errors.Is(err, pgx.ErrNoRows) {
				dedupedCount++
				if err := tx.QueryRow(ctx, `SELECT id FROM fornix.chunks WHERE workspace_id=$1 AND content_sha256=$2`, job.WorkspaceID, chunk.hash).Scan(&id); err != nil {
					return 0, err
				}
			} else if err != nil {
				return 0, err
			}
		}
		if _, err := tx.Exec(ctx, `INSERT INTO fornix.ingest_lineage(workspace_id,job_id,file_id,source_kind,source_id,target_kind,target_id,relation,content_hash) VALUES($1,$2,$3,'repository_file',$4,'chunk',$5,'indexed',$6) ON CONFLICT DO NOTHING`, job.WorkspaceID, job.ID, file.ID, file.Path, strconv.FormatInt(id, 10), chunk.hash); err != nil {
			return 0, err
		}
	}
	if len(prepared.symbols) > 0 {
		if _, err := tx.Exec(ctx, `UPDATE fornix.symbols SET deleted_at=clock_timestamp() WHERE workspace_id=$1 AND repo=$2 AND file_path=$3 AND deleted_at IS NULL`, job.WorkspaceID, job.Repository, file.Path); err != nil {
			return 0, err
		}
	}
	for index, symbol := range prepared.symbols {
		symbol.ID = stableIngestSymbolID(job.ID, file.ID, symbol.SymbolName, symbol.SymbolKind, symbol.LineStart)
		symbol.WorkspaceID, symbol.JobID, symbol.FileID, symbol.Repository = job.WorkspaceID, job.ID, file.ID, job.Repository
		if _, err := tx.Exec(ctx, `INSERT INTO fornix.ingest_symbols(id,workspace_id,job_id,file_id,repository,file_path,symbol_name,symbol_kind,language,line_start,line_end,signature,docstring,content_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT DO NOTHING`, symbol.ID, symbol.WorkspaceID, symbol.JobID, symbol.FileID, symbol.Repository, symbol.FilePath, symbol.SymbolName, symbol.SymbolKind, symbol.Language, symbol.LineStart, symbol.LineEnd, symbol.Signature, symbol.Docstring, symbol.ContentHash); err != nil {
			return 0, err
		}
		var currentID int64
		if err := tx.QueryRow(ctx, `INSERT INTO fornix.symbols(workspace_id,repo,file_path,symbol_name,symbol_kind,language,line_start,line_end,signature,docstring,sha256,deleted_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NULL) ON CONFLICT(workspace_id,repo,file_path,symbol_name,symbol_kind) DO UPDATE SET language=EXCLUDED.language,line_start=EXCLUDED.line_start,line_end=EXCLUDED.line_end,signature=EXCLUDED.signature,docstring=EXCLUDED.docstring,sha256=EXCLUDED.sha256,updated_at=clock_timestamp(),deleted_at=NULL RETURNING id`, job.WorkspaceID, job.Repository, symbol.FilePath, symbol.SymbolName, symbol.SymbolKind, symbol.Language, symbol.LineStart, symbol.LineEnd, symbol.Signature, symbol.Docstring, symbol.ContentHash).Scan(&currentID); err != nil {
			return 0, err
		}
		if _, err := tx.Exec(ctx, `INSERT INTO fornix.ingest_lineage(workspace_id,job_id,file_id,source_kind,source_id,target_kind,target_id,relation,content_hash) VALUES($1,$2,$3,'repository_file',$4,'symbol',$5,'indexed',$6) ON CONFLICT DO NOTHING`, job.WorkspaceID, job.ID, file.ID, file.Path, strconv.FormatInt(currentID, 10), symbol.ContentHash); err != nil {
			return 0, err
		}
		_ = index
	}
	return dedupedCount, nil
}

func (s *IngestStore) finalizeEmpty(ctx context.Context, job contracts.IngestJob, actor contracts.ActorRef) (IngestBatchResult, error) {
	if job.FileCount > job.Checkpoint.NextOrdinal {
		return IngestBatchResult{}, ErrIngestCheckpoint
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return IngestBatchResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	locked, err := readIngestJobTx(ctx, tx, job.WorkspaceID, job.ID, true)
	if err != nil {
		return IngestBatchResult{}, err
	}
	if locked.Status == contracts.IngestSucceeded {
		if err := tx.Commit(ctx); err != nil {
			return IngestBatchResult{}, err
		}
		return IngestBatchResult{Job: locked, Deduped: true}, nil
	}
	report := reportFrom(locked, locked.Checkpoint, contracts.IngestSucceeded, "")
	report.ReportHash = report.StableHash()
	raw, _ := json.Marshal(report)
	if _, err := tx.Exec(ctx, `UPDATE fornix.ingest_jobs SET status='succeeded',report=$2::jsonb,report_hash=$3,completed_at=clock_timestamp(),updated_at=clock_timestamp() WHERE id=$1`, job.ID, raw, report.ReportHash); err != nil {
		return IngestBatchResult{}, err
	}
	event, _ := contracts.NewEvent("ingest.job_completed", map[string]any{"job_id": job.ID, "report_hash": report.ReportHash})
	event.Scope.WorkspaceID, event.Actor, event.IdempotencyKey = job.WorkspaceID, actor, "ingest-complete:"+job.ID
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return IngestBatchResult{}, err
	}
	if err := s.commit(ctx, tx); err != nil {
		return IngestBatchResult{}, err
	}
	result, _, err := s.Get(ctx, job.WorkspaceID, job.ID)
	return IngestBatchResult{Job: result, Report: result.Report, Advanced: true}, err
}

func authorizeIngestTaskTx(ctx context.Context, tx pgx.Tx, job contracts.IngestJob) error {
	if job.Task == nil {
		return nil
	}
	taskID, err := strconv.ParseInt(job.Task.ID, 10, 64)
	if err != nil || job.TaskOwnerID == "" || job.TaskFence == 0 {
		return ErrIngestFence
	}
	task, err := readTaskTx(ctx, tx, job.WorkspaceID, taskID, true)
	if err != nil {
		return err
	}
	if _, err := authorizeTaskLeaseTx(ctx, tx, task, job.TaskOwnerID, job.TaskFence); err != nil {
		return fmt.Errorf("%w: %v", ErrIngestFence, err)
	}
	return nil
}

func readIngestJobTx(ctx context.Context, tx pgx.Tx, workspaceID, id string, lock bool) (contracts.IngestJob, error) {
	query := `SELECT id,workspace_id,repository,source_root,mount_root,manifest_hash,request_hash,idempotency_key,schema_version,actor,task_ref,session_ref,task_owner_id,task_fence,causation_id,correlation_id,source_config,status,batch_size,file_count,skipped_files,processed_files,removed_files,chunk_count,symbol_count,source_bytes,indexed_bytes,deduped_chunks,embedding_attempts,embedding_skipped,batch_count,report,report_hash,report_artifact_id,last_error,created_at,updated_at,started_at,completed_at FROM fornix.ingest_jobs WHERE workspace_id=$1 AND id=$2`
	if lock {
		query += " FOR UPDATE"
	}
	var job contracts.IngestJob
	var actorRaw, taskRaw, sessionRaw, sourceRaw, reportRaw []byte
	var fence int64
	var reportHash string
	var artifactID *int64
	err := tx.QueryRow(ctx, query, workspaceID, id).Scan(&job.ID, &job.WorkspaceID, &job.Repository, &job.SourceRoot, &job.MountRoot, &job.ManifestHash, &job.RequestHash, &job.IdempotencyKey, &job.SchemaVersion, &actorRaw, &taskRaw, &sessionRaw, &job.TaskOwnerID, &fence, &job.CausationID, &job.CorrelationID, &sourceRaw, &job.Status, &job.BatchSize, &job.FileCount, &job.SkippedFiles, &job.ProcessedFiles, &job.RemovedFiles, &job.ChunkCount, &job.SymbolCount, &job.SourceBytes, &job.IndexedBytes, &job.DedupedChunks, &job.EmbeddingAttempts, &job.EmbeddingSkipped, &job.BatchCount, &reportRaw, &reportHash, &artifactID, &job.LastError, &job.CreatedAt, &job.UpdatedAt, &job.StartedAt, &job.CompletedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.IngestJob{}, ErrIngestJobNotFound
	}
	if err != nil {
		return contracts.IngestJob{}, err
	}
	job.TaskFence = uint64(fence)
	job.ReportArtifactID = artifactID
	job.Report = nil
	job.Report = decodeReport(reportRaw)
	_ = json.Unmarshal(actorRaw, &job.Actor)
	job.Task = ingestDecodeEntityRef(taskRaw)
	job.Session = ingestDecodeEntityRef(sessionRaw)
	if err := json.Unmarshal(sourceRaw, &job.Source); err != nil {
		return contracts.IngestJob{}, err
	}
	job.ReportHash = reportHash
	checkpoint, err := readCheckpointTx(ctx, tx, workspaceID, id, lock)
	if err != nil {
		return contracts.IngestJob{}, err
	}
	job.Checkpoint = checkpoint
	return job, nil
}

func readIngestJobByKeyTx(ctx context.Context, tx pgx.Tx, workspaceID, key string, lock bool) (contracts.IngestJob, error) {
	var id string
	query := `SELECT id FROM fornix.ingest_jobs WHERE workspace_id=$1 AND idempotency_key=$2`
	if lock {
		query += " FOR UPDATE"
	}
	if err := tx.QueryRow(ctx, query, workspaceID, key).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.IngestJob{}, ErrIngestJobNotFound
		}
		return contracts.IngestJob{}, err
	}
	return readIngestJobTx(ctx, tx, workspaceID, id, lock)
}
func readIngestJobByManifestTx(ctx context.Context, tx pgx.Tx, workspaceID, repository, manifest string, lock bool) (contracts.IngestJob, error) {
	var id string
	query := `SELECT id FROM fornix.ingest_jobs WHERE workspace_id=$1 AND repository=$2 AND manifest_hash=$3`
	if lock {
		query += " FOR UPDATE"
	}
	if err := tx.QueryRow(ctx, query, workspaceID, repository, manifest).Scan(&id); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.IngestJob{}, ErrIngestJobNotFound
		}
		return contracts.IngestJob{}, err
	}
	return readIngestJobTx(ctx, tx, workspaceID, id, lock)
}

func readCheckpointTx(ctx context.Context, tx pgx.Tx, workspaceID, jobID string, lock bool) (contracts.IngestCheckpoint, error) {
	query := `SELECT job_id,workspace_id,next_ordinal,processed_files,removed_files,chunk_count,symbol_count,source_bytes,indexed_bytes,deduped_chunks,batch_count,last_batch_hash,state_hash,updated_at FROM fornix.ingest_checkpoints WHERE workspace_id=$1 AND job_id=$2`
	if lock {
		query += " FOR UPDATE"
	}
	var c contracts.IngestCheckpoint
	err := tx.QueryRow(ctx, query, workspaceID, jobID).Scan(&c.JobID, &c.WorkspaceID, &c.NextOrdinal, &c.ProcessedFiles, &c.RemovedFiles, &c.ChunkCount, &c.SymbolCount, &c.SourceBytes, &c.IndexedBytes, &c.DedupedChunks, &c.BatchCount, &c.LastBatchHash, &c.StateHash, &c.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.IngestCheckpoint{}, ErrIngestJobNotFound
	}
	return c, err
}

func scanIngestFile(row interface{ Scan(...any) error }) (contracts.IngestFile, error) {
	var f contracts.IngestFile
	var supersedes *string
	if err := row.Scan(&f.ID, &f.JobID, &f.WorkspaceID, &f.Ordinal, &f.Path, &f.Mode, &f.ByteSize, &f.ContentHash, &f.State, &supersedes, &f.ChunkCount, &f.SymbolCount, &f.IndexedBytes, &f.SkippedReason, &f.CreatedAt, &f.IndexedAt); err != nil {
		return contracts.IngestFile{}, err
	}
	if supersedes != nil {
		f.SupersedesFileID = *supersedes
	}
	return f, nil
}
func insertIngestFileTx(ctx context.Context, tx pgx.Tx, f contracts.IngestFile) error {
	_, err := tx.Exec(ctx, `INSERT INTO fornix.ingest_files(id,job_id,workspace_id,ordinal,path,mode,byte_size,content_hash,state,supersedes_file_id) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,NULLIF($10,''))`, f.ID, f.JobID, f.WorkspaceID, f.Ordinal, f.Path, f.Mode, f.ByteSize, f.ContentHash, f.State, f.SupersedesFileID)
	return err
}
func previousIngestFilesTx(ctx context.Context, tx pgx.Tx, workspaceID, repository string) (map[string]contracts.IngestFile, error) {
	var jobID string
	if err := tx.QueryRow(ctx, `SELECT id FROM fornix.ingest_jobs WHERE workspace_id=$1 AND repository=$2 AND status='succeeded' ORDER BY completed_at DESC NULLS LAST,id DESC LIMIT 1`, workspaceID, repository).Scan(&jobID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return map[string]contracts.IngestFile{}, nil
		}
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT id,job_id,workspace_id,ordinal,path,mode,byte_size,content_hash,state,supersedes_file_id,chunk_count,symbol_count,indexed_bytes,skipped_reason,created_at,indexed_at FROM fornix.ingest_files WHERE job_id=$1`, jobID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := map[string]contracts.IngestFile{}
	for rows.Next() {
		f, err := scanIngestFile(rows)
		if err != nil {
			return nil, err
		}
		result[f.Path] = f
	}
	return result, rows.Err()
}

func reportFrom(job contracts.IngestJob, c contracts.IngestCheckpoint, status, lastError string) contracts.IngestReport {
	return contracts.IngestReport{SchemaVersion: contracts.IngestSchemaVersion, JobID: job.ID, WorkspaceID: job.WorkspaceID, Repository: job.Repository, SourceRoot: job.SourceRoot, ManifestHash: job.ManifestHash, Status: status, FileCount: job.FileCount, SkippedFiles: job.SkippedFiles, ProcessedFiles: c.ProcessedFiles, RemovedFiles: c.RemovedFiles, ChunkCount: c.ChunkCount, SymbolCount: c.SymbolCount, SourceBytes: c.SourceBytes, IndexedBytes: c.IndexedBytes, DedupedChunks: c.DedupedChunks, BatchCount: c.BatchCount, EmbeddingAttempts: job.EmbeddingAttempts, EmbeddingSkipped: job.EmbeddingSkipped, LastError: lastError}
}
func batchStats(files []preparedFile) batchStatsValue {
	var result batchStatsValue
	for _, f := range files {
		if f.file.State == contracts.IngestFileRemoved {
			result.removed++
			continue
		}
		result.processed++
		result.chunks += len(f.chunks)
		result.symbols += len(f.symbols)
		result.embeddingAttempts += f.embeddingAttempts
		result.embeddingSkipped += f.embeddingSkipped
		result.sourceBytes += f.sourceBytes
		result.indexedBytes += f.sourceBytes
	}
	return result
}
func preparedHash(files []preparedFile) string {
	h := sha256.New()
	for _, f := range files {
		fmt.Fprintf(h, "%d\x00%s\x00%s\x00", f.file.Ordinal, f.file.Path, f.file.ContentHash)
		for _, c := range f.chunks {
			h.Write([]byte(c.hash))
			h.Write([]byte{0})
		}
		for _, sym := range f.symbols {
			h.Write([]byte(sym.ContentHash))
			h.Write([]byte{0})
		}
	}
	return hex.EncodeToString(h.Sum(nil))
}
func checkpointHash(c contracts.IngestCheckpoint) string {
	raw, _ := json.Marshal(struct {
		JobID                                     string `json:"job_id"`
		WorkspaceID                               string `json:"workspace_id"`
		Next, Processed, Removed, Chunks, Symbols int
		Source, Indexed                           int64
		Deduped, Batches                          int
		Last                                      string `json:"last"`
	}{c.JobID, c.WorkspaceID, c.NextOrdinal, c.ProcessedFiles, c.RemovedFiles, c.ChunkCount, c.SymbolCount, c.SourceBytes, c.IndexedBytes, c.DedupedChunks, c.BatchCount, c.LastBatchHash})
	d := sha256.Sum256(raw)
	return hex.EncodeToString(d[:])
}
func stableIngestID(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return "ingest_" + hex.EncodeToString(h.Sum(nil)[:16])
}
func stableIngestFileID(jobID, path, hash string) string { return stableIngestID(jobID, path, hash) }
func stableIngestSymbolID(parts ...any) string {
	values := make([]string, len(parts))
	for i, p := range parts {
		values[i] = fmt.Sprint(p)
	}
	return stableIngestID(values...)
}
func nullableJSON(value any) []byte {
	if value == nil {
		return nil
	}
	raw, _ := json.Marshal(value)
	return raw
}
func ingestDecodeEntityRef(raw []byte) *contracts.EntityRef {
	if len(raw) == 0 {
		return nil
	}
	var ref contracts.EntityRef
	if json.Unmarshal(raw, &ref) != nil || ref.ID == "" {
		return nil
	}
	return &ref
}
func decodeReport(raw []byte) *contracts.IngestReport {
	if len(raw) == 0 {
		return nil
	}
	var report contracts.IngestReport
	if json.Unmarshal(raw, &report) != nil {
		return nil
	}
	return &report
}
