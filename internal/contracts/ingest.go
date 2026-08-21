package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const IngestSchemaVersion = 1

const (
	IngestQueued    = "queued"
	IngestCancelled = "cancelled"

	IngestFilePresent = "present"
	IngestFileRemoved = "removed"
	IngestFileSkipped = "skipped"
	IngestFileIndexed = "indexed"

	DefaultIngestBatchSize                = 32
	MaxIngestBatchSize                    = 256
	DefaultIngestMaxFiles                 = 10000
	DefaultIngestMaxFileBytes       int64 = 64 << 20
	DefaultIngestMaxTotalBytes      int64 = 512 << 20
	DefaultIngestChunkBytes               = 4096
	DefaultIngestChunkOverlap             = 400
	DefaultIngestEmbeddingMaxChunks       = 64
	DefaultIngestEmbeddingMaxBytes  int64 = 4 << 20
	MaxIngestIgnoreRules                  = 128
	MaxIngestReportBytes                  = 64 << 10
)

// RepositorySource is the caller-visible source policy. MountRoot is filled
// by the server from the authenticated workspace and is never accepted from
// an untrusted request.
type RepositorySource struct {
	Repository     string          `json:"repository"`
	SourceRoot     string          `json:"source_root"`
	MountRoot      string          `json:"-"`
	IgnoreRules    []string        `json:"ignore_rules,omitempty"`
	MaxFiles       int             `json:"max_files,omitempty"`
	MaxFileBytes   int64           `json:"max_file_bytes,omitempty"`
	MaxTotalBytes  int64           `json:"max_total_bytes,omitempty"`
	ChunkBytes     int             `json:"chunk_bytes,omitempty"`
	ChunkOverlap   int             `json:"chunk_overlap,omitempty"`
	ExtractSymbols bool            `json:"extract_symbols,omitempty"`
	Embedding      EmbeddingPolicy `json:"embedding,omitempty"`
}

type EmbeddingPolicy struct {
	Enabled         bool  `json:"enabled"`
	MaxChunks       int   `json:"max_chunks,omitempty"`
	MaxBytes        int64 `json:"max_bytes,omitempty"`
	RequireProvider bool  `json:"require_provider,omitempty"`
}

type RepositorySourceRequest struct {
	WorkspaceID    string          `json:"workspace_id,omitempty"`
	Repository     string          `json:"repository"`
	SourceRoot     string          `json:"source_root"`
	IgnoreRules    []string        `json:"ignore_rules,omitempty"`
	MaxFiles       int             `json:"max_files,omitempty"`
	MaxFileBytes   int64           `json:"max_file_bytes,omitempty"`
	MaxTotalBytes  int64           `json:"max_total_bytes,omitempty"`
	ChunkBytes     int             `json:"chunk_bytes,omitempty"`
	ChunkOverlap   int             `json:"chunk_overlap,omitempty"`
	ExtractSymbols bool            `json:"extract_symbols,omitempty"`
	Embedding      EmbeddingPolicy `json:"embedding,omitempty"`
}

type IngestJobRequest struct {
	SchemaVersion  int              `json:"schema_version,omitempty"`
	ID             string           `json:"id,omitempty"`
	RequestID      string           `json:"request_id,omitempty"`
	IdempotencyKey string           `json:"idempotency_key"`
	CausationID    string           `json:"causation_id,omitempty"`
	CorrelationID  string           `json:"correlation_id,omitempty"`
	WorkspaceID    string           `json:"workspace_id,omitempty"`
	Actor          ActorRef         `json:"actor,omitempty"`
	Task           *EntityRef       `json:"task,omitempty"`
	Session        *EntityRef       `json:"session,omitempty"`
	TaskOwnerID    string           `json:"task_owner_id,omitempty"`
	TaskFence      uint64           `json:"task_fence,omitempty"`
	Source         RepositorySource `json:"source"`
	BatchSize      int              `json:"batch_size,omitempty"`
}

type IngestFile struct {
	ID               string     `json:"id"`
	JobID            string     `json:"job_id"`
	WorkspaceID      string     `json:"workspace_id"`
	Ordinal          int        `json:"ordinal"`
	Path             string     `json:"path"`
	Mode             uint32     `json:"mode"`
	ByteSize         int64      `json:"byte_size"`
	ContentHash      string     `json:"content_hash"`
	State            string     `json:"state"`
	SupersedesFileID string     `json:"supersedes_file_id,omitempty"`
	ChunkCount       int        `json:"chunk_count"`
	SymbolCount      int        `json:"symbol_count"`
	IndexedBytes     int64      `json:"indexed_bytes"`
	SkippedReason    string     `json:"skipped_reason,omitempty"`
	CreatedAt        time.Time  `json:"created_at,omitempty"`
	IndexedAt        *time.Time `json:"indexed_at,omitempty"`
}

type IngestChunk struct {
	ID               int64  `json:"id,omitempty"`
	WorkspaceID      string `json:"workspace_id"`
	JobID            string `json:"job_id"`
	FileID           string `json:"file_id"`
	SourcePath       string `json:"source_path"`
	SourceRange      string `json:"source_range"`
	Content          string `json:"content"`
	ContentHash      string `json:"content_hash"`
	ByteSize         int64  `json:"byte_size"`
	EmbeddingSkipped bool   `json:"embedding_skipped,omitempty"`
}

type IngestSymbol struct {
	ID          string    `json:"id"`
	WorkspaceID string    `json:"workspace_id"`
	JobID       string    `json:"job_id"`
	FileID      string    `json:"file_id"`
	Repository  string    `json:"repository"`
	FilePath    string    `json:"file_path"`
	SymbolName  string    `json:"symbol_name"`
	SymbolKind  string    `json:"symbol_kind"`
	Language    string    `json:"language,omitempty"`
	LineStart   int       `json:"line_start,omitempty"`
	LineEnd     int       `json:"line_end,omitempty"`
	Signature   string    `json:"signature,omitempty"`
	Docstring   string    `json:"docstring,omitempty"`
	ContentHash string    `json:"content_hash"`
	CreatedAt   time.Time `json:"created_at,omitempty"`
}

type IngestCheckpoint struct {
	JobID          string    `json:"job_id"`
	WorkspaceID    string    `json:"workspace_id"`
	NextOrdinal    int       `json:"next_ordinal"`
	ProcessedFiles int       `json:"processed_files"`
	RemovedFiles   int       `json:"removed_files"`
	ChunkCount     int       `json:"chunk_count"`
	SymbolCount    int       `json:"symbol_count"`
	SourceBytes    int64     `json:"source_bytes"`
	IndexedBytes   int64     `json:"indexed_bytes"`
	DedupedChunks  int       `json:"deduped_chunks"`
	BatchCount     int       `json:"batch_count"`
	LastBatchHash  string    `json:"last_batch_hash,omitempty"`
	StateHash      string    `json:"state_hash"`
	UpdatedAt      time.Time `json:"updated_at,omitempty"`
}

type IngestJob struct {
	SchemaVersion     int              `json:"schema_version"`
	ID                string           `json:"id"`
	WorkspaceID       string           `json:"workspace_id"`
	Repository        string           `json:"repository"`
	SourceRoot        string           `json:"source_root"`
	MountRoot         string           `json:"-"`
	ManifestHash      string           `json:"manifest_hash"`
	RequestHash       string           `json:"request_hash"`
	IdempotencyKey    string           `json:"idempotency_key"`
	Status            string           `json:"status"`
	Actor             ActorRef         `json:"actor,omitempty"`
	Task              *EntityRef       `json:"task,omitempty"`
	Session           *EntityRef       `json:"session,omitempty"`
	TaskOwnerID       string           `json:"task_owner_id,omitempty"`
	TaskFence         uint64           `json:"task_fence,omitempty"`
	CausationID       string           `json:"causation_id,omitempty"`
	CorrelationID     string           `json:"correlation_id,omitempty"`
	Source            RepositorySource `json:"source"`
	BatchSize         int              `json:"batch_size"`
	FileCount         int              `json:"file_count"`
	SkippedFiles      int              `json:"skipped_files"`
	ProcessedFiles    int              `json:"processed_files"`
	RemovedFiles      int              `json:"removed_files"`
	ChunkCount        int              `json:"chunk_count"`
	SymbolCount       int              `json:"symbol_count"`
	SourceBytes       int64            `json:"source_bytes"`
	IndexedBytes      int64            `json:"indexed_bytes"`
	DedupedChunks     int              `json:"deduped_chunks"`
	EmbeddingAttempts int              `json:"embedding_attempts"`
	EmbeddingSkipped  int              `json:"embedding_skipped"`
	BatchCount        int              `json:"batch_count"`
	Checkpoint        IngestCheckpoint `json:"checkpoint"`
	Report            *IngestReport    `json:"report,omitempty"`
	ReportArtifactID  *int64           `json:"report_artifact_id,omitempty"`
	ReportHash        string           `json:"report_hash,omitempty"`
	LastError         string           `json:"last_error,omitempty"`
	CreatedAt         time.Time        `json:"created_at,omitempty"`
	UpdatedAt         time.Time        `json:"updated_at,omitempty"`
	StartedAt         *time.Time       `json:"started_at,omitempty"`
	CompletedAt       *time.Time       `json:"completed_at,omitempty"`
}

type IngestReport struct {
	SchemaVersion     int    `json:"schema_version"`
	JobID             string `json:"job_id"`
	WorkspaceID       string `json:"workspace_id"`
	Repository        string `json:"repository"`
	SourceRoot        string `json:"source_root"`
	ManifestHash      string `json:"manifest_hash"`
	Status            string `json:"status"`
	DryRun            bool   `json:"dry_run,omitempty"`
	FileCount         int    `json:"file_count"`
	ProcessedFiles    int    `json:"processed_files"`
	RemovedFiles      int    `json:"removed_files"`
	SkippedFiles      int    `json:"skipped_files"`
	ChunkCount        int    `json:"chunk_count"`
	SymbolCount       int    `json:"symbol_count"`
	SourceBytes       int64  `json:"source_bytes"`
	IndexedBytes      int64  `json:"indexed_bytes"`
	DedupedChunks     int    `json:"deduped_chunks"`
	BatchCount        int    `json:"batch_count"`
	EmbeddingAttempts int    `json:"embedding_attempts"`
	EmbeddingSkipped  int    `json:"embedding_skipped"`
	DiscoveryMillis   int64  `json:"discovery_millis,omitempty"`
	IndexMillis       int64  `json:"index_millis,omitempty"`
	SQLQueries        int    `json:"sql_queries,omitempty"`
	ReportHash        string `json:"report_hash"`
	LastError         string `json:"last_error,omitempty"`
}

type IngestPage struct {
	Items      []IngestJob `json:"jobs"`
	NextCursor string      `json:"next_cursor,omitempty"`
}

type IngestBatchRequest struct {
	WorkspaceID string   `json:"workspace_id,omitempty"`
	JobID       string   `json:"job_id"`
	WorkerID    string   `json:"worker_id,omitempty"`
	BatchSize   int      `json:"batch_size,omitempty"`
	TaskOwnerID string   `json:"task_owner_id,omitempty"`
	TaskFence   uint64   `json:"task_fence,omitempty"`
	Actor       ActorRef `json:"actor,omitempty"`
}

func (s RepositorySource) Normalize() (RepositorySource, error) {
	s.Repository = strings.TrimSpace(s.Repository)
	s.SourceRoot = strings.TrimSpace(s.SourceRoot)
	s.MountRoot = strings.TrimSpace(s.MountRoot)
	if s.Repository == "" || s.SourceRoot == "" {
		return RepositorySource{}, fmt.Errorf("repository and source_root are required")
	}
	if !filepath.IsAbs(s.SourceRoot) {
		return RepositorySource{}, fmt.Errorf("source_root must be absolute")
	}
	if s.MountRoot == "" || !filepath.IsAbs(s.MountRoot) {
		return RepositorySource{}, fmt.Errorf("configured mount root is required")
	}
	if len(s.IgnoreRules) > MaxIngestIgnoreRules {
		return RepositorySource{}, fmt.Errorf("too many ignore rules")
	}
	for i := range s.IgnoreRules {
		s.IgnoreRules[i] = strings.TrimSpace(filepath.ToSlash(s.IgnoreRules[i]))
		if s.IgnoreRules[i] == "" {
			return RepositorySource{}, fmt.Errorf("ignore rule cannot be empty")
		}
	}
	sort.Strings(s.IgnoreRules)
	if s.MaxFiles <= 0 {
		s.MaxFiles = DefaultIngestMaxFiles
	}
	if s.MaxFileBytes <= 0 {
		s.MaxFileBytes = DefaultIngestMaxFileBytes
	}
	if s.MaxTotalBytes <= 0 {
		s.MaxTotalBytes = DefaultIngestMaxTotalBytes
	}
	if s.ChunkBytes <= 0 {
		s.ChunkBytes = DefaultIngestChunkBytes
	}
	if s.ChunkOverlap < 0 {
		return RepositorySource{}, fmt.Errorf("chunk_overlap cannot be negative")
	}
	if s.ChunkOverlap >= s.ChunkBytes {
		return RepositorySource{}, fmt.Errorf("chunk_overlap must be smaller than chunk_bytes")
	}
	if s.Embedding.MaxChunks < 0 || s.Embedding.MaxBytes < 0 {
		return RepositorySource{}, fmt.Errorf("embedding budgets cannot be negative")
	}
	if s.Embedding.Enabled {
		if s.Embedding.MaxChunks == 0 {
			s.Embedding.MaxChunks = DefaultIngestEmbeddingMaxChunks
		}
		if s.Embedding.MaxBytes == 0 {
			s.Embedding.MaxBytes = DefaultIngestEmbeddingMaxBytes
		}
	}
	return s, nil
}

func (r IngestJobRequest) Normalize() (IngestJobRequest, error) {
	if r.SchemaVersion == 0 {
		r.SchemaVersion = IngestSchemaVersion
	}
	if r.SchemaVersion != IngestSchemaVersion {
		return IngestJobRequest{}, fmt.Errorf("unsupported ingest schema_version %d", r.SchemaVersion)
	}
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	r.IdempotencyKey = strings.TrimSpace(r.IdempotencyKey)
	if r.WorkspaceID == "" || r.IdempotencyKey == "" {
		return IngestJobRequest{}, fmt.Errorf("workspace_id and idempotency_key are required")
	}
	if len(r.IdempotencyKey) > MaxIdempotencyLength {
		return IngestJobRequest{}, fmt.Errorf("idempotency_key is too large")
	}
	if r.BatchSize <= 0 {
		r.BatchSize = DefaultIngestBatchSize
	}
	if r.BatchSize > MaxIngestBatchSize {
		r.BatchSize = MaxIngestBatchSize
	}
	var err error
	r.Source, err = r.Source.Normalize()
	if err != nil {
		return IngestJobRequest{}, err
	}
	if r.Actor.WorkspaceID == "" {
		r.Actor.WorkspaceID = r.WorkspaceID
	}
	if r.Task != nil && (r.Task.WorkspaceID != "" && r.Task.WorkspaceID != r.WorkspaceID) {
		return IngestJobRequest{}, fmt.Errorf("task crosses workspace boundary")
	}
	if r.Session != nil && (r.Session.WorkspaceID != "" && r.Session.WorkspaceID != r.WorkspaceID) {
		return IngestJobRequest{}, fmt.Errorf("session crosses workspace boundary")
	}
	if r.Task != nil && (strings.TrimSpace(r.TaskOwnerID) == "" || r.TaskFence == 0) {
		return IngestJobRequest{}, fmt.Errorf("task-bound ingestion requires task_owner_id and task_fence")
	}
	return r, nil
}

func (r IngestJobRequest) RequestHash() string {
	clone := r
	clone.ID, clone.RequestID, clone.IdempotencyKey = "", "", ""
	raw, _ := json.Marshal(clone)
	d := sha256.Sum256(raw)
	return hex.EncodeToString(d[:])
}

func ManifestHash(files []IngestFile) string {
	ordered := append([]IngestFile(nil), files...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Path < ordered[j].Path })
	h := sha256.New()
	for _, f := range ordered {
		fmt.Fprintf(h, "%s\x00%d\x00%d\x00%s\x00", filepath.ToSlash(f.Path), f.ByteSize, f.Mode, strings.ToLower(f.ContentHash))
	}
	return hex.EncodeToString(h.Sum(nil))
}

func (r IngestReport) StableHash() string {
	clone := r
	clone.ReportHash = ""
	clone.DiscoveryMillis, clone.IndexMillis, clone.SQLQueries = 0, 0, 0
	raw, _ := json.Marshal(clone)
	d := sha256.Sum256(raw)
	return hex.EncodeToString(d[:])
}

func IsIngestTerminal(status string) bool {
	switch strings.TrimSpace(status) {
	case IngestSucceeded, IngestFailed, IngestCancelled:
		return true
	default:
		return false
	}
}
