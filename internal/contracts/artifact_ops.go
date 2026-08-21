package contracts

import "time"

const (
	DefaultArtifactOperationBatch = 100
	MaxArtifactOperationBatch     = 1000
)

// ArtifactBackfillRequest selects a bounded, resumable output backfill. Dry
// runs must not create artifacts or links.
type ArtifactBackfillRequest struct {
	WorkspaceID   string   `json:"workspace_id"`
	SourceKind    string   `json:"source_kind"`
	Cursor        string   `json:"cursor,omitempty"`
	BatchSize     int      `json:"batch_size,omitempty"`
	DryRun        bool     `json:"dry_run,omitempty"`
	Actor         ActorRef `json:"actor,omitempty"`
	CausationID   string   `json:"causation_id,omitempty"`
	CorrelationID string   `json:"correlation_id,omitempty"`
}

// ArtifactBackfillResult reports one deterministic backfill page and cursor.
type ArtifactBackfillResult struct {
	WorkspaceID string `json:"workspace_id"`
	SourceKind  string `json:"source_kind"`
	Cursor      string `json:"cursor,omitempty"`
	NextCursor  string `json:"next_cursor,omitempty"`
	BatchSize   int    `json:"batch_size"`
	DryRun      bool   `json:"dry_run"`
	Examined    int    `json:"examined"`
	Eligible    int    `json:"eligible"`
	Created     int    `json:"created"`
	Linked      int    `json:"linked"`
	Skipped     int    `json:"skipped"`
}

// ArtifactRetentionSweepRequest selects a bounded retention operation. Only
// explicit policy and unreferenced artifacts may be changed.
type ArtifactRetentionSweepRequest struct {
	WorkspaceID   string    `json:"workspace_id"`
	BatchSize     int       `json:"batch_size,omitempty"`
	DryRun        bool      `json:"dry_run,omitempty"`
	Now           time.Time `json:"now,omitempty"`
	Actor         ActorRef  `json:"actor,omitempty"`
	CausationID   string    `json:"causation_id,omitempty"`
	CorrelationID string    `json:"correlation_id,omitempty"`
}

// ArtifactRetentionSweepResult reports archive, deletion, blocking, and
// integrity outcomes for one retention page.
type ArtifactRetentionSweepResult struct {
	WorkspaceID string `json:"workspace_id"`
	BatchSize   int    `json:"batch_size"`
	DryRun      bool   `json:"dry_run"`
	Examined    int    `json:"examined"`
	Archived    int    `json:"archived"`
	Deleted     int    `json:"deleted"`
	Blocked     int    `json:"blocked"`
	Corrupt     int    `json:"corrupt"`
	NextCursor  string `json:"next_cursor,omitempty"`
}

// ArtifactIntegrityRequest selects a bounded artifact verification page.
type ArtifactIntegrityRequest struct {
	WorkspaceID string `json:"workspace_id"`
	Cursor      int64  `json:"cursor,omitempty"`
	BatchSize   int    `json:"batch_size,omitempty"`
	DryRun      bool   `json:"dry_run,omitempty"`
}

// ArtifactIntegrityReport is the deterministic result of one verification
// page, including corrupt artifact identities.
type ArtifactIntegrityReport struct {
	WorkspaceID string  `json:"workspace_id"`
	Cursor      int64   `json:"cursor,omitempty"`
	NextCursor  int64   `json:"next_cursor,omitempty"`
	BatchSize   int     `json:"batch_size"`
	DryRun      bool    `json:"dry_run"`
	Examined    int     `json:"examined"`
	Valid       int     `json:"valid"`
	Corrupt     int     `json:"corrupt"`
	CorruptIDs  []int64 `json:"corrupt_ids,omitempty"`
}

// ArtifactStorageMetrics separates logical bytes from deduplicated physical
// bytes for workspace-scoped storage accounting.
type ArtifactStorageMetrics struct {
	WorkspaceID        string  `json:"workspace_id"`
	Artifacts          int64   `json:"artifacts"`
	ActiveArtifacts    int64   `json:"active_artifacts"`
	ArchivedArtifacts  int64   `json:"archived_artifacts"`
	DeletedArtifacts   int64   `json:"deleted_artifacts"`
	ArtifactBytes      int64   `json:"artifact_bytes"`
	ChunkBytes         int64   `json:"chunk_bytes"`
	References         int64   `json:"references"`
	AuthoritativeRefs  int64   `json:"authoritative_references"`
	UniqueContentBytes int64   `json:"unique_content_bytes"`
	LogicalBytes       int64   `json:"logical_bytes"`
	DedupRatio         float64 `json:"dedup_ratio"`
}
