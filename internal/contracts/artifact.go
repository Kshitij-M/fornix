package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	ArtifactSchemaVersion       = 1
	DefaultArtifactChunkBytes   = 256 << 10
	MaxArtifactChunkBytes       = 1 << 20
	MaxArtifactBytes            = 64 << 20
	MaxArtifactGistBytes        = 16 << 10
	MaxArtifactDetailBytes      = 1 << 20
	MaxArtifactMetadataEntries  = 64
	MaxArtifactMetadataValueLen = 4096
	MaxArtifactDisclosureBytes  = 16 << 20
	MaxArtifactDisclosureTokens = 262144
	MaxArtifactDisclosureItems  = 128
)

const (
	ArtifactActive   = "active"
	ArtifactArchived = "archived"
	ArtifactDeleted  = "deleted"

	ArtifactIntegrityUnknown = "unknown"
	ArtifactIntegrityValid   = "valid"
	ArtifactIntegrityCorrupt = "corrupt"

	ArtifactDisclosureGist   = "gist"
	ArtifactDisclosureDetail = "detail"
	ArtifactDisclosureRaw    = "raw"
)

// Artifact is the immutable content identity plus mutable operational state.
// Raw bytes are addressed by ContentHash and are never represented inline in
// this contract; callers use ArtifactDisclosureResult for bounded disclosure.
type Artifact struct {
	SchemaVersion  int              `json:"schema_version"`
	ID             int64            `json:"id"`
	WorkspaceID    string           `json:"workspace_id"`
	ContentHash    string           `json:"content_hash"`
	Kind           string           `json:"kind"`
	MediaType      string           `json:"media_type"`
	ByteSize       int64            `json:"byte_size"`
	ChunkSize      int              `json:"chunk_size"`
	ChunkCount     int              `json:"chunk_count"`
	Manifest       ArtifactManifest `json:"manifest"`
	Status         string           `json:"status"`
	IntegrityState string           `json:"integrity_state"`
	Retention      RetentionPolicy  `json:"retention"`
	CreatedAt      time.Time        `json:"created_at"`
	ArchivedAt     *time.Time       `json:"archived_at,omitempty"`
	DeletedAt      *time.Time       `json:"deleted_at,omitempty"`
	IntegrityAt    *time.Time       `json:"integrity_at,omitempty"`
}

// ArtifactChunk is an ordered immutable raw-byte fragment. Data is returned
// only by internal store code and is excluded from normal JSON serialization.
type ArtifactChunk struct {
	SchemaVersion int    `json:"schema_version"`
	ArtifactID    int64  `json:"artifact_id"`
	WorkspaceID   string `json:"workspace_id"`
	Index         int    `json:"index"`
	ContentHash   string `json:"content_hash"`
	ByteSize      int    `json:"byte_size"`
	Data          []byte `json:"-"`
}

// ArtifactRef is the durable pointer carried by model/tool/event/evidence
// records. It is safe to serialize because it contains no raw bytes or
// credentials.
type ArtifactRef struct {
	SchemaVersion  int       `json:"schema_version"`
	ID             int64     `json:"id"`
	ArtifactID     int64     `json:"artifact_id"`
	WorkspaceID    string    `json:"workspace_id"`
	ContentHash    string    `json:"content_hash"`
	SourceKind     string    `json:"source_kind"`
	SourceID       string    `json:"source_id"`
	Role           string    `json:"role"`
	MediaType      string    `json:"media_type"`
	ByteSize       int64     `json:"byte_size"`
	IdempotencyKey string    `json:"idempotency_key"`
	CausationID    string    `json:"causation_id,omitempty"`
	CorrelationID  string    `json:"correlation_id,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

// ArtifactManifest holds bounded derived disclosure views. The raw content
// hash remains the identity for every view.
type ArtifactManifest struct {
	SchemaVersion int               `json:"schema_version"`
	Gist          string            `json:"gist,omitempty"`
	Detail        string            `json:"detail,omitempty"`
	GistHash      string            `json:"gist_hash,omitempty"`
	DetailHash    string            `json:"detail_hash,omitempty"`
	Metadata      map[string]string `json:"metadata,omitempty"`
}

type ArtifactProvenanceLink struct {
	SchemaVersion int               `json:"schema_version"`
	ID            int64             `json:"id"`
	WorkspaceID   string            `json:"workspace_id"`
	FromArtifact  int64             `json:"from_artifact_id"`
	ToArtifact    int64             `json:"to_artifact_id"`
	Relation      string            `json:"relation"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
}

type ArtifactDisclosureRequest struct {
	SchemaVersion     int    `json:"schema_version,omitempty"`
	WorkspaceID       string `json:"workspace_id"`
	ArtifactID        int64  `json:"artifact_id,omitempty"`
	ContentHash       string `json:"content_hash,omitempty"`
	Level             string `json:"level"`
	MaxBytes          int    `json:"max_bytes,omitempty"`
	MaxTokens         int    `json:"max_tokens,omitempty"`
	MaxItems          int    `json:"max_items,omitempty"`
	IncludeProvenance bool   `json:"include_provenance,omitempty"`
	MaxDepth          int    `json:"max_depth,omitempty"`
}

// ArtifactCreateRequest is the authenticated HTTP/input boundary. Actor
// identity is filled by the server and is intentionally absent here.
type ArtifactCreateRequest struct {
	SchemaVersion  int              `json:"schema_version,omitempty"`
	WorkspaceID    string           `json:"workspace_id,omitempty"`
	Kind           string           `json:"kind"`
	MediaType      string           `json:"media_type,omitempty"`
	Raw            []byte           `json:"raw"`
	Manifest       ArtifactManifest `json:"manifest,omitempty"`
	Retention      RetentionPolicy  `json:"retention,omitempty"`
	SourceKind     string           `json:"source_kind"`
	SourceID       string           `json:"source_id"`
	Role           string           `json:"role"`
	IdempotencyKey string           `json:"idempotency_key,omitempty"`
}

type ArtifactDisclosureResult struct {
	SchemaVersion     int                      `json:"schema_version"`
	WorkspaceID       string                   `json:"workspace_id"`
	ArtifactID        int64                    `json:"artifact_id"`
	ContentHash       string                   `json:"content_hash"`
	Status            string                   `json:"status"`
	Level             string                   `json:"level"`
	MediaType         string                   `json:"media_type"`
	ByteSize          int64                    `json:"byte_size"`
	Gist              string                   `json:"gist,omitempty"`
	Detail            string                   `json:"detail,omitempty"`
	Raw               []byte                   `json:"raw,omitempty"`
	Provenance        []ArtifactProvenanceLink `json:"provenance,omitempty"`
	References        []ArtifactRef            `json:"references,omitempty"`
	IntegrityVerified bool                     `json:"integrity_verified"`
	Truncated         bool                     `json:"truncated"`
	TotalBytes        int                      `json:"total_bytes"`
	TotalTokens       int                      `json:"total_tokens"`
	ContentViewHash   string                   `json:"content_view_hash"`
}

type RetentionPolicy struct {
	SchemaVersion int        `json:"schema_version"`
	RetainUntil   *time.Time `json:"retain_until,omitempty"`
	ArchiveAfter  *time.Time `json:"archive_after,omitempty"`
	DeleteAfter   *time.Time `json:"delete_after,omitempty"`
	AllowDelete   bool       `json:"allow_delete"`
}

func (r ArtifactDisclosureRequest) Normalize() (ArtifactDisclosureRequest, error) {
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	r.ContentHash = strings.ToLower(strings.TrimSpace(r.ContentHash))
	if r.SchemaVersion == 0 {
		r.SchemaVersion = ArtifactSchemaVersion
	}
	if r.WorkspaceID == "" || (r.ArtifactID <= 0 && r.ContentHash == "") {
		return ArtifactDisclosureRequest{}, fmt.Errorf("workspace_id and artifact_id or content_hash are required")
	}
	if r.ArtifactID > 0 && r.ContentHash != "" {
		return ArtifactDisclosureRequest{}, fmt.Errorf("artifact_id and content_hash are mutually exclusive")
	}
	if r.Level == "" {
		r.Level = ArtifactDisclosureGist
	}
	if r.Level != ArtifactDisclosureGist && r.Level != ArtifactDisclosureDetail && r.Level != ArtifactDisclosureRaw {
		return ArtifactDisclosureRequest{}, fmt.Errorf("unsupported artifact disclosure level %q", r.Level)
	}
	if r.MaxBytes == 0 {
		r.MaxBytes = DefaultArtifactChunkBytes
	}
	if r.MaxTokens == 0 {
		r.MaxTokens = r.MaxBytes / 4
	}
	if r.MaxItems == 0 {
		r.MaxItems = MaxArtifactDisclosureItems
	}
	if r.MaxBytes < 1 || r.MaxBytes > MaxArtifactDisclosureBytes || r.MaxTokens < 1 || r.MaxTokens > MaxArtifactDisclosureTokens || r.MaxItems < 1 || r.MaxItems > MaxArtifactDisclosureItems {
		return ArtifactDisclosureRequest{}, fmt.Errorf("artifact disclosure budget exceeds configured bounds")
	}
	if r.MaxDepth < 0 || r.MaxDepth > 16 {
		return ArtifactDisclosureRequest{}, fmt.Errorf("artifact provenance depth is out of bounds")
	}
	return r, nil
}

func (r RetentionPolicy) Normalize() (RetentionPolicy, error) {
	if r.SchemaVersion == 0 {
		r.SchemaVersion = ArtifactSchemaVersion
	}
	if r.RetainUntil != nil && r.ArchiveAfter != nil && r.ArchiveAfter.Before(*r.RetainUntil) {
		return RetentionPolicy{}, fmt.Errorf("archive_after cannot precede retain_until")
	}
	if r.ArchiveAfter != nil && r.DeleteAfter != nil && r.DeleteAfter.Before(*r.ArchiveAfter) {
		return RetentionPolicy{}, fmt.Errorf("delete_after cannot precede archive_after")
	}
	return r, nil
}

func ArtifactContentHash(raw []byte) string {
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func (m ArtifactManifest) Normalize() (ArtifactManifest, error) {
	if m.SchemaVersion == 0 {
		m.SchemaVersion = ArtifactSchemaVersion
	}
	m.Gist = strings.TrimSpace(m.Gist)
	if len(m.Gist) > MaxArtifactGistBytes || len(m.Detail) > MaxArtifactDetailBytes {
		return ArtifactManifest{}, fmt.Errorf("artifact manifest disclosure exceeds configured bounds")
	}
	if len(m.Metadata) > MaxArtifactMetadataEntries {
		return ArtifactManifest{}, fmt.Errorf("artifact metadata exceeds configured entry limit")
	}
	clean := make(map[string]string, len(m.Metadata))
	for key, value := range m.Metadata {
		key = strings.TrimSpace(key)
		if key == "" || len(key) > 256 || len(value) > MaxArtifactMetadataValueLen {
			return ArtifactManifest{}, fmt.Errorf("invalid artifact metadata")
		}
		clean[key] = value
	}
	m.Metadata = clean
	if m.Gist != "" {
		m.GistHash = ArtifactContentHash([]byte(m.Gist))
	}
	if m.Detail != "" {
		m.DetailHash = ArtifactContentHash([]byte(m.Detail))
	}
	return m, nil
}

func (m ArtifactManifest) JSON() ([]byte, error) {
	normalized, err := m.Normalize()
	if err != nil {
		return nil, err
	}
	return json.Marshal(normalized)
}
