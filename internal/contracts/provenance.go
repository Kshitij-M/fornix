package contracts

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	ProvenanceSchemaVersion = 1
	MaxEvidenceRawBytes     = 4 << 20
	MaxEvidenceGistBytes    = 16 << 10
	MaxEvidenceDetailBytes  = 4 << 20
	DefaultDisclosureBytes  = 32 << 10
	DefaultDisclosureTokens = 8192
	DefaultProvenanceDepth  = 2
	DefaultProvenanceNodes  = 64
	MaxProvenanceDepth      = 8
	MaxProvenanceNodes      = 512
)

// DisclosureLevel controls how much of one immutable evidence record may be
// disclosed. The order is intentional: callers can request a deeper level
// without changing the source record or its evidence hash.
type DisclosureLevel string

const (
	DisclosureGist   DisclosureLevel = "gist"
	DisclosureDetail DisclosureLevel = "detail"
	DisclosureRaw    DisclosureLevel = "raw"
)

func (l DisclosureLevel) valid() bool {
	return l == DisclosureGist || l == DisclosureDetail || l == DisclosureRaw
}

// ProvenanceRelation is a small, typed vocabulary. Unknown relations are
// rejected at the write boundary so graph traversal remains auditable.
type ProvenanceRelation string

const (
	RelationDerivedFrom ProvenanceRelation = "derived_from"
	RelationSupports    ProvenanceRelation = "supports"
	RelationContradicts ProvenanceRelation = "contradicts"
	RelationSupersedes  ProvenanceRelation = "supersedes"
	RelationCausedBy    ProvenanceRelation = "caused_by"
	RelationRefines     ProvenanceRelation = "refines"
)

func (r ProvenanceRelation) valid() bool {
	switch r {
	case RelationDerivedFrom, RelationSupports, RelationContradicts,
		RelationSupersedes, RelationCausedBy, RelationRefines:
		return true
	default:
		return false
	}
}

// SourceRecord is the durable evidence identity. RawPayload is intentionally
// omitted by default and is populated only for a raw disclosure.
type SourceRecord struct {
	ID                int64        `json:"id"`
	WorkspaceID       string       `json:"workspace_id"`
	SourceReference   string       `json:"source_reference"`
	DeduplicationKey  string       `json:"deduplication_key,omitempty"`
	Kind              string       `json:"kind"`
	MediaType         string       `json:"media_type"`
	Gist              string       `json:"gist"`
	Detail            string       `json:"detail,omitempty"`
	RawPayload        []byte       `json:"raw_payload,omitempty"`
	EvidenceHash      string       `json:"evidence_hash"`
	RawSizeBytes      int64        `json:"raw_size_bytes"`
	RawArtifact       *ArtifactRef `json:"raw_artifact,omitempty"`
	SupersedesID      *int64       `json:"supersedes_id,omitempty"`
	CreatedAt         time.Time    `json:"created_at"`
	IntegrityVerified bool         `json:"integrity_verified,omitempty"`
}

// SourceRecordInput is the JSON-friendly write contract. The store hashes
// RawPayload itself; no caller-provided evidence hash is accepted.
type SourceRecordInput struct {
	WorkspaceID      string          `json:"workspace_id,omitempty"`
	SourceReference  string          `json:"source_reference"`
	DeduplicationKey string          `json:"deduplication_key,omitempty"`
	Kind             string          `json:"kind"`
	MediaType        string          `json:"media_type,omitempty"`
	Gist             string          `json:"gist"`
	Detail           string          `json:"detail,omitempty"`
	RawPayload       json.RawMessage `json:"raw_payload"`
	SupersedesID     *int64          `json:"supersedes_id,omitempty"`
	Contradicts      []int64         `json:"contradicts,omitempty"`
}

// ProvenanceEdge is immutable graph metadata. Depth and Direction are
// traversal annotations and are zero-valued on a stored edge.
type ProvenanceEdge struct {
	ID             int64              `json:"id"`
	WorkspaceID    string             `json:"workspace_id"`
	FromEvidenceID int64              `json:"from_evidence_id"`
	ToEvidenceID   int64              `json:"to_evidence_id"`
	Relation       ProvenanceRelation `json:"relation"`
	Metadata       json.RawMessage    `json:"metadata,omitempty"`
	CreatedAt      time.Time          `json:"created_at"`
	Depth          int                `json:"depth,omitempty"`
	Direction      string             `json:"direction,omitempty"`
}

// ProvenanceEdgeInput requests one immutable, workspace-local graph edge.
type ProvenanceEdgeInput struct {
	WorkspaceID    string             `json:"workspace_id,omitempty"`
	FromEvidenceID int64              `json:"from_evidence_id"`
	ToEvidenceID   int64              `json:"to_evidence_id"`
	Relation       ProvenanceRelation `json:"relation"`
	Metadata       json.RawMessage    `json:"metadata,omitempty"`
}

// ProvenanceTraversalRequest bounds traversal from one evidence node. The
// store applies the workspace condition to every edge and node lookup.
type ProvenanceTraversalRequest struct {
	WorkspaceID string `json:"workspace_id"`
	EvidenceID  int64  `json:"evidence_id"`
	MaxDepth    int    `json:"max_depth,omitempty"`
	MaxNodes    int    `json:"max_nodes,omitempty"`
}

// DisclosureRequest is deterministic input to the disclosure compiler.
// EvidenceID and SourceReference are alternatives; exactly one is required.
type DisclosureRequest struct {
	WorkspaceID       string          `json:"workspace_id"`
	EvidenceID        int64           `json:"evidence_id,omitempty"`
	SourceReference   string          `json:"source_reference,omitempty"`
	Level             DisclosureLevel `json:"level,omitempty"`
	MaxBytes          int             `json:"max_bytes,omitempty"`
	MaxTokens         int             `json:"max_tokens,omitempty"`
	MaxDepth          int             `json:"max_depth,omitempty"`
	MaxNodes          int             `json:"max_nodes,omitempty"`
	IncludeProvenance *bool           `json:"include_provenance,omitempty"`
}

// Normalize validates disclosure identity, level, and hard byte/token/node
// budgets before any evidence is read.
func (r DisclosureRequest) Normalize() (DisclosureRequest, error) {
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	if r.WorkspaceID == "" {
		return DisclosureRequest{}, fmt.Errorf("workspace_id is required")
	}
	r.SourceReference = strings.TrimSpace(r.SourceReference)
	if r.EvidenceID <= 0 && r.SourceReference == "" {
		return DisclosureRequest{}, fmt.Errorf("evidence_id or source_reference is required")
	}
	if r.EvidenceID > 0 && r.SourceReference != "" {
		return DisclosureRequest{}, fmt.Errorf("evidence_id and source_reference are mutually exclusive")
	}
	if r.Level == "" {
		r.Level = DisclosureGist
	}
	if !r.Level.valid() {
		return DisclosureRequest{}, fmt.Errorf("unsupported disclosure level %q", r.Level)
	}
	if r.MaxBytes == 0 {
		r.MaxBytes = DefaultDisclosureBytes
	}
	if r.MaxTokens == 0 {
		r.MaxTokens = DefaultDisclosureTokens
	}
	if r.MaxBytes < 1 || r.MaxBytes > MaxEvidenceRawBytes {
		return DisclosureRequest{}, fmt.Errorf("max_bytes must be between 1 and %d", MaxEvidenceRawBytes)
	}
	if r.MaxTokens < 1 {
		return DisclosureRequest{}, fmt.Errorf("max_tokens must be positive")
	}
	if r.MaxDepth == 0 {
		r.MaxDepth = DefaultProvenanceDepth
	}
	if r.MaxNodes == 0 {
		r.MaxNodes = DefaultProvenanceNodes
	}
	if r.MaxDepth < 0 || r.MaxDepth > MaxProvenanceDepth {
		return DisclosureRequest{}, fmt.Errorf("max_depth must be between 0 and %d", MaxProvenanceDepth)
	}
	if r.MaxNodes < 1 || r.MaxNodes > MaxProvenanceNodes {
		return DisclosureRequest{}, fmt.Errorf("max_nodes must be between 1 and %d", MaxProvenanceNodes)
	}
	if r.IncludeProvenance == nil {
		include := true
		r.IncludeProvenance = &include
	}
	return r, nil
}

// DisclosureResult contains only the requested, budgeted representation. A
// result can be truncated while retaining the full hash and raw size so a
// caller knows that it received a bounded view rather than authoritative raw.
type DisclosureResult struct {
	SchemaVersion       int              `json:"schema_version"`
	WorkspaceID         string           `json:"workspace_id"`
	EvidenceID          int64            `json:"evidence_id"`
	SourceReference     string           `json:"source_reference"`
	Kind                string           `json:"kind"`
	MediaType           string           `json:"media_type"`
	Level               DisclosureLevel  `json:"level"`
	EvidenceHash        string           `json:"evidence_hash"`
	RawSizeBytes        int64            `json:"raw_size_bytes"`
	Gist                string           `json:"gist,omitempty"`
	Detail              string           `json:"detail,omitempty"`
	RawPayload          []byte           `json:"raw_payload,omitempty"`
	SupersedesID        *int64           `json:"supersedes_id,omitempty"`
	SupersededBy        []int64          `json:"superseded_by,omitempty"`
	ContradictedBy      []int64          `json:"contradicted_by,omitempty"`
	Provenance          []ProvenanceEdge `json:"provenance,omitempty"`
	ProvenanceTruncated bool             `json:"provenance_truncated,omitempty"`
	Truncated           bool             `json:"truncated"`
	TotalBytes          int              `json:"total_bytes"`
	TotalTokens         int              `json:"total_tokens"`
	ContentHash         string           `json:"content_hash"`
	QueryCount          int              `json:"query_count,omitempty"`
	IntegrityVerified   bool             `json:"integrity_verified"`
}
