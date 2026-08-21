package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	RetrievalSchemaVersion = 1
	DefaultRetrievalItems  = 20
	DefaultRetrievalBytes  = 32 << 10
	DefaultRetrievalTokens = 8192
	MaxRetrievalItems      = 100
	MaxRetrievalBytes      = 1 << 20
	MaxRetrievalTokens     = 65536
	MaxQueryEmbeddingDim   = embeddingDimension
)

const embeddingDimension = 768

// RetrievalStage names one deterministic retrieval tier. Stages are ordered
// from low-cost structured lookup to explicitly gated vector search.
type RetrievalStage string

const (
	StageStructured RetrievalStage = "structured"
	StageLexical    RetrievalStage = "lexical"
	StageGraph      RetrievalStage = "graph"
	StageVector     RetrievalStage = "vector"
)

// RetrievalRequest is the complete deterministic input to the retrieval
// planner. QueryEmbedding is optional caller-provided data; the planner never
// invokes an embedding model on behalf of a read.
type RetrievalRequest struct {
	WorkspaceID     string    `json:"workspace_id"`
	RequestID       string    `json:"request_id,omitempty"`
	IdempotencyKey  string    `json:"idempotency_key,omitempty"`
	Actor           ActorRef  `json:"actor,omitempty"`
	CausationID     string    `json:"causation_id,omitempty"`
	CorrelationID   string    `json:"correlation_id,omitempty"`
	Query           string    `json:"query,omitempty"`
	ExactSourceRefs []string  `json:"exact_source_refs,omitempty"`
	TaskID          string    `json:"task_id,omitempty"`
	MemoType        string    `json:"memo_type,omitempty"`
	Repo            string    `json:"repo,omitempty"`
	MaxItems        int       `json:"max_items,omitempty"`
	MaxBytes        int       `json:"max_bytes,omitempty"`
	MaxTokens       int       `json:"max_tokens,omitempty"`
	MinResults      int       `json:"min_results,omitempty"`
	MinScore        float64   `json:"min_score,omitempty"`
	CandidateLimit  int       `json:"candidate_limit,omitempty"`
	EnableGraph     *bool     `json:"enable_graph,omitempty"`
	EnableVector    *bool     `json:"enable_vector,omitempty"`
	QueryEmbedding  []float32 `json:"query_embedding,omitempty"`
}

// RetrievalBudget is repeated in the plan and pack so callers can audit the
// exact hard limits that were applied.
type RetrievalBudget struct {
	MaxItems  int `json:"max_items"`
	MaxBytes  int `json:"max_bytes"`
	MaxTokens int `json:"max_tokens"`
}

// RetrievalStagePlan describes one planned retrieval stage and its admission
// reason without executing the query.
type RetrievalStagePlan struct {
	Name           RetrievalStage `json:"name"`
	Enabled        bool           `json:"enabled"`
	CandidateLimit int            `json:"candidate_limit"`
	Reason         string         `json:"reason,omitempty"`
}

// RetrievalPlan is the deterministic, hashable execution plan for a request.
type RetrievalPlan struct {
	SchemaVersion int                  `json:"schema_version"`
	Policy        string               `json:"policy"`
	RequestHash   string               `json:"request_hash"`
	WorkspaceID   string               `json:"workspace_id"`
	Budget        RetrievalBudget      `json:"budget"`
	Stages        []RetrievalStagePlan `json:"stages"`
}

// RetrievalStageTrace records bounded measurements and outcomes for one stage.
type RetrievalStageTrace struct {
	Name           RetrievalStage `json:"name"`
	Status         string         `json:"status"`
	Reason         string         `json:"reason,omitempty"`
	Queries        int            `json:"queries"`
	Candidates     int            `json:"candidates"`
	Accepted       int            `json:"accepted"`
	Duplicates     int            `json:"duplicates"`
	DurationMicros int64          `json:"duration_micros"`
	Error          string         `json:"error,omitempty"`
}

// RetrievalTrace is the redacted execution trace used for replay and cost
// accounting; it never substitutes for authoritative source records.
type RetrievalTrace struct {
	PlanHash       string                `json:"plan_hash"`
	Stages         []RetrievalStageTrace `json:"stages"`
	Candidates     int                   `json:"candidates"`
	Accepted       int                   `json:"accepted"`
	Duplicates     int                   `json:"duplicates"`
	CompiledItems  int                   `json:"compiled_items"`
	CompiledBytes  int                   `json:"compiled_bytes"`
	CompiledTokens int                   `json:"compiled_tokens"`
	TruncatedItems int                   `json:"truncated_items"`
	Abstained      bool                  `json:"abstained"`
}

// ContextItem is a bounded rendering of an authoritative source. Text may be
// truncated, but EvidenceHash and SourceReference always identify the full
// source representation that was ranked.
type ContextItem struct {
	WorkspaceID     string         `json:"workspace_id"`
	SourceReference string         `json:"source_reference"`
	Kind            string         `json:"kind"`
	Representation  string         `json:"representation"`
	Text            string         `json:"text"`
	EvidenceHash    string         `json:"evidence_hash"`
	Score           float64        `json:"score"`
	Stage           RetrievalStage `json:"stage"`
	Provenance      []Provenance   `json:"provenance,omitempty"`
	OriginalBytes   int            `json:"original_bytes"`
	OriginalTokens  int            `json:"original_tokens"`
	Truncated       bool           `json:"truncated,omitempty"`
}

// ContextPack is the bounded compiled context returned to an execution step.
// Each item retains its workspace and evidence identity for provenance.
type ContextPack struct {
	SchemaVersion int           `json:"schema_version"`
	WorkspaceID   string        `json:"workspace_id"`
	RequestHash   string        `json:"request_hash"`
	Items         []ContextItem `json:"items"`
	TotalBytes    int           `json:"total_bytes"`
	TotalTokens   int           `json:"total_tokens"`
	Truncated     bool          `json:"truncated"`
	Abstained     bool          `json:"abstained"`
	ContentHash   string        `json:"content_hash"`
}

// Normalize validates workspace scope, query bounds, and retrieval budgets.
func (r RetrievalRequest) Normalize() (RetrievalRequest, error) {
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	r.RequestID = strings.TrimSpace(r.RequestID)
	r.IdempotencyKey = strings.TrimSpace(r.IdempotencyKey)
	if r.WorkspaceID == "" {
		return RetrievalRequest{}, fmt.Errorf("workspace_id is required")
	}
	if len(r.RequestID) > MaxEventIDLength || len(r.IdempotencyKey) > MaxIdempotencyLength {
		return RetrievalRequest{}, fmt.Errorf("retrieval request identity is too large")
	}
	r.Query = strings.TrimSpace(r.Query)
	r.CausationID = strings.TrimSpace(r.CausationID)
	r.CorrelationID = strings.TrimSpace(r.CorrelationID)
	r.TaskID = strings.TrimSpace(r.TaskID)
	r.MemoType = strings.TrimSpace(r.MemoType)
	r.Repo = strings.TrimSpace(r.Repo)
	r.ExactSourceRefs = normalizeStrings(r.ExactSourceRefs)
	if r.MaxItems == 0 {
		r.MaxItems = DefaultRetrievalItems
	}
	if r.MaxBytes == 0 {
		r.MaxBytes = DefaultRetrievalBytes
	}
	if r.MaxTokens == 0 {
		r.MaxTokens = DefaultRetrievalTokens
	}
	if r.MinResults == 0 {
		r.MinResults = 1
	}
	if r.MinScore == 0 {
		r.MinScore = 0.75
	}
	if r.CandidateLimit == 0 {
		r.CandidateLimit = r.MaxItems * 5
	}
	if r.MaxItems < 1 || r.MaxItems > MaxRetrievalItems {
		return RetrievalRequest{}, fmt.Errorf("max_items must be between 1 and %d", MaxRetrievalItems)
	}
	if r.MaxBytes < 1 || r.MaxBytes > MaxRetrievalBytes {
		return RetrievalRequest{}, fmt.Errorf("max_bytes must be between 1 and %d", MaxRetrievalBytes)
	}
	if r.MaxTokens < 1 || r.MaxTokens > MaxRetrievalTokens {
		return RetrievalRequest{}, fmt.Errorf("max_tokens must be between 1 and %d", MaxRetrievalTokens)
	}
	if r.MinResults < 1 || r.MinResults > r.MaxItems {
		return RetrievalRequest{}, fmt.Errorf("min_results must be between 1 and max_items")
	}
	if r.CandidateLimit < 1 || r.CandidateLimit > MaxRetrievalItems*10 {
		return RetrievalRequest{}, fmt.Errorf("candidate_limit must be between 1 and %d", MaxRetrievalItems*10)
	}
	if r.MinScore < 0 || r.MinScore > 1 || math.IsNaN(r.MinScore) || math.IsInf(r.MinScore, 0) {
		return RetrievalRequest{}, fmt.Errorf("min_score must be between 0 and 1")
	}
	if r.EnableGraph == nil {
		enabled := true
		r.EnableGraph = &enabled
	}
	if r.EnableVector == nil {
		enabled := true
		r.EnableVector = &enabled
	}
	if len(r.QueryEmbedding) > 0 {
		if len(r.QueryEmbedding) != MaxQueryEmbeddingDim {
			return RetrievalRequest{}, fmt.Errorf("query_embedding must have %d dimensions", MaxQueryEmbeddingDim)
		}
		for _, value := range r.QueryEmbedding {
			if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
				return RetrievalRequest{}, fmt.Errorf("query_embedding contains a non-finite value")
			}
		}
	}
	return r, nil
}

func normalizeStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

// Hash returns the stable logical identity of a normalized retrieval request.
func (r RetrievalRequest) Hash() (string, error) {
	normalized, err := r.Normalize()
	if err != nil {
		return "", err
	}
	// Admission identity is metadata, not retrieval content. Keep plans and
	// context hashes stable when the same logical request is retried with a
	// different transport request ID or authenticated actor representation.
	normalized.RequestID = ""
	normalized.IdempotencyKey = ""
	normalized.Actor = ActorRef{}
	normalized.CausationID = ""
	normalized.CorrelationID = ""
	b, err := json.Marshal(normalized)
	if err != nil {
		return "", fmt.Errorf("hash retrieval request: %w", err)
	}
	digest := sha256.Sum256(b)
	return hex.EncodeToString(digest[:]), nil
}

// EstimateTokens is deliberately conservative and model-independent. A
// future tokenizer may be added as a measured policy, but this bound must
// remain safe when no model is involved.
func EstimateTokens(text string) int {
	if text == "" {
		return 0
	}
	return (utf8.RuneCountInString(text) + 3) / 4
}
