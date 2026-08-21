package contracts

import (
	"fmt"
	"math"
	"strings"
	"time"
)

const (
	RetrievalSurfaceSchemaVersion = 1
	MaxRetrievalSurfaceBytes      = 64 << 10
	MaxRetrievalSurfaceRefs       = MaxRetrievalItems
	MaxRetrievalSurfaceIDLength   = 128
)

// RetrievalSurfaceReference is the redacted ranked identity of one compiled
// context item. It deliberately excludes the rendered text.
type RetrievalSurfaceReference struct {
	SourceReference string         `json:"source_reference"`
	Kind            string         `json:"kind"`
	EvidenceHash    string         `json:"evidence_hash"`
	Score           float64        `json:"score"`
	Stage           RetrievalStage `json:"stage"`
	Representation  string         `json:"representation,omitempty"`
	Truncated       bool           `json:"truncated,omitempty"`
}

// RetrievalSurface is an append-only, replay-safe capture of a retrieval
// result. It contains hashes, references, budgets, and bounded measurements,
// never query text, embeddings, prompts, credentials, or rendered context.
type RetrievalSurface struct {
	SchemaVersion  int                         `json:"schema_version"`
	ID             string                      `json:"id"`
	WorkspaceID    string                      `json:"workspace_id"`
	RequestID      string                      `json:"request_id"`
	IdempotencyKey string                      `json:"idempotency_key"`
	PayloadHash    string                      `json:"payload_hash"`
	RequestHash    string                      `json:"request_hash"`
	PlanHash       string                      `json:"plan_hash"`
	ContextHash    string                      `json:"context_hash"`
	Budget         RetrievalBudget             `json:"budget"`
	Trace          RetrievalTrace              `json:"trace"`
	References     []RetrievalSurfaceReference `json:"references,omitempty"`
	DurationMS     int64                       `json:"duration_ms"`
	SQLQueries     int                         `json:"sql_queries"`
	CostUSD        float64                     `json:"cost_usd"`
	CostKnown      bool                        `json:"cost_known"`
	CostEstimated  bool                        `json:"cost_estimated"`
	Actor          ActorRef                    `json:"actor,omitempty"`
	CausationID    string                      `json:"causation_id,omitempty"`
	CorrelationID  string                      `json:"correlation_id,omitempty"`
	CapturedAt     time.Time                   `json:"captured_at"`
}

// RetrievalSurfacePage is the bounded operator read shape.
type RetrievalSurfacePage struct {
	Items      []RetrievalSurface `json:"items"`
	NextCursor string             `json:"next_cursor,omitempty"`
}

// Normalize redacts stage errors, validates evidence identities, and derives a
// canonical payload hash without including timing or transport metadata.
func (s *RetrievalSurface) Normalize() error {
	if s == nil {
		return fmt.Errorf("retrieval surface is nil")
	}
	if s.SchemaVersion == 0 {
		s.SchemaVersion = RetrievalSurfaceSchemaVersion
	}
	if s.SchemaVersion != RetrievalSurfaceSchemaVersion {
		return fmt.Errorf("unsupported retrieval surface schema_version %d", s.SchemaVersion)
	}
	s.ID = strings.TrimSpace(s.ID)
	if s.ID == "" {
		s.ID = NewID("surface")
	}
	s.WorkspaceID = strings.TrimSpace(s.WorkspaceID)
	s.RequestID = strings.TrimSpace(s.RequestID)
	s.IdempotencyKey = strings.TrimSpace(s.IdempotencyKey)
	s.PayloadHash = strings.ToLower(strings.TrimSpace(s.PayloadHash))
	s.RequestHash = strings.ToLower(strings.TrimSpace(s.RequestHash))
	s.PlanHash = strings.ToLower(strings.TrimSpace(s.PlanHash))
	s.ContextHash = strings.ToLower(strings.TrimSpace(s.ContextHash))
	s.CausationID = strings.TrimSpace(s.CausationID)
	s.CorrelationID = strings.TrimSpace(s.CorrelationID)
	if s.WorkspaceID == "" || s.RequestID == "" || s.IdempotencyKey == "" || s.RequestHash == "" || s.PlanHash == "" || s.ContextHash == "" {
		return fmt.Errorf("surface workspace, request, idempotency, request, plan, and context identities are required")
	}
	if len(s.ID) > MaxRetrievalSurfaceIDLength || len(s.RequestID) > MaxRetrievalSurfaceIDLength || len(s.IdempotencyKey) > MaxIdempotencyLength {
		return fmt.Errorf("retrieval surface identity is too large")
	}
	for name, value := range map[string]string{"request_hash": s.RequestHash, "plan_hash": s.PlanHash, "context_hash": s.ContextHash} {
		if !canonicalSHA256(value) {
			return fmt.Errorf("%s must be a lowercase sha256", name)
		}
	}
	if s.Budget.MaxItems < 1 || s.Budget.MaxItems > MaxRetrievalItems || s.Budget.MaxBytes < 1 || s.Budget.MaxBytes > MaxRetrievalBytes || s.Budget.MaxTokens < 1 || s.Budget.MaxTokens > MaxRetrievalTokens {
		return fmt.Errorf("retrieval surface budget is invalid")
	}
	if len(s.References) > MaxRetrievalSurfaceRefs {
		return fmt.Errorf("retrieval surface has too many references")
	}
	for i := range s.Trace.Stages {
		if s.Trace.Stages[i].Error != "" {
			// Stage errors can contain driver/user text. Preserve only the stable
			// failure fact in the redacted evaluation surface.
			s.Trace.Stages[i].Error = "stage_failed"
		}
	}
	seen := make(map[string]struct{}, len(s.References))
	for i := range s.References {
		ref := &s.References[i]
		ref.SourceReference = strings.TrimSpace(ref.SourceReference)
		ref.Kind = strings.TrimSpace(ref.Kind)
		ref.EvidenceHash = strings.ToLower(strings.TrimSpace(ref.EvidenceHash))
		ref.Representation = strings.TrimSpace(ref.Representation)
		if ref.SourceReference == "" || ref.Kind == "" || !canonicalSHA256(ref.EvidenceHash) {
			return fmt.Errorf("retrieval surface reference %d is invalid", i)
		}
		if _, ok := seen[ref.EvidenceHash]; ok {
			return fmt.Errorf("retrieval surface contains duplicate evidence hash %q", ref.EvidenceHash)
		}
		seen[ref.EvidenceHash] = struct{}{}
		if ref.Score < 0 || ref.Score > 1 || math.IsNaN(ref.Score) || math.IsInf(ref.Score, 0) {
			return fmt.Errorf("retrieval surface reference %d score is invalid", i)
		}
		if !validRetrievalStage(ref.Stage) {
			return fmt.Errorf("retrieval surface reference %d stage is invalid", i)
		}
	}
	if s.DurationMS < 0 || s.SQLQueries < 0 || s.CostUSD < 0 || math.IsNaN(s.CostUSD) || math.IsInf(s.CostUSD, 0) {
		return fmt.Errorf("retrieval surface measurements are invalid")
	}
	if s.CostKnown && s.CostEstimated {
		return fmt.Errorf("retrieval surface cost cannot be both known and estimated")
	}
	if s.CapturedAt.IsZero() {
		s.CapturedAt = time.Now().UTC()
	}
	canonicalPayloadHash := hashJSON(s.payloadForHash())
	if s.PayloadHash == "" {
		s.PayloadHash = canonicalPayloadHash
	} else if s.PayloadHash != canonicalPayloadHash {
		return fmt.Errorf("payload_hash does not match canonical surface payload")
	}
	if !canonicalSHA256(s.PayloadHash) {
		return fmt.Errorf("payload_hash must be a lowercase sha256")
	}
	return nil
}

func (s RetrievalSurface) payloadForHash() any {
	clone := s
	clone.ID, clone.PayloadHash, clone.CapturedAt = "", "", time.Time{}
	// Transport/admission identity is not retrieval content. A retried
	// capture may carry a new request ID while retaining the same idempotency
	// key and logical result.
	clone.RequestID, clone.IdempotencyKey = "", ""
	clone.Actor = ActorRef{}
	clone.CausationID, clone.CorrelationID = "", ""
	// Timing and accounting are observations of delivery, not the logical
	// retrieval result. Excluding them makes same-key retries replayable.
	clone.DurationMS, clone.SQLQueries, clone.CostUSD, clone.CostKnown, clone.CostEstimated = 0, 0, 0, false, false
	clone.Trace.Stages = append([]RetrievalStageTrace(nil), s.Trace.Stages...)
	for i := range clone.Trace.Stages {
		clone.Trace.Stages[i].DurationMicros = 0
	}
	return clone
}

// CanonicalPayloadHash returns the stable logical result hash used to compare
// duplicate captures and offline replay.
func (s RetrievalSurface) CanonicalPayloadHash() string {
	return hashJSON(s.payloadForHash())
}

// ContextPack reconstructs the redacted ranking shape needed by offline
// scoring. Rendered text is intentionally absent; quality scoring uses the
// immutable evidence hashes and the recorded context hash.
func (s RetrievalSurface) ContextPack() ContextPack {
	items := make([]ContextItem, 0, len(s.References))
	for _, ref := range s.References {
		items = append(items, ContextItem{
			WorkspaceID: s.WorkspaceID, SourceReference: ref.SourceReference,
			Kind: ref.Kind, EvidenceHash: ref.EvidenceHash, Score: ref.Score,
			Stage: ref.Stage, Representation: ref.Representation, Truncated: ref.Truncated,
		})
	}
	return ContextPack{
		SchemaVersion: RetrievalSchemaVersion, WorkspaceID: s.WorkspaceID,
		RequestHash: s.RequestHash, Items: items,
		TotalBytes: s.Trace.CompiledBytes, TotalTokens: s.Trace.CompiledTokens,
		Truncated: s.Trace.TruncatedItems > 0, Abstained: s.Trace.Abstained,
		ContentHash: s.ContextHash,
	}
}

func canonicalSHA256(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	for _, r := range value {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return false
		}
	}
	return true
}

func validRetrievalStage(stage RetrievalStage) bool {
	switch stage {
	case StageStructured, StageLexical, StageGraph, StageVector:
		return true
	default:
		return false
	}
}
