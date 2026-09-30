package contracts

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	EmbeddingSchemaVersion = 1
	EmbeddingDimension     = 768
	MaxEmbeddingInputBytes = 2000
	MaxEmbeddingMetadata   = 32
	MaxEmbeddingVector     = 4096
)

const (
	EmbeddingCallRunning          = "running"
	EmbeddingCallPending          = "pending"
	EmbeddingCallSucceeded        = "succeeded"
	EmbeddingCallFailed           = "failed"
	EmbeddingCallRecoveryRequired = "recovery_required"
	EmbeddingCallCancelled        = "cancelled"
	EmbeddingCallExpired          = "expired"
)

const (
	EmbeddingRetentionAuthoritative = "authoritative"
	EmbeddingRetentionQuery         = "query"
)

const (
	EmbeddingFailureInvalidRequest = "invalid_request"
	EmbeddingFailureAuthentication = "authentication"
	EmbeddingFailureQuota          = "quota"
	EmbeddingFailureRateLimit      = "rate_limit"
	EmbeddingFailureContextWindow  = "context_window"
	EmbeddingFailureProvider       = "provider"
	EmbeddingFailureTransport      = "transport"
	EmbeddingFailureTimeout        = "timeout"
	EmbeddingFailureBudget         = "budget"
	EmbeddingFailureInProgress     = "in_progress"
	EmbeddingFailureWorkspace      = "workspace_isolation"
)

// EmbeddingBudget bounds one provider request and its returned vector. A
// zero field receives the conservative local default during normalization.
type EmbeddingBudget struct {
	MaxInputBytes int     `json:"max_input_bytes,omitempty"`
	Dimension     int     `json:"dimension,omitempty"`
	MaxCostUSD    float64 `json:"max_cost_usd,omitempty"`
	TimeoutMS     int     `json:"timeout_ms,omitempty"`
}

// EmbeddingRequest is the durable identity for one vector-generation attempt.
// Text is accepted only in memory and is deliberately excluded from JSON,
// evidence, and the stable request hash. SourceHash carries the content
// identity needed for deterministic replay without storing raw input again.
type EmbeddingRequest struct {
	SchemaVersion  int               `json:"schema_version"`
	RequestID      string            `json:"request_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	CausationID    string            `json:"causation_id,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	WorkspaceID    string            `json:"workspace_id"`
	Actor          ActorRef          `json:"actor"`
	Task           *EntityRef        `json:"task,omitempty"`
	TaskOwnerID    string            `json:"task_owner_id,omitempty"`
	TaskFence      uint64            `json:"task_fence,omitempty"`
	Session        *EntityRef        `json:"session,omitempty"`
	Provider       ProviderRef       `json:"provider"`
	Model          string            `json:"model"`
	SourceKind     string            `json:"source_kind"`
	SourceID       string            `json:"source_id,omitempty"`
	SourceHash     string            `json:"source_hash"`
	Text           string            `json:"-"`
	Budget         EmbeddingBudget   `json:"budget"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// Normalize validates the request and computes SourceHash from in-memory text
// only when a caller has not already supplied the authoritative source hash.
func (r *EmbeddingRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("embedding request is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = EmbeddingSchemaVersion
	}
	if r.SchemaVersion != EmbeddingSchemaVersion {
		return fmt.Errorf("unsupported embedding schema_version %d", r.SchemaVersion)
	}
	r.RequestID = strings.TrimSpace(r.RequestID)
	r.IdempotencyKey = strings.TrimSpace(r.IdempotencyKey)
	if r.RequestID == "" {
		r.RequestID = NewID("embreq")
	}
	if r.IdempotencyKey == "" || len(r.IdempotencyKey) > MaxIdempotencyLength {
		return fmt.Errorf("embedding idempotency_key is required and bounded")
	}
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	if r.WorkspaceID == "" {
		return fmt.Errorf("embedding workspace_id is required")
	}
	if err := normalizeDomainActor(&r.Actor, r.WorkspaceID); err != nil {
		return err
	}
	if err := normalizeDomainEntity(r.Task, "task", r.WorkspaceID); err != nil {
		return err
	}
	if r.Task != nil {
		if (strings.TrimSpace(r.TaskOwnerID) == "") != (r.TaskFence == 0) {
			return fmt.Errorf("embedding task owner and fence must be supplied together")
		}
	} else if r.TaskOwnerID != "" || r.TaskFence != 0 {
		return fmt.Errorf("embedding task fence requires a task")
	}
	if err := normalizeDomainEntity(r.Session, "session", r.WorkspaceID); err != nil {
		return err
	}
	r.CausationID, r.CorrelationID = strings.TrimSpace(r.CausationID), strings.TrimSpace(r.CorrelationID)
	r.Model = strings.TrimSpace(r.Model)
	if r.Model == "" || len(r.Model) > MaxDomainNameLength {
		return fmt.Errorf("embedding model is required and bounded")
	}
	r.Provider.Provider = strings.ToLower(strings.TrimSpace(r.Provider.Provider))
	r.Provider.Endpoint = strings.TrimSpace(r.Provider.Endpoint)
	if r.Provider.Provider == "" {
		return fmt.Errorf("embedding provider is required")
	}
	r.Provider.Model = r.Model
	if len(r.Provider.Endpoint) > MaxDomainIDLength {
		return fmt.Errorf("embedding provider endpoint is too large")
	}
	r.SourceKind = strings.ToLower(strings.TrimSpace(r.SourceKind))
	if r.SourceKind == "" || len(r.SourceKind) > MaxDomainNameLength {
		return fmt.Errorf("embedding source_kind is required and bounded")
	}
	r.SourceID = strings.TrimSpace(r.SourceID)
	if len(r.SourceID) > MaxDomainIDLength {
		return fmt.Errorf("embedding source_id is too large")
	}
	if len([]byte(r.Text)) > MaxEmbeddingInputBytes {
		return fmt.Errorf("embedding input exceeds %d bytes", MaxEmbeddingInputBytes)
	}
	if strings.TrimSpace(r.SourceHash) == "" {
		if r.Text == "" {
			return fmt.Errorf("embedding source_hash or text is required")
		}
		digest := sha256.Sum256([]byte(r.Text))
		r.SourceHash = hex.EncodeToString(digest[:])
	} else {
		r.SourceHash = strings.ToLower(strings.TrimSpace(r.SourceHash))
		if len(r.SourceHash) != 64 || !canonicalSHA256(r.SourceHash) {
			return fmt.Errorf("embedding source_hash must be a lowercase sha256")
		}
		if r.Text != "" && EmbeddingSourceHash(r.Text) != r.SourceHash {
			return fmt.Errorf("embedding source_hash does not match text")
		}
	}
	if r.Text != "" && utf8.RuneCountInString(r.Text) == 0 {
		return fmt.Errorf("embedding text is invalid")
	}
	if r.Budget.MaxInputBytes == 0 {
		r.Budget.MaxInputBytes = MaxEmbeddingInputBytes
	}
	if r.Budget.Dimension == 0 {
		r.Budget.Dimension = EmbeddingDimension
	}
	if r.Budget.TimeoutMS == 0 {
		r.Budget.TimeoutMS = 30_000
	}
	if r.Budget.MaxInputBytes < 1 || r.Budget.MaxInputBytes > MaxEmbeddingInputBytes || r.Budget.Dimension != EmbeddingDimension || r.Budget.TimeoutMS < 1 || r.Budget.TimeoutMS > 600_000 || r.Budget.MaxCostUSD < 0 {
		return fmt.Errorf("embedding budget is outside supported bounds")
	}
	if len([]byte(r.Text)) > r.Budget.MaxInputBytes {
		return fmt.Errorf("embedding input exceeds configured budget of %d bytes", r.Budget.MaxInputBytes)
	}
	if len(r.Metadata) > MaxEmbeddingMetadata {
		return fmt.Errorf("embedding metadata exceeds %d entries", MaxEmbeddingMetadata)
	}
	for key, value := range r.Metadata {
		if strings.TrimSpace(key) == "" || len(key) > MaxDomainMetadataKeyLength || len(value) > MaxDomainMetadataValueLen || strings.ContainsAny(key+value, "\x00\r\n") {
			return fmt.Errorf("embedding metadata is invalid")
		}
	}
	return nil
}

// RequestHash excludes raw text because SourceHash is its content identity.
func (r EmbeddingRequest) RequestHash() string {
	clone := r
	clone.Text = ""
	clone.RequestID, clone.IdempotencyKey = "", ""
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// EmbeddingFailure is a bounded provider-neutral failure. PossiblyStarted is
// true when the provider may have accepted the request and retry is unsafe.
type EmbeddingFailure struct {
	Code              string `json:"code"`
	Message           string `json:"message"`
	Provider          string `json:"provider,omitempty"`
	ProviderRequestID string `json:"provider_request_id,omitempty"`
	Retryable         bool   `json:"retryable,omitempty"`
	PossiblyStarted   bool   `json:"possibly_started,omitempty"`
}

type EmbeddingUsage struct {
	InputBytes int64   `json:"input_bytes"`
	Dimension  int     `json:"dimension"`
	Source     string  `json:"source"`
	Measured   bool    `json:"measured"`
	CostUSD    float64 `json:"cost_usd,omitempty"`
}

// EmbeddingResponse is the provider-neutral result returned by an explicit
// embedding capability. The vector is in-memory provider output; durable
// stores retain it only through the scoped embedding-call ledger.
type EmbeddingResponse struct {
	RequestID         string            `json:"request_id"`
	Provider          ProviderRef       `json:"provider"`
	SourceHash        string            `json:"source_hash"`
	Vector            []float32         `json:"-"`
	VectorHash        string            `json:"vector_hash"`
	Dimension         int               `json:"dimension"`
	Usage             EmbeddingUsage    `json:"usage"`
	ProviderRequestID string            `json:"provider_request_id,omitempty"`
	Failure           *EmbeddingFailure `json:"failure,omitempty"`
}

// EmbeddingCallRecord is the durable, hash-and-reference ledger. Vector data
// is returned to callers but is not serialized into evidence or events.
type EmbeddingCallRecord struct {
	ID                int64             `json:"id"`
	WorkspaceID       string            `json:"workspace_id"`
	RequestID         string            `json:"request_id"`
	IdempotencyKey    string            `json:"idempotency_key"`
	RequestHash       string            `json:"request_hash"`
	SchemaVersion     int               `json:"schema_version"`
	CausationID       string            `json:"causation_id,omitempty"`
	CorrelationID     string            `json:"correlation_id,omitempty"`
	SourceKind        string            `json:"source_kind"`
	SourceID          string            `json:"source_id,omitempty"`
	SourceHash        string            `json:"source_hash"`
	Provider          ProviderRef       `json:"provider"`
	Actor             ActorRef          `json:"actor"`
	Task              *EntityRef        `json:"task,omitempty"`
	Session           *EntityRef        `json:"session,omitempty"`
	TaskOwnerID       string            `json:"task_owner_id,omitempty"`
	TaskFence         uint64            `json:"task_fence,omitempty"`
	Metadata          map[string]string `json:"metadata,omitempty"`
	Status            string            `json:"status"`
	AttemptCount      int               `json:"attempt_count"`
	ProviderRequestID string            `json:"provider_request_id,omitempty"`
	Usage             EmbeddingUsage    `json:"usage"`
	Budget            EmbeddingBudget   `json:"budget"`
	Failure           *EmbeddingFailure `json:"failure,omitempty"`
	VectorHash        string            `json:"vector_hash,omitempty"`
	VectorDimension   int               `json:"vector_dimension,omitempty"`
	Vector            []float32         `json:"-"`
	RequestEvidence   json.RawMessage   `json:"request_evidence,omitempty"`
	ResponseEvidence  json.RawMessage   `json:"response_evidence,omitempty"`
	RetentionClass    string            `json:"retention_class,omitempty"`
	RetentionDeadline *time.Time        `json:"retention_deadline,omitempty"`
	ExpiredAt         *time.Time        `json:"expired_at,omitempty"`
	TombstoneHash     string            `json:"tombstone_hash,omitempty"`
	CreatedAt         time.Time         `json:"created_at"`
	StartedAt         *time.Time        `json:"started_at,omitempty"`
	FinishedAt        *time.Time        `json:"finished_at,omitempty"`
	DurationMS        int64             `json:"duration_ms"`
	UpdatedAt         time.Time         `json:"updated_at"`
}

type EmbeddingCallStart struct {
	Record   EmbeddingCallRecord
	Existing bool
}

type EmbeddingCallResult struct {
	WorkspaceID       string
	RequestID         string
	TaskOwnerID       string
	TaskFence         uint64
	Status            string
	AttemptCount      int
	ProviderRequestID string
	Usage             EmbeddingUsage
	Vector            []float32
	Failure           *EmbeddingFailure
	ResponseEvidence  json.RawMessage
}

// EmbeddingReconciliationRequest is the no-raw-input identity supplied to a
// provider that can prove the outcome of an earlier ambiguous request. It is
// intentionally not an EmbeddingRequest: recovery must never reconstruct or
// resend source text.
type EmbeddingReconciliationRequest struct {
	SchemaVersion     int             `json:"schema_version"`
	WorkspaceID       string          `json:"workspace_id"`
	RequestID         string          `json:"request_id"`
	IdempotencyKey    string          `json:"idempotency_key"`
	RequestHash       string          `json:"request_hash"`
	CausationID       string          `json:"causation_id,omitempty"`
	CorrelationID     string          `json:"correlation_id,omitempty"`
	Provider          ProviderRef     `json:"provider"`
	Model             string          `json:"model"`
	SourceKind        string          `json:"source_kind"`
	SourceID          string          `json:"source_id,omitempty"`
	SourceHash        string          `json:"source_hash"`
	ProviderRequestID string          `json:"provider_request_id,omitempty"`
	Actor             ActorRef        `json:"actor"`
	Task              *EntityRef      `json:"task,omitempty"`
	TaskOwnerID       string          `json:"task_owner_id,omitempty"`
	TaskFence         uint64          `json:"task_fence,omitempty"`
	Session           *EntityRef      `json:"session,omitempty"`
	Budget            EmbeddingBudget `json:"budget"`
}

// Normalize validates the bounded, hash-only recovery identity.
func (r *EmbeddingReconciliationRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("embedding reconciliation request is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = EmbeddingSchemaVersion
	}
	if r.SchemaVersion != EmbeddingSchemaVersion {
		return fmt.Errorf("unsupported embedding reconciliation schema_version %d", r.SchemaVersion)
	}
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	r.RequestID = strings.TrimSpace(r.RequestID)
	r.IdempotencyKey = strings.TrimSpace(r.IdempotencyKey)
	r.RequestHash = strings.ToLower(strings.TrimSpace(r.RequestHash))
	r.ProviderRequestID = strings.TrimSpace(r.ProviderRequestID)
	if r.WorkspaceID == "" || r.RequestID == "" || r.IdempotencyKey == "" || len(r.RequestHash) != 64 || !canonicalSHA256(r.RequestHash) {
		return fmt.Errorf("embedding reconciliation identity is required and bounded")
	}
	if r.ProviderRequestID == "" {
		return fmt.Errorf("embedding reconciliation provider_request_id is required")
	}
	if err := normalizeDomainActor(&r.Actor, r.WorkspaceID); err != nil {
		return err
	}
	if err := normalizeDomainEntity(r.Task, "task", r.WorkspaceID); err != nil {
		return err
	}
	if r.Task != nil {
		if (strings.TrimSpace(r.TaskOwnerID) == "") != (r.TaskFence == 0) {
			return fmt.Errorf("embedding reconciliation task owner and fence must be supplied together")
		}
	} else if r.TaskOwnerID != "" || r.TaskFence != 0 {
		return fmt.Errorf("embedding reconciliation task fence requires a task")
	}
	if err := normalizeDomainEntity(r.Session, "session", r.WorkspaceID); err != nil {
		return err
	}
	r.Provider.Provider = strings.ToLower(strings.TrimSpace(r.Provider.Provider))
	r.Provider.Endpoint = strings.TrimSpace(r.Provider.Endpoint)
	r.Model, r.SourceKind, r.SourceID = strings.TrimSpace(r.Model), strings.ToLower(strings.TrimSpace(r.SourceKind)), strings.TrimSpace(r.SourceID)
	r.SourceHash = strings.ToLower(strings.TrimSpace(r.SourceHash))
	if r.Provider.Provider == "" || r.Model == "" || r.SourceKind == "" || len(r.SourceHash) != 64 || !canonicalSHA256(r.SourceHash) {
		return fmt.Errorf("embedding reconciliation provider and source identity are required")
	}
	r.Provider.Model = r.Model
	return nil
}

// EmbeddingReconciliationResult is a provider-proven result. A successful
// result must carry the same source identity and a provider request identity;
// failure results carry no vector.
type EmbeddingReconciliationResult struct {
	WorkspaceID       string
	RequestID         string
	Actor             ActorRef
	TaskOwnerID       string
	TaskFence         uint64
	Status            string
	Provider          ProviderRef
	SourceHash        string
	ProviderRequestID string
	Usage             EmbeddingUsage
	Vector            []float32 `json:"-"`
	Failure           *EmbeddingFailure
	ResponseEvidence  json.RawMessage
}

// EmbeddingRecoveryFinalizeRequest is the hash-only, fenced command used by
// the local recovery coordinator. The provider proof is carried in Result;
// the request contains no source text, credentials, or caller-supplied vector
// outside that already-validated provider result.
type EmbeddingRecoveryFinalizeRequest struct {
	WorkspaceID           string                        `json:"workspace_id"`
	RequestID             string                        `json:"request_id"`
	OwnerID               string                        `json:"owner_id"`
	Fence                 uint64                        `json:"fence"`
	ExpectedEffectVersion int64                         `json:"expected_effect_version"`
	ExpectedLinkVersion   int64                         `json:"expected_link_version"`
	IdempotencyKey        string                        `json:"idempotency_key"`
	Actor                 ActorRef                      `json:"actor"`
	Result                EmbeddingReconciliationResult `json:"result"`
}

// Normalize validates the bounded local recovery command. The expected link
// version is intentionally required so a stale operator cannot infer current
// state from a previous read and still advance the link.
func (r *EmbeddingRecoveryFinalizeRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("embedding recovery finalize request is nil")
	}
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	r.RequestID = strings.TrimSpace(r.RequestID)
	r.OwnerID = strings.TrimSpace(r.OwnerID)
	r.IdempotencyKey = strings.TrimSpace(r.IdempotencyKey)
	if r.WorkspaceID == "" || r.RequestID == "" || r.OwnerID == "" || r.Fence == 0 || r.ExpectedEffectVersion < 1 || r.ExpectedLinkVersion < 1 || r.IdempotencyKey == "" || len(r.IdempotencyKey) > MaxIdempotencyLength {
		return fmt.Errorf("embedding recovery finalize identity is required and bounded")
	}
	if err := normalizeDomainActor(&r.Actor, r.WorkspaceID); err != nil {
		return err
	}
	if err := r.Result.Normalize(); err != nil {
		return err
	}
	if r.Result.WorkspaceID != r.WorkspaceID || r.Result.RequestID != r.RequestID {
		return fmt.Errorf("embedding recovery result identity does not match command")
	}
	if r.Result.Actor.ID == "" {
		r.Result.Actor = r.Actor
	}
	if r.Result.TaskOwnerID != "" && r.Result.TaskFence == 0 {
		return fmt.Errorf("embedding recovery result task fence is incomplete")
	}
	return nil
}

// Normalize validates the provider proof carried by a recovery finalization.
// It is deliberately separate from EmbeddingReconciliationRequest because it
// may contain a vector only after a trusted provider adapter has verified the
// source and provider request identity.
func (r *EmbeddingReconciliationResult) Normalize() error {
	if r == nil {
		return fmt.Errorf("embedding reconciliation result is nil")
	}
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	r.RequestID = strings.TrimSpace(r.RequestID)
	r.SourceHash = strings.ToLower(strings.TrimSpace(r.SourceHash))
	r.Provider.Provider = strings.ToLower(strings.TrimSpace(r.Provider.Provider))
	r.Provider.Endpoint = strings.TrimSpace(r.Provider.Endpoint)
	r.Provider.Model = strings.TrimSpace(r.Provider.Model)
	r.ProviderRequestID = strings.TrimSpace(r.ProviderRequestID)
	if r.WorkspaceID == "" || r.RequestID == "" || r.Provider.Provider == "" || r.Provider.Model == "" || r.ProviderRequestID == "" || len(r.SourceHash) != 64 || !canonicalSHA256(r.SourceHash) {
		return fmt.Errorf("embedding reconciliation result identity is required")
	}
	if r.Status != EmbeddingCallSucceeded && r.Status != EmbeddingCallFailed {
		return fmt.Errorf("invalid embedding reconciliation result status %q", r.Status)
	}
	if r.Status == EmbeddingCallSucceeded {
		if _, err := EmbeddingVectorHash(r.Vector); err != nil {
			return err
		}
	} else if len(r.Vector) != 0 {
		return fmt.Errorf("failed embedding reconciliation cannot contain a vector")
	}
	if len(r.ResponseEvidence) > 16<<10 {
		return fmt.Errorf("embedding reconciliation evidence exceeds 16KiB")
	}
	return nil
}

// EmbeddingRetentionSweepRequest bounds cleanup of derived query vectors.
// DryRun is read-only and never advances a cursor or changes authoritative
// embedding-call history.
type EmbeddingRetentionSweepRequest struct {
	WorkspaceID string    `json:"workspace_id"`
	Before      time.Time `json:"before"`
	BatchSize   int       `json:"batch_size"`
	DryRun      bool      `json:"dry_run"`
	Actor       ActorRef  `json:"actor"`
}

type EmbeddingRetentionSweepResult struct {
	WorkspaceID string `json:"workspace_id"`
	BatchSize   int    `json:"batch_size"`
	Candidates  int    `json:"candidates"`
	Expired     int    `json:"expired"`
	DryRun      bool   `json:"dry_run"`
}

// EmbeddingTargetAttachment records the immutable relationship between a
// successful provider result and a derived domain projection. The target row
// remains authoritative for its domain; this record makes the vector lineage
// auditable and lets a projector resume after a crash without re-running the
// provider.
type EmbeddingTargetAttachment struct {
	WorkspaceID string    `json:"workspace_id"`
	RequestID   string    `json:"request_id"`
	TargetKind  string    `json:"target_kind"`
	TargetID    string    `json:"target_id"`
	SourceHash  string    `json:"source_hash"`
	VectorHash  string    `json:"vector_hash"`
	AttachedAt  time.Time `json:"attached_at,omitempty"`
}

// EmbeddingQueryUse attributes one bounded retrieval use of a canonical query
// embedding. It stores hashes and measured/estimated usage only; raw query
// text and vectors remain outside this ledger.
type EmbeddingQueryUse struct {
	SchemaVersion      int            `json:"schema_version"`
	ID                 string         `json:"id"`
	WorkspaceID        string         `json:"workspace_id"`
	IdempotencyKey     string         `json:"idempotency_key"`
	RequestID          string         `json:"request_id"`
	EmbeddingRequestID string         `json:"embedding_request_id"`
	SourceHash         string         `json:"source_hash"`
	Provider           ProviderRef    `json:"provider"`
	Actor              ActorRef       `json:"actor"`
	Route              string         `json:"route"`
	GateReason         string         `json:"gate_reason"`
	CacheHit           bool           `json:"cache_hit"`
	DuplicateWork      bool           `json:"duplicate_work"`
	Usage              EmbeddingUsage `json:"usage"`
	CostUSD            float64        `json:"cost_usd,omitempty"`
	CostKnown          bool           `json:"cost_known"`
	UsageMeasured      bool           `json:"usage_measured"`
	UsageEstimated     bool           `json:"usage_estimated"`
	CreatedAt          time.Time      `json:"created_at,omitempty"`
}

func (u *EmbeddingQueryUse) Normalize() error {
	if u == nil {
		return fmt.Errorf("embedding query use is nil")
	}
	if u.SchemaVersion == 0 {
		u.SchemaVersion = EmbeddingSchemaVersion
	}
	if u.SchemaVersion != EmbeddingSchemaVersion {
		return fmt.Errorf("unsupported embedding query use schema_version %d", u.SchemaVersion)
	}
	u.ID = strings.TrimSpace(u.ID)
	u.WorkspaceID = strings.TrimSpace(u.WorkspaceID)
	u.IdempotencyKey = strings.TrimSpace(u.IdempotencyKey)
	u.RequestID = strings.TrimSpace(u.RequestID)
	u.EmbeddingRequestID = strings.TrimSpace(u.EmbeddingRequestID)
	u.SourceHash = strings.ToLower(strings.TrimSpace(u.SourceHash))
	u.Route = strings.ToLower(strings.TrimSpace(u.Route))
	u.GateReason = strings.ToLower(strings.TrimSpace(u.GateReason))
	if u.ID == "" {
		u.ID = NewID("embedding-use")
	}
	if u.WorkspaceID == "" || u.IdempotencyKey == "" || u.RequestID == "" || u.EmbeddingRequestID == "" || u.Route == "" || u.GateReason == "" || len(u.SourceHash) != 64 || !canonicalSHA256(u.SourceHash) {
		return fmt.Errorf("embedding query use identity is required")
	}
	if err := normalizeDomainActor(&u.Actor, u.WorkspaceID); err != nil {
		return err
	}
	u.Provider.Provider = strings.ToLower(strings.TrimSpace(u.Provider.Provider))
	u.Provider.Model = strings.TrimSpace(u.Provider.Model)
	if u.Provider.Provider == "" || u.Provider.Model == "" || len(u.Route) > MaxDomainNameLength || len(u.GateReason) > MaxDomainNameLength || u.CostUSD < 0 {
		return fmt.Errorf("embedding query use provider or budget is invalid")
	}
	if u.UsageMeasured && u.UsageEstimated {
		return fmt.Errorf("embedding query usage cannot be both measured and estimated")
	}
	return nil
}

// EmbeddingSourceHash returns the stable identity of a bounded input.
func EmbeddingSourceHash(text string) string {
	digest := sha256.Sum256([]byte(text))
	return hex.EncodeToString(digest[:])
}

// EmbeddingVectorHash identifies a vector from canonical IEEE-754 float32
// bits. It deliberately avoids pgvector's textual representation, whose
// formatting is not a cryptographic canonicalization boundary.
func EmbeddingVectorHash(vector []float32) (string, error) {
	if len(vector) != EmbeddingDimension {
		return "", fmt.Errorf("embedding vector dimension must be %d", EmbeddingDimension)
	}
	canonical := make([]byte, 4*len(vector))
	for i, value := range vector {
		if math.IsNaN(float64(value)) || math.IsInf(float64(value), 0) {
			return "", fmt.Errorf("embedding vector contains a non-finite value at index %d", i)
		}
		binary.BigEndian.PutUint32(canonical[i*4:], math.Float32bits(value))
	}
	digest := sha256.Sum256(canonical)
	return hex.EncodeToString(digest[:]), nil
}
