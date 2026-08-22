package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// WorkReceiptSchemaVersion is the durable contract version for a verified
// repository-work receipt. A receipt is a derived verification envelope; the
// task, event, evidence, artifact, model, tool, and replay stores remain the
// authorities for their respective records.
const WorkReceiptSchemaVersion = 1

const (
	MaxWorkReceiptSteps       = 64
	MaxWorkReceiptReferences  = 128
	MaxWorkReceiptEvidence    = 128
	MaxWorkReceiptArtifacts   = 128
	MaxWorkReceiptMetadata    = 16
	MaxWorkReceiptIDLength    = 128
	MaxWorkReceiptNameLength  = 128
	MaxWorkReceiptRoleLength  = 128
	MaxWorkReceiptHashLength  = 64
	MaxWorkReceiptPayloadSize = 1 << 20

	DefaultWorkReceiptDisclosureBytes  = 32 << 10
	DefaultWorkReceiptDisclosureTokens = 8 << 10
	DefaultWorkReceiptDisclosureItems  = 64
	MaxWorkReceiptDisclosureBytes      = 1 << 20
	MaxWorkReceiptDisclosureTokens     = 262144
	MaxWorkReceiptDisclosureItems      = 128
)

const (
	WorkReceiptStatusVerified = "verified"
	WorkReceiptStatusRejected = "rejected"

	WorkReceiptDisclosureGist   = "gist"
	WorkReceiptDisclosureDetail = "detail"
	WorkReceiptDisclosureRaw    = "raw"
)

const (
	WorkReceiptReferenceTask             = "task"
	WorkReceiptReferenceEvent            = "event"
	WorkReceiptReferenceAgentRun         = "agent_run"
	WorkReceiptReferenceRetrievalSurface = "retrieval_surface"
	WorkReceiptReferenceModelCall        = "model_call"
	WorkReceiptReferenceToolRun          = "tool_run"
	WorkReceiptReferenceEvidence         = "evidence"
	WorkReceiptReferenceArtifact         = "artifact"
	WorkReceiptReferenceValidation       = "validation"
	WorkReceiptReferenceObservation      = "observation"
	WorkReceiptReferenceCost             = "cost"
	WorkReceiptReferenceReplay           = "replay"
)

// WorkReceiptEvidence is a typed, hash-only link to immutable evidence. Raw
// evidence is disclosed through EvidenceStore, never copied into a receipt.
type WorkReceiptEvidence struct {
	ID              int64  `json:"id"`
	WorkspaceID     string `json:"workspace_id"`
	EvidenceHash    string `json:"evidence_hash"`
	SourceReference string `json:"source_reference,omitempty"`
	Role            string `json:"role,omitempty"`
}

// WorkReceiptArtifact is a typed link to content-addressed immutable bytes.
// The receipt carries the hash and identity, not a second copy of the bytes.
type WorkReceiptArtifact struct {
	ID          int64  `json:"id"`
	ArtifactID  int64  `json:"artifact_id"`
	WorkspaceID string `json:"workspace_id"`
	ContentHash string `json:"content_hash"`
	SourceKind  string `json:"source_kind,omitempty"`
	SourceID    string `json:"source_id,omitempty"`
	Role        string `json:"role,omitempty"`
}

// WorkReceiptReference identifies one authoritative record used to verify a
// step. Hash is the source record's request/content/payload hash where one is
// available; identity-only records still remain workspace-scoped and are
// checked by the store before finalization.
type WorkReceiptReference struct {
	WorkspaceID string `json:"workspace_id"`
	Kind        string `json:"kind"`
	SourceID    string `json:"source_id"`
	Role        string `json:"role,omitempty"`
	Hash        string `json:"hash,omitempty"`
}

// WorkReceiptStep is the bounded, deterministic summary of one phase of a
// run. It contains measurements and hashes, never prompts, credentials, or
// arbitrary output text.
type WorkReceiptStep struct {
	Ordinal          int               `json:"ordinal"`
	ID               string            `json:"id"`
	Name             string            `json:"name"`
	Kind             string            `json:"kind"`
	Status           string            `json:"status"`
	SourceKind       string            `json:"source_kind,omitempty"`
	SourceID         string            `json:"source_id,omitempty"`
	SourceHash       string            `json:"source_hash,omitempty"`
	InputHash        string            `json:"input_hash,omitempty"`
	OutputHash       string            `json:"output_hash,omitempty"`
	ReferenceRoles   []string          `json:"reference_roles,omitempty"`
	DurationMS       int64             `json:"duration_ms,omitempty"`
	Attempts         int               `json:"attempts,omitempty"`
	RetryCount       int               `json:"retry_count,omitempty"`
	DuplicateWork    bool              `json:"duplicate_work,omitempty"`
	ExternalEffect   bool              `json:"external_effect,omitempty"`
	ExternalBoundary string            `json:"external_boundary,omitempty"`
	Metadata         map[string]string `json:"metadata,omitempty"`
}

// WorkReceiptCost keeps measured and estimated accounting separate. A receipt
// must not turn missing provider usage into an exact cost claim.
type WorkReceiptCost struct {
	ModelUSD             float64 `json:"model_usd"`
	ToolUSD              float64 `json:"tool_usd"`
	RetrievalUSD         float64 `json:"retrieval_usd"`
	ArtifactUSD          float64 `json:"artifact_usd"`
	RetryUSD             float64 `json:"retry_usd"`
	DuplicateWorkUSD     float64 `json:"duplicate_work_usd"`
	TotalUSD             float64 `json:"total_usd"`
	Measured             bool    `json:"measured"`
	Estimated            bool    `json:"estimated"`
	UnknownProviderUsage bool    `json:"unknown_provider_usage"`
}

// WorkReceiptVerification is the durable result of reference and replay
// checks. Failure codes are stable categories, not unbounded driver errors.
type WorkReceiptVerification struct {
	Status              string    `json:"status"`
	ReceiptHash         string    `json:"receipt_hash"`
	ReplayHash          string    `json:"replay_hash,omitempty"`
	ReplayVerified      bool      `json:"replay_verified"`
	ReferencesChecked   int       `json:"references_checked"`
	ReferencesResolved  int       `json:"references_resolved"`
	IntegrityChecks     int       `json:"integrity_checks"`
	IntegrityFailures   []string  `json:"integrity_failures,omitempty"`
	ExternalBoundaries  []string  `json:"external_boundaries,omitempty"`
	AtLeastOnceDeclared bool      `json:"at_least_once_declared"`
	VerifiedAt          time.Time `json:"verified_at"`
}

// WorkReceipt is the immutable, machine-verifiable record behind a future
// human-facing Verified Change Packet.
type WorkReceipt struct {
	SchemaVersion      int                     `json:"schema_version"`
	ID                 string                  `json:"id"`
	WorkspaceID        string                  `json:"workspace_id"`
	WorkKind           string                  `json:"work_kind"`
	WorkID             string                  `json:"work_id"`
	RequestID          string                  `json:"request_id"`
	IdempotencyKey     string                  `json:"idempotency_key"`
	RequestHash        string                  `json:"request_hash"`
	CanonicalHash      string                  `json:"canonical_hash"`
	Status             string                  `json:"status"`
	Actor              ActorRef                `json:"actor"`
	Task               *EntityRef              `json:"task,omitempty"`
	Session            *EntityRef              `json:"session,omitempty"`
	TaskOwnerID        string                  `json:"task_owner_id,omitempty"`
	TaskFence          uint64                  `json:"task_fence,omitempty"`
	SourceManifestHash string                  `json:"source_manifest_hash,omitempty"`
	ReplayHash         string                  `json:"replay_hash,omitempty"`
	Steps              []WorkReceiptStep       `json:"steps"`
	Evidence           []WorkReceiptEvidence   `json:"evidence,omitempty"`
	Artifacts          []WorkReceiptArtifact   `json:"artifacts,omitempty"`
	References         []WorkReceiptReference  `json:"references,omitempty"`
	Cost               WorkReceiptCost         `json:"cost"`
	Verification       WorkReceiptVerification `json:"verification"`
	CreatedAt          time.Time               `json:"created_at"`
	VerifiedAt         time.Time               `json:"verified_at"`
}

// WorkReceiptFinalizeRequest is the authenticated input for one receipt. The
// server supplies actor identity; callers supply only bounded hashes and
// authoritative record identities.
type WorkReceiptFinalizeRequest struct {
	SchemaVersion      int                    `json:"schema_version,omitempty"`
	ReceiptID          string                 `json:"receipt_id,omitempty"`
	RequestID          string                 `json:"request_id,omitempty"`
	IdempotencyKey     string                 `json:"idempotency_key"`
	WorkspaceID        string                 `json:"workspace_id"`
	Actor              ActorRef               `json:"actor,omitempty"`
	WorkKind           string                 `json:"work_kind"`
	WorkID             string                 `json:"work_id"`
	Task               *EntityRef             `json:"task,omitempty"`
	Session            *EntityRef             `json:"session,omitempty"`
	TaskOwnerID        string                 `json:"task_owner_id,omitempty"`
	TaskFence          uint64                 `json:"task_fence,omitempty"`
	SourceManifestHash string                 `json:"source_manifest_hash,omitempty"`
	ReplayHash         string                 `json:"replay_hash,omitempty"`
	Steps              []WorkReceiptStep      `json:"steps"`
	Evidence           []WorkReceiptEvidence  `json:"evidence,omitempty"`
	Artifacts          []WorkReceiptArtifact  `json:"artifacts,omitempty"`
	References         []WorkReceiptReference `json:"references,omitempty"`
	Cost               WorkReceiptCost        `json:"cost,omitempty"`
}

// WorkReceiptDisclosureRequest selects a bounded view of one receipt.
type WorkReceiptDisclosureRequest struct {
	SchemaVersion int    `json:"schema_version,omitempty"`
	WorkspaceID   string `json:"workspace_id"`
	ReceiptID     string `json:"receipt_id"`
	Level         string `json:"level"`
	MaxBytes      int    `json:"max_bytes,omitempty"`
	MaxTokens     int    `json:"max_tokens,omitempty"`
	MaxItems      int    `json:"max_items,omitempty"`
}

// WorkReceiptGist is safe for compact operator lists.
type WorkReceiptGist struct {
	ID                 string `json:"id"`
	WorkspaceID        string `json:"workspace_id"`
	WorkKind           string `json:"work_kind"`
	WorkID             string `json:"work_id"`
	Status             string `json:"status"`
	CanonicalHash      string `json:"canonical_hash"`
	SourceManifestHash string `json:"source_manifest_hash,omitempty"`
	ReplayHash         string `json:"replay_hash,omitempty"`
	StepCount          int    `json:"step_count"`
	ReferenceCount     int    `json:"reference_count"`
}

// WorkReceiptDisclosureResult preserves the canonical hash across disclosure
// levels. Raw is the canonical redacted receipt JSON, not original work data.
type WorkReceiptDisclosureResult struct {
	SchemaVersion   int              `json:"schema_version"`
	WorkspaceID     string           `json:"workspace_id"`
	ReceiptID       string           `json:"receipt_id"`
	Level           string           `json:"level"`
	CanonicalHash   string           `json:"canonical_hash"`
	ContentViewHash string           `json:"content_view_hash"`
	Gist            *WorkReceiptGist `json:"gist,omitempty"`
	Detail          *WorkReceipt     `json:"detail,omitempty"`
	Raw             json.RawMessage  `json:"raw,omitempty"`
	Truncated       bool             `json:"truncated"`
	TotalBytes      int              `json:"total_bytes"`
	TotalTokens     int              `json:"total_tokens"`
	TotalItems      int              `json:"total_items"`
}

// Normalize validates and canonicalizes a finalization request.
func (r *WorkReceiptFinalizeRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("work receipt request is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = WorkReceiptSchemaVersion
	}
	if r.SchemaVersion != WorkReceiptSchemaVersion {
		return fmt.Errorf("unsupported work receipt schema_version %d", r.SchemaVersion)
	}
	r.ReceiptID = strings.TrimSpace(r.ReceiptID)
	r.RequestID = strings.TrimSpace(r.RequestID)
	r.IdempotencyKey = strings.TrimSpace(r.IdempotencyKey)
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	r.WorkKind = strings.TrimSpace(r.WorkKind)
	r.WorkID = strings.TrimSpace(r.WorkID)
	r.TaskOwnerID = strings.TrimSpace(r.TaskOwnerID)
	r.SourceManifestHash = normalizeReceiptHash(r.SourceManifestHash)
	r.ReplayHash = normalizeReceiptHash(r.ReplayHash)
	if r.RequestID == "" {
		r.RequestID = NewID("receipt-request")
	}
	if r.ReceiptID == "" {
		r.ReceiptID = NewID("receipt")
	}
	if r.WorkspaceID == "" || r.IdempotencyKey == "" || r.WorkKind == "" || r.WorkID == "" {
		return fmt.Errorf("workspace_id, idempotency_key, work_kind, and work_id are required")
	}
	if len(r.ReceiptID) > MaxWorkReceiptIDLength || len(r.RequestID) > MaxWorkReceiptIDLength || len(r.IdempotencyKey) > MaxIdempotencyLength || len(r.WorkKind) > MaxWorkReceiptNameLength || len(r.WorkID) > MaxWorkReceiptIDLength {
		return fmt.Errorf("work receipt identity is too large")
	}
	if r.Task != nil && (r.Task.Kind != "task" || strings.TrimSpace(r.Task.ID) == "" || r.Task.WorkspaceID != r.WorkspaceID) {
		return fmt.Errorf("task reference must be workspace-scoped")
	}
	if r.Session != nil && (r.Session.Kind != "session" || strings.TrimSpace(r.Session.ID) == "" || r.Session.WorkspaceID != r.WorkspaceID) {
		return fmt.Errorf("session reference must be workspace-scoped")
	}
	if r.Task != nil && (r.TaskOwnerID == "" || r.TaskFence == 0) {
		return fmt.Errorf("task-bound receipt requires task_owner_id and task_fence")
	}
	if len(r.Steps) == 0 || len(r.Steps) > MaxWorkReceiptSteps {
		return fmt.Errorf("work receipt must contain between 1 and %d steps", MaxWorkReceiptSteps)
	}
	if len(r.References) > MaxWorkReceiptReferences || len(r.Evidence) > MaxWorkReceiptEvidence || len(r.Artifacts) > MaxWorkReceiptArtifacts {
		return fmt.Errorf("work receipt reference count exceeds configured bounds")
	}
	for i := range r.Steps {
		if err := normalizeWorkReceiptStep(&r.Steps[i], i); err != nil {
			return fmt.Errorf("steps[%d]: %w", i, err)
		}
	}
	seenOrdinals := make(map[int]struct{}, len(r.Steps))
	for _, step := range r.Steps {
		if _, exists := seenOrdinals[step.Ordinal]; exists {
			return fmt.Errorf("work receipt contains duplicate step ordinal %d", step.Ordinal)
		}
		seenOrdinals[step.Ordinal] = struct{}{}
	}
	for i := range r.References {
		if err := normalizeWorkReceiptReference(&r.References[i], r.WorkspaceID); err != nil {
			return fmt.Errorf("references[%d]: %w", i, err)
		}
	}
	for i := range r.Evidence {
		if err := normalizeWorkReceiptEvidence(&r.Evidence[i], r.WorkspaceID); err != nil {
			return fmt.Errorf("evidence[%d]: %w", i, err)
		}
	}
	for i := range r.Artifacts {
		if err := normalizeWorkReceiptArtifact(&r.Artifacts[i], r.WorkspaceID); err != nil {
			return fmt.Errorf("artifacts[%d]: %w", i, err)
		}
	}
	// Typed evidence/artifact links are also represented in the normalized
	// reference set so the SQL link table can answer provenance queries without
	// parsing the canonical payload.
	for _, evidence := range r.Evidence {
		ref := WorkReceiptReference{WorkspaceID: r.WorkspaceID, Kind: WorkReceiptReferenceEvidence, SourceID: strconv.FormatInt(evidence.ID, 10), Role: evidence.Role, Hash: evidence.EvidenceHash}
		if !containsReceiptReference(r.References, ref) {
			r.References = append(r.References, ref)
		}
	}
	for _, artifact := range r.Artifacts {
		ref := WorkReceiptReference{WorkspaceID: r.WorkspaceID, Kind: WorkReceiptReferenceArtifact, SourceID: strconv.FormatInt(artifact.ArtifactID, 10), Role: artifact.Role, Hash: artifact.ContentHash}
		if !containsReceiptReference(r.References, ref) {
			r.References = append(r.References, ref)
		}
	}
	if len(r.References) > MaxWorkReceiptReferences {
		return fmt.Errorf("work receipt reference count exceeds configured bounds")
	}
	sort.Slice(r.Steps, func(i, j int) bool { return r.Steps[i].Ordinal < r.Steps[j].Ordinal })
	sort.Slice(r.References, func(i, j int) bool {
		return receiptReferenceKey(r.References[i]) < receiptReferenceKey(r.References[j])
	})
	sort.Slice(r.Evidence, func(i, j int) bool { return receiptEvidenceKey(r.Evidence[i]) < receiptEvidenceKey(r.Evidence[j]) })
	sort.Slice(r.Artifacts, func(i, j int) bool { return receiptArtifactKey(r.Artifacts[i]) < receiptArtifactKey(r.Artifacts[j]) })
	if err := normalizeWorkReceiptCost(&r.Cost); err != nil {
		return err
	}
	return nil
}

// ToReceipt converts the normalized request into the immutable receipt shape.
func (r WorkReceiptFinalizeRequest) ToReceipt(now time.Time) (WorkReceipt, error) {
	r = cloneWorkReceiptFinalizeRequest(r)
	if err := r.Normalize(); err != nil {
		return WorkReceipt{}, err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	receipt := WorkReceipt{
		SchemaVersion: r.SchemaVersion, ID: r.ReceiptID, WorkspaceID: r.WorkspaceID,
		WorkKind: r.WorkKind, WorkID: r.WorkID, RequestID: r.RequestID,
		IdempotencyKey: r.IdempotencyKey, Status: WorkReceiptStatusVerified,
		Actor: r.Actor, Task: r.Task, Session: r.Session, TaskOwnerID: r.TaskOwnerID,
		TaskFence: r.TaskFence, SourceManifestHash: r.SourceManifestHash,
		ReplayHash: r.ReplayHash, Steps: append([]WorkReceiptStep(nil), r.Steps...),
		Evidence:   append([]WorkReceiptEvidence(nil), r.Evidence...),
		Artifacts:  append([]WorkReceiptArtifact(nil), r.Artifacts...),
		References: append([]WorkReceiptReference(nil), r.References...), Cost: r.Cost,
		CreatedAt: now, VerifiedAt: now,
	}
	receipt.Verification = WorkReceiptVerification{Status: WorkReceiptStatusVerified, ReplayHash: r.ReplayHash, ReplayVerified: r.ReplayHash != "", AtLeastOnceDeclared: hasExternalBoundary(r.Steps), VerifiedAt: now}
	receipt.CanonicalHash = receipt.StableHash()
	receipt.RequestHash = hashReceiptJSON(receipt.requestPayload())
	receipt.Verification.ReceiptHash = receipt.CanonicalHash
	return receipt, nil
}

func cloneWorkReceiptFinalizeRequest(r WorkReceiptFinalizeRequest) WorkReceiptFinalizeRequest {
	if r.Task != nil {
		task := *r.Task
		r.Task = &task
	}
	if r.Session != nil {
		session := *r.Session
		r.Session = &session
	}
	r.Steps = append([]WorkReceiptStep(nil), r.Steps...)
	for i := range r.Steps {
		r.Steps[i].ReferenceRoles = append([]string(nil), r.Steps[i].ReferenceRoles...)
		if r.Steps[i].Metadata != nil {
			metadata := r.Steps[i].Metadata
			r.Steps[i].Metadata = make(map[string]string, len(metadata))
			for key, value := range metadata {
				r.Steps[i].Metadata[key] = value
			}
		}
	}
	r.Evidence = append([]WorkReceiptEvidence(nil), r.Evidence...)
	r.Artifacts = append([]WorkReceiptArtifact(nil), r.Artifacts...)
	r.References = append([]WorkReceiptReference(nil), r.References...)
	return r
}

// Normalize validates and applies disclosure defaults.
func (r WorkReceiptDisclosureRequest) Normalize() (WorkReceiptDisclosureRequest, error) {
	r.WorkspaceID, r.ReceiptID, r.Level = strings.TrimSpace(r.WorkspaceID), strings.TrimSpace(r.ReceiptID), strings.ToLower(strings.TrimSpace(r.Level))
	if r.SchemaVersion == 0 {
		r.SchemaVersion = WorkReceiptSchemaVersion
	}
	if r.SchemaVersion != WorkReceiptSchemaVersion || r.WorkspaceID == "" || r.ReceiptID == "" {
		return WorkReceiptDisclosureRequest{}, fmt.Errorf("schema_version, workspace_id, and receipt_id are required")
	}
	if r.Level == "" {
		r.Level = WorkReceiptDisclosureGist
	}
	if r.Level != WorkReceiptDisclosureGist && r.Level != WorkReceiptDisclosureDetail && r.Level != WorkReceiptDisclosureRaw {
		return WorkReceiptDisclosureRequest{}, fmt.Errorf("unsupported work receipt disclosure level %q", r.Level)
	}
	if r.MaxBytes == 0 {
		r.MaxBytes = DefaultWorkReceiptDisclosureBytes
	}
	if r.MaxTokens == 0 {
		r.MaxTokens = DefaultWorkReceiptDisclosureTokens
	}
	if r.MaxItems == 0 {
		r.MaxItems = DefaultWorkReceiptDisclosureItems
	}
	if r.MaxBytes < 1 || r.MaxBytes > MaxWorkReceiptDisclosureBytes || r.MaxTokens < 1 || r.MaxTokens > MaxWorkReceiptDisclosureTokens || r.MaxItems < 1 || r.MaxItems > MaxWorkReceiptDisclosureItems {
		return WorkReceiptDisclosureRequest{}, fmt.Errorf("work receipt disclosure budget exceeds configured bounds")
	}
	return r, nil
}

// Normalize makes a stored receipt safe to hash and disclose.
func (r *WorkReceipt) Normalize() error {
	if r == nil {
		return fmt.Errorf("work receipt is nil")
	}
	req := WorkReceiptFinalizeRequest{
		SchemaVersion: r.SchemaVersion, ReceiptID: r.ID, RequestID: r.RequestID,
		IdempotencyKey: r.IdempotencyKey, WorkspaceID: r.WorkspaceID, Actor: r.Actor,
		WorkKind: r.WorkKind, WorkID: r.WorkID, Task: r.Task, Session: r.Session,
		TaskOwnerID: r.TaskOwnerID, TaskFence: r.TaskFence, SourceManifestHash: r.SourceManifestHash,
		ReplayHash: r.ReplayHash, Steps: r.Steps, Evidence: r.Evidence, Artifacts: r.Artifacts,
		References: r.References, Cost: r.Cost,
	}
	if err := req.Normalize(); err != nil {
		return err
	}
	r.Steps, r.Evidence, r.Artifacts, r.References, r.Cost = req.Steps, req.Evidence, req.Artifacts, req.References, req.Cost
	r.SchemaVersion, r.ID, r.WorkspaceID, r.WorkKind, r.WorkID = req.SchemaVersion, req.ReceiptID, req.WorkspaceID, req.WorkKind, req.WorkID
	r.Status = strings.TrimSpace(r.Status)
	if r.Status == "" {
		r.Status = WorkReceiptStatusVerified
	}
	if r.Status != WorkReceiptStatusVerified && r.Status != WorkReceiptStatusRejected {
		return fmt.Errorf("invalid work receipt status %q", r.Status)
	}
	if r.CanonicalHash == "" {
		r.CanonicalHash = r.StableHash()
	}
	if !canonicalSHA256(r.CanonicalHash) {
		return fmt.Errorf("canonical_hash must be a lowercase sha256")
	}
	if r.RequestHash == "" {
		r.RequestHash = hashReceiptJSON(r.requestPayload())
	}
	if !canonicalSHA256(r.RequestHash) {
		return fmt.Errorf("request_hash must be a lowercase sha256")
	}
	if r.Verification.Status == "" {
		r.Verification.Status = r.Status
	}
	if r.Verification.ReceiptHash == "" {
		r.Verification.ReceiptHash = r.CanonicalHash
	}
	return nil
}

// StableHash returns the canonical receipt hash with transport and wall-clock
// fields omitted. It is stable across replay and duplicate delivery.
func (r WorkReceipt) StableHash() string { return hashReceiptJSON(r.logicalPayload()) }

// RequestContentHash returns the stable hash used to detect conflicting
// idempotent finalization requests. Delivery IDs and timestamps are excluded.
func (r WorkReceipt) RequestContentHash() string { return hashReceiptJSON(r.requestPayload()) }

// CanonicalJSON returns the bounded redacted receipt payload used for raw
// disclosure. It contains no credentials or original prompt/output bytes.
func (r WorkReceipt) CanonicalJSON() ([]byte, error) {
	if err := r.Normalize(); err != nil {
		return nil, err
	}
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(payload) > MaxWorkReceiptPayloadSize {
		return nil, fmt.Errorf("canonical work receipt payload exceeds %d bytes", MaxWorkReceiptPayloadSize)
	}
	return payload, nil
}

func (r WorkReceipt) logicalPayload() any {
	verification := r.Verification
	verification.ReceiptHash, verification.VerifiedAt = "", time.Time{}
	return struct {
		SchemaVersion      int                     `json:"schema_version"`
		WorkspaceID        string                  `json:"workspace_id"`
		WorkKind           string                  `json:"work_kind"`
		WorkID             string                  `json:"work_id"`
		Status             string                  `json:"status"`
		Actor              ActorRef                `json:"actor"`
		Task               *EntityRef              `json:"task,omitempty"`
		Session            *EntityRef              `json:"session,omitempty"`
		TaskOwnerID        string                  `json:"task_owner_id,omitempty"`
		TaskFence          uint64                  `json:"task_fence,omitempty"`
		SourceManifestHash string                  `json:"source_manifest_hash,omitempty"`
		ReplayHash         string                  `json:"replay_hash,omitempty"`
		Steps              []WorkReceiptStep       `json:"steps"`
		Evidence           []WorkReceiptEvidence   `json:"evidence,omitempty"`
		Artifacts          []WorkReceiptArtifact   `json:"artifacts,omitempty"`
		References         []WorkReceiptReference  `json:"references,omitempty"`
		Cost               WorkReceiptCost         `json:"cost"`
		Verification       WorkReceiptVerification `json:"verification"`
	}{r.SchemaVersion, r.WorkspaceID, r.WorkKind, r.WorkID, r.Status, redactedActor(r.Actor), r.Task, r.Session, r.TaskOwnerID, r.TaskFence, r.SourceManifestHash, r.ReplayHash, r.Steps, r.Evidence, r.Artifacts, r.References, r.Cost, verification}
}

func (r WorkReceipt) requestPayload() any {
	payload := r.logicalPayload()
	return struct {
		WorkspaceID string     `json:"workspace_id"`
		WorkKind    string     `json:"work_kind"`
		WorkID      string     `json:"work_id"`
		Actor       ActorRef   `json:"actor"`
		Task        *EntityRef `json:"task,omitempty"`
		Session     *EntityRef `json:"session,omitempty"`
		TaskOwnerID string     `json:"task_owner_id,omitempty"`
		TaskFence   uint64     `json:"task_fence,omitempty"`
		Payload     any        `json:"payload"`
	}{r.WorkspaceID, r.WorkKind, r.WorkID, redactedActor(r.Actor), r.Task, r.Session, r.TaskOwnerID, r.TaskFence, payload}
}

func normalizeWorkReceiptStep(s *WorkReceiptStep, _ int) error {
	s.ID, s.Name, s.Kind, s.Status = strings.TrimSpace(s.ID), strings.TrimSpace(s.Name), strings.TrimSpace(s.Kind), strings.TrimSpace(s.Status)
	s.SourceKind, s.SourceID, s.SourceHash = strings.TrimSpace(s.SourceKind), strings.TrimSpace(s.SourceID), normalizeReceiptHash(s.SourceHash)
	s.InputHash, s.OutputHash = normalizeReceiptHash(s.InputHash), normalizeReceiptHash(s.OutputHash)
	s.ExternalBoundary = strings.TrimSpace(s.ExternalBoundary)
	if s.Ordinal < 0 || s.Ordinal >= MaxWorkReceiptSteps || s.ID == "" || s.Name == "" || s.Kind == "" || s.Status == "" {
		return fmt.Errorf("ordinal, id, name, kind, and status are required")
	}
	if len(s.ID) > MaxWorkReceiptIDLength || len(s.Name) > MaxWorkReceiptNameLength || len(s.Kind) > MaxWorkReceiptNameLength || len(s.SourceID) > MaxWorkReceiptIDLength || len(s.SourceKind) > MaxWorkReceiptNameLength || len(s.ExternalBoundary) > MaxWorkReceiptNameLength {
		return fmt.Errorf("step identity is too large")
	}
	if s.SourceHash != "" && !canonicalSHA256(s.SourceHash) || s.InputHash != "" && !canonicalSHA256(s.InputHash) || s.OutputHash != "" && !canonicalSHA256(s.OutputHash) {
		return fmt.Errorf("step hashes must be lowercase sha256")
	}
	if s.DurationMS < 0 || s.Attempts < 0 || s.RetryCount < 0 {
		return fmt.Errorf("step measurements cannot be negative")
	}
	if s.ExternalEffect && s.ExternalBoundary == "" {
		return fmt.Errorf("external steps require an external boundary")
	}
	if len(s.ReferenceRoles) > MaxWorkReceiptReferences {
		return fmt.Errorf("step has too many reference roles")
	}
	for i := range s.ReferenceRoles {
		s.ReferenceRoles[i] = strings.TrimSpace(s.ReferenceRoles[i])
		if s.ReferenceRoles[i] == "" || len(s.ReferenceRoles[i]) > MaxWorkReceiptRoleLength {
			return fmt.Errorf("step reference role is invalid")
		}
	}
	if len(s.Metadata) > MaxWorkReceiptMetadata {
		return fmt.Errorf("step metadata exceeds %d entries", MaxWorkReceiptMetadata)
	}
	for key, value := range s.Metadata {
		if !safeReceiptMetadataKey(key) || len(value) > 256 || looksLikeSecret(value) {
			return fmt.Errorf("step metadata contains unsafe value")
		}
	}
	sort.Strings(s.ReferenceRoles)
	return nil
}

func normalizeWorkReceiptReference(r *WorkReceiptReference, workspaceID string) error {
	r.WorkspaceID, r.Kind, r.SourceID, r.Role, r.Hash = strings.TrimSpace(r.WorkspaceID), strings.TrimSpace(r.Kind), strings.TrimSpace(r.SourceID), strings.TrimSpace(r.Role), normalizeReceiptHash(r.Hash)
	if r.WorkspaceID != workspaceID || r.Kind == "" || r.SourceID == "" {
		return fmt.Errorf("workspace, kind, and source_id must be present")
	}
	if !validWorkReceiptReferenceKind(r.Kind) || len(r.Kind) > MaxWorkReceiptNameLength || len(r.SourceID) > MaxWorkReceiptIDLength || len(r.Role) > MaxWorkReceiptRoleLength {
		return fmt.Errorf("reference identity is invalid")
	}
	if r.Hash != "" && !canonicalSHA256(r.Hash) {
		return fmt.Errorf("reference hash must be lowercase sha256")
	}
	return nil
}

func normalizeWorkReceiptEvidence(e *WorkReceiptEvidence, workspaceID string) error {
	e.WorkspaceID, e.EvidenceHash, e.SourceReference, e.Role = strings.TrimSpace(e.WorkspaceID), normalizeReceiptHash(e.EvidenceHash), strings.TrimSpace(e.SourceReference), strings.TrimSpace(e.Role)
	if e.ID <= 0 || e.WorkspaceID != workspaceID || !canonicalSHA256(e.EvidenceHash) || len(e.Role) > MaxWorkReceiptRoleLength || len(e.SourceReference) > MaxWorkReceiptIDLength*2 {
		return fmt.Errorf("evidence identity is invalid")
	}
	return nil
}

func normalizeWorkReceiptArtifact(a *WorkReceiptArtifact, workspaceID string) error {
	a.WorkspaceID, a.ContentHash, a.SourceKind, a.SourceID, a.Role = strings.TrimSpace(a.WorkspaceID), normalizeReceiptHash(a.ContentHash), strings.TrimSpace(a.SourceKind), strings.TrimSpace(a.SourceID), strings.TrimSpace(a.Role)
	if a.ID <= 0 || a.ArtifactID <= 0 || a.WorkspaceID != workspaceID || !canonicalSHA256(a.ContentHash) || len(a.SourceKind) > MaxWorkReceiptNameLength || len(a.SourceID) > MaxWorkReceiptIDLength || len(a.Role) > MaxWorkReceiptRoleLength {
		return fmt.Errorf("artifact identity is invalid")
	}
	return nil
}

func normalizeWorkReceiptCost(c *WorkReceiptCost) error {
	values := []float64{c.ModelUSD, c.ToolUSD, c.RetrievalUSD, c.ArtifactUSD, c.RetryUSD, c.DuplicateWorkUSD, c.TotalUSD}
	for _, value := range values {
		if value < 0 || math.IsNaN(value) || math.IsInf(value, 0) {
			return fmt.Errorf("work receipt cost is invalid")
		}
	}
	if c.Measured && c.Estimated {
		return fmt.Errorf("work receipt cost cannot be both measured and estimated")
	}
	return nil
}

func receiptReferenceKey(r WorkReceiptReference) string {
	return r.Kind + "\x00" + r.SourceID + "\x00" + r.Role + "\x00" + r.Hash
}
func receiptEvidenceKey(e WorkReceiptEvidence) string {
	return fmt.Sprintf("%020d\x00%s\x00%s", e.ID, e.EvidenceHash, e.Role)
}
func receiptArtifactKey(a WorkReceiptArtifact) string {
	return fmt.Sprintf("%020d\x00%s\x00%s", a.ArtifactID, a.ContentHash, a.Role)
}

func validWorkReceiptReferenceKind(kind string) bool {
	switch kind {
	case WorkReceiptReferenceTask, WorkReceiptReferenceEvent, WorkReceiptReferenceAgentRun, WorkReceiptReferenceRetrievalSurface, WorkReceiptReferenceModelCall, WorkReceiptReferenceToolRun, WorkReceiptReferenceEvidence, WorkReceiptReferenceArtifact, WorkReceiptReferenceValidation, WorkReceiptReferenceObservation, WorkReceiptReferenceCost, WorkReceiptReferenceReplay:
		return true
	default:
		return false
	}
}

func hasExternalBoundary(steps []WorkReceiptStep) bool {
	for _, step := range steps {
		if step.ExternalEffect {
			return true
		}
	}
	return false
}

func normalizeReceiptHash(value string) string { return strings.ToLower(strings.TrimSpace(value)) }

func hashReceiptJSON(value any) string {
	payload, _ := json.Marshal(value)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func redactedActor(actor ActorRef) ActorRef {
	return actor
}

func containsReceiptReference(values []WorkReceiptReference, wanted WorkReceiptReference) bool {
	wantedKey := receiptReferenceKey(wanted)
	for _, value := range values {
		if receiptReferenceKey(value) == wantedKey {
			return true
		}
	}
	return false
}

func safeReceiptMetadataKey(key string) bool {
	key = strings.ToLower(strings.TrimSpace(key))
	if key == "" || len(key) > 64 {
		return false
	}
	for _, forbidden := range []string{"prompt", "secret", "credential", "token", "password", "authorization", "body", "content", "output", "input", "environment"} {
		if strings.Contains(key, forbidden) {
			return false
		}
	}
	return true
}

func looksLikeSecret(value string) bool {
	lower := strings.ToLower(value)
	for _, marker := range []string{"sk-", "bearer ", "api_key", "password=", "secret="} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}
