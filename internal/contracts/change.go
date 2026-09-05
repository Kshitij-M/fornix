package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// ChangeSchemaVersion is the durable contract version for repository change
// proposals and applications.
const ChangeSchemaVersion = 1

const (
	DefaultChangeMaxOperations = 32
	MaxChangeOperations        = 128
	DefaultChangeMaxFileBytes  = 4 << 20
	MaxChangeFileBytes         = 64 << 20
	DefaultChangeMaxTotalBytes = 16 << 20
	MaxChangeTotalBytes        = 256 << 20
	MaxChangePathLength        = 4096
	MaxChangeReportBytes       = 1 << 20
	MaxChangeDisclosureBytes   = 1 << 20
	MaxChangeDisclosureItems   = 100
)

const (
	ChangeProposed          = "proposed"
	ChangeAwaitingApproval  = "awaiting_approval"
	ChangeApproved          = "approved"
	ChangeRejected          = "rejected"
	ChangeApplying          = "applying"
	ChangeApplied           = "applied"
	ChangeConflicted        = "conflicted"
	ChangeFailed            = "failed"
	ChangeCancelled         = "cancelled"
	ChangeExpired           = "expired"
	ChangeRecoveryRequired  = "recovery_required"
	ChangeApprovalAutomatic = "automatic"
	ChangeApprovalRequired  = "required"
	ChangeApprovalDenied    = "denied"
)

const (
	ChangeOpCreate  = "create_file"
	ChangeOpReplace = "replace_file"
	ChangeOpDelete  = "delete_file"
	ChangeOpRename  = "rename_file"
	ChangeOpChmod   = "chmod_file"
)

const (
	ChangeFailureInvalidRequest = "invalid_request"
	ChangeFailureUnauthorized   = "unauthorized"
	ChangeFailureUnsafePath     = "unsafe_path"
	ChangeFailureSourceConflict = "source_conflict"
	ChangeFailureApproval       = "approval_required"
	ChangeFailureStaleFence     = "stale_fence"
	ChangeFailureBudget         = "budget"
	ChangeFailureFilesystem     = "filesystem"
	ChangeFailureRecovery       = "recovery_required"
	ChangeFailureConflict       = "conflict"
	ChangeFailureInProgress     = "in_progress"
)

// ChangeSourceFile is an immutable precondition for one normalized repository
// path. Hashes describe the observed source, not a mutable index projection.
type ChangeSourceFile struct {
	Path        string `json:"path"`
	Mode        uint32 `json:"mode"`
	ByteSize    int64  `json:"byte_size"`
	ContentHash string `json:"content_hash"`
	Exists      bool   `json:"exists"`
}

// ChangeSourceSnapshot identifies the exact source state against which a
// packet was planned. SourceRoot is an operator-visible configured path; the
// canonical hash uses the normalized relative paths and file hashes.
type ChangeSourceSnapshot struct {
	WorkspaceID  string             `json:"workspace_id"`
	Repository   string             `json:"repository"`
	SourceRoot   string             `json:"source_root"`
	ManifestHash string             `json:"manifest_hash"`
	Files        []ChangeSourceFile `json:"files"`
	Actor        ActorRef           `json:"actor,omitempty"`
	Task         *EntityRef         `json:"task,omitempty"`
	Session      *EntityRef         `json:"session,omitempty"`
	AgentRun     *EntityRef         `json:"agent_run,omitempty"`
	TaskOwnerID  string             `json:"task_owner_id,omitempty"`
	TaskFence    uint64             `json:"task_fence,omitempty"`
	CapturedAt   time.Time          `json:"captured_at,omitempty"`
}

// ChangeOperationInput is the untrusted proposal input. Content is accepted
// only at the bounded API boundary and is immediately moved to ArtifactStore;
// it is never persisted inline in a change operation.
type ChangeOperationInput struct {
	ID           string `json:"id,omitempty"`
	Ordinal      int    `json:"ordinal,omitempty"`
	Type         string `json:"type"`
	Path         string `json:"path"`
	Destination  string `json:"destination,omitempty"`
	ExpectedHash string `json:"expected_hash,omitempty"`
	ExpectedMode uint32 `json:"expected_mode,omitempty"`
	Content      []byte `json:"content,omitempty"`
	NewMode      uint32 `json:"new_mode,omitempty"`
}

// ChangeOperation is the persisted, content-addressed form of one file
// operation. It carries no raw content.
type ChangeOperation struct {
	ID                 string       `json:"id"`
	Ordinal            int          `json:"ordinal"`
	Type               string       `json:"type"`
	Path               string       `json:"path"`
	Destination        string       `json:"destination,omitempty"`
	ExpectedHash       string       `json:"expected_hash,omitempty"`
	ExpectedMode       uint32       `json:"expected_mode,omitempty"`
	NewContentHash     string       `json:"new_content_hash,omitempty"`
	NewContentArtifact *ArtifactRef `json:"new_content_artifact,omitempty"`
	NewByteSize        int64        `json:"new_byte_size,omitempty"`
	ResultHash         string       `json:"result_hash,omitempty"`
	NewMode            uint32       `json:"new_mode,omitempty"`
}

// ChangeBudgets are hard limits. Callers may lower them but not exceed the
// server contract maxima.
type ChangeBudgets struct {
	MaxOperations int   `json:"max_operations,omitempty"`
	MaxFileBytes  int64 `json:"max_file_bytes,omitempty"`
	MaxTotalBytes int64 `json:"max_total_bytes,omitempty"`
}

// ChangeProposalRequest is the API/store input for a deterministic proposal.
type ChangeProposalRequest struct {
	SchemaVersion  int                    `json:"schema_version,omitempty"`
	ID             string                 `json:"id,omitempty"`
	RequestID      string                 `json:"request_id,omitempty"`
	IdempotencyKey string                 `json:"idempotency_key"`
	CausationID    string                 `json:"causation_id,omitempty"`
	CorrelationID  string                 `json:"correlation_id,omitempty"`
	WorkspaceID    string                 `json:"workspace_id"`
	Actor          ActorRef               `json:"actor,omitempty"`
	Task           *EntityRef             `json:"task,omitempty"`
	Session        *EntityRef             `json:"session,omitempty"`
	AgentRun       *EntityRef             `json:"agent_run,omitempty"`
	Policy         *ValidationPolicyRef   `json:"policy,omitempty"`
	TaskOwnerID    string                 `json:"task_owner_id,omitempty"`
	TaskFence      uint64                 `json:"task_fence,omitempty"`
	Repository     string                 `json:"repository"`
	Source         ChangeSourceSnapshot   `json:"source"`
	Operations     []ChangeOperationInput `json:"operations"`
	ApprovalMode   string                 `json:"approval_mode,omitempty"`
	Budgets        ChangeBudgets          `json:"budgets,omitempty"`
}

// ChangePacket is the immutable normalized proposal body used for hashing and
// approval. It is not a second authority; the persisted proposal owns it.
type ChangePacket struct {
	SchemaVersion    int                  `json:"schema_version"`
	WorkspaceID      string               `json:"workspace_id"`
	Repository       string               `json:"repository"`
	Source           ChangeSourceSnapshot `json:"source"`
	Operations       []ChangeOperation    `json:"operations"`
	Budgets          ChangeBudgets        `json:"budgets"`
	ExpectedTreeHash string               `json:"expected_tree_hash"`
	Policy           *ValidationPolicyRef `json:"policy,omitempty"`
}

// ChangeProposal is the durable proposal read shape.
type ChangeProposal struct {
	SchemaVersion    int                  `json:"schema_version"`
	ID               string               `json:"id"`
	WorkspaceID      string               `json:"workspace_id"`
	RequestID        string               `json:"request_id"`
	IdempotencyKey   string               `json:"idempotency_key"`
	RequestHash      string               `json:"request_hash"`
	PacketHash       string               `json:"packet_hash"`
	ExpectedTreeHash string               `json:"expected_tree_hash,omitempty"`
	Status           string               `json:"status"`
	Actor            ActorRef             `json:"actor"`
	Task             *EntityRef           `json:"task,omitempty"`
	Session          *EntityRef           `json:"session,omitempty"`
	AgentRun         *EntityRef           `json:"agent_run,omitempty"`
	Policy           *ValidationPolicyRef `json:"policy,omitempty"`
	TaskOwnerID      string               `json:"task_owner_id,omitempty"`
	TaskFence        uint64               `json:"task_fence,omitempty"`
	Repository       string               `json:"repository"`
	Source           ChangeSourceSnapshot `json:"source"`
	Operations       []ChangeOperation    `json:"operations"`
	Budgets          ChangeBudgets        `json:"budgets"`
	ApprovalMode     string               `json:"approval_mode"`
	DiffArtifact     *ArtifactRef         `json:"diff_artifact,omitempty"`
	CreatedAt        time.Time            `json:"created_at"`
	UpdatedAt        time.Time            `json:"updated_at"`
}

// ChangeApprovalRequest records a decision over one exact packet hash.
type ChangeApprovalRequest struct {
	SchemaVersion  int                  `json:"schema_version,omitempty"`
	ID             string               `json:"id,omitempty"`
	WorkspaceID    string               `json:"workspace_id"`
	ProposalID     string               `json:"proposal_id"`
	PacketHash     string               `json:"packet_hash"`
	Decision       string               `json:"decision"`
	Reason         string               `json:"reason,omitempty"`
	Actor          ActorRef             `json:"actor"`
	Policy         *ValidationPolicyRef `json:"policy,omitempty"`
	ExpiresAt      *time.Time           `json:"expires_at,omitempty"`
	IdempotencyKey string               `json:"idempotency_key"`
}

// ChangeApproval is the durable audit record for one decision.
type ChangeApproval struct {
	ID             string               `json:"id"`
	WorkspaceID    string               `json:"workspace_id"`
	ProposalID     string               `json:"proposal_id"`
	PacketHash     string               `json:"packet_hash"`
	Decision       string               `json:"decision"`
	Reason         string               `json:"reason,omitempty"`
	Actor          ActorRef             `json:"actor"`
	Policy         *ValidationPolicyRef `json:"policy,omitempty"`
	IdempotencyKey string               `json:"idempotency_key"`
	ExpiresAt      *time.Time           `json:"expires_at,omitempty"`
	CreatedAt      time.Time            `json:"created_at"`
}

// ChangeApplicationRequest starts or resumes an approved application.
type ChangeApplicationRequest struct {
	SchemaVersion  int                  `json:"schema_version,omitempty"`
	ID             string               `json:"id,omitempty"`
	WorkspaceID    string               `json:"workspace_id"`
	ProposalID     string               `json:"proposal_id"`
	PacketHash     string               `json:"packet_hash"`
	IdempotencyKey string               `json:"idempotency_key"`
	Actor          ActorRef             `json:"actor"`
	Policy         *ValidationPolicyRef `json:"policy,omitempty"`
	TaskOwnerID    string               `json:"task_owner_id,omitempty"`
	TaskFence      uint64               `json:"task_fence,omitempty"`
	DryRun         bool                 `json:"dry_run,omitempty"`
}

// ChangeApplication is the durable application attempt and verification
// result. RecoveryRequired is explicit because filesystem effects are external
// to the Postgres transaction.
type ChangeApplication struct {
	ID               string               `json:"id"`
	WorkspaceID      string               `json:"workspace_id"`
	ProposalID       string               `json:"proposal_id"`
	PacketHash       string               `json:"packet_hash"`
	Status           string               `json:"status"`
	ExpectedTreeHash string               `json:"expected_tree_hash,omitempty"`
	ResultTreeHash   string               `json:"result_tree_hash,omitempty"`
	DiffArtifact     *ArtifactRef         `json:"diff_artifact,omitempty"`
	ResultArtifact   *ArtifactRef         `json:"result_artifact,omitempty"`
	Receipt          *WorkReceipt         `json:"receipt,omitempty"`
	Conflict         *ChangeConflict      `json:"conflict,omitempty"`
	Failure          *ChangeFailure       `json:"failure,omitempty"`
	Operations       []ChangeOperation    `json:"operations,omitempty"`
	Actor            ActorRef             `json:"actor"`
	TaskOwnerID      string               `json:"task_owner_id,omitempty"`
	TaskFence        uint64               `json:"task_fence,omitempty"`
	Policy           *ValidationPolicyRef `json:"policy,omitempty"`
	CreatedAt        time.Time            `json:"created_at"`
	UpdatedAt        time.Time            `json:"updated_at"`
}

// ChangeConflict describes a fail-closed source or post-state mismatch.
type ChangeConflict struct {
	Path           string `json:"path"`
	ExpectedHash   string `json:"expected_hash,omitempty"`
	ObservedHash   string `json:"observed_hash,omitempty"`
	ExpectedExists bool   `json:"expected_exists"`
	ObservedExists bool   `json:"observed_exists"`
	Reason         string `json:"reason"`
}

// ChangeFailure is a stable, redacted failure classification.
type ChangeFailure struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ChangeDisclosureRequest selects a bounded change view.
type ChangeDisclosureRequest struct {
	WorkspaceID   string `json:"workspace_id"`
	ProposalID    string `json:"proposal_id,omitempty"`
	ApplicationID string `json:"application_id,omitempty"`
	Level         string `json:"level,omitempty"`
	MaxBytes      int    `json:"max_bytes,omitempty"`
	MaxItems      int    `json:"max_items,omitempty"`
}

// ChangeDisclosureResult is a redacted, budgeted view retaining canonical
// proposal/application hashes and provenance references.
type ChangeDisclosureResult struct {
	WorkspaceID      string             `json:"workspace_id"`
	Proposal         *ChangeProposal    `json:"proposal,omitempty"`
	Application      *ChangeApplication `json:"application,omitempty"`
	PacketHash       string             `json:"packet_hash"`
	ResultTreeHash   string             `json:"result_tree_hash,omitempty"`
	DiffArtifactHash string             `json:"diff_artifact_hash,omitempty"`
	Truncated        bool               `json:"truncated"`
	TotalBytes       int                `json:"total_bytes"`
	ContentViewHash  string             `json:"content_view_hash"`
}

// Normalize validates a change proposal's bounded identity and workspace
// references. Filesystem-specific path and source validation belongs to the
// change planner, where the configured mount is available.
func (r ChangeProposalRequest) Normalize() (ChangeProposalRequest, error) {
	if r.SchemaVersion == 0 {
		r.SchemaVersion = ChangeSchemaVersion
	}
	if r.SchemaVersion != ChangeSchemaVersion {
		return ChangeProposalRequest{}, fmt.Errorf("unsupported change schema_version %d", r.SchemaVersion)
	}
	r.WorkspaceID, r.Repository, r.IdempotencyKey = strings.TrimSpace(r.WorkspaceID), strings.TrimSpace(r.Repository), strings.TrimSpace(r.IdempotencyKey)
	if r.WorkspaceID == "" || r.Repository == "" || r.IdempotencyKey == "" {
		return ChangeProposalRequest{}, fmt.Errorf("workspace_id, repository, and idempotency_key are required")
	}
	if len(r.IdempotencyKey) > MaxIdempotencyLength {
		return ChangeProposalRequest{}, fmt.Errorf("idempotency_key is too large")
	}
	if r.RequestID == "" {
		r.RequestID = NewID("req")
	}
	if r.ID == "" {
		r.ID = NewID("chg")
	}
	if r.Actor.WorkspaceID == "" {
		r.Actor.WorkspaceID = r.WorkspaceID
	}
	if r.Actor.WorkspaceID != r.WorkspaceID {
		return ChangeProposalRequest{}, fmt.Errorf("actor crosses workspace boundary")
	}
	if r.Task != nil && r.Task.WorkspaceID != "" && r.Task.WorkspaceID != r.WorkspaceID {
		return ChangeProposalRequest{}, fmt.Errorf("task crosses workspace boundary")
	}
	if r.Session != nil && r.Session.WorkspaceID != "" && r.Session.WorkspaceID != r.WorkspaceID {
		return ChangeProposalRequest{}, fmt.Errorf("session crosses workspace boundary")
	}
	if r.AgentRun != nil && r.AgentRun.WorkspaceID != "" && r.AgentRun.WorkspaceID != r.WorkspaceID {
		return ChangeProposalRequest{}, fmt.Errorf("agent run crosses workspace boundary")
	}
	if r.Policy != nil {
		if err := r.Policy.Normalize(); err != nil {
			return ChangeProposalRequest{}, err
		}
		if r.Policy.WorkspaceID != r.WorkspaceID {
			return ChangeProposalRequest{}, fmt.Errorf("policy crosses workspace boundary")
		}
	}
	if r.Task != nil && (r.TaskOwnerID == "" || r.TaskFence == 0) {
		return ChangeProposalRequest{}, fmt.Errorf("task-bound change requires owner and fence")
	}
	if len(r.Operations) == 0 || len(r.Operations) > MaxChangeOperations {
		return ChangeProposalRequest{}, fmt.Errorf("operations must contain between 1 and %d items", MaxChangeOperations)
	}
	if r.ApprovalMode == "" {
		r.ApprovalMode = ChangeApprovalRequired
	}
	if r.ApprovalMode != ChangeApprovalRequired && r.ApprovalMode != ChangeApprovalAutomatic && r.ApprovalMode != ChangeApprovalDenied {
		return ChangeProposalRequest{}, fmt.Errorf("unsupported approval_mode %q", r.ApprovalMode)
	}
	if r.Budgets.MaxOperations == 0 {
		r.Budgets.MaxOperations = DefaultChangeMaxOperations
	}
	if r.Budgets.MaxFileBytes == 0 {
		r.Budgets.MaxFileBytes = DefaultChangeMaxFileBytes
	}
	if r.Budgets.MaxTotalBytes == 0 {
		r.Budgets.MaxTotalBytes = DefaultChangeMaxTotalBytes
	}
	if r.Budgets.MaxOperations < 1 || r.Budgets.MaxOperations > MaxChangeOperations || r.Budgets.MaxFileBytes < 1 || r.Budgets.MaxFileBytes > MaxChangeFileBytes || r.Budgets.MaxTotalBytes < 1 || r.Budgets.MaxTotalBytes > MaxChangeTotalBytes {
		return ChangeProposalRequest{}, fmt.Errorf("change budgets exceed configured bounds")
	}
	if len(r.Operations) > r.Budgets.MaxOperations {
		return ChangeProposalRequest{}, fmt.Errorf("operation count exceeds budget")
	}
	return r, nil
}

// StableHash returns the deterministic packet identity. It includes content
// hashes but never raw content or volatile timestamps.
func (p ChangePacket) StableHash() string {
	p = normalizePacketForHash(p)
	raw, _ := json.Marshal(p)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// RequestHash returns a stable proposal request identity without embedding
// raw proposed content in the hash payload.
func (r ChangeProposalRequest) RequestHash() string {
	input := struct {
		SchemaVersion  int                  `json:"schema_version"`
		WorkspaceID    string               `json:"workspace_id"`
		Repository     string               `json:"repository"`
		IdempotencyKey string               `json:"idempotency_key"`
		Source         ChangeSourceSnapshot `json:"source"`
		Operations     []struct {
			ID, Type, Path, Destination, ExpectedHash string
			Ordinal                                   int
			ContentHash                               string
			NewMode                                   uint32
		} `json:"operations"`
		ApprovalMode string               `json:"approval_mode"`
		Budgets      ChangeBudgets        `json:"budgets"`
		Policy       *ValidationPolicyRef `json:"policy,omitempty"`
	}{SchemaVersion: r.SchemaVersion, WorkspaceID: r.WorkspaceID, Repository: r.Repository, IdempotencyKey: r.IdempotencyKey, Source: normalizedSource(r.Source), ApprovalMode: r.ApprovalMode, Budgets: r.Budgets, Policy: r.Policy}
	for _, op := range r.Operations {
		hash := ""
		if len(op.Content) > 0 {
			hash = ArtifactContentHash(op.Content)
		}
		input.Operations = append(input.Operations, struct {
			ID, Type, Path, Destination, ExpectedHash string
			Ordinal                                   int
			ContentHash                               string
			NewMode                                   uint32
		}{op.ID, op.Type, op.Path, op.Destination, op.ExpectedHash, op.Ordinal, hash, op.NewMode})
	}
	raw, _ := json.Marshal(input)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func normalizePacketForHash(p ChangePacket) ChangePacket {
	p.Source = normalizedSource(p.Source)
	p.Operations = append([]ChangeOperation(nil), p.Operations...)
	sort.SliceStable(p.Operations, func(i, j int) bool {
		if p.Operations[i].Ordinal != p.Operations[j].Ordinal {
			return p.Operations[i].Ordinal < p.Operations[j].Ordinal
		}
		if p.Operations[i].Path != p.Operations[j].Path {
			return p.Operations[i].Path < p.Operations[j].Path
		}
		return p.Operations[i].ID < p.Operations[j].ID
	})
	return p
}

func normalizedSource(s ChangeSourceSnapshot) ChangeSourceSnapshot {
	s.Files = append([]ChangeSourceFile(nil), s.Files...)
	sort.Slice(s.Files, func(i, j int) bool { return s.Files[i].Path < s.Files[j].Path })
	s.SourceRoot = ""
	s.CapturedAt = time.Time{}
	return s
}

// NormalizeDisclosure applies hard limits before any source is returned.
func (r ChangeDisclosureRequest) Normalize() (ChangeDisclosureRequest, error) {
	r.WorkspaceID, r.ProposalID, r.ApplicationID, r.Level = strings.TrimSpace(r.WorkspaceID), strings.TrimSpace(r.ProposalID), strings.TrimSpace(r.ApplicationID), strings.ToLower(strings.TrimSpace(r.Level))
	if r.WorkspaceID == "" || (r.ProposalID == "" && r.ApplicationID == "") || (r.ProposalID != "" && r.ApplicationID != "") {
		return ChangeDisclosureRequest{}, fmt.Errorf("workspace and exactly one change identity are required")
	}
	if r.Level == "" {
		r.Level = string(DisclosureGist)
	}
	if r.Level != string(DisclosureGist) && r.Level != string(DisclosureDetail) && r.Level != string(DisclosureRaw) {
		return ChangeDisclosureRequest{}, fmt.Errorf("unsupported change disclosure level %q", r.Level)
	}
	if r.MaxBytes == 0 {
		r.MaxBytes = DefaultDisclosureBytes
	}
	if r.MaxItems == 0 {
		r.MaxItems = MaxChangeDisclosureItems
	}
	if r.MaxBytes < 1 || r.MaxBytes > MaxChangeDisclosureBytes || r.MaxItems < 1 || r.MaxItems > MaxChangeDisclosureItems {
		return ChangeDisclosureRequest{}, fmt.Errorf("change disclosure budget exceeds bounds")
	}
	return r, nil
}
