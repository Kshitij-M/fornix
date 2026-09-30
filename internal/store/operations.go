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
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

const (
	operationCommandCreate     = "create"
	operationCommandTransition = "transition"
	maxOperationFence          = uint64(1<<63 - 1)
	maxOperationLeaseTTL       = 24 * time.Hour
	maxOperationReplayLimit    = 4096
	maxOperationActiveLimit    = 4096
)

var (
	ErrOperationNotFound                 = errors.New("operation not found")
	ErrOperationIdempotency              = errors.New("operation idempotency conflict")
	ErrOperationTransition               = errors.New("invalid operation transition")
	ErrOperationLeaseMissing             = errors.New("operation lease not found")
	ErrOperationLeaseHeld                = errors.New("operation lease is held by another owner")
	ErrOperationLeaseOwned               = errors.New("operation lease is not owned by this worker")
	ErrOperationLeaseFenced              = errors.New("operation lease fence is stale")
	ErrOperationLeaseExpired             = errors.New("operation lease is expired")
	ErrOperationLeaseReleased            = errors.New("operation lease is released")
	ErrOperationResourceBusy             = errors.New("operation resource is leased by another operation")
	ErrOperationFenceExhausted           = errors.New("operation lease fence is exhausted")
	ErrOperationTaskFence                = errors.New("task-bound operation fence is invalid")
	ErrOperationReplay                   = errors.New("operation replay integrity failure")
	ErrOperationWorkspace                = errors.New("operation workspace violation")
	ErrOperationPlanConflict             = errors.New("operation plan conflicts with existing state")
	ErrOperationResultNotFound           = errors.New("operation result not found")
	ErrOperationResultConflict           = errors.New("operation result conflicts with existing state")
	ErrOperationTerminal                 = errors.New("operation is terminal")
	ErrOperationAdmissionRequired        = errors.New("effectful operation requires deployment admission")
	ErrOperationAdmissionUnavailable     = errors.New("deployment admission authority is unavailable")
	ErrOperationExternalBoundaryRequired = errors.New("external effect requires controlled boundary authority")
	ErrOperationExternalBoundaryMismatch = errors.New("external effect boundary does not match deployment evidence")
)

// Operation is the current projection of one generic operation. Raw inputs and
// outputs remain in the existing artifact/evidence authorities; this record
// contains typed references and hashes only.
type Operation struct {
	WorkspaceID    string
	ID             string
	SchemaVersion  int
	RequestID      string
	IdempotencyKey string
	RequestHash    string
	OperationHash  string
	Status         string
	Request        contracts.OperationRequest
	Plan           *contracts.OperationPlan
	PlanHash       string
	ResultHash     string
	ReportHash     string
	Failure        *contracts.OperationFailure
	NextRetryAt    *time.Time
	StateVersion   int64
	StateHash      string
	TaskOwnerID    string
	TaskFence      uint64
	CreatedAt      time.Time
	UpdatedAt      time.Time
	StartedAt      *time.Time
	CompletedAt    *time.Time
}

type OperationResource struct {
	WorkspaceID  string
	OperationID  string
	Ordinal      int
	ResourceKind string
	ResourceID   string
	ResourceHash string
	Role         string
}

// OperationLink connects the generic authority to an existing specialized
// authority. The source record remains authoritative for its own lifecycle.
type OperationLink struct {
	WorkspaceID string
	OperationID string
	LinkKind    string
	SourceID    string
	SourceHash  string
	Role        string
}

type OperationCreateInput struct {
	Request     contracts.OperationRequest
	Plan        *contracts.OperationPlan
	Resources   []OperationResource
	Links       []OperationLink
	TaskOwnerID string
	TaskFence   uint64
}

type OperationCreateResult struct {
	Operation Operation
	Event     contracts.EventEnvelope
	Duplicate bool
}

// OperationPlanInput is the fenced command that persists a connector's
// normalized deterministic plan before any adapter execution begins.
type OperationPlanInput struct {
	WorkspaceID    string
	OperationID    string
	OwnerID        string
	Fence          uint64
	TaskOwnerID    string
	TaskFence      uint64
	Actor          contracts.ActorRef
	RequestID      string
	IdempotencyKey string
	CausationID    string
	CorrelationID  string
	Plan           contracts.OperationPlan
}

type OperationPlanResult struct {
	Operation  Operation
	Transition OperationTransition
	Event      contracts.EventEnvelope
	Duplicate  bool
}

// OperationResultRecord is the immutable, hash-only result authority for one
// generic operation. Domain adapters own raw output and evidence bytes.
type OperationResultRecord struct {
	WorkspaceID string
	OperationID string
	ResultID    string
	ResultHash  string
	Result      contracts.OperationResult
	CreatedAt   time.Time
}

type OperationResultInput struct {
	WorkspaceID               string
	OperationID               string
	OwnerID                   string
	Fence                     uint64
	TaskOwnerID               string
	TaskFence                 uint64
	Actor                     contracts.ActorRef
	RequestID                 string
	IdempotencyKey            string
	CausationID               string
	CorrelationID             string
	TrustPolicyHash           string
	TrustPolicyRevision       string
	CredentialLeaseID         string
	CredentialLeaseFence      uint64
	CredentialRevocationEpoch uint64
	CredentialSourceVersion   string
	CredentialSourceExpiresAt *time.Time
	ExternalBoundary          *contracts.ExternalBoundaryAuthority
	SchemaCatalogHash         string
	SchemaCatalogRevision     string
	Result                    contracts.OperationResult
}

type OperationResultWrite struct {
	Operation  Operation
	Record     OperationResultRecord
	Transition OperationTransition
	Event      contracts.EventEnvelope
	Duplicate  bool
}

// OperationTransitionInput is a fenced command. The store never calls an
// adapter while applying a transition.
type OperationTransitionInput struct {
	WorkspaceID    string
	OperationID    string
	OwnerID        string
	Fence          uint64
	TaskOwnerID    string
	TaskFence      uint64
	Actor          contracts.ActorRef
	RequestID      string
	IdempotencyKey string
	CausationID    string
	CorrelationID  string
	ToStatus       string
	ReasonCode     string
	ResultHash     string
	ReportHash     string
	Failure        *contracts.OperationFailure
	NextRetryAt    *time.Time
}

type OperationTransition struct {
	WorkspaceID       string
	OperationID       string
	StateVersion      int64
	FromStatus        string
	ToStatus          string
	RequestID         string
	IdempotencyKey    string
	Actor             contracts.ActorRef
	TaskOwnerID       string
	TaskFence         uint64
	OperationOwnerID  string
	OperationFence    uint64
	CausationID       string
	CorrelationID     string
	ReasonCode        string
	State             json.RawMessage
	StateHash         string
	PreviousStateHash string
	EventSequence     *int64
	OccurredAt        time.Time
}

type OperationTransitionResult struct {
	Operation  Operation
	Transition OperationTransition
	Event      contracts.EventEnvelope
	Duplicate  bool
}

type OperationLease struct {
	WorkspaceID string
	OperationID string
	OwnerID     string
	Fence       uint64
	LeaseUntil  time.Time
	AcquiredAt  time.Time
	RenewedAt   time.Time
	ReleasedAt  *time.Time
}

type OperationLeaseResult struct {
	Lease    OperationLease
	Acquired bool
	Reused   bool
	Takeover bool
}

// OperationClaim is one atomically selected operation and its fenced worker
// lease. The operation remains authoritative in Postgres; the claim is only
// permission for a worker to perform the next bounded step.
type OperationClaim struct {
	Operation      Operation
	Lease          OperationLease
	ResourceLeases []OperationResourceLease
}

// ValidateEffectAuthority re-checks the live operation and task leases in one
// Postgres transaction. It is useful for read-only qualification and for
// validating an already committed dispatch intent; effectful dispatchers must
// use BeginEffectDispatch so validation and durable intent share one commit.
func (s *OperationStore) ValidateEffectAuthority(ctx context.Context, authority contracts.EffectAuthority) error {
	if s == nil || s.pool == nil {
		return errors.New("operation store is not configured")
	}
	if err := authority.Normalize(); err != nil {
		return fmt.Errorf("normalize effect authority: %w", err)
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, authority.WorkspaceID)
	if err != nil {
		return fmt.Errorf("begin effect authority validation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setWorkspaceContext(ctx, tx, authority.WorkspaceID); err != nil {
		return err
	}
	if err := s.validateEffectAuthorityTx(ctx, tx, authority, contracts.ExternalEffectDispatching); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// validateEffectAuthorityTx performs the live authority check in a caller-
// owned workspace transaction. expectedEffectState may be empty when the
// caller is about to establish the first dispatching transition in this same
// transaction; otherwise the existing state must match exactly.
func (s *OperationStore) validateEffectAuthorityTx(ctx context.Context, tx pgx.Tx, authority contracts.EffectAuthority, expectedEffectState string) error {
	operation, err := readOperationByID(ctx, tx, authority.WorkspaceID, authority.OperationID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrOperationNotFound
	}
	if err != nil {
		return fmt.Errorf("read effect operation: %w", err)
	}
	if contracts.IsTerminalOperationStatus(operation.Status) {
		return ErrOperationTerminal
	}
	if operation.OperationHash == "" {
		return ErrOperationTransition
	}
	if _, err := s.validateLease(ctx, tx, OperationLease{
		WorkspaceID: authority.WorkspaceID,
		OperationID: authority.OperationID,
		OwnerID:     authority.OperationOwnerID,
		Fence:       authority.OperationFence,
	}); err != nil {
		return err
	}
	if err := validateOperationTaskFence(operation, authority.TaskOwnerID, authority.TaskFence); err != nil {
		return err
	}
	if operation.Request.Task != nil {
		if err := validateTaskFenceForOperationTx(ctx, tx, operation.Request.Task, authority.TaskOwnerID, authority.TaskFence); err != nil {
			return err
		}
	}
	if err := validateAgentRunEffectFenceTx(ctx, tx, authority.WorkspaceID, authority.AgentRunID, authority.AgentRunOwnerID, int64(authority.AgentRunFence)); err != nil {
		return err
	}
	if authority.EffectID != "" {
		var effect OperationEffect
		var credentialFence, credentialEpoch int64
		var egressHash, destinationHash, networkBoundary, networkHash string
		if err := tx.QueryRow(ctx, `SELECT workspace_id,operation_id,step_id,attempt_id,effect_id,effect_class,boundary,idempotency_key,provider_request_id,provider_idempotency_supported,delivery_semantics,verification_required,verification_status,compensation_status,request_hash,schema_catalog_hash,schema_catalog_revision,credential_lease_id,credential_lease_fence,credential_revocation_epoch,credential_source_version,credential_source_expires_at,egress_policy_hash,destination_policy_hash,network_boundary,network_boundary_hash,created_at,verified_at FROM fornix.operation_effects WHERE workspace_id=$1 AND operation_id=$2 AND effect_id=$3 FOR SHARE`, authority.WorkspaceID, authority.OperationID, authority.EffectID).Scan(&effect.WorkspaceID, &effect.OperationID, &effect.StepID, &effect.AttemptID, &effect.EffectID, &effect.EffectClass, &effect.Boundary, &effect.IdempotencyKey, &effect.ProviderRequestID, &effect.ProviderIdempotency, &effect.DeliverySemantics, &effect.VerificationRequired, &effect.VerificationStatus, &effect.CompensationStatus, &effect.RequestHash, &effect.SchemaCatalogHash, &effect.SchemaCatalogRevision, &effect.CredentialLeaseID, &credentialFence, &credentialEpoch, &effect.CredentialSourceVersion, &effect.CredentialSourceExpiresAt, &egressHash, &destinationHash, &networkBoundary, &networkHash, &effect.CreatedAt, &effect.VerifiedAt); errors.Is(err, pgx.ErrNoRows) {
			return ErrOperationTransition
		} else if err != nil {
			return fmt.Errorf("read effect authority reservation: %w", err)
		}
		if credentialFence < 0 || credentialEpoch < 0 {
			return ErrOperationTransition
		}
		effect.CredentialLeaseFence, effect.CredentialRevocationEpoch = uint64(credentialFence), uint64(credentialEpoch)
		var boundaryErr error
		effect.ExternalBoundary, boundaryErr = boundaryFromColumns(egressHash, destinationHash, networkBoundary, networkHash)
		if boundaryErr != nil {
			return ErrOperationTransition
		}
		if authority.AttemptID != "" && effect.AttemptID != authority.AttemptID {
			return ErrOperationLeaseFenced
		}
		if authority.RequestHash != "" && effect.RequestHash != authority.RequestHash {
			return ErrOperationTransition
		}
		if authority.EffectReservationHash != "" {
			candidate := contracts.ExternalEffect{WorkspaceID: effect.WorkspaceID, Boundary: effect.Boundary, Class: contracts.EffectClass(effect.EffectClass), DeliveryGuarantee: effect.DeliverySemantics, ProviderIdempotency: effect.ProviderIdempotency, VerificationRequired: effect.VerificationRequired, VerificationStatus: effect.VerificationStatus, CompensationStatus: effect.CompensationStatus}
			if candidate.StableHash() != authority.EffectReservationHash {
				return ErrOperationTransition
			}
		}
		var effectState string
		if err := tx.QueryRow(ctx, `SELECT state FROM fornix.operation_effect_state WHERE workspace_id=$1 AND effect_id=$2 FOR SHARE`, authority.WorkspaceID, authority.EffectID).Scan(&effectState); errors.Is(err, pgx.ErrNoRows) {
			return ErrOperationTransition
		} else if err != nil {
			return fmt.Errorf("read live effect state: %w", err)
		}
		if expectedEffectState != "" && effectState != expectedEffectState {
			return fmt.Errorf("%w: effect state %q does not match expected state %q", ErrOperationTransition, effectState, expectedEffectState)
		}
		if err := validateAuthorityFactsWithBoundary(ctx, tx, authority.WorkspaceID, effect.SchemaCatalogHash, effect.SchemaCatalogRevision, effect.CredentialLeaseID, effect.CredentialLeaseFence, effect.CredentialRevocationEpoch, effect.CredentialSourceVersion, effect.CredentialSourceExpiresAt, effect.ExternalBoundary, contracts.EffectClass(effect.EffectClass)); err != nil {
			return fmt.Errorf("validate live effect authority facts: %w", err)
		}
	}
	return nil
}

// BeginEffectDispatch atomically validates the live operation, task, agent-run,
// credential, and effect authorities and commits the unique dispatch intent.
// This transaction is the authorization linearization point: a lease takeover
// that commits first rejects this call; a takeover after it does not revoke an
// already-issued one-shot external-effect permit. External execution remains
// at-least-once and is never held inside a database transaction.
func (s *OperationStore) BeginEffectDispatch(ctx context.Context, authority contracts.EffectAuthority, update contracts.ExternalEffectUpdate) (EffectStateResult, error) {
	if s == nil || s.pool == nil {
		return EffectStateResult{}, errors.New("operation store is not configured")
	}
	if err := authority.Normalize(); err != nil {
		return EffectStateResult{}, fmt.Errorf("normalize effect authority: %w", err)
	}
	if err := update.Normalize(); err != nil {
		return EffectStateResult{}, fmt.Errorf("normalize dispatch intent: %w", err)
	}
	if update.State != contracts.ExternalEffectDispatching || update.LeaseKind != "" ||
		update.WorkspaceID != authority.WorkspaceID || update.OperationID != authority.OperationID || update.EffectID != authority.EffectID ||
		update.OwnerID != authority.OperationOwnerID || update.Fence != authority.OperationFence {
		return EffectStateResult{}, fmt.Errorf("dispatch intent does not match its effect authority")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, authority.WorkspaceID)
	if err != nil {
		return EffectStateResult{}, fmt.Errorf("begin authorized dispatch intent: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.validateEffectAuthorityTx(ctx, tx, authority, ""); err != nil {
		return EffectStateResult{}, err
	}
	state, err := NewAdmissionStore(s.pool, s.events).UpdateEffectTx(ctx, tx, update)
	if err != nil {
		return EffectStateResult{}, err
	}
	if state.State.State != contracts.ExternalEffectDispatching {
		return EffectStateResult{}, fmt.Errorf("%w: dispatch intent committed in state %q", ErrOperationTransition, state.State.State)
	}
	if err := tx.Commit(ctx); err != nil {
		return EffectStateResult{}, fmt.Errorf("commit authorized dispatch intent: %w", err)
	}
	return state, nil
}

// OperationClaimOptions bounds one queue claim. MaxActive is a durable
// workspace-wide cap; zero leaves the cap disabled for compatibility with
// callers that already enforce concurrency elsewhere.
type OperationClaimOptions struct {
	Limit        int
	TTL          time.Duration
	MaxActive    int
	ReadOnlyOnly bool
}

// OperationResourceLease serializes one typed resource while an operation
// claim is active. Its fence is independent from the operation fence; every
// mutation still requires the operation lease fence as the authoritative
// operation boundary.
type OperationResourceLease struct {
	WorkspaceID    string
	ResourceKey    string
	OperationID    string
	OwnerID        string
	OperationFence uint64
	Fence          uint64
	LeaseUntil     time.Time
	AcquiredAt     time.Time
	RenewedAt      time.Time
	ReleasedAt     *time.Time
}

type OperationAttemptInput struct {
	WorkspaceID    string
	OperationID    string
	StepID         string
	Attempt        int
	AttemptID      string
	OwnerID        string
	Fence          uint64
	RequestHash    string
	IdempotencyKey string
}

type OperationAttempt struct {
	WorkspaceID          string
	OperationID          string
	StepID               string
	Attempt              int
	AttemptID            string
	OperationOwnerID     string
	OperationFence       uint64
	Status               string
	RequestHash          string
	ResponseHash         string
	Failure              json.RawMessage
	ExternalEffectStatus string
	StartedAt            time.Time
	CompletedAt          *time.Time
}

type OperationEffectInput struct {
	WorkspaceID               string
	OperationID               string
	StepID                    string
	AttemptID                 string
	OwnerID                   string
	Fence                     uint64
	Effect                    contracts.ExternalEffect
	RequestHash               string
	SchemaCatalogHash         string
	SchemaCatalogRevision     string
	CredentialLeaseID         string
	CredentialLeaseFence      uint64
	CredentialRevocationEpoch uint64
	CredentialSourceVersion   string
	CredentialSourceExpiresAt *time.Time
	ExternalBoundary          *contracts.ExternalBoundaryAuthority
	// RequireAllowedAdmission is set by the shared dispatcher and strict
	// public effect path. It is opt-in so recovery fixtures can inspect and
	// reconcile legacy reservations without rewriting their original decision.
	RequireAllowedAdmission bool
}

type OperationEffect struct {
	WorkspaceID               string
	OperationID               string
	StepID                    string
	AttemptID                 string
	EffectID                  string
	EffectClass               string
	Boundary                  string
	IdempotencyKey            string
	ProviderRequestID         string
	ProviderIdempotency       bool
	DeliverySemantics         string
	VerificationRequired      bool
	VerificationStatus        string
	CompensationStatus        string
	RequestHash               string
	ResponseHash              string
	SchemaCatalogHash         string
	SchemaCatalogRevision     string
	CredentialLeaseID         string
	CredentialLeaseFence      uint64
	CredentialRevocationEpoch uint64
	CredentialSourceVersion   string
	CredentialSourceExpiresAt *time.Time
	ExternalBoundary          *contracts.ExternalBoundaryAuthority
	CreatedAt                 time.Time
	VerifiedAt                *time.Time
}

type OperationCallbackInput struct {
	WorkspaceID  string
	OperationID  string
	OwnerID      string
	Fence        uint64
	CallbackID   string
	CallbackKind string
	ExternalID   string
	RequestHash  string
	ResponseHash string
	Status       string
}

type OperationCallback struct {
	WorkspaceID  string
	OperationID  string
	CallbackID   string
	CallbackKind string
	ExternalID   string
	RequestHash  string
	ResponseHash string
	Status       string
	ReceivedAt   time.Time
}

type OperationReplayResult struct {
	Operation           Operation
	StateVersion        int64
	StateHash           string
	CurrentStateVersion int64
	CurrentStateHash    string
	NextFromVersion     int64
	HasMore             bool
	Complete            bool
	ReplayHash          string
	TransitionCount     int
	Verified            bool
}

// OperationStore is the sole Postgres mutation boundary for generic operation
// authority. Existing task/run/tool/evidence authorities remain unchanged.
type OperationStore struct {
	pool                       *pgxpool.Pool
	events                     *EventStore
	failureHook                func(string) error
	deploymentEvidence         *DeploymentEvidenceStore
	requireDeploymentAdmission bool
	requireExternalBoundary    bool
}

func NewOperationStore(pool *pgxpool.Pool, events *EventStore) *OperationStore {
	if events == nil {
		events = NewEventStore(pool)
	}
	return &OperationStore{pool: pool, events: events}
}

// SetDeploymentAdmissionAuthority wires the existing deployment-evidence
// authority into generic effect reservation. The operation store does not
// copy release or verification rows; it only revalidates a request's
// hash-only reference inside the reservation transaction.
func (s *OperationStore) SetDeploymentAdmissionAuthority(authority *DeploymentEvidenceStore, required bool) {
	if s == nil {
		return
	}
	s.deploymentEvidence = authority
	s.requireDeploymentAdmission = required
}

// SetExternalBoundaryAuthorityRequired enables strict production admission for
// network effects. Development fake/read-only paths may leave it disabled;
// the strict server configuration enables it automatically.
func (s *OperationStore) SetExternalBoundaryAuthorityRequired(required bool) {
	if s != nil {
		s.requireExternalBoundary = required
	}
}

// SetFailureHook creates deterministic transaction crash tests. It is nil in
// production and never changes commit semantics.
func (s *OperationStore) SetFailureHook(hook func(string) error) {
	if s != nil {
		s.failureHook = hook
	}
}

// Create persists operation identity, optional plan/links, idempotency, and
// the initial created event atomically.
func (s *OperationStore) Create(ctx context.Context, input OperationCreateInput) (OperationCreateResult, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return OperationCreateResult{}, errors.New("operation store is not configured")
	}
	request := input.Request
	if err := request.Normalize(); err != nil {
		return OperationCreateResult{}, fmt.Errorf("normalize operation request: %w", err)
	}
	input.Request = request
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return OperationCreateResult{}, fmt.Errorf("begin operation create: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := s.CreateTx(ctx, tx, input)
	if err != nil {
		return OperationCreateResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return OperationCreateResult{}, fmt.Errorf("commit operation create: %w", err)
	}
	return result, nil
}

// CreateTx persists an operation and its initial event in the caller's
// transaction. Workflow creation uses this seam to make operation identity,
// workflow state, and the initial checkpoint one atomic Postgres commit.
func (s *OperationStore) CreateTx(ctx context.Context, tx pgx.Tx, input OperationCreateInput) (OperationCreateResult, error) {
	if s == nil || s.events == nil || tx == nil {
		return OperationCreateResult{}, errors.New("operation create transaction is not configured")
	}
	request := input.Request
	if err := request.Normalize(); err != nil {
		return OperationCreateResult{}, fmt.Errorf("normalize operation request: %w", err)
	}
	if err := setWorkspaceContext(ctx, tx, request.WorkspaceID); err != nil {
		return OperationCreateResult{}, err
	}
	requestHash, err := request.CanonicalHash()
	if err != nil {
		return OperationCreateResult{}, err
	}
	planJSON, planHash, plan, err := normalizePlan(request, input.Plan)
	if err != nil {
		return OperationCreateResult{}, err
	}
	operationHash, err := hashValue(struct {
		RequestHash string `json:"request_hash"`
		PlanHash    string `json:"plan_hash,omitempty"`
	}{requestHash, planHash})
	if err != nil {
		return OperationCreateResult{}, fmt.Errorf("hash operation command: %w", err)
	}
	if request.Task != nil && (strings.TrimSpace(input.TaskOwnerID) == "" || input.TaskFence == 0) {
		return OperationCreateResult{}, ErrOperationTaskFence
	}
	if request.Task == nil && (strings.TrimSpace(input.TaskOwnerID) != "" || input.TaskFence != 0) {
		return OperationCreateResult{}, ErrOperationTaskFence
	}
	if input.TaskFence > maxOperationFence {
		return OperationCreateResult{}, ErrOperationTaskFence
	}
	requestJSON, err := json.Marshal(request)
	if err != nil {
		return OperationCreateResult{}, fmt.Errorf("marshal operation request: %w", err)
	}
	stateHash, err := hashState(operationState{Status: contracts.OperationStatusCreated, StateVersion: 0, PlanHash: planHash})
	if err != nil {
		return OperationCreateResult{}, err
	}
	actorJSON, _ := json.Marshal(request.Actor)
	taskJSON, err := entityJSON(request.Task)
	if err != nil {
		return OperationCreateResult{}, err
	}
	sessionJSON, err := entityJSON(request.Session)
	if err != nil {
		return OperationCreateResult{}, err
	}
	inserted, err := tx.Exec(ctx, `
		INSERT INTO fornix.operations(workspace_id,id,schema_version,request_id,idempotency_key,request_hash,operation_hash,status,actor,task_ref,session_ref,request,plan,plan_hash,state_hash,task_owner_id,task_fence)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10::jsonb,$11::jsonb,$12::jsonb,$13::jsonb,$14,$15,$16,$17)
		ON CONFLICT (workspace_id,idempotency_key) DO NOTHING`, request.WorkspaceID, request.ID, request.SchemaVersion, request.RequestID, request.IdempotencyKey, requestHash, operationHash, contracts.OperationStatusCreated, actorJSON, taskJSON, sessionJSON, requestJSON, planJSON, planHash, stateHash, strings.TrimSpace(input.TaskOwnerID), int64(input.TaskFence))
	if err != nil {
		return OperationCreateResult{}, fmt.Errorf("insert operation: %w", err)
	}
	if inserted.RowsAffected() == 0 {
		stored, readErr := readOperationByKey(ctx, tx, request.WorkspaceID, request.IdempotencyKey)
		if readErr != nil {
			return OperationCreateResult{}, readErr
		}
		if stored.OperationHash != operationHash {
			return OperationCreateResult{}, fmt.Errorf("%w: %s", ErrOperationIdempotency, request.IdempotencyKey)
		}
		return OperationCreateResult{Operation: stored, Duplicate: true}, nil
	}
	if request.Task != nil {
		if err := validateTaskFenceForOperationTx(ctx, tx, request.Task, input.TaskOwnerID, input.TaskFence); err != nil {
			return OperationCreateResult{}, err
		}
	}
	if err := s.insertChildren(ctx, tx, request, plan, input.Resources, input.Links); err != nil {
		return OperationCreateResult{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.operation_idempotency(workspace_id,command,idempotency_key,request_hash,operation_id,outcome_hash) VALUES($1,'create',$2,$3,$4,$5)`, request.WorkspaceID, request.IdempotencyKey, operationHash, request.ID, stateHash); err != nil {
		return OperationCreateResult{}, fmt.Errorf("insert operation idempotency: %w", err)
	}
	event, err := s.eventFor(request, operationHash, contracts.OperationStatusCreated, 0, stateHash, operationCommandCreate)
	if err != nil {
		return OperationCreateResult{}, err
	}
	appended, err := s.events.AppendTx(ctx, tx, event)
	if err != nil {
		return OperationCreateResult{}, fmt.Errorf("append operation create event: %w", err)
	}
	if err := s.fail("operation_created"); err != nil {
		return OperationCreateResult{}, err
	}
	stored, err := readOperationByID(ctx, tx, request.WorkspaceID, request.ID)
	if err != nil {
		return OperationCreateResult{}, err
	}
	return OperationCreateResult{Operation: stored, Event: appended.Event}, nil
}

func (s *OperationStore) Get(ctx context.Context, workspaceID, operationID string) (Operation, error) {
	if s == nil || s.pool == nil {
		return Operation{}, errors.New("operation store is not configured")
	}
	workspaceID, operationID = strings.TrimSpace(workspaceID), strings.TrimSpace(operationID)
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return Operation{}, fmt.Errorf("begin operation read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	value, err := readOperationByID(ctx, tx, workspaceID, operationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, ErrOperationNotFound
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	return value, err
}

// GetEffect reads one immutable effect reservation inside its workspace. The
// verifier uses this bounded identity snapshot to bind adapter proof input;
// effect state transitions remain owned by AdmissionStore.
func (s *OperationStore) GetEffect(ctx context.Context, workspaceID, effectID string) (OperationEffect, error) {
	if s == nil || s.pool == nil {
		return OperationEffect{}, errors.New("operation store is not configured")
	}
	workspaceID, effectID = strings.TrimSpace(workspaceID), strings.TrimSpace(effectID)
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return OperationEffect{}, fmt.Errorf("begin effect read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	value, err := readEffectByID(ctx, tx, workspaceID, effectID)
	if errors.Is(err, pgx.ErrNoRows) {
		return OperationEffect{}, ErrOperationNotFound
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	return value, err
}

// AttachPlan persists the connector-produced plan and advances an operation
// from created to planned in one fenced transaction. The operation lease and,
// when present, the live task lease are both validated before mutation.
func (s *OperationStore) AttachPlan(ctx context.Context, input OperationPlanInput) (OperationPlanResult, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return OperationPlanResult{}, errors.New("operation store is not configured")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, input.WorkspaceID)
	if err != nil {
		return OperationPlanResult{}, fmt.Errorf("begin operation plan: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := s.attachPlanTx(ctx, tx, input)
	if err != nil {
		return OperationPlanResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return OperationPlanResult{}, fmt.Errorf("commit operation plan: %w", err)
	}
	return result, nil
}

func (s *OperationStore) attachPlanTx(ctx context.Context, tx pgx.Tx, input OperationPlanInput) (OperationPlanResult, error) {
	input.WorkspaceID, input.OperationID, input.OwnerID = strings.TrimSpace(input.WorkspaceID), strings.TrimSpace(input.OperationID), strings.TrimSpace(input.OwnerID)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if err := setWorkspaceContext(ctx, tx, input.WorkspaceID); err != nil {
		return OperationPlanResult{}, err
	}
	if input.WorkspaceID == "" || input.OperationID == "" || input.OwnerID == "" || input.Fence == 0 || input.Fence > maxOperationFence || input.IdempotencyKey == "" {
		return OperationPlanResult{}, ErrOperationLeaseFenced
	}
	operation, err := readOperationByID(ctx, tx, input.WorkspaceID, input.OperationID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return OperationPlanResult{}, ErrOperationNotFound
	}
	if err != nil {
		return OperationPlanResult{}, err
	}
	planJSON, planHash, plan, err := normalizePlan(operation.Request, &input.Plan)
	if err != nil {
		return OperationPlanResult{}, err
	}
	if operation.Status != contracts.OperationStatusCreated {
		if operation.PlanHash == planHash {
			return OperationPlanResult{Operation: operation, Duplicate: true}, nil
		}
		return OperationPlanResult{}, ErrOperationPlanConflict
	}
	if err := validateOperationTaskFence(operation, input.TaskOwnerID, input.TaskFence); err != nil {
		return OperationPlanResult{}, err
	}
	if operation.Request.Task != nil {
		if input.OwnerID != operation.TaskOwnerID || input.TaskFence == 0 {
			return OperationPlanResult{}, ErrOperationTaskFence
		}
		if err := validateTaskFenceForOperationTx(ctx, tx, operation.Request.Task, input.TaskOwnerID, input.TaskFence); err != nil {
			return OperationPlanResult{}, err
		}
	}
	if _, err := s.validateLease(ctx, tx, OperationLease{WorkspaceID: input.WorkspaceID, OperationID: input.OperationID, OwnerID: input.OwnerID, Fence: input.Fence}); err != nil {
		return OperationPlanResult{}, err
	}
	if operation.PlanHash != "" {
		if operation.PlanHash != planHash {
			return OperationPlanResult{}, ErrOperationPlanConflict
		}
	} else {
		if _, err := tx.Exec(ctx, `UPDATE fornix.operations SET plan=$3::jsonb,plan_hash=$4 WHERE workspace_id=$1 AND id=$2 AND status=$5`, input.WorkspaceID, input.OperationID, planJSON, planHash, contracts.OperationStatusCreated); err != nil {
			return OperationPlanResult{}, fmt.Errorf("persist operation plan: %w", err)
		}
		if err := s.insertChildren(ctx, tx, operation.Request, plan, nil, nil); err != nil {
			return OperationPlanResult{}, fmt.Errorf("persist operation steps: %w", err)
		}
	}
	transition, err := s.transitionTx(ctx, tx, OperationTransitionInput{
		WorkspaceID: input.WorkspaceID, OperationID: input.OperationID, OwnerID: input.OwnerID, Fence: input.Fence,
		TaskOwnerID: input.TaskOwnerID, TaskFence: input.TaskFence, Actor: input.Actor, RequestID: input.RequestID,
		IdempotencyKey: input.IdempotencyKey, CausationID: input.CausationID, CorrelationID: input.CorrelationID,
		ToStatus: contracts.OperationStatusPlanned, ReasonCode: "connector_plan",
	})
	if err != nil {
		return OperationPlanResult{}, err
	}
	_ = plan
	return OperationPlanResult{Operation: transition.Operation, Transition: transition.Transition, Event: transition.Event}, nil
}

// GetResult reads the immutable result for one workspace-scoped operation.
func (s *OperationStore) GetResult(ctx context.Context, workspaceID, operationID string) (OperationResultRecord, error) {
	if s == nil || s.pool == nil {
		return OperationResultRecord{}, errors.New("operation store is not configured")
	}
	workspaceID, operationID = strings.TrimSpace(workspaceID), strings.TrimSpace(operationID)
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return OperationResultRecord{}, fmt.Errorf("begin operation result read: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	record, err := readOperationResult(ctx, tx, workspaceID, operationID)
	if errors.Is(err, pgx.ErrNoRows) {
		return OperationResultRecord{}, ErrOperationResultNotFound
	}
	if err == nil {
		err = tx.Commit(ctx)
	}
	return record, err
}

// RecordResult inserts one bounded hash-only connector result and advances the
// operation to the result status atomically. A committed duplicate returns
// the original result without requiring a live lease, while a new write is
// always fenced.
func (s *OperationStore) RecordResult(ctx context.Context, input OperationResultInput) (OperationResultWrite, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return OperationResultWrite{}, errors.New("operation store is not configured")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, input.WorkspaceID)
	if err != nil {
		return OperationResultWrite{}, fmt.Errorf("begin operation result: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := s.RecordResultTx(ctx, tx, input)
	if err != nil {
		return OperationResultWrite{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return OperationResultWrite{}, fmt.Errorf("commit operation result: %w", err)
	}
	return result, nil
}

// RecordResultTx appends one fenced, workspace-scoped operation result to a
// caller-owned transaction. It performs no commit and is intended for local
// authority compositions that must atomically finalize an effect, its domain
// link, and its operation result. The caller must use the same Postgres pool
// and must not perform external I/O inside the transaction.
func (s *OperationStore) RecordResultTx(ctx context.Context, tx pgx.Tx, input OperationResultInput) (OperationResultWrite, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return OperationResultWrite{}, errors.New("operation store is not configured")
	}
	if tx == nil {
		return OperationResultWrite{}, errors.New("operation result transaction is nil")
	}
	if err := input.Result.Normalize(); err != nil {
		return OperationResultWrite{}, fmt.Errorf("normalize operation result: %w", err)
	}
	if input.Result.WorkspaceID != strings.TrimSpace(input.WorkspaceID) || input.Result.OperationID != strings.TrimSpace(input.OperationID) {
		return OperationResultWrite{}, ErrOperationWorkspace
	}
	if input.Result.StableHash() == "" {
		return OperationResultWrite{}, ErrOperationResultConflict
	}
	return s.recordResultTx(ctx, tx, input)
}

func (s *OperationStore) recordResultTx(ctx context.Context, tx pgx.Tx, input OperationResultInput) (OperationResultWrite, error) {
	if err := setWorkspaceContext(ctx, tx, input.WorkspaceID); err != nil {
		return OperationResultWrite{}, err
	}
	operation, err := readOperationByID(ctx, tx, input.WorkspaceID, input.OperationID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return OperationResultWrite{}, ErrOperationNotFound
	}
	if err != nil {
		return OperationResultWrite{}, err
	}
	if input.Result.OperationHash != operation.OperationHash {
		return OperationResultWrite{}, ErrOperationResultConflict
	}
	resultHash := input.Result.StableHash()
	if existing, readErr := readOperationResult(ctx, tx, input.WorkspaceID, input.OperationID); readErr == nil {
		if existing.ResultHash != resultHash {
			return OperationResultWrite{}, ErrOperationResultConflict
		}
		return OperationResultWrite{Operation: operation, Record: existing, Duplicate: true}, nil
	} else if !errors.Is(readErr, pgx.ErrNoRows) {
		return OperationResultWrite{}, readErr
	}
	if operation.Status != contracts.OperationStatusRunning && operation.Status != contracts.OperationStatusRecoveryRequired {
		return OperationResultWrite{}, fmt.Errorf("%w: operation is %s", ErrOperationTransition, operation.Status)
	}
	if input.Actor.ID == "" {
		input.Actor = operation.Request.Actor
	}
	if input.Actor.WorkspaceID != operation.WorkspaceID || input.Actor.ID != operation.Request.Actor.ID || input.Actor.Kind != operation.Request.Actor.Kind || input.Actor.Name != operation.Request.Actor.Name {
		return OperationResultWrite{}, ErrOperationWorkspace
	}
	if err := validateOperationTaskFence(operation, input.TaskOwnerID, input.TaskFence); err != nil {
		return OperationResultWrite{}, err
	}
	if operation.Request.Task != nil {
		if input.OwnerID != operation.TaskOwnerID || input.TaskFence == 0 {
			return OperationResultWrite{}, ErrOperationTaskFence
		}
		if err := validateTaskFenceForOperationTx(ctx, tx, operation.Request.Task, input.TaskOwnerID, input.TaskFence); err != nil {
			return OperationResultWrite{}, err
		}
	}
	if _, err := s.validateLease(ctx, tx, OperationLease{WorkspaceID: input.WorkspaceID, OperationID: input.OperationID, OwnerID: input.OwnerID, Fence: input.Fence}); err != nil {
		return OperationResultWrite{}, err
	}
	if err := mergeResultAuthorityFactsTx(ctx, tx, &input); err != nil {
		return OperationResultWrite{}, fmt.Errorf("resolve operation authority facts: %w", err)
	}
	resultID := input.Result.ID
	if resultID == "" {
		resultID = contracts.NewID("operation-result")
	}
	// Keep the embedded durable contract and the relational identity bound to
	// the same result record. StableHash intentionally excludes this delivery
	// identity, so retries remain idempotent even when the caller omitted it.
	input.Result.ID = resultID
	resultJSON, err := json.Marshal(input.Result)
	if err != nil {
		return OperationResultWrite{}, err
	}
	if len(resultJSON) > 262144 {
		return OperationResultWrite{}, ErrOperationResultConflict
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.operation_results(workspace_id,operation_id,result_id,result_hash,result) VALUES($1,$2,$3,$4,$5::jsonb)`, input.WorkspaceID, input.OperationID, resultID, resultHash, resultJSON); err != nil {
		return OperationResultWrite{}, fmt.Errorf("insert operation result: %w", err)
	}
	for _, step := range input.Result.Steps {
		updated, updateErr := tx.Exec(ctx, `UPDATE fornix.operation_steps SET status=$4,output_hash=$5 WHERE workspace_id=$1 AND operation_id=$2 AND step_id=$3`, input.WorkspaceID, input.OperationID, step.StepID, step.Status, step.OutputHash)
		if updateErr != nil {
			return OperationResultWrite{}, fmt.Errorf("update operation step result: %w", updateErr)
		}
		if updated.RowsAffected() != 1 {
			return OperationResultWrite{}, fmt.Errorf("operation result references unknown step %q", step.StepID)
		}
	}
	transition, err := s.transitionTx(ctx, tx, OperationTransitionInput{
		WorkspaceID: input.WorkspaceID, OperationID: input.OperationID, OwnerID: input.OwnerID, Fence: input.Fence,
		TaskOwnerID: input.TaskOwnerID, TaskFence: input.TaskFence, Actor: input.Actor, RequestID: input.RequestID,
		IdempotencyKey: input.IdempotencyKey, CausationID: input.CausationID, CorrelationID: input.CorrelationID,
		ToStatus: input.Result.Status, ResultHash: resultHash, ReportHash: input.Result.ReportHash, Failure: input.Result.Failure,
	})
	if err != nil {
		return OperationResultWrite{}, err
	}
	admission, err := admissionForOperationTx(ctx, tx, input.WorkspaceID, input.OperationID)
	if err != nil {
		return OperationResultWrite{}, fmt.Errorf("read operation admission authority: %w", err)
	}
	if _, _, err := appendAuthorityLinkTx(ctx, tx, authorityResultLink(input, transition.Operation, input.Result, resultID, resultHash, admission)); err != nil {
		return OperationResultWrite{}, fmt.Errorf("append result authority link: %w", err)
	}
	if err := s.fail("operation_result_recorded"); err != nil {
		return OperationResultWrite{}, err
	}
	record, err := readOperationResult(ctx, tx, input.WorkspaceID, input.OperationID)
	if err != nil {
		return OperationResultWrite{}, err
	}
	return OperationResultWrite{Operation: transition.Operation, Record: record, Transition: transition.Transition, Event: transition.Event}, nil
}

func (s *OperationStore) AcquireLease(ctx context.Context, workspaceID, operationID, ownerID string, ttl time.Duration) (OperationLeaseResult, error) {
	if s == nil || s.pool == nil {
		return OperationLeaseResult{}, errors.New("operation store is not configured")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return OperationLeaseResult{}, fmt.Errorf("begin operation lease acquire: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := s.AcquireLeaseTx(ctx, tx, workspaceID, operationID, ownerID, ttl)
	if err != nil {
		return OperationLeaseResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return OperationLeaseResult{}, fmt.Errorf("commit operation lease acquire: %w", err)
	}
	return result, nil
}

func (s *OperationStore) AcquireLeaseTx(ctx context.Context, tx pgx.Tx, workspaceID, operationID, ownerID string, ttl time.Duration) (OperationLeaseResult, error) {
	workspaceID, operationID, ownerID = strings.TrimSpace(workspaceID), strings.TrimSpace(operationID), strings.TrimSpace(ownerID)
	if tx == nil {
		return OperationLeaseResult{}, errors.New("operation lease transaction is nil")
	}
	if workspaceID == "" || operationID == "" || ownerID == "" {
		return OperationLeaseResult{}, errors.New("workspace_id, operation_id, and owner_id are required")
	}
	if err := setWorkspaceContext(ctx, tx, workspaceID); err != nil {
		return OperationLeaseResult{}, err
	}
	operation, err := readOperationByID(ctx, tx, workspaceID, operationID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return OperationLeaseResult{}, ErrOperationNotFound
	}
	if err != nil {
		return OperationLeaseResult{}, fmt.Errorf("check operation for lease: %w", err)
	}
	if contracts.IsTerminalOperationStatus(operation.Status) {
		return OperationLeaseResult{}, ErrOperationTerminal
	}
	if operation.Request.Task != nil {
		if ownerID != operation.TaskOwnerID || operation.TaskFence == 0 {
			return OperationLeaseResult{}, ErrOperationTaskFence
		}
		if err := validateTaskFenceForOperationTx(ctx, tx, operation.Request.Task, operation.TaskOwnerID, operation.TaskFence); err != nil {
			return OperationLeaseResult{}, err
		}
	}
	ttl = boundedLeaseTTL(ttl)
	inserted, err := tx.Exec(ctx, `INSERT INTO fornix.operation_leases(workspace_id,operation_id,owner_id,fence,lease_until) VALUES($1,$2,$3,1,clock_timestamp()+($4::double precision * interval '1 millisecond')) ON CONFLICT (workspace_id,operation_id) DO NOTHING`, workspaceID, operationID, ownerID, ttl.Milliseconds())
	if err != nil {
		return OperationLeaseResult{}, fmt.Errorf("insert operation lease: %w", err)
	}
	lease, active, err := readLease(ctx, tx, workspaceID, operationID, true)
	if err != nil {
		return OperationLeaseResult{}, err
	}
	if active {
		if lease.OwnerID != ownerID {
			return OperationLeaseResult{}, ErrOperationLeaseHeld
		}
		if err := s.acquireResourceLeasesTx(ctx, tx, operation, lease, ttl); err != nil {
			return OperationLeaseResult{}, err
		}
		return OperationLeaseResult{Lease: lease, Acquired: inserted.RowsAffected() == 1, Reused: inserted.RowsAffected() == 0}, nil
	}
	if lease.Fence >= maxOperationFence {
		return OperationLeaseResult{}, ErrOperationFenceExhausted
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.operation_leases SET owner_id=$3,fence=fence+1,lease_until=clock_timestamp()+($4::double precision * interval '1 millisecond'),acquired_at=clock_timestamp(),renewed_at=clock_timestamp(),released_at=NULL WHERE workspace_id=$1 AND operation_id=$2 AND fence=$5`, workspaceID, operationID, ownerID, ttl.Milliseconds(), int64(lease.Fence)); err != nil {
		return OperationLeaseResult{}, fmt.Errorf("take over operation lease: %w", err)
	}
	updated, updatedActive, err := readLease(ctx, tx, workspaceID, operationID, true)
	if err != nil {
		return OperationLeaseResult{}, err
	}
	if !updatedActive || updated.OwnerID != ownerID || updated.Fence <= lease.Fence {
		return OperationLeaseResult{}, errors.New("operation lease takeover did not produce a higher active fence")
	}
	if err := s.acquireResourceLeasesTx(ctx, tx, operation, updated, ttl); err != nil {
		return OperationLeaseResult{}, err
	}
	return OperationLeaseResult{Lease: updated, Acquired: true, Takeover: true}, nil
}

// acquireResourceLeasesTx serializes every declared operation resource. The
// advisory transaction locks prevent two claim transactions from observing the
// same free resource before either writes its current lease. Resource keys are
// sorted so multi-resource operations cannot deadlock one another.
func (s *OperationStore) acquireResourceLeasesTx(ctx context.Context, tx pgx.Tx, operation Operation, lease OperationLease, ttl time.Duration) error {
	keys, err := operationResourceKeysTx(ctx, tx, operation)
	if err != nil {
		return err
	}
	for _, resourceKey := range keys {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, operation.WorkspaceID+":resource:"+resourceKey); err != nil {
			return fmt.Errorf("lock operation resource %s: %w", resourceKey, err)
		}
		var current OperationResourceLease
		var releasedAt *time.Time
		var active bool
		err := tx.QueryRow(ctx, `
			SELECT workspace_id,resource_key,operation_id,owner_id,operation_fence,fence,
			       lease_until,acquired_at,renewed_at,released_at,
			       (released_at IS NULL AND lease_until > clock_timestamp())
			FROM fornix.operation_resource_leases
			WHERE workspace_id=$1 AND resource_key=$2
			FOR UPDATE`, operation.WorkspaceID, resourceKey).Scan(
			&current.WorkspaceID, &current.ResourceKey, &current.OperationID, &current.OwnerID,
			&current.OperationFence, &current.Fence, &current.LeaseUntil, &current.AcquiredAt,
			&current.RenewedAt, &releasedAt, &active)
		if errors.Is(err, pgx.ErrNoRows) {
			if _, err := tx.Exec(ctx, `
				INSERT INTO fornix.operation_resource_leases(
				 workspace_id,resource_key,operation_id,owner_id,operation_fence,fence,lease_until)
				VALUES($1,$2,$3,$4,$5,1,clock_timestamp()+($6::double precision * interval '1 millisecond'))`,
				operation.WorkspaceID, resourceKey, operation.ID, lease.OwnerID, int64(lease.Fence), ttl.Milliseconds()); err != nil {
				return fmt.Errorf("insert operation resource lease: %w", err)
			}
			if err := recordResourceLeaseHistoryTx(ctx, tx, operation.WorkspaceID, resourceKey, operation.ID, lease.OwnerID, lease.Fence, 1, "acquired"); err != nil {
				return err
			}
			continue
		}
		if err != nil {
			return fmt.Errorf("read operation resource lease: %w", err)
		}
		if active && (current.OperationID != operation.ID || current.OwnerID != lease.OwnerID || current.OperationFence == lease.Fence) {
			if current.OperationID != operation.ID || current.OwnerID != lease.OwnerID {
				return fmt.Errorf("%w: %s", ErrOperationResourceBusy, resourceKey)
			}
			continue
		}
		if current.Fence >= maxOperationFence {
			return ErrOperationFenceExhausted
		}
		action := "takeover"
		if releasedAt != nil {
			action = "acquired"
		}
		if _, err := tx.Exec(ctx, `
			UPDATE fornix.operation_resource_leases
			SET operation_id=$3,owner_id=$4,operation_fence=$5,fence=fence+1,
			    lease_until=clock_timestamp()+($6::double precision * interval '1 millisecond'),
			    acquired_at=clock_timestamp(),renewed_at=clock_timestamp(),released_at=NULL
			WHERE workspace_id=$1 AND resource_key=$2 AND fence=$7`,
			operation.WorkspaceID, resourceKey, operation.ID, lease.OwnerID, int64(lease.Fence),
			ttl.Milliseconds(), int64(current.Fence)); err != nil {
			return fmt.Errorf("take over operation resource lease: %w", err)
		}
		if err := recordResourceLeaseHistoryTx(ctx, tx, operation.WorkspaceID, resourceKey, operation.ID, lease.OwnerID, lease.Fence, current.Fence+1, action); err != nil {
			return err
		}
	}
	return nil
}

func operationResourceKeysTx(ctx context.Context, tx pgx.Tx, operation Operation) ([]string, error) {
	rows, err := tx.Query(ctx, `
		SELECT resource_kind,resource_id
		FROM fornix.operation_resources
		WHERE workspace_id=$1 AND operation_id=$2
		ORDER BY ordinal`, operation.WorkspaceID, operation.ID)
	if err != nil {
		return nil, fmt.Errorf("read operation resources: %w", err)
	}
	defer rows.Close()
	seen := make(map[string]struct{})
	keys := make([]string, 0, 4)
	for rows.Next() {
		var kind, id string
		if err := rows.Scan(&kind, &id); err != nil {
			return nil, fmt.Errorf("scan operation resource: %w", err)
		}
		key := resourceLeaseKey(kind, id)
		if _, exists := seen[key]; !exists {
			seen[key] = struct{}{}
			keys = append(keys, key)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate operation resources: %w", err)
	}
	sort.Strings(keys)
	return keys, nil
}

func resourceLeaseKey(kind, id string) string {
	return strings.TrimSpace(kind) + ":" + strings.TrimSpace(id)
}

func recordResourceLeaseHistoryTx(ctx context.Context, tx pgx.Tx, workspaceID, resourceKey, operationID, ownerID string, operationFence, fence uint64, action string) error {
	var leaseUntil time.Time
	if err := tx.QueryRow(ctx, `SELECT lease_until FROM fornix.operation_resource_leases WHERE workspace_id=$1 AND resource_key=$2`, workspaceID, resourceKey).Scan(&leaseUntil); err != nil {
		return fmt.Errorf("read operation resource lease history deadline: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.operation_resource_lease_history(
		 workspace_id,resource_key,operation_id,owner_id,operation_fence,fence,action,lease_until)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8)`,
		workspaceID, resourceKey, operationID, ownerID, int64(operationFence), int64(fence), action, leaseUntil); err != nil {
		return fmt.Errorf("append operation resource lease history: %w", err)
	}
	return nil
}

func readResourceLeasesTx(ctx context.Context, tx pgx.Tx, workspaceID, operationID string) ([]OperationResourceLease, error) {
	rows, err := tx.Query(ctx, `
		SELECT workspace_id,resource_key,operation_id,owner_id,operation_fence,fence,
		       lease_until,acquired_at,renewed_at,released_at
		FROM fornix.operation_resource_leases
		WHERE workspace_id=$1 AND operation_id=$2
		ORDER BY resource_key`, workspaceID, operationID)
	if err != nil {
		return nil, fmt.Errorf("read operation resource leases: %w", err)
	}
	defer rows.Close()
	leases := make([]OperationResourceLease, 0)
	for rows.Next() {
		var lease OperationResourceLease
		if err := rows.Scan(&lease.WorkspaceID, &lease.ResourceKey, &lease.OperationID, &lease.OwnerID, &lease.OperationFence, &lease.Fence, &lease.LeaseUntil, &lease.AcquiredAt, &lease.RenewedAt, &lease.ReleasedAt); err != nil {
			return nil, fmt.Errorf("scan operation resource lease: %w", err)
		}
		leases = append(leases, lease)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate operation resource leases: %w", err)
	}
	return leases, nil
}

func validateResourceLeasesForRenewalTx(ctx context.Context, tx pgx.Tx, lease OperationLease) error {
	var expected, total, valid int
	if err := tx.QueryRow(ctx, `
		SELECT count(DISTINCT btrim(resource_kind) || ':' || btrim(resource_id))
		FROM fornix.operation_resources
		WHERE workspace_id=$1 AND operation_id=$2`, lease.WorkspaceID, lease.OperationID).Scan(&expected); err != nil {
		return fmt.Errorf("count expected operation resource leases: %w", err)
	}
	if err := tx.QueryRow(ctx, `
		SELECT count(*)::int,
		       count(*) FILTER (WHERE owner_id=$3 AND operation_fence=$4 AND released_at IS NULL AND lease_until > clock_timestamp())::int
		FROM fornix.operation_resource_leases
		WHERE workspace_id=$1 AND operation_id=$2`, lease.WorkspaceID, lease.OperationID, lease.OwnerID, int64(lease.Fence)).Scan(&total, &valid); err != nil {
		return fmt.Errorf("validate operation resource leases: %w", err)
	}
	if expected != total || total != valid {
		return ErrOperationLeaseFenced
	}
	return nil
}

func lockResourceLeasesTx(ctx context.Context, tx pgx.Tx, lease OperationLease) error {
	rows, err := tx.Query(ctx, `
		SELECT resource_key
		FROM fornix.operation_resource_leases
		WHERE workspace_id=$1 AND operation_id=$2
		ORDER BY resource_key`, lease.WorkspaceID, lease.OperationID)
	if err != nil {
		return fmt.Errorf("read operation resource keys for lock: %w", err)
	}
	keys := make([]string, 0, 4)
	for rows.Next() {
		var resourceKey string
		if err := rows.Scan(&resourceKey); err != nil {
			rows.Close()
			return fmt.Errorf("scan operation resource key for lock: %w", err)
		}
		keys = append(keys, resourceKey)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate operation resource keys for lock: %w", err)
	}
	rows.Close()
	for _, resourceKey := range keys {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, lease.WorkspaceID+":resource:"+resourceKey); err != nil {
			return fmt.Errorf("lock operation resource %s: %w", resourceKey, err)
		}
	}
	return nil
}

func renewResourceLeasesTx(ctx context.Context, tx pgx.Tx, lease OperationLease, ttl time.Duration) error {
	if err := lockResourceLeasesTx(ctx, tx, lease); err != nil {
		return err
	}
	result, err := tx.Exec(ctx, `
		UPDATE fornix.operation_resource_leases
		SET lease_until=clock_timestamp()+($5::double precision * interval '1 millisecond'),renewed_at=clock_timestamp()
		WHERE workspace_id=$1 AND operation_id=$2 AND owner_id=$3 AND operation_fence=$4 AND released_at IS NULL`,
		lease.WorkspaceID, lease.OperationID, lease.OwnerID, int64(lease.Fence), ttl.Milliseconds())
	if err != nil {
		return fmt.Errorf("renew operation resource leases: %w", err)
	}
	var resourceCount int
	if err := tx.QueryRow(ctx, `
		SELECT count(*)::int
		FROM fornix.operation_resource_leases
		WHERE workspace_id=$1 AND operation_id=$2`, lease.WorkspaceID, lease.OperationID).Scan(&resourceCount); err != nil {
		return fmt.Errorf("count renewed operation resource leases: %w", err)
	}
	if result.RowsAffected() == 0 && resourceCount > 0 {
		return ErrOperationLeaseFenced
	}
	return nil
}

func releaseResourceLeasesTx(ctx context.Context, tx pgx.Tx, lease OperationLease) error {
	if err := lockResourceLeasesTx(ctx, tx, lease); err != nil {
		return err
	}
	rows, err := tx.Query(ctx, `
		UPDATE fornix.operation_resource_leases
		SET released_at=clock_timestamp(),lease_until=clock_timestamp()
		WHERE workspace_id=$1 AND operation_id=$2 AND owner_id=$3 AND operation_fence=$4 AND released_at IS NULL
		RETURNING resource_key,fence`, lease.WorkspaceID, lease.OperationID, lease.OwnerID, int64(lease.Fence))
	if err != nil {
		return fmt.Errorf("release operation resource leases: %w", err)
	}
	type releasedResource struct {
		key   string
		fence int64
	}
	released := make([]releasedResource, 0, 4)
	for rows.Next() {
		var resourceKey string
		var fence int64
		if err := rows.Scan(&resourceKey, &fence); err != nil {
			rows.Close()
			return fmt.Errorf("scan released operation resource lease: %w", err)
		}
		released = append(released, releasedResource{key: resourceKey, fence: fence})
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return fmt.Errorf("iterate released operation resource leases: %w", err)
	}
	rows.Close()
	for _, resource := range released {
		if _, err := tx.Exec(ctx, `
			INSERT INTO fornix.operation_resource_lease_history(
			 workspace_id,resource_key,operation_id,owner_id,operation_fence,fence,action)
			VALUES($1,$2,$3,$4,$5,$6,'released')`,
			lease.WorkspaceID, resource.key, lease.OperationID, lease.OwnerID, int64(lease.Fence), resource.fence); err != nil {
			return fmt.Errorf("append released operation resource lease history: %w", err)
		}
	}
	return nil
}

// ClaimReady selects a bounded deterministic batch of due operations and
// acquires their operation leases in the same transaction. The operation row
// lock serializes a queue claim with a direct lease acquisition; the monotonic
// lease fence remains the authority for every later mutation.
//
// Generic queue selection intentionally excludes awaiting_external. An
// uncertain provider effect must be handled through the independent effect
// recovery lease and reconciliation API, never by silently redispatching it.
func (s *OperationStore) ClaimReady(ctx context.Context, workspaceID, ownerID string, limit int, ttl time.Duration) ([]OperationClaim, error) {
	return s.ClaimReadyWithOptions(ctx, workspaceID, ownerID, OperationClaimOptions{Limit: limit, TTL: ttl})
}

// ClaimReadyWithOptions selects a bounded deterministic batch and optionally
// enforces a workspace-wide active-lease quota. The quota check is serialized
// with a transaction advisory lock, so concurrent workers cannot both observe
// spare capacity and exceed the configured limit.
func (s *OperationStore) ClaimReadyWithOptions(ctx context.Context, workspaceID, ownerID string, options OperationClaimOptions) ([]OperationClaim, error) {
	if s == nil || s.pool == nil {
		return nil, errors.New("operation store is not configured")
	}
	workspaceID, ownerID = strings.TrimSpace(workspaceID), strings.TrimSpace(ownerID)
	if workspaceID == "" || ownerID == "" {
		return nil, errors.New("workspace_id and owner_id are required")
	}
	limit := options.Limit
	if limit <= 0 || limit > 64 {
		limit = 64
	}
	ttl := options.TTL
	maxActive := options.MaxActive
	if maxActive < 0 || maxActive > maxOperationActiveLimit {
		return nil, fmt.Errorf("max_active must be between 0 and %d", maxOperationActiveLimit)
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return nil, fmt.Errorf("begin operation queue claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setWorkspaceContext(ctx, tx, workspaceID); err != nil {
		return nil, err
	}
	if maxActive > 0 {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, workspaceID+":operation-queue"); err != nil {
			return nil, fmt.Errorf("lock operation workspace queue: %w", err)
		}
		var active int
		if err := tx.QueryRow(ctx, `
			SELECT count(*)::int
			FROM fornix.operation_leases
			WHERE workspace_id=$1 AND released_at IS NULL AND lease_until > clock_timestamp()`, workspaceID).Scan(&active); err != nil {
			return nil, fmt.Errorf("count active operation leases: %w", err)
		}
		remaining := maxActive - active
		if remaining <= 0 {
			if err := tx.Commit(ctx); err != nil {
				return nil, fmt.Errorf("commit full operation workspace queue: %w", err)
			}
			return []OperationClaim{}, nil
		}
		if limit > remaining {
			limit = remaining
		}
	}
	rows, err := tx.Query(ctx, `
		SELECT o.id
		FROM fornix.operations o
		LEFT JOIN fornix.operation_leases l
		  ON l.workspace_id=o.workspace_id AND l.operation_id=o.id
		WHERE o.workspace_id=$1
		  AND o.status IN ('created','planned','admitted','awaiting_retry','verifying','recovery_required')
		  AND (o.next_retry_at IS NULL OR o.next_retry_at <= clock_timestamp())
		  AND ($3 = false OR (
				 o.plan IS NOT NULL
				 AND jsonb_array_length(COALESCE(o.plan->'steps', '[]'::jsonb)) > 0
				 AND NOT EXISTS (
					 SELECT 1
					 FROM jsonb_array_elements(o.plan->'steps') AS step
					 WHERE COALESCE(step->>'effect', '') NOT IN ('read_only', 'observation')
				 )
			))
		  AND (l.operation_id IS NULL OR l.released_at IS NOT NULL OR l.lease_until <= clock_timestamp())
		ORDER BY COALESCE(o.next_retry_at, o.created_at), o.created_at, o.id
		FOR UPDATE OF o SKIP LOCKED
		LIMIT $2`, workspaceID, limit, options.ReadOnlyOnly)
	if err != nil {
		return nil, fmt.Errorf("select ready operations: %w", err)
	}
	defer rows.Close()
	operationIDs := make([]string, 0, limit)
	for rows.Next() {
		var operationID string
		if err := rows.Scan(&operationID); err != nil {
			return nil, fmt.Errorf("scan ready operation: %w", err)
		}
		operationIDs = append(operationIDs, operationID)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate ready operations: %w", err)
	}
	claims := make([]OperationClaim, 0, len(operationIDs))
	for _, operationID := range operationIDs {
		lease, err := s.AcquireLeaseTx(ctx, tx, workspaceID, operationID, ownerID, ttl)
		if errors.Is(err, ErrOperationLeaseHeld) {
			// A direct claimant may have committed between candidate selection
			// and this row's lock acquisition. It is safe to omit this item;
			// the next poll will see its active lease.
			continue
		}
		if errors.Is(err, ErrOperationResourceBusy) {
			// AcquireLeaseTx may have advanced this operation's fence before a
			// resource conflict was discovered. Roll that claim back to released
			// state inside the same transaction so the next worker can retry it.
			if current, _, readErr := readLease(ctx, tx, workspaceID, operationID, true); readErr == nil && current.OwnerID == ownerID {
				_ = s.ReleaseLeaseTx(ctx, tx, current)
			}
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("claim operation %s: %w", operationID, err)
		}
		operation, err := readOperationByID(ctx, tx, workspaceID, operationID)
		if err != nil {
			return nil, fmt.Errorf("read claimed operation %s: %w", operationID, err)
		}
		resourceLeases, err := readResourceLeasesTx(ctx, tx, workspaceID, operationID)
		if err != nil {
			return nil, fmt.Errorf("read claimed operation resources %s: %w", operationID, err)
		}
		claims = append(claims, OperationClaim{Operation: operation, Lease: lease.Lease, ResourceLeases: resourceLeases})
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit operation queue claim: %w", err)
	}
	return claims, nil
}

func (s *OperationStore) RenewLease(ctx context.Context, lease OperationLease, ttl time.Duration) (OperationLease, error) {
	if s == nil || s.pool == nil {
		return OperationLease{}, errors.New("operation store is not configured")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, lease.WorkspaceID)
	if err != nil {
		return OperationLease{}, fmt.Errorf("begin operation lease renew: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	updated, err := s.RenewLeaseTx(ctx, tx, lease, ttl)
	if err != nil {
		return OperationLease{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return OperationLease{}, fmt.Errorf("commit operation lease renew: %w", err)
	}
	return updated, nil
}

func (s *OperationStore) RenewLeaseTx(ctx context.Context, tx pgx.Tx, lease OperationLease, ttl time.Duration) (OperationLease, error) {
	if err := setWorkspaceContext(ctx, tx, lease.WorkspaceID); err != nil {
		return OperationLease{}, err
	}
	if _, err := s.validateLease(ctx, tx, lease); err != nil {
		return OperationLease{}, err
	}
	if err := validateResourceLeasesForRenewalTx(ctx, tx, lease); err != nil {
		return OperationLease{}, err
	}
	ttl = boundedLeaseTTL(ttl)
	if _, err := tx.Exec(ctx, `UPDATE fornix.operation_leases SET lease_until=clock_timestamp()+($3::double precision * interval '1 millisecond'),renewed_at=clock_timestamp() WHERE workspace_id=$1 AND operation_id=$2 AND owner_id=$4 AND fence=$5`, lease.WorkspaceID, lease.OperationID, ttl.Milliseconds(), lease.OwnerID, int64(lease.Fence)); err != nil {
		return OperationLease{}, fmt.Errorf("renew operation lease: %w", err)
	}
	if err := renewResourceLeasesTx(ctx, tx, lease, ttl); err != nil {
		return OperationLease{}, err
	}
	updated, active, err := readLease(ctx, tx, lease.WorkspaceID, lease.OperationID, true)
	if err != nil {
		return OperationLease{}, err
	}
	if !active {
		return OperationLease{}, ErrOperationLeaseExpired
	}
	return updated, nil
}

func (s *OperationStore) ReleaseLease(ctx context.Context, lease OperationLease) error {
	if s == nil || s.pool == nil {
		return errors.New("operation store is not configured")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, lease.WorkspaceID)
	if err != nil {
		return fmt.Errorf("begin operation lease release: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.ReleaseLeaseTx(ctx, tx, lease); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit operation lease release: %w", err)
	}
	return nil
}

func (s *OperationStore) ReleaseLeaseTx(ctx context.Context, tx pgx.Tx, lease OperationLease) error {
	if err := setWorkspaceContext(ctx, tx, lease.WorkspaceID); err != nil {
		return err
	}
	if _, err := s.validateLease(ctx, tx, lease); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.operation_leases SET released_at=clock_timestamp(),lease_until=clock_timestamp() WHERE workspace_id=$1 AND operation_id=$2 AND owner_id=$3 AND fence=$4`, lease.WorkspaceID, lease.OperationID, lease.OwnerID, int64(lease.Fence)); err != nil {
		return fmt.Errorf("release operation lease: %w", err)
	}
	if err := releaseResourceLeasesTx(ctx, tx, lease); err != nil {
		return err
	}
	return nil
}

func (s *OperationStore) Transition(ctx context.Context, input OperationTransitionInput) (OperationTransitionResult, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return OperationTransitionResult{}, errors.New("operation store is not configured")
	}
	input, err := normalizeTransition(input)
	if err != nil {
		return OperationTransitionResult{}, err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, input.WorkspaceID)
	if err != nil {
		return OperationTransitionResult{}, fmt.Errorf("begin operation transition: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := s.transitionTx(ctx, tx, input)
	if err != nil {
		return OperationTransitionResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return OperationTransitionResult{}, fmt.Errorf("commit operation transition: %w", err)
	}
	return result, nil
}

func (s *OperationStore) transitionTx(ctx context.Context, tx pgx.Tx, input OperationTransitionInput) (OperationTransitionResult, error) {
	if err := setWorkspaceContext(ctx, tx, input.WorkspaceID); err != nil {
		return OperationTransitionResult{}, err
	}
	operation, err := readOperationByID(ctx, tx, input.WorkspaceID, input.OperationID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return OperationTransitionResult{}, ErrOperationNotFound
	}
	if err != nil {
		return OperationTransitionResult{}, err
	}
	if input.Actor.ID == "" {
		input.Actor = operation.Request.Actor
	}
	commandHash, err := transitionHash(input)
	if err != nil {
		return OperationTransitionResult{}, err
	}
	inserted, err := tx.Exec(ctx, `INSERT INTO fornix.operation_idempotency(workspace_id,command,idempotency_key,request_hash,operation_id) VALUES($1,'transition',$2,$3,$4) ON CONFLICT (workspace_id,command,idempotency_key) DO NOTHING`, input.WorkspaceID, input.IdempotencyKey, commandHash, input.OperationID)
	if err != nil {
		return OperationTransitionResult{}, fmt.Errorf("reserve transition idempotency: %w", err)
	}
	if inserted.RowsAffected() == 0 {
		var existingHash, existingOperation string
		var version *int64
		if err := tx.QueryRow(ctx, `SELECT request_hash,operation_id,transition_version FROM fornix.operation_idempotency WHERE workspace_id=$1 AND command='transition' AND idempotency_key=$2 FOR UPDATE`, input.WorkspaceID, input.IdempotencyKey).Scan(&existingHash, &existingOperation, &version); err != nil {
			return OperationTransitionResult{}, fmt.Errorf("read duplicate transition: %w", err)
		}
		if existingHash != commandHash || existingOperation != input.OperationID || version == nil {
			return OperationTransitionResult{}, ErrOperationIdempotency
		}
		transition, err := readTransition(ctx, tx, input.WorkspaceID, input.OperationID, *version)
		if err != nil {
			return OperationTransitionResult{}, err
		}
		return OperationTransitionResult{Operation: operation, Transition: transition, Duplicate: true}, nil
	}
	if input.Actor.WorkspaceID != operation.WorkspaceID || input.Actor.ID != operation.Request.Actor.ID || input.Actor.Kind != operation.Request.Actor.Kind || input.Actor.Name != operation.Request.Actor.Name {
		return OperationTransitionResult{}, ErrOperationWorkspace
	}
	if err := validateOperationTaskFence(operation, input.TaskOwnerID, input.TaskFence); err != nil {
		return OperationTransitionResult{}, err
	}
	if operation.Request.Task != nil {
		if input.OwnerID != operation.TaskOwnerID {
			return OperationTransitionResult{}, ErrOperationTaskFence
		}
		if err := validateTaskFenceForOperationTx(ctx, tx, operation.Request.Task, input.TaskOwnerID, input.TaskFence); err != nil {
			return OperationTransitionResult{}, err
		}
	}
	if _, err := s.validateLease(ctx, tx, OperationLease{WorkspaceID: input.WorkspaceID, OperationID: input.OperationID, OwnerID: input.OwnerID, Fence: input.Fence}); err != nil {
		return OperationTransitionResult{}, err
	}
	if !contracts.CanTransitionOperation(operation.Status, input.ToStatus) {
		return OperationTransitionResult{}, fmt.Errorf("%w: %s -> %s", ErrOperationTransition, operation.Status, input.ToStatus)
	}
	version := operation.StateVersion + 1
	state := operationState{Status: input.ToStatus, StateVersion: version, PreviousStateHash: operation.StateHash, PlanHash: operation.PlanHash, ResultHash: input.ResultHash, ReportHash: input.ReportHash, Failure: input.Failure, NextRetryAt: canonicalTime(input.NextRetryAt)}
	stateJSON, err := json.Marshal(state)
	if err != nil {
		return OperationTransitionResult{}, fmt.Errorf("marshal operation state: %w", err)
	}
	stateHash, err := hashState(state)
	if err != nil {
		return OperationTransitionResult{}, err
	}
	event, err := s.eventFor(operation.Request, operation.OperationHash, input.ToStatus, version, stateHash, input.IdempotencyKey)
	if err != nil {
		return OperationTransitionResult{}, err
	}
	event.Actor, event.CausationID, event.CorrelationID = input.Actor, input.CausationID, input.CorrelationID
	appended, err := s.events.AppendTx(ctx, tx, event)
	if err != nil {
		return OperationTransitionResult{}, fmt.Errorf("append operation transition event: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.operation_transitions(workspace_id,operation_id,state_version,from_status,to_status,request_id,idempotency_key,actor,task_ref,session_ref,task_owner_id,task_fence,operation_owner_id,operation_fence,causation_id,correlation_id,reason_code,state,state_hash,previous_state_hash,event_sequence) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10::jsonb,$11,$12,$13,$14,$15,$16,$17,$18::jsonb,$19,$20,$21)`, input.WorkspaceID, input.OperationID, version, operation.Status, input.ToStatus, input.RequestID, input.IdempotencyKey, mustJSON(input.Actor), entityValue(operation.Request.Task), entityValue(operation.Request.Session), input.TaskOwnerID, int64(input.TaskFence), input.OwnerID, int64(input.Fence), input.CausationID, input.CorrelationID, input.ReasonCode, stateJSON, stateHash, operation.StateHash, appended.Event.Sequence); err != nil {
		return OperationTransitionResult{}, fmt.Errorf("insert operation transition: %w", err)
	}
	updated, err := tx.Exec(ctx, `UPDATE fornix.operations SET status=$3,state_version=$4,state_hash=$5,result_hash=$6,report_hash=$7,failure=$8::jsonb,next_retry_at=$9,task_owner_id=$10,task_fence=$11,updated_at=clock_timestamp(),started_at=CASE WHEN $3='running' AND started_at IS NULL THEN clock_timestamp() ELSE started_at END,completed_at=CASE WHEN $3 IN ('succeeded','failed','cancelled','dead_letter','abstained') THEN COALESCE(completed_at,clock_timestamp()) ELSE completed_at END WHERE workspace_id=$1 AND id=$2 AND state_version=$12`, input.WorkspaceID, input.OperationID, input.ToStatus, version, stateHash, input.ResultHash, input.ReportHash, operationNullableJSON(input.Failure), input.NextRetryAt, input.TaskOwnerID, int64(input.TaskFence), operation.StateVersion)
	if err != nil {
		return OperationTransitionResult{}, fmt.Errorf("update operation projection: %w", err)
	}
	if updated.RowsAffected() != 1 {
		return OperationTransitionResult{}, ErrOperationTransition
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.operation_idempotency SET transition_version=$4,outcome_hash=$5 WHERE workspace_id=$1 AND command='transition' AND idempotency_key=$2 AND operation_id=$3`, input.WorkspaceID, input.IdempotencyKey, input.OperationID, version, stateHash); err != nil {
		return OperationTransitionResult{}, fmt.Errorf("complete transition idempotency: %w", err)
	}
	if err := s.fail("operation_transition_committed"); err != nil {
		return OperationTransitionResult{}, err
	}
	transition, err := readTransition(ctx, tx, input.WorkspaceID, input.OperationID, version)
	if err != nil {
		return OperationTransitionResult{}, err
	}
	operation, err = readOperationByID(ctx, tx, input.WorkspaceID, input.OperationID)
	if err != nil {
		return OperationTransitionResult{}, err
	}
	return OperationTransitionResult{Operation: operation, Transition: transition, Event: appended.Event}, nil
}

func (s *OperationStore) ReserveAttempt(ctx context.Context, input OperationAttemptInput) (OperationAttempt, bool, error) {
	if s == nil || s.pool == nil {
		return OperationAttempt{}, false, errors.New("operation store is not configured")
	}
	input.WorkspaceID, input.OperationID, input.StepID, input.OwnerID = strings.TrimSpace(input.WorkspaceID), strings.TrimSpace(input.OperationID), strings.TrimSpace(input.StepID), strings.TrimSpace(input.OwnerID)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if input.AttemptID == "" {
		input.AttemptID = contracts.NewID("attempt")
	}
	if !validHash(input.RequestHash) || input.Attempt < 1 || input.Fence == 0 || input.Fence > maxOperationFence || input.IdempotencyKey == "" || len(input.IdempotencyKey) > contracts.MaxIdempotencyLength || len(input.OwnerID) > contracts.MaxDomainIDLength || len(input.AttemptID) > contracts.MaxDomainIDLength {
		return OperationAttempt{}, false, errors.New("attempt requires a request hash, positive attempt, fence, and idempotency key")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, input.WorkspaceID)
	if err != nil {
		return OperationAttempt{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setWorkspaceContext(ctx, tx, input.WorkspaceID); err != nil {
		return OperationAttempt{}, false, err
	}
	operation, err := readOperationByID(ctx, tx, input.WorkspaceID, input.OperationID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return OperationAttempt{}, false, ErrOperationNotFound
	}
	if err != nil {
		return OperationAttempt{}, false, err
	}
	if stored, err := readAttemptByKey(ctx, tx, input.WorkspaceID, input.OperationID, input.StepID, input.IdempotencyKey); err == nil {
		if stored.RequestHash != input.RequestHash || stored.Attempt != input.Attempt {
			return OperationAttempt{}, false, ErrOperationIdempotency
		}
		if err := tx.Commit(ctx); err != nil {
			return OperationAttempt{}, false, err
		}
		return stored, false, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return OperationAttempt{}, false, err
	}
	if contracts.IsTerminalOperationStatus(operation.Status) {
		return OperationAttempt{}, false, ErrOperationTerminal
	}
	if operation.Request.Task != nil {
		if input.OwnerID != operation.TaskOwnerID || operation.TaskFence == 0 {
			return OperationAttempt{}, false, ErrOperationTaskFence
		}
		if err := validateTaskFenceForOperationTx(ctx, tx, operation.Request.Task, operation.TaskOwnerID, operation.TaskFence); err != nil {
			return OperationAttempt{}, false, err
		}
	}
	if _, err := s.validateLease(ctx, tx, OperationLease{WorkspaceID: input.WorkspaceID, OperationID: input.OperationID, OwnerID: input.OwnerID, Fence: input.Fence}); err != nil {
		return OperationAttempt{}, false, err
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM fornix.operation_steps WHERE workspace_id=$1 AND operation_id=$2 AND step_id=$3)`, input.WorkspaceID, input.OperationID, input.StepID).Scan(&exists); err != nil || !exists {
		if err != nil {
			return OperationAttempt{}, false, err
		}
		return OperationAttempt{}, false, errors.New("operation step does not exist")
	}
	inserted, err := tx.Exec(ctx, `INSERT INTO fornix.operation_attempts(workspace_id,operation_id,step_id,attempt,attempt_id,idempotency_key,operation_owner_id,operation_fence,status,request_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'reserved',$9) ON CONFLICT DO NOTHING`, input.WorkspaceID, input.OperationID, input.StepID, input.Attempt, input.AttemptID, input.IdempotencyKey, input.OwnerID, int64(input.Fence), input.RequestHash)
	if err != nil {
		return OperationAttempt{}, false, err
	}
	stored, err := readAttempt(ctx, tx, input.WorkspaceID, input.AttemptID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) || inserted.RowsAffected() != 0 {
			return OperationAttempt{}, false, err
		}
		stored, err = readAttemptByKey(ctx, tx, input.WorkspaceID, input.OperationID, input.StepID, input.IdempotencyKey)
		if err != nil {
			return OperationAttempt{}, false, err
		}
	}
	if inserted.RowsAffected() == 0 {
		if stored.OperationID != input.OperationID || stored.StepID != input.StepID || stored.Attempt != input.Attempt || stored.RequestHash != input.RequestHash {
			return OperationAttempt{}, false, ErrOperationIdempotency
		}
		if err := tx.Commit(ctx); err != nil {
			return OperationAttempt{}, false, err
		}
		return stored, false, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return OperationAttempt{}, false, err
	}
	return stored, true, nil
}

// ReserveEffect records the external boundary before a connector is called.
// It never claims exactly-once execution and never performs the effect.
func (s *OperationStore) ReserveEffect(ctx context.Context, input OperationEffectInput) (OperationEffect, bool, error) {
	if s == nil || s.pool == nil {
		return OperationEffect{}, false, errors.New("operation store is not configured")
	}
	effect := input.Effect
	if effect.ID == "" {
		effect.ID = contracts.NewID("effect")
	}
	if err := effect.Normalize(); err != nil {
		return OperationEffect{}, false, err
	}
	input.WorkspaceID, input.OperationID, input.StepID, input.AttemptID, input.OwnerID = strings.TrimSpace(input.WorkspaceID), strings.TrimSpace(input.OperationID), strings.TrimSpace(input.StepID), strings.TrimSpace(input.AttemptID), strings.TrimSpace(input.OwnerID)
	if effect.WorkspaceID != input.WorkspaceID || input.OperationID == "" || input.StepID == "" || input.AttemptID == "" || !validHash(input.RequestHash) {
		return OperationEffect{}, false, ErrOperationWorkspace
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, input.WorkspaceID)
	if err != nil {
		return OperationEffect{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setWorkspaceContext(ctx, tx, input.WorkspaceID); err != nil {
		return OperationEffect{}, false, err
	}
	operation, err := readOperationByID(ctx, tx, input.WorkspaceID, input.OperationID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return OperationEffect{}, false, ErrOperationNotFound
	}
	if err != nil {
		return OperationEffect{}, false, err
	}
	if stored, err := readEffect(ctx, tx, input.WorkspaceID, input.AttemptID); err == nil {
		if stored.RequestHash != input.RequestHash || stored.OperationID != input.OperationID || stored.StepID != input.StepID || storedEffectIdentityHash(stored) != effect.StableHash() || !storedEffectAuthorityMatches(stored, input) {
			return OperationEffect{}, false, ErrOperationIdempotency
		}
		if err := tx.Commit(ctx); err != nil {
			return OperationEffect{}, false, err
		}
		return stored, false, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return OperationEffect{}, false, err
	}
	if contracts.IsTerminalOperationStatus(operation.Status) {
		return OperationEffect{}, false, ErrOperationTerminal
	}
	if err := s.validateDeploymentAdmissionTx(ctx, tx, operation); err != nil {
		return OperationEffect{}, false, err
	}
	if err := mergeEffectAuthorityFactsTx(ctx, tx, &input); err != nil {
		return OperationEffect{}, false, fmt.Errorf("resolve effect authority facts: %w", err)
	}
	if input.RequireAllowedAdmission {
		if err := requireDispatchableAdmissionTx(ctx, tx, input.WorkspaceID, input.OperationID); err != nil {
			return OperationEffect{}, false, fmt.Errorf("validate dispatchable admission: %w", err)
		}
	}
	if contracts.RequiresExternalBoundary(input.Effect.Class) && s.requireExternalBoundary && input.ExternalBoundary == nil {
		return OperationEffect{}, false, ErrOperationExternalBoundaryRequired
	}
	if contracts.RequiresExternalBoundary(input.Effect.Class) && s.requireExternalBoundary && s.requireDeploymentAdmission {
		reference := operation.Request.DeploymentAdmission
		if reference == nil || reference.ExternalBoundaryHash == "" || reference.BoundaryEvidenceHash == "" {
			return OperationEffect{}, false, ErrOperationExternalBoundaryMismatch
		}
		if input.ExternalBoundary == nil || reference.ExternalBoundaryHash != input.ExternalBoundary.StableHash() {
			return OperationEffect{}, false, ErrOperationExternalBoundaryMismatch
		}
	}
	if err := validateAuthorityFactsWithBoundary(ctx, tx, input.WorkspaceID, input.SchemaCatalogHash, input.SchemaCatalogRevision, input.CredentialLeaseID, input.CredentialLeaseFence, input.CredentialRevocationEpoch, input.CredentialSourceVersion, input.CredentialSourceExpiresAt, input.ExternalBoundary, input.Effect.Class); err != nil {
		return OperationEffect{}, false, fmt.Errorf("validate effect authority facts: %w", err)
	}
	if operation.Request.Task != nil {
		if input.OwnerID != operation.TaskOwnerID || operation.TaskFence == 0 {
			return OperationEffect{}, false, ErrOperationTaskFence
		}
		if err := validateTaskFenceForOperationTx(ctx, tx, operation.Request.Task, operation.TaskOwnerID, operation.TaskFence); err != nil {
			return OperationEffect{}, false, err
		}
	}
	if _, err := s.validateLease(ctx, tx, OperationLease{WorkspaceID: input.WorkspaceID, OperationID: input.OperationID, OwnerID: input.OwnerID, Fence: input.Fence}); err != nil {
		return OperationEffect{}, false, err
	}
	var attemptOperation, attemptStep, attemptOwner string
	var attemptFence int64
	if err := tx.QueryRow(ctx, `SELECT operation_id,step_id,operation_owner_id,operation_fence FROM fornix.operation_attempts WHERE workspace_id=$1 AND attempt_id=$2 FOR UPDATE`, input.WorkspaceID, input.AttemptID).Scan(&attemptOperation, &attemptStep, &attemptOwner, &attemptFence); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return OperationEffect{}, false, errors.New("operation attempt does not exist")
		}
		return OperationEffect{}, false, err
	}
	if attemptOperation != input.OperationID || attemptStep != input.StepID {
		return OperationEffect{}, false, ErrOperationWorkspace
	}
	// The attempt is an immutable snapshot of the operation lease that created
	// it. Requiring the same owner and fence prevents a later operation worker
	// from attaching a new effect to work reserved by a stale worker.
	if attemptOwner != input.OwnerID || attemptFence <= 0 || uint64(attemptFence) != input.Fence {
		return OperationEffect{}, false, ErrOperationLeaseFenced
	}
	egressHash, destinationHash, networkBoundary, networkHash, err := boundaryColumns(input.ExternalBoundary)
	if err != nil {
		return OperationEffect{}, false, err
	}
	inserted, err := tx.Exec(ctx, `INSERT INTO fornix.operation_effects(workspace_id,operation_id,step_id,attempt_id,effect_id,effect_class,boundary,idempotency_key,provider_request_id,provider_idempotency_supported,delivery_semantics,verification_required,verification_status,compensation_status,request_hash,schema_catalog_hash,schema_catalog_revision,credential_lease_id,credential_lease_fence,credential_revocation_epoch,credential_source_version,credential_source_expires_at,egress_policy_hash,destination_policy_hash,network_boundary,network_boundary_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26) ON CONFLICT DO NOTHING`, input.WorkspaceID, input.OperationID, input.StepID, input.AttemptID, effect.ID, effect.Class, effect.Boundary, effect.IdempotencyKey, effect.ProviderRequestID, effect.ProviderIdempotency, effect.DeliveryGuarantee, effect.VerificationRequired, effect.VerificationStatus, effect.CompensationStatus, input.RequestHash, input.SchemaCatalogHash, input.SchemaCatalogRevision, input.CredentialLeaseID, int64(input.CredentialLeaseFence), int64(input.CredentialRevocationEpoch), input.CredentialSourceVersion, input.CredentialSourceExpiresAt, egressHash, destinationHash, networkBoundary, networkHash)
	if err != nil {
		return OperationEffect{}, false, err
	}
	stored, err := readEffect(ctx, tx, input.WorkspaceID, input.AttemptID)
	if err != nil {
		if !errors.Is(err, pgx.ErrNoRows) || inserted.RowsAffected() != 0 {
			return OperationEffect{}, false, err
		}
		stored, err = readEffectByID(ctx, tx, input.WorkspaceID, effect.ID)
		if err != nil {
			return OperationEffect{}, false, err
		}
		return OperationEffect{}, false, ErrOperationIdempotency
	}
	if inserted.RowsAffected() == 0 && stored.RequestHash != input.RequestHash {
		return OperationEffect{}, false, ErrOperationIdempotency
	}
	if inserted.RowsAffected() == 1 {
		if _, err := ensureEffectState(ctx, tx, input.WorkspaceID, stored.EffectID, input.OperationID, input.OwnerID, input.Fence); err != nil {
			return OperationEffect{}, false, fmt.Errorf("initialize external effect state: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return OperationEffect{}, false, err
	}
	return stored, inserted.RowsAffected() == 1, nil
}

func (s *OperationStore) validateDeploymentAdmissionTx(ctx context.Context, tx pgx.Tx, operation Operation) error {
	reference := operation.Request.DeploymentAdmission
	if reference == nil {
		if s.requireDeploymentAdmission {
			return ErrOperationAdmissionRequired
		}
		return nil
	}
	if s.deploymentEvidence == nil {
		return ErrOperationAdmissionUnavailable
	}
	if err := s.deploymentEvidence.ValidateAdmissionReferenceTx(ctx, tx, *reference, time.Now().UTC()); err != nil {
		return fmt.Errorf("validate deployment admission: %w", err)
	}
	return nil
}

func (s *OperationStore) RecordCallback(ctx context.Context, input OperationCallbackInput) (OperationCallback, bool, error) {
	if s == nil || s.pool == nil {
		return OperationCallback{}, false, errors.New("operation store is not configured")
	}
	input.WorkspaceID, input.OperationID, input.OwnerID, input.CallbackID, input.CallbackKind = strings.TrimSpace(input.WorkspaceID), strings.TrimSpace(input.OperationID), strings.TrimSpace(input.OwnerID), strings.TrimSpace(input.CallbackID), strings.TrimSpace(input.CallbackKind)
	if input.WorkspaceID == "" || input.OperationID == "" || input.OwnerID == "" || input.Fence == 0 || input.Fence > maxOperationFence || input.CallbackID == "" || input.CallbackKind == "" || !validHash(input.RequestHash) || (input.ResponseHash != "" && !validHash(input.ResponseHash)) || strings.TrimSpace(input.Status) == "" || len(input.CallbackID) > contracts.MaxDomainIDLength || len(input.CallbackKind) > contracts.MaxDomainNameLength || len(input.Status) > 128 {
		return OperationCallback{}, false, errors.New("callback identity, owner, fence, status, and hashes are required")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, input.WorkspaceID)
	if err != nil {
		return OperationCallback{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setWorkspaceContext(ctx, tx, input.WorkspaceID); err != nil {
		return OperationCallback{}, false, err
	}
	if stored, err := readCallback(ctx, tx, input.WorkspaceID, input.CallbackID); err == nil {
		if stored.RequestHash != input.RequestHash || stored.OperationID != input.OperationID {
			return OperationCallback{}, false, ErrOperationIdempotency
		}
		if err := tx.Commit(ctx); err != nil {
			return OperationCallback{}, false, err
		}
		return stored, false, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return OperationCallback{}, false, err
	}
	operation, err := readOperationByID(ctx, tx, input.WorkspaceID, input.OperationID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return OperationCallback{}, false, ErrOperationNotFound
	}
	if err != nil {
		return OperationCallback{}, false, err
	}
	if contracts.IsTerminalOperationStatus(operation.Status) {
		return OperationCallback{}, false, ErrOperationTerminal
	}
	if operation.Request.Task != nil {
		if input.OwnerID != operation.TaskOwnerID || operation.TaskFence == 0 {
			return OperationCallback{}, false, ErrOperationTaskFence
		}
		if err := validateTaskFenceForOperationTx(ctx, tx, operation.Request.Task, operation.TaskOwnerID, operation.TaskFence); err != nil {
			return OperationCallback{}, false, err
		}
	}
	if _, err := s.validateLease(ctx, tx, OperationLease{WorkspaceID: input.WorkspaceID, OperationID: input.OperationID, OwnerID: input.OwnerID, Fence: input.Fence}); err != nil {
		return OperationCallback{}, false, err
	}
	inserted, err := tx.Exec(ctx, `INSERT INTO fornix.operation_callbacks(workspace_id,operation_id,callback_id,callback_kind,external_id,request_hash,response_hash,status) VALUES($1,$2,$3,$4,$5,$6,$7,$8) ON CONFLICT (workspace_id,callback_id) DO NOTHING`, input.WorkspaceID, input.OperationID, input.CallbackID, input.CallbackKind, strings.TrimSpace(input.ExternalID), input.RequestHash, input.ResponseHash, strings.TrimSpace(input.Status))
	if err != nil {
		return OperationCallback{}, false, err
	}
	stored, err := readCallback(ctx, tx, input.WorkspaceID, input.CallbackID)
	if err != nil {
		return OperationCallback{}, false, err
	}
	if inserted.RowsAffected() == 0 && (stored.RequestHash != input.RequestHash || stored.OperationID != input.OperationID) {
		return OperationCallback{}, false, ErrOperationIdempotency
	}
	if err := tx.Commit(ctx); err != nil {
		return OperationCallback{}, false, err
	}
	return stored, inserted.RowsAffected() == 1, nil
}

// Replay is read-only and validates a bounded page of committed transitions
// against one repeatable-read snapshot. It cannot invoke a connector, model,
// tool, or callback. A page can be continued with NextFromVersion; when the
// page reaches the current projection, Complete is true.
func (s *OperationStore) Replay(ctx context.Context, workspaceID, operationID string, fromVersion int64, limit int) (OperationReplayResult, error) {
	workspaceID, operationID = strings.TrimSpace(workspaceID), strings.TrimSpace(operationID)
	if s == nil || s.pool == nil {
		return OperationReplayResult{}, errors.New("operation store is not configured")
	}
	if fromVersion < 0 {
		return OperationReplayResult{}, ErrOperationReplay
	}
	if limit <= 0 || limit > maxOperationReplayLimit {
		limit = maxOperationReplayLimit
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return OperationReplayResult{}, fmt.Errorf("begin operation replay: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := setWorkspaceContext(ctx, tx, workspaceID); err != nil {
		return OperationReplayResult{}, err
	}
	operation, err := readOperationByID(ctx, tx, workspaceID, operationID, false)
	if errors.Is(err, pgx.ErrNoRows) {
		return OperationReplayResult{}, ErrOperationNotFound
	}
	if err != nil {
		return OperationReplayResult{}, err
	}
	if fromVersion > operation.StateVersion {
		return OperationReplayResult{}, ErrOperationReplay
	}

	previousVersion := int64(0)
	previousStatus := contracts.OperationStatusCreated
	previousHash, err := hashState(operationState{Status: previousStatus, StateVersion: 0, PlanHash: operation.PlanHash})
	if err != nil {
		return OperationReplayResult{}, fmt.Errorf("hash initial operation state: %w", err)
	}
	if fromVersion > 0 {
		var checkpointHash string
		var checkpointStateJSON []byte
		if err := tx.QueryRow(ctx, `
			SELECT state_hash,state
			FROM fornix.operation_transitions
			WHERE workspace_id=$1 AND operation_id=$2 AND state_version=$3`, workspaceID, operationID, fromVersion).
			Scan(&checkpointHash, &checkpointStateJSON); err != nil {
			return OperationReplayResult{}, fmt.Errorf("read replay checkpoint: %w", ErrOperationReplay)
		}
		var checkpoint operationState
		if err := json.Unmarshal(checkpointStateJSON, &checkpoint); err != nil {
			return OperationReplayResult{}, ErrOperationReplay
		}
		computed, hashErr := hashState(checkpoint)
		if hashErr != nil || computed != checkpointHash || checkpoint.StateVersion != fromVersion || !contracts.IsKnownOperationStatus(checkpoint.Status) {
			return OperationReplayResult{}, ErrOperationReplay
		}
		previousVersion, previousStatus, previousHash = fromVersion, checkpoint.Status, checkpointHash
	}

	rows, err := tx.Query(ctx, `
		SELECT t.state_version, t.state_hash, t.previous_state_hash,
		       t.from_status, t.to_status, t.state, t.event_sequence,
		       e.sequence, e.event_type, e.workspace_id, e.payload
		FROM fornix.operation_transitions t
		LEFT JOIN fornix.control_events e
		  ON e.workspace_id=t.workspace_id AND e.sequence=t.event_sequence
		WHERE t.workspace_id=$1 AND t.operation_id=$2 AND t.state_version>$3
		ORDER BY t.state_version ASC LIMIT $4`, workspaceID, operationID, fromVersion, limit+1)
	if err != nil {
		return OperationReplayResult{}, err
	}
	defer rows.Close()
	transitions := make([]string, 0, limit)
	count := 0
	hasMore := false
	for rows.Next() {
		var version int64
		var storedHash, storedPreviousHash string
		var fromStatus, toStatus string
		var stateJSON []byte
		var eventSequence, matchedEventSequence *int64
		var eventType, eventWorkspace string
		var eventPayload []byte
		if err := rows.Scan(&version, &storedHash, &storedPreviousHash, &fromStatus, &toStatus, &stateJSON, &eventSequence, &matchedEventSequence, &eventType, &eventWorkspace, &eventPayload); err != nil {
			return OperationReplayResult{}, err
		}
		if count == limit {
			hasMore = true
			break
		}
		if version != previousVersion+1 || storedPreviousHash != previousHash || fromStatus != previousStatus || eventSequence == nil || matchedEventSequence == nil || *eventSequence != *matchedEventSequence || eventWorkspace != workspaceID {
			return OperationReplayResult{}, fmt.Errorf("%w: broken transition chain at version %d", ErrOperationReplay, version)
		}
		var state operationState
		if err := json.Unmarshal(stateJSON, &state); err != nil {
			return OperationReplayResult{}, ErrOperationReplay
		}
		computed, err := hashState(state)
		if err != nil || computed != storedHash || state.StateVersion != version || state.Status != toStatus || state.PreviousStateHash != storedPreviousHash || !contracts.CanTransitionOperation(fromStatus, toStatus) {
			return OperationReplayResult{}, ErrOperationReplay
		}
		var payload struct {
			OperationID   string `json:"operation_id"`
			OperationHash string `json:"operation_hash"`
			Status        string `json:"status"`
			StateVersion  int64  `json:"state_version"`
			StateHash     string `json:"state_hash"`
		}
		if eventType != "operation."+toStatus || json.Unmarshal(eventPayload, &payload) != nil || payload.OperationID != operation.ID || payload.OperationHash != operation.OperationHash || payload.Status != toStatus || payload.StateVersion != version || payload.StateHash != storedHash {
			return OperationReplayResult{}, fmt.Errorf("%w: event binding at version %d", ErrOperationReplay, version)
		}
		previousVersion, previousStatus, previousHash = version, toStatus, storedHash
		count++
		transitions = append(transitions, fmt.Sprintf("%d:%s", version, storedHash))
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return OperationReplayResult{}, err
	}
	if previousVersion != operation.StateVersion && !hasMore {
		return OperationReplayResult{}, ErrOperationReplay
	}
	complete := !hasMore && previousVersion == operation.StateVersion && previousHash == operation.StateHash
	if !hasMore && !complete {
		return OperationReplayResult{}, ErrOperationReplay
	}
	replayHash, err := hashValue(struct {
		WorkspaceID string   `json:"workspace_id"`
		OperationID string   `json:"operation_id"`
		FromVersion int64    `json:"from_version"`
		ToVersion   int64    `json:"to_version"`
		States      []string `json:"states"`
	}{workspaceID, operationID, fromVersion, previousVersion, transitions})
	if err != nil {
		return OperationReplayResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return OperationReplayResult{}, fmt.Errorf("commit operation replay snapshot: %w", err)
	}
	return OperationReplayResult{
		Operation: operation, StateVersion: previousVersion, StateHash: previousHash,
		CurrentStateVersion: operation.StateVersion, CurrentStateHash: operation.StateHash,
		NextFromVersion: previousVersion, HasMore: hasMore, Complete: complete,
		ReplayHash: replayHash, TransitionCount: count, Verified: true,
	}, nil
}

type operationState struct {
	Status            string                      `json:"status"`
	StateVersion      int64                       `json:"state_version"`
	PreviousStateHash string                      `json:"previous_state_hash,omitempty"`
	PlanHash          string                      `json:"plan_hash,omitempty"`
	ResultHash        string                      `json:"result_hash,omitempty"`
	ReportHash        string                      `json:"report_hash,omitempty"`
	Failure           *contracts.OperationFailure `json:"failure,omitempty"`
	NextRetryAt       string                      `json:"next_retry_at,omitempty"`
}

func normalizePlan(request contracts.OperationRequest, input *contracts.OperationPlan) ([]byte, string, *contracts.OperationPlan, error) {
	if input == nil {
		return nil, "", nil, nil
	}
	plan := *input
	plan.WorkspaceID, plan.OperationID, plan.OperationHash = request.WorkspaceID, request.ID, request.StableHash()
	if err := plan.Normalize(); err != nil {
		return nil, "", nil, fmt.Errorf("normalize operation plan: %w", err)
	}
	hash := plan.StableHash()
	if !validHash(hash) {
		return nil, "", nil, errors.New("operation plan has no stable hash")
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		return nil, "", nil, err
	}
	return raw, hash, &plan, nil
}

func (s *OperationStore) insertChildren(ctx context.Context, tx pgx.Tx, request contracts.OperationRequest, plan *contracts.OperationPlan, resources []OperationResource, links []OperationLink) error {
	if len(resources) > contracts.MaxDomainReferences || len(links) > contracts.MaxDomainReferences {
		return errors.New("operation resources or links exceed bounds")
	}
	for ordinal, value := range resources {
		if value.WorkspaceID == "" {
			value.WorkspaceID = request.WorkspaceID
		}
		if value.WorkspaceID != request.WorkspaceID || strings.TrimSpace(value.ResourceKind) == "" || strings.TrimSpace(value.ResourceID) == "" || value.Ordinal != 0 && value.Ordinal != ordinal || value.Ordinal < 0 || value.Ordinal > 127 || (value.ResourceHash != "" && !validHash(value.ResourceHash)) {
			return ErrOperationWorkspace
		}
		if _, err := tx.Exec(ctx, `INSERT INTO fornix.operation_resources(workspace_id,operation_id,ordinal,resource_kind,resource_id,resource_hash,role) VALUES($1,$2,$3,$4,$5,$6,$7)`, request.WorkspaceID, request.ID, ordinal, strings.TrimSpace(value.ResourceKind), strings.TrimSpace(value.ResourceID), strings.ToLower(value.ResourceHash), strings.TrimSpace(value.Role)); err != nil {
			return fmt.Errorf("insert operation resource: %w", err)
		}
	}
	if plan != nil {
		for _, step := range plan.Steps {
			capability, _ := json.Marshal(step.Capability)
			target, _ := json.Marshal(step.Target)
			dependsOn, _ := json.Marshal(step.DependsOn)
			profile, _ := json.Marshal(step.Profile)
			if _, err := tx.Exec(ctx, `INSERT INTO fornix.operation_steps(workspace_id,operation_id,step_id,ordinal,kind,capability,target,depends_on,effect_class,profile,input_hash,state_hash) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8::jsonb,$9,$10::jsonb,$11,$12)`, request.WorkspaceID, request.ID, step.ID, step.Ordinal, step.Kind, capability, target, dependsOn, step.Effect, profile, step.InputHash, plan.OperationHash); err != nil {
				return fmt.Errorf("insert operation step %s: %w", step.ID, err)
			}
		}
	}
	for _, value := range links {
		if value.WorkspaceID == "" {
			value.WorkspaceID = request.WorkspaceID
		}
		if value.WorkspaceID != request.WorkspaceID || strings.TrimSpace(value.LinkKind) == "" || strings.TrimSpace(value.SourceID) == "" || (value.SourceHash != "" && !validHash(value.SourceHash)) {
			return ErrOperationWorkspace
		}
		if _, err := tx.Exec(ctx, `INSERT INTO fornix.operation_links(workspace_id,operation_id,link_kind,source_id,source_hash,role) VALUES($1,$2,$3,$4,$5,$6)`, request.WorkspaceID, request.ID, strings.TrimSpace(value.LinkKind), strings.TrimSpace(value.SourceID), strings.ToLower(value.SourceHash), strings.TrimSpace(value.Role)); err != nil {
			return fmt.Errorf("insert operation link: %w", err)
		}
	}
	return nil
}

func (s *OperationStore) eventFor(request contracts.OperationRequest, operationHash, status string, version int64, stateHash, command string) (contracts.EventEnvelope, error) {
	if operationHash == "" {
		operationHash = request.StableHash()
	}
	event, err := contracts.NewEvent("operation."+status, map[string]any{"operation_id": request.ID, "operation_hash": operationHash, "status": status, "state_version": version, "state_hash": stateHash})
	if err != nil {
		return contracts.EventEnvelope{}, err
	}
	event.Scope = contracts.Scope{WorkspaceID: request.WorkspaceID, Subject: request.ID}
	event.Actor, event.Task, event.Session = request.Actor, request.Task, request.Session
	event.CausationID, event.CorrelationID = request.CausationID, request.CorrelationID
	event.IdempotencyKey = "operation-event-" + hashString(request.WorkspaceID + "\x00" + request.ID + "\x00" + fmt.Sprint(version) + "\x00" + command)[:48]
	event.StateDeltas = []contracts.StateDelta{{Op: contracts.DeltaSet, Path: "/operations/" + escapeJSONPointerToken(request.ID) + "/status", Value: mustJSON(status)}}
	return event, nil
}

func escapeJSONPointerToken(value string) string {
	value = strings.ReplaceAll(value, "~", "~0")
	return strings.ReplaceAll(value, "/", "~1")
}

func normalizeTransition(input OperationTransitionInput) (OperationTransitionInput, error) {
	input.WorkspaceID, input.OperationID, input.OwnerID, input.IdempotencyKey = strings.TrimSpace(input.WorkspaceID), strings.TrimSpace(input.OperationID), strings.TrimSpace(input.OwnerID), strings.TrimSpace(input.IdempotencyKey)
	input.ToStatus, input.ReasonCode = strings.ToLower(strings.TrimSpace(input.ToStatus)), strings.TrimSpace(input.ReasonCode)
	if input.WorkspaceID == "" || input.OperationID == "" || input.OwnerID == "" || input.IdempotencyKey == "" || input.ToStatus == "" || input.Fence == 0 || input.Fence > maxOperationFence {
		return OperationTransitionInput{}, errors.New("operation transition identity, status, fence, and idempotency_key are required")
	}
	if (input.ResultHash != "" && !validHash(input.ResultHash)) || (input.ReportHash != "" && !validHash(input.ReportHash)) {
		return OperationTransitionInput{}, errors.New("operation transition result/report hashes are invalid")
	}
	if input.Failure != nil {
		if err := input.Failure.Normalize(); err != nil {
			return OperationTransitionInput{}, fmt.Errorf("operation transition failure: %w", err)
		}
		if input.Failure.WorkspaceID != input.WorkspaceID {
			return OperationTransitionInput{}, ErrOperationWorkspace
		}
	}
	if len(input.ReasonCode) > 128 || len(input.IdempotencyKey) > contracts.MaxIdempotencyLength {
		return OperationTransitionInput{}, errors.New("operation transition field is too large")
	}
	if input.RequestID == "" {
		input.RequestID = contracts.NewID("operation-transition")
	}
	if len(input.RequestID) > contracts.MaxDomainIDLength {
		return OperationTransitionInput{}, errors.New("operation transition request_id is too large")
	}
	input.Actor.ID, input.Actor.Kind, input.Actor.WorkspaceID = strings.TrimSpace(input.Actor.ID), strings.ToLower(strings.TrimSpace(input.Actor.Kind)), strings.TrimSpace(input.Actor.WorkspaceID)
	return input, nil
}

func transitionHash(input OperationTransitionInput) (string, error) {
	return hashValue(struct {
		WorkspaceID string                      `json:"workspace_id"`
		OperationID string                      `json:"operation_id"`
		Actor       contracts.ActorRef          `json:"actor"`
		ToStatus    string                      `json:"to_status"`
		ReasonCode  string                      `json:"reason_code,omitempty"`
		ResultHash  string                      `json:"result_hash,omitempty"`
		ReportHash  string                      `json:"report_hash,omitempty"`
		Failure     *contracts.OperationFailure `json:"failure,omitempty"`
		NextRetryAt string                      `json:"next_retry_at,omitempty"`
	}{input.WorkspaceID, input.OperationID, input.Actor, input.ToStatus, input.ReasonCode, input.ResultHash, input.ReportHash, input.Failure, canonicalTime(input.NextRetryAt)})
}

func validateOperationTaskFence(operation Operation, owner string, fence uint64) error {
	if operation.Request.Task == nil {
		if strings.TrimSpace(owner) != "" || fence != 0 {
			return ErrOperationTaskFence
		}
		return nil
	}
	if operation.TaskOwnerID == "" || operation.TaskFence == 0 || strings.TrimSpace(owner) != operation.TaskOwnerID || fence != operation.TaskFence {
		return ErrOperationTaskFence
	}
	return nil
}

// validateTaskFenceForOperationTx re-reads the authoritative task lease while
// the operation transaction is open. The operation stores a fence snapshot,
// but that snapshot is not authority: a task takeover must immediately fence
// out every operation still carrying the old task assignment.
func validateTaskFenceForOperationTx(ctx context.Context, tx pgx.Tx, task *contracts.EntityRef, owner string, fence uint64) error {
	if task == nil {
		if strings.TrimSpace(owner) != "" || fence != 0 {
			return ErrOperationTaskFence
		}
		return nil
	}
	if strings.ToLower(strings.TrimSpace(task.Kind)) != "task" || strings.TrimSpace(owner) == "" || fence == 0 || fence > maxOperationFence {
		return ErrOperationTaskFence
	}
	taskID, err := strconv.ParseInt(strings.TrimSpace(task.ID), 10, 64)
	if err != nil || taskID <= 0 {
		return ErrOperationTaskFence
	}
	// Match TaskStore's mutation lock order: task row first, lease row second.
	// A joined `FOR UPDATE OF l, t` can acquire row locks in a planner-selected
	// order and deadlock with Renew/Complete when each holds the other row.
	var assigned *string
	err = tx.QueryRow(ctx, `
		SELECT assigned_session
		FROM fornix.tasks
		WHERE workspace_id=$1 AND id=$2
		FOR UPDATE`, task.WorkspaceID, taskID).Scan(&assigned)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrOperationTaskFence
	}
	if err != nil {
		return fmt.Errorf("lock operation task for fence validation: %w", err)
	}
	var currentOwner string
	var currentFence int64
	err = tx.QueryRow(ctx, `
		SELECT owner_id, fence
		FROM fornix.task_execution_leases
		WHERE workspace_id=$1 AND task_id=$2
		  AND released_at IS NULL AND lease_until > clock_timestamp()
		FOR UPDATE`, task.WorkspaceID, taskID).Scan(&currentOwner, &currentFence)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrOperationTaskFence
	}
	if err != nil {
		return fmt.Errorf("lock operation task lease for fence validation: %w", err)
	}
	if currentFence <= 0 || uint64(currentFence) != fence || currentOwner != strings.TrimSpace(owner) || assigned == nil || *assigned != strings.TrimSpace(owner) {
		return ErrOperationTaskFence
	}
	return nil
}

func (s *OperationStore) validateLease(ctx context.Context, tx pgx.Tx, lease OperationLease) (OperationLease, error) {
	if tx == nil {
		return OperationLease{}, ErrOperationLeaseMissing
	}
	if lease.Fence == 0 || lease.Fence > maxOperationFence || strings.TrimSpace(lease.OwnerID) == "" {
		return OperationLease{}, ErrOperationLeaseFenced
	}
	current, active, err := readLease(ctx, tx, lease.WorkspaceID, lease.OperationID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return OperationLease{}, ErrOperationLeaseMissing
	}
	if err != nil {
		return OperationLease{}, err
	}
	if current.Fence != lease.Fence {
		return current, ErrOperationLeaseFenced
	}
	if current.OwnerID != lease.OwnerID {
		return current, ErrOperationLeaseOwned
	}
	if current.ReleasedAt != nil {
		return current, ErrOperationLeaseReleased
	}
	if !active {
		return current, ErrOperationLeaseExpired
	}
	return current, nil
}

func readLease(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, operationID string, lock bool) (OperationLease, bool, error) {
	query := `SELECT workspace_id,operation_id,owner_id,fence,lease_until,acquired_at,renewed_at,released_at,(released_at IS NULL AND lease_until > clock_timestamp()) FROM fornix.operation_leases WHERE workspace_id=$1 AND operation_id=$2`
	if lock {
		query += " FOR UPDATE"
	}
	var lease OperationLease
	var active bool
	if err := queryer.QueryRow(ctx, query, workspaceID, operationID).Scan(&lease.WorkspaceID, &lease.OperationID, &lease.OwnerID, &lease.Fence, &lease.LeaseUntil, &lease.AcquiredAt, &lease.RenewedAt, &lease.ReleasedAt, &active); err != nil {
		return OperationLease{}, false, err
	}
	return lease, active, nil
}

const operationColumns = `workspace_id,id,schema_version,request_id,idempotency_key,request_hash,operation_hash,status,request,plan,plan_hash,result_hash,report_hash,failure,next_retry_at,state_version,state_hash,task_owner_id,task_fence,created_at,updated_at,started_at,completed_at`

func readOperationByKey(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, key string) (Operation, error) {
	return readOperationQuery(ctx, queryer, `SELECT `+operationColumns+` FROM fornix.operations WHERE workspace_id=$1 AND idempotency_key=$2 FOR UPDATE`, workspaceID, key)
}

func readOperationByID(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, operationID string, lock ...bool) (Operation, error) {
	query := `SELECT ` + operationColumns + ` FROM fornix.operations WHERE workspace_id=$1 AND id=$2`
	if len(lock) > 0 && lock[0] {
		query += " FOR UPDATE"
	}
	return readOperationQuery(ctx, queryer, query, workspaceID, operationID)
}

func readOperationQuery(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, query string, args ...any) (Operation, error) {
	var value Operation
	var requestJSON, planJSON, failureJSON []byte
	if err := queryer.QueryRow(ctx, query, args...).Scan(&value.WorkspaceID, &value.ID, &value.SchemaVersion, &value.RequestID, &value.IdempotencyKey, &value.RequestHash, &value.OperationHash, &value.Status, &requestJSON, &planJSON, &value.PlanHash, &value.ResultHash, &value.ReportHash, &failureJSON, &value.NextRetryAt, &value.StateVersion, &value.StateHash, &value.TaskOwnerID, &value.TaskFence, &value.CreatedAt, &value.UpdatedAt, &value.StartedAt, &value.CompletedAt); err != nil {
		return Operation{}, err
	}
	if err := json.Unmarshal(requestJSON, &value.Request); err != nil {
		return Operation{}, fmt.Errorf("decode operation request: %w", err)
	}
	if len(planJSON) > 0 && string(planJSON) != "null" {
		var plan contracts.OperationPlan
		if err := json.Unmarshal(planJSON, &plan); err != nil {
			return Operation{}, fmt.Errorf("decode operation plan: %w", err)
		}
		value.Plan = &plan
	}
	if len(failureJSON) > 0 && string(failureJSON) != "null" {
		var failure contracts.OperationFailure
		if err := json.Unmarshal(failureJSON, &failure); err != nil {
			return Operation{}, fmt.Errorf("decode operation failure: %w", err)
		}
		value.Failure = &failure
	}
	return value, nil
}

func storedEffectAuthorityMatches(stored OperationEffect, input OperationEffectInput) bool {
	return stored.SchemaCatalogHash == input.SchemaCatalogHash && stored.SchemaCatalogRevision == input.SchemaCatalogRevision && stored.CredentialLeaseID == input.CredentialLeaseID && stored.CredentialLeaseFence == input.CredentialLeaseFence && stored.CredentialRevocationEpoch == input.CredentialRevocationEpoch && stored.CredentialSourceVersion == input.CredentialSourceVersion && sameOptionalTime(stored.CredentialSourceExpiresAt, input.CredentialSourceExpiresAt) && sameExternalBoundary(stored.ExternalBoundary, input.ExternalBoundary)
}

func readOperationResult(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, operationID string) (OperationResultRecord, error) {
	var record OperationResultRecord
	var resultJSON []byte
	if err := queryer.QueryRow(ctx, `SELECT workspace_id,operation_id,result_id,result_hash,result,created_at FROM fornix.operation_results WHERE workspace_id=$1 AND operation_id=$2`, workspaceID, operationID).Scan(&record.WorkspaceID, &record.OperationID, &record.ResultID, &record.ResultHash, &resultJSON, &record.CreatedAt); err != nil {
		return OperationResultRecord{}, err
	}
	if err := json.Unmarshal(resultJSON, &record.Result); err != nil {
		return OperationResultRecord{}, fmt.Errorf("decode operation result: %w", err)
	}
	if record.Result.StableHash() != record.ResultHash {
		return OperationResultRecord{}, ErrOperationResultConflict
	}
	return record, nil
}

func readTransition(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, operationID string, version int64) (OperationTransition, error) {
	var value OperationTransition
	var actorJSON, stateJSON []byte
	if err := queryer.QueryRow(ctx, `SELECT workspace_id,operation_id,state_version,from_status,to_status,request_id,idempotency_key,actor,task_owner_id,task_fence,operation_owner_id,operation_fence,causation_id,correlation_id,reason_code,state,state_hash,previous_state_hash,event_sequence,occurred_at FROM fornix.operation_transitions WHERE workspace_id=$1 AND operation_id=$2 AND state_version=$3`, workspaceID, operationID, version).Scan(&value.WorkspaceID, &value.OperationID, &value.StateVersion, &value.FromStatus, &value.ToStatus, &value.RequestID, &value.IdempotencyKey, &actorJSON, &value.TaskOwnerID, &value.TaskFence, &value.OperationOwnerID, &value.OperationFence, &value.CausationID, &value.CorrelationID, &value.ReasonCode, &stateJSON, &value.StateHash, &value.PreviousStateHash, &value.EventSequence, &value.OccurredAt); err != nil {
		return OperationTransition{}, err
	}
	if err := json.Unmarshal(actorJSON, &value.Actor); err != nil {
		return OperationTransition{}, err
	}
	value.State = append(json.RawMessage(nil), stateJSON...)
	return value, nil
}

func readAttempt(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, attemptID string) (OperationAttempt, error) {
	var value OperationAttempt
	var failure []byte
	if err := queryer.QueryRow(ctx, `SELECT workspace_id,operation_id,step_id,attempt,attempt_id,operation_owner_id,operation_fence,status,request_hash,response_hash,failure,external_effect_status,started_at,completed_at FROM fornix.operation_attempts WHERE workspace_id=$1 AND attempt_id=$2`, workspaceID, attemptID).Scan(&value.WorkspaceID, &value.OperationID, &value.StepID, &value.Attempt, &value.AttemptID, &value.OperationOwnerID, &value.OperationFence, &value.Status, &value.RequestHash, &value.ResponseHash, &failure, &value.ExternalEffectStatus, &value.StartedAt, &value.CompletedAt); err != nil {
		return OperationAttempt{}, err
	}
	value.Failure = append(json.RawMessage(nil), failure...)
	return value, nil
}

func readAttemptByKey(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, operationID, stepID, idempotencyKey string) (OperationAttempt, error) {
	var value OperationAttempt
	var failure []byte
	if err := queryer.QueryRow(ctx, `SELECT workspace_id,operation_id,step_id,attempt,attempt_id,operation_owner_id,operation_fence,status,request_hash,response_hash,failure,external_effect_status,started_at,completed_at FROM fornix.operation_attempts WHERE workspace_id=$1 AND operation_id=$2 AND step_id=$3 AND idempotency_key=$4`, workspaceID, operationID, stepID, idempotencyKey).Scan(&value.WorkspaceID, &value.OperationID, &value.StepID, &value.Attempt, &value.AttemptID, &value.OperationOwnerID, &value.OperationFence, &value.Status, &value.RequestHash, &value.ResponseHash, &failure, &value.ExternalEffectStatus, &value.StartedAt, &value.CompletedAt); err != nil {
		return OperationAttempt{}, err
	}
	value.Failure = append(json.RawMessage(nil), failure...)
	return value, nil
}

func readEffect(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, attemptID string) (OperationEffect, error) {
	return readEffectQuery(ctx, queryer, `SELECT workspace_id,operation_id,step_id,attempt_id,effect_id,effect_class,boundary,idempotency_key,provider_request_id,provider_idempotency_supported,delivery_semantics,verification_required,verification_status,compensation_status,request_hash,response_hash,schema_catalog_hash,schema_catalog_revision,credential_lease_id,credential_lease_fence,credential_revocation_epoch,credential_source_version,credential_source_expires_at,egress_policy_hash,destination_policy_hash,network_boundary,network_boundary_hash,created_at,verified_at FROM fornix.operation_effects WHERE workspace_id=$1 AND attempt_id=$2`, workspaceID, attemptID)
}

func readEffectByID(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, effectID string) (OperationEffect, error) {
	return readEffectQuery(ctx, queryer, `SELECT workspace_id,operation_id,step_id,attempt_id,effect_id,effect_class,boundary,idempotency_key,provider_request_id,provider_idempotency_supported,delivery_semantics,verification_required,verification_status,compensation_status,request_hash,response_hash,schema_catalog_hash,schema_catalog_revision,credential_lease_id,credential_lease_fence,credential_revocation_epoch,credential_source_version,credential_source_expires_at,egress_policy_hash,destination_policy_hash,network_boundary,network_boundary_hash,created_at,verified_at FROM fornix.operation_effects WHERE workspace_id=$1 AND effect_id=$2`, workspaceID, effectID)
}

func readEffectQuery(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, query string, args ...any) (OperationEffect, error) {
	var value OperationEffect
	var credentialFence, credentialEpoch int64
	var egressHash, destinationHash, networkBoundary, networkHash string
	if err := queryer.QueryRow(ctx, query, args...).Scan(&value.WorkspaceID, &value.OperationID, &value.StepID, &value.AttemptID, &value.EffectID, &value.EffectClass, &value.Boundary, &value.IdempotencyKey, &value.ProviderRequestID, &value.ProviderIdempotency, &value.DeliverySemantics, &value.VerificationRequired, &value.VerificationStatus, &value.CompensationStatus, &value.RequestHash, &value.ResponseHash, &value.SchemaCatalogHash, &value.SchemaCatalogRevision, &value.CredentialLeaseID, &credentialFence, &credentialEpoch, &value.CredentialSourceVersion, &value.CredentialSourceExpiresAt, &egressHash, &destinationHash, &networkBoundary, &networkHash, &value.CreatedAt, &value.VerifiedAt); err != nil {
		return OperationEffect{}, err
	}
	if credentialFence < 0 || credentialEpoch < 0 {
		return OperationEffect{}, ErrOperationIdempotency
	}
	value.CredentialLeaseFence, value.CredentialRevocationEpoch = uint64(credentialFence), uint64(credentialEpoch)
	var err error
	value.ExternalBoundary, err = boundaryFromColumns(egressHash, destinationHash, networkBoundary, networkHash)
	if err != nil {
		return OperationEffect{}, err
	}
	return value, nil
}

func storedEffectIdentityHash(effect OperationEffect) string {
	value := contracts.ExternalEffect{
		SchemaVersion:        contracts.DomainNeutralSchemaVersion,
		ID:                   effect.EffectID,
		WorkspaceID:          effect.WorkspaceID,
		Boundary:             effect.Boundary,
		Class:                contracts.EffectClass(effect.EffectClass),
		DeliveryGuarantee:    effect.DeliverySemantics,
		IdempotencyKey:       effect.IdempotencyKey,
		ProviderRequestID:    effect.ProviderRequestID,
		ProviderIdempotency:  effect.ProviderIdempotency,
		VerificationRequired: effect.VerificationRequired,
		VerificationStatus:   effect.VerificationStatus,
		CompensationStatus:   effect.CompensationStatus,
	}
	return value.StableHash()
}

func readCallback(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, callbackID string) (OperationCallback, error) {
	var value OperationCallback
	if err := queryer.QueryRow(ctx, `SELECT workspace_id,operation_id,callback_id,callback_kind,external_id,request_hash,response_hash,status,received_at FROM fornix.operation_callbacks WHERE workspace_id=$1 AND callback_id=$2`, workspaceID, callbackID).Scan(&value.WorkspaceID, &value.OperationID, &value.CallbackID, &value.CallbackKind, &value.ExternalID, &value.RequestHash, &value.ResponseHash, &value.Status, &value.ReceivedAt); err != nil {
		return OperationCallback{}, err
	}
	return value, nil
}

func (s *OperationStore) fail(point string) error {
	if s != nil && s.failureHook != nil {
		return s.failureHook(point)
	}
	return nil
}

func hashState(state operationState) (string, error) { return hashValue(state) }

func hashValue(value any) (string, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

func hashString(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func validHash(value string) bool {
	if len(value) != sha256.Size*2 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func canonicalJSON(raw []byte) ([]byte, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return json.Marshal(value)
}

func canonicalTime(value *time.Time) string {
	if value == nil || value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}

func entityJSON(ref *contracts.EntityRef) ([]byte, error) {
	if ref == nil {
		return nil, nil
	}
	return json.Marshal(ref)
}

func entityValue(ref *contracts.EntityRef) any {
	if ref == nil {
		return nil
	}
	return ref
}

func mustJSON(value any) []byte { raw, _ := json.Marshal(value); return raw }

func operationNullableJSON(failure *contracts.OperationFailure) any {
	if failure == nil {
		return nil
	}
	raw, err := json.Marshal(failure)
	if err != nil {
		return nil
	}
	return raw
}

func boundedLeaseTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return time.Second
	}
	if ttl > maxOperationLeaseTTL {
		return maxOperationLeaseTTL
	}
	return ttl
}
