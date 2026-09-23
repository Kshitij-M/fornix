package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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
)

var (
	ErrOperationNotFound       = errors.New("operation not found")
	ErrOperationIdempotency    = errors.New("operation idempotency conflict")
	ErrOperationTransition     = errors.New("invalid operation transition")
	ErrOperationLeaseMissing   = errors.New("operation lease not found")
	ErrOperationLeaseHeld      = errors.New("operation lease is held by another owner")
	ErrOperationLeaseOwned     = errors.New("operation lease is not owned by this worker")
	ErrOperationLeaseFenced    = errors.New("operation lease fence is stale")
	ErrOperationLeaseExpired   = errors.New("operation lease is expired")
	ErrOperationLeaseReleased  = errors.New("operation lease is released")
	ErrOperationFenceExhausted = errors.New("operation lease fence is exhausted")
	ErrOperationTaskFence      = errors.New("task-bound operation fence is invalid")
	ErrOperationReplay         = errors.New("operation replay integrity failure")
	ErrOperationWorkspace      = errors.New("operation workspace violation")
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
	WorkspaceID string
	OperationID string
	StepID      string
	AttemptID   string
	OwnerID     string
	Fence       uint64
	Effect      contracts.ExternalEffect
	RequestHash string
}

type OperationEffect struct {
	WorkspaceID         string
	OperationID         string
	StepID              string
	AttemptID           string
	EffectID            string
	EffectClass         string
	Boundary            string
	IdempotencyKey      string
	ProviderRequestID   string
	ProviderIdempotency bool
	DeliverySemantics   string
	VerificationStatus  string
	CompensationStatus  string
	RequestHash         string
	ResponseHash        string
	CreatedAt           time.Time
	VerifiedAt          *time.Time
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
	Operation       Operation
	StateVersion    int64
	StateHash       string
	ReplayHash      string
	TransitionCount int
	Verified        bool
}

// OperationStore is the sole Postgres mutation boundary for generic operation
// authority. Existing task/run/tool/evidence authorities remain unchanged.
type OperationStore struct {
	pool        *pgxpool.Pool
	events      *EventStore
	failureHook func(string) error
}

func NewOperationStore(pool *pgxpool.Pool, events *EventStore) *OperationStore {
	if events == nil {
		events = NewEventStore(pool)
	}
	return &OperationStore{pool: pool, events: events}
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
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return OperationCreateResult{}, fmt.Errorf("begin operation create: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
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
		if err := tx.Commit(ctx); err != nil {
			return OperationCreateResult{}, fmt.Errorf("commit duplicate operation: %w", err)
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
	if err := tx.Commit(ctx); err != nil {
		return OperationCreateResult{}, fmt.Errorf("commit operation create: %w", err)
	}
	return OperationCreateResult{Operation: stored, Event: appended.Event}, nil
}

func (s *OperationStore) Get(ctx context.Context, workspaceID, operationID string) (Operation, error) {
	if s == nil || s.pool == nil {
		return Operation{}, errors.New("operation store is not configured")
	}
	value, err := readOperationByID(ctx, s.pool, strings.TrimSpace(workspaceID), strings.TrimSpace(operationID))
	if errors.Is(err, pgx.ErrNoRows) {
		return Operation{}, ErrOperationNotFound
	}
	return value, err
}

func (s *OperationStore) AcquireLease(ctx context.Context, workspaceID, operationID, ownerID string, ttl time.Duration) (OperationLeaseResult, error) {
	if s == nil || s.pool == nil {
		return OperationLeaseResult{}, errors.New("operation store is not configured")
	}
	tx, err := s.pool.Begin(ctx)
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
	operation, err := readOperationByID(ctx, tx, workspaceID, operationID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return OperationLeaseResult{}, ErrOperationNotFound
	}
	if err != nil {
		return OperationLeaseResult{}, fmt.Errorf("check operation for lease: %w", err)
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
	return OperationLeaseResult{Lease: updated, Acquired: true, Takeover: true}, nil
}

func (s *OperationStore) RenewLease(ctx context.Context, lease OperationLease, ttl time.Duration) (OperationLease, error) {
	if s == nil || s.pool == nil {
		return OperationLease{}, errors.New("operation store is not configured")
	}
	tx, err := s.pool.Begin(ctx)
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
	if _, err := s.validateLease(ctx, tx, lease); err != nil {
		return OperationLease{}, err
	}
	ttl = boundedLeaseTTL(ttl)
	if _, err := tx.Exec(ctx, `UPDATE fornix.operation_leases SET lease_until=clock_timestamp()+($3::double precision * interval '1 millisecond'),renewed_at=clock_timestamp() WHERE workspace_id=$1 AND operation_id=$2 AND owner_id=$4 AND fence=$5`, lease.WorkspaceID, lease.OperationID, ttl.Milliseconds(), lease.OwnerID, int64(lease.Fence)); err != nil {
		return OperationLease{}, fmt.Errorf("renew operation lease: %w", err)
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
	tx, err := s.pool.Begin(ctx)
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
	if _, err := s.validateLease(ctx, tx, lease); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.operation_leases SET released_at=clock_timestamp(),lease_until=clock_timestamp() WHERE workspace_id=$1 AND operation_id=$2 AND owner_id=$3 AND fence=$4`, lease.WorkspaceID, lease.OperationID, lease.OwnerID, int64(lease.Fence)); err != nil {
		return fmt.Errorf("release operation lease: %w", err)
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
	tx, err := s.pool.Begin(ctx)
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
	if input.Actor.WorkspaceID != operation.WorkspaceID || input.Actor.ID != operation.Request.Actor.ID || input.Actor.Kind != operation.Request.Actor.Kind {
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
	updated, err := tx.Exec(ctx, `UPDATE fornix.operations SET status=$3,state_version=$4,state_hash=$5,result_hash=$6,report_hash=$7,failure=$8::jsonb,next_retry_at=$9,task_owner_id=$10,task_fence=$11,updated_at=clock_timestamp(),started_at=CASE WHEN $3='running' AND started_at IS NULL THEN clock_timestamp() ELSE started_at END,completed_at=CASE WHEN $3 IN ('succeeded','failed','cancelled','dead_letter') THEN COALESCE(completed_at,clock_timestamp()) ELSE completed_at END WHERE workspace_id=$1 AND id=$2 AND state_version=$12`, input.WorkspaceID, input.OperationID, input.ToStatus, version, stateHash, input.ResultHash, input.ReportHash, operationNullableJSON(input.Failure), input.NextRetryAt, input.TaskOwnerID, int64(input.TaskFence), operation.StateVersion)
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
	if !validHash(input.RequestHash) || input.Attempt < 1 || input.Fence == 0 || input.Fence > maxOperationFence || input.IdempotencyKey == "" {
		return OperationAttempt{}, false, errors.New("attempt requires a request hash, positive attempt, fence, and idempotency key")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return OperationAttempt{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
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
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return OperationEffect{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	operation, err := readOperationByID(ctx, tx, input.WorkspaceID, input.OperationID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return OperationEffect{}, false, ErrOperationNotFound
	}
	if err != nil {
		return OperationEffect{}, false, err
	}
	if stored, err := readEffect(ctx, tx, input.WorkspaceID, input.AttemptID); err == nil {
		if stored.RequestHash != input.RequestHash || stored.OperationID != input.OperationID || stored.StepID != input.StepID {
			return OperationEffect{}, false, ErrOperationIdempotency
		}
		if err := tx.Commit(ctx); err != nil {
			return OperationEffect{}, false, err
		}
		return stored, false, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return OperationEffect{}, false, err
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
	var attemptOperation, attemptStep string
	if err := tx.QueryRow(ctx, `SELECT operation_id,step_id FROM fornix.operation_attempts WHERE workspace_id=$1 AND attempt_id=$2`, input.WorkspaceID, input.AttemptID).Scan(&attemptOperation, &attemptStep); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return OperationEffect{}, false, errors.New("operation attempt does not exist")
		}
		return OperationEffect{}, false, err
	}
	if attemptOperation != input.OperationID || attemptStep != input.StepID {
		return OperationEffect{}, false, ErrOperationWorkspace
	}
	inserted, err := tx.Exec(ctx, `INSERT INTO fornix.operation_effects(workspace_id,operation_id,step_id,attempt_id,effect_id,effect_class,boundary,idempotency_key,provider_request_id,provider_idempotency_supported,delivery_semantics,verification_status,compensation_status,request_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14) ON CONFLICT DO NOTHING`, input.WorkspaceID, input.OperationID, input.StepID, input.AttemptID, effect.ID, effect.Class, effect.Boundary, effect.IdempotencyKey, effect.ProviderRequestID, effect.ProviderIdempotency, effect.DeliveryGuarantee, effect.VerificationStatus, effect.CompensationStatus, input.RequestHash)
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
	if err := tx.Commit(ctx); err != nil {
		return OperationEffect{}, false, err
	}
	return stored, inserted.RowsAffected() == 1, nil
}

func (s *OperationStore) RecordCallback(ctx context.Context, input OperationCallbackInput) (OperationCallback, bool, error) {
	if s == nil || s.pool == nil {
		return OperationCallback{}, false, errors.New("operation store is not configured")
	}
	input.WorkspaceID, input.OperationID, input.OwnerID, input.CallbackID, input.CallbackKind = strings.TrimSpace(input.WorkspaceID), strings.TrimSpace(input.OperationID), strings.TrimSpace(input.OwnerID), strings.TrimSpace(input.CallbackID), strings.TrimSpace(input.CallbackKind)
	if input.WorkspaceID == "" || input.OperationID == "" || input.OwnerID == "" || input.Fence == 0 || input.Fence > maxOperationFence || input.CallbackID == "" || input.CallbackKind == "" || !validHash(input.RequestHash) || (input.ResponseHash != "" && !validHash(input.ResponseHash)) || strings.TrimSpace(input.Status) == "" || len(input.CallbackID) > contracts.MaxDomainIDLength || len(input.CallbackKind) > contracts.MaxDomainNameLength || len(input.Status) > 128 {
		return OperationCallback{}, false, errors.New("callback identity, owner, fence, status, and hashes are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return OperationCallback{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
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

// Replay is read-only and validates every committed transition hash and
// version. It cannot invoke a connector, model, tool, or callback.
func (s *OperationStore) Replay(ctx context.Context, workspaceID, operationID string, fromVersion int64, limit int) (OperationReplayResult, error) {
	operation, err := s.Get(ctx, workspaceID, operationID)
	if err != nil {
		return OperationReplayResult{}, err
	}
	if fromVersion < 0 || fromVersion > operation.StateVersion {
		return OperationReplayResult{}, ErrOperationReplay
	}
	if limit <= 0 || limit > maxOperationReplayLimit {
		limit = maxOperationReplayLimit
	}
	rows, err := s.pool.Query(ctx, `
		SELECT t.state_version, t.state_hash, t.previous_state_hash,
		       t.from_status, t.to_status, t.state, t.event_sequence,
		       e.sequence
		FROM fornix.operation_transitions t
		LEFT JOIN fornix.control_events e
		  ON e.workspace_id=t.workspace_id AND e.sequence=t.event_sequence
		WHERE t.workspace_id=$1 AND t.operation_id=$2
		ORDER BY t.state_version ASC LIMIT $3`, strings.TrimSpace(workspaceID), strings.TrimSpace(operationID), limit)
	if err != nil {
		return OperationReplayResult{}, err
	}
	defer rows.Close()
	previousVersion := int64(0)
	previousStatus := contracts.OperationStatusCreated
	previousHash, err := hashState(operationState{
		Status:       contracts.OperationStatusCreated,
		StateVersion: 0,
		PlanHash:     operation.PlanHash,
	})
	if err != nil {
		return OperationReplayResult{}, fmt.Errorf("hash initial operation state: %w", err)
	}
	transitions := make([]string, 0)
	count := 0
	checkpointSeen := fromVersion == 0
	for rows.Next() {
		var version int64
		var storedHash string
		var storedPreviousHash string
		var fromStatus, toStatus string
		var stateJSON []byte
		var eventSequence, matchedEventSequence *int64
		if err := rows.Scan(&version, &storedHash, &storedPreviousHash, &fromStatus, &toStatus, &stateJSON, &eventSequence, &matchedEventSequence); err != nil {
			return OperationReplayResult{}, err
		}
		if version != previousVersion+1 || storedPreviousHash != previousHash || fromStatus != previousStatus || eventSequence == nil || matchedEventSequence == nil {
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
		previousVersion, previousStatus, previousHash = version, toStatus, storedHash
		if version == fromVersion {
			checkpointSeen = true
		}
		if version > fromVersion {
			count++
			transitions = append(transitions, fmt.Sprintf("%d:%s", version, storedHash))
		}
	}
	if err := rows.Err(); err != nil {
		return OperationReplayResult{}, err
	}
	if !checkpointSeen || previousVersion != operation.StateVersion || previousHash != operation.StateHash {
		return OperationReplayResult{}, ErrOperationReplay
	}
	replayHash, err := hashValue(struct {
		WorkspaceID string   `json:"workspace_id"`
		OperationID string   `json:"operation_id"`
		FromVersion int64    `json:"from_version"`
		States      []string `json:"states"`
	}{workspaceID, operationID, fromVersion, transitions})
	if err != nil {
		return OperationReplayResult{}, err
	}
	return OperationReplayResult{Operation: operation, StateVersion: operation.StateVersion, StateHash: operation.StateHash, ReplayHash: replayHash, TransitionCount: count, Verified: true}, nil
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
	var currentOwner string
	var currentFence int64
	var assigned *string
	err = tx.QueryRow(ctx, `
		SELECT l.owner_id, l.fence, t.assigned_session
		FROM fornix.task_execution_leases l
		JOIN fornix.tasks t ON t.workspace_id=l.workspace_id AND t.id=l.task_id
		WHERE l.workspace_id=$1 AND l.task_id=$2
		  AND l.released_at IS NULL AND l.lease_until > clock_timestamp()
		FOR UPDATE OF l, t`, task.WorkspaceID, taskID).Scan(&currentOwner, &currentFence, &assigned)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrOperationTaskFence
	}
	if err != nil {
		return fmt.Errorf("validate operation task fence: %w", err)
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
	return readEffectQuery(ctx, queryer, `SELECT workspace_id,operation_id,step_id,attempt_id,effect_id,effect_class,boundary,idempotency_key,provider_request_id,provider_idempotency_supported,delivery_semantics,verification_status,compensation_status,request_hash,response_hash,created_at,verified_at FROM fornix.operation_effects WHERE workspace_id=$1 AND attempt_id=$2`, workspaceID, attemptID)
}

func readEffectByID(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, effectID string) (OperationEffect, error) {
	return readEffectQuery(ctx, queryer, `SELECT workspace_id,operation_id,step_id,attempt_id,effect_id,effect_class,boundary,idempotency_key,provider_request_id,provider_idempotency_supported,delivery_semantics,verification_status,compensation_status,request_hash,response_hash,created_at,verified_at FROM fornix.operation_effects WHERE workspace_id=$1 AND effect_id=$2`, workspaceID, effectID)
}

func readEffectQuery(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, query string, args ...any) (OperationEffect, error) {
	var value OperationEffect
	if err := queryer.QueryRow(ctx, query, args...).Scan(&value.WorkspaceID, &value.OperationID, &value.StepID, &value.AttemptID, &value.EffectID, &value.EffectClass, &value.Boundary, &value.IdempotencyKey, &value.ProviderRequestID, &value.ProviderIdempotency, &value.DeliverySemantics, &value.VerificationStatus, &value.CompensationStatus, &value.RequestHash, &value.ResponseHash, &value.CreatedAt, &value.VerifiedAt); err != nil {
		return OperationEffect{}, err
	}
	return value, nil
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
