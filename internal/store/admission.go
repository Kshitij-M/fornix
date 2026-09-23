package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	policyruntime "github.com/omaveda/fornix/internal/policy"
)

var (
	ErrAdmissionNotFound       = errors.New("operation admission not found")
	ErrAdmissionConflict       = errors.New("operation admission conflicts with existing state")
	ErrAdmissionOperation      = errors.New("operation admission does not match operation")
	ErrAdmissionApproval       = errors.New("operation approval is not pending")
	ErrAdmissionApprover       = errors.New("operation approval requires a distinct authorized actor")
	ErrAdmissionEffect         = errors.New("external effect state transition is invalid")
	ErrAdmissionEffectTerminal = errors.New("external effect state is terminal")
)

// AdmissionResult contains the immutable decision and, for write-like
// effects, the exact approval request that must be decided before execution.
type AdmissionResult struct {
	Decision  contracts.AdmissionDecision
	Approval  *contracts.OperationApprovalRequest
	Duplicate bool
}

// EffectState is the current projection of an external-effect history. The
// append-only transition rows remain the recovery authority.
type EffectState struct {
	WorkspaceID       string
	EffectID          string
	OperationID       string
	State             string
	Version           int64
	ProviderRequestID string
	ResponseHash      string
	VerificationHash  string
	CompensationHash  string
	FailureCode       string
	UpdatedAt         time.Time
}

type EffectStateResult struct {
	State     EffectState
	Duplicate bool
}

// AdmissionStore is the Postgres authority for generic policy decisions,
// operation-bound approvals, and external-effect recovery states. It never
// calls a connector or resolves credential values.
type AdmissionStore struct {
	pool        *pgxpool.Pool
	events      *EventStore
	failureHook func(string) error
}

func NewAdmissionStore(pool *pgxpool.Pool, events *EventStore) *AdmissionStore {
	if events == nil {
		events = NewEventStore(pool)
	}
	return &AdmissionStore{pool: pool, events: events}
}

// SetFailureHook provides deterministic transaction crash points for tests.
func (s *AdmissionStore) SetFailureHook(hook func(string) error) {
	if s != nil {
		s.failureHook = hook
	}
}

// GetDecision reads one immutable decision within its workspace. It never
// evaluates policy or contacts a connector.
func (s *AdmissionStore) GetDecision(ctx context.Context, workspaceID, decisionID string) (contracts.AdmissionDecision, error) {
	if s == nil || s.pool == nil {
		return contracts.AdmissionDecision{}, fmt.Errorf("admission store is not configured")
	}
	return readAdmissionDecision(ctx, s.pool, workspaceID, decisionID)
}

// GetApproval reads the exact workspace-scoped approval binding.
func (s *AdmissionStore) GetApproval(ctx context.Context, workspaceID, approvalID string) (contracts.OperationApprovalRequest, error) {
	if s == nil || s.pool == nil {
		return contracts.OperationApprovalRequest{}, fmt.Errorf("admission store is not configured")
	}
	return readOperationApproval(ctx, s.pool, workspaceID, approvalID)
}

// GetEffectState reads the current projection of an append-only effect
// history. Callers should inspect the history for recovery and audit detail.
func (s *AdmissionStore) GetEffectState(ctx context.Context, workspaceID, effectID string) (EffectState, error) {
	if s == nil || s.pool == nil {
		return EffectState{}, fmt.Errorf("admission store is not configured")
	}
	return readEffectState(ctx, s.pool, workspaceID, effectID)
}

func (s *AdmissionStore) fail(stage string) error {
	if s != nil && s.failureHook != nil {
		return s.failureHook(stage)
	}
	return nil
}

// Admit evaluates and durably records one operation admission. The operation
// must already exist; its operation hash, actor, capability, and target are
// checked while the row is locked. Quota usage is read from committed
// admission decisions in the same transaction before evaluation.
func (s *AdmissionStore) Admit(ctx context.Context, input contracts.AdmissionInput) (AdmissionResult, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return AdmissionResult{}, fmt.Errorf("admission store is not configured")
	}
	if err := input.Normalize(); err != nil {
		return AdmissionResult{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return AdmissionResult{}, fmt.Errorf("begin operation admission: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	operation, err := readOperationByID(ctx, tx, input.WorkspaceID, input.OperationID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return AdmissionResult{}, ErrOperationNotFound
	}
	if err != nil {
		return AdmissionResult{}, err
	}
	if err := validateAdmissionOperation(input, operation); err != nil {
		return AdmissionResult{}, err
	}
	if existing, err := readAdmissionDecisionByKey(ctx, tx, input.WorkspaceID, input.IdempotencyKey); err == nil {
		if existing.InputHash != input.StableHash() {
			return AdmissionResult{}, ErrAdmissionConflict
		}
		var approval contracts.OperationApprovalRequest
		if existing.ApprovalID != "" {
			approval, err = readOperationApproval(ctx, tx, input.WorkspaceID, existing.ApprovalID)
			if err != nil {
				return AdmissionResult{}, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return AdmissionResult{}, fmt.Errorf("commit duplicate operation admission: %w", err)
		}
		return AdmissionResult{Decision: existing, Approval: approvalPtr(approval), Duplicate: true}, nil
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return AdmissionResult{}, err
	}
	if contracts.IsTerminalOperationStatus(operation.Status) {
		return AdmissionResult{}, ErrAdmissionOperation
	}
	if operation.Request.Task != nil {
		input.TaskFenceValid = false
		if input.TaskOwnerID == operation.TaskOwnerID && input.TaskFence == operation.TaskFence && input.TaskFence != 0 {
			if err := validateTaskFenceForOperationTx(ctx, tx, operation.Request.Task, input.TaskOwnerID, input.TaskFence); err == nil {
				input.TaskFenceValid = true
			} else if !errors.Is(err, ErrOperationTaskFence) {
				return AdmissionResult{}, err
			}
		}
	} else if input.TaskOwnerID != "" || input.TaskFence != 0 {
		return AdmissionResult{}, ErrAdmissionOperation
	}
	// Serialize quota reservations for one workspace/actor. The advisory key
	// is derived only from bounded identity fields; a collision is conservative
	// (it adds waiting) rather than an isolation or quota bypass.
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1, 0))`, input.WorkspaceID+":"+input.Actor.ID); err != nil {
		return AdmissionResult{}, fmt.Errorf("lock admission quota: %w", err)
	}
	var quotaOperations int
	var quotaCost int64
	if err := tx.QueryRow(ctx, `
		SELECT count(*)::int, COALESCE(sum(cost_micros), 0)::bigint
		FROM fornix.operation_admission_decisions
		WHERE workspace_id=$1 AND (actor->>'id')=$2 AND status <> 'denied'
		  AND created_at >= clock_timestamp() - ($3::int * interval '1 second')`,
		input.WorkspaceID, input.Actor.ID, input.Policy.QuotaWindowSeconds).Scan(&quotaOperations, &quotaCost); err != nil {
		return AdmissionResult{}, fmt.Errorf("read admission quota: %w", err)
	}
	input.QuotaOperations, input.QuotaCostMicros = quotaOperations, quotaCost
	evaluation, err := policyruntime.Evaluate(input)
	if err != nil {
		return AdmissionResult{}, err
	}
	decision := evaluation.Decision
	capabilityJSON, _ := json.Marshal(input.Capability.Ref)
	targetJSON, _ := json.Marshal(input.Target)
	policyJSON, _ := json.Marshal(input.Policy)
	inserted, err := tx.Exec(ctx, `
		INSERT INTO fornix.operation_admission_decisions(
		 workspace_id,decision_id,operation_id,operation_hash,request_id,idempotency_key,
		 actor,capability,target,effect_class,policy_id,policy_version,policy_hash,
		 policy_snapshot,input_hash,decision_hash,status,reason_code,approval_id,cost_micros)
		VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9::jsonb,$10,$11,$12,$13,$14::jsonb,$15,$16,$17,$18,$19,$20)
		ON CONFLICT DO NOTHING`,
		decision.WorkspaceID, decision.ID, decision.OperationID, decision.OperationHash, decision.RequestID, decision.IdempotencyKey,
		mustJSON(decision.Actor), capabilityJSON, targetJSON, decision.Effect, decision.PolicyID, decision.PolicyVersion, decision.PolicyHash,
		policyJSON, decision.InputHash, decision.DecisionHash, decision.Status, decision.ReasonCode, decision.ApprovalID, decision.CostMicros)
	if err != nil {
		return AdmissionResult{}, fmt.Errorf("insert operation admission: %w", err)
	}
	if inserted.RowsAffected() == 0 {
		existing, readErr := readAdmissionDecisionByKey(ctx, tx, input.WorkspaceID, input.IdempotencyKey)
		if readErr != nil {
			return AdmissionResult{}, readErr
		}
		if existing.InputHash != decision.InputHash {
			return AdmissionResult{}, ErrAdmissionConflict
		}
		var approval contracts.OperationApprovalRequest
		if existing.ApprovalID != "" {
			approval, err = readOperationApproval(ctx, tx, input.WorkspaceID, existing.ApprovalID)
			if err != nil {
				return AdmissionResult{}, err
			}
		}
		if err := tx.Commit(ctx); err != nil {
			return AdmissionResult{}, fmt.Errorf("commit conflicting operation admission: %w", err)
		}
		return AdmissionResult{Decision: existing, Approval: approvalPtr(approval), Duplicate: true}, nil
	}
	if evaluation.Approval != nil {
		approval := evaluation.Approval
		if _, err := tx.Exec(ctx, `
			INSERT INTO fornix.operation_approvals(
			 workspace_id,approval_id,decision_id,operation_id,operation_hash,decision_hash,
			 capability_hash,target_hash,input_hash,policy_hash,effect_class,requested_by,status,expires_at)
			VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13,$14)`,
			approval.WorkspaceID, approval.ID, decision.ID, approval.OperationID, approval.OperationHash, approval.DecisionHash,
			approval.CapabilityHash, approval.TargetHash, approval.InputHash, approval.PolicyHash, approval.Effect, mustJSON(approval.RequestedBy), approval.Status, approval.ExpiresAt); err != nil {
			return AdmissionResult{}, fmt.Errorf("insert operation approval: %w", err)
		}
		commandHash := hashString("approval-request:" + approval.ID)
		if _, err := tx.Exec(ctx, `
			INSERT INTO fornix.operation_approval_transitions(workspace_id,approval_id,version,from_status,to_status,request_id,idempotency_key,actor,command_hash)
			VALUES($1,$2,1,'pending','pending',$3,$4,$5::jsonb,$6)`,
			approval.WorkspaceID, approval.ID, input.RequestID, "approval-request:"+approval.ID, mustJSON(approval.RequestedBy), commandHash); err != nil {
			return AdmissionResult{}, fmt.Errorf("insert operation approval history: %w", err)
		}
	}
	event, err := admissionEvent("operation.admission_decided", decision.WorkspaceID, decision.Actor, "admission:"+input.IdempotencyKey, map[string]any{
		"decision_id": decision.ID, "operation_id": decision.OperationID, "operation_hash": decision.OperationHash,
		"decision_hash": decision.DecisionHash, "input_hash": decision.InputHash, "status": decision.Status,
		"reason_code": decision.ReasonCode, "effect": decision.Effect, "policy_id": decision.PolicyID,
		"policy_version": decision.PolicyVersion, "policy_hash": decision.PolicyHash, "approval_id": decision.ApprovalID,
	})
	if err != nil {
		return AdmissionResult{}, err
	}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return AdmissionResult{}, fmt.Errorf("append admission event: %w", err)
	}
	if err := s.fail("operation_admission_committed"); err != nil {
		return AdmissionResult{}, err
	}
	stored, err := readAdmissionDecision(ctx, tx, decision.WorkspaceID, decision.ID)
	if err != nil {
		return AdmissionResult{}, err
	}
	var approval *contracts.OperationApprovalRequest
	if stored.ApprovalID != "" {
		value, readErr := readOperationApproval(ctx, tx, stored.WorkspaceID, stored.ApprovalID)
		if readErr != nil {
			return AdmissionResult{}, readErr
		}
		approval = approvalPtr(value)
	}
	if err := tx.Commit(ctx); err != nil {
		return AdmissionResult{}, fmt.Errorf("commit operation admission: %w", err)
	}
	return AdmissionResult{Decision: stored, Approval: approval}, nil
}

// DecideApproval applies one authorized, idempotent approval decision. An
// approver must be distinct from the requester so an AI actor cannot approve
// its own write through the generic boundary.
func (s *AdmissionStore) DecideApproval(ctx context.Context, command contracts.OperationApprovalDecision) (contracts.OperationApprovalRequest, bool, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return contracts.OperationApprovalRequest{}, false, fmt.Errorf("admission store is not configured")
	}
	if err := command.Normalize(); err != nil {
		return contracts.OperationApprovalRequest{}, false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.OperationApprovalRequest{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	approval, err := readOperationApproval(ctx, tx, command.WorkspaceID, command.ApprovalID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.OperationApprovalRequest{}, false, ErrAdmissionNotFound
	}
	if err != nil {
		return contracts.OperationApprovalRequest{}, false, err
	}
	commandHash := command.StableHash()
	var existingVersion int64
	var existingHash string
	err = tx.QueryRow(ctx, `SELECT version,command_hash FROM fornix.operation_approval_transitions WHERE workspace_id=$1 AND approval_id=$2 AND idempotency_key=$3`, command.WorkspaceID, command.ApprovalID, command.IdempotencyKey).Scan(&existingVersion, &existingHash)
	if err == nil {
		if existingHash != commandHash {
			return contracts.OperationApprovalRequest{}, false, ErrAdmissionConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.OperationApprovalRequest{}, false, err
		}
		return approval, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return contracts.OperationApprovalRequest{}, false, err
	}
	if approval.Status != contracts.ApprovalRequestPending {
		return approval, false, ErrAdmissionApproval
	}
	if time.Now().UTC().After(approval.ExpiresAt) && command.Decision != contracts.ApprovalRequestExpired {
		command.Decision = contracts.ApprovalRequestExpired
	}
	if approval.RequestedBy.ID == command.Actor.ID {
		return contracts.OperationApprovalRequest{}, false, ErrAdmissionApprover
	}
	decisionTime := time.Now().UTC()
	if _, err := tx.Exec(ctx, `UPDATE fornix.operation_approvals SET status=$3,decided_at=$4,decided_by=$5::jsonb,decision_reason_hash=$6 WHERE workspace_id=$1 AND approval_id=$2`, command.WorkspaceID, command.ApprovalID, command.Decision, decisionTime, mustJSON(command.Actor), command.ReasonHash); err != nil {
		return contracts.OperationApprovalRequest{}, false, err
	}
	if err := tx.QueryRow(ctx, `SELECT COALESCE(max(version),0)+1 FROM fornix.operation_approval_transitions WHERE workspace_id=$1 AND approval_id=$2`, command.WorkspaceID, command.ApprovalID).Scan(&existingVersion); err != nil {
		return contracts.OperationApprovalRequest{}, false, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.operation_approval_transitions(workspace_id,approval_id,version,from_status,to_status,request_id,idempotency_key,actor,command_hash,reason_hash)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10)`,
		command.WorkspaceID, command.ApprovalID, existingVersion, approval.Status, command.Decision, command.RequestID, command.IdempotencyKey, mustJSON(command.Actor), commandHash, command.ReasonHash); err != nil {
		return contracts.OperationApprovalRequest{}, false, err
	}
	event, err := admissionEvent("operation.approval_decided", command.WorkspaceID, command.Actor, "approval:"+command.IdempotencyKey, map[string]any{
		"approval_id": command.ApprovalID, "operation_id": approval.OperationID, "decision": command.Decision,
		"request_id": command.RequestID, "command_hash": commandHash,
	})
	if err != nil {
		return contracts.OperationApprovalRequest{}, false, err
	}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.OperationApprovalRequest{}, false, err
	}
	if err := s.fail("operation_approval_committed"); err != nil {
		return contracts.OperationApprovalRequest{}, false, err
	}
	approval, err = readOperationApproval(ctx, tx, command.WorkspaceID, command.ApprovalID)
	if err != nil {
		return contracts.OperationApprovalRequest{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.OperationApprovalRequest{}, false, err
	}
	return approval, false, nil
}

// UpdateEffect records a fenced external state transition. The connector is
// called outside this transaction; a crash after dispatch therefore remains
// visibly acknowledged/uncertain and is never silently retried.
func (s *AdmissionStore) UpdateEffect(ctx context.Context, update contracts.ExternalEffectUpdate) (EffectStateResult, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return EffectStateResult{}, fmt.Errorf("admission store is not configured")
	}
	if err := update.Normalize(); err != nil {
		return EffectStateResult{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return EffectStateResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var operationID string
	if err := tx.QueryRow(ctx, `SELECT operation_id FROM fornix.operation_effects WHERE workspace_id=$1 AND effect_id=$2 FOR SHARE`, update.WorkspaceID, update.EffectID).Scan(&operationID); errors.Is(err, pgx.ErrNoRows) {
		return EffectStateResult{}, ErrAdmissionNotFound
	} else if err != nil {
		return EffectStateResult{}, err
	} else if operationID != update.OperationID {
		return EffectStateResult{}, ErrOperationWorkspace
	}
	var commandHash string
	if err := externalEffectCommandHash(update, &commandHash); err != nil {
		return EffectStateResult{}, err
	}
	var priorVersion int64
	var priorHash string
	err = tx.QueryRow(ctx, `SELECT version,command_hash FROM fornix.operation_effect_transitions WHERE workspace_id=$1 AND effect_id=$2 AND idempotency_key=$3`, update.WorkspaceID, update.EffectID, update.IdempotencyKey).Scan(&priorVersion, &priorHash)
	if err == nil {
		if priorHash != commandHash {
			return EffectStateResult{}, ErrAdmissionConflict
		}
		state, readErr := readEffectState(ctx, tx, update.WorkspaceID, update.EffectID)
		if readErr != nil {
			return EffectStateResult{}, readErr
		}
		if err := tx.Commit(ctx); err != nil {
			return EffectStateResult{}, err
		}
		return EffectStateResult{State: state, Duplicate: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return EffectStateResult{}, err
	}
	operation, err := readOperationByID(ctx, tx, update.WorkspaceID, update.OperationID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return EffectStateResult{}, ErrOperationNotFound
	}
	if err != nil {
		return EffectStateResult{}, err
	}
	if _, err := (&OperationStore{pool: s.pool}).validateLease(ctx, tx, OperationLease{WorkspaceID: update.WorkspaceID, OperationID: update.OperationID, OwnerID: update.OwnerID, Fence: update.Fence}); err != nil {
		return EffectStateResult{}, err
	}
	if operation.Request.Task != nil {
		if err := validateTaskFenceForOperationTx(ctx, tx, operation.Request.Task, operation.TaskOwnerID, operation.TaskFence); err != nil {
			return EffectStateResult{}, err
		}
	}
	state, err := ensureEffectState(ctx, tx, update.WorkspaceID, update.EffectID, update.OperationID, update.OwnerID, update.Fence)
	if err != nil {
		return EffectStateResult{}, err
	}
	if !validEffectTransition(state.State, update.State) {
		if isTerminalEffectState(state.State) {
			return EffectStateResult{}, ErrAdmissionEffectTerminal
		}
		return EffectStateResult{}, fmt.Errorf("%w: %s -> %s", ErrAdmissionEffect, state.State, update.State)
	}
	nextVersion := state.Version + 1
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.operation_effect_transitions(
		 workspace_id,effect_id,operation_id,version,from_state,to_state,request_id,idempotency_key,owner_id,fence,command_hash,provider_request_id,response_hash,verification_hash,compensation_hash,failure_code)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
		update.WorkspaceID, update.EffectID, update.OperationID, nextVersion, state.State, update.State, update.RequestID, update.IdempotencyKey, update.OwnerID, int64(update.Fence), commandHash, update.ProviderRequestID, update.ResponseHash, update.VerificationHash, update.CompensationHash, update.FailureCode); err != nil {
		return EffectStateResult{}, err
	}
	updated, err := tx.Exec(ctx, `
		UPDATE fornix.operation_effect_state SET state=$3,version=$4,
		 provider_request_id=CASE WHEN $5='' THEN provider_request_id ELSE $5 END,
		 response_hash=CASE WHEN $6='' THEN response_hash ELSE $6 END,
		 verification_hash=CASE WHEN $7='' THEN verification_hash ELSE $7 END,
		 compensation_hash=CASE WHEN $8='' THEN compensation_hash ELSE $8 END,
		 failure_code=$9,updated_at=clock_timestamp()
		WHERE workspace_id=$1 AND effect_id=$2 AND version=$10`, update.WorkspaceID, update.EffectID, update.State, nextVersion, update.ProviderRequestID, update.ResponseHash, update.VerificationHash, update.CompensationHash, update.FailureCode, state.Version)
	if err != nil {
		return EffectStateResult{}, err
	}
	if updated.RowsAffected() != 1 {
		return EffectStateResult{}, ErrAdmissionEffect
	}
	event, err := admissionEvent("operation.external_effect_state_changed", update.WorkspaceID, operation.Request.Actor, "effect:"+update.IdempotencyKey, map[string]any{
		"operation_id": update.OperationID, "effect_id": update.EffectID, "from_state": state.State,
		"to_state": update.State, "version": nextVersion, "provider_request_id": update.ProviderRequestID,
		"response_hash": update.ResponseHash, "verification_hash": update.VerificationHash, "compensation_hash": update.CompensationHash,
		"failure_code": update.FailureCode,
	})
	if err != nil {
		return EffectStateResult{}, err
	}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return EffectStateResult{}, err
	}
	if err := s.fail("operation_effect_state_committed"); err != nil {
		return EffectStateResult{}, err
	}
	state, err = readEffectState(ctx, tx, update.WorkspaceID, update.EffectID)
	if err != nil {
		return EffectStateResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return EffectStateResult{}, err
	}
	return EffectStateResult{State: state}, nil
}

func validateAdmissionOperation(input contracts.AdmissionInput, operation Operation) error {
	if operation.OperationHash != input.OperationHash || operation.WorkspaceID != input.WorkspaceID || operation.Request.Actor.ID != input.Actor.ID || operation.Request.Actor.WorkspaceID != input.Actor.WorkspaceID {
		return ErrAdmissionOperation
	}
	if operation.Request.Capability.StableHash() != input.Capability.Ref.StableHash() || operation.Request.Target.StableHash() != input.Target.StableHash() {
		return ErrAdmissionOperation
	}
	if (operation.Request.Task != nil) != input.TaskBound {
		return ErrAdmissionOperation
	}
	return nil
}

func admissionEvent(eventType, workspace string, actor contracts.ActorRef, idempotency string, payload any) (contracts.EventEnvelope, error) {
	event, err := contracts.NewEvent(eventType, payload)
	if err != nil {
		return contracts.EventEnvelope{}, err
	}
	event.Scope = contracts.Scope{WorkspaceID: workspace}
	event.Actor = actor
	event.IdempotencyKey = "oa_" + hashString(idempotency)
	event.CorrelationID = hashString(workspace + "\x00" + idempotency)
	return event, nil
}

func approvalPtr(value contracts.OperationApprovalRequest) *contracts.OperationApprovalRequest {
	if value.ID == "" {
		return nil
	}
	return &value
}

func readAdmissionDecisionByKey(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, key string) (contracts.AdmissionDecision, error) {
	return readAdmissionDecisionQuery(ctx, queryer, `SELECT decision_id,workspace_id,operation_id,operation_hash,request_id,idempotency_key,actor,capability,target,effect_class,policy_id,policy_version,policy_hash,input_hash,decision_hash,status,reason_code,approval_id,cost_micros,created_at FROM fornix.operation_admission_decisions WHERE workspace_id=$1 AND idempotency_key=$2 FOR UPDATE`, workspaceID, key)
}

func readAdmissionDecision(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, id string) (contracts.AdmissionDecision, error) {
	return readAdmissionDecisionQuery(ctx, queryer, `SELECT decision_id,workspace_id,operation_id,operation_hash,request_id,idempotency_key,actor,capability,target,effect_class,policy_id,policy_version,policy_hash,input_hash,decision_hash,status,reason_code,approval_id,cost_micros,created_at FROM fornix.operation_admission_decisions WHERE workspace_id=$1 AND decision_id=$2`, workspaceID, id)
}

func readAdmissionDecisionQuery(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, query string, args ...any) (contracts.AdmissionDecision, error) {
	var value contracts.AdmissionDecision
	var actorJSON, capabilityJSON, targetJSON []byte
	if err := queryer.QueryRow(ctx, query, args...).Scan(&value.ID, &value.WorkspaceID, &value.OperationID, &value.OperationHash, &value.RequestID, &value.IdempotencyKey, &actorJSON, &capabilityJSON, &targetJSON, &value.Effect, &value.PolicyID, &value.PolicyVersion, &value.PolicyHash, &value.InputHash, &value.DecisionHash, &value.Status, &value.ReasonCode, &value.ApprovalID, &value.CostMicros, &value.CreatedAt); err != nil {
		return contracts.AdmissionDecision{}, err
	}
	if err := json.Unmarshal(actorJSON, &value.Actor); err != nil {
		return contracts.AdmissionDecision{}, err
	}
	if err := json.Unmarshal(capabilityJSON, &value.Capability); err != nil {
		return contracts.AdmissionDecision{}, err
	}
	if err := json.Unmarshal(targetJSON, &value.Target); err != nil {
		return contracts.AdmissionDecision{}, err
	}
	return value, nil
}

func readOperationApproval(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, approvalID string, lock ...bool) (contracts.OperationApprovalRequest, error) {
	query := `SELECT schema_version,approval_id,workspace_id,operation_id,operation_hash,decision_hash,capability_hash,target_hash,input_hash,policy_hash,effect_class,requested_by,status,expires_at,created_at,decided_at,decided_by,decision_reason_hash FROM fornix.operation_approvals WHERE workspace_id=$1 AND approval_id=$2`
	if len(lock) > 0 && lock[0] {
		query += " FOR UPDATE"
	}
	var value contracts.OperationApprovalRequest
	var requestedJSON, decidedJSON []byte
	if err := queryer.QueryRow(ctx, query, workspaceID, approvalID).Scan(&value.SchemaVersion, &value.ID, &value.WorkspaceID, &value.OperationID, &value.OperationHash, &value.DecisionHash, &value.CapabilityHash, &value.TargetHash, &value.InputHash, &value.PolicyHash, &value.Effect, &requestedJSON, &value.Status, &value.ExpiresAt, &value.CreatedAt, &value.DecidedAt, &decidedJSON, &value.DecisionReasonHash); err != nil {
		return contracts.OperationApprovalRequest{}, err
	}
	if err := json.Unmarshal(requestedJSON, &value.RequestedBy); err != nil {
		return contracts.OperationApprovalRequest{}, err
	}
	if len(decidedJSON) > 0 && string(decidedJSON) != "null" && string(decidedJSON) != "{}" {
		var actor contracts.ActorRef
		if err := json.Unmarshal(decidedJSON, &actor); err != nil {
			return contracts.OperationApprovalRequest{}, err
		}
		value.DecidedBy = &actor
	}
	return value, nil
}

func readEffectState(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, effectID string) (EffectState, error) {
	var value EffectState
	if err := queryer.QueryRow(ctx, `SELECT workspace_id,effect_id,operation_id,state,version,provider_request_id,response_hash,verification_hash,compensation_hash,failure_code,updated_at FROM fornix.operation_effect_state WHERE workspace_id=$1 AND effect_id=$2`, workspaceID, effectID).Scan(&value.WorkspaceID, &value.EffectID, &value.OperationID, &value.State, &value.Version, &value.ProviderRequestID, &value.ResponseHash, &value.VerificationHash, &value.CompensationHash, &value.FailureCode, &value.UpdatedAt); err != nil {
		return EffectState{}, err
	}
	return value, nil
}

func ensureEffectState(ctx context.Context, tx pgx.Tx, workspaceID, effectID, operationID, owner string, fence uint64) (EffectState, error) {
	state, err := readEffectState(ctx, tx, workspaceID, effectID)
	if err == nil {
		return state, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return EffectState{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.operation_effect_state(workspace_id,effect_id,operation_id,state,version) VALUES($1,$2,$3,'reserved',1) ON CONFLICT DO NOTHING`, workspaceID, effectID, operationID); err != nil {
		return EffectState{}, err
	}
	commandHash := hashString("effect-reserved:" + effectID)
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.operation_effect_transitions(workspace_id,effect_id,operation_id,version,from_state,to_state,request_id,idempotency_key,owner_id,fence,command_hash) VALUES($1,$2,$3,1,'reserved','reserved',$4,$5,$6,$7,$8) ON CONFLICT DO NOTHING`, workspaceID, effectID, operationID, "effect-reserved:"+effectID, "effect-reserved:"+effectID, owner, int64(fence), commandHash); err != nil {
		return EffectState{}, err
	}
	return readEffectState(ctx, tx, workspaceID, effectID)
}

func hashInto(value any, target *string) error {
	hash, err := hashValue(value)
	if err != nil {
		return err
	}
	*target = hash
	return nil
}

func externalEffectCommandHash(update contracts.ExternalEffectUpdate, target *string) error {
	clone := update
	clone.RequestID, clone.IdempotencyKey, clone.OwnerID, clone.Fence = "", "", "", 0
	return hashInto(clone, target)
}

func validEffectTransition(from, to string) bool {
	switch from {
	case contracts.ExternalEffectReserved:
		return to == contracts.ExternalEffectDispatched || to == contracts.ExternalEffectRecoveryRequired
	case contracts.ExternalEffectDispatched:
		return to == contracts.ExternalEffectAcknowledged || to == contracts.ExternalEffectRecoveryRequired
	case contracts.ExternalEffectAcknowledged:
		return to == contracts.ExternalEffectVerificationPending || to == contracts.ExternalEffectVerified || to == contracts.ExternalEffectRecoveryRequired
	case contracts.ExternalEffectVerificationPending:
		return to == contracts.ExternalEffectVerified || to == contracts.ExternalEffectVerificationFailed || to == contracts.ExternalEffectRecoveryRequired
	case contracts.ExternalEffectVerificationFailed:
		return to == contracts.ExternalEffectCompensationPending || to == contracts.ExternalEffectRecoveryRequired
	case contracts.ExternalEffectCompensationPending:
		return to == contracts.ExternalEffectCompensated || to == contracts.ExternalEffectRecoveryRequired
	case contracts.ExternalEffectRecoveryRequired:
		return to == contracts.ExternalEffectVerificationPending || to == contracts.ExternalEffectCompensationPending
	default:
		return false
	}
}

func isTerminalEffectState(state string) bool {
	return state == contracts.ExternalEffectVerified || state == contracts.ExternalEffectCompensated
}
