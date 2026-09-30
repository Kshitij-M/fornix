package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

const (
	maxWorkflowReplayLimit = 4096
	workflowCommandCreate  = "create"
	workflowCommandStart   = "start"
	workflowCommandFinish  = "finish"
	workflowCommandCancel  = "cancel"
)

var (
	ErrWorkflowNotFound           = errors.New("workflow not found")
	ErrWorkflowIdempotency        = errors.New("workflow idempotency conflict")
	ErrWorkflowIncomplete         = errors.New("workflow idempotency record is incomplete")
	ErrWorkflowTransition         = errors.New("invalid workflow transition")
	ErrWorkflowLease              = errors.New("workflow lease is invalid")
	ErrWorkflowWorkspace          = errors.New("workflow workspace violation")
	ErrWorkflowCancelled          = errors.New("workflow is cancelled")
	ErrWorkflowTerminal           = errors.New("workflow is terminal")
	ErrWorkflowStepNotFound       = errors.New("workflow step not found")
	ErrWorkflowStepNotRunnable    = errors.New("workflow step is not runnable")
	ErrWorkflowStepAlreadyStarted = errors.New("workflow step is already started")
	ErrWorkflowReplay             = errors.New("workflow replay integrity failure")
	ErrWorkflowBudget             = errors.New("workflow budget exceeded")
	ErrWorkflowRetryNotDue        = errors.New("workflow retry deadline has not elapsed")
)

// WorkflowStore owns the durable workflow projection. OperationStore remains
// the root authority for operation identity, leases, task fences, and the
// generic lifecycle. This store never invokes an adapter while a transaction
// is open: it commits a checkpoint first, then the runtime may execute work.
type WorkflowStore struct {
	pool        *pgxpool.Pool
	events      *EventStore
	operations  *OperationStore
	failureHook func(string) error
}

type WorkflowCreateInput struct {
	Operation OperationCreateInput
	Budget    contracts.WorkflowBudget
}

type WorkflowCreateResult struct {
	Run       contracts.WorkflowRun
	Operation Operation
	Event     contracts.EventEnvelope
	Duplicate bool
}

type WorkflowLease = OperationLease
type WorkflowLeaseResult = OperationLeaseResult

type WorkflowStepStartInput struct {
	WorkspaceID    string
	RunID          string
	StepID         string
	OwnerID        string
	Fence          uint64
	TaskOwnerID    string
	TaskFence      uint64
	Actor          contracts.ActorRef
	RequestID      string
	IdempotencyKey string
	CausationID    string
	CorrelationID  string
}

type WorkflowStepStartResult struct {
	Run       contracts.WorkflowRun
	Step      contracts.WorkflowStepState
	Duplicate bool
}

type WorkflowStepCompleteInput struct {
	WorkspaceID    string
	RunID          string
	StepID         string
	Attempt        int
	OwnerID        string
	Fence          uint64
	TaskOwnerID    string
	TaskFence      uint64
	Actor          contracts.ActorRef
	RequestID      string
	IdempotencyKey string
	CausationID    string
	CorrelationID  string
	Result         contracts.WorkflowStepResult
}

type WorkflowStepCompleteResult struct {
	Run       contracts.WorkflowRun
	Step      contracts.WorkflowStepState
	Duplicate bool
}

type WorkflowReplayResult struct {
	Run             contracts.WorkflowRun
	ReplayHash      string
	TransitionCount int
	Verified        bool
}

func NewWorkflowStore(pool *pgxpool.Pool, events *EventStore, operations *OperationStore) *WorkflowStore {
	if events == nil {
		events = NewEventStore(pool)
	}
	if operations == nil {
		operations = NewOperationStore(pool, events)
	}
	return &WorkflowStore{pool: pool, events: events, operations: operations}
}

// SetFailureHook provides deterministic transaction-failure injection for
// tests. The hook is never set by production composition code.
func (s *WorkflowStore) SetFailureHook(hook func(string) error) {
	if s != nil {
		s.failureHook = hook
	}
}

func (s *WorkflowStore) fail(point string) error {
	if s != nil && s.failureHook != nil {
		return s.failureHook(point)
	}
	return nil
}

// Create atomically creates the generic operation, workflow projection, step
// projections, and the initial workflow event. A duplicate request returns
// the existing run only when its operation and budget hashes match.
func (s *WorkflowStore) Create(ctx context.Context, input WorkflowCreateInput) (WorkflowCreateResult, error) {
	if s == nil || s.pool == nil || s.events == nil || s.operations == nil {
		return WorkflowCreateResult{}, errors.New("workflow store is not configured")
	}
	if err := input.Budget.Normalize(); err != nil {
		return WorkflowCreateResult{}, fmt.Errorf("normalize workflow budget: %w", err)
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, input.Operation.Request.WorkspaceID)
	if err != nil {
		return WorkflowCreateResult{}, fmt.Errorf("begin workflow create: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := s.createTx(ctx, tx, input)
	if err != nil {
		return WorkflowCreateResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return WorkflowCreateResult{}, fmt.Errorf("commit workflow create: %w", err)
	}
	return result, nil
}

func (s *WorkflowStore) createTx(ctx context.Context, tx pgx.Tx, input WorkflowCreateInput) (WorkflowCreateResult, error) {
	created, err := s.operations.CreateTx(ctx, tx, input.Operation)
	if err != nil {
		return WorkflowCreateResult{}, err
	}
	operation := created.Operation
	if operation.Plan == nil {
		return WorkflowCreateResult{}, errors.New("workflow operation plan is required")
	}
	budgetHash, err := workflowHash(input.Budget)
	if err != nil {
		return WorkflowCreateResult{}, err
	}
	if existing, readErr := readWorkflowByOperation(ctx, tx, operation.WorkspaceID, operation.ID, true); readErr == nil {
		if existing.Run.Budget != input.Budget || existing.Run.PlanHash != operation.PlanHash {
			return WorkflowCreateResult{}, ErrWorkflowIdempotency
		}
		return existing, nil
	} else if !errors.Is(readErr, pgx.ErrNoRows) {
		return WorkflowCreateResult{}, readErr
	}
	run := initialWorkflowRun(operation, input.Budget)
	runHash := run.StableHash()
	if runHash == "" {
		return WorkflowCreateResult{}, errors.New("workflow initial state hash is empty")
	}
	run.StateHash = runHash
	actorJSON, _ := json.Marshal(run.Actor)
	taskJSON := workflowNullableJSON(run.Task)
	sessionJSON := workflowNullableJSON(run.Session)
	budgetJSON, _ := json.Marshal(run.Budget)
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.workflow_runs(
			workspace_id,run_id,operation_id,operation_hash,plan_hash,schema_version,
			actor,task_ref,session_ref,status,budget,state_hash,output_bytes,tokens,cost_micros
		) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9::jsonb,$10,$11::jsonb,$12,$13,$14,$15)`,
		run.WorkspaceID, run.ID, operation.ID, operation.OperationHash, run.PlanHash, run.SchemaVersion,
		actorJSON, taskJSON, sessionJSON, run.Status, budgetJSON, run.StateHash, run.OutputBytes, run.Tokens, run.CostMicros); err != nil {
		return WorkflowCreateResult{}, fmt.Errorf("insert workflow run: %w", err)
	}
	for _, step := range run.Steps {
		stepHash := workflowStepHash(step)
		if _, err := tx.Exec(ctx, `
			INSERT INTO fornix.workflow_step_states(
				workspace_id,run_id,step_id,ordinal,kind,status,attempt,idempotency_key,
				output_hash,evidence,artifacts,effect,wait,failure,state_version,state_hash
			) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12::jsonb,$13::jsonb,$14::jsonb,$15,$16)`,
			step.WorkspaceID, step.RunID, step.StepID, step.Ordinal, step.Kind, step.Status, step.Attempt,
			step.IdempotencyKey, step.OutputHash, mustJSON(step.Evidence), mustJSON(step.Artifacts), workflowNullableJSONValue(step.Effect),
			workflowNullableJSONValue(step.Wait), workflowNullableJSONValue(step.Failure), step.StateVersion, stepHash); err != nil {
			return WorkflowCreateResult{}, fmt.Errorf("insert workflow step %s: %w", step.StepID, err)
		}
	}
	commandHash, err := workflowHash(struct {
		OperationHash string `json:"operation_hash"`
		PlanHash      string `json:"plan_hash"`
		BudgetHash    string `json:"budget_hash"`
	}{operation.OperationHash, operation.PlanHash, budgetHash})
	if err != nil {
		return WorkflowCreateResult{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.workflow_idempotency(workspace_id,run_id,step_id,idempotency_key,command_hash,outcome_hash) VALUES($1,$2,'',$3,$4,$5)`, run.WorkspaceID, run.ID, input.Operation.Request.IdempotencyKey, commandHash, run.StateHash); err != nil {
		return WorkflowCreateResult{}, fmt.Errorf("insert workflow create idempotency: %w", err)
	}
	event, err := workflowEvent(run, "workflow.created", "", 0, run.StateHash, input.Operation.Request.IdempotencyKey)
	if err != nil {
		return WorkflowCreateResult{}, err
	}
	appended, err := s.events.AppendTx(ctx, tx, event)
	if err != nil {
		return WorkflowCreateResult{}, fmt.Errorf("append workflow create event: %w", err)
	}
	if err := s.fail("workflow_created"); err != nil {
		return WorkflowCreateResult{}, err
	}
	stored, err := readWorkflowByOperation(ctx, tx, run.WorkspaceID, run.Operation.ID, false)
	if err != nil {
		return WorkflowCreateResult{}, err
	}
	return WorkflowCreateResult{Run: stored.Run, Operation: operation, Event: appended.Event, Duplicate: false}, nil
}

func (s *WorkflowStore) Get(ctx context.Context, workspaceID, runID string) (contracts.WorkflowRun, error) {
	if s == nil || s.pool == nil {
		return contracts.WorkflowRun{}, errors.New("workflow store is not configured")
	}
	value, err := readWorkflowByRun(ctx, s.pool, strings.TrimSpace(workspaceID), strings.TrimSpace(runID), false)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.WorkflowRun{}, ErrWorkflowNotFound
	}
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	return value.Run, nil
}

func (s *WorkflowStore) AcquireLease(ctx context.Context, workspaceID, runID, ownerID string, ttl time.Duration) (WorkflowLeaseResult, error) {
	return s.operations.AcquireLease(ctx, workspaceID, runID, ownerID, ttl)
}

func (s *WorkflowStore) RenewLease(ctx context.Context, lease WorkflowLease, ttl time.Duration) (WorkflowLease, error) {
	return s.operations.RenewLease(ctx, lease, ttl)
}

func (s *WorkflowStore) ReleaseLease(ctx context.Context, lease WorkflowLease) error {
	return s.operations.ReleaseLease(ctx, lease)
}

func (s *WorkflowStore) StartStep(ctx context.Context, input WorkflowStepStartInput) (WorkflowStepStartResult, error) {
	if s == nil || s.pool == nil || s.operations == nil || s.events == nil {
		return WorkflowStepStartResult{}, errors.New("workflow store is not configured")
	}
	if err := normalizeWorkflowCommand(input.WorkspaceID, input.RunID, input.StepID, input.OwnerID, input.Fence, input.IdempotencyKey); err != nil {
		return WorkflowStepStartResult{}, err
	}
	if input.RequestID == "" {
		input.RequestID = contracts.NewID("workflow-start")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, input.WorkspaceID)
	if err != nil {
		return WorkflowStepStartResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	operation, err := readOperationByID(ctx, tx, input.WorkspaceID, input.RunID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return WorkflowStepStartResult{}, ErrWorkflowNotFound
	}
	if err != nil {
		return WorkflowStepStartResult{}, err
	}
	if err := s.validateWorkflowLeaseAndTask(ctx, tx, operation, input.OwnerID, input.Fence, input.TaskOwnerID, input.TaskFence); err != nil {
		return WorkflowStepStartResult{}, err
	}
	run, err := readWorkflowForOperation(ctx, tx, operation, true)
	if err != nil {
		return WorkflowStepStartResult{}, err
	}
	commandHash, err := workflowHash(struct {
		WorkspaceID string `json:"workspace_id"`
		RunID       string `json:"run_id"`
		StepID      string `json:"step_id"`
	}{input.WorkspaceID, input.RunID, input.StepID})
	if err != nil {
		return WorkflowStepStartResult{}, err
	}
	duplicate, err := reserveWorkflowCommand(ctx, tx, input.WorkspaceID, run.ID, input.StepID, input.IdempotencyKey, commandHash)
	if err != nil {
		return WorkflowStepStartResult{}, err
	}
	if duplicate {
		return workflowStartDuplicate(run, input.StepID)
	}
	stepIndex, step, err := findWorkflowStep(run, input.StepID)
	if err != nil {
		return WorkflowStepStartResult{}, err
	}
	if step.Status == contracts.WorkflowStepAwaitingRetry && step.NextRetryAt != nil {
		// Queue selection is not the final boundary: callers can acquire a
		// workflow lease directly, so recheck the durable deadline with the
		// same database clock before reserving this attempt.
		var due bool
		if err := tx.QueryRow(ctx, `SELECT $1::timestamptz <= clock_timestamp()`, *step.NextRetryAt).Scan(&due); err != nil {
			return WorkflowStepStartResult{}, fmt.Errorf("check workflow retry deadline: %w", err)
		}
		if !due {
			return WorkflowStepStartResult{}, ErrWorkflowRetryNotDue
		}
	}
	if contracts.IsTerminalOperationStatus(operation.Status) || run.Status == contracts.WorkflowStatusCancelled || run.Status == contracts.WorkflowStatusSucceeded || run.Status == contracts.WorkflowStatusFailed || run.Status == contracts.WorkflowStatusDeadLetter {
		return WorkflowStepStartResult{}, ErrWorkflowTerminal
	}
	if !workflowWithinWallBudget(run) {
		return WorkflowStepStartResult{}, ErrWorkflowBudget
	}
	if step.Status == contracts.WorkflowStepRunning {
		return WorkflowStepStartResult{}, ErrWorkflowStepAlreadyStarted
	}
	if step.Status != contracts.WorkflowStepPlanned && step.Status != contracts.WorkflowStepReady && step.Status != contracts.WorkflowStepAwaitingRetry && step.Status != contracts.WorkflowStepRecoveryRequired {
		return WorkflowStepStartResult{}, ErrWorkflowStepNotRunnable
	}
	if !workflowDependenciesComplete(run, stepIndex) {
		return WorkflowStepStartResult{}, ErrWorkflowStepNotRunnable
	}
	next := cloneWorkflowRun(run)
	nextStep := &next.Steps[stepIndex]
	nextStep.Status = contracts.WorkflowStepRunning
	nextStep.Attempt++
	nextStep.IdempotencyKey = input.IdempotencyKey
	nextStep.Wait = nil
	nextStep.Failure = nil
	nextStep.NextRetryAt = nil
	nextStep.StateVersion = run.StateVersion + 1
	next.Status = contracts.WorkflowStatusRunning
	next.Wait = nil
	next.Failure = nil
	next.StateVersion++
	next.UpdatedAt = time.Now().UTC()
	next.StateHash = next.StableHash()
	if err := s.finalizeWorkflowTransition(ctx, tx, operation, run, next, input.StepID, step.Status, nextStep.Status, input.OwnerID, input.Fence, input.TaskOwnerID, input.TaskFence, input.Actor, input.RequestID, input.IdempotencyKey, input.CausationID, input.CorrelationID, commandHash, nil); err != nil {
		return WorkflowStepStartResult{}, err
	}
	if _, err := s.advanceOperationForWorkflow(ctx, tx, operation, workflowOperationStatus(next.Status), next.StateHash, input, "start", nil, nil); err != nil {
		return WorkflowStepStartResult{}, err
	}
	if err := s.fail("workflow_step_started"); err != nil {
		return WorkflowStepStartResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return WorkflowStepStartResult{}, err
	}
	return WorkflowStepStartResult{Run: next, Step: next.Steps[stepIndex]}, nil
}

func (s *WorkflowStore) CompleteStep(ctx context.Context, input WorkflowStepCompleteInput) (WorkflowStepCompleteResult, error) {
	if s == nil || s.pool == nil || s.operations == nil || s.events == nil {
		return WorkflowStepCompleteResult{}, errors.New("workflow store is not configured")
	}
	if err := normalizeWorkflowCommand(input.WorkspaceID, input.RunID, input.StepID, input.OwnerID, input.Fence, input.IdempotencyKey); err != nil {
		return WorkflowStepCompleteResult{}, err
	}
	if input.Attempt < 1 {
		return WorkflowStepCompleteResult{}, errors.New("workflow completion attempt is required")
	}
	if err := input.Result.Normalize(input.WorkspaceID); err != nil {
		return WorkflowStepCompleteResult{}, err
	}
	result := input.Result
	if input.RequestID == "" {
		input.RequestID = contracts.NewID("workflow-complete")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, input.WorkspaceID)
	if err != nil {
		return WorkflowStepCompleteResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	operation, err := readOperationByID(ctx, tx, input.WorkspaceID, input.RunID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return WorkflowStepCompleteResult{}, ErrWorkflowNotFound
	}
	if err != nil {
		return WorkflowStepCompleteResult{}, err
	}
	if err := s.validateWorkflowLeaseAndTask(ctx, tx, operation, input.OwnerID, input.Fence, input.TaskOwnerID, input.TaskFence); err != nil {
		return WorkflowStepCompleteResult{}, err
	}
	run, err := readWorkflowForOperation(ctx, tx, operation, true)
	if err != nil {
		return WorkflowStepCompleteResult{}, err
	}
	if !workflowWithinWallBudget(run) {
		return WorkflowStepCompleteResult{}, ErrWorkflowBudget
	}
	stepIndex, step, err := findWorkflowStep(run, input.StepID)
	if err != nil {
		return WorkflowStepCompleteResult{}, err
	}
	commandHash, err := workflowHash(struct {
		StepID  string                       `json:"step_id"`
		Attempt int                          `json:"attempt"`
		Result  contracts.WorkflowStepResult `json:"result"`
	}{input.StepID, input.Attempt, input.Result})
	if err != nil {
		return WorkflowStepCompleteResult{}, err
	}
	duplicate, err := reserveWorkflowCommand(ctx, tx, input.WorkspaceID, run.ID, input.StepID, input.IdempotencyKey, commandHash)
	if err != nil {
		return WorkflowStepCompleteResult{}, err
	}
	if duplicate {
		return workflowCompleteDuplicate(run, input.StepID)
	}
	resumable := step.Status == contracts.WorkflowStepAwaitingApproval || step.Status == contracts.WorkflowStepAwaitingHuman || step.Status == contracts.WorkflowStepAwaitingCallback || step.Status == contracts.WorkflowStepAwaitingRetry || step.Status == contracts.WorkflowStepAwaitingExternal || step.Status == contracts.WorkflowStepRecoveryRequired
	if (step.Status != contracts.WorkflowStepRunning && !resumable) || step.Attempt != input.Attempt {
		return WorkflowStepCompleteResult{}, ErrWorkflowTransition
	}
	if result.Status == contracts.WorkflowStepAwaitingRetry && step.Attempt >= run.Budget.MaxRetries+1 {
		result.Status = contracts.WorkflowStepFailed
		result.Wait = nil
		result.Failure = &contracts.WorkflowFailure{Code: "retry_budget_exhausted", Retryable: false, Attempt: step.Attempt}
	}
	next := cloneWorkflowRun(run)
	nextStep := &next.Steps[stepIndex]
	nextStep.Status = result.Status
	nextStep.OutputHash = result.OutputHash
	nextStep.Evidence = append([]contracts.OperationEvidenceRef(nil), result.Evidence...)
	nextStep.Artifacts = append([]contracts.ArtifactRef(nil), result.Artifacts...)
	nextStep.Effect = result.Effect
	nextStep.Wait = result.Wait
	nextStep.Failure = result.Failure
	nextStep.NextRetryAt = nil
	if result.Wait != nil {
		nextStep.NextRetryAt = result.Wait.ExpiresAt
	}
	nextStep.StateVersion = run.StateVersion + 1
	next.OutputBytes += result.OutputBytes
	next.Tokens += result.Tokens
	next.CostMicros += result.CostMicros
	if next.OutputBytes > next.Budget.MaxOutputBytes || next.Tokens > next.Budget.MaxTokens || (next.Budget.MaxCostMicros > 0 && next.CostMicros > next.Budget.MaxCostMicros) {
		return WorkflowStepCompleteResult{}, ErrWorkflowBudget
	}
	next.Status = workflowStatusAfterStep(next, result.Status)
	next.Wait = result.Wait
	next.Failure = result.Failure
	if waitStatus(next.Status) {
		next.Wait, next.Failure = workflowWaitDetailsForStatus(next, next.Status)
	}
	if next.Status == contracts.WorkflowStatusSucceeded {
		next.TerminalReason = "all_steps_succeeded"
	} else if result.Failure != nil {
		next.TerminalReason = result.Failure.Code
	}
	next.StateVersion++
	next.UpdatedAt = time.Now().UTC()
	next.StateHash = next.StableHash()
	if err := s.finalizeWorkflowTransition(ctx, tx, operation, run, next, input.StepID, step.Status, nextStep.Status, input.OwnerID, input.Fence, input.TaskOwnerID, input.TaskFence, input.Actor, input.RequestID, input.IdempotencyKey, input.CausationID, input.CorrelationID, commandHash, result.Failure); err != nil {
		return WorkflowStepCompleteResult{}, err
	}
	var retryDeadline *time.Time
	if next.Status == contracts.WorkflowStatusAwaitingRetry {
		retryDeadline = workflowRetryDeadline(next)
	}
	if _, err := s.advanceOperationForWorkflow(ctx, tx, operation, workflowOperationStatus(next.Status), next.StateHash, input, "complete", retryDeadline, result.Failure); err != nil {
		return WorkflowStepCompleteResult{}, err
	}
	if err := s.fail("workflow_step_completed"); err != nil {
		return WorkflowStepCompleteResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return WorkflowStepCompleteResult{}, err
	}
	return WorkflowStepCompleteResult{Run: next, Step: next.Steps[stepIndex]}, nil
}

func (s *WorkflowStore) Cancel(ctx context.Context, workspaceID, runID, ownerID string, fence uint64, actor contracts.ActorRef, taskOwnerID string, taskFence uint64, idempotencyKey string) (contracts.WorkflowRun, error) {
	if err := normalizeWorkflowCommand(workspaceID, runID, runID, ownerID, fence, idempotencyKey); err != nil {
		return contracts.WorkflowRun{}, err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	operation, err := readOperationByID(ctx, tx, workspaceID, runID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.WorkflowRun{}, ErrWorkflowNotFound
	}
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	if err := s.validateWorkflowLeaseAndTask(ctx, tx, operation, ownerID, fence, taskOwnerID, taskFence); err != nil {
		return contracts.WorkflowRun{}, err
	}
	run, err := readWorkflowForOperation(ctx, tx, operation, true)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	commandHash, _ := workflowHash(struct {
		RunID  string `json:"run_id"`
		Status string `json:"status"`
	}{runID, contracts.WorkflowStatusCancelled})
	duplicate, err := reserveWorkflowCommand(ctx, tx, workspaceID, run.ID, "", idempotencyKey, commandHash)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	if duplicate {
		return run, nil
	}
	if run.Status == contracts.WorkflowStatusCancelled {
		return run, nil
	}
	if run.Status == contracts.WorkflowStatusSucceeded || run.Status == contracts.WorkflowStatusFailed || run.Status == contracts.WorkflowStatusDeadLetter {
		return contracts.WorkflowRun{}, ErrWorkflowTerminal
	}
	next := cloneWorkflowRun(run)
	for i := range next.Steps {
		if next.Steps[i].Status != contracts.WorkflowStepSucceeded && next.Steps[i].Status != contracts.WorkflowStepFailed && next.Steps[i].Status != contracts.WorkflowStepCancelled && next.Steps[i].Status != contracts.WorkflowStepCompensated {
			next.Steps[i].Status = contracts.WorkflowStepCancelled
			next.Steps[i].StateVersion = run.StateVersion + 1
		}
	}
	next.Status, next.TerminalReason, next.StateVersion, next.UpdatedAt = contracts.WorkflowStatusCancelled, "cancelled", run.StateVersion+1, time.Now().UTC()
	next.StateHash = next.StableHash()
	if err := s.finalizeWorkflowTransition(ctx, tx, operation, run, next, "", "", contracts.WorkflowStepCancelled, ownerID, fence, taskOwnerID, taskFence, actor, contracts.NewID("workflow-cancel"), idempotencyKey, "", "", commandHash, &contracts.WorkflowFailure{Code: "cancelled", Message: "workflow cancelled"}); err != nil {
		return contracts.WorkflowRun{}, err
	}
	if _, err := s.advanceOperationForWorkflow(ctx, tx, operation, contracts.OperationStatusCancelled, next.StateHash, WorkflowStepStartInput{WorkspaceID: workspaceID, RunID: runID, OwnerID: ownerID, Fence: fence, TaskOwnerID: taskOwnerID, TaskFence: taskFence, Actor: actor, RequestID: contracts.NewID("workflow-cancel-request"), IdempotencyKey: idempotencyKey}, "cancel", nil, &contracts.WorkflowFailure{Code: "cancelled", Message: "workflow cancelled"}); err != nil {
		return contracts.WorkflowRun{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.WorkflowRun{}, err
	}
	return next, nil
}

// RecoverRunning turns a step left in running state by a crashed worker into
// an explicit recovery state. It never guesses that an external effect did or
// did not happen; the executor must decide whether to verify or compensate.
func (s *WorkflowStore) RecoverRunning(ctx context.Context, workspaceID, runID, stepID, ownerID string, fence uint64, actor contracts.ActorRef, taskOwnerID string, taskFence uint64, idempotencyKey string) (contracts.WorkflowRun, error) {
	run, err := s.Get(ctx, workspaceID, runID)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	for _, step := range run.Steps {
		if step.StepID == stepID && step.Status == contracts.WorkflowStepRunning {
			result := contracts.WorkflowStepResult{Status: contracts.WorkflowStepRecoveryRequired, Failure: &contracts.WorkflowFailure{Code: "worker_crash", Retryable: true, External: step.Effect != nil}}
			completed, completeErr := s.CompleteStep(ctx, WorkflowStepCompleteInput{WorkspaceID: workspaceID, RunID: runID, StepID: stepID, Attempt: step.Attempt, OwnerID: ownerID, Fence: fence, TaskOwnerID: taskOwnerID, TaskFence: taskFence, Actor: actor, IdempotencyKey: idempotencyKey, Result: result})
			if completeErr != nil {
				return contracts.WorkflowRun{}, completeErr
			}
			return completed.Run, nil
		}
	}
	return run, ErrWorkflowStepNotFound
}

// RetryRecovery safely requeues a step left uncertain by a worker crash. The
// control plane permits automatic retry only for read-only or observation
// steps; effectful steps remain recovery_required until a verifier or operator
// supplies an explicit decision. The mutation is fenced and append-only.
func (s *WorkflowStore) RetryRecovery(ctx context.Context, input WorkflowStepStartInput) (contracts.WorkflowRun, error) {
	if s == nil || s.pool == nil || s.operations == nil || s.events == nil {
		return contracts.WorkflowRun{}, errors.New("workflow store is not configured")
	}
	if err := normalizeWorkflowCommand(input.WorkspaceID, input.RunID, input.StepID, input.OwnerID, input.Fence, input.IdempotencyKey); err != nil {
		return contracts.WorkflowRun{}, err
	}
	if input.RequestID == "" {
		input.RequestID = contracts.NewID("workflow-recovery")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, input.WorkspaceID)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	operation, err := readOperationByID(ctx, tx, input.WorkspaceID, input.RunID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.WorkflowRun{}, ErrWorkflowNotFound
	}
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	if err := s.validateWorkflowLeaseAndTask(ctx, tx, operation, input.OwnerID, input.Fence, input.TaskOwnerID, input.TaskFence); err != nil {
		return contracts.WorkflowRun{}, err
	}
	run, err := readWorkflowForOperation(ctx, tx, operation, true)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	stepIndex, step, err := findWorkflowStep(run, input.StepID)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	if step.Status != contracts.WorkflowStepRecoveryRequired {
		return run, ErrWorkflowTransition
	}
	var planStep contracts.OperationStep
	for _, candidate := range run.Plan.Steps {
		if candidate.ID == input.StepID {
			planStep = candidate
			break
		}
	}
	if planStep.ID == "" {
		return contracts.WorkflowRun{}, ErrWorkflowStepNotFound
	}
	if planStep.Effect != contracts.EffectClassReadOnly && planStep.Effect != contracts.EffectClassObservation {
		return run, ErrWorkflowTransition
	}
	commandHash, err := workflowHash(struct {
		StepID       string `json:"step_id"`
		StateVersion int64  `json:"state_version"`
		Recovery     bool   `json:"recovery"`
	}{input.StepID, run.StateVersion, true})
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	duplicate, err := reserveWorkflowCommand(ctx, tx, input.WorkspaceID, run.ID, input.StepID, input.IdempotencyKey, commandHash)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	if duplicate {
		return run, nil
	}
	next := cloneWorkflowRun(run)
	nextStep := &next.Steps[stepIndex]
	nextStep.Status, nextStep.Failure, nextStep.Wait, nextStep.NextRetryAt = contracts.WorkflowStepPlanned, nil, nil, nil
	nextStep.IdempotencyKey = ""
	nextStep.StateVersion = run.StateVersion + 1
	next.StateVersion = run.StateVersion + 1
	next.Status, next.Wait, next.Failure, next.TerminalReason = contracts.WorkflowStatusRunning, nil, nil, "recovery_retry"
	next.UpdatedAt = time.Now().UTC()
	next.StateHash = next.StableHash()
	if err := s.finalizeWorkflowTransition(ctx, tx, operation, run, next, input.StepID, step.Status, nextStep.Status, input.OwnerID, input.Fence, input.TaskOwnerID, input.TaskFence, input.Actor, input.RequestID, input.IdempotencyKey, input.CausationID, input.CorrelationID, commandHash, nil); err != nil {
		return contracts.WorkflowRun{}, err
	}
	if _, err := s.advanceOperationForWorkflow(ctx, tx, operation, contracts.OperationStatusRunning, next.StateHash, input, "recovery_retry", nil, nil); err != nil {
		return contracts.WorkflowRun{}, err
	}
	if err := s.fail("workflow_recovery_retried"); err != nil {
		return contracts.WorkflowRun{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.WorkflowRun{}, err
	}
	return next, nil
}

func (s *WorkflowStore) Replay(ctx context.Context, workspaceID, runID string, fromVersion int64, limit int) (WorkflowReplayResult, error) {
	if s == nil || s.pool == nil {
		return WorkflowReplayResult{}, errors.New("workflow store is not configured")
	}
	run, err := s.Get(ctx, workspaceID, runID)
	if err != nil {
		return WorkflowReplayResult{}, err
	}
	if fromVersion < 0 || fromVersion > run.StateVersion {
		return WorkflowReplayResult{}, ErrWorkflowReplay
	}
	if limit <= 0 || limit > maxWorkflowReplayLimit {
		limit = maxWorkflowReplayLimit
	}
	operation, err := s.operations.Get(ctx, workspaceID, run.Operation.ID)
	if err != nil {
		return WorkflowReplayResult{}, err
	}
	initial := initialWorkflowRun(operation, run.Budget)
	initial.StateHash = initial.StableHash()
	previousHash := initial.StateHash
	previousVersion := int64(0)
	transitions := make([]string, 0)
	count := 0
	checkpointSeen := fromVersion == 0
	current := initial
	err = workspaceQueryRows(ctx, s.pool, workspaceID, `SELECT version,step_id,from_run_status,to_run_status,from_step_status,to_step_status,command_hash,previous_state_hash,state_hash,state,event_sequence FROM fornix.workflow_transitions WHERE workspace_id=$1 AND run_id=$2 ORDER BY version ASC LIMIT $3`, []any{workspaceID, runID, limit}, func(rows pgx.Rows) error {
		for rows.Next() {
			var version int64
			var stepID, fromStatus, toStatus, fromStepStatus, toStepStatus, commandHash, storedPrevious, stateHash string
			var stateJSON []byte
			var eventSequence *int64
			if err := rows.Scan(&version, &stepID, &fromStatus, &toStatus, &fromStepStatus, &toStepStatus, &commandHash, &storedPrevious, &stateHash, &stateJSON, &eventSequence); err != nil {
				return err
			}
			if version != previousVersion+1 || storedPrevious != previousHash || eventSequence == nil || fromStatus != current.Status {
				return fmt.Errorf("%w: broken chain at version %d", ErrWorkflowReplay, version)
			}
			var checkpoint contracts.WorkflowCheckpoint
			if err := json.Unmarshal(stateJSON, &checkpoint); err != nil || checkpoint.Version != version || checkpoint.PreviousHash != storedPrevious || checkpoint.StateHash != stateHash {
				return ErrWorkflowReplay
			}
			current.Status, current.Steps = checkpoint.Status, checkpoint.StepStates
			current.Wait, current.Failure, current.TerminalReason = checkpoint.Wait, checkpoint.Failure, checkpoint.TerminalReason
			current.OutputBytes, current.Tokens, current.CostMicros = checkpoint.OutputBytes, checkpoint.Tokens, checkpoint.CostMicros
			current.StateVersion, current.StateHash = version, stateHash
			if current.StableHash() != stateHash || toStatus != current.Status {
				return ErrWorkflowReplay
			}
			previousVersion, previousHash = version, stateHash
			if version == fromVersion {
				checkpointSeen = true
			}
			if version > fromVersion {
				count++
				transitions = append(transitions, fmt.Sprintf("%d:%s:%s", version, commandHash, stateHash))
			}
			_ = stepID
			_ = fromStepStatus
			_ = toStepStatus
		}
		return nil
	})
	if err != nil {
		return WorkflowReplayResult{}, err
	}
	if !checkpointSeen || previousVersion != run.StateVersion || previousHash != run.StateHash || current.StableHash() != run.StateHash {
		return WorkflowReplayResult{}, ErrWorkflowReplay
	}
	replayHash, err := workflowHash(struct {
		WorkspaceID string   `json:"workspace_id"`
		RunID       string   `json:"run_id"`
		FromVersion int64    `json:"from_version"`
		States      []string `json:"states"`
	}{workspaceID, runID, fromVersion, transitions})
	if err != nil {
		return WorkflowReplayResult{}, err
	}
	return WorkflowReplayResult{Run: run, ReplayHash: replayHash, TransitionCount: count, Verified: true}, nil
}

func (s *WorkflowStore) validateWorkflowLeaseAndTask(ctx context.Context, tx pgx.Tx, operation Operation, owner string, fence uint64, taskOwner string, taskFence uint64) error {
	if _, err := s.operations.validateLease(ctx, tx, OperationLease{WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, OwnerID: owner, Fence: fence}); err != nil {
		return err
	}
	if err := validateOperationTaskFence(operation, taskOwner, taskFence); err != nil {
		return err
	}
	if operation.Request.Task != nil {
		if err := validateTaskFenceForOperationTx(ctx, tx, operation.Request.Task, taskOwner, taskFence); err != nil {
			return err
		}
	}
	return nil
}

func (s *WorkflowStore) finalizeWorkflowTransition(ctx context.Context, tx pgx.Tx, operation Operation, previous, next contracts.WorkflowRun, stepID, fromStep, toStep, owner string, fence uint64, taskOwner string, taskFence uint64, actor contracts.ActorRef, requestID, idempotencyKey, causationID, correlationID, commandHash string, failure *contracts.WorkflowFailure) error {
	if next.StateHash == "" {
		next.StateHash = next.StableHash()
	}
	checkpoint := contracts.WorkflowCheckpoint{WorkspaceID: next.WorkspaceID, RunID: next.ID, Version: next.StateVersion, PreviousHash: previous.StateHash, StateHash: next.StateHash, Status: next.Status, StepStates: next.Steps, Wait: next.Wait, Failure: next.Failure, TerminalReason: next.TerminalReason, OutputBytes: next.OutputBytes, Tokens: next.Tokens, CostMicros: next.CostMicros}
	stateJSON, err := json.Marshal(checkpoint)
	if err != nil {
		return err
	}
	if len(stateJSON) > 512<<10 {
		return ErrWorkflowBudget
	}
	actor = workflowActor(actor, operation.Request.Actor)
	if actor.WorkspaceID != operation.WorkspaceID || actor.ID != operation.Request.Actor.ID || actor.Kind != operation.Request.Actor.Kind || actor.Name != operation.Request.Actor.Name {
		return ErrWorkflowWorkspace
	}
	event, err := workflowEvent(next, "workflow.transition", stepID, next.StateVersion, next.StateHash, idempotencyKey)
	if err != nil {
		return err
	}
	event.Actor, event.CausationID, event.CorrelationID = actor, causationID, correlationID
	appended, err := s.events.AppendTx(ctx, tx, event)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.workflow_transitions(workspace_id,run_id,version,step_id,from_run_status,to_run_status,from_step_status,to_step_status,request_id,idempotency_key,command_hash,actor,owner_id,fence,task_owner_id,task_fence,previous_state_hash,state_hash,state,event_sequence) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12::jsonb,$13,$14,$15,$16,$17,$18,$19::jsonb,$20)`, next.WorkspaceID, next.ID, next.StateVersion, stepID, previous.Status, next.Status, fromStep, toStep, requestID, idempotencyKey, commandHash, mustJSON(actor), owner, int64(fence), taskOwner, int64(taskFence), previous.StateHash, next.StateHash, stateJSON, appended.Event.Sequence); err != nil {
		return err
	}
	updatedRun, err := tx.Exec(ctx, `UPDATE fornix.workflow_runs SET status=$3,wait=$4::jsonb,failure=$5::jsonb,terminal_reason=$6,state_version=$7,state_hash=$8,output_bytes=$9,tokens=$10,cost_micros=$11,updated_at=clock_timestamp() WHERE workspace_id=$1 AND run_id=$2 AND state_version=$12`, next.WorkspaceID, next.ID, next.Status, workflowNullableJSONValue(next.Wait), workflowNullableJSONValue(next.Failure), next.TerminalReason, next.StateVersion, next.StateHash, next.OutputBytes, next.Tokens, next.CostMicros, previous.StateVersion)
	if err != nil {
		return err
	}
	if updatedRun.RowsAffected() != 1 {
		return ErrWorkflowTransition
	}
	stepIndex, _, err := findWorkflowStep(next, stepID)
	if stepID != "" && err == nil {
		step := next.Steps[stepIndex]
		updatedStep, err := tx.Exec(ctx, `UPDATE fornix.workflow_step_states SET status=$4,attempt=$5,idempotency_key=$6,output_hash=$7,evidence=$8::jsonb,artifacts=$9::jsonb,effect=$10::jsonb,wait=$11::jsonb,failure=$12::jsonb,next_retry_at=$13,state_version=$14,state_hash=$15,updated_at=clock_timestamp() WHERE workspace_id=$1 AND run_id=$2 AND step_id=$3`, next.WorkspaceID, next.ID, step.StepID, step.Status, step.Attempt, step.IdempotencyKey, step.OutputHash, mustJSON(step.Evidence), mustJSON(step.Artifacts), workflowNullableJSONValue(step.Effect), workflowNullableJSONValue(step.Wait), workflowNullableJSONValue(step.Failure), step.NextRetryAt, step.StateVersion, workflowStepHash(step))
		if err != nil {
			return err
		}
		if updatedStep.RowsAffected() != 1 {
			return ErrWorkflowTransition
		}
	} else if stepID == "" {
		for _, step := range next.Steps {
			if _, err := tx.Exec(ctx, `UPDATE fornix.workflow_step_states SET status=$4,state_version=$5,state_hash=$6,updated_at=clock_timestamp() WHERE workspace_id=$1 AND run_id=$2 AND step_id=$3`, next.WorkspaceID, next.ID, step.StepID, step.Status, step.StateVersion, workflowStepHash(step)); err != nil {
				return err
			}
		}
	}
	updatedCommand, err := tx.Exec(ctx, `UPDATE fornix.workflow_idempotency SET transition_version=$5,outcome_hash=$6 WHERE workspace_id=$1 AND run_id=$2 AND step_id=$3 AND idempotency_key=$4 AND command_hash=$7`, next.WorkspaceID, next.ID, stepID, idempotencyKey, next.StateVersion, next.StateHash, commandHash)
	if err != nil {
		return err
	}
	if updatedCommand.RowsAffected() != 1 {
		return ErrWorkflowIncomplete
	}
	_ = failure
	return nil
}

func (s *WorkflowStore) advanceOperationForWorkflow(ctx context.Context, tx pgx.Tx, operation Operation, target, resultHash string, input interface{}, phase string, retryDeadline *time.Time, workflowFailure *contracts.WorkflowFailure) (Operation, error) {
	var owner, taskOwner, correlation, causation, requestID, key string
	var fence, taskFence uint64
	var actor contracts.ActorRef
	switch value := input.(type) {
	case WorkflowStepStartInput:
		owner, fence, taskOwner, taskFence, actor, correlation, causation, requestID, key = value.OwnerID, value.Fence, value.TaskOwnerID, value.TaskFence, value.Actor, value.CorrelationID, value.CausationID, value.RequestID, value.IdempotencyKey
	case WorkflowStepCompleteInput:
		owner, fence, taskOwner, taskFence, actor, correlation, causation, requestID, key = value.OwnerID, value.Fence, value.TaskOwnerID, value.TaskFence, value.Actor, value.CorrelationID, value.CausationID, value.RequestID, value.IdempotencyKey
	default:
		return Operation{}, errors.New("unsupported workflow operation command")
	}
	actor = workflowActor(actor, operation.Request.Actor)
	for operation.Status != target {
		next, ok := nextOperationStatus(operation.Status, target)
		if !ok {
			return Operation{}, fmt.Errorf("%w: operation %s -> %s", ErrWorkflowTransition, operation.Status, target)
		}
		transitionKey := "workflow-op-" + workflowHashString(operation.ID + "\x00" + phase + "\x00" + key + "\x00" + next)[:48]
		var transitionDeadline *time.Time
		if next == contracts.OperationStatusAwaitingRetry {
			transitionDeadline = retryDeadline
		}
		transitionInput := OperationTransitionInput{WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, OwnerID: owner, Fence: fence, TaskOwnerID: taskOwner, TaskFence: taskFence, Actor: actor, RequestID: requestID, IdempotencyKey: transitionKey, CausationID: causation, CorrelationID: correlation, ToStatus: next, ResultHash: resultHash, Failure: operationFailureFromWorkflow(operation.WorkspaceID, workflowFailure), NextRetryAt: transitionDeadline}
		updated, err := s.operations.transitionTx(ctx, tx, transitionInput)
		if err != nil {
			return Operation{}, err
		}
		operation = updated.Operation
	}
	return operation, nil
}

// workflowRetryDeadline returns the earliest pending retry time because the
// operation queue represents a workflow with one not-before timestamp. A
// retry without an expiry retains the existing immediately-eligible behavior.
func workflowRetryDeadline(run contracts.WorkflowRun) *time.Time {
	var earliest *time.Time
	for _, step := range run.Steps {
		if step.Status != contracts.WorkflowStepAwaitingRetry {
			continue
		}
		if step.NextRetryAt == nil {
			return nil
		}
		if earliest == nil || step.NextRetryAt.Before(*earliest) {
			deadline := *step.NextRetryAt
			earliest = &deadline
		}
	}
	return earliest
}

func operationFailureFromWorkflow(workspaceID string, failure *contracts.WorkflowFailure) *contracts.OperationFailure {
	if failure == nil {
		return nil
	}
	code := strings.ToLower(strings.TrimSpace(failure.Code))
	switch code {
	case "timeout":
		code = contracts.OperationFailureTimeout
	case "cancelled":
		code = contracts.OperationFailureCancelled
	case "retry_budget_exhausted", "budget_exceeded":
		code = contracts.OperationFailureBudget
	case "workspace_isolation":
		code = contracts.OperationFailureWorkspace
	case "unauthorized":
		code = contracts.OperationFailureUnauthorized
	case "conflict":
		code = contracts.OperationFailureConflict
	case "external_uncertain", "worker_crash":
		code = contracts.OperationFailureExternalUncertain
	case "invalid_request":
		code = contracts.OperationFailureInvalidRequest
	default:
		code = contracts.OperationFailureAdapter
	}
	value := &contracts.OperationFailure{SchemaVersion: contracts.DomainNeutralSchemaVersion, WorkspaceID: workspaceID, Code: code, Retryable: failure.Retryable, Attempts: failure.Attempt}
	if failure.External {
		value.ExternalEffectStatus = contracts.ExternalVerificationUnknown
	}
	if failure.Message != "" {
		value.DetailHash = workflowHashString(failure.Message)
	}
	return value
}

func nextOperationStatus(current, target string) (string, bool) {
	if current == target {
		return target, true
	}
	if contracts.CanTransitionOperation(current, target) {
		return target, true
	}
	switch current {
	case contracts.OperationStatusCreated:
		return contracts.OperationStatusPlanned, true
	case contracts.OperationStatusPlanned:
		return contracts.OperationStatusAdmitted, true
	case contracts.OperationStatusAdmitted:
		if target == contracts.OperationStatusAwaitingApproval {
			return target, true
		}
		return contracts.OperationStatusRunning, true
	case contracts.OperationStatusAwaitingApproval, contracts.OperationStatusAwaitingRetry:
		return contracts.OperationStatusRunning, true
	case contracts.OperationStatusAwaitingExternal:
		return contracts.OperationStatusVerifying, true
	default:
		return "", false
	}
}

func workflowOperationStatus(status string) string {
	switch status {
	case contracts.WorkflowStatusCreated:
		return contracts.OperationStatusCreated
	case contracts.WorkflowStatusAwaitingApproval:
		return contracts.OperationStatusAwaitingApproval
	case contracts.WorkflowStatusAwaitingRetry:
		return contracts.OperationStatusAwaitingRetry
	case contracts.WorkflowStatusAwaitingCallback, contracts.WorkflowStatusAwaitingHuman, contracts.WorkflowStatusAwaitingExternal:
		return contracts.OperationStatusAwaitingExternal
	case contracts.WorkflowStatusRecoveryRequired:
		return contracts.OperationStatusRecoveryRequired
	case contracts.WorkflowStatusSucceeded:
		return contracts.OperationStatusSucceeded
	case contracts.WorkflowStatusFailed:
		return contracts.OperationStatusFailed
	case contracts.WorkflowStatusCancelled:
		return contracts.OperationStatusCancelled
	case contracts.WorkflowStatusDeadLetter:
		return contracts.OperationStatusDeadLetter
	default:
		return contracts.OperationStatusRunning
	}
}

func workflowStatusAfterStep(run contracts.WorkflowRun, stepStatus string) string {
	switch stepStatus {
	case contracts.WorkflowStepFailed:
		return contracts.WorkflowStatusFailed
	case contracts.WorkflowStepCancelled:
		return contracts.WorkflowStatusCancelled
	}
	// A run can contain several independent steps in different wait states.
	// Explicit approval/human/external waits and uncertain recovery take
	// precedence over timer retries so a due retry cannot bypass them.
	for _, waiting := range []struct{ stepStatus, runStatus string }{
		{contracts.WorkflowStepRecoveryRequired, contracts.WorkflowStatusRecoveryRequired},
		{contracts.WorkflowStepAwaitingApproval, contracts.WorkflowStatusAwaitingApproval},
		{contracts.WorkflowStepAwaitingHuman, contracts.WorkflowStatusAwaitingHuman},
		{contracts.WorkflowStepAwaitingCallback, contracts.WorkflowStatusAwaitingCallback},
		{contracts.WorkflowStepAwaitingExternal, contracts.WorkflowStatusAwaitingExternal},
		{contracts.WorkflowStepAwaitingRetry, contracts.WorkflowStatusAwaitingRetry},
	} {
		for _, step := range run.Steps {
			if step.Status == waiting.stepStatus {
				return waiting.runStatus
			}
		}
	}
	for _, step := range run.Steps {
		if step.Status != contracts.WorkflowStepSucceeded && step.Status != contracts.WorkflowStepCompensated {
			return contracts.WorkflowStatusRunning
		}
	}
	return contracts.WorkflowStatusSucceeded
}

func workflowWaitDetailsForStatus(run contracts.WorkflowRun, runStatus string) (*contracts.WorkflowWait, *contracts.WorkflowFailure) {
	var stepStatus string
	switch runStatus {
	case contracts.WorkflowStatusAwaitingApproval:
		stepStatus = contracts.WorkflowStepAwaitingApproval
	case contracts.WorkflowStatusAwaitingHuman:
		stepStatus = contracts.WorkflowStepAwaitingHuman
	case contracts.WorkflowStatusAwaitingCallback:
		stepStatus = contracts.WorkflowStepAwaitingCallback
	case contracts.WorkflowStatusAwaitingRetry:
		stepStatus = contracts.WorkflowStepAwaitingRetry
	case contracts.WorkflowStatusAwaitingExternal:
		stepStatus = contracts.WorkflowStepAwaitingExternal
	case contracts.WorkflowStatusRecoveryRequired:
		stepStatus = contracts.WorkflowStepRecoveryRequired
	default:
		return nil, nil
	}
	var selected *contracts.WorkflowStepState
	for i := range run.Steps {
		step := &run.Steps[i]
		if step.Status != stepStatus {
			continue
		}
		if selected == nil || stepStatus == contracts.WorkflowStepAwaitingRetry && workflowRetryWaitPrecedes(step, selected) {
			selected = step
		}
	}
	if selected == nil {
		return nil, nil
	}
	var wait *contracts.WorkflowWait
	if selected.Wait != nil {
		value := *selected.Wait
		wait = &value
	}
	var failure *contracts.WorkflowFailure
	if selected.Failure != nil {
		value := *selected.Failure
		failure = &value
	}
	return wait, failure
}

func workflowRetryWaitPrecedes(candidate, current *contracts.WorkflowStepState) bool {
	if candidate.NextRetryAt == nil {
		if current.NextRetryAt != nil {
			return true
		}
	} else if current.NextRetryAt != nil {
		if !candidate.NextRetryAt.Equal(*current.NextRetryAt) {
			return candidate.NextRetryAt.Before(*current.NextRetryAt)
		}
	} else {
		return false
	}
	if candidate.Ordinal != current.Ordinal {
		return candidate.Ordinal < current.Ordinal
	}
	return candidate.StepID < current.StepID
}

func waitStatus(status string) bool {
	switch status {
	case contracts.WorkflowStatusAwaitingApproval, contracts.WorkflowStatusAwaitingHuman, contracts.WorkflowStatusAwaitingCallback, contracts.WorkflowStatusAwaitingRetry, contracts.WorkflowStatusAwaitingExternal, contracts.WorkflowStatusRecoveryRequired:
		return true
	default:
		return false
	}
}

func initialWorkflowRun(operation Operation, budget contracts.WorkflowBudget) contracts.WorkflowRun {
	steps := make([]contracts.WorkflowStepState, 0, len(operation.Plan.Steps))
	for _, planStep := range operation.Plan.Steps {
		steps = append(steps, contracts.WorkflowStepState{WorkspaceID: operation.WorkspaceID, RunID: operation.ID, StepID: planStep.ID, Ordinal: planStep.Ordinal, Kind: planStep.Kind, Status: contracts.WorkflowStepPlanned})
	}
	now := time.Now().UTC()
	run := contracts.WorkflowRun{SchemaVersion: contracts.WorkflowSchemaVersion, WorkspaceID: operation.WorkspaceID, ID: operation.ID, Operation: contracts.OperationReference{SchemaVersion: contracts.DomainNeutralSchemaVersion, WorkspaceID: operation.WorkspaceID, ID: operation.ID, Hash: operation.OperationHash}, Plan: *operation.Plan, PlanHash: operation.PlanHash, Actor: operation.Request.Actor, Task: operation.Request.Task, Session: operation.Request.Session, Status: contracts.WorkflowStatusCreated, Budget: budget, Steps: steps, StateVersion: 0, CreatedAt: now, UpdatedAt: now}
	return run
}

func readWorkflowByOperation(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, workspaceID, operationID string, lock bool) (WorkflowCreateResult, error) {
	operation, err := readOperationByID(ctx, queryer, workspaceID, operationID, lock)
	if err != nil {
		return WorkflowCreateResult{}, err
	}
	value, err := readWorkflowForOperation(ctx, queryer, operation, lock)
	if err != nil {
		return WorkflowCreateResult{}, err
	}
	return WorkflowCreateResult{Run: value, Operation: operation, Duplicate: true}, nil
}

func readWorkflowByRun(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, workspaceID, runID string, lock bool) (WorkflowCreateResult, error) {
	query := `SELECT operation_id FROM fornix.workflow_runs WHERE workspace_id=$1 AND run_id=$2`
	if lock {
		query += " FOR UPDATE"
	}
	var operationID string
	if err := queryer.QueryRow(ctx, query, workspaceID, runID).Scan(&operationID); err != nil {
		return WorkflowCreateResult{}, err
	}
	operation, err := readOperationByID(ctx, queryer, workspaceID, operationID, lock)
	if err != nil {
		return WorkflowCreateResult{}, err
	}
	run, err := readWorkflowForOperation(ctx, queryer, operation, lock)
	if err != nil {
		return WorkflowCreateResult{}, err
	}
	return WorkflowCreateResult{Run: run, Operation: operation}, nil
}

func readWorkflowForOperation(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
	Query(context.Context, string, ...any) (pgx.Rows, error)
}, operation Operation, lock bool) (contracts.WorkflowRun, error) {
	query := `SELECT schema_version,actor,task_ref,session_ref,status,budget,wait,failure,terminal_reason,state_version,state_hash,output_bytes,tokens,cost_micros,created_at,updated_at FROM fornix.workflow_runs WHERE workspace_id=$1 AND run_id=$2`
	if lock {
		query += " FOR UPDATE"
	}
	var run contracts.WorkflowRun
	var actorJSON, taskJSON, sessionJSON, budgetJSON, waitJSON, failureJSON []byte
	if err := queryer.QueryRow(ctx, query, operation.WorkspaceID, operation.ID).Scan(&run.SchemaVersion, &actorJSON, &taskJSON, &sessionJSON, &run.Status, &budgetJSON, &waitJSON, &failureJSON, &run.TerminalReason, &run.StateVersion, &run.StateHash, &run.OutputBytes, &run.Tokens, &run.CostMicros, &run.CreatedAt, &run.UpdatedAt); err != nil {
		return contracts.WorkflowRun{}, err
	}
	if err := json.Unmarshal(actorJSON, &run.Actor); err != nil {
		return contracts.WorkflowRun{}, err
	}
	if len(taskJSON) > 0 && string(taskJSON) != "null" {
		run.Task = new(contracts.EntityRef)
		if err := json.Unmarshal(taskJSON, run.Task); err != nil {
			return contracts.WorkflowRun{}, err
		}
	}
	if len(sessionJSON) > 0 && string(sessionJSON) != "null" {
		run.Session = new(contracts.EntityRef)
		if err := json.Unmarshal(sessionJSON, run.Session); err != nil {
			return contracts.WorkflowRun{}, err
		}
	}
	if err := json.Unmarshal(budgetJSON, &run.Budget); err != nil {
		return contracts.WorkflowRun{}, err
	}
	if len(waitJSON) > 0 && string(waitJSON) != "null" {
		run.Wait = new(contracts.WorkflowWait)
		if err := json.Unmarshal(waitJSON, run.Wait); err != nil {
			return contracts.WorkflowRun{}, err
		}
	}
	if len(failureJSON) > 0 && string(failureJSON) != "null" {
		run.Failure = new(contracts.WorkflowFailure)
		if err := json.Unmarshal(failureJSON, run.Failure); err != nil {
			return contracts.WorkflowRun{}, err
		}
	}
	if operation.Plan == nil {
		return contracts.WorkflowRun{}, errors.New("workflow operation plan is missing")
	}
	run.WorkspaceID, run.ID = operation.WorkspaceID, operation.ID
	run.Operation = contracts.OperationReference{SchemaVersion: contracts.DomainNeutralSchemaVersion, WorkspaceID: operation.WorkspaceID, ID: operation.ID, Hash: operation.OperationHash}
	run.Plan, run.PlanHash = *operation.Plan, operation.PlanHash
	rows, err := queryer.Query(ctx, `SELECT workspace_id,run_id,step_id,ordinal,kind,status,attempt,idempotency_key,output_hash,evidence,artifacts,effect,wait,failure,next_retry_at,state_version,state_hash FROM fornix.workflow_step_states WHERE workspace_id=$1 AND run_id=$2 ORDER BY ordinal ASC`, operation.WorkspaceID, operation.ID)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	defer rows.Close()
	run.Steps = make([]contracts.WorkflowStepState, 0, len(operation.Plan.Steps))
	for rows.Next() {
		var step contracts.WorkflowStepState
		var evidenceJSON, artifactsJSON, effectJSON, stepWaitJSON, stepFailureJSON []byte
		if err := rows.Scan(&step.WorkspaceID, &step.RunID, &step.StepID, &step.Ordinal, &step.Kind, &step.Status, &step.Attempt, &step.IdempotencyKey, &step.OutputHash, &evidenceJSON, &artifactsJSON, &effectJSON, &stepWaitJSON, &stepFailureJSON, &step.NextRetryAt, &step.StateVersion, &step.StateHash); err != nil {
			return contracts.WorkflowRun{}, err
		}
		if err := json.Unmarshal(evidenceJSON, &step.Evidence); err != nil {
			return contracts.WorkflowRun{}, err
		}
		if err := json.Unmarshal(artifactsJSON, &step.Artifacts); err != nil {
			return contracts.WorkflowRun{}, err
		}
		if len(effectJSON) > 0 && string(effectJSON) != "null" {
			step.Effect = new(contracts.ExternalEffect)
			if err := json.Unmarshal(effectJSON, step.Effect); err != nil {
				return contracts.WorkflowRun{}, err
			}
		}
		if len(stepWaitJSON) > 0 && string(stepWaitJSON) != "null" {
			step.Wait = new(contracts.WorkflowWait)
			if err := json.Unmarshal(stepWaitJSON, step.Wait); err != nil {
				return contracts.WorkflowRun{}, err
			}
		}
		if len(stepFailureJSON) > 0 && string(stepFailureJSON) != "null" {
			step.Failure = new(contracts.WorkflowFailure)
			if err := json.Unmarshal(stepFailureJSON, step.Failure); err != nil {
				return contracts.WorkflowRun{}, err
			}
		}
		run.Steps = append(run.Steps, step)
	}
	if err := rows.Err(); err != nil {
		return contracts.WorkflowRun{}, err
	}
	if err := run.Normalize(); err != nil {
		return contracts.WorkflowRun{}, fmt.Errorf("normalize stored workflow: %w", err)
	}
	return run, nil
}

func reserveWorkflowCommand(ctx context.Context, tx pgx.Tx, workspaceID, runID, stepID, key, commandHash string) (bool, error) {
	inserted, err := tx.Exec(ctx, `INSERT INTO fornix.workflow_idempotency(workspace_id,run_id,step_id,idempotency_key,command_hash) VALUES($1,$2,$3,$4,$5) ON CONFLICT (workspace_id,run_id,step_id,idempotency_key) DO NOTHING`, workspaceID, runID, stepID, key, commandHash)
	if err != nil {
		return false, err
	}
	if inserted.RowsAffected() == 1 {
		return false, nil
	}
	var storedHash string
	var version *int64
	if err := tx.QueryRow(ctx, `SELECT command_hash,transition_version FROM fornix.workflow_idempotency WHERE workspace_id=$1 AND run_id=$2 AND step_id=$3 AND idempotency_key=$4 FOR UPDATE`, workspaceID, runID, stepID, key).Scan(&storedHash, &version); err != nil {
		return false, err
	}
	if storedHash != commandHash {
		return false, ErrWorkflowIdempotency
	}
	if version == nil {
		return false, ErrWorkflowIncomplete
	}
	return true, nil
}

func workflowStartDuplicate(run contracts.WorkflowRun, stepID string) (WorkflowStepStartResult, error) {
	_, step, err := findWorkflowStep(run, stepID)
	if err != nil {
		return WorkflowStepStartResult{}, err
	}
	return WorkflowStepStartResult{Run: run, Step: step, Duplicate: true}, nil
}

func workflowCompleteDuplicate(run contracts.WorkflowRun, stepID string) (WorkflowStepCompleteResult, error) {
	_, step, err := findWorkflowStep(run, stepID)
	if err != nil {
		return WorkflowStepCompleteResult{}, err
	}
	return WorkflowStepCompleteResult{Run: run, Step: step, Duplicate: true}, nil
}

func findWorkflowStep(run contracts.WorkflowRun, stepID string) (int, contracts.WorkflowStepState, error) {
	for i, step := range run.Steps {
		if step.StepID == stepID {
			return i, step, nil
		}
	}
	return -1, contracts.WorkflowStepState{}, ErrWorkflowStepNotFound
}

func workflowDependenciesComplete(run contracts.WorkflowRun, index int) bool {
	completed := make(map[string]bool, len(run.Steps))
	for _, step := range run.Steps {
		completed[step.StepID] = step.Status == contracts.WorkflowStepSucceeded || step.Status == contracts.WorkflowStepCompensated
	}
	for _, dep := range run.Plan.Steps[index].DependsOn {
		if !completed[dep] {
			return false
		}
	}
	return true
}

func cloneWorkflowRun(value contracts.WorkflowRun) contracts.WorkflowRun {
	raw, _ := json.Marshal(value)
	var clone contracts.WorkflowRun
	_ = json.Unmarshal(raw, &clone)
	return clone
}

func workflowStepHash(step contracts.WorkflowStepState) string {
	clone := step
	clone.StateHash = ""
	return workflowHashString(fmt.Sprintf("%s:%d:%s:%d:%s:%s", clone.StepID, clone.Ordinal, clone.Status, clone.Attempt, clone.OutputHash, clone.IdempotencyKey))
}

func workflowHash(value any) (string, error) { return hashValue(value) }
func workflowHashString(value string) string { return hashString(value) }

func workflowWithinWallBudget(run contracts.WorkflowRun) bool {
	if run.CreatedAt.IsZero() || run.Budget.MaxWallTimeMS <= 0 {
		return true
	}
	return time.Now().UTC().Before(run.CreatedAt.Add(time.Duration(run.Budget.MaxWallTimeMS) * time.Millisecond))
}

func workflowActor(actor, fallback contracts.ActorRef) contracts.ActorRef {
	if strings.TrimSpace(actor.ID) == "" {
		return fallback
	}
	return actor
}

func workflowEvent(run contracts.WorkflowRun, eventType, stepID string, version int64, stateHash, command string) (contracts.EventEnvelope, error) {
	event, err := contracts.NewEvent(eventType, map[string]any{"run_id": run.ID, "operation_id": run.Operation.ID, "step_id": stepID, "status": run.Status, "version": version, "state_hash": stateHash})
	if err != nil {
		return contracts.EventEnvelope{}, err
	}
	event.Scope = contracts.Scope{WorkspaceID: run.WorkspaceID, Subject: run.ID}
	event.Actor, event.Task, event.Session = run.Actor, run.Task, run.Session
	event.IdempotencyKey = "workflow-event-" + workflowHashString(run.WorkspaceID + "\x00" + run.ID + "\x00" + fmt.Sprint(version) + "\x00" + stepID + "\x00" + command)[:48]
	event.StateDeltas = []contracts.StateDelta{{Op: contracts.DeltaSet, Path: "/workflows/" + escapeJSONPointerToken(run.ID) + "/status", Value: mustJSON(run.Status)}}
	return event, nil
}

func normalizeWorkflowCommand(workspaceID, runID, stepID, ownerID string, fence uint64, idempotencyKey string) error {
	if strings.TrimSpace(workspaceID) == "" || strings.TrimSpace(runID) == "" || strings.TrimSpace(stepID) == "" || strings.TrimSpace(ownerID) == "" || fence == 0 || strings.TrimSpace(idempotencyKey) == "" || len(idempotencyKey) > contracts.MaxIdempotencyLength {
		return ErrWorkflowLease
	}
	return nil
}

func workflowNullableJSON(value any) []byte {
	if value == nil {
		return []byte("null")
	}
	raw, _ := json.Marshal(value)
	return raw
}

func workflowNullableJSONValue(value any) []byte {
	return workflowNullableJSON(value)
}
