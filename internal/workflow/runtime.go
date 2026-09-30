// Package workflow provides the deterministic execution boundary for generic
// multi-step work. It coordinates durable checkpoints; it does not own model,
// tool, connector, approval, or human-input implementations.
package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

var (
	ErrRuntimeNotConfigured = errors.New("workflow runtime is not configured")
	ErrRuntimeNoRunnable    = errors.New("workflow has no runnable step")
	ErrRuntimeBudget        = errors.New("workflow runtime budget exhausted")
)

// StepExecutor is deliberately narrower than a model or tool interface. The
// runtime supplies a typed plan step and current checkpoint; an implementation
// may call an adapter only after StartStep has committed a fenced reservation.
// The executor must return hashes and references, never unbounded raw output.
type StepExecutor interface {
	Execute(context.Context, contracts.WorkflowRun, contracts.WorkflowStepState, contracts.OperationStep) (contracts.WorkflowStepResult, error)
}

// FencedStepExecutor is an optional extension for steps that need to cross
// another durable authority boundary. The runtime passes the already-checked
// workflow lease and task fence so an executor cannot invent or reacquire an
// unrelated owner while dispatching an external effect.
type FencedStepExecutor interface {
	ExecuteWithLease(context.Context, contracts.WorkflowRun, contracts.WorkflowStepState, contracts.OperationStep, store.WorkflowLease, string, uint64) (contracts.WorkflowStepResult, error)
}

// RunningStepRecoverer is an optional, read-only recovery hook. It may
// reconstruct a result only from an authoritative durable record proving the
// outcome; unproven running steps continue through WorkflowStore's conservative
// recovery-required transition.
type RunningStepRecoverer interface {
	RecoverStepResult(context.Context, contracts.WorkflowRun, contracts.WorkflowStepState, contracts.OperationStep) (contracts.WorkflowStepResult, bool, error)
}

type ExecutorFunc func(context.Context, contracts.WorkflowRun, contracts.WorkflowStepState, contracts.OperationStep) (contracts.WorkflowStepResult, error)

func (f ExecutorFunc) Execute(ctx context.Context, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep) (contracts.WorkflowStepResult, error) {
	return f(ctx, run, state, step)
}

type Runtime struct {
	Store    *store.WorkflowStore
	Executor StepExecutor
	Now      func() time.Time
}

type RunInput struct {
	WorkspaceID string
	RunID       string
	Lease       store.WorkflowLease
	TaskOwnerID string
	TaskFence   uint64
	Actor       contracts.ActorRef
}

type AdvanceResult struct {
	Run            contracts.WorkflowRun
	StepID         string
	StepIDs        []string
	Attempt        int
	Waiting        bool
	Terminal       bool
	Recovery       bool
	Duplicate      bool
	TransitionHash string
}

func (r *Runtime) Run(ctx context.Context, input RunInput) (contracts.WorkflowRun, error) {
	if err := r.validate(input); err != nil {
		return contracts.WorkflowRun{}, err
	}
	if r.Now == nil {
		r.Now = time.Now
	}
	run, err := r.Store.Get(ctx, input.WorkspaceID, input.RunID)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	deadline := run.CreatedAt.Add(time.Duration(run.Budget.MaxWallTimeMS) * time.Millisecond)
	if !run.CreatedAt.IsZero() && !r.Now().Before(deadline) {
		return contracts.WorkflowRun{}, ErrRuntimeBudget
	}
	runCtx := ctx
	if !run.CreatedAt.IsZero() {
		var cancel context.CancelFunc
		runCtx, cancel = context.WithDeadline(ctx, deadline)
		defer cancel()
	}
	maxTransitions := run.Budget.MaxSteps * 2
	if maxTransitions < 2 {
		maxTransitions = 2
	}
	for transitions := 0; transitions < maxTransitions; transitions++ {
		if contracts.IsTerminalWorkflowStatus(run.Status) || (isWaiting(run.Status) && run.Status != contracts.WorkflowStatusAwaitingRetry) {
			return run, nil
		}
		result, stepErr := r.AdvanceBatch(runCtx, input)
		if stepErr != nil {
			return contracts.WorkflowRun{}, stepErr
		}
		run = result.Run
		if result.Terminal {
			return run, nil
		}
		if result.Waiting {
			// Retry scheduling is a durable yield point. Return even when this
			// call just committed one attempt; a later advance resumes the next
			// eligible retry under a fresh scheduler/lease turn.
			return run, nil
		}
	}
	return run, ErrRuntimeBudget
}

// Advance reserves and executes the next deterministic batch. If a previous worker
// left a step running, it is first converted to recovery_required; this is
// safer than guessing whether an external effect happened.
func (r *Runtime) Advance(ctx context.Context, input RunInput) (AdvanceResult, error) {
	return r.AdvanceBatch(ctx, input)
}

// AdvanceBatch executes independent read-only/observation steps concurrently,
// but commits their durable results in canonical ordinal order. Effectful
// siblings are never included in the same batch.
func (r *Runtime) AdvanceBatch(ctx context.Context, input RunInput) (AdvanceResult, error) {
	if err := r.validate(input); err != nil {
		return AdvanceResult{}, err
	}
	run, err := r.Store.Get(ctx, input.WorkspaceID, input.RunID)
	if err != nil {
		return AdvanceResult{}, err
	}
	if contracts.IsTerminalWorkflowStatus(run.Status) {
		return AdvanceResult{Run: run, Terminal: true}, nil
	}
	for _, state := range run.Steps {
		if state.Status == contracts.WorkflowStepRunning {
			if recoverer, ok := r.Executor.(RunningStepRecoverer); ok {
				planStep, planErr := planStepFor(run, state.StepID)
				if planErr != nil {
					return AdvanceResult{}, planErr
				}
				result, recovered, recoveryErr := recoverer.RecoverStepResult(ctx, run, state, planStep)
				if recoveryErr != nil {
					return AdvanceResult{}, recoveryErr
				}
				if recovered {
					waitToken := ""
					if result.Wait != nil {
						waitToken = result.Wait.Token
					}
					key := deterministicKey("recover-proven-result", run.ID, state.StepID, fmt.Sprint(state.Attempt), waitToken)
					completed, completeErr := r.Store.CompleteStep(ctx, store.WorkflowStepCompleteInput{
						WorkspaceID: input.WorkspaceID, RunID: input.RunID, StepID: state.StepID, Attempt: state.Attempt,
						OwnerID: input.Lease.OwnerID, Fence: input.Lease.Fence, TaskOwnerID: input.TaskOwnerID,
						TaskFence: input.TaskFence, Actor: r.actor(run, input.Actor), IdempotencyKey: key,
						CausationID: run.Operation.ID, CorrelationID: run.Operation.ID, Result: result,
					})
					if completeErr != nil {
						return AdvanceResult{}, completeErr
					}
					return AdvanceResult{Run: completed.Run, StepID: state.StepID, Attempt: state.Attempt, Waiting: isWaiting(completed.Run.Status), Terminal: contracts.IsTerminalWorkflowStatus(completed.Run.Status), Recovery: true, Duplicate: completed.Duplicate, TransitionHash: completed.Run.StateHash}, nil
				}
			}
			key := deterministicKey("recover", run.ID, state.StepID, fmt.Sprint(state.Attempt))
			recovered, recoverErr := r.Store.RecoverRunning(ctx, input.WorkspaceID, input.RunID, state.StepID, input.Lease.OwnerID, input.Lease.Fence, r.actor(run, input.Actor), input.TaskOwnerID, input.TaskFence, key)
			if recoverErr != nil {
				return AdvanceResult{}, recoverErr
			}
			return AdvanceResult{Run: recovered, StepID: state.StepID, Attempt: state.Attempt, Recovery: true}, nil
		}
	}
	if isWaiting(run.Status) {
		if run.Status == contracts.WorkflowStatusAwaitingRetry {
			return r.advanceDueRetry(ctx, input, run)
		}
		return AdvanceResult{Run: run, Waiting: true}, nil
	}
	ready := run.WorkflowRunnableSteps()
	if len(ready) == 0 {
		return AdvanceResult{}, ErrRuntimeNoRunnable
	}
	if len(ready) > 1 {
		return r.advanceParallel(ctx, input, run, ready)
	}
	return r.advanceOne(ctx, input, run, ready[0])
}

// advanceDueRetry tries retry-waiting steps in plan order. StartStep performs
// the authoritative PostgreSQL-clock check and fenced attempt reservation;
// an early step is skipped so it cannot hide a later due retry.
func (r *Runtime) advanceDueRetry(ctx context.Context, input RunInput, run contracts.WorkflowRun) (AdvanceResult, error) {
	for _, state := range run.Steps {
		if state.Status != contracts.WorkflowStepAwaitingRetry {
			continue
		}
		result, err := r.advanceOne(ctx, input, run, state)
		if errors.Is(err, store.ErrWorkflowRetryNotDue) {
			continue
		}
		if err != nil {
			return AdvanceResult{}, err
		}
		return result, nil
	}
	return AdvanceResult{Run: run, Waiting: true}, nil
}

func (r *Runtime) advanceOne(ctx context.Context, input RunInput, run contracts.WorkflowRun, state contracts.WorkflowStepState) (AdvanceResult, error) {
	planStep, err := planStepFor(run, state.StepID)
	if err != nil {
		return AdvanceResult{}, err
	}
	startKey := deterministicKey("start", run.ID, state.StepID, fmt.Sprint(state.Attempt+1))
	started, err := r.Store.StartStep(ctx, store.WorkflowStepStartInput{WorkspaceID: input.WorkspaceID, RunID: input.RunID, StepID: state.StepID, OwnerID: input.Lease.OwnerID, Fence: input.Lease.Fence, TaskOwnerID: input.TaskOwnerID, TaskFence: input.TaskFence, Actor: r.actor(run, input.Actor), IdempotencyKey: startKey, CausationID: run.Operation.ID, CorrelationID: run.Operation.ID})
	if err != nil {
		return AdvanceResult{}, err
	}
	result, executeErr := r.executeStep(ctx, input, started.Run, started.Step, planStep)
	if executeErr != nil {
		result = contracts.WorkflowStepResult{Status: contracts.WorkflowStepFailed, Failure: &contracts.WorkflowFailure{Code: classifyExecutorError(executeErr), Retryable: false}}
	}
	completeKey := deterministicKey("complete", run.ID, state.StepID, fmt.Sprint(started.Step.Attempt), result.Status, result.OutputHash)
	completed, err := r.Store.CompleteStep(ctx, store.WorkflowStepCompleteInput{WorkspaceID: input.WorkspaceID, RunID: input.RunID, StepID: state.StepID, Attempt: started.Step.Attempt, OwnerID: input.Lease.OwnerID, Fence: input.Lease.Fence, TaskOwnerID: input.TaskOwnerID, TaskFence: input.TaskFence, Actor: r.actor(run, input.Actor), IdempotencyKey: completeKey, CausationID: run.Operation.ID, CorrelationID: run.Operation.ID, Result: result})
	if err != nil {
		return AdvanceResult{}, err
	}
	return AdvanceResult{Run: completed.Run, StepID: state.StepID, Attempt: started.Step.Attempt, Waiting: isWaiting(completed.Run.Status), Terminal: contracts.IsTerminalWorkflowStatus(completed.Run.Status), Duplicate: started.Duplicate || completed.Duplicate, TransitionHash: completed.Run.StateHash}, nil
}

func (r *Runtime) advanceParallel(ctx context.Context, input RunInput, run contracts.WorkflowRun, ready []contracts.WorkflowStepState) (AdvanceResult, error) {
	type startedStep struct {
		state  contracts.WorkflowStepState
		plan   contracts.OperationStep
		result contracts.WorkflowStepResult
		start  store.WorkflowStepStartResult
	}
	started := make([]startedStep, 0, len(ready))
	for _, state := range ready {
		planStep, err := planStepFor(run, state.StepID)
		if err != nil {
			return AdvanceResult{}, err
		}
		startKey := deterministicKey("start", run.ID, state.StepID, fmt.Sprint(state.Attempt+1))
		reservation, err := r.Store.StartStep(ctx, store.WorkflowStepStartInput{WorkspaceID: input.WorkspaceID, RunID: input.RunID, StepID: state.StepID, OwnerID: input.Lease.OwnerID, Fence: input.Lease.Fence, TaskOwnerID: input.TaskOwnerID, TaskFence: input.TaskFence, Actor: r.actor(run, input.Actor), IdempotencyKey: startKey, CausationID: run.Operation.ID, CorrelationID: run.Operation.ID})
		if err != nil {
			return AdvanceResult{}, err
		}
		started = append(started, startedStep{state: state, plan: planStep, start: reservation})
	}
	var wg sync.WaitGroup
	for index := range started {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			result, err := r.executeStep(ctx, input, started[index].start.Run, started[index].start.Step, started[index].plan)
			if err != nil {
				result = contracts.WorkflowStepResult{Status: contracts.WorkflowStepFailed, Failure: &contracts.WorkflowFailure{Code: classifyExecutorError(err)}}
			}
			started[index].result = result
		}(index)
	}
	wg.Wait()
	current := run
	stepIDs := make([]string, 0, len(started))
	duplicate := false
	for _, value := range started {
		completeKey := deterministicKey("complete", run.ID, value.state.StepID, fmt.Sprint(value.start.Step.Attempt), value.result.Status, value.result.OutputHash)
		completed, err := r.Store.CompleteStep(ctx, store.WorkflowStepCompleteInput{WorkspaceID: input.WorkspaceID, RunID: input.RunID, StepID: value.state.StepID, Attempt: value.start.Step.Attempt, OwnerID: input.Lease.OwnerID, Fence: input.Lease.Fence, TaskOwnerID: input.TaskOwnerID, TaskFence: input.TaskFence, Actor: r.actor(run, input.Actor), IdempotencyKey: completeKey, CausationID: run.Operation.ID, CorrelationID: run.Operation.ID, Result: value.result})
		if err != nil {
			return AdvanceResult{}, err
		}
		current = completed.Run
		duplicate = duplicate || value.start.Duplicate || completed.Duplicate
		stepIDs = append(stepIDs, value.state.StepID)
	}
	return AdvanceResult{Run: current, StepID: stepIDs[0], StepIDs: stepIDs, Waiting: isWaiting(current.Status), Terminal: contracts.IsTerminalWorkflowStatus(current.Status), Duplicate: duplicate, TransitionHash: current.StateHash}, nil
}

func (r *Runtime) executeStep(ctx context.Context, input RunInput, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep) (contracts.WorkflowStepResult, error) {
	if fenced, ok := r.Executor.(FencedStepExecutor); ok {
		return fenced.ExecuteWithLease(ctx, run, state, step, input.Lease, input.TaskOwnerID, input.TaskFence)
	}
	return r.Executor.Execute(ctx, run, state, step)
}

// ResumeWait records an approval, callback, human response, timer, or
// external verification result. The response is already canonicalized by the
// caller and is represented here only by bounded hashes/references.
func (r *Runtime) ResumeWait(ctx context.Context, input RunInput, stepID string, result contracts.WorkflowStepResult) (contracts.WorkflowRun, error) {
	if err := r.validate(input); err != nil {
		return contracts.WorkflowRun{}, err
	}
	run, err := r.Store.Get(ctx, input.WorkspaceID, input.RunID)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	_, step, err := findStep(run, stepID)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	if step.Status != contracts.WorkflowStepAwaitingApproval && step.Status != contracts.WorkflowStepAwaitingHuman && step.Status != contracts.WorkflowStepAwaitingCallback && step.Status != contracts.WorkflowStepAwaitingExternal && step.Status != contracts.WorkflowStepRecoveryRequired && step.Status != contracts.WorkflowStepAwaitingRetry {
		return contracts.WorkflowRun{}, fmt.Errorf("step %s is not awaiting a resumable input", stepID)
	}
	key, err := deterministicResumeKey(input.WorkspaceID, run.ID, stepID, step.Attempt, result)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	completed, err := r.Store.CompleteStep(ctx, store.WorkflowStepCompleteInput{WorkspaceID: input.WorkspaceID, RunID: input.RunID, StepID: stepID, Attempt: step.Attempt, OwnerID: input.Lease.OwnerID, Fence: input.Lease.Fence, TaskOwnerID: input.TaskOwnerID, TaskFence: input.TaskFence, Actor: r.actor(run, input.Actor), IdempotencyKey: key, Result: result})
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	return completed.Run, nil
}

func (r *Runtime) Cancel(ctx context.Context, input RunInput) (contracts.WorkflowRun, error) {
	if err := r.validate(input); err != nil {
		return contracts.WorkflowRun{}, err
	}
	run, err := r.Store.Get(ctx, input.WorkspaceID, input.RunID)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	return r.Store.Cancel(ctx, input.WorkspaceID, input.RunID, input.Lease.OwnerID, input.Lease.Fence, r.actor(run, input.Actor), input.TaskOwnerID, input.TaskFence, deterministicKey("cancel", run.ID))
}

func (r *Runtime) validate(input RunInput) error {
	if r == nil || r.Store == nil || r.Executor == nil {
		return ErrRuntimeNotConfigured
	}
	if strings.TrimSpace(input.WorkspaceID) == "" || strings.TrimSpace(input.RunID) == "" || strings.TrimSpace(input.Lease.OwnerID) == "" || input.Lease.Fence == 0 {
		return store.ErrWorkflowLease
	}
	return nil
}

func (r *Runtime) actor(run contracts.WorkflowRun, supplied contracts.ActorRef) contracts.ActorRef {
	if supplied.ID == "" {
		return run.Actor
	}
	return supplied
}

func planStepFor(run contracts.WorkflowRun, stepID string) (contracts.OperationStep, error) {
	for _, step := range run.Plan.Steps {
		if step.ID == stepID {
			return step, nil
		}
	}
	return contracts.OperationStep{}, store.ErrWorkflowStepNotFound
}

func findStep(run contracts.WorkflowRun, stepID string) (int, contracts.WorkflowStepState, error) {
	for index, step := range run.Steps {
		if step.StepID == stepID {
			return index, step, nil
		}
	}
	return -1, contracts.WorkflowStepState{}, store.ErrWorkflowStepNotFound
}

func deterministicKey(parts ...string) string {
	return "workflow-" + contracts.HashStrings(parts...)[:48]
}

func deterministicResumeKey(workspaceID, runID, stepID string, attempt int, result contracts.WorkflowStepResult) (string, error) {
	if err := result.Normalize(workspaceID); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(result)
	if err != nil {
		return "", fmt.Errorf("hash workflow resume result: %w", err)
	}
	return deterministicKey("resume", runID, stepID, fmt.Sprint(attempt), contracts.HashStrings(string(canonical))), nil
}

func classifyExecutorError(err error) string {
	if err == nil {
		return "executor_failure"
	}
	text := strings.ToLower(strings.TrimSpace(err.Error()))
	if strings.Contains(text, "timeout") {
		return "timeout"
	}
	if strings.Contains(text, "cancel") {
		return "cancelled"
	}
	return "executor_failure"
}

func isWaiting(status string) bool {
	switch status {
	case contracts.WorkflowStatusAwaitingApproval, contracts.WorkflowStatusAwaitingHuman, contracts.WorkflowStatusAwaitingCallback, contracts.WorkflowStatusAwaitingRetry, contracts.WorkflowStatusAwaitingExternal, contracts.WorkflowStatusRecoveryRequired:
		return true
	default:
		return false
	}
}
