package agentloop

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/model"
	"github.com/omaveda/fornix/internal/tool"
)

var (
	ErrLoopNotConfigured = errors.New("agent loop is not configured")
	ErrLoopWaiting       = errors.New("agent loop is waiting for an external decision")
	ErrLoopBudget        = errors.New("agent loop budget exceeded")
	ErrLoopLease         = errors.New("agent loop worker lease is unavailable")
)

// RunStore is implemented by the Postgres agent-run store. The loop only
// constructs transitions; the store owns compare-and-swap, task fencing, and
// atomic event/checkpoint commits.
type RunStore interface {
	Reserve(context.Context, contracts.AgentRunRequest) (contracts.AgentRun, bool, error)
	Get(context.Context, string, string) (contracts.AgentRun, error)
	Commit(context.Context, contracts.AgentRun, contracts.AgentRun, string, any) (contracts.AgentRun, error)
	ValidateTaskFence(context.Context, contracts.AgentRun) error
	Replay(context.Context, string, string, uint64, int) ([]contracts.EventEnvelope, error)
}

// OwnedRunStore is implemented by the Postgres scheduler-aware store. It is
// optional so the deterministic in-memory loop tests and direct API paths can
// continue using the original RunStore contract; a worker-owned context fails
// closed when this stronger boundary is unavailable.
type OwnedRunStore interface {
	RunStore
	CommitOwned(context.Context, contracts.AgentRun, contracts.AgentRun, string, any, contracts.AgentRunLease) (contracts.AgentRun, error)
	ValidateAgentRunLease(context.Context, contracts.AgentRun, contracts.AgentRunLease) error
}

type workerLeaseContextKey struct{}

// WithWorkerLease binds one scheduler lease to an orchestrator invocation.
// The lease is immutable context data; Postgres remains authoritative.
func WithWorkerLease(ctx context.Context, lease contracts.AgentRunLease) context.Context {
	return context.WithValue(ctx, workerLeaseContextKey{}, lease)
}

func workerLeaseFromContext(ctx context.Context) (contracts.AgentRunLease, bool) {
	lease, ok := ctx.Value(workerLeaseContextKey{}).(contracts.AgentRunLease)
	return lease, ok
}

type ModelGateway interface {
	Complete(context.Context, contracts.ModelRequest, ...contracts.ProviderRef) (contracts.ModelResponse, error)
}

type ToolInvoker interface {
	Execute(context.Context, contracts.ToolRequest) (tool.Outcome, error)
	Definition(string) (contracts.ToolDefinition, bool)
}

type ApprovalReader interface {
	GetApproval(context.Context, string, string) (contracts.ApprovalRequest, error)
}

// ContextRetriever is the deterministic retrieval boundary. The loop records
// the resulting content hash and rendered evidence in its own history before
// admitting a model call, so retrieval is never repeated implicitly on retry.
type ContextRetriever interface {
	Retrieve(context.Context, contracts.RetrievalRequest) (contracts.ContextPack, error)
}

type ContextRetrieverFunc func(context.Context, contracts.RetrievalRequest) (contracts.ContextPack, error)

func (f ContextRetrieverFunc) Retrieve(ctx context.Context, request contracts.RetrievalRequest) (contracts.ContextPack, error) {
	return f(ctx, request)
}

type Clock func() time.Time

type Orchestrator struct {
	Runs      RunStore
	Models    ModelGateway
	Tools     ToolInvoker
	Approvals ApprovalReader
	Retriever ContextRetriever
	Now       Clock
}

func New(runs RunStore, models ModelGateway, tools ToolInvoker) *Orchestrator {
	return &Orchestrator{Runs: runs, Models: models, Tools: tools, Now: func() time.Time { return time.Now().UTC() }}
}

func (o *Orchestrator) Create(ctx context.Context, request contracts.AgentRunRequest) (contracts.AgentRun, bool, error) {
	if o == nil || o.Runs == nil {
		return contracts.AgentRun{}, false, ErrLoopNotConfigured
	}
	return o.Runs.Reserve(ctx, request)
}

// Run advances a durable run until it reaches a terminal or explicit waiting
// state. Each external effect is separated from its checkpoint by a durable
// model/tool ledger identity, so a process crash is resumable.
func (o *Orchestrator) Run(ctx context.Context, workspaceID, runID string) (contracts.LoopDecision, error) {
	if o == nil || o.Runs == nil || o.Models == nil || o.Tools == nil {
		return contracts.LoopDecision{}, ErrLoopNotConfigured
	}
	run, err := o.Runs.Get(ctx, workspaceID, runID)
	if err != nil {
		return contracts.LoopDecision{}, err
	}
	deadline := run.CreatedAt.Add(time.Duration(run.Budget.MaxWallTimeMS) * time.Millisecond)
	if !run.CreatedAt.IsZero() && o.now().After(deadline) && !contracts.IsAgentTerminal(run.State) {
		return o.fail(ctx, run, &contracts.LoopFailure{Code: contracts.AgentFailureBudget, Message: "agent wall-clock budget exceeded", Phase: run.Phase}, contracts.AgentTerminationBudget)
	}
	maxPhases := run.Budget.MaxModelSteps + run.Budget.MaxToolCalls + run.Budget.MaxTurns + 4
	if maxPhases < 4 {
		maxPhases = 4
	}
	var decision contracts.LoopDecision
	for i := 0; i < maxPhases; i++ {
		decision, err = o.Advance(ctx, workspaceID, runID)
		if err != nil {
			return decision, err
		}
		if decision.Action != contracts.AgentActionAdvanced {
			return decision, nil
		}
	}
	return o.fail(ctx, decision.Run, &contracts.LoopFailure{Code: contracts.AgentFailureBudget, Message: "agent phase budget exceeded", Phase: decision.Run.Phase}, contracts.AgentTerminationBudget)
}

func (o *Orchestrator) Advance(ctx context.Context, workspaceID, runID string) (contracts.LoopDecision, error) {
	if o == nil || o.Runs == nil || o.Models == nil || o.Tools == nil {
		return contracts.LoopDecision{}, ErrLoopNotConfigured
	}
	run, err := o.Runs.Get(ctx, strings.TrimSpace(workspaceID), strings.TrimSpace(runID))
	if err != nil {
		return contracts.LoopDecision{}, err
	}
	if contracts.IsAgentTerminal(run.State) {
		return terminalDecision(run), nil
	}
	if err := o.validateWorkerLease(ctx, run); err != nil {
		return contracts.LoopDecision{}, err
	}
	if err := o.checkWallClock(run); err != nil {
		return o.fail(ctx, run, loopFailureFromError(err, run.Phase), contracts.AgentTerminationBudget)
	}
	if run.State == contracts.AgentRunAwaitingExternal {
		return waitingDecision(run), nil
	}
	if run.State == contracts.AgentRunAwaitingApproval {
		return o.advanceApproval(ctx, run)
	}
	if run.State == contracts.AgentRunAwaitingRetry {
		if run.NextRetryAt != nil && run.NextRetryAt.After(o.now()) {
			return waitingDecision(run), nil
		}
		next := run
		next.State, next.LastFailure, next.NextRetryAt = contracts.AgentRunRunning, nil, nil
		committed, commitErr := o.commit(ctx, run, next, contracts.AgentEventCheckpointed, map[string]any{"run_id": run.ID, "reason": "retry_due"})
		if commitErr != nil {
			return contracts.LoopDecision{}, commitErr
		}
		return contracts.LoopDecision{Action: contracts.AgentActionAdvanced, Run: committed, Checkpoint: committed.Checkpoint()}, nil
	}
	if len(run.PendingTools) > 0 {
		return o.advanceTool(ctx, run)
	}
	return o.advanceModel(ctx, run)
}

func (o *Orchestrator) Cancel(ctx context.Context, workspaceID, runID, reason string) (contracts.LoopDecision, error) {
	run, err := o.Runs.Get(ctx, strings.TrimSpace(workspaceID), strings.TrimSpace(runID))
	if err != nil {
		return contracts.LoopDecision{}, err
	}
	if contracts.IsAgentTerminal(run.State) {
		return terminalDecision(run), nil
	}
	if err := o.validateWorkerLease(ctx, run); err != nil {
		return contracts.LoopDecision{}, err
	}
	next := run
	next.State, next.Phase, next.Termination = contracts.AgentRunCancelled, contracts.AgentPhaseModel, contracts.AgentTerminationCancelled
	next.PendingTools = nil
	next.LastFailure = &contracts.LoopFailure{Code: contracts.AgentFailureCancelled, Message: strings.TrimSpace(reason), Phase: run.Phase}
	if next.LastFailure.Message == "" {
		next.LastFailure.Message = "agent run cancelled"
	}
	committed, err := o.commit(ctx, run, next, contracts.AgentEventCancelled, map[string]any{"run_id": run.ID, "reason": next.LastFailure.Message})
	if err != nil {
		return contracts.LoopDecision{}, err
	}
	return terminalDecision(committed), nil
}

// WaitExternal durably pauses a run for an integration that is outside the
// model/tool runtime. No model or tool effect is admitted while this state is
// active; CompleteExternal is the only normal resume path.
func (o *Orchestrator) WaitExternal(ctx context.Context, workspaceID, runID, reason string) (contracts.LoopDecision, error) {
	run, err := o.Runs.Get(ctx, strings.TrimSpace(workspaceID), strings.TrimSpace(runID))
	if err != nil {
		return contracts.LoopDecision{}, err
	}
	if contracts.IsAgentTerminal(run.State) {
		return terminalDecision(run), nil
	}
	if err := o.validateWorkerLease(ctx, run); err != nil {
		return contracts.LoopDecision{}, err
	}
	reason = strings.TrimSpace(reason)
	if reason == "" {
		reason = "external completion is required"
	}
	next := run
	next.State, next.LastFailure, next.NextRetryAt = contracts.AgentRunAwaitingExternal, &contracts.LoopFailure{Code: contracts.AgentFailureExternal, Message: reason, Phase: run.Phase}, nil
	committed, err := o.commit(ctx, run, next, contracts.AgentEventExternalWaiting, map[string]any{"run_id": run.ID, "reason": reason})
	if err != nil {
		return contracts.LoopDecision{}, err
	}
	return waitingDecision(committed), nil
}

// CompleteExternal commits one caller-supplied external result. The result is
// treated as an assistant output only after the state-version CAS succeeds,
// making duplicate completion requests harmless.
func (o *Orchestrator) CompleteExternal(ctx context.Context, workspaceID, runID, output string) (contracts.LoopDecision, error) {
	run, err := o.Runs.Get(ctx, strings.TrimSpace(workspaceID), strings.TrimSpace(runID))
	if err != nil {
		return contracts.LoopDecision{}, err
	}
	if contracts.IsAgentTerminal(run.State) {
		return terminalDecision(run), nil
	}
	if run.State != contracts.AgentRunAwaitingExternal {
		return contracts.LoopDecision{}, fmt.Errorf("agent run is not awaiting external completion")
	}
	if err := o.validateWorkerLease(ctx, run); err != nil {
		return contracts.LoopDecision{}, err
	}
	output = strings.TrimSpace(output)
	if output == "" {
		return contracts.LoopDecision{}, fmt.Errorf("external completion output is required")
	}
	if contracts.EstimateModelTokens(output) > run.Budget.MaxOutputTokens-run.OutputTokens || len([]byte(output)) > run.Budget.MaxContextBytes {
		return o.fail(ctx, run, &contracts.LoopFailure{Code: contracts.AgentFailureBudget, Message: "external completion exceeds run budget", Phase: run.Phase}, contracts.AgentTerminationBudget)
	}
	next := run
	next.State, next.Phase, next.Termination = contracts.AgentRunSucceeded, contracts.AgentPhaseModel, contracts.AgentTerminationCompleted
	next.LastFailure, next.NextRetryAt, next.LastOutput = nil, nil, output
	next.Turn, next.Step = run.Turn+1, run.Step+1
	next.OutputTokens += contracts.EstimateModelTokens(output)
	next.TotalTokens += contracts.EstimateModelTokens(output)
	next.History = append(cloneMessages(run.History), contracts.ModelMessage{Role: "assistant", Content: output})
	committed, err := o.commit(ctx, run, next, contracts.AgentEventExternalCompleted, map[string]any{"run_id": run.ID, "output": output})
	if err != nil {
		return contracts.LoopDecision{}, err
	}
	return terminalDecision(committed), nil
}

func (o *Orchestrator) advanceModel(ctx context.Context, run contracts.AgentRun) (contracts.LoopDecision, error) {
	if o.Retriever != nil && run.ContextHash == "" {
		request := contracts.RetrievalRequest{WorkspaceID: run.WorkspaceID, Query: run.Goal,
			MaxBytes:  minInt(run.Budget.MaxContextBytes, contracts.MaxRetrievalBytes),
			MaxTokens: minInt(run.Budget.MaxOutputTokens, contracts.MaxRetrievalTokens)}
		if run.Retrieval != nil {
			request = *run.Retrieval
		}
		pack, err := o.Retriever.Retrieve(ctx, request)
		if err != nil {
			return o.fail(ctx, run, &contracts.LoopFailure{Code: contracts.AgentFailureAbstained, Message: "context compilation failed: " + err.Error(), Phase: contracts.AgentPhaseModel}, contracts.AgentTerminationAbstained)
		}
		if pack.WorkspaceID != run.WorkspaceID || pack.ContentHash == "" {
			return o.fail(ctx, run, &contracts.LoopFailure{Code: contracts.AgentFailureWorkspace, Message: "context compiler returned an invalid workspace or content hash", Phase: contracts.AgentPhaseModel}, contracts.AgentTerminationAbstained)
		}
		next := run
		next.ContextHash = pack.ContentHash
		next.ContextBytes = pack.TotalBytes
		if len(pack.Items) > 0 {
			next.History = append(cloneMessages(run.History), contracts.ModelMessage{Role: "system", Content: stableContextContent(pack)})
		}
		committed, err := o.commit(ctx, run, next, contracts.AgentEventContextCompiled, map[string]any{"run_id": run.ID, "content_hash": pack.ContentHash, "items": len(pack.Items), "bytes": pack.TotalBytes, "tokens": pack.TotalTokens, "abstained": pack.Abstained})
		if err != nil {
			return contracts.LoopDecision{}, err
		}
		return contracts.LoopDecision{Action: contracts.AgentActionAdvanced, Run: committed, Checkpoint: committed.Checkpoint()}, nil
	}
	if run.Turn >= run.Budget.MaxTurns || run.ModelAttempts >= run.Budget.MaxModelSteps {
		return o.fail(ctx, run, &contracts.LoopFailure{Code: contracts.AgentFailureBudget, Message: "model turn or step budget exceeded", Phase: contracts.AgentPhaseModel}, contracts.AgentTerminationBudget)
	}
	inputBytes := estimateHistoryBytes(run.History) + estimateToolCatalogBytes(run.Tools)
	if inputBytes > run.Budget.MaxContextBytes {
		return o.fail(ctx, run, &contracts.LoopFailure{Code: contracts.AgentFailureBudget, Message: "context byte budget exceeded", Phase: contracts.AgentPhaseModel}, contracts.AgentTerminationAbstained)
	}
	attempt := run.ModelAttempts + 1
	request := contracts.ModelRequest{
		SchemaVersion:  contracts.ModelSchemaVersion,
		RequestID:      stableID("agent-model-request", run.ID, fmt.Sprint(run.Turn+1), fmt.Sprint(run.Step+1), fmt.Sprint(attempt)),
		IdempotencyKey: stableID("agent-model", run.ID, fmt.Sprint(run.Turn+1), fmt.Sprint(run.Step+1), fmt.Sprint(attempt)),
		CausationID:    run.CausationID, CorrelationID: run.CorrelationID, WorkspaceID: run.WorkspaceID,
		Actor: run.Actor, Task: run.Task, Session: run.Session, Provider: run.Provider,
		Messages: cloneMessages(run.History), Tools: append([]contracts.ModelToolDefinition(nil), run.Tools...),
		Budget:      contracts.ModelBudget{MaxInputBytes: run.Budget.MaxContextBytes, MaxOutputTokens: minInt(contracts.MaxModelOutputTokens, run.Budget.MaxOutputTokens-run.OutputTokens), MaxTotalTokens: contracts.MaxModelInputTokens + minInt(contracts.MaxModelOutputTokens, run.Budget.MaxOutputTokens-run.OutputTokens), MaxCostUSD: maxFloat(0, run.Budget.MaxCostUSD-run.Cost.TotalCostUSD), TimeoutMS: int(minInt64(int64(contracts.MaxModelTimeout/time.Millisecond), run.Budget.MaxWallTimeMS))},
		RetryPolicy: contracts.DefaultRetryPolicy(),
	}
	if request.Budget.MaxOutputTokens < 1 {
		return o.fail(ctx, run, &contracts.LoopFailure{Code: contracts.AgentFailureBudget, Message: "output token budget exhausted", Phase: contracts.AgentPhaseModel}, contracts.AgentTerminationBudget)
	}
	if request.Budget.TimeoutMS < 1 {
		request.Budget.TimeoutMS = int(contracts.DefaultModelTimeout / time.Millisecond)
	}
	started := o.now()
	response, callErr := o.Models.Complete(ctx, request)
	if callErr != nil {
		failure := loopFailureFromModelError(callErr, run.Phase, attempt)
		next := run
		next.ModelAttempts = attempt
		next.LastFailure = &failure
		if failure.Retryable && !failure.ContentEmitted && attempt < run.Budget.MaxModelSteps {
			next.State, next.NextRetryAt = contracts.AgentRunAwaitingRetry, ptrTime(started.Add(retryDelay(attempt)))
			committed, err := o.commit(ctx, run, next, contracts.AgentEventRetryWaiting, map[string]any{"run_id": run.ID, "phase": contracts.AgentPhaseModel, "attempt": attempt, "failure": failure})
			if err != nil {
				return contracts.LoopDecision{}, err
			}
			return contracts.LoopDecision{Action: contracts.AgentActionWaiting, Run: committed, Checkpoint: committed.Checkpoint(), Failure: &failure}, nil
		}
		return o.failWithAttempt(ctx, run, failure, contracts.AgentTerminationModelFailure, attempt)
	}
	if response.Usage.OutputTokens > run.Budget.MaxOutputTokens-run.OutputTokens || response.Usage.TotalTokens+run.TotalTokens > run.Budget.MaxOutputTokens+run.InputTokens || run.Cost.TotalCostUSD+response.Cost.TotalCostUSD > run.Budget.MaxCostUSD {
		return o.failWithAttempt(ctx, run, contracts.LoopFailure{Code: contracts.AgentFailureBudget, Message: "model response exceeded cumulative budget", Phase: contracts.AgentPhaseModel, Attempt: attempt}, contracts.AgentTerminationBudget, attempt)
	}
	response.RequestID = request.RequestID
	next := run
	next.State, next.Phase, next.Turn, next.Step = contracts.AgentRunRunning, contracts.AgentPhaseModel, run.Turn+1, run.Step+1
	next.ModelAttempts, next.ModelCalls = attempt, run.ModelCalls+1
	next.InputTokens += response.Usage.InputTokens
	next.OutputTokens += response.Usage.OutputTokens
	next.TotalTokens += response.Usage.TotalTokens
	next.ContextBytes = inputBytes
	next.Cost = addCost(run.Cost, response.Cost)
	next.LastFailure = nil
	next.NextRetryAt = nil
	next.History = append(cloneMessages(run.History), contracts.ModelMessage{Role: "assistant", Content: response.Content, ToolCalls: cloneModelToolCalls(response.ToolCalls)})
	step := contracts.ModelStep{ID: stableID("model-step", run.ID, fmt.Sprint(next.Step)), RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey, Attempt: attempt, Provider: response.Provider, Response: &response, Usage: response.Usage, Cost: response.Cost, ContentEmitted: response.Content != "", StartedAt: started, FinishedAt: o.now()}
	if len(response.ToolCalls) > 0 {
		if run.ToolCalls+len(response.ToolCalls) > run.Budget.MaxToolCalls || len(response.ToolCalls) > contracts.MaxAgentPendingTools {
			return o.failWithAttempt(ctx, run, contracts.LoopFailure{Code: contracts.AgentFailureBudget, Message: "tool-call budget exceeded", Phase: contracts.AgentPhaseTool, Attempt: attempt}, contracts.AgentTerminationBudget, attempt)
		}
		next.Phase, next.PendingTools, next.ToolCalls = contracts.AgentPhaseTool, make([]contracts.PendingToolCall, 0, len(response.ToolCalls)), run.ToolCalls+len(response.ToolCalls)
		for _, call := range response.ToolCalls {
			next.PendingTools = append(next.PendingTools, contracts.PendingToolCall{ID: call.ID, ToolID: call.ToolID, Arguments: append([]byte(nil), call.Arguments...)})
		}
		committed, err := o.commit(ctx, run, next, contracts.AgentEventModelCompleted, map[string]any{"run_id": run.ID, "step": next.Step, "model_step": step, "tool_calls": response.ToolCalls})
		if err != nil {
			return contracts.LoopDecision{}, err
		}
		return contracts.LoopDecision{Action: contracts.AgentActionAdvanced, Run: committed, Checkpoint: committed.Checkpoint(), Model: &step}, nil
	}
	next.State, next.Termination, next.LastOutput = contracts.AgentRunSucceeded, contracts.AgentTerminationCompleted, response.Content
	committed, err := o.commit(ctx, run, next, contracts.AgentEventCompleted, map[string]any{"run_id": run.ID, "step": next.Step, "output": response.Content, "model_step": step})
	if err != nil {
		return contracts.LoopDecision{}, err
	}
	return contracts.LoopDecision{Action: contracts.AgentActionTerminal, Run: committed, Checkpoint: committed.Checkpoint(), Model: &step}, nil
}

func (o *Orchestrator) advanceTool(ctx context.Context, run contracts.AgentRun) (contracts.LoopDecision, error) {
	call := run.PendingTools[0]
	definition, ok := o.Tools.Definition(call.ToolID)
	if !ok {
		return o.fail(ctx, run, &contracts.LoopFailure{Code: contracts.AgentFailureTool, Message: "model requested an unregistered tool", Phase: contracts.AgentPhaseTool}, contracts.AgentTerminationToolFailure)
	}
	if call.Attempt >= run.Budget.MaxToolAttempts {
		return o.fail(ctx, run, &contracts.LoopFailure{Code: contracts.AgentFailureTool, Message: "tool retry budget exhausted", Phase: contracts.AgentPhaseTool, Attempt: call.Attempt}, contracts.AgentTerminationToolFailure)
	}
	args, err := decodeToolArguments(call.Arguments)
	if err != nil {
		return o.fail(ctx, run, &contracts.LoopFailure{Code: contracts.AgentFailureTool, Message: err.Error(), Phase: contracts.AgentPhaseTool}, contracts.AgentTerminationToolFailure)
	}
	attempt := call.Attempt + 1
	request := contracts.ToolRequest{SchemaVersion: contracts.ToolSchemaVersion, RequestID: stableID("agent-tool-request", run.ID, call.ID, fmt.Sprint(attempt)), IdempotencyKey: stableID("agent-tool", run.ID, call.ID, fmt.Sprint(attempt)), CausationID: run.CausationID, CorrelationID: run.CorrelationID, WorkspaceID: run.WorkspaceID, Actor: run.Actor, Task: run.Task, Session: run.Session, TaskOwnerID: run.TaskOwnerID, TaskFence: run.TaskFence, ToolID: definition.ID, Capability: definition.Capability, Argv: append([]string{definition.Executable}, args.Argv...), Environment: args.Environment, Workdir: args.Workdir, Mode: contracts.ToolModeAutomatic, Budget: contracts.SandboxProfile{TimeoutMS: minInt(definition.Sandbox.TimeoutMS, int(run.Budget.MaxWallTimeMS))}}
	if run.State == contracts.AgentRunAwaitingApproval {
		request.Mode = contracts.ToolModeInteractive
		request.ApprovalID = call.ApprovalID
	}
	started := o.now()
	outcome, execErr := o.Tools.Execute(ctx, request)
	if execErr != nil {
		if failureErr := toolFailure(execErr); failureErr != nil {
			if failureErr.Code == contracts.ToolFailureApprovalRequired || (outcome.Approval != nil && outcome.Approval.Status == contracts.ApprovalPending) {
				next := run
				next.State, next.Phase = contracts.AgentRunAwaitingApproval, contracts.AgentPhaseTool
				next.PendingTools = clonePendingTools(run.PendingTools)
				next.PendingTools[0].ApprovalID = outcome.Run.ApprovalID
				next.LastFailure = &contracts.LoopFailure{Code: contracts.AgentFailureApproval, Message: failureErr.Message, Phase: contracts.AgentPhaseTool}
				committed, err := o.commit(ctx, run, next, contracts.AgentEventApprovalWaiting, map[string]any{"run_id": run.ID, "tool_id": call.ToolID, "approval_id": outcome.Run.ApprovalID})
				if err != nil {
					return contracts.LoopDecision{}, err
				}
				return contracts.LoopDecision{Action: contracts.AgentActionWaiting, Run: committed, Checkpoint: committed.Checkpoint(), Failure: next.LastFailure}, nil
			}
			if failureErr.Code == contracts.ToolFailureInProgress {
				return contracts.LoopDecision{}, ErrLoopWaiting
			}
			failure := contracts.LoopFailure{Code: AgentToolFailureCode(failureErr.Code), Message: failureErr.Message, Phase: contracts.AgentPhaseTool, Retryable: failureErr.Retryable, Attempt: attempt}
			next := run
			next.LastFailure = &failure
			next.PendingTools = clonePendingTools(run.PendingTools)
			next.PendingTools[0].Attempt = attempt
			next.PendingTools[0].LastFailure = &failure
			if failure.Retryable && attempt < run.Budget.MaxToolAttempts {
				next.State, next.NextRetryAt = contracts.AgentRunAwaitingRetry, ptrTime(started.Add(retryDelay(attempt)))
				committed, err := o.commit(ctx, run, next, contracts.AgentEventRetryWaiting, map[string]any{"run_id": run.ID, "phase": contracts.AgentPhaseTool, "tool_id": call.ToolID, "attempt": attempt, "failure": failure})
				if err != nil {
					return contracts.LoopDecision{}, err
				}
				return contracts.LoopDecision{Action: contracts.AgentActionWaiting, Run: committed, Checkpoint: committed.Checkpoint(), Failure: &failure}, nil
			}
			return o.failWithTool(ctx, run, failure, contracts.AgentTerminationToolFailure, attempt)
		}
		return contracts.LoopDecision{}, execErr
	}
	if outcome.Result == nil {
		return contracts.LoopDecision{}, fmt.Errorf("tool outcome has no result")
	}
	step := contracts.ToolStep{ID: call.ID, ToolID: call.ToolID, RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey, Attempt: attempt, Status: outcome.Run.Status, ApprovalID: request.ApprovalID, Result: outcome.Result, StartedAt: started, FinishedAt: o.now()}
	next := run
	next.State, next.Phase, next.LastFailure, next.NextRetryAt = contracts.AgentRunRunning, contracts.AgentPhaseModel, nil, nil
	next.PendingTools = clonePendingTools(run.PendingTools)[1:]
	next.History = append(cloneMessages(run.History), contracts.ModelMessage{Role: "tool", ToolCallID: call.ID, Content: stableToolResultContent(*outcome.Result)})
	committed, err := o.commit(ctx, run, next, contracts.AgentEventToolCompleted, map[string]any{"run_id": run.ID, "tool_step": step, "tool_result": stableToolResultContent(*outcome.Result)})
	if err != nil {
		return contracts.LoopDecision{}, err
	}
	return contracts.LoopDecision{Action: contracts.AgentActionAdvanced, Run: committed, Checkpoint: committed.Checkpoint(), Tool: &step}, nil
}

func (o *Orchestrator) advanceApproval(ctx context.Context, run contracts.AgentRun) (contracts.LoopDecision, error) {
	if len(run.PendingTools) == 0 {
		return o.fail(ctx, run, &contracts.LoopFailure{Code: contracts.AgentFailureApproval, Message: "approval state has no pending tool", Phase: contracts.AgentPhaseTool}, contracts.AgentTerminationAbstained)
	}
	call := run.PendingTools[0]
	if call.ApprovalID == "" || o.Approvals == nil {
		return waitingDecision(run), nil
	}
	approval, err := o.Approvals.GetApproval(ctx, run.WorkspaceID, call.ApprovalID)
	if err != nil {
		return waitingDecision(run), nil
	}
	switch approval.Status {
	case contracts.ApprovalPending:
		return waitingDecision(run), nil
	case contracts.ApprovalDenied, contracts.ApprovalExpired:
		failure := &contracts.LoopFailure{Code: contracts.AgentFailureApproval, Message: "tool approval was not granted", Phase: contracts.AgentPhaseTool}
		return o.fail(ctx, run, failure, contracts.AgentTerminationToolFailure)
	case contracts.ApprovalApproved:
		return o.advanceTool(ctx, run)
	default:
		return waitingDecision(run), nil
	}
}

func (o *Orchestrator) fail(ctx context.Context, run contracts.AgentRun, failure *contracts.LoopFailure, termination string) (contracts.LoopDecision, error) {
	return o.failWithAttempt(ctx, run, *failure, termination, failure.Attempt)
}

func (o *Orchestrator) failWithAttempt(ctx context.Context, run contracts.AgentRun, failure contracts.LoopFailure, termination string, attempt int) (contracts.LoopDecision, error) {
	next := run
	next.State, next.Termination, next.Phase, next.LastFailure, next.PendingTools = contracts.AgentRunFailed, termination, run.Phase, &failure, nil
	next.ModelAttempts = maxInt(next.ModelAttempts, attempt)
	committed, err := o.commit(ctx, run, next, contracts.AgentEventFailed, map[string]any{"run_id": run.ID, "failure": failure, "termination": termination})
	if err != nil {
		return contracts.LoopDecision{}, err
	}
	return terminalDecisionWithFailure(committed, &failure), nil
}

func (o *Orchestrator) failWithTool(ctx context.Context, run contracts.AgentRun, failure contracts.LoopFailure, termination string, attempt int) (contracts.LoopDecision, error) {
	next := run
	next.State, next.Termination, next.Phase, next.LastFailure, next.PendingTools = contracts.AgentRunFailed, termination, contracts.AgentPhaseTool, &failure, nil
	next.ToolCalls = maxInt(next.ToolCalls, run.ToolCalls)
	_ = attempt
	committed, err := o.commit(ctx, run, next, contracts.AgentEventFailed, map[string]any{"run_id": run.ID, "failure": failure, "termination": termination})
	if err != nil {
		return contracts.LoopDecision{}, err
	}
	return terminalDecisionWithFailure(committed, &failure), nil
}

func (o *Orchestrator) checkWallClock(run contracts.AgentRun) error {
	if run.CreatedAt.IsZero() {
		return nil
	}
	if o.now().After(run.CreatedAt.Add(time.Duration(run.Budget.MaxWallTimeMS) * time.Millisecond)) {
		return ErrLoopBudget
	}
	return nil
}

func (o *Orchestrator) now() time.Time {
	if o.Now == nil {
		return time.Now().UTC()
	}
	return o.Now().UTC()
}

func (o *Orchestrator) validateWorkerLease(ctx context.Context, run contracts.AgentRun) error {
	lease, owned := workerLeaseFromContext(ctx)
	if !owned {
		return o.Runs.ValidateTaskFence(ctx, run)
	}
	store, ok := o.Runs.(OwnedRunStore)
	if !ok {
		return ErrLoopLease
	}
	return store.ValidateAgentRunLease(ctx, run, lease)
}

func (o *Orchestrator) commit(ctx context.Context, current, next contracts.AgentRun, eventType string, payload any) (contracts.AgentRun, error) {
	lease, owned := workerLeaseFromContext(ctx)
	if !owned {
		return o.Runs.Commit(ctx, current, next, eventType, payload)
	}
	store, ok := o.Runs.(OwnedRunStore)
	if !ok {
		return contracts.AgentRun{}, ErrLoopLease
	}
	return store.CommitOwned(ctx, current, next, eventType, payload, lease)
}

type toolArguments struct {
	Argv        []string          `json:"argv"`
	Environment map[string]string `json:"env,omitempty"`
	Workdir     string            `json:"workdir,omitempty"`
}

func decodeToolArguments(raw []byte) (toolArguments, error) {
	if len(raw) == 0 {
		raw = []byte(`{}`)
	}
	var args toolArguments
	if err := json.Unmarshal(raw, &args); err != nil {
		return toolArguments{}, fmt.Errorf("tool arguments must be a JSON object: %w", err)
	}
	if len(args.Argv) == 0 {
		return toolArguments{}, fmt.Errorf("tool arguments require argv")
	}
	return args, nil
}

func stableToolResultContent(result contracts.ToolResult) string {
	value, _ := json.Marshal(struct {
		Status   string                 `json:"status"`
		ExitCode int                    `json:"exit_code"`
		Stdout   string                 `json:"stdout,omitempty"`
		Stderr   string                 `json:"stderr,omitempty"`
		Failure  *contracts.ToolFailure `json:"failure,omitempty"`
	}{result.Status, result.ExitCode, result.Stdout, result.Stderr, result.Failure})
	return string(value)
}

func stableContextContent(pack contracts.ContextPack) string {
	value, _ := json.Marshal(struct {
		Hash  string                  `json:"content_hash"`
		Items []contracts.ContextItem `json:"items"`
	}{pack.ContentHash, pack.Items})
	return "fornix_context\n" + string(value)
}

func loopFailureFromModelError(err error, phase string, attempt int) contracts.LoopFailure {
	failure := contracts.LoopFailure{Code: contracts.AgentFailureModel, Message: "model execution failed", Phase: phase, Attempt: attempt}
	var providerFailure *model.FailureError
	if errors.As(err, &providerFailure) {
		failure.Code, failure.Message, failure.Retryable = providerFailure.Failure.Code, providerFailure.Failure.Message, providerFailure.Failure.Retryable
		failure.ContentEmitted = providerFailure.Failure.ContentEmitted
	}
	return failure
}

func toolFailure(err error) *contracts.ToolFailure {
	var value *tool.FailureError
	if errors.As(err, &value) {
		return &value.Failure
	}
	return nil
}

func AgentToolFailureCode(code string) string { return "tool:" + strings.TrimSpace(code) }

func loopFailureFromError(err error, phase string) *contracts.LoopFailure {
	return &contracts.LoopFailure{Code: contracts.AgentFailureBudget, Message: err.Error(), Phase: phase}
}

func terminalDecision(run contracts.AgentRun) contracts.LoopDecision {
	return contracts.LoopDecision{Action: contracts.AgentActionTerminal, Run: run, Checkpoint: run.Checkpoint(), Failure: run.LastFailure}
}
func terminalDecisionWithFailure(run contracts.AgentRun, failure *contracts.LoopFailure) contracts.LoopDecision {
	decision := terminalDecision(run)
	decision.Failure = failure
	return decision
}
func waitingDecision(run contracts.AgentRun) contracts.LoopDecision {
	return contracts.LoopDecision{Action: contracts.AgentActionWaiting, Run: run, Checkpoint: run.Checkpoint(), Failure: run.LastFailure}
}

func stableID(prefix string, parts ...string) string {
	raw := strings.Join(append([]string{prefix}, parts...), "\x00")
	d := sha256.Sum256([]byte(raw))
	return prefix + "_" + hex.EncodeToString(d[:16])
}
func cloneMessages(in []contracts.ModelMessage) []contracts.ModelMessage {
	out := append([]contracts.ModelMessage(nil), in...)
	for i := range out {
		out[i].ToolCalls = cloneModelToolCalls(in[i].ToolCalls)
	}
	return out
}
func cloneModelToolCalls(in []contracts.ModelToolCall) []contracts.ModelToolCall {
	out := append([]contracts.ModelToolCall(nil), in...)
	for i := range out {
		out[i].Arguments = append([]byte(nil), in[i].Arguments...)
	}
	return out
}
func clonePendingTools(in []contracts.PendingToolCall) []contracts.PendingToolCall {
	out := append([]contracts.PendingToolCall(nil), in...)
	for i := range out {
		out[i].Arguments = append([]byte(nil), in[i].Arguments...)
		if in[i].LastFailure != nil {
			copy := *in[i].LastFailure
			out[i].LastFailure = &copy
		}
	}
	return out
}
func estimateHistoryBytes(history []contracts.ModelMessage) int {
	b, _ := json.Marshal(history)
	return len(b)
}
func estimateToolCatalogBytes(tools []contracts.ModelToolDefinition) int {
	b, _ := json.Marshal(tools)
	return len(b)
}
func ptrTime(value time.Time) *time.Time { value = value.UTC(); return &value }
func retryDelay(attempt int) time.Duration {
	delay := 100 * time.Millisecond
	for i := 1; i < attempt; i++ {
		if delay >= 10*time.Second {
			return 10 * time.Second
		}
		delay *= 2
	}
	if delay > 10*time.Second {
		return 10 * time.Second
	}
	return delay
}
func addCost(left, right contracts.ModelCost) contracts.ModelCost {
	return contracts.ModelCost{Currency: "USD", InputCostUSD: left.InputCostUSD + right.InputCostUSD, OutputCostUSD: left.OutputCostUSD + right.OutputCostUSD, TotalCostUSD: left.TotalCostUSD + right.TotalCostUSD, Source: "agent-aggregate"}
}
func minInt(left, right int) int {
	if left < right {
		return left
	}
	return right
}
func minInt64(left, right int64) int64 {
	if left < right {
		return left
	}
	return right
}
func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}
func maxFloat(left, right float64) float64 {
	if left > right {
		return left
	}
	return right
}
