package agentloop

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/model"
	"github.com/omaveda/fornix/internal/tool"
)

type memoryRuns struct {
	mu     sync.Mutex
	runs   map[string]contracts.AgentRun
	events []contracts.EventEnvelope
	seq    uint64
}

func newMemoryRuns() *memoryRuns { return &memoryRuns{runs: make(map[string]contracts.AgentRun)} }

func (s *memoryRuns) Reserve(_ context.Context, request contracts.AgentRunRequest) (contracts.AgentRun, bool, error) {
	if err := request.Normalize(); err != nil {
		return contracts.AgentRun{}, false, err
	}
	hash, err := request.RequestHash()
	if err != nil {
		return contracts.AgentRun{}, false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.runs[request.IdempotencyKey]; ok {
		if existing.RequestHash != hash {
			return contracts.AgentRun{}, false, errors.New("idempotency conflict")
		}
		return existing, true, nil
	}
	now := time.Unix(100, 0).UTC()
	run := contracts.AgentRun{ID: request.RunID, WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey, RequestHash: hash, SchemaVersion: request.SchemaVersion, Actor: request.Actor, Goal: request.Goal, Provider: request.Provider, Tools: request.Tools, Retrieval: request.Retrieval, Budget: request.Budget, State: contracts.AgentRunPending, Phase: contracts.AgentPhaseModel, History: []contracts.ModelMessage{{Role: "user", Content: request.Goal}}, StateVersion: 1, CreatedAt: now, UpdatedAt: now}
	run.StateHash = run.ComputeStateHash()
	s.seq++
	s.runs[run.IdempotencyKey] = run
	event, _ := contracts.NewEvent(contracts.AgentEventCreated, map[string]any{"run_id": run.ID})
	event.EventType, event.Scope.WorkspaceID, event.Sequence = contracts.AgentEventCreated, run.WorkspaceID, s.seq
	event.Payload, _ = json.Marshal(map[string]any{"run_id": run.ID})
	s.events = append(s.events, event)
	run.EventSequence = s.seq
	s.runs[run.IdempotencyKey] = run
	return run, false, nil
}

func (s *memoryRuns) Get(_ context.Context, workspaceID, runID string) (contracts.AgentRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, run := range s.runs {
		if run.WorkspaceID == workspaceID && run.ID == runID {
			return run, nil
		}
	}
	return contracts.AgentRun{}, fmt.Errorf("run not found")
}

func (s *memoryRuns) Commit(_ context.Context, current, next contracts.AgentRun, eventType string, payload any) (contracts.AgentRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	stored, ok := s.runs[current.IdempotencyKey]
	if !ok || stored.StateVersion != current.StateVersion {
		return contracts.AgentRun{}, errors.New("state version conflict")
	}
	next.StateVersion = current.StateVersion + 1
	next.EventSequence = s.seq + 1
	next.UpdatedAt = stored.UpdatedAt.Add(time.Second)
	next.StateHash = next.ComputeStateHash()
	s.seq++
	event, err := contracts.NewEvent(eventType, payload)
	if err != nil {
		return contracts.AgentRun{}, err
	}
	event.Scope.WorkspaceID, event.Sequence = next.WorkspaceID, s.seq
	s.events = append(s.events, event)
	s.runs[current.IdempotencyKey] = next
	return next, nil
}

func (s *memoryRuns) ValidateTaskFence(context.Context, contracts.AgentRun) error { return nil }

func (s *memoryRuns) Replay(_ context.Context, workspaceID, runID string, from uint64, limit int) ([]contracts.EventEnvelope, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]contracts.EventEnvelope, 0)
	for _, event := range s.events {
		if event.Scope.WorkspaceID != workspaceID || event.Sequence <= from || len(result) >= limit {
			continue
		}
		var payload struct {
			RunID string `json:"run_id"`
		}
		if json.Unmarshal(event.Payload, &payload) == nil && payload.RunID == runID {
			result = append(result, event)
		}
	}
	return result, nil
}

type scriptedModel struct {
	mu        sync.Mutex
	responses []contracts.ModelResponse
	requests  []contracts.ModelRequest
}

type retryModel struct{ calls int }

func (m *retryModel) Complete(_ context.Context, request contracts.ModelRequest, _ ...contracts.ProviderRef) (contracts.ModelResponse, error) {
	m.calls++
	if m.calls == 1 {
		return contracts.ModelResponse{}, &model.FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureTransport, Message: "transient", Retryable: true}}
	}
	return contracts.ModelResponse{RequestID: request.RequestID, Provider: request.Provider, Content: "recovered", FinishReason: "stop", Usage: contracts.ModelUsage{InputTokens: 2, OutputTokens: 1, TotalTokens: 3}}, nil
}

func (m *scriptedModel) Complete(_ context.Context, request contracts.ModelRequest, _ ...contracts.ProviderRef) (contracts.ModelResponse, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.requests = append(m.requests, request)
	if len(m.responses) == 0 {
		return contracts.ModelResponse{}, errors.New("no scripted response")
	}
	response := m.responses[0]
	m.responses = m.responses[1:]
	response.RequestID = request.RequestID
	if response.Provider.Provider == "" {
		response.Provider = request.Provider
	}
	return response, nil
}

type fakeTools struct {
	definition contracts.ToolDefinition
	calls      int
	approval   bool
	executed   int
}

func (t *fakeTools) Definition(id string) (contracts.ToolDefinition, bool) {
	return t.definition, id == t.definition.ID
}

func (t *fakeTools) Execute(_ context.Context, request contracts.ToolRequest) (tool.Outcome, error) {
	t.calls++
	if t.approval {
		return tool.Outcome{Run: contracts.ToolRun{ID: "tool-run", WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, ToolID: request.ToolID, Status: contracts.ToolRunAwaitingApproval, ApprovalID: "approval-1"}, Approval: &contracts.ApprovalRequest{ID: "approval-1", WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, RunID: "tool-run", ToolID: request.ToolID, Status: contracts.ApprovalPending}}, &tool.FailureError{Failure: contracts.ToolFailure{Code: contracts.ToolFailureApprovalRequired, Message: "approval required"}}
	}
	t.executed++
	result := contracts.ToolResult{RequestID: request.RequestID, RunID: "tool-run", ToolID: request.ToolID, Status: contracts.ToolRunSucceeded, Stdout: "tool-output"}
	return tool.Outcome{Run: contracts.ToolRun{ID: "tool-run", WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, ToolID: request.ToolID, Status: contracts.ToolRunSucceeded}, Result: &result}, nil
}

type fakeApprovalReader struct{ status string }

func (a *fakeApprovalReader) GetApproval(_ context.Context, workspaceID, approvalID string) (contracts.ApprovalRequest, error) {
	if approvalID != "approval-1" {
		return contracts.ApprovalRequest{}, errors.New("approval not found")
	}
	return contracts.ApprovalRequest{ID: approvalID, WorkspaceID: workspaceID, Status: a.status}, nil
}

type fakeRetriever struct {
	calls int
	pack  contracts.ContextPack
}

func (r *fakeRetriever) Retrieve(_ context.Context, request contracts.RetrievalRequest) (contracts.ContextPack, error) {
	r.calls++
	if request.WorkspaceID != r.pack.WorkspaceID {
		return contracts.ContextPack{}, errors.New("workspace leak")
	}
	return r.pack, nil
}

func agentRequest(workspace, key string) contracts.AgentRunRequest {
	return contracts.AgentRunRequest{RunID: "run-" + key, RequestID: "request-" + key, IdempotencyKey: key, WorkspaceID: workspace, Goal: "deterministic goal", Provider: contracts.ProviderRef{Provider: "fake", Model: "fake-model"}, Budget: contracts.AgentBudget{MaxTurns: 4, MaxModelSteps: 4, MaxToolCalls: 4, MaxContextBytes: 4096, MaxOutputTokens: 128, MaxWallTimeMS: 60_000, MaxCostUSD: 1, MaxToolAttempts: 2}}
}

func TestRunCompilesContextOnceAndReplaysDeterministically(t *testing.T) {
	makeLoop := func() (*Orchestrator, *memoryRuns, *fakeRetriever, *scriptedModel) {
		runs := newMemoryRuns()
		retriever := &fakeRetriever{pack: contracts.ContextPack{WorkspaceID: "workspace-a", ContentHash: "context-hash", Items: []contracts.ContextItem{{WorkspaceID: "workspace-a", SourceReference: "memo:1", EvidenceHash: "evidence-1", Kind: "memo", Text: "bounded evidence"}}, TotalBytes: 16, TotalTokens: 4}}
		model := &scriptedModel{responses: []contracts.ModelResponse{{Content: "stable output", FinishReason: "stop", Usage: contracts.ModelUsage{InputTokens: 4, OutputTokens: 3, TotalTokens: 7}, Cost: contracts.ModelCost{Currency: "USD", TotalCostUSD: 0.01}}}}
		loop := New(runs, model, &fakeTools{})
		loop.Retriever = retriever
		loop.Now = func() time.Time { return time.Unix(101, 0).UTC() }
		return loop, runs, retriever, model
	}
	firstLoop, firstRuns, firstRetriever, firstModel := makeLoop()
	first, _, err := firstLoop.Create(context.Background(), agentRequest("workspace-a", "same-key"))
	if err != nil {
		t.Fatal(err)
	}
	firstDecision, err := firstLoop.Run(context.Background(), first.WorkspaceID, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	secondLoop, secondRuns, secondRetriever, secondModel := makeLoop()
	second, _, err := secondLoop.Create(context.Background(), agentRequest("workspace-a", "same-key"))
	if err != nil {
		t.Fatal(err)
	}
	secondDecision, err := secondLoop.Run(context.Background(), second.WorkspaceID, second.ID)
	if err != nil {
		t.Fatal(err)
	}
	if firstDecision.Run.State != contracts.AgentRunSucceeded || secondDecision.Run.State != contracts.AgentRunSucceeded {
		t.Fatalf("runs did not succeed: first=%+v second=%+v", firstDecision.Run, secondDecision.Run)
	}
	if firstDecision.Run.StateHash != secondDecision.Run.StateHash || firstDecision.Checkpoint.HistoryHash != secondDecision.Checkpoint.HistoryHash || firstDecision.Run.LastOutput != secondDecision.Run.LastOutput {
		t.Fatalf("replay was not deterministic: first=%+v second=%+v", firstDecision.Checkpoint, secondDecision.Checkpoint)
	}
	if firstRetriever.calls != 1 || secondRetriever.calls != 1 || len(firstModel.requests) != 1 || len(secondModel.requests) != 1 {
		t.Fatalf("context/model calls were not bounded: retrievers=%d/%d models=%d/%d", firstRetriever.calls, secondRetriever.calls, len(firstModel.requests), len(secondModel.requests))
	}
	if firstDecision.Run.ContextHash != "context-hash" || len(firstDecision.Run.History) != 3 || firstRuns.seq != 3 || secondRuns.seq != 3 {
		t.Fatalf("unexpected durable history: run=%+v first_events=%d second_events=%d", firstDecision.Run, firstRuns.seq, secondRuns.seq)
	}
	events, err := firstRuns.Replay(context.Background(), "workspace-a", first.ID, 0, 100)
	if err != nil || len(events) != 3 {
		t.Fatalf("run-scoped replay=%d err=%v", len(events), err)
	}
}

func TestRunToolStepIsOrderedAndCancellationIsDurable(t *testing.T) {
	runs := newMemoryRuns()
	model := &scriptedModel{responses: []contracts.ModelResponse{
		{ToolCalls: []contracts.ModelToolCall{{ID: "call-1", ToolID: "tool.echo", Arguments: json.RawMessage(`{"argv":["hello"]}`)}}, FinishReason: "tool_calls", Usage: contracts.ModelUsage{InputTokens: 4, TotalTokens: 4}},
		{Content: "after tool", FinishReason: "stop", Usage: contracts.ModelUsage{InputTokens: 8, OutputTokens: 2, TotalTokens: 10}},
	}}
	tools := &fakeTools{definition: contracts.ToolDefinition{ID: "tool.echo", Name: "echo", Version: "1", Capability: "process.echo", Executable: "/bin/echo"}}
	loop := New(runs, model, tools)
	loop.Now = func() time.Time { return time.Unix(101, 0).UTC() }
	run, _, err := loop.Create(context.Background(), agentRequest("workspace-tools", "tool-key"))
	if err != nil {
		t.Fatal(err)
	}
	decision, err := loop.Run(context.Background(), run.WorkspaceID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if decision.Run.State != contracts.AgentRunSucceeded || tools.calls != 1 || len(decision.Run.History) != 4 {
		t.Fatalf("tool loop did not preserve order: decision=%+v calls=%d history=%d", decision, tools.calls, len(decision.Run.History))
	}
	if decision.Run.History[1].Role != "assistant" || len(decision.Run.History[1].ToolCalls) != 1 || decision.Run.History[2].Role != "tool" || decision.Run.History[3].Role != "assistant" {
		t.Fatalf("unexpected model/tool history: %+v", decision.Run.History)
	}

	cancelRuns := newMemoryRuns()
	cancelLoop := New(cancelRuns, &scriptedModel{responses: []contracts.ModelResponse{{Content: "must not run"}}}, &fakeTools{})
	cancelLoop.Now = func() time.Time { return time.Unix(101, 0).UTC() }
	cancelled, _, err := cancelLoop.Create(context.Background(), agentRequest("workspace-cancel", "cancel-key"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cancelLoop.Cancel(context.Background(), cancelled.WorkspaceID, cancelled.ID, "operator stop"); err != nil {
		t.Fatal(err)
	}
	terminal, err := cancelLoop.Run(context.Background(), cancelled.WorkspaceID, cancelled.ID)
	if err != nil || terminal.Run.State != contracts.AgentRunCancelled || terminal.Run.Termination != contracts.AgentTerminationCancelled {
		t.Fatalf("cancellation was not durable: %+v err=%v", terminal, err)
	}
}

func TestRunApprovalPausesAndResumesWithoutRepeatingModelEffect(t *testing.T) {
	runs := newMemoryRuns()
	model := &scriptedModel{responses: []contracts.ModelResponse{
		{ToolCalls: []contracts.ModelToolCall{{ID: "approval-call", ToolID: "tool.echo", Arguments: json.RawMessage(`{"argv":["approved"]}`)}}, FinishReason: "tool_calls", Usage: contracts.ModelUsage{InputTokens: 4, TotalTokens: 4}},
		{Content: "approved output", FinishReason: "stop", Usage: contracts.ModelUsage{InputTokens: 8, OutputTokens: 2, TotalTokens: 10}},
	}}
	tools := &fakeTools{definition: contracts.ToolDefinition{ID: "tool.echo", Name: "echo", Version: "1", Capability: "process.echo", Executable: "/bin/echo"}, approval: true}
	approvals := &fakeApprovalReader{status: contracts.ApprovalPending}
	loop := New(runs, model, tools)
	loop.Approvals, loop.Now = approvals, func() time.Time { return time.Unix(101, 0).UTC() }
	run, _, err := loop.Create(context.Background(), agentRequest("workspace-approval", "approval-key"))
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := loop.Run(context.Background(), run.WorkspaceID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if waiting.Action != contracts.AgentActionWaiting || waiting.Run.State != contracts.AgentRunAwaitingApproval || len(model.requests) != 1 || tools.executed != 0 {
		t.Fatalf("approval did not pause before effect: decision=%+v model_calls=%d executed=%d", waiting, len(model.requests), tools.executed)
	}
	tools.approval = false
	approvals.status = contracts.ApprovalApproved
	finished, err := loop.Run(context.Background(), run.WorkspaceID, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if finished.Run.State != contracts.AgentRunSucceeded || len(model.requests) != 2 || tools.executed != 1 {
		t.Fatalf("approval resume duplicated or failed: decision=%+v model_calls=%d executed=%d", finished, len(model.requests), tools.executed)
	}
}

func TestRunRetryHonorsDurableWaitAndDoesNotRetryContentEmittedFailure(t *testing.T) {
	clock := time.Unix(101, 0).UTC()
	runs := newMemoryRuns()
	model := &retryModel{}
	loop := New(runs, model, &fakeTools{})
	loop.Now = func() time.Time { return clock }
	run, _, err := loop.Create(context.Background(), agentRequest("workspace-retry", "retry-key"))
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := loop.Run(context.Background(), run.WorkspaceID, run.ID)
	if err != nil || waiting.Run.State != contracts.AgentRunAwaitingRetry || waiting.Action != contracts.AgentActionWaiting {
		t.Fatalf("retry did not become durable wait: decision=%+v err=%v", waiting, err)
	}
	clock = clock.Add(time.Second)
	finished, err := loop.Run(context.Background(), run.WorkspaceID, run.ID)
	if err != nil || finished.Run.State != contracts.AgentRunSucceeded || model.calls != 2 {
		t.Fatalf("retry did not resume deterministically: decision=%+v calls=%d err=%v", finished, model.calls, err)
	}

	funcModel := funcModelError{failure: contracts.ModelFailure{Code: contracts.ModelFailureTransport, Message: "partial", Retryable: true, ContentEmitted: true}}
	noRetryRuns := newMemoryRuns()
	noRetryLoop := New(noRetryRuns, &funcModel, &fakeTools{})
	noRetryLoop.Now = func() time.Time { return clock }
	noRetryRun, _, err := noRetryLoop.Create(context.Background(), agentRequest("workspace-no-retry", "no-retry-key"))
	if err != nil {
		t.Fatal(err)
	}
	noRetry, err := noRetryLoop.Run(context.Background(), noRetryRun.WorkspaceID, noRetryRun.ID)
	if err != nil || noRetry.Run.State != contracts.AgentRunFailed || noRetry.Run.State == contracts.AgentRunAwaitingRetry {
		t.Fatalf("content-emitted failure was retried: decision=%+v err=%v", noRetry, err)
	}
}

func TestRunExternalWaitAndCompletionAreDurableAndDuplicateSafe(t *testing.T) {
	runs := newMemoryRuns()
	loop := New(runs, &scriptedModel{responses: []contracts.ModelResponse{{Content: "must not execute"}}}, &fakeTools{})
	loop.Now = func() time.Time { return time.Unix(101, 0).UTC() }
	run, _, err := loop.Create(context.Background(), agentRequest("workspace-external", "external-key"))
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := loop.WaitExternal(context.Background(), run.WorkspaceID, run.ID, "awaiting deployment")
	if err != nil || waiting.Run.State != contracts.AgentRunAwaitingExternal {
		t.Fatalf("external wait was not durable: decision=%+v err=%v", waiting, err)
	}
	stillWaiting, err := loop.Run(context.Background(), run.WorkspaceID, run.ID)
	if err != nil || stillWaiting.Action != contracts.AgentActionWaiting {
		t.Fatalf("waiting run admitted work: decision=%+v err=%v", stillWaiting, err)
	}
	completed, err := loop.CompleteExternal(context.Background(), run.WorkspaceID, run.ID, "deployment finished")
	if err != nil || completed.Run.State != contracts.AgentRunSucceeded || completed.Run.LastOutput != "deployment finished" {
		t.Fatalf("external completion failed: decision=%+v err=%v", completed, err)
	}
	duplicate, err := loop.CompleteExternal(context.Background(), run.WorkspaceID, run.ID, "different duplicate")
	if err != nil || duplicate.Run.State != contracts.AgentRunSucceeded || duplicate.Run.LastOutput != "deployment finished" {
		t.Fatalf("duplicate completion changed terminal effect: decision=%+v err=%v", duplicate, err)
	}
}

type funcModelError struct{ failure contracts.ModelFailure }

func (m *funcModelError) Complete(_ context.Context, _ contracts.ModelRequest, _ ...contracts.ProviderRef) (contracts.ModelResponse, error) {
	return contracts.ModelResponse{}, &model.FailureError{Failure: m.failure}
}
