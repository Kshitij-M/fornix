package tool

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func testDefinition(executable string) contracts.ToolDefinition {
	return contracts.ToolDefinition{ID: "test.tool", Name: "test", Version: "1", Capability: "test.execute", Executable: executable, Enabled: true, Sandbox: contracts.DefaultSandboxProfile()}
}

func testPolicy(t *testing.T, workspace, toolID, mode string) *Policy {
	t.Helper()
	policy, err := NewPolicy([]contracts.ToolPolicyRule{{ID: "allow", WorkspaceID: workspace, ToolID: toolID, Capability: "test.execute", Mode: mode, Enabled: true, Sandbox: contracts.DefaultSandboxProfile()}})
	if err != nil {
		t.Fatal(err)
	}
	return policy
}

func testRequest() contracts.ToolRequest {
	return contracts.ToolRequest{WorkspaceID: "w1", RequestID: "request-1", IdempotencyKey: "idempotency-1", Actor: contracts.ActorRef{ID: "actor-1", Kind: "test"}, ToolID: "test.tool", Capability: "test.execute", Argv: []string{"/bin/echo", "hello"}, Budget: contracts.DefaultSandboxProfile()}
}

func TestRegistryIsExplicitAndDeterministic(t *testing.T) {
	registry := NewRegistry()
	definition := testDefinition("/bin/echo")
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	if _, ok := registry.Lookup("TEST"); !ok {
		t.Fatal("expected case-insensitive name lookup")
	}
	if err := registry.Register(definition); err == nil {
		t.Fatal("expected duplicate registration to fail")
	}
	if got := registry.Names(); len(got) != 1 || got[0] != "test.tool" {
		t.Fatalf("unexpected names: %#v", got)
	}
}

func TestPolicyDeniesByDefaultAndScopesWorkspaceActorAndCapability(t *testing.T) {
	policy := testPolicy(t, "w1", "test.tool", contracts.ToolModeAutomatic)
	request := testRequest()
	definition := testDefinition("/bin/echo")
	if _, err := policy.Evaluate(request, definition); err != nil {
		t.Fatal(err)
	}
	request.WorkspaceID = "w2"
	if _, err := policy.Evaluate(request, definition); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected workspace deny, got %v", err)
	}
	request.WorkspaceID = "w1"
	request.Capability = "other"
	if _, err := policy.Evaluate(request, definition); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected capability deny, got %v", err)
	}
}

func TestPolicyWorkspaceRepositoryRegistrationIsScopedAndIdempotent(t *testing.T) {
	policy, err := NewPolicy(nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.RegisterWorkspaceTool("w1", "fornix.repository.read", "repository.read", "/workspace/repo"); err != nil {
		t.Fatal(err)
	}
	definition := contracts.ToolDefinition{ID: "fornix.repository.read", Name: "repository.read", Version: "1", Capability: "repository.read", Executable: "/bin/cat", Enabled: true, Sandbox: contracts.DefaultSandboxProfile()}
	request := contracts.ToolRequest{WorkspaceID: "w1", ToolID: definition.ID, Capability: definition.Capability, Argv: []string{"/bin/cat", "README.md"}, Workdir: "/workspace/repo", Budget: contracts.DefaultSandboxProfile()}
	if _, err := policy.Evaluate(request, definition); err != nil {
		t.Fatal(err)
	}
	request.WorkspaceID = "w2"
	if _, err := policy.Evaluate(request, definition); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("expected workspace-scoped deny, got %v", err)
	}
	if err := policy.RegisterWorkspaceTool("w1", definition.ID, definition.Capability, "/workspace/repo"); err != nil {
		t.Fatal(err)
	}
}

func TestLocalExecutorUsesStructuredArgvWithoutShellExpansion(t *testing.T) {
	request := testRequest()
	request.Argv = []string{"/bin/echo", "$(touch", "/tmp/fornix-must-not-exist)"}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	result, err := (LocalExecutor{}).Run(context.Background(), testDefinition("/bin/echo"), request)
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(result.Stdout) != "$(touch /tmp/fornix-must-not-exist)" {
		t.Fatalf("argv was not passed literally: %q", result.Stdout)
	}
}

func TestLocalExecutorEnforcesTimeoutAndOutputBudget(t *testing.T) {
	timeoutRequest := testRequest()
	timeoutRequest.Argv = []string{"/bin/sleep", "1"}
	timeoutRequest.Budget = contracts.DefaultSandboxProfile()
	timeoutRequest.Budget.TimeoutMS = 20
	if err := timeoutRequest.Normalize(); err != nil {
		t.Fatal(err)
	}
	timeoutResult, err := (LocalExecutor{}).Run(context.Background(), testDefinition("/bin/sleep"), timeoutRequest)
	if err != nil {
		t.Fatal(err)
	}
	if timeoutResult.Failure == nil || timeoutResult.Failure.Code != contracts.ToolFailureTimeout {
		t.Fatalf("expected timeout, got %#v", timeoutResult)
	}
	outputRequest := testRequest()
	outputRequest.Argv = []string{"/bin/echo", "0123456789"}
	outputRequest.Budget = contracts.DefaultSandboxProfile()
	outputRequest.Budget.MaxStdoutBytes = 4
	if err := outputRequest.Normalize(); err != nil {
		t.Fatal(err)
	}
	outputResult, err := (LocalExecutor{}).Run(context.Background(), testDefinition("/bin/echo"), outputRequest)
	if err != nil {
		t.Fatal(err)
	}
	if outputResult.Failure == nil || outputResult.Failure.Code != contracts.ToolFailureOutputLimit {
		t.Fatalf("expected output limit, got %#v", outputResult)
	}
}

func TestLocalExecutorRejectsFilesystemSymlinkEscapes(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}

	definition := testDefinition("/bin/cat")
	definition.WorkdirRoot = root
	definition.PathArgvIndexes = []int{1}
	definition.Sandbox.ReadOnlyWorkdir = true
	definition.Sandbox.AllowedWorkdirRoot = root
	request := testRequest()
	request.ToolID = definition.ID
	request.Capability = definition.Capability
	request.Argv = []string{"/bin/cat", "secret.txt"}
	request.Workdir = filepath.Join(root, "linked")
	request.Budget = definition.Sandbox
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	result, err := (LocalExecutor{}).Run(context.Background(), definition, request)
	var failureErr *FailureError
	if !errors.As(err, &failureErr) || failureErr.Failure.Code != contracts.ToolFailureWorkdirDenied {
		t.Fatalf("expected symlinked workdir rejection, result=%#v err=%v", result, err)
	}
}

func TestLocalExecutorRestrictsReadOnlyPathArguments(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(root, "linked.txt")); err != nil {
		t.Fatal(err)
	}

	definition := testDefinition("/bin/cat")
	definition.WorkdirRoot = root
	definition.PathArgvIndexes = []int{1}
	definition.Sandbox.ReadOnlyWorkdir = true
	definition.Sandbox.AllowedWorkdirRoot = root
	for _, path := range []string{"/etc/passwd", "../secret.txt", "linked.txt"} {
		request := testRequest()
		request.ToolID = definition.ID
		request.Capability = definition.Capability
		request.Argv = []string{"/bin/cat", path}
		request.Workdir = root
		request.Budget = definition.Sandbox
		if err := request.Normalize(); err != nil {
			t.Fatal(err)
		}
		result, err := (LocalExecutor{}).Run(context.Background(), definition, request)
		var failureErr *FailureError
		if !errors.As(err, &failureErr) || failureErr.Failure.Code != contracts.ToolFailureWorkdirDenied {
			t.Fatalf("expected path %q rejection, result=%#v err=%v", path, result, err)
		}
	}
}

func TestExecutorDuplicateAndTerminalReplayDoNotRunProcessTwice(t *testing.T) {
	store := newFakeRunStore()
	process := &countingProcess{result: contracts.ToolResult{Status: contracts.ToolRunSucceeded, Stdout: "ok"}}
	executor := &Executor{Registry: registryForTest(t), Policy: testPolicy(t, "w1", "test.tool", contracts.ToolModeAutomatic), Store: store, Process: process}
	request := testRequest()
	if _, err := executor.Execute(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	second, err := executor.Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Deduplicated || process.calls != 1 {
		t.Fatalf("duplicate was not replayed: dedup=%t calls=%d", second.Deduplicated, process.calls)
	}
}

func TestExecutorInteractiveApprovalThenContinuation(t *testing.T) {
	store := newFakeRunStore()
	executor := &Executor{Registry: registryForTest(t), Policy: testPolicy(t, "w1", "test.tool", contracts.ToolModeInteractive), Store: store, Process: &countingProcess{result: contracts.ToolResult{Status: contracts.ToolRunSucceeded, Stdout: "approved"}}}
	request := testRequest()
	out, err := executor.Execute(context.Background(), request)
	if !errors.Is(err, ErrApprovalRequired) || out.Approval == nil {
		t.Fatalf("expected durable approval request, outcome=%#v err=%v", out, err)
	}
	if _, err := store.DecideApproval(context.Background(), contracts.ApprovalDecision{WorkspaceID: "w1", ApprovalID: out.Approval.ID, Decision: contracts.ApprovalApproved, Actor: contracts.ActorRef{ID: "reviewer", Kind: "human"}}); err != nil {
		t.Fatal(err)
	}
	request.ApprovalID = out.Approval.ID
	continued, err := executor.Execute(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if continued.Result == nil || continued.Result.Status != contracts.ToolRunSucceeded {
		t.Fatalf("approval continuation did not execute: %#v", continued)
	}
}

func TestExecutorStaleFenceFailsClosedBeforeProcess(t *testing.T) {
	store := newFakeRunStore()
	process := &countingProcess{result: contracts.ToolResult{Status: contracts.ToolRunSucceeded}}
	executor := &Executor{Registry: registryForTest(t), Policy: testPolicy(t, "w1", "test.tool", contracts.ToolModeAutomatic), Store: store, Fence: rejectingFence{}, Process: process}
	out, err := executor.Execute(context.Background(), testRequest())
	if !errors.Is(err, ErrStaleTaskFence) || out.Result == nil || out.Result.Failure == nil || process.calls != 0 {
		t.Fatalf("stale fence was not rejected before process: outcome=%#v err=%v calls=%d", out, err, process.calls)
	}
}

func TestExecutorInFlightDuplicateFailsClosed(t *testing.T) {
	store := newFakeRunStore()
	request := testRequest()
	run, _, err := store.Reserve(context.Background(), request, contracts.ToolModeAutomatic)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkStarted(context.Background(), run); err != nil {
		t.Fatal(err)
	}
	executor := &Executor{Registry: registryForTest(t), Policy: testPolicy(t, "w1", "test.tool", contracts.ToolModeAutomatic), Store: store, Process: &countingProcess{result: contracts.ToolResult{Status: contracts.ToolRunSucceeded}}}
	_, err = executor.Execute(context.Background(), request)
	if !errors.Is(err, ErrRunInProgress) {
		t.Fatalf("in-flight duplicate error=%v", err)
	}
}

type countingProcess struct {
	mu     sync.Mutex
	calls  int
	result contracts.ToolResult
}

func (p *countingProcess) Run(context.Context, contracts.ToolDefinition, contracts.ToolRequest) (contracts.ToolResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	return p.result, nil
}

func registryForTest(t *testing.T) *Registry {
	t.Helper()
	r := NewRegistry()
	if err := r.Register(testDefinition("/bin/echo")); err != nil {
		t.Fatal(err)
	}
	return r
}

type rejectingFence struct{}

func (rejectingFence) ValidateTaskFence(context.Context, contracts.ToolRequest) error {
	return ErrStaleTaskFence
}

type fakeRunStore struct {
	mu        sync.Mutex
	runs      map[string]contracts.ToolRun
	approvals map[string]contracts.ApprovalRequest
}

func newFakeRunStore() *fakeRunStore {
	return &fakeRunStore{runs: map[string]contracts.ToolRun{}, approvals: map[string]contracts.ApprovalRequest{}}
}
func (s *fakeRunStore) Reserve(_ context.Context, req contracts.ToolRequest, mode string) (contracts.ToolRun, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := req.Normalize(); err != nil {
		return contracts.ToolRun{}, false, err
	}
	hash, _ := req.RequestHash()
	if run, ok := s.runs[req.IdempotencyKey]; ok {
		if run.RequestHash != hash {
			return contracts.ToolRun{}, false, ErrRunConflict
		}
		return run, true, nil
	}
	run := contracts.ToolRun{ID: contracts.NewID("run"), WorkspaceID: req.WorkspaceID, RequestID: req.RequestID, IdempotencyKey: req.IdempotencyKey, RequestHash: hash, SchemaVersion: req.SchemaVersion, CausationID: req.CausationID, CorrelationID: req.CorrelationID, ToolID: req.ToolID, Capability: req.Capability, Actor: req.Actor, Task: req.Task, Session: req.Session, Mode: mode, Status: contracts.ToolRunPending, CreatedAt: time.Now().UTC()}
	s.runs[req.IdempotencyKey] = run
	return run, false, nil
}
func (s *fakeRunStore) SetAwaitingApproval(_ context.Context, run contracts.ToolRun, approval contracts.ApprovalRequest) (contracts.ToolRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.runs[run.IdempotencyKey]
	current.Status, current.ApprovalID = contracts.ToolRunAwaitingApproval, approval.ID
	s.runs[run.IdempotencyKey] = current
	return current, nil
}
func (s *fakeRunStore) MarkStarted(_ context.Context, run contracts.ToolRun) (contracts.ToolRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.runs[run.IdempotencyKey]
	current.Status, current.Attempt = contracts.ToolRunRunning, current.Attempt+1
	s.runs[run.IdempotencyKey] = current
	return current, nil
}
func (s *fakeRunStore) Finish(_ context.Context, run contracts.ToolRun, result contracts.ToolResult) (contracts.ToolRun, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	current := s.runs[run.IdempotencyKey]
	current.Status, current.Result, current.Failure = result.Status, &result, result.Failure
	s.runs[run.IdempotencyKey] = current
	return current, nil
}
func (s *fakeRunStore) CreateApproval(_ context.Context, run contracts.ToolRun, req contracts.ToolRequest, ttl time.Duration) (contracts.ApprovalRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	approval := contracts.ApprovalRequest{ID: contracts.NewID("approval"), WorkspaceID: run.WorkspaceID, RequestID: req.RequestID, RunID: run.ID, RequestHash: run.RequestHash, ToolID: run.ToolID, Status: contracts.ApprovalPending, Actor: req.Actor, ExpiresAt: time.Now().UTC().Add(ttl), CreatedAt: time.Now().UTC()}
	s.approvals[approval.ID] = approval
	return approval, nil
}
func (s *fakeRunStore) GetApproval(_ context.Context, workspaceID, approvalID string) (contracts.ApprovalRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	approval, ok := s.approvals[approvalID]
	if !ok || approval.WorkspaceID != workspaceID {
		return contracts.ApprovalRequest{}, ErrApprovalRequired
	}
	return approval, nil
}
func (s *fakeRunStore) DecideApproval(_ context.Context, decision contracts.ApprovalDecision) (contracts.ApprovalRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	approval, ok := s.approvals[decision.ApprovalID]
	if !ok {
		return contracts.ApprovalRequest{}, ErrApprovalRequired
	}
	now := time.Now().UTC()
	approval.Status, approval.DecidedAt = decision.Decision, &now
	s.approvals[approval.ID] = approval
	return approval, nil
}
