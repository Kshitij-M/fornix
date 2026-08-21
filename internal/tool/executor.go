package tool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

// RunStore is the durable idempotent lifecycle boundary for tool runs and
// approvals.
type RunStore interface {
	Reserve(ctx context.Context, req contracts.ToolRequest, mode string) (contracts.ToolRun, bool, error)
	SetAwaitingApproval(ctx context.Context, run contracts.ToolRun, approval contracts.ApprovalRequest) (contracts.ToolRun, error)
	MarkStarted(ctx context.Context, run contracts.ToolRun) (contracts.ToolRun, error)
	Finish(ctx context.Context, run contracts.ToolRun, result contracts.ToolResult) (contracts.ToolRun, error)
	CreateApproval(ctx context.Context, run contracts.ToolRun, req contracts.ToolRequest, ttl time.Duration) (contracts.ApprovalRequest, error)
	GetApproval(ctx context.Context, workspaceID, approvalID string) (contracts.ApprovalRequest, error)
	DecideApproval(ctx context.Context, decision contracts.ApprovalDecision) (contracts.ApprovalRequest, error)
}

// FenceValidator rejects task-bound execution from stale workers.
type FenceValidator interface {
	ValidateTaskFence(ctx context.Context, req contracts.ToolRequest) error
}

// ProcessExecutor is the local or future sandbox implementation behind the
// policy and durable lifecycle.
type ProcessExecutor interface {
	Run(ctx context.Context, def contracts.ToolDefinition, req contracts.ToolRequest) (contracts.ToolResult, error)
}

// LocalExecutor runs a registered executable with structured argv and bounded
// process output. It is not a kernel sandbox.
type LocalExecutor struct{}

// Run executes one already-admitted tool request without invoking a shell.
func (LocalExecutor) Run(parent context.Context, def contracts.ToolDefinition, req contracts.ToolRequest) (contracts.ToolResult, error) {
	start := time.Now().UTC()
	profile := def.Sandbox
	if req.Budget.MaxStdoutBytes < profile.MaxStdoutBytes {
		profile.MaxStdoutBytes = req.Budget.MaxStdoutBytes
	}
	if req.Budget.MaxStderrBytes < profile.MaxStderrBytes {
		profile.MaxStderrBytes = req.Budget.MaxStderrBytes
	}
	if req.Budget.TimeoutMS < profile.TimeoutMS {
		profile.TimeoutMS = req.Budget.TimeoutMS
	}
	if len(req.Argv) == 0 || req.Argv[0] != def.Executable || !prefixMatches(req.Argv, append([]string{def.Executable}, def.ArgvPrefix...)) {
		return contracts.ToolResult{}, failure(contracts.ToolFailureInvalidRequest, "argv does not match registered tool", false)
	}
	if len(req.Argv) > profile.MaxArgCount {
		return contracts.ToolResult{}, failure(contracts.ToolFailureArgumentLimit, "argv exceeds tool budget", false)
	}
	for _, arg := range req.Argv {
		if len(arg) > profile.MaxArgBytes {
			return contracts.ToolResult{}, failure(contracts.ToolFailureArgumentLimit, "argv argument exceeds tool budget", false)
		}
	}
	workdir := req.Workdir
	if workdir == "" {
		workdir = def.WorkdirRoot
		if workdir == "" {
			workdir = profile.AllowedWorkdirRoot
		}
	}
	if workdir != "" && !withinRoot(workdir, firstNonEmpty(def.WorkdirRoot, profile.AllowedWorkdirRoot)) {
		return contracts.ToolResult{}, failure(contracts.ToolFailureWorkdirDenied, "working directory is outside tool policy", false)
	}
	root := firstNonEmpty(def.WorkdirRoot, profile.AllowedWorkdirRoot)
	canonicalRoot := root
	if root != "" {
		var err error
		canonicalRoot, err = filepath.EvalSymlinks(root)
		if err != nil {
			return contracts.ToolResult{}, failure(contracts.ToolFailureWorkdirDenied, "tool workdir root cannot be resolved", false)
		}
	}
	if workdir != "" {
		canonical, err := filepath.EvalSymlinks(workdir)
		if err != nil {
			return contracts.ToolResult{}, failure(contracts.ToolFailureWorkdirDenied, "working directory cannot be resolved", false)
		}
		if canonicalRoot != "" && !withinRoot(canonical, canonicalRoot) {
			return contracts.ToolResult{}, failure(contracts.ToolFailureWorkdirDenied, "working directory resolves outside tool policy", false)
		}
		workdir = canonical
	}
	if profile.ReadOnlyWorkdir && len(def.PathArgvIndexes) > 0 {
		if workdir == "" || canonicalRoot == "" {
			return contracts.ToolResult{}, failure(contracts.ToolFailureWorkdirDenied, "read-only path arguments require an authorized workdir", false)
		}
		for _, index := range def.PathArgvIndexes {
			if index >= len(req.Argv) || filepath.IsAbs(req.Argv[index]) {
				return contracts.ToolResult{}, failure(contracts.ToolFailureWorkdirDenied, "path argument is outside the authorized workdir", false)
			}
			candidate := filepath.Join(workdir, req.Argv[index])
			if !withinRoot(candidate, canonicalRoot) {
				return contracts.ToolResult{}, failure(contracts.ToolFailureWorkdirDenied, "path argument is outside the authorized workdir", false)
			}
			if resolved, err := filepath.EvalSymlinks(candidate); err == nil && !withinRoot(resolved, canonicalRoot) {
				return contracts.ToolResult{}, failure(contracts.ToolFailureWorkdirDenied, "path argument resolves outside the authorized workdir", false)
			}
		}
	}
	allowed := map[string]struct{}{}
	for _, key := range def.AllowedEnvKeys {
		allowed[key] = struct{}{}
	}
	env := make([]string, 0, len(req.Environment))
	keys := make([]string, 0, len(req.Environment))
	for key := range req.Environment {
		if _, ok := allowed[key]; !ok {
			return contracts.ToolResult{}, failure(contracts.ToolFailureEnvironmentLimit, "environment key is not allow-listed", false)
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		env = append(env, key+"="+req.Environment[key])
	}
	if len(env) > profile.MaxEnvEntries {
		return contracts.ToolResult{}, failure(contracts.ToolFailureEnvironmentLimit, "environment exceeds tool budget", false)
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(profile.TimeoutMS)*time.Millisecond)
	defer cancel()
	cmd := exec.CommandContext(ctx, def.Executable, req.Argv[1:]...)
	cmd.Env, cmd.Dir = env, workdir
	stdout := &boundedOutput{limit: profile.MaxStdoutBytes, cancel: cancel}
	stderr := &boundedOutput{limit: profile.MaxStderrBytes, cancel: cancel}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	waitErr := cmd.Run()
	out, errOut := stdout.Bytes(), stderr.Bytes()
	result := contracts.ToolResult{Status: contracts.ToolRunSucceeded, ExitCode: 0, Stdout: redactOutput(string(out), req.Environment), Stderr: redactOutput(string(errOut), req.Environment), StartedAt: start, FinishedAt: time.Now().UTC(), DurationMS: time.Since(start).Milliseconds()}
	result.ContentHash = result.Hash()
	if stdout.Overflow() || stderr.Overflow() {
		result.Status = contracts.ToolRunFailed
		result.Failure = &contracts.ToolFailure{Code: contracts.ToolFailureOutputLimit, Message: "tool output exceeded budget"}
		return result, nil
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		result.Status = contracts.ToolRunFailed
		result.Failure = &contracts.ToolFailure{Code: contracts.ToolFailureTimeout, Message: "tool execution timed out", Retryable: true}
		return result, nil
	}
	if errors.Is(ctx.Err(), context.Canceled) && parent.Err() != nil {
		result.Status = contracts.ToolRunCancelled
		result.Failure = &contracts.ToolFailure{Code: contracts.ToolFailureCancelled, Message: "tool execution cancelled"}
		return result, nil
	}
	if waitErr != nil {
		result.Status = contracts.ToolRunFailed
		result.Failure = &contracts.ToolFailure{Code: contracts.ToolFailureExecution, Message: "tool exited unsuccessfully"}
		if exitErr, ok := waitErr.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		}
		return result, nil
	}
	return result, nil
}

func redactOutput(value string, secrets map[string]string) string {
	for _, secret := range secrets {
		if strings.TrimSpace(secret) != "" {
			value = strings.ReplaceAll(value, secret, "[REDACTED]")
		}
	}
	return value
}

var errOutputLimit = errors.New("tool output limit exceeded")

type boundedOutput struct {
	mu       sync.Mutex
	buffer   bytes.Buffer
	limit    int
	overflow bool
	cancel   context.CancelFunc
}

// Write captures at most the configured output limit and cancels the process
// when the child attempts to exceed it.
func (w *boundedOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	remaining := w.limit - w.buffer.Len()
	if remaining > 0 {
		if len(p) > remaining {
			_, _ = w.buffer.Write(p[:remaining])
		} else {
			_, _ = w.buffer.Write(p)
		}
	}
	if len(p) > remaining {
		w.overflow = true
		w.cancel()
		return len(p), errOutputLimit
	}
	return len(p), nil
}

// Bytes returns a defensive copy of the bounded output.
func (w *boundedOutput) Bytes() []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]byte(nil), w.buffer.Bytes()...)
}

// Overflow reports whether the process exceeded its output limit.
func (w *boundedOutput) Overflow() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.overflow
}
func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func mergeSandboxProfiles(definition, policy contracts.SandboxProfile) contracts.SandboxProfile {
	out := definition
	if policy.TimeoutMS > 0 && policy.TimeoutMS < out.TimeoutMS {
		out.TimeoutMS = policy.TimeoutMS
	}
	if policy.MaxStdoutBytes > 0 && policy.MaxStdoutBytes < out.MaxStdoutBytes {
		out.MaxStdoutBytes = policy.MaxStdoutBytes
	}
	if policy.MaxStderrBytes > 0 && policy.MaxStderrBytes < out.MaxStderrBytes {
		out.MaxStderrBytes = policy.MaxStderrBytes
	}
	if policy.MaxArgCount > 0 && policy.MaxArgCount < out.MaxArgCount {
		out.MaxArgCount = policy.MaxArgCount
	}
	if policy.MaxArgBytes > 0 && policy.MaxArgBytes < out.MaxArgBytes {
		out.MaxArgBytes = policy.MaxArgBytes
	}
	if policy.MaxEnvEntries > 0 && policy.MaxEnvEntries < out.MaxEnvEntries {
		out.MaxEnvEntries = policy.MaxEnvEntries
	}
	if policy.MaxEnvBytes > 0 && policy.MaxEnvBytes < out.MaxEnvBytes {
		out.MaxEnvBytes = policy.MaxEnvBytes
	}
	return out
}

func intersectStrings(left, right []string) []string {
	allowed := make(map[string]struct{}, len(right))
	for _, value := range right {
		allowed[value] = struct{}{}
	}
	out := make([]string, 0, len(left))
	for _, value := range left {
		if _, ok := allowed[value]; ok {
			out = append(out, value)
		}
	}
	return out
}

// Outcome combines durable tool-run state with a result or approval gate.
type Outcome struct {
	Run          contracts.ToolRun          `json:"run"`
	Result       *contracts.ToolResult      `json:"result,omitempty"`
	Approval     *contracts.ApprovalRequest `json:"approval,omitempty"`
	Deduplicated bool                       `json:"deduplicated,omitempty"`
}

// Executor composes registry, deny-by-default policy, durable lifecycle, task
// fencing, and the process implementation.
type Executor struct {
	Registry    *Registry
	Policy      *Policy
	Store       RunStore
	Fence       FenceValidator
	Process     ProcessExecutor
	ApprovalTTL time.Duration
}

// Definition exposes the registered capability metadata to orchestration
// layers. It is a lookup only; policy evaluation still happens in Execute.
func (e *Executor) Definition(name string) (contracts.ToolDefinition, bool) {
	if e == nil || e.Registry == nil {
		return contracts.ToolDefinition{}, false
	}
	return e.Registry.Lookup(name)
}

// Execute admits and runs one tool request. Duplicate durable requests replay
// their existing outcome; remote or local execution is otherwise at-least-once.
func (e *Executor) Execute(ctx context.Context, req contracts.ToolRequest) (Outcome, error) {
	if e == nil || e.Registry == nil || e.Policy == nil || e.Store == nil {
		return Outcome{}, fmt.Errorf("tool executor is not configured")
	}
	if err := req.Normalize(); err != nil {
		return Outcome{}, failure(contracts.ToolFailureInvalidRequest, err.Error(), false)
	}
	def, ok := e.Registry.Lookup(req.ToolID)
	if !ok {
		return Outcome{}, failure(contracts.ToolFailureUnknownTool, "tool is not registered", false)
	}
	if req.Capability == "" {
		req.Capability = def.Capability
	}
	decision, err := e.Policy.Evaluate(req, def)
	if err != nil {
		return e.finishDenied(ctx, req, contracts.ToolFailureUnauthorized, err.Error())
	}
	run, existing, err := e.Store.Reserve(ctx, req, decision.Mode)
	if err != nil {
		return Outcome{}, err
	}
	approvedContinuation := false
	if existing {
		if run.Status == contracts.ToolRunAwaitingApproval && strings.TrimSpace(req.ApprovalID) != "" {
			approval, approvalErr := e.Store.GetApproval(ctx, req.WorkspaceID, req.ApprovalID)
			approvedContinuation = approvalErr == nil && approval.Status == contracts.ApprovalApproved && approval.RequestHash == run.RequestHash && approval.ExpiresAt.After(time.Now().UTC())
		}
		if !approvedContinuation {
			return e.replayOrConflict(run)
		}
	}
	out := Outcome{Run: run}
	if decision.Mode == contracts.ToolModeDenied {
		return e.finishDeniedRun(ctx, run, contracts.ToolFailureUnauthorized, "tool policy denied execution")
	}
	if decision.Mode == contracts.ToolModeInteractive && !approvedContinuation {
		approval, createErr := e.Store.CreateApproval(ctx, run, req, e.approvalTTL())
		if createErr != nil {
			return Outcome{}, createErr
		}
		updated, setErr := e.Store.SetAwaitingApproval(ctx, run, approval)
		if setErr != nil {
			return Outcome{}, setErr
		}
		out.Run, out.Approval = updated, &approval
		return out, failure(contracts.ToolFailureApprovalRequired, "interactive approval is required", false)
	}
	if decision.Mode == contracts.ToolModePreApproved {
		approval, getErr := e.Store.GetApproval(ctx, req.WorkspaceID, req.ApprovalID)
		if getErr != nil {
			return e.finishDeniedRun(ctx, run, contracts.ToolFailureApprovalRequired, "approved grant is required")
		}
		if approval.Status != contracts.ApprovalApproved || approval.RequestHash != run.RequestHash || approval.ExpiresAt.Before(time.Now().UTC()) {
			return e.finishDeniedRun(ctx, run, contracts.ToolFailureApprovalDenied, "approval is missing, expired, or does not match request")
		}
	}
	if e.Fence != nil {
		if err := e.Fence.ValidateTaskFence(ctx, req); err != nil {
			return e.finishStale(ctx, run, err)
		}
	}
	started, err := e.Store.MarkStarted(ctx, run)
	if err != nil {
		return e.finishStale(ctx, run, err)
	}
	effectiveDefinition := def
	effectiveDefinition.Sandbox = mergeSandboxProfiles(def.Sandbox, decision.Rule.Sandbox)
	if decision.Rule.WorkdirRoot != "" {
		effectiveDefinition.WorkdirRoot = decision.Rule.WorkdirRoot
		effectiveDefinition.Sandbox.AllowedWorkdirRoot = decision.Rule.WorkdirRoot
	}
	if len(decision.Rule.AllowedEnvKeys) > 0 {
		effectiveDefinition.AllowedEnvKeys = intersectStrings(def.AllowedEnvKeys, decision.Rule.AllowedEnvKeys)
	}
	result, runErr := e.process().Run(ctx, effectiveDefinition, req)
	if runErr != nil {
		var failureErr *FailureError
		if errors.As(runErr, &failureErr) {
			failedResult := contracts.ToolResult{RequestID: started.RequestID, RunID: started.ID, ToolID: started.ToolID, Status: contracts.ToolRunFailed, Failure: &failureErr.Failure}
			finished, finishErr := e.Store.Finish(ctx, started, failedResult)
			if finishErr != nil {
				return Outcome{Run: started, Result: &failedResult}, finishErr
			}
			return Outcome{Run: finished, Result: &failedResult}, runErr
		}
		return Outcome{Run: started}, runErr
	}
	finished, finishErr := e.Store.Finish(ctx, started, result)
	if finishErr != nil {
		return Outcome{Run: started, Result: &result}, finishErr
	}
	out.Run, out.Result = finished, &result
	if result.Failure != nil {
		return out, &FailureError{Failure: *result.Failure}
	}
	return out, nil
}

func (e *Executor) process() ProcessExecutor {
	if e.Process != nil {
		return e.Process
	}
	return LocalExecutor{}
}
func (e *Executor) approvalTTL() time.Duration {
	if e.ApprovalTTL <= 0 {
		return 10 * time.Minute
	}
	return e.ApprovalTTL
}
func (e *Executor) replayOrConflict(run contracts.ToolRun) (Outcome, error) {
	if run.Status == contracts.ToolRunSucceeded || run.Status == contracts.ToolRunFailed || run.Status == contracts.ToolRunDenied || run.Status == contracts.ToolRunCancelled {
		out := Outcome{Run: run, Deduplicated: true}
		if run.Result != nil {
			out.Result = run.Result
		}
		return out, nil
	}
	return Outcome{Run: run, Deduplicated: true}, failure(contracts.ToolFailureInProgress, "tool run is already reserved or running", false)
}
func (e *Executor) finishDenied(ctx context.Context, req contracts.ToolRequest, code, message string) (Outcome, error) {
	run, existing, err := e.Store.Reserve(ctx, req, contracts.ToolModeDenied)
	if err != nil {
		return Outcome{}, err
	}
	if existing {
		return e.replayOrConflict(run)
	}
	return e.finishDeniedRun(ctx, run, code, message)
}
func (e *Executor) finishDeniedRun(ctx context.Context, run contracts.ToolRun, code, message string) (Outcome, error) {
	result := contracts.ToolResult{RequestID: run.RequestID, RunID: run.ID, ToolID: run.ToolID, Status: contracts.ToolRunDenied, Failure: &contracts.ToolFailure{Code: code, Message: message}}
	finished, err := e.Store.Finish(ctx, run, result)
	if err != nil {
		return Outcome{Run: run, Result: &result}, err
	}
	return Outcome{Run: finished, Result: &result}, &FailureError{Failure: *result.Failure}
}
func (e *Executor) finishStale(ctx context.Context, run contracts.ToolRun, err error) (Outcome, error) {
	return e.finishDeniedRun(ctx, run, contracts.ToolFailureStaleFence, err.Error())
}
