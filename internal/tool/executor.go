package tool

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
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

// ProcessExecutor is one implementation detail of a SandboxProvider. The
// registry, not this interface alone, is the authorized runtime extension
// point because it also binds a backend and its enforced capabilities.
type ProcessExecutor interface {
	Run(ctx context.Context, def contracts.ToolDefinition, req contracts.ToolRequest) (contracts.ToolResult, error)
}

// EffectRunner is the generic durable boundary for production tool
// composition. The callback receives the exact durable dispatch authority and
// is the only process boundary; it runs only after operation/effect
// reservation and live fence validation.
type EffectRunner interface {
	Run(ctx context.Context, request contracts.ToolRequest, definition contracts.ToolDefinition, run contracts.ToolRun, invoke func(context.Context, contracts.EffectAuthority) (contracts.ToolResult, error)) (contracts.ToolResult, error)
}

// LocalExecutor runs a registered executable with structured argv and bounded
// process output. It is not a kernel sandbox.
type LocalExecutor struct{}

// Backend names the same-host execution boundary provided by LocalExecutor.
// It is not a kernel or filesystem sandbox.
func (LocalExecutor) Backend() contracts.SandboxBackend {
	return contracts.SandboxBackendLocalProcess
}

// Capabilities reports the local limits and the isolation guarantees it lacks.
func (LocalExecutor) Capabilities() contracts.SandboxBackendCapabilities {
	capabilities := contracts.SandboxBackendCapabilities{
		Backend:   contracts.SandboxBackendLocalProcess,
		Available: supportsProcessGroupTermination(),
		Enforced: []contracts.SandboxCapability{
			contracts.SandboxCapabilityStructuredArgv,
			contracts.SandboxCapabilityExplicitEnvironment,
			contracts.SandboxCapabilityWallTimeLimit,
			contracts.SandboxCapabilityOutputByteLimit,
			contracts.SandboxCapabilityArgumentBudget,
			contracts.SandboxCapabilityEnvironmentBudget,
			contracts.SandboxCapabilityWorkdirPathPreflight,
		},
		NotEnforced: []contracts.SandboxCapability{
			contracts.SandboxCapabilityReadOnlyMount,
			contracts.SandboxCapabilityFilesystemIsolation,
			contracts.SandboxCapabilityNetworkIsolation,
			contracts.SandboxCapabilityCPULimit,
			contracts.SandboxCapabilityMemoryLimit,
			contracts.SandboxCapabilityProcessLimit,
			contracts.SandboxCapabilityScratchLimit,
		},
		Limitations: []string{
			"child process runs with the Fornix host identity; path checks do not isolate filesystem access",
			"process-group termination cannot contain a child that deliberately escapes into a new process group",
		},
	}
	if supportsProcessGroupTermination() {
		capabilities.Enforced = append(capabilities.Enforced, contracts.SandboxCapabilityProcessGroupTermination)
	} else {
		capabilities.NotEnforced = append(capabilities.NotEnforced, contracts.SandboxCapabilityProcessGroupTermination)
	}
	return capabilities
}

// Run executes one already-admitted tool request without invoking a shell.
func (LocalExecutor) Run(parent context.Context, def contracts.ToolDefinition, req contracts.ToolRequest) (contracts.ToolResult, error) {
	start := time.Now().UTC()
	profile := def.Sandbox
	if err := profile.Normalize(); err != nil {
		return contracts.ToolResult{}, failure(contracts.ToolFailureInvalidRequest, "registered sandbox profile is invalid", false)
	}
	requestBudget := req.Budget
	if err := requestBudget.Normalize(); err != nil {
		return contracts.ToolResult{}, failure(contracts.ToolFailureInvalidRequest, "request sandbox budget is invalid", false)
	}
	if contracts.SandboxBackend(profile.Backend) != contracts.SandboxBackendLocalProcess || contracts.SandboxBackend(requestBudget.Backend) != contracts.SandboxBackendLocalProcess {
		return contracts.ToolResult{}, failure(contracts.ToolFailureSandboxUnavailable, "requested sandbox backend is unavailable", false)
	}
	profile, err := tightenSandboxProfile(profile, requestBudget)
	if err != nil {
		return contracts.ToolResult{}, failure(contracts.ToolFailureWorkdirDenied, "request sandbox scope exceeds the registered tool scope", false)
	}
	if profile.AllowedWorkdirRoot != "" {
		def.WorkdirRoot = profile.AllowedWorkdirRoot
	}
	capabilities := LocalExecutor{}.Capabilities()
	requiredCapabilities, err := profile.RequiredEnforcements()
	if err != nil {
		return contracts.ToolResult{}, failure(contracts.ToolFailureSandboxCapability, "sandbox profile has invalid enforcement requirements", false)
	}
	for _, required := range requiredCapabilities {
		if !containsSandboxCapability(capabilities.Enforced, required) {
			return contracts.ToolResult{}, failure(contracts.ToolFailureSandboxCapability, "local process backend does not enforce a required capability", false)
		}
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
			resolved, err := filepath.EvalSymlinks(candidate)
			if err != nil || !withinRoot(resolved, canonicalRoot) {
				return contracts.ToolResult{}, failure(contracts.ToolFailureWorkdirDenied, "path argument cannot be resolved inside the authorized workdir", false)
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
	envBytes := 0
	for _, key := range keys {
		entry := key + "=" + req.Environment[key]
		env = append(env, entry)
		envBytes += len(entry)
	}
	if len(env) > profile.MaxEnvEntries || envBytes > profile.MaxEnvBytes {
		return contracts.ToolResult{}, failure(contracts.ToolFailureEnvironmentLimit, "environment exceeds tool budget", false)
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(profile.TimeoutMS)*time.Millisecond)
	defer cancel()
	cmd := exec.CommandContext(ctx, def.Executable, req.Argv[1:]...)
	configureProcessGroup(cmd)
	cmd.Env, cmd.Dir = env, workdir
	stdout := &boundedOutput{limit: profile.MaxStdoutBytes, cancel: cancel}
	stderr := &boundedOutput{limit: profile.MaxStderrBytes, cancel: cancel}
	cmd.Stdout, cmd.Stderr = stdout, stderr
	waitErr := cmd.Run()
	terminateProcessGroup(cmd)
	out, errOut := stdout.Bytes(), stderr.Bytes()
	result := contracts.ToolResult{Status: contracts.ToolRunSucceeded, ExitCode: 0, Stdout: redactOutput(string(out), req.Environment), Stderr: redactOutput(string(errOut), req.Environment), StartedAt: start, FinishedAt: time.Now().UTC(), DurationMS: time.Since(start).Milliseconds()}
	if stdout.Overflow() || stderr.Overflow() {
		result.Status = contracts.ToolRunFailed
		result.Failure = &contracts.ToolFailure{Code: contracts.ToolFailureOutputLimit, Message: "tool output exceeded budget"}
		result.ContentHash = result.Hash()
		return result, nil
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		result.Status = contracts.ToolRunFailed
		result.Failure = &contracts.ToolFailure{Code: contracts.ToolFailureTimeout, Message: "tool execution timed out", Retryable: true}
		result.ContentHash = result.Hash()
		return result, nil
	}
	if errors.Is(ctx.Err(), context.Canceled) && parent.Err() != nil {
		result.Status = contracts.ToolRunCancelled
		result.Failure = &contracts.ToolFailure{Code: contracts.ToolFailureCancelled, Message: "tool execution cancelled"}
		result.ContentHash = result.Hash()
		return result, nil
	}
	if waitErr != nil {
		result.Status = contracts.ToolRunFailed
		result.Failure = &contracts.ToolFailure{Code: contracts.ToolFailureExecution, Message: "tool exited unsuccessfully"}
		if exitErr, ok := waitErr.(*exec.ExitError); ok {
			result.ExitCode = exitErr.ExitCode()
		}
		result.ContentHash = result.Hash()
		return result, nil
	}
	result.ContentHash = result.Hash()
	return result, nil
}

func tightenSandboxProfile(definition, request contracts.SandboxProfile) (contracts.SandboxProfile, error) {
	if request.Backend != string(contracts.SandboxBackendLocalProcess) && request.Backend != definition.Backend {
		return definition, fmt.Errorf("request selected a different sandbox backend")
	}
	if request.ImageDigest != "" && request.ImageDigest != definition.ImageDigest {
		return definition, fmt.Errorf("request image digest differs from the registered profile")
	}
	if !request.ImagePlatform.IsZero() && request.ImagePlatform != definition.ImagePlatform {
		return definition, fmt.Errorf("request image platform differs from the registered profile")
	}
	if request.TimeoutMS < definition.TimeoutMS {
		definition.TimeoutMS = request.TimeoutMS
	}
	if request.MaxStdoutBytes < definition.MaxStdoutBytes {
		definition.MaxStdoutBytes = request.MaxStdoutBytes
	}
	if request.MaxStderrBytes < definition.MaxStderrBytes {
		definition.MaxStderrBytes = request.MaxStderrBytes
	}
	if request.MaxArgCount < definition.MaxArgCount {
		definition.MaxArgCount = request.MaxArgCount
	}
	if request.MaxArgBytes < definition.MaxArgBytes {
		definition.MaxArgBytes = request.MaxArgBytes
	}
	if request.MaxEnvEntries < definition.MaxEnvEntries {
		definition.MaxEnvEntries = request.MaxEnvEntries
	}
	if request.MaxEnvBytes < definition.MaxEnvBytes {
		definition.MaxEnvBytes = request.MaxEnvBytes
	}
	definition.CPUQuotaMilli = minPositiveInt(definition.CPUQuotaMilli, request.CPUQuotaMilli)
	definition.MemoryBytes = minPositiveInt64(definition.MemoryBytes, request.MemoryBytes)
	definition.PIDsLimit = minPositiveInt(definition.PIDsLimit, request.PIDsLimit)
	definition.ScratchBytes = minPositiveInt64(definition.ScratchBytes, request.ScratchBytes)
	definition.ReadOnlyWorkdir = definition.ReadOnlyWorkdir || request.ReadOnlyWorkdir
	requestedRoot := strings.TrimSpace(request.AllowedWorkdirRoot)
	if requestedRoot != "" {
		root, err := restrictWorkdirRoot(requestedRoot, definition.AllowedWorkdirRoot, definition.Backend)
		if err != nil {
			return definition, fmt.Errorf("request workdir root exceeds the registered root: %w", err)
		}
		definition.AllowedWorkdirRoot = root
	}
	definition.RequiredCapabilities = append(definition.RequiredCapabilities, request.RequiredCapabilities...)
	if err := definition.Normalize(); err != nil {
		return definition, err
	}
	return definition, nil
}

// restrictWorkdirRoot makes a requested root narrower than the registered
// root. Local-process roots are resolved through symlinks before comparison;
// a lexical child path must not redefine an outside symlink target as trusted.
func restrictWorkdirRoot(requested, registered string, backend string) (string, error) {
	requested = strings.TrimSpace(requested)
	registered = strings.TrimSpace(registered)
	if requested == "" {
		return "", fmt.Errorf("requested workdir root is empty")
	}
	requested = filepath.Clean(requested)
	if registered != "" {
		registered = filepath.Clean(registered)
	}
	if backend == string(contracts.SandboxBackendLocalProcess) {
		if registered == "" {
			return "", fmt.Errorf("local-process tools require a registered workdir root")
		}
		requestedAbsolute, err := filepath.Abs(requested)
		if err != nil {
			return "", fmt.Errorf("make requested workdir root absolute: %w", err)
		}
		registeredAbsolute, err := filepath.Abs(registered)
		if err != nil {
			return "", fmt.Errorf("make registered workdir root absolute: %w", err)
		}
		lexicalRelative, err := filepath.Rel(registeredAbsolute, requestedAbsolute)
		if err != nil || !filepath.IsLocal(lexicalRelative) {
			return "", fmt.Errorf("request workdir root is outside the registered root")
		}
		registered, err = canonicalWorkdirRoot(registeredAbsolute)
		if err != nil {
			return "", fmt.Errorf("resolve registered workdir root: %w", err)
		}
		requested, err = canonicalWorkdirRoot(requestedAbsolute)
		if err != nil {
			return "", fmt.Errorf("resolve requested workdir root: %w", err)
		}
		relative, err := filepath.Rel(registered, requested)
		if err != nil || !filepath.IsLocal(relative) {
			return "", fmt.Errorf("resolved request workdir root is outside the registered root")
		}
		registeredRoot, err := os.OpenRoot(registered)
		if err != nil {
			return "", fmt.Errorf("open registered workdir root: %w", err)
		}
		defer registeredRoot.Close()
		requestedRoot, err := registeredRoot.OpenRoot(relative)
		if err != nil {
			return "", fmt.Errorf("open requested workdir root within registered root: %w", err)
		}
		defer requestedRoot.Close()
		info, err := requestedRoot.Stat(".")
		if err != nil {
			return "", fmt.Errorf("stat requested workdir root: %w", err)
		}
		if !info.IsDir() {
			return "", fmt.Errorf("requested workdir root is not a directory")
		}
		return requested, nil
	}
	if registered != "" && !withinRoot(requested, registered) {
		return "", fmt.Errorf("request workdir root is outside the registered root")
	}
	return requested, nil
}

func canonicalWorkdirRoot(root string) (string, error) {
	absolute, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", err
	}
	return filepath.Clean(resolved), nil
}

func minPositiveInt(left, right int) int {
	if left == 0 || (right > 0 && right < left) {
		return right
	}
	return left
}

func minPositiveInt64(left, right int64) int64 {
	if left == 0 || (right > 0 && right < left) {
		return right
	}
	return left
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

func mergeSandboxProfiles(definition, policy contracts.SandboxProfile) (contracts.SandboxProfile, error) {
	out := definition
	if definition.Backend != policy.Backend || definition.ImageDigest != policy.ImageDigest || definition.ImagePlatform != policy.ImagePlatform || definition.ReadOnlyRootFS != policy.ReadOnlyRootFS || definition.AllowNetwork != policy.AllowNetwork || definition.InheritEnvironment != policy.InheritEnvironment {
		return out, fmt.Errorf("tool policy sandbox backend does not match the registered backend")
	}
	out.ReadOnlyWorkdir = definition.ReadOnlyWorkdir || policy.ReadOnlyWorkdir
	if policy.AllowedWorkdirRoot != "" {
		root, err := restrictWorkdirRoot(policy.AllowedWorkdirRoot, definition.AllowedWorkdirRoot, definition.Backend)
		if err != nil {
			return out, fmt.Errorf("tool policy workdir root exceeds the registered root: %w", err)
		}
		out.AllowedWorkdirRoot = root
	}
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
	out.CPUQuotaMilli = minPositiveInt(out.CPUQuotaMilli, policy.CPUQuotaMilli)
	out.MemoryBytes = minPositiveInt64(out.MemoryBytes, policy.MemoryBytes)
	out.PIDsLimit = minPositiveInt(out.PIDsLimit, policy.PIDsLimit)
	out.ScratchBytes = minPositiveInt64(out.ScratchBytes, policy.ScratchBytes)
	out.RequiredCapabilities = append(out.RequiredCapabilities, policy.RequiredCapabilities...)
	return out, nil
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
// fencing, and exact sandbox-provider selection.
type Executor struct {
	Registry    *Registry
	Policy      *Policy
	Store       RunStore
	Fence       FenceValidator
	Sandboxes   *SandboxRegistry
	Effects     EffectRunner
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

// AuthorizeModelTool performs a side-effect-free capability admission before
// agent-run reservation and model egress. Argument-specific policy is checked
// again by Execute against the actual model-produced request.
func (e *Executor) AuthorizeModelTool(ctx context.Context, scope contracts.ToolRequest, definition contracts.ToolDefinition) error {
	if e == nil || e.Registry == nil || e.Policy == nil {
		return fmt.Errorf("%w: tool catalog policy is unavailable", ErrUnauthorized)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	definition = cloneDefinition(definition)
	if err := definition.Normalize(); err != nil {
		return fmt.Errorf("%w: registered tool definition is invalid", ErrUnauthorized)
	}
	registered, ok := e.Registry.Lookup(definition.ID)
	if !ok {
		return fmt.Errorf("%w: tool is not registered", ErrUnauthorized)
	}
	registeredJSON, registeredErr := json.Marshal(registered)
	definitionJSON, definitionErr := json.Marshal(definition)
	if registeredErr != nil || definitionErr != nil || !bytes.Equal(registeredJSON, definitionJSON) {
		return fmt.Errorf("%w: tool definition changed during catalog admission", ErrUnauthorized)
	}
	probe := scope
	probe.WorkspaceID = strings.TrimSpace(scope.WorkspaceID)
	if probe.WorkspaceID == "" {
		return fmt.Errorf("%w: workspace scope is required", ErrUnauthorized)
	}
	probe.ToolID = definition.ID
	probe.Capability = definition.Capability
	probe.Argv = append([]string{definition.Executable}, definition.ArgvPrefix...)
	probe.Workdir = firstNonEmpty(definition.WorkdirRoot, definition.Sandbox.AllowedWorkdirRoot)
	probe.Environment = nil
	decision, err := e.Policy.Evaluate(probe, definition)
	if err != nil {
		return err
	}
	if decision.Mode == contracts.ToolModeDenied {
		return fmt.Errorf("%w: tool policy denied model exposure", ErrUnauthorized)
	}
	return nil
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
	effectiveDefinition := def
	effectiveDefinition.Sandbox, err = mergeSandboxProfiles(def.Sandbox, decision.Rule.Sandbox)
	if err != nil {
		return Outcome{}, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "sandbox policy is incompatible with the registered tool")
	}
	if decision.Rule.WorkdirRoot != "" {
		root, rootErr := restrictWorkdirRoot(decision.Rule.WorkdirRoot, effectiveDefinition.Sandbox.AllowedWorkdirRoot, effectiveDefinition.Sandbox.Backend)
		if rootErr != nil {
			return Outcome{}, failure(contracts.ToolFailureWorkdirDenied, "tool policy workdir exceeds the registered root", false)
		}
		effectiveDefinition.WorkdirRoot = root
		effectiveDefinition.Sandbox.AllowedWorkdirRoot = root
	}
	if len(decision.Rule.AllowedEnvKeys) > 0 {
		effectiveDefinition.AllowedEnvKeys = intersectStrings(def.AllowedEnvKeys, decision.Rule.AllowedEnvKeys)
	}
	effectiveDefinition.Sandbox, err = tightenSandboxProfile(effectiveDefinition.Sandbox, req.Budget)
	if err != nil {
		return Outcome{}, failure(contracts.ToolFailureWorkdirDenied, "request sandbox scope exceeds the registered tool scope", false)
	}
	if effectiveDefinition.Sandbox.AllowedWorkdirRoot != "" {
		effectiveDefinition.WorkdirRoot = effectiveDefinition.Sandbox.AllowedWorkdirRoot
	}
	if err := effectiveDefinition.Sandbox.Normalize(); err != nil {
		return Outcome{}, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "effective sandbox profile is invalid")
	}
	definitionHash, err := effectiveDefinition.Hash()
	if err != nil {
		return Outcome{}, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "tool execution definition is invalid")
	}
	profileHash, err := effectiveDefinition.Sandbox.Hash()
	if err != nil {
		return Outcome{}, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "effective sandbox profile is invalid")
	}
	if (req.ToolDefinitionHash != "" && req.ToolDefinitionHash != definitionHash) || (req.SandboxProfileHash != "" && req.SandboxProfileHash != profileHash) {
		return Outcome{}, failure(contracts.ToolFailureConflict, "submitted tool execution identity does not match the registered definition", false)
	}
	req.ToolDefinitionHash, req.SandboxProfileHash = definitionHash, profileHash
	qualificationHash := ""
	if contracts.SandboxBackend(effectiveDefinition.Sandbox.Backend) != contracts.SandboxBackendLocalProcess {
		if e.Sandboxes == nil {
			return Outcome{}, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "requested sandbox backend is unavailable")
		}
		qualificationHash, err = e.Sandboxes.QualificationHash(effectiveDefinition.Sandbox, req.WorkspaceID)
		if err != nil || qualificationHash == "" {
			return Outcome{}, sandboxFailure(contracts.ToolFailureSandboxCapability, "requested sandbox backend has no current trusted qualification")
		}
	}
	if req.SandboxQualificationHash != "" && req.SandboxQualificationHash != qualificationHash {
		return Outcome{}, failure(contracts.ToolFailureConflict, "submitted sandbox qualification identity does not match the active backend", false)
	}
	req.SandboxQualificationHash = qualificationHash
	if err := req.Normalize(); err != nil {
		return Outcome{}, failure(contracts.ToolFailureInvalidRequest, err.Error(), false)
	}
	run, existing, err := e.Store.Reserve(ctx, req, decision.Mode)
	if err != nil {
		return Outcome{}, err
	}
	approvedContinuation := false
	if existing {
		if identityErr := verifyReplayExecutionIdentity(run, req); identityErr != nil {
			return Outcome{}, identityErr
		}
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
	result, runErr := e.runEffect(ctx, req, effectiveDefinition, started)
	if runErr != nil {
		var uncertain interface{ UncertainExternalOutcome() bool }
		if errors.As(runErr, &uncertain) && uncertain.UncertainExternalOutcome() {
			recoveryResult := contracts.ToolResult{
				RequestID: started.RequestID, RunID: started.ID, ToolID: started.ToolID,
				Status:  contracts.ToolRunRecoveryRequired,
				Failure: &contracts.ToolFailure{Code: contracts.ToolFailureExternalUncertain, Message: "external tool outcome requires reconciliation"},
			}
			finished, finishErr := e.Store.Finish(ctx, started, recoveryResult)
			if finishErr != nil {
				return Outcome{Run: started, Result: &recoveryResult}, finishErr
			}
			return Outcome{Run: finished, Result: &recoveryResult}, runErr
		}
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

func (e *Executor) runEffect(ctx context.Context, req contracts.ToolRequest, def contracts.ToolDefinition, run contracts.ToolRun) (contracts.ToolResult, error) {
	executionDefinition := def
	var err error
	executionDefinition.Sandbox, err = tightenSandboxProfile(def.Sandbox, req.Budget)
	if err != nil {
		return contracts.ToolResult{}, failure(contracts.ToolFailureWorkdirDenied, "request sandbox scope exceeds the registered tool scope", false)
	}
	if err := executionDefinition.Sandbox.Normalize(); err != nil {
		return contracts.ToolResult{}, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "effective sandbox profile is invalid")
	}
	executionRequest := req
	executionRequest.Budget = executionDefinition.Sandbox
	provider, err := e.sandboxFor(executionDefinition.Sandbox, req.Budget, req.WorkspaceID)
	if err != nil {
		return contracts.ToolResult{}, err
	}
	if e.Effects != nil {
		return e.Effects.Run(ctx, req, executionDefinition, run, func(runCtx context.Context, authority contracts.EffectAuthority) (contracts.ToolResult, error) {
			if provider.Backend() == contracts.SandboxBackendLocalProcess {
				return provider.Run(runCtx, executionDefinition, executionRequest)
			}
			qualificationHash, qualificationErr := e.Sandboxes.QualificationHash(executionDefinition.Sandbox, req.WorkspaceID)
			if qualificationErr != nil || qualificationHash == "" || qualificationHash != req.SandboxQualificationHash {
				return contracts.ToolResult{}, sandboxFailure(contracts.ToolFailureSandboxCapability, "sandbox qualification changed before runtime invocation")
			}
			attemptProvider, ok := provider.(AttemptAwareSandboxProvider)
			if !ok {
				return contracts.ToolResult{}, sandboxFailure(contracts.ToolFailureSandboxCapability, "non-local sandbox provider does not support fenced attempt reconciliation")
			}
			identity, identityErr := sandboxExecutionIdentity(run, req, executionDefinition, authority)
			if identityErr != nil {
				return contracts.ToolResult{}, sandboxFailure(contracts.ToolFailureSandboxCapability, "sandbox attempt identity is incomplete or inconsistent")
			}
			return attemptProvider.RunAttempt(runCtx, executionDefinition, executionRequest, identity)
		})
	}
	if provider.Backend() != contracts.SandboxBackendLocalProcess {
		return contracts.ToolResult{}, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "non-local sandbox execution requires the durable effect dispatcher")
	}
	return provider.Run(ctx, executionDefinition, executionRequest)
}

func (e *Executor) sandboxFor(profile, request contracts.SandboxProfile, workspaceID string) (SandboxProvider, error) {
	if err := profile.Normalize(); err != nil {
		return nil, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "sandbox profile is invalid")
	}
	if err := request.Normalize(); err != nil || (request.Backend != string(contracts.SandboxBackendLocalProcess) && profile.Backend != request.Backend) {
		return nil, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "requested sandbox backend does not match the admitted backend")
	}
	if e.Sandboxes != nil {
		return e.Sandboxes.Resolve(profile, workspaceID)
	}
	if contracts.SandboxBackend(profile.Backend) != contracts.SandboxBackendLocalProcess {
		return nil, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "requested sandbox backend is unavailable")
	}
	local := LocalExecutor{}
	requiredCapabilities, err := profile.RequiredEnforcements()
	if err != nil {
		return nil, sandboxFailure(contracts.ToolFailureSandboxCapability, "sandbox profile has invalid enforcement requirements")
	}
	for _, required := range requiredCapabilities {
		if !containsSandboxCapability(local.Capabilities().Enforced, required) {
			return nil, sandboxFailure(contracts.ToolFailureSandboxCapability, "local process backend does not enforce a required capability")
		}
	}
	return local, nil
}
func (e *Executor) approvalTTL() time.Duration {
	if e.ApprovalTTL <= 0 {
		return 10 * time.Minute
	}
	return e.ApprovalTTL
}
func (e *Executor) replayOrConflict(run contracts.ToolRun) (Outcome, error) {
	if run.Status == contracts.ToolRunRecoveryRequired {
		out := Outcome{Run: run, Deduplicated: true}
		if run.Result != nil {
			out.Result = run.Result
		}
		return out, failure(contracts.ToolFailureExternalUncertain, "tool run requires external outcome recovery", false)
	}
	if run.Status == contracts.ToolRunSucceeded || run.Status == contracts.ToolRunFailed || run.Status == contracts.ToolRunDenied || run.Status == contracts.ToolRunCancelled {
		out := Outcome{Run: run, Deduplicated: true}
		if run.Result != nil {
			out.Result = run.Result
		}
		return out, nil
	}
	return Outcome{Run: run, Deduplicated: true}, failure(contracts.ToolFailureInProgress, "tool run is already reserved or running", false)
}

func verifyReplayExecutionIdentity(run contracts.ToolRun, request contracts.ToolRequest) error {
	var recorded struct {
		ToolDefinitionHash       string `json:"tool_definition_hash"`
		SandboxProfileHash       string `json:"sandbox_profile_hash"`
		SandboxQualificationHash string `json:"sandbox_qualification_hash"`
	}
	if len(run.RequestEvidence) > 0 {
		if err := json.Unmarshal(run.RequestEvidence, &recorded); err != nil {
			return fmt.Errorf("%w: stored tool execution identity is unreadable", ErrRunConflict)
		}
	}
	if recorded.ToolDefinitionHash == "" && recorded.SandboxProfileHash == "" && recorded.SandboxQualificationHash == "" {
		switch run.Status {
		case contracts.ToolRunSucceeded, contracts.ToolRunFailed, contracts.ToolRunDenied, contracts.ToolRunCancelled, contracts.ToolRunRecoveryRequired:
			// Historical terminal runs predate execution identity evidence. They
			// may be replayed, but are never executed again by this path.
			return nil
		case contracts.ToolRunRunning:
			return ErrRunInProgress
		default:
			return fmt.Errorf("%w: nonterminal historical tool run has no execution identity", ErrRunConflict)
		}
	}
	if recorded.ToolDefinitionHash != request.ToolDefinitionHash || recorded.SandboxProfileHash != request.SandboxProfileHash || recorded.SandboxQualificationHash != request.SandboxQualificationHash {
		return fmt.Errorf("%w: registered tool, sandbox profile, or qualification changed for this idempotency key", ErrRunConflict)
	}
	return nil
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
