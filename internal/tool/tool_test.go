package tool

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/qualification"
)

func testDefinition(executable string) contracts.ToolDefinition {
	return contracts.ToolDefinition{ID: "test.tool", Name: "test", Version: "1", Capability: "test.execute", Executable: executable, Enabled: true, Sandbox: contracts.DefaultSandboxProfile()}
}

type testSandboxProvider struct {
	ProcessExecutor
	capabilities *contracts.SandboxBackendCapabilities
}

func (testSandboxProvider) Backend() contracts.SandboxBackend {
	return contracts.SandboxBackendLocalProcess
}

func (p testSandboxProvider) Capabilities() contracts.SandboxBackendCapabilities {
	if p.capabilities != nil {
		return *p.capabilities
	}
	return LocalExecutor{}.Capabilities()
}

func testSandboxRegistry(t *testing.T, process ProcessExecutor) *SandboxRegistry {
	t.Helper()
	registry := NewSandboxRegistry()
	if err := registry.Register(testSandboxProvider{ProcessExecutor: process}); err != nil {
		t.Fatal(err)
	}
	return registry
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

func TestRegistryPreservesAndDefensivelyCopiesPathArgumentRestrictions(t *testing.T) {
	root := t.TempDir()
	definition := testDefinition("/bin/cat")
	definition.WorkdirRoot = root
	definition.PathArgvIndexes = []int{1}
	definition.Sandbox.ReadOnlyWorkdir = true
	definition.Sandbox.AllowedWorkdirRoot = root
	registry := NewRegistry()
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	definition.PathArgvIndexes[0] = 0
	registered, ok := registry.Lookup(definition.ID)
	if !ok || len(registered.PathArgvIndexes) != 1 || registered.PathArgvIndexes[0] != 1 {
		t.Fatalf("registry lost or aliased path argument restrictions: %+v", registered.PathArgvIndexes)
	}
	registered.PathArgvIndexes[0] = 0
	again, ok := registry.Lookup(definition.ID)
	if !ok || len(again.PathArgvIndexes) != 1 || again.PathArgvIndexes[0] != 1 {
		t.Fatalf("lookup mutation changed registered path restrictions: %+v", again.PathArgvIndexes)
	}

	executor := &Executor{Registry: registry, Policy: testPolicy(t, "w1", definition.ID, contracts.ToolModeAutomatic), Store: newFakeRunStore(), Sandboxes: NewDefaultSandboxRegistry()}
	request := testRequest()
	request.Argv = []string{definition.Executable, "/etc/passwd"}
	request.Workdir = root
	request.Budget = definition.Sandbox
	_, err := executor.Execute(context.Background(), request)
	var failureErr *FailureError
	if !errors.As(err, &failureErr) || failureErr.Failure.Code != contracts.ToolFailureWorkdirDenied {
		t.Fatalf("registered executor did not enforce its path restriction: err=%v", err)
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

func TestExecutorAdmitsModelCatalogOnlyForAuthorizedWorkspaceAndActor(t *testing.T) {
	definition := testDefinition("/bin/echo")
	registry := NewRegistry()
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	policy, err := NewPolicy([]contracts.ToolPolicyRule{{
		ID: "actor-scoped", WorkspaceID: "w1", ActorID: "actor-1", ToolID: definition.ID,
		Capability: definition.Capability, Mode: contracts.ToolModeAutomatic, Enabled: true,
		Sandbox: contracts.DefaultSandboxProfile(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	executor := &Executor{Registry: registry, Policy: policy}
	scope := contracts.ToolRequest{WorkspaceID: "w1", Actor: contracts.ActorRef{ID: "actor-1", Kind: "test"}}
	if err := executor.AuthorizeModelTool(context.Background(), scope, definition); err != nil {
		t.Fatalf("authorized tool catalog rejected: %v", err)
	}
	scope.WorkspaceID = "w2"
	if err := executor.AuthorizeModelTool(context.Background(), scope, definition); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("cross-workspace model exposure error=%v, want ErrUnauthorized", err)
	}
	scope.WorkspaceID = "w1"
	scope.Actor.ID = "actor-2"
	if err := executor.AuthorizeModelTool(context.Background(), scope, definition); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("cross-actor model exposure error=%v, want ErrUnauthorized", err)
	}
}

func TestExecutorDoesNotExposeExplicitlyDeniedToolToModel(t *testing.T) {
	definition := testDefinition("/bin/echo")
	registry := NewRegistry()
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	policy, err := NewPolicy([]contracts.ToolPolicyRule{{
		ID: "deny", WorkspaceID: "w1", ToolID: definition.ID, Capability: definition.Capability,
		Mode: contracts.ToolModeDenied, Enabled: true, Sandbox: contracts.DefaultSandboxProfile(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	executor := &Executor{Registry: registry, Policy: policy}
	if err := executor.AuthorizeModelTool(context.Background(), testRequest(), definition); !errors.Is(err, ErrUnauthorized) {
		t.Fatalf("explicitly denied tool exposure error=%v, want ErrUnauthorized", err)
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
	if timeoutResult.ContentHash == "" || timeoutResult.ContentHash != timeoutResult.Hash() {
		t.Fatalf("timeout result hash does not bind its final status: %+v", timeoutResult)
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
	if outputResult.ContentHash == "" || outputResult.ContentHash != outputResult.Hash() {
		t.Fatalf("output-limit result hash does not bind its final status: %+v", outputResult)
	}
}

func TestLocalExecutorUsesStrictestArgumentAndEnvironmentBudgets(t *testing.T) {
	cases := []struct {
		name     string
		request  contracts.ToolRequest
		wantCode string
	}{
		{
			name: "argument count",
			request: func() contracts.ToolRequest {
				r := testRequest()
				r.Argv = []string{"/bin/echo", "one", "two"}
				r.Budget.MaxArgCount = 2
				return r
			}(),
			wantCode: contracts.ToolFailureArgumentLimit,
		},
		{
			name: "argument bytes",
			request: func() contracts.ToolRequest {
				r := testRequest()
				r.Argv = []string{"/bin/echo", "long"}
				r.Budget.MaxArgBytes = 3
				return r
			}(),
			wantCode: contracts.ToolFailureArgumentLimit,
		},
		{
			name: "environment bytes include separator",
			request: func() contracts.ToolRequest {
				r := testRequest()
				r.Environment = map[string]string{"TOKEN": "1234"}
				r.Budget.MaxEnvBytes = 8
				return r
			}(),
			wantCode: contracts.ToolFailureEnvironmentLimit,
		},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			definition := testDefinition("/bin/echo")
			result, err := (LocalExecutor{}).Run(context.Background(), definition, test.request)
			var failureErr *FailureError
			if !errors.As(err, &failureErr) || failureErr.Failure.Code != test.wantCode {
				t.Fatalf("result=%+v err=%v, want failure %s", result, err, test.wantCode)
			}
		})
	}
}

func TestLocalExecutorFailsClosedForMissingIsolationCapabilities(t *testing.T) {
	definition := testDefinition("/bin/echo")
	definition.Sandbox.RequiredCapabilities = []contracts.SandboxCapability{contracts.SandboxCapabilityFilesystemIsolation}
	request := testRequest()
	result, err := (LocalExecutor{}).Run(context.Background(), definition, request)
	var failureErr *FailureError
	if !errors.As(err, &failureErr) || failureErr.Failure.Code != contracts.ToolFailureSandboxCapability {
		t.Fatalf("expected missing capability failure, result=%#v err=%v", result, err)
	}
}

func TestLocalExecutorDoesNotAcceptAnotherBackend(t *testing.T) {
	definition := testDefinition("/bin/echo")
	definition.Sandbox.Backend = string(contracts.SandboxBackendOCI)
	definition.Sandbox.ImageDigest = "sha256:" + strings.Repeat("a", 64)
	definition.Sandbox.ImagePlatform = contracts.SandboxImagePlatform{OS: "linux", Architecture: "amd64"}
	definition.Sandbox.ReadOnlyRootFS = true
	definition.Sandbox.ReadOnlyWorkdir = true
	definition.Sandbox.CPUQuotaMilli = 1000
	definition.Sandbox.MemoryBytes = 512 << 20
	definition.Sandbox.PIDsLimit = 64
	definition.Sandbox.ScratchBytes = 1 << 30
	request := testRequest()
	request.Budget.Backend = string(contracts.SandboxBackendOCI)
	request.Budget.ImageDigest = definition.Sandbox.ImageDigest
	request.Budget.ImagePlatform = definition.Sandbox.ImagePlatform
	request.Budget.ReadOnlyRootFS = true
	request.Budget.ReadOnlyWorkdir = true
	request.Budget.CPUQuotaMilli = 1000
	request.Budget.MemoryBytes = 512 << 20
	request.Budget.PIDsLimit = 64
	request.Budget.ScratchBytes = 1 << 30
	result, err := (LocalExecutor{}).Run(context.Background(), definition, request)
	var failureErr *FailureError
	if !errors.As(err, &failureErr) || failureErr.Failure.Code != contracts.ToolFailureSandboxUnavailable {
		t.Fatalf("expected unavailable backend failure, result=%#v err=%v", result, err)
	}
}

func TestSandboxProfilesApplyStrictestResourceBudgets(t *testing.T) {
	definition := contracts.DefaultSandboxProfile()
	definition.CPUQuotaMilli = 2000
	definition.MemoryBytes = 1 << 30
	definition.PIDsLimit = 256
	definition.ScratchBytes = 2 << 30
	policy := definition
	policy.CPUQuotaMilli = 1000
	policy.MemoryBytes = 512 << 20
	policy.PIDsLimit = 64
	policy.ScratchBytes = 1 << 30
	merged, err := mergeSandboxProfiles(definition, policy)
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.DefaultSandboxProfile()
	request.CPUQuotaMilli = 500
	request.MemoryBytes = 256 << 20
	request.PIDsLimit = 32
	request.ScratchBytes = 512 << 20
	effective, err := tightenSandboxProfile(merged, request)
	if err != nil {
		t.Fatal(err)
	}
	if effective.CPUQuotaMilli != 500 || effective.MemoryBytes != 256<<20 || effective.PIDsLimit != 32 || effective.ScratchBytes != 512<<20 {
		t.Fatalf("resource budgets did not intersect by minimum: %#v", effective)
	}
}

func TestSandboxProfilesRejectImagePlatformDrift(t *testing.T) {
	definition := nonLocalTestProfile(t)
	policy := definition
	policy.ImagePlatform = contracts.SandboxImagePlatform{OS: "linux", Architecture: "arm64"}
	if _, err := mergeSandboxProfiles(definition, policy); err == nil {
		t.Fatal("policy changed the registered image platform")
	}
	if _, err := tightenSandboxProfile(definition, policy); err == nil {
		t.Fatal("request changed the registered image platform")
	}
}

func TestLocalExecutorHonorsNarrowerRequestWorkdirAndReadOnlyRequirement(t *testing.T) {
	root := t.TempDir()
	narrowRoot := filepath.Join(root, "allowed")
	otherRoot := filepath.Join(root, "other")
	for _, path := range []string{narrowRoot, otherRoot} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	definition := testDefinition("/bin/echo")
	definition.WorkdirRoot = root
	definition.Sandbox.AllowedWorkdirRoot = root
	definition.PathArgvIndexes = []int{1}
	request := testRequest()
	request.Workdir = otherRoot
	request.Budget.AllowedWorkdirRoot = narrowRoot
	request.Budget.ReadOnlyWorkdir = true
	result, err := (LocalExecutor{}).Run(context.Background(), definition, request)
	var failureErr *FailureError
	if !errors.As(err, &failureErr) || failureErr.Failure.Code != contracts.ToolFailureWorkdirDenied {
		t.Fatalf("request workdir restriction was ignored: result=%#v err=%v", result, err)
	}
}

func TestExecutorRejectsPolicyWorkdirThatWidensDefinitionRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "registered")
	policyRoot := filepath.Join(parent, "other")
	for _, path := range []string{root, policyRoot} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	definition := testDefinition("/bin/echo")
	definition.WorkdirRoot = root
	definition.Sandbox.AllowedWorkdirRoot = root
	registry := NewRegistry()
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	policy, err := NewPolicy([]contracts.ToolPolicyRule{{
		ID: "widen-root", WorkspaceID: "w1", ToolID: definition.ID, Capability: definition.Capability,
		Mode: contracts.ToolModeAutomatic, Enabled: true, WorkdirRoot: policyRoot,
		Sandbox: contracts.DefaultSandboxProfile(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	process := &countingProcess{result: contracts.ToolResult{Status: contracts.ToolRunSucceeded}}
	executor := &Executor{Registry: registry, Policy: policy, Store: newFakeRunStore(), Sandboxes: testSandboxRegistry(t, process)}
	request := testRequest()
	request.Workdir = policyRoot
	result, err := executor.Execute(context.Background(), request)
	var failureErr *FailureError
	if !errors.As(err, &failureErr) || failureErr.Failure.Code != contracts.ToolFailureWorkdirDenied || process.calls != 0 {
		t.Fatalf("widened policy workdir was not rejected before execution: result=%#v err=%v calls=%d", result, err, process.calls)
	}
}

func TestExecutorUsesWorkspacePolicyRootWhenToolHasNoGlobalHostRoot(t *testing.T) {
	root := t.TempDir()
	definition := testDefinition("/bin/echo")
	registry := NewRegistry()
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	policy, err := NewPolicy([]contracts.ToolPolicyRule{{
		ID: "workspace-root", WorkspaceID: "w1", ToolID: definition.ID, Capability: definition.Capability,
		Mode: contracts.ToolModeAutomatic, Enabled: true, WorkdirRoot: root,
		Sandbox: contracts.DefaultSandboxProfile(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	process := &workdirRecordingProcess{}
	executor := &Executor{Registry: registry, Policy: policy, Store: newFakeRunStore(), Sandboxes: testSandboxRegistry(t, process)}
	request := testRequest()
	request.Workdir = root

	if _, err := executor.Execute(context.Background(), request); err != nil {
		t.Fatalf("workspace policy root should admit the workspace-scoped tool: %v", err)
	}
	canonicalRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	if process.calls != 1 || process.definitionRoot != canonicalRoot || process.requestWorkdir != root {
		t.Fatalf("workspace root was not applied to execution: calls=%d definition root=%q request workdir=%q", process.calls, process.definitionRoot, process.requestWorkdir)
	}
}

func TestExecutorRejectsSymlinkedRequestRootEscapingRegisteredRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "registered")
	outside := filepath.Join(parent, "outside")
	for _, path := range []string{root, outside} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(root, "escape")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	definition := testDefinition("/bin/echo")
	definition.WorkdirRoot = root
	definition.Sandbox.AllowedWorkdirRoot = root
	registry := NewRegistry()
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	process := &countingProcess{}
	executor := &Executor{Registry: registry, Policy: testPolicy(t, "w1", definition.ID, contracts.ToolModeAutomatic), Store: newFakeRunStore(), Sandboxes: testSandboxRegistry(t, process)}
	request := testRequest()
	request.Budget.AllowedWorkdirRoot = link
	if _, err := executor.Execute(context.Background(), request); err == nil {
		t.Fatal("symlinked request root outside the registered root was admitted")
	}
	if process.calls != 0 {
		t.Fatalf("symlinked request root executed a process: calls=%d", process.calls)
	}
}

func TestRestrictWorkdirRootRejectsOutsidePathBeforeFilesystemResolution(t *testing.T) {
	parent := t.TempDir()
	registered := filepath.Join(parent, "registered")
	if err := os.Mkdir(registered, 0o700); err != nil {
		t.Fatal(err)
	}
	outside := filepath.Join(parent, "not-created", "repository")
	_, err := restrictWorkdirRoot(outside, registered, string(contracts.SandboxBackendLocalProcess))
	if err == nil || !strings.Contains(err.Error(), "outside the registered root") {
		t.Fatalf("outside path should fail containment before filesystem resolution, got: %v", err)
	}
}

func TestExecutorRejectsSymlinkedPolicyRootEscapingRegisteredRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "registered")
	outside := filepath.Join(parent, "outside")
	for _, path := range []string{root, outside} {
		if err := os.Mkdir(path, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	link := filepath.Join(root, "escape")
	if err := os.Symlink(outside, link); err != nil {
		t.Fatal(err)
	}
	definition := testDefinition("/bin/echo")
	definition.WorkdirRoot = root
	definition.Sandbox.AllowedWorkdirRoot = root
	registry := NewRegistry()
	if err := registry.Register(definition); err != nil {
		t.Fatal(err)
	}
	policy, err := NewPolicy([]contracts.ToolPolicyRule{{
		ID: "symlink-root", WorkspaceID: "w1", ToolID: definition.ID, Capability: definition.Capability,
		Mode: contracts.ToolModeAutomatic, Enabled: true, WorkdirRoot: link,
		Sandbox: contracts.DefaultSandboxProfile(),
	}})
	if err != nil {
		t.Fatal(err)
	}
	process := &countingProcess{}
	executor := &Executor{Registry: registry, Policy: policy, Store: newFakeRunStore(), Sandboxes: testSandboxRegistry(t, process)}
	if _, err := executor.Execute(context.Background(), testRequest()); err == nil {
		t.Fatal("symlinked policy root outside the registered root was admitted")
	}
	if process.calls != 0 {
		t.Fatalf("symlinked policy root executed a process: calls=%d", process.calls)
	}
}

func TestSandboxRegistryReportsCapabilitiesAndNeverFallsBack(t *testing.T) {
	registry := NewSandboxRegistry()
	if err := registry.Register(LocalExecutor{}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(LocalExecutor{}); err == nil {
		t.Fatal("duplicate backend registration was accepted")
	}
	matrix := CapabilityMatrix(registry)
	if len(matrix) != len(contracts.KnownSandboxBackends()) {
		t.Fatalf("matrix omitted known backends: %#v", matrix)
	}
	if matrix[0].Backend != contracts.SandboxBackendLocalProcess || matrix[0].Available != supportsProcessGroupTermination() {
		t.Fatalf("matrix is not stable or local backend is not available: %#v", matrix[0])
	}
	if supportsProcessGroupTermination() && !strings.Contains(strings.Join(matrix[0].Limitations, " "), "escapes into a new process group") {
		t.Fatalf("local process-group limitation is missing from operator capabilities: %#v", matrix[0])
	}
	local, err := registry.Resolve(contracts.DefaultSandboxProfile())
	if supportsProcessGroupTermination() {
		if err != nil || local.Backend() != contracts.SandboxBackendLocalProcess {
			t.Fatalf("local backend resolution failed: provider=%#v err=%v", local, err)
		}
	} else if err == nil {
		t.Fatal("local backend without process-group termination was admitted")
	}
	profile := contracts.DefaultSandboxProfile()
	profile.RequiredCapabilities = []contracts.SandboxCapability{contracts.SandboxCapabilityFilesystemIsolation}
	if _, err := registry.Resolve(profile); err == nil {
		t.Fatal("provider missing a required isolation capability was selected")
	}
	profile = nonLocalTestProfile(t)
	if _, err := registry.Resolve(profile); err == nil {
		t.Fatal("missing OCI provider silently fell back to local process")
	}
	underqualified := NewSandboxRegistry()
	capabilities := LocalExecutor{}.Capabilities()
	for i, capability := range capabilities.Enforced {
		if capability == contracts.SandboxCapabilityOutputByteLimit {
			capabilities.Enforced = append(capabilities.Enforced[:i], capabilities.Enforced[i+1:]...)
			break
		}
	}
	if err := underqualified.Register(testSandboxProvider{ProcessExecutor: &countingProcess{}, capabilities: &capabilities}); err != nil {
		t.Fatal(err)
	}
	if _, err := underqualified.Resolve(contracts.DefaultSandboxProfile()); err == nil {
		t.Fatal("provider without output-byte enforcement was admitted")
	}
}

type declaredSandboxProvider struct {
	backend      contracts.SandboxBackend
	capabilities contracts.SandboxBackendCapabilities
	process      ProcessExecutor
}

func (p declaredSandboxProvider) Run(ctx context.Context, definition contracts.ToolDefinition, request contracts.ToolRequest) (contracts.ToolResult, error) {
	return p.process.Run(ctx, definition, request)
}

func (p declaredSandboxProvider) Backend() contracts.SandboxBackend { return p.backend }

func (p declaredSandboxProvider) Capabilities() contracts.SandboxBackendCapabilities {
	return p.capabilities
}

type attemptSandboxProvider struct {
	declaredSandboxProvider
	identity        contracts.SandboxExecutionIdentity
	runtimeIdentity contracts.SandboxRuntimeIdentity
	proof           contracts.SandboxQualificationProof
	calls           int
}

func (p *attemptSandboxProvider) RuntimeIdentity() contracts.SandboxRuntimeIdentity {
	return p.runtimeIdentity
}

func (p *attemptSandboxProvider) QualificationProof() contracts.SandboxQualificationProof {
	return p.proof
}

func (p *attemptSandboxProvider) RunAttempt(_ context.Context, _ contracts.ToolDefinition, _ contracts.ToolRequest, identity contracts.SandboxExecutionIdentity) (contracts.ToolResult, error) {
	p.calls++
	p.identity = identity
	return contracts.ToolResult{Status: contracts.ToolRunSucceeded, Stdout: "sandbox result"}, nil
}

func (p *attemptSandboxProvider) ReconcileAttempt(_ context.Context, identity contracts.SandboxExecutionIdentity) (contracts.SandboxAttemptObservation, error) {
	return contracts.SandboxAttemptObservation{IdentityHash: identity.StableHash(), State: contracts.SandboxAttemptUnknown}, nil
}

type recordedRecoverySandboxProvider struct {
	*attemptSandboxProvider
	observation    contracts.SandboxAttemptObservation
	identity       contracts.SandboxExecutionIdentity
	reconcileCalls int
	runCalls       int
}

func (p *recordedRecoverySandboxProvider) RunAttempt(context.Context, contracts.ToolDefinition, contracts.ToolRequest, contracts.SandboxExecutionIdentity) (contracts.ToolResult, error) {
	p.runCalls++
	return contracts.ToolResult{}, errors.New("unexpected execution during recovery")
}

func (p *recordedRecoverySandboxProvider) ReconcileAttempt(_ context.Context, identity contracts.SandboxExecutionIdentity) (contracts.SandboxAttemptObservation, error) {
	p.reconcileCalls++
	p.identity = identity
	return p.observation, nil
}

type authorityToolEffects struct{ authority contracts.EffectAuthority }

func (e authorityToolEffects) Run(ctx context.Context, _ contracts.ToolRequest, _ contracts.ToolDefinition, _ contracts.ToolRun, invoke func(context.Context, contracts.EffectAuthority) (contracts.ToolResult, error)) (contracts.ToolResult, error) {
	return invoke(ctx, e.authority)
}

func nonLocalTestProfile(t *testing.T) contracts.SandboxProfile {
	t.Helper()
	profile := contracts.DefaultSandboxProfile()
	profile.Backend = string(contracts.SandboxBackendOCI)
	profile.ImageDigest = "sha256:" + strings.Repeat("a", 64)
	profile.ImagePlatform = contracts.SandboxImagePlatform{OS: "linux", Architecture: "amd64"}
	profile.ReadOnlyRootFS, profile.ReadOnlyWorkdir = true, true
	profile.CPUQuotaMilli, profile.MemoryBytes, profile.PIDsLimit, profile.ScratchBytes = 500, 256<<20, 32, 64<<20
	if err := profile.Normalize(); err != nil {
		t.Fatal(err)
	}
	return profile
}

func nonLocalTestProvider(t *testing.T, process ProcessExecutor) *attemptSandboxProvider {
	t.Helper()
	profile := nonLocalTestProfile(t)
	required, err := profile.RequiredEnforcements()
	if err != nil {
		t.Fatal(err)
	}
	provider := &attemptSandboxProvider{declaredSandboxProvider: declaredSandboxProvider{
		backend:      contracts.SandboxBackendOCI,
		capabilities: contracts.SandboxBackendCapabilities{Backend: contracts.SandboxBackendOCI, Available: true, Enforced: required, Limitations: []string{"test-only provider"}},
		process:      process,
	}}
	provider.runtimeIdentity = contracts.SandboxRuntimeIdentity{
		ProviderBuildHash:        contracts.HashStrings("test-sandbox-provider-build"),
		RuntimeBuildHash:         contracts.HashStrings("test-sandbox-runtime-build"),
		RuntimeConfigurationHash: contracts.HashStrings("test-sandbox-runtime-config"),
		ImageDigest:              profile.ImageDigest,
		ImagePlatform:            profile.ImagePlatform,
	}
	provider.proof = testSandboxQualificationProof(t, provider.runtimeIdentity, provider.Capabilities())
	return provider
}

func testSandboxTrust() qualification.SandboxTrust {
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x42}, ed25519.SeedSize))
	return qualification.SandboxTrust{
		KeyID: "sandbox-test-key", PublicKey: privateKey.Public().(ed25519.PublicKey),
		TargetHash: contracts.HashStrings("sandbox-test-deployment"), WorkspaceID: "w1",
		Now: func() time.Time { return time.Now().UTC() },
	}
}

func testSandboxQualificationProof(t *testing.T, runtime contracts.SandboxRuntimeIdentity, capabilities contracts.SandboxBackendCapabilities) contracts.SandboxQualificationProof {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Second)
	trust := testSandboxTrust()
	evidence := contracts.SandboxQualificationEvidence{
		SchemaVersion: contracts.SandboxQualificationSchemaVersion, ID: "sandbox-conformance-v1",
		Backend: contracts.SandboxBackendOCI, Runtime: runtime, CapabilityHash: capabilities.ControlHash(),
		Limits: contracts.SandboxLimitEnvelope{
			TimeoutMS: int(contracts.MaxToolTimeout / time.Millisecond), MaxStdoutBytes: contracts.MaxToolOutputBytes,
			MaxStderrBytes: contracts.MaxToolOutputBytes, MaxArgCount: contracts.MaxToolArgCount, MaxArgBytes: contracts.MaxToolArgBytes,
			MaxEnvEntries: contracts.MaxToolEnvCount, MaxEnvBytes: contracts.MaxToolEnvBytes, CPUQuotaMilli: 500,
			MemoryBytes: 256 << 20, PIDsLimit: 32, ScratchBytes: 64 << 20,
		},
		TestSuiteHash: contracts.HashStrings("sandbox-conformance-suite-v1"), Outcome: contracts.QualificationOutcomePassed,
		Measured: true, ObservedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	evidenceHash := evidence.StableHash()
	if evidenceHash == "" {
		t.Fatal("test sandbox qualification evidence is invalid")
	}
	report := contracts.QualificationReport{
		RunID: "sandbox-qualification-run", WorkspaceID: trust.WorkspaceID, TargetHash: trust.TargetHash,
		Outcome: contracts.QualificationOutcomePassed,
		Cases:   []contracts.QualificationCase{{Name: evidence.ID, Category: contracts.QualificationCategorySandbox, Outcome: contracts.QualificationOutcomePassed, EvidenceHash: evidenceHash}},
	}
	if err := report.Normalize(); err != nil {
		t.Fatal(err)
	}
	manifest := contracts.QualificationManifest{
		RunID: report.RunID, WorkspaceID: report.WorkspaceID, TargetHash: report.TargetHash,
		ReportHash: report.ReportHash, RunnerVersion: "1",
		Checks: []contracts.QualificationCheckManifest{{Name: evidence.ID, Version: "1", Category: contracts.QualificationCategorySandbox, ExecutionMode: "offline", InputHash: evidenceHash, Outcome: contracts.QualificationOutcomePassed, EvidenceHash: evidenceHash}},
	}
	bundle := contracts.QualificationBundle{Report: report, Manifest: manifest}
	if err := bundle.Normalize(); err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x42}, ed25519.SeedSize))
	signed, err := contracts.SignQualificationBundle(bundle, trust.KeyID, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return contracts.SandboxQualificationProof{Evidence: evidence, Bundle: signed}
}

func qualifiedSandboxRegistry(t *testing.T) *SandboxRegistry {
	t.Helper()
	registry, err := NewSandboxRegistryWithQualificationTrust(testSandboxTrust())
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func TestNonLocalSandboxRequiresAttemptExecutionAndReconciliation(t *testing.T) {
	registry := NewSandboxRegistry()
	unsafe := declaredSandboxProvider{backend: contracts.SandboxBackendOCI, capabilities: nonLocalTestProvider(t, &countingProcess{}).Capabilities(), process: &countingProcess{}}
	if err := registry.Register(unsafe); err == nil {
		t.Fatal("non-local provider without attempt execution and reconciliation was registered")
	}

	provider := nonLocalTestProvider(t, &countingProcess{})
	qualifiedRegistry := qualifiedSandboxRegistry(t)
	if err := qualifiedRegistry.Register(provider); err != nil {
		t.Fatalf("attempt-aware provider registration failed: %v", err)
	}
	resolved, err := qualifiedRegistry.Resolve(nonLocalTestProfile(t), "w1")
	if err != nil || resolved.Backend() != contracts.SandboxBackendOCI {
		t.Fatalf("exact OCI provider resolution=%v, err=%v", resolved, err)
	}
	if _, err := qualifiedRegistry.Resolve(nonLocalTestProfile(t)); err == nil {
		t.Fatal("workspace-pinned qualification was resolved without an execution workspace")
	}
	if _, err := qualifiedRegistry.Resolve(nonLocalTestProfile(t), "workspace-b"); err == nil {
		t.Fatal("workspace-pinned qualification was used by a different workspace")
	}
	wrongPlatform := nonLocalTestProfile(t)
	wrongPlatform.ImagePlatform = contracts.SandboxImagePlatform{OS: "linux", Architecture: "arm64"}
	if _, err := qualifiedRegistry.Resolve(wrongPlatform, "w1"); err == nil {
		t.Fatal("profile platform not bound by signed qualification was admitted")
	}
	outsideEnvelope := nonLocalTestProfile(t)
	outsideEnvelope.MemoryBytes++
	if _, err := qualifiedRegistry.Resolve(outsideEnvelope, "w1"); err == nil {
		t.Fatal("profile above the measured memory envelope was admitted")
	}
}

func TestSandboxRegistryRejectsQualifiedProviderDrift(t *testing.T) {
	profile := nonLocalTestProfile(t)
	provider := nonLocalTestProvider(t, &countingProcess{})
	registry := qualifiedSandboxRegistry(t)
	if err := registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Resolve(profile, "w1"); err != nil {
		t.Fatalf("initial qualified provider resolution: %v", err)
	}
	provider.runtimeIdentity.RuntimeBuildHash = contracts.HashStrings("runtime changed after registration")
	if _, err := registry.Resolve(profile, "w1"); err == nil {
		t.Fatal("provider runtime drift after registration was accepted")
	}

	provider = nonLocalTestProvider(t, &countingProcess{})
	registry = qualifiedSandboxRegistry(t)
	if err := registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	provider.runtimeIdentity.ImagePlatform = contracts.SandboxImagePlatform{OS: "linux", Architecture: "arm64"}
	if _, err := registry.Resolve(profile, "w1"); err == nil {
		t.Fatal("provider platform drift after registration was accepted")
	}

	provider = nonLocalTestProvider(t, &countingProcess{})
	registry = qualifiedSandboxRegistry(t)
	if err := registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	provider.capabilities.Limitations = append(provider.capabilities.Limitations, "control report changed")
	if _, err := registry.Resolve(profile, "w1"); err == nil {
		t.Fatal("provider capability drift after registration was accepted")
	}
}

func TestReconcileSandboxAttemptUsesRecordedIdentityWithoutExecuting(t *testing.T) {
	profile := nonLocalTestProfile(t)
	definition := testDefinition("/bin/echo")
	definition.Sandbox = profile
	definitionHash, err := definition.Hash()
	if err != nil {
		t.Fatal(err)
	}
	profileHash, err := profile.Hash()
	if err != nil {
		t.Fatal(err)
	}
	request := testRequest()
	request.Budget = profile
	provider := nonLocalTestProvider(t, &countingProcess{})
	request.SandboxQualificationHash = provider.proof.Evidence.StableHash()
	request.ToolDefinitionHash, request.SandboxProfileHash = definitionHash, profileHash
	requestHash, err := request.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	evidence, err := request.RedactedEvidence()
	if err != nil {
		t.Fatal(err)
	}
	run := contracts.ToolRun{
		ID: "tool-run-recovery", WorkspaceID: request.WorkspaceID, RequestID: request.RequestID,
		IdempotencyKey: request.IdempotencyKey, RequestHash: requestHash, ToolID: request.ToolID,
		Task: request.Task, TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence,
		AgentRun: request.AgentRun, AgentRunOwnerID: request.AgentRunOwnerID, AgentRunFence: request.AgentRunFence,
		RequestEvidence: evidence, Attempt: 1, Status: contracts.ToolRunRecoveryRequired,
	}
	link := contracts.DomainEffectLink{
		WorkspaceID: run.WorkspaceID, OperationID: "operation-recovery", OperationHash: contracts.HashStrings("operation"),
		OperationOwnerID: "original-operation-owner", OperationFence: 8, AttemptID: "attempt-recovery", EffectID: "effect-recovery",
		EffectReservationHash: contracts.HashStrings("reservation"), DomainKind: contracts.DomainEffectKindToolRun,
		DomainID: run.ID, DomainHash: run.RequestHash, LinkRole: contracts.DomainEffectLinkRolePrimary,
		Boundary: string(contracts.SandboxBackendOCI), Status: contracts.DomainEffectLinkStatusRecoveryRequired,
		TaskOwnerID: run.TaskOwnerID, TaskFence: run.TaskFence,
	}
	result := contracts.ToolResult{RequestID: run.RequestID, RunID: run.ID, ToolID: run.ToolID, Status: contracts.ToolRunSucceeded, Stdout: "recovered output"}
	recoveryProvider := &recordedRecoverySandboxProvider{
		attemptSandboxProvider: provider,
		observation:            contracts.SandboxAttemptObservation{State: contracts.SandboxAttemptCompleted, Result: &result, ResultHash: result.Hash()},
	}
	registry := qualifiedSandboxRegistry(t)
	if err := registry.Register(recoveryProvider); err != nil {
		t.Fatal(err)
	}
	identity := contracts.SandboxExecutionIdentity{
		SchemaVersion: contracts.SandboxExecutionIdentitySchemaVersion, WorkspaceID: run.WorkspaceID,
		ToolRunID: run.ID, ToolAttempt: run.Attempt, Backend: contracts.SandboxBackendOCI,
		OperationID: link.OperationID, OperationOwnerID: link.OperationOwnerID, OperationFence: link.OperationFence,
		AttemptID: link.AttemptID, EffectID: link.EffectID, ToolRequestHash: run.RequestHash,
		OperationRequestHash: link.OperationHash, EffectReservationHash: link.EffectReservationHash,
		ToolDefinitionHash: definitionHash, SandboxProfileHash: profileHash,
		QualificationHash: request.SandboxQualificationHash,
	}
	recoveryProvider.observation.IdentityHash = identity.StableHash()
	executor := &Executor{Sandboxes: registry}
	observedIdentity, observation, err := executor.ReconcileSandboxAttempt(context.Background(), run, link)
	if err != nil {
		t.Fatalf("reconcile attempt: %v", err)
	}
	if observation.State != contracts.SandboxAttemptCompleted || observation.ResultHash != result.Hash() || recoveryProvider.reconcileCalls != 1 || recoveryProvider.runCalls != 0 {
		t.Fatalf("reconciliation did not inspect exactly once without execution: observation=%+v reconcile=%d run=%d", observation, recoveryProvider.reconcileCalls, recoveryProvider.runCalls)
	}
	if recoveryProvider.identity.StableHash() != identity.StableHash() || observedIdentity.StableHash() != identity.StableHash() {
		t.Fatalf("provider received or returned non-canonical durable identity: got=%+v returned=%+v want=%+v", recoveryProvider.identity, observedIdentity, identity)
	}
}

func TestReconcileSandboxAttemptFailsClosedForUnknownOrMismatchedIdentity(t *testing.T) {
	profile := nonLocalTestProfile(t)
	request := testRequest()
	request.Budget = profile
	baseProvider := nonLocalTestProvider(t, &countingProcess{})
	request.SandboxQualificationHash = baseProvider.proof.Evidence.StableHash()
	definition := testDefinition("/bin/echo")
	definition.Sandbox = profile
	definitionHash, _ := definition.Hash()
	profileHash, _ := profile.Hash()
	request.ToolDefinitionHash, request.SandboxProfileHash = definitionHash, profileHash
	requestHash, _ := request.RequestHash()
	evidence, _ := request.RedactedEvidence()
	run := contracts.ToolRun{ID: "tool-run-recovery", WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey, RequestHash: requestHash, ToolID: request.ToolID, RequestEvidence: evidence, Attempt: 1, Status: contracts.ToolRunRecoveryRequired}
	link := contracts.DomainEffectLink{WorkspaceID: run.WorkspaceID, OperationID: "operation-recovery", OperationHash: contracts.HashStrings("operation"), OperationOwnerID: "original-owner", OperationFence: 1, AttemptID: "attempt-recovery", EffectID: "effect-recovery", EffectReservationHash: contracts.HashStrings("reservation"), DomainKind: contracts.DomainEffectKindToolRun, DomainID: run.ID, DomainHash: run.RequestHash, LinkRole: contracts.DomainEffectLinkRolePrimary, Boundary: string(contracts.SandboxBackendOCI), Status: contracts.DomainEffectLinkStatusRecoveryRequired}
	identity := contracts.SandboxExecutionIdentity{SchemaVersion: contracts.SandboxExecutionIdentitySchemaVersion, WorkspaceID: run.WorkspaceID, ToolRunID: run.ID, ToolAttempt: run.Attempt, Backend: contracts.SandboxBackendOCI, OperationID: link.OperationID, OperationOwnerID: link.OperationOwnerID, OperationFence: link.OperationFence, AttemptID: link.AttemptID, EffectID: link.EffectID, ToolRequestHash: run.RequestHash, OperationRequestHash: link.OperationHash, EffectReservationHash: link.EffectReservationHash, ToolDefinitionHash: definitionHash, SandboxProfileHash: profileHash, QualificationHash: request.SandboxQualificationHash}
	provider := &recordedRecoverySandboxProvider{attemptSandboxProvider: baseProvider, observation: contracts.SandboxAttemptObservation{IdentityHash: identity.StableHash(), State: contracts.SandboxAttemptUnknown}}
	registry := qualifiedSandboxRegistry(t)
	if err := registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	executor := &Executor{Sandboxes: registry}
	if _, got, err := executor.ReconcileSandboxAttempt(context.Background(), run, link); err != nil || got.State != contracts.SandboxAttemptUnknown {
		t.Fatalf("unknown outcome should remain non-terminal: got=%+v err=%v", got, err)
	}
	link.WorkspaceID = "other-workspace"
	if _, _, err := executor.ReconcileSandboxAttempt(context.Background(), run, link); err == nil {
		t.Fatal("cross-workspace link was accepted")
	}
	link.WorkspaceID = run.WorkspaceID
	link.DomainHash = contracts.HashStrings("different tool request")
	if _, _, err := executor.ReconcileSandboxAttempt(context.Background(), run, link); err == nil {
		t.Fatal("mismatched tool request hash was accepted")
	}
	link.DomainHash = run.RequestHash
	legacyRequest := request
	legacyRequest.SandboxQualificationHash = ""
	legacyEvidence, err := legacyRequest.RedactedEvidence()
	if err != nil {
		t.Fatal(err)
	}
	legacyRun := run
	legacyRun.RequestEvidence = legacyEvidence
	if _, _, err := executor.ReconcileSandboxAttempt(context.Background(), legacyRun, link); err == nil {
		t.Fatal("legacy non-local attempt without qualification identity was silently reconciled")
	}
	if provider.reconcileCalls != 1 || provider.runCalls != 0 {
		t.Fatalf("invalid recovery identities reached provider or executed: reconcile=%d run=%d", provider.reconcileCalls, provider.runCalls)
	}
}

func TestNonLocalSandboxReceivesFencedEffectIdentity(t *testing.T) {
	provider := nonLocalTestProvider(t, &countingProcess{})
	registry := qualifiedSandboxRegistry(t)
	if err := registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	profile := nonLocalTestProfile(t)
	definition := testDefinition("/bin/echo")
	definition.Sandbox = profile
	request := testRequest()
	request.Budget = profile
	request.SandboxQualificationHash = provider.proof.Evidence.StableHash()
	request.Task = &contracts.EntityRef{ID: "task-a", Kind: "task", WorkspaceID: request.WorkspaceID}
	request.TaskOwnerID, request.TaskFence = "task-worker-a", 7
	request.AgentRun = &contracts.EntityRef{ID: "agent-run-a", Kind: "agent_run", WorkspaceID: request.WorkspaceID}
	request.AgentRunOwnerID, request.AgentRunFence = "agent-worker-a", 3
	requestHash, err := request.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	definitionHash, err := definition.Hash()
	if err != nil {
		t.Fatal(err)
	}
	profileHash, err := profile.Hash()
	if err != nil {
		t.Fatal(err)
	}
	authority := contracts.EffectAuthority{
		WorkspaceID: request.WorkspaceID, OperationID: "operation-a", OperationOwnerID: "operation-worker", OperationFence: 4,
		AttemptID: "attempt-a", EffectID: "effect-a", RequestHash: contracts.HashStrings("parent-operation"), EffectReservationHash: contracts.HashStrings("reservation"),
		TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence,
	}
	if err := authority.Normalize(); err != nil {
		t.Fatal(err)
	}
	run := contracts.ToolRun{ID: "tool-run-a", WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, ToolID: request.ToolID, RequestHash: requestHash, Attempt: 1, Task: request.Task, TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence, AgentRun: request.AgentRun, AgentRunOwnerID: request.AgentRunOwnerID, AgentRunFence: request.AgentRunFence}
	executor := &Executor{Sandboxes: registry, Effects: authorityToolEffects{authority: authority}}
	if _, err := executor.runEffect(context.Background(), request, definition, run); err != nil {
		t.Fatalf("non-local run failed: %v", err)
	}
	if provider.calls != 1 {
		t.Fatalf("attempt provider calls=%d, want 1", provider.calls)
	}
	identity := provider.identity
	if identity.WorkspaceID != request.WorkspaceID || identity.ToolRunID != run.ID || identity.OperationFence != authority.OperationFence || identity.AttemptID != authority.AttemptID || identity.EffectID != authority.EffectID || identity.ToolRequestHash != requestHash || identity.OperationRequestHash != authority.RequestHash || identity.TaskOwnerID != "task-worker-a" || identity.TaskFence != 7 || identity.AgentRunID != "agent-run-a" || identity.AgentRunOwnerID != "agent-worker-a" || identity.AgentRunFence != 3 || identity.ToolDefinitionHash != definitionHash || identity.SandboxProfileHash != profileHash || identity.QualificationHash != request.SandboxQualificationHash {
		t.Fatalf("provider received incomplete execution identity: %+v", identity)
	}
	if identity.RuntimeName() == "" {
		t.Fatal("provider execution identity did not produce an opaque runtime name")
	}
}

func TestExecutePersistsSandboxQualificationInDurableRunEvidence(t *testing.T) {
	profile := nonLocalTestProfile(t)
	definition := testDefinition("/bin/echo")
	definition.Sandbox = profile
	definitions := NewRegistry()
	if err := definitions.Register(definition); err != nil {
		t.Fatal(err)
	}
	policy, err := NewPolicy([]contracts.ToolPolicyRule{{
		ID: "allow-qualified", WorkspaceID: "w1", ToolID: definition.ID, Capability: definition.Capability,
		Mode: contracts.ToolModeAutomatic, Enabled: true, Sandbox: profile,
	}})
	if err != nil {
		t.Fatal(err)
	}
	provider := nonLocalTestProvider(t, &countingProcess{})
	sandboxes := qualifiedSandboxRegistry(t)
	if err := sandboxes.Register(provider); err != nil {
		t.Fatal(err)
	}
	store := newFakeRunStore()
	authority := contracts.EffectAuthority{
		WorkspaceID: "w1", OperationID: "operation-qualified", OperationOwnerID: "worker-qualified", OperationFence: 1,
		AttemptID: "attempt-qualified", EffectID: "effect-qualified", RequestHash: contracts.HashStrings("parent operation"),
		EffectReservationHash: contracts.HashStrings("effect reservation"),
	}
	executor := &Executor{Registry: definitions, Policy: policy, Store: store, Sandboxes: sandboxes, Effects: authorityToolEffects{authority: authority}}
	request := testRequest()
	request.Budget = profile
	outcome, err := executor.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("execute qualified tool request: %v", err)
	}
	var recorded contracts.ToolRequest
	if err := json.Unmarshal(outcome.Run.RequestEvidence, &recorded); err != nil {
		t.Fatalf("decode durable request evidence: %v", err)
	}
	wantQualificationHash := provider.proof.Evidence.StableHash()
	if recorded.SandboxQualificationHash != wantQualificationHash || provider.identity.QualificationHash != wantQualificationHash {
		t.Fatalf("qualification identity was not persisted and passed to attempt: evidence=%q attempt=%q want=%q", recorded.SandboxQualificationHash, provider.identity.QualificationHash, wantQualificationHash)
	}
}

func TestNonLocalSandboxRequiresDurableEffectDispatcher(t *testing.T) {
	provider := nonLocalTestProvider(t, &countingProcess{})
	registry := qualifiedSandboxRegistry(t)
	if err := registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	profile := nonLocalTestProfile(t)
	definition := testDefinition("/bin/echo")
	definition.Sandbox = profile
	request := testRequest()
	request.Budget = profile
	request.SandboxQualificationHash = provider.proof.Evidence.StableHash()
	requestHash, err := request.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	executor := &Executor{Sandboxes: registry}
	_, err = executor.runEffect(context.Background(), request, definition, contracts.ToolRun{ID: "run-a", WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, ToolID: request.ToolID, RequestHash: requestHash, Attempt: 1})
	var failureErr *FailureError
	if !errors.As(err, &failureErr) || failureErr.Failure.Code != contracts.ToolFailureSandboxUnavailable {
		t.Fatalf("non-local provider without effect authority error=%v", err)
	}
	if provider.calls != 0 {
		t.Fatalf("provider ran without durable effect authority: calls=%d", provider.calls)
	}
}

func TestNonLocalSandboxRejectsMismatchedWorkspaceBeforeInvocation(t *testing.T) {
	provider := nonLocalTestProvider(t, &countingProcess{})
	registry := qualifiedSandboxRegistry(t)
	if err := registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	profile := nonLocalTestProfile(t)
	definition := testDefinition("/bin/echo")
	definition.Sandbox = profile
	request := testRequest()
	request.Budget = profile
	request.SandboxQualificationHash = provider.proof.Evidence.StableHash()
	requestHash, err := request.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	authority := contracts.EffectAuthority{WorkspaceID: "other-workspace", OperationID: "operation-a", OperationOwnerID: "worker-a", OperationFence: 1, AttemptID: "attempt-a", EffectID: "effect-a", RequestHash: requestHash, EffectReservationHash: contracts.HashStrings("reservation")}
	executor := &Executor{Sandboxes: registry, Effects: authorityToolEffects{authority: authority}}
	_, err = executor.runEffect(context.Background(), request, definition, contracts.ToolRun{ID: "run-a", WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, ToolID: request.ToolID, RequestHash: requestHash, Attempt: 1})
	var failureErr *FailureError
	if !errors.As(err, &failureErr) || failureErr.Failure.Code != contracts.ToolFailureSandboxCapability {
		t.Fatalf("mismatched authority error=%v, want sandbox capability failure", err)
	}
	if provider.calls != 0 {
		t.Fatalf("mismatched authority reached provider %d times", provider.calls)
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

func TestLocalExecutorRejectsMissingLeafThroughOutsideSymlink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, "linked")); err != nil {
		t.Fatal(err)
	}

	definition := testDefinition("/usr/bin/touch")
	definition.WorkdirRoot = root
	definition.PathArgvIndexes = []int{1}
	definition.Sandbox.ReadOnlyWorkdir = true
	definition.Sandbox.AllowedWorkdirRoot = root
	request := testRequest()
	request.ToolID = definition.ID
	request.Capability = definition.Capability
	request.Argv = []string{definition.Executable, filepath.Join("linked", "must-not-exist.txt")}
	request.Workdir = root
	request.Budget = definition.Sandbox
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	result, err := (LocalExecutor{}).Run(context.Background(), definition, request)
	var failureErr *FailureError
	if !errors.As(err, &failureErr) || failureErr.Failure.Code != contracts.ToolFailureWorkdirDenied {
		t.Fatalf("expected unresolved outside path rejection, result=%#v err=%v", result, err)
	}
	if _, err := os.Stat(filepath.Join(outside, "must-not-exist.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("process reached a path outside the authorized root: stat err=%v", err)
	}
}

func TestExecutorDuplicateAndTerminalReplayDoNotRunProcessTwice(t *testing.T) {
	store := newFakeRunStore()
	process := &countingProcess{result: contracts.ToolResult{Status: contracts.ToolRunSucceeded, Stdout: "ok"}}
	executor := &Executor{Registry: registryForTest(t), Policy: testPolicy(t, "w1", "test.tool", contracts.ToolModeAutomatic), Store: store, Sandboxes: testSandboxRegistry(t, process)}
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

func TestExecutorReplayRejectsChangedToolDefinitionForSameIdempotencyKey(t *testing.T) {
	store := newFakeRunStore()
	firstDefinition := testDefinition("/bin/echo")
	firstRegistry := NewRegistry()
	if err := firstRegistry.Register(firstDefinition); err != nil {
		t.Fatal(err)
	}
	firstProcess := &countingProcess{result: contracts.ToolResult{Status: contracts.ToolRunSucceeded, Stdout: "first"}}
	first := &Executor{Registry: firstRegistry, Policy: testPolicy(t, "w1", firstDefinition.ID, contracts.ToolModeAutomatic), Store: store, Sandboxes: testSandboxRegistry(t, firstProcess)}
	request := testRequest()
	if _, err := first.Execute(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	changedDefinition := firstDefinition
	changedDefinition.Version = "2"
	changedRegistry := NewRegistry()
	if err := changedRegistry.Register(changedDefinition); err != nil {
		t.Fatal(err)
	}
	secondProcess := &countingProcess{result: contracts.ToolResult{Status: contracts.ToolRunSucceeded, Stdout: "second"}}
	second := &Executor{Registry: changedRegistry, Policy: testPolicy(t, "w1", changedDefinition.ID, contracts.ToolModeAutomatic), Store: store, Sandboxes: testSandboxRegistry(t, secondProcess)}
	if _, err := second.Execute(context.Background(), request); !errors.Is(err, ErrRunConflict) {
		t.Fatalf("changed tool definition did not conflict with prior execution: %v", err)
	}
	if secondProcess.calls != 0 {
		t.Fatalf("changed definition executed despite idempotency conflict: calls=%d", secondProcess.calls)
	}
}

func TestExecutorReplayRejectsChangedSandboxProfileForSameIdempotencyKey(t *testing.T) {
	store := newFakeRunStore()
	firstDefinition := testDefinition("/bin/echo")
	firstRegistry := NewRegistry()
	if err := firstRegistry.Register(firstDefinition); err != nil {
		t.Fatal(err)
	}
	firstProcess := &countingProcess{result: contracts.ToolResult{Status: contracts.ToolRunSucceeded, Stdout: "first"}}
	first := &Executor{Registry: firstRegistry, Policy: testPolicy(t, "w1", firstDefinition.ID, contracts.ToolModeAutomatic), Store: store, Sandboxes: testSandboxRegistry(t, firstProcess)}
	request := testRequest()
	if _, err := first.Execute(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	changedDefinition := firstDefinition
	changedDefinition.Sandbox.TimeoutMS = firstDefinition.Sandbox.TimeoutMS / 2
	changedRegistry := NewRegistry()
	if err := changedRegistry.Register(changedDefinition); err != nil {
		t.Fatal(err)
	}
	secondProcess := &countingProcess{result: contracts.ToolResult{Status: contracts.ToolRunSucceeded, Stdout: "second"}}
	second := &Executor{Registry: changedRegistry, Policy: testPolicy(t, "w1", changedDefinition.ID, contracts.ToolModeAutomatic), Store: store, Sandboxes: testSandboxRegistry(t, secondProcess)}
	if _, err := second.Execute(context.Background(), request); !errors.Is(err, ErrRunConflict) {
		t.Fatalf("changed sandbox profile did not conflict with prior execution: %v", err)
	}
	if secondProcess.calls != 0 {
		t.Fatalf("changed sandbox profile executed despite idempotency conflict: calls=%d", secondProcess.calls)
	}
}

func TestExecutorInteractiveApprovalThenContinuation(t *testing.T) {
	store := newFakeRunStore()
	executor := &Executor{Registry: registryForTest(t), Policy: testPolicy(t, "w1", "test.tool", contracts.ToolModeInteractive), Store: store, Sandboxes: testSandboxRegistry(t, &countingProcess{result: contracts.ToolResult{Status: contracts.ToolRunSucceeded, Stdout: "approved"}})}
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
	executor := &Executor{Registry: registryForTest(t), Policy: testPolicy(t, "w1", "test.tool", contracts.ToolModeAutomatic), Store: store, Fence: rejectingFence{}, Sandboxes: testSandboxRegistry(t, process)}
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
	executor := &Executor{Registry: registryForTest(t), Policy: testPolicy(t, "w1", "test.tool", contracts.ToolModeAutomatic), Store: store, Sandboxes: testSandboxRegistry(t, &countingProcess{result: contracts.ToolResult{Status: contracts.ToolRunSucceeded}})}
	_, err = executor.Execute(context.Background(), request)
	if !errors.Is(err, ErrRunInProgress) {
		t.Fatalf("in-flight duplicate error=%v", err)
	}
}

func TestExecutorPersistsRecoveryRequiredForUncertainExternalOutcome(t *testing.T) {
	store := newFakeRunStore()
	process := &countingProcess{result: contracts.ToolResult{Status: contracts.ToolRunSucceeded, Stdout: "must not run"}}
	executor := &Executor{
		Registry:  registryForTest(t),
		Policy:    testPolicy(t, "w1", "test.tool", contracts.ToolModeAutomatic),
		Store:     store,
		Sandboxes: testSandboxRegistry(t, process),
		Effects:   uncertainToolEffects{},
	}
	request := testRequest()
	first, err := executor.Execute(context.Background(), request)
	if err == nil || first.Run.Status != contracts.ToolRunRecoveryRequired {
		t.Fatalf("uncertain tool outcome = %#v err=%v", first, err)
	}
	if process.calls != 0 {
		t.Fatalf("process calls = %d, want zero", process.calls)
	}
	second, err := executor.Execute(context.Background(), request)
	if !errors.Is(err, ErrRunRecovery) || !second.Deduplicated || process.calls != 0 {
		t.Fatalf("recovery duplicate = %#v err=%v calls=%d", second, err, process.calls)
	}
}

type uncertainToolEffects struct{}

func (uncertainToolEffects) Run(context.Context, contracts.ToolRequest, contracts.ToolDefinition, contracts.ToolRun, func(context.Context, contracts.EffectAuthority) (contracts.ToolResult, error)) (contracts.ToolResult, error) {
	return contracts.ToolResult{}, uncertainToolError{}
}

type uncertainToolError struct{}

func (uncertainToolError) Error() string                  { return "test tool outcome uncertain" }
func (uncertainToolError) UncertainExternalOutcome() bool { return true }

type countingProcess struct {
	mu     sync.Mutex
	calls  int
	result contracts.ToolResult
}

type workdirRecordingProcess struct {
	calls          int
	definitionRoot string
	requestWorkdir string
}

func (p *workdirRecordingProcess) Run(_ context.Context, definition contracts.ToolDefinition, request contracts.ToolRequest) (contracts.ToolResult, error) {
	p.calls++
	p.definitionRoot = definition.Sandbox.AllowedWorkdirRoot
	p.requestWorkdir = request.Workdir
	return contracts.ToolResult{Status: contracts.ToolRunSucceeded}, nil
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
	run.RequestEvidence, _ = req.RedactedEvidence()
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
