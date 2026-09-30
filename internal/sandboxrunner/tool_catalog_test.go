package sandboxrunner

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func catalogTestDefinition(t *testing.T) contracts.ToolDefinition {
	t.Helper()
	profile := contracts.SandboxProfile{
		Backend: string(contracts.SandboxBackendOCI), TimeoutMS: 5000,
		MaxStdoutBytes: 1024, MaxStderrBytes: 1024,
		MaxArgCount: 16, MaxArgBytes: 1024, MaxEnvEntries: 0, MaxEnvBytes: 0,
		CPUQuotaMilli: 500, MemoryBytes: 64 << 20, PIDsLimit: 32, ScratchBytes: 8 << 20,
		ImageDigest:     "sha256:" + strings.Repeat("a", 64),
		ImagePlatform:   contracts.SandboxImagePlatform{OS: "linux", Architecture: "amd64"},
		ReadOnlyWorkdir: true, ReadOnlyRootFS: true,
	}
	definition := contracts.ToolDefinition{
		ID: "repository.show", Version: "1", Name: "Repository show", Capability: "repository.read",
		Executable: "/usr/bin/git", ArgvPrefix: []string{"show"}, PathArgvIndexes: []int{2},
		Sandbox: profile, Enabled: true,
	}
	if err := definition.Normalize(); err != nil {
		t.Fatalf("normalize test tool: %v", err)
	}
	return definition
}

func catalogTestRequest(t *testing.T, definition contracts.ToolDefinition, reference contracts.WorkspaceMountRef) contracts.SandboxRunnerRequest {
	t.Helper()
	definitionHash, err := definition.Hash()
	if err != nil {
		t.Fatal(err)
	}
	profileHash, err := definition.Sandbox.Hash()
	if err != nil {
		t.Fatal(err)
	}
	runnerProfile, err := contracts.NewSandboxRunnerProfile(definition.Sandbox)
	if err != nil {
		t.Fatal(err)
	}
	identity := contracts.SandboxExecutionIdentity{
		SchemaVersion: contracts.SandboxExecutionIdentitySchemaVersion,
		WorkspaceID:   reference.WorkspaceID, ToolRunID: "tool-run-catalog", ToolAttempt: 1,
		Backend: contracts.SandboxBackendOCI, OperationID: "operation-catalog", OperationOwnerID: "owner-catalog", OperationFence: 1,
		AttemptID: "attempt-catalog", EffectID: "effect-catalog",
		ToolRequestHash: contracts.HashStrings("tool request"), OperationRequestHash: contracts.HashStrings("operation request"),
		EffectReservationHash: contracts.HashStrings("reservation"), ToolDefinitionHash: definitionHash,
		SandboxProfileHash: profileHash, QualificationHash: contracts.HashStrings("qualification"),
	}
	return contracts.SandboxRunnerRequest{
		SchemaVersion: contracts.SandboxRunnerProtocolVersion,
		RequestID:     "toolreq-catalog", WorkspaceID: reference.WorkspaceID, WorkspaceMount: reference,
		Execution: identity, ToolID: definition.ID, ToolDefinitionHash: definitionHash,
		SandboxProfileHash: profileHash, RunnerProfileHash: runnerProfile.Hash(),
		Argv: []string{"show", "notes.txt"}, WorkingDirectory: "src", Profile: runnerProfile,
	}
}

func catalogTestMount(t *testing.T, workspace string) (*MountCatalog, *ResolvedMount, contracts.WorkspaceMountRef, string) {
	t.Helper()
	root := canonicalTempDir(t)
	if err := os.Mkdir(filepath.Join(root, "src"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "src", "notes.txt"), []byte("safe"), 0o600); err != nil {
		t.Fatal(err)
	}
	reference := mountRef(workspace, "c")
	catalog, err := NewMountCatalog([]MountRegistration{{Reference: reference, HostPath: root}})
	if err != nil {
		t.Fatal(err)
	}
	mount, err := catalog.Resolve(reference)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mount.Close() })
	return catalog, mount, reference, root
}

func TestToolCatalogResolvesExactDefinitionAndWorkspaceBoundPaths(t *testing.T) {
	mountCatalog, mount, reference, _ := catalogTestMount(t, "workspace-a")
	_ = mountCatalog
	definition := catalogTestDefinition(t)
	catalog, err := NewToolCatalog([]contracts.ToolDefinition{definition})
	if err != nil {
		t.Fatal(err)
	}
	request := catalogTestRequest(t, definition, reference)
	invocation, err := catalog.Resolve(request, mount)
	if err != nil {
		t.Fatalf("resolve trusted invocation: %v", err)
	}
	if invocation.Executable != "/usr/bin/git" || strings.Join(invocation.Argv, " ") != "/usr/bin/git show notes.txt" || invocation.WorkingDirectory != "/workspace/src" {
		t.Fatalf("unexpected invocation: %#v", invocation)
	}
	if len(invocation.Environment) != 0 {
		t.Fatalf("offline invocation unexpectedly has environment: %#v", invocation.Environment)
	}
}

func TestToolCatalogFailsClosedForUnknownProfileWorkspaceAndArgv(t *testing.T) {
	_, mount, reference, _ := catalogTestMount(t, "workspace-a")
	definition := catalogTestDefinition(t)
	catalog, err := NewToolCatalog([]contracts.ToolDefinition{definition})
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		change func(*contracts.SandboxRunnerRequest)
		want   error
	}{
		{name: "unregistered hash", change: func(r *contracts.SandboxRunnerRequest) { r.ToolDefinitionHash = contracts.HashStrings("unregistered") }, want: ErrInvocationDenied},
		{name: "wrong profile", change: func(r *contracts.SandboxRunnerRequest) { r.Profile.TimeoutMS--; r.RunnerProfileHash = r.Profile.Hash() }, want: ErrInvocationDenied},
		{name: "argv prefix", change: func(r *contracts.SandboxRunnerRequest) { r.Argv[0] = "status" }, want: ErrInvocationDenied},
		{name: "argument byte budget", change: func(r *contracts.SandboxRunnerRequest) { r.Argv[1] = strings.Repeat("x", r.Profile.MaxArgBytes+1) }, want: ErrInvocationDenied},
		{name: "environment denied", change: func(r *contracts.SandboxRunnerRequest) { r.Environment = map[string]string{"TOKEN": "do-not-echo"} }, want: ErrInvocationDenied},
		{name: "path traversal", change: func(r *contracts.SandboxRunnerRequest) { r.Argv[1] = "../outside.txt" }, want: ErrInvocationDenied},
		{name: "absolute path", change: func(r *contracts.SandboxRunnerRequest) { r.Argv[1] = "/etc/passwd" }, want: ErrInvocationDenied},
		{name: "missing path", change: func(r *contracts.SandboxRunnerRequest) { r.Argv[1] = "absent.txt" }, want: ErrInvocationDenied},
		{name: "other mount identity", change: func(r *contracts.SandboxRunnerRequest) { r.WorkspaceMount = mountRef("workspace-a", "d") }, want: ErrInvocationDenied},
		{name: "cross-workspace mount", change: func(r *contracts.SandboxRunnerRequest) { r.WorkspaceMount.WorkspaceID = "workspace-b" }, want: ErrInvocationDenied},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := catalogTestRequest(t, definition, reference)
			test.change(&request)
			_, err := catalog.Resolve(request, mount)
			if !errors.Is(err, test.want) {
				t.Fatalf("Resolve() error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestToolCatalogRejectsSymlinkEscapeAndShellDefinitions(t *testing.T) {
	_, mount, reference, root := catalogTestMount(t, "workspace-a")
	outside := canonicalTempDir(t)
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "src", "external")); err != nil {
		t.Fatal(err)
	}
	definition := catalogTestDefinition(t)
	catalog, err := NewToolCatalog([]contracts.ToolDefinition{definition})
	if err != nil {
		t.Fatal(err)
	}
	request := catalogTestRequest(t, definition, reference)
	request.Argv[1] = "external/secret.txt"
	if _, err := catalog.Resolve(request, mount); !errors.Is(err, ErrInvocationDenied) {
		t.Fatalf("symlink escape error = %v, want invocation denied", err)
	}
	definition.Executable = "/bin/sh"
	if err := catalog.Register(definition); !errors.Is(err, ErrToolCatalogUnavailable) {
		t.Fatalf("shell registration error = %v, want catalog unavailable", err)
	}
}

func TestToolCatalogRegistrationIsIdempotentAndResolutionIsConcurrent(t *testing.T) {
	_, mount, reference, _ := catalogTestMount(t, "workspace-a")
	definition := catalogTestDefinition(t)
	catalog, err := NewToolCatalog([]contracts.ToolDefinition{definition})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Register(definition); err != nil {
		t.Fatalf("identical registration: %v", err)
	}
	request := catalogTestRequest(t, definition, reference)
	const workers = 16
	var wait sync.WaitGroup
	errCh := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			invocation, err := catalog.Resolve(request, mount)
			if err == nil && (invocation.ToolID != definition.ID || invocation.WorkingDirectory != "/workspace/src") {
				err = ErrInvocationDenied
			}
			errCh <- err
		}()
	}
	wait.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("concurrent resolve: %v", err)
		}
	}
}

func TestToolCatalogKeepsDistinctDefinitionSnapshots(t *testing.T) {
	_, mount, reference, _ := catalogTestMount(t, "workspace-a")
	definition := catalogTestDefinition(t)
	catalog, err := NewToolCatalog([]contracts.ToolDefinition{definition})
	if err != nil {
		t.Fatal(err)
	}
	oldRequest := catalogTestRequest(t, definition, reference)
	updated := cloneToolDefinition(definition)
	updated.Version = "2"
	if err := catalog.Register(updated); err != nil {
		t.Fatalf("register updated definition snapshot: %v", err)
	}
	newRequest := catalogTestRequest(t, updated, reference)
	for _, request := range []contracts.SandboxRunnerRequest{oldRequest, newRequest} {
		if _, err := catalog.Resolve(request, mount); err != nil {
			t.Fatalf("resolve registered snapshot %s: %v", request.ToolDefinitionHash, err)
		}
	}
}

func TestToolCatalogErrorsDoNotEchoUntrustedArgumentsOrHostPaths(t *testing.T) {
	_, mount, reference, root := catalogTestMount(t, "workspace-a")
	definition := catalogTestDefinition(t)
	catalog, err := NewToolCatalog([]contracts.ToolDefinition{definition})
	if err != nil {
		t.Fatal(err)
	}
	request := catalogTestRequest(t, definition, reference)
	request.Argv[0] = "secret-command-value"
	_, err = catalog.Resolve(request, mount)
	if err == nil {
		t.Fatal("invalid argv unexpectedly resolved")
	}
	if strings.Contains(err.Error(), "secret-command-value") || strings.Contains(err.Error(), root) {
		t.Fatalf("error exposed request or host path data: %v", err)
	}
}
