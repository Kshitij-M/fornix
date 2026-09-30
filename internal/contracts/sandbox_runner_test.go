package contracts

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func validSandboxRunnerRequest(t *testing.T) SandboxRunnerRequest {
	return validSandboxRunnerRequestForPlatform(t, SandboxImagePlatform{OS: "linux", Architecture: "amd64"})
}

func validSandboxRunnerRequestForPlatform(t *testing.T, platform SandboxImagePlatform) SandboxRunnerRequest {
	t.Helper()
	profile := SandboxProfile{
		Backend: string(SandboxBackendOCI), TimeoutMS: 5000,
		MaxStdoutBytes: 1024, MaxStderrBytes: 1024,
		MaxArgCount: 16, MaxArgBytes: 1024, MaxEnvEntries: 8, MaxEnvBytes: 1024,
		CPUQuotaMilli: 500, MemoryBytes: 64 << 20, PIDsLimit: 32, ScratchBytes: 8 << 20,
		ImageDigest:        "sha256:" + strings.Repeat("a", 64),
		ImagePlatform:      platform,
		AllowedWorkdirRoot: "/workspace/repository",
		AllowNetwork:       false, ReadOnlyWorkdir: true, ReadOnlyRootFS: true,
	}
	if err := profile.Normalize(); err != nil {
		t.Fatal(err)
	}
	profileHash, err := profile.Hash()
	if err != nil {
		t.Fatal(err)
	}
	runnerProfile, err := NewSandboxRunnerProfile(profile)
	if err != nil {
		t.Fatal(err)
	}
	identity := validSandboxExecutionIdentity()
	identity.SandboxProfileHash = profileHash
	identity.ToolDefinitionHash = HashStrings("registered git status tool")
	return SandboxRunnerRequest{
		SchemaVersion:      SandboxRunnerProtocolVersion,
		RequestID:          "toolreq-runner-test",
		WorkspaceID:        "workspace-a",
		WorkspaceMount:     WorkspaceMountRef{ID: "wsmount_" + strings.Repeat("b", 32), WorkspaceID: "workspace-a"},
		Execution:          identity,
		ToolID:             "repository.status",
		ToolDefinitionHash: identity.ToolDefinitionHash,
		SandboxProfileHash: profileHash,
		RunnerProfileHash:  runnerProfile.Hash(),
		Argv:               []string{"status", "--short"},
		WorkingDirectory:   "src",
		Profile:            runnerProfile,
	}
}

func TestSandboxRunnerIdentityBindsImagePlatform(t *testing.T) {
	amd64 := validSandboxRunnerRequestForPlatform(t, SandboxImagePlatform{OS: "linux", Architecture: "amd64"})
	arm64 := validSandboxRunnerRequestForPlatform(t, SandboxImagePlatform{OS: "linux", Architecture: "arm64"})
	if err := amd64.Normalize(); err != nil {
		t.Fatal(err)
	}
	if err := arm64.Normalize(); err != nil {
		t.Fatal(err)
	}
	if amd64.StableHash() == arm64.StableHash() || amd64.RunnerProfileHash == arm64.RunnerProfileHash || amd64.SandboxProfileHash == arm64.SandboxProfileHash {
		t.Fatal("runner request/profile identity did not bind the selected image platform")
	}
}

func TestSandboxRunnerRequestNormalizesAndHashesDeterministically(t *testing.T) {
	request := validSandboxRunnerRequest(t)
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	first := request.StableHash()
	if first == "" {
		t.Fatal("valid request has no stable hash")
	}
	clone := validSandboxRunnerRequest(t)
	clone.Argv = []string{"status", "--short"}
	if err := clone.Normalize(); err != nil {
		t.Fatal(err)
	}
	if got := clone.StableHash(); got != first {
		t.Fatalf("canonical request hash changed: %s != %s", got, first)
	}
	clone.Argv[1] = "--branch"
	if got := clone.StableHash(); got == first {
		t.Fatal("argv change did not alter request hash")
	}

	raw, err := request.MarshalBounded()
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > MaxSandboxRunnerRequestBytes {
		t.Fatalf("serialized request exceeds hard cap: %d", len(raw))
	}
	var decoded SandboxRunnerRequest
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Normalize(); err != nil {
		t.Fatalf("round-tripped request is invalid: %v", err)
	}
	for _, forbiddenField := range []string{"host_path", "executable", "allowed_workdir_root", "/workspace/repository", "docker_options", "mounts", "daemon_socket"} {
		if bytes.Contains(raw, []byte(forbiddenField)) {
			t.Fatalf("protocol unexpectedly serializes forbidden field %q", forbiddenField)
		}
	}
}

func TestSandboxRunnerRequestRejectsUnsafeOrMismatchedAuthority(t *testing.T) {
	tests := []struct {
		name   string
		change func(*SandboxRunnerRequest)
	}{
		{"legacy protocol v1", func(r *SandboxRunnerRequest) { r.SchemaVersion = 1 }},
		{"unsupported protocol", func(r *SandboxRunnerRequest) { r.SchemaVersion = SandboxRunnerProtocolVersion + 1 }},
		{"mount workspace mismatch", func(r *SandboxRunnerRequest) { r.WorkspaceMount.WorkspaceID = "workspace-b" }},
		{"malformed mount ref", func(r *SandboxRunnerRequest) { r.WorkspaceMount.ID = "/Users/operator/repo" }},
		{"identity workspace mismatch", func(r *SandboxRunnerRequest) { r.Execution.WorkspaceID = "workspace-b" }},
		{"wrong backend", func(r *SandboxRunnerRequest) { r.Execution.Backend = SandboxBackendGVisor }},
		{"definition hash mismatch", func(r *SandboxRunnerRequest) { r.ToolDefinitionHash = HashStrings("other tool") }},
		{"profile hash mismatch", func(r *SandboxRunnerRequest) { r.SandboxProfileHash = HashStrings("other profile") }},
		{"absolute workdir", func(r *SandboxRunnerRequest) { r.WorkingDirectory = "/etc" }},
		{"workdir traversal", func(r *SandboxRunnerRequest) { r.WorkingDirectory = "src/../etc" }},
		{"backslash workdir", func(r *SandboxRunnerRequest) { r.WorkingDirectory = `src\\..\\etc` }},
		{"network enabled", func(r *SandboxRunnerRequest) { r.Profile.AllowNetwork = true }},
		{"writable root", func(r *SandboxRunnerRequest) { r.Profile.ReadOnlyRootFS = false }},
		{"writable workspace", func(r *SandboxRunnerRequest) { r.Profile.ReadOnlyWorkdir = false }},
		{"oversized argument", func(r *SandboxRunnerRequest) { r.Argv = []string{strings.Repeat("x", 1025)} }},
		{"nul argument", func(r *SandboxRunnerRequest) { r.Argv = []string{"bad\x00arg"} }},
		{"user environment value", func(r *SandboxRunnerRequest) { r.Environment = map[string]string{"TOKEN": "secret-value"} }},
		{"inherited environment", func(r *SandboxRunnerRequest) { r.Profile.InheritEnvironment = true }},
		{"oversized output policy", func(r *SandboxRunnerRequest) { r.Profile.MaxStdoutBytes = MaxToolOutputBytes + 1 }},
		{"unqualified identity", func(r *SandboxRunnerRequest) { r.Execution.QualificationHash = "" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := validSandboxRunnerRequest(t)
			test.change(&request)
			if err := request.Normalize(); err == nil {
				t.Fatal("unsafe or inconsistent request was accepted")
			}
		})
	}
}

func TestSandboxRunnerRejectsAnyRequestEnvironmentBeforeSerialization(t *testing.T) {
	request := validSandboxRunnerRequest(t)
	request.Environment = map[string]string{"TOKEN": "secret-value"}
	if err := request.Normalize(); err == nil {
		t.Fatal("credential-bearing request environment was accepted")
	}
	if raw, err := request.MarshalBounded(); err == nil || raw != nil {
		t.Fatal("invalid request environment was serialized")
	}
}

func TestSandboxRunnerRequestEnforcesAggregateWireBudget(t *testing.T) {
	request := validSandboxRunnerRequest(t)
	request.Profile.MaxArgCount = MaxToolArgCount
	request.Profile.MaxArgBytes = MaxToolArgBytes
	request.Argv = make([]string, 17)
	for index := range request.Argv {
		request.Argv[index] = strings.Repeat("x", MaxToolArgBytes)
	}
	if err := request.Profile.Normalize(); err != nil {
		t.Fatal(err)
	}
	profileHash := request.Profile.Hash()
	request.RunnerProfileHash = profileHash
	request.SandboxProfileHash = profileHash
	request.Execution.SandboxProfileHash = profileHash
	if err := request.Normalize(); err == nil {
		t.Fatal("aggregate command larger than wire budget was accepted")
	}
}

func TestSandboxRunnerResponseBindsIdentityAndOutputBudgets(t *testing.T) {
	request := validSandboxRunnerRequest(t)
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC().Truncate(time.Millisecond)
	response := SandboxRunnerResponse{
		SchemaVersion: SandboxRunnerProtocolVersion,
		RequestID:     request.RequestID,
		ExecutionHash: request.Execution.StableHash(),
		RequestHash:   request.StableHash(),
		Outcome:       SandboxRunnerOutcomeCompleted,
		Stdout:        []byte(" M README.md\n"),
		StartedAt:     start,
		FinishedAt:    start.Add(12 * time.Millisecond),
	}
	result, err := response.ToolResult(request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != ToolRunSucceeded || result.RunID != request.Execution.ToolRunID || result.RequestID != request.RequestID || result.ContentHash == "" {
		t.Fatalf("unexpected converted result: %+v", result)
	}
	raw, err := response.MarshalBoundedFor(request)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) > MaxSandboxRunnerResponseBytes {
		t.Fatalf("serialized response exceeds hard cap: %d", len(raw))
	}

	badResponses := []struct {
		name   string
		change func(*SandboxRunnerResponse)
	}{
		{"wrong request", func(r *SandboxRunnerResponse) { r.RequestID = "other-request" }},
		{"wrong attempt", func(r *SandboxRunnerResponse) { r.ExecutionHash = HashStrings("other attempt") }},
		{"wrong invocation", func(r *SandboxRunnerResponse) { r.RequestHash = HashStrings("other invocation") }},
		{"unknown outcome", func(r *SandboxRunnerResponse) { r.Outcome = "retry" }},
		{"failure without code", func(r *SandboxRunnerResponse) { r.Outcome = SandboxRunnerOutcomeFailed }},
		{"timed out with wrong code", func(r *SandboxRunnerResponse) {
			r.Outcome = SandboxRunnerOutcomeTimedOut
			r.FailureCode = ToolFailureExecution
		}},
		{"cancelled with wrong code", func(r *SandboxRunnerResponse) {
			r.Outcome = SandboxRunnerOutcomeCancelled
			r.FailureCode = ToolFailureExecution
		}},
		{"completed with failure", func(r *SandboxRunnerResponse) { r.FailureCode = ToolFailureExecution }},
		{"stdout over budget", func(r *SandboxRunnerResponse) { r.Stdout = bytes.Repeat([]byte("x"), request.Profile.MaxStdoutBytes+1) }},
		{"stderr over budget", func(r *SandboxRunnerResponse) { r.Stderr = bytes.Repeat([]byte("x"), request.Profile.MaxStderrBytes+1) }},
		{"invalid time", func(r *SandboxRunnerResponse) { r.FinishedAt = r.StartedAt.Add(-time.Millisecond) }},
		{"unbounded time", func(r *SandboxRunnerResponse) {
			r.FinishedAt = r.StartedAt.Add(time.Duration(request.Profile.TimeoutMS+31000) * time.Millisecond)
		}},
	}
	for _, test := range badResponses {
		t.Run(test.name, func(t *testing.T) {
			candidate := response
			candidate.Stdout = append([]byte(nil), response.Stdout...)
			candidate.Stderr = append([]byte(nil), response.Stderr...)
			test.change(&candidate)
			if err := candidate.NormalizeFor(request); err == nil {
				t.Fatal("invalid response was accepted")
			}
		})
	}
	changedInvocation := validSandboxRunnerRequest(t)
	if err := changedInvocation.Normalize(); err != nil {
		t.Fatal(err)
	}
	changedInvocation.Argv[1] = "--branch"
	if err := response.NormalizeFor(changedInvocation); err == nil {
		t.Fatal("response for a different invocation was accepted under the same execution identity")
	}
}

func TestSandboxRunnerResponseMapsFailureWithoutRawDiagnostics(t *testing.T) {
	request := validSandboxRunnerRequest(t)
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	start := time.Now().UTC()
	response := SandboxRunnerResponse{
		RequestID: request.RequestID, ExecutionHash: request.Execution.StableHash(), RequestHash: request.StableHash(),
		Outcome: SandboxRunnerOutcomeTimedOut, FailureCode: ToolFailureTimeout,
		StartedAt: start, FinishedAt: start.Add(time.Second),
	}
	result, err := response.ToolResult(request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != ToolRunFailed || result.Failure == nil || result.Failure.Code != ToolFailureTimeout || !result.Failure.Retryable || strings.Contains(result.Failure.Message, "secret") {
		t.Fatalf("failure was not safely mapped: %+v", result)
	}

	response.Outcome = SandboxRunnerOutcomeCompleted
	response.FailureCode = ""
	response.ExitCode = 2
	result, err = response.ToolResult(request)
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != ToolRunFailed || result.Failure == nil || result.Failure.Code != ToolFailureExecution {
		t.Fatalf("nonzero exit was not mapped to tool failure: %+v", result)
	}
}
