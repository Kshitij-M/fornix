package contracts

import (
	"reflect"
	"strings"
	"testing"
)

func TestToolRequestHashExcludesApprovalAndTransportIdentity(t *testing.T) {
	request := ToolRequest{
		WorkspaceID: "w1", RequestID: "request-a", IdempotencyKey: "run-a",
		ToolID: "fornix.echo", Argv: []string{"/bin/echo", "hello"},
		Mode: ToolModeInteractive, ApprovalID: "approval-a", CausationID: "cause-a",
		Budget: DefaultSandboxProfile(),
	}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	first, err := request.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	request.RequestID = "request-b"
	request.IdempotencyKey = "run-b"
	request.Mode = ToolModePreApproved
	request.ApprovalID = "approval-b"
	request.CausationID = "cause-b"
	second, err := request.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("authorization/transport fields changed request hash: %s != %s", first, second)
	}
}

func TestToolRequestHashIgnoresDeliveryFence(t *testing.T) {
	request := ToolRequest{WorkspaceID: "w1", RequestID: "request-a", IdempotencyKey: "run-a", ToolID: "fornix.echo", Argv: []string{"/bin/echo", "hello"}, Budget: DefaultSandboxProfile(), AgentRun: &EntityRef{ID: "agent-1", Kind: "agent_run", WorkspaceID: "w1"}, AgentRunOwnerID: "worker-a", AgentRunFence: 1}
	first, err := request.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	request.AgentRunOwnerID, request.AgentRunFence = "worker-b", 2
	second, err := request.RequestHash()
	if err != nil || first != second {
		t.Fatalf("delivery fence changed logical tool hash: first=%s second=%s err=%v", first, second, err)
	}
}

func TestToolRequestDefinitionIdentityPreservesLegacyRequestHash(t *testing.T) {
	request := ToolRequest{WorkspaceID: "w1", IdempotencyKey: "identity-a", ToolID: "fornix.echo", Argv: []string{"/bin/echo", "hello"}, Budget: DefaultSandboxProfile()}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	before, err := request.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	request.ToolDefinitionHash = strings.Repeat("a", 64)
	request.SandboxProfileHash = strings.Repeat("b", 64)
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	after, err := request.RequestHash()
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("execution identity changed the legacy logical request hash: %s != %s", before, after)
	}
	const legacyRequestHash = "a16c71495df39f9fd4fc86bcdce22be0e4e8928d98eae27880684aafac6a28e0"
	if before != legacyRequestHash {
		t.Fatalf("logical request hash changed from the legacy schema: got %s want %s", before, legacyRequestHash)
	}
	definition := ToolDefinition{ID: "fornix.echo", Name: "echo", Version: "1", Capability: "process.echo", Executable: "/bin/echo", Enabled: true, Sandbox: DefaultSandboxProfile()}
	first, err := definition.Hash()
	if err != nil {
		t.Fatal(err)
	}
	definition.Version = "2"
	second, err := definition.Hash()
	if err != nil {
		t.Fatal(err)
	}
	if first == second {
		t.Fatal("tool definition hash did not change with the registered version")
	}
}

func TestToolRequestRedactedEvidenceDoesNotContainEnvironmentValue(t *testing.T) {
	request := ToolRequest{WorkspaceID: "w1", IdempotencyKey: "run-a", ToolID: "fornix.echo", Argv: []string{"/bin/echo", "x"}, Environment: map[string]string{"TOKEN": "secret-value"}, Budget: DefaultSandboxProfile()}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	evidence, err := request.RedactedEvidence()
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(evidence), "secret-value") || !strings.Contains(string(evidence), "[REDACTED]") {
		t.Fatalf("environment secret was not redacted: %s", evidence)
	}
}

func TestToolRequestEnforcesEffectiveArgumentAndEnvironmentBudgets(t *testing.T) {
	base := ToolRequest{WorkspaceID: "w1", IdempotencyKey: "run-a", ToolID: "fornix.echo", Argv: []string{"/bin/echo", "x"}, Budget: DefaultSandboxProfile()}
	tooManyArgs := base
	tooManyArgs.Budget.MaxArgCount = 1
	if err := tooManyArgs.Normalize(); err == nil {
		t.Fatal("request exceeding its own argument-count budget was accepted")
	}
	tooLargeArg := base
	tooLargeArg.Budget.MaxArgBytes = 4
	if err := tooLargeArg.Normalize(); err == nil {
		t.Fatal("request exceeding its own argument-byte budget was accepted")
	}
	tooLargeEnvironment := base
	tooLargeEnvironment.Environment = map[string]string{"TOKEN": "123"}
	tooLargeEnvironment.Budget.MaxEnvBytes = 8 // TOKEN=123 is 9 bytes.
	if err := tooLargeEnvironment.Normalize(); err == nil {
		t.Fatal("environment byte budget did not include the assignment separator")
	}
}

func TestSandboxProfileRejectsUnknownBackendAndCanonicalizesRequirements(t *testing.T) {
	profile := DefaultSandboxProfile()
	profile.RequiredCapabilities = []SandboxCapability{
		SandboxCapabilityOutputByteLimit,
		SandboxCapabilityStructuredArgv,
		SandboxCapabilityOutputByteLimit,
	}
	if err := profile.Normalize(); err != nil {
		t.Fatal(err)
	}
	want := []SandboxCapability{SandboxCapabilityOutputByteLimit, SandboxCapabilityStructuredArgv}
	if !reflect.DeepEqual(profile.RequiredCapabilities, want) {
		t.Fatalf("required capabilities are not canonical: got %v want %v", profile.RequiredCapabilities, want)
	}
	profile.Backend = "mystery-container"
	if err := profile.Normalize(); err == nil {
		t.Fatal("unknown backend was accepted")
	}
}

func TestSandboxProfileRequiresPinnedImageAndHardLimitsForContainerTiers(t *testing.T) {
	profile := DefaultSandboxProfile()
	profile.Backend = string(SandboxBackendOCI)
	if err := profile.Normalize(); err == nil {
		t.Fatal("OCI profile without immutable image and hard resource limits was accepted")
	}
	profile.ImageDigest = "sha256:" + strings.Repeat("a", 64)
	profile.ImagePlatform = SandboxImagePlatform{OS: "linux", Architecture: "amd64"}
	profile.ReadOnlyRootFS = true
	profile.ReadOnlyWorkdir = true
	profile.CPUQuotaMilli = 1000
	profile.MemoryBytes = 512 << 20
	profile.PIDsLimit = 64
	profile.ScratchBytes = 1 << 30
	if err := profile.Normalize(); err != nil {
		t.Fatalf("complete pinned OCI profile was rejected: %v", err)
	}
	required, err := profile.RequiredEnforcements()
	if err != nil {
		t.Fatal(err)
	}
	for _, capability := range []SandboxCapability{
		SandboxCapabilityProcessTreeTermination,
		SandboxCapabilityReadOnlyRootFS,
		SandboxCapabilityReadOnlyMount,
		SandboxCapabilityFilesystemIsolation,
		SandboxCapabilityNetworkIsolation,
	} {
		if !hasSandboxCapability(required, capability) {
			t.Errorf("container profile does not require %q: %v", capability, required)
		}
	}
	if hasSandboxCapability(required, SandboxCapabilityProcessGroupTermination) {
		t.Fatalf("container profile incorrectly requires the local-process group guarantee: %v", required)
	}
	profile.ImageDigest = "example:latest"
	if err := profile.Normalize(); err == nil {
		t.Fatal("mutable image tag was accepted")
	}
}

func hasSandboxCapability(capabilities []SandboxCapability, expected SandboxCapability) bool {
	for _, capability := range capabilities {
		if capability == expected {
			return true
		}
	}
	return false
}

func TestToolDefinitionRejectsShellExecutable(t *testing.T) {
	definition := ToolDefinition{ID: "shell", Name: "shell", Version: "1", Capability: "process", Executable: "/bin/sh", Enabled: true, Sandbox: DefaultSandboxProfile()}
	if err := definition.Normalize(); err == nil {
		t.Fatal("expected shell executable to be rejected")
	}
}

func TestToolDefinitionRejectsExecutableAsPathArgument(t *testing.T) {
	definition := ToolDefinition{ID: "tool", Name: "tool", Version: "1", Capability: "inspect", Executable: "/usr/bin/tool", PathArgvIndexes: []int{0}, Enabled: true, Sandbox: DefaultSandboxProfile()}
	if err := definition.Normalize(); err == nil {
		t.Fatal("path argv index zero must not designate the registered executable")
	}
}

func TestToolDefinitionModelMetadataAndEnvironmentCatalogAreBounded(t *testing.T) {
	base := ToolDefinition{ID: "tool", Name: "tool", Version: "1", Capability: "inspect", Executable: "/usr/bin/tool", Enabled: true, Sandbox: DefaultSandboxProfile()}
	tooLongDescription := base
	tooLongDescription.Description = strings.Repeat("d", MaxToolDescriptionBytes+1)
	if err := tooLongDescription.Normalize(); err == nil {
		t.Fatal("oversized model-facing description was accepted")
	}
	tooLongKey := base
	tooLongKey.AllowedEnvKeys = []string{strings.Repeat("A", MaxToolEnvKeyLength+1)}
	if err := tooLongKey.Normalize(); err == nil {
		t.Fatal("oversized environment key was accepted into the model schema")
	}
	tooManyKeys := base
	tooManyKeys.AllowedEnvKeys = make([]string, MaxToolEnvCount+1)
	for i := range tooManyKeys.AllowedEnvKeys {
		tooManyKeys.AllowedEnvKeys[i] = "KEY_" + strings.Repeat("A", i+1)
	}
	if err := tooManyKeys.Normalize(); err == nil {
		t.Fatal("oversized environment key catalog was accepted")
	}
}

func TestToolRequestAgentRunFenceRequiresCompleteScopedLease(t *testing.T) {
	base := ToolRequest{WorkspaceID: "w1", IdempotencyKey: "run-a", ToolID: "fornix.echo", Argv: []string{"/bin/echo", "x"}, Budget: DefaultSandboxProfile()}
	base.AgentRun = &EntityRef{ID: "agent-1", Kind: "agent_run", WorkspaceID: "w1"}
	if err := base.Normalize(); err == nil {
		t.Fatal("expected incomplete agent-run fence to fail closed")
	}
	base.AgentRunOwnerID = "worker-a"
	base.AgentRunFence = 7
	if err := base.Normalize(); err != nil {
		t.Fatalf("valid agent-run fence rejected: %v", err)
	}
	wrongWorkspace := base
	wrongWorkspace.AgentRun = &EntityRef{ID: "agent-1", Kind: "agent_run", WorkspaceID: "w2"}
	if err := wrongWorkspace.Normalize(); err == nil {
		t.Fatal("expected cross-workspace agent-run reference to fail closed")
	}
}
