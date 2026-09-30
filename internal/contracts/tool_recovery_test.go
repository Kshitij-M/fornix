package contracts

import "testing"

func TestToolRecoveryFinalizeRequestRequiresExactCompletedAttempt(t *testing.T) {
	identity := SandboxExecutionIdentity{
		SchemaVersion: SandboxExecutionIdentitySchemaVersion, WorkspaceID: "w-recovery", ToolRunID: "toolrun-1", ToolAttempt: 1,
		Backend: SandboxBackendOCI, OperationID: "operation-1", OperationOwnerID: "operation-owner", OperationFence: 2,
		AttemptID: "attempt-1", EffectID: "effect-1", ToolRequestHash: HashStrings("tool request"),
		OperationRequestHash: HashStrings("operation request"), EffectReservationHash: HashStrings("effect reservation"),
		ToolDefinitionHash: HashStrings("definition"), SandboxProfileHash: HashStrings("profile"), QualificationHash: HashStrings("sandbox qualification"),
	}
	result := ToolResult{RequestID: "tool-request-1", RunID: identity.ToolRunID, ToolID: "inspect", Status: ToolRunSucceeded, Stdout: "read-only result"}
	request := ToolRecoveryFinalizeRequest{
		WorkspaceID: identity.WorkspaceID, ToolRunID: identity.ToolRunID, OwnerID: "recovery-owner", Fence: 3,
		ExpectedEffectVer: 4, ExpectedLinkVersion: 2, RequestID: "recovery-request", ToolRequestID: result.RequestID,
		IdempotencyKey: "recovery-1", Actor: ActorRef{ID: "operator-1", Kind: "user", WorkspaceID: identity.WorkspaceID},
		Identity: identity, Observation: SandboxAttemptObservation{IdentityHash: identity.StableHash(), State: SandboxAttemptCompleted, ResultHash: result.Hash(), Result: &result},
	}
	if err := request.Normalize(); err != nil {
		t.Fatalf("normalize completed recovery request: %v", err)
	}

	wrongScope := request
	wrongScope.WorkspaceID = "other-workspace"
	if err := wrongScope.Normalize(); err == nil {
		t.Fatal("cross-workspace recovery identity was accepted")
	}

	unknown := request
	unknown.Observation = SandboxAttemptObservation{IdentityHash: identity.StableHash(), State: SandboxAttemptUnknown}
	if err := unknown.Normalize(); err == nil {
		t.Fatal("unknown attempt state authorized tool finalization")
	}

	wrongResult := request
	wrongResult.Observation.ResultHash = HashStrings("different result")
	if err := wrongResult.Normalize(); err == nil {
		t.Fatal("result hash mismatch was accepted")
	}
}
