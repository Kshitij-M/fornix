package contracts

import (
	"encoding/json"
	"strings"
	"testing"
)

func validSandboxExecutionIdentity() SandboxExecutionIdentity {
	return SandboxExecutionIdentity{
		WorkspaceID: "workspace-a", ToolRunID: "tool-run-a", ToolAttempt: 1,
		Backend: SandboxBackendOCI, OperationID: "operation-a", OperationOwnerID: "worker-a", OperationFence: 2,
		AttemptID: "attempt-a", EffectID: "effect-a",
		ToolRequestHash: HashStrings("tool-request"), OperationRequestHash: HashStrings("operation-request"),
		EffectReservationHash: HashStrings("reservation"), ToolDefinitionHash: HashStrings("definition"),
		SandboxProfileHash: HashStrings("profile"), QualificationHash: HashStrings("sandbox qualification"),
	}
}

func TestSandboxExecutionIdentityIsOpaqueStableAndFenceBound(t *testing.T) {
	identity := validSandboxExecutionIdentity()
	if err := identity.Normalize(); err != nil {
		t.Fatal(err)
	}
	firstHash, firstName := identity.StableHash(), identity.RuntimeName()
	if firstHash == "" || len(firstName) != len("fornix-")+40 || !strings.HasPrefix(firstName, "fornix-") {
		t.Fatalf("invalid opaque runtime identity: hash=%q name=%q", firstHash, firstName)
	}
	if firstName != SandboxRuntimeNameFromHash(firstHash) {
		t.Fatalf("identity name=%q differs from canonical hash name=%q", firstName, SandboxRuntimeNameFromHash(firstHash))
	}
	serialized, err := json.Marshal(identity)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"/Users/", "password", "credential", "argv", "environment"} {
		if strings.Contains(strings.ToLower(string(serialized)), strings.ToLower(forbidden)) {
			t.Fatalf("execution identity exposed %q: %s", forbidden, serialized)
		}
	}

	changed := identity
	changed.OperationFence++
	if changed.StableHash() == firstHash || changed.RuntimeName() == firstName {
		t.Fatal("fencing change did not change the deterministic runtime identity")
	}
	changed = identity
	changed.WorkspaceID = "workspace-b"
	if changed.StableHash() == firstHash {
		t.Fatal("workspace change did not change the deterministic runtime identity")
	}
	changed = identity
	changed.SandboxProfileHash = HashStrings("different-profile")
	if changed.StableHash() == firstHash {
		t.Fatal("profile change did not change the deterministic runtime identity")
	}
	for name, mutate := range map[string]func(*SandboxExecutionIdentity){
		"tool attempt":           func(i *SandboxExecutionIdentity) { i.ToolAttempt++ },
		"attempt ID":             func(i *SandboxExecutionIdentity) { i.AttemptID = "attempt-b" },
		"effect ID":              func(i *SandboxExecutionIdentity) { i.EffectID = "effect-b" },
		"tool request hash":      func(i *SandboxExecutionIdentity) { i.ToolRequestHash = HashStrings("tool-request-b") },
		"operation request hash": func(i *SandboxExecutionIdentity) { i.OperationRequestHash = HashStrings("operation-request-b") },
		"definition hash":        func(i *SandboxExecutionIdentity) { i.ToolDefinitionHash = HashStrings("definition-b") },
		"qualification hash":     func(i *SandboxExecutionIdentity) { i.QualificationHash = HashStrings("qualification-b") },
	} {
		t.Run(name, func(t *testing.T) {
			candidate := identity
			mutate(&candidate)
			if candidate.StableHash() == firstHash {
				t.Fatal("identity input did not change the runtime identity")
			}
		})
	}
}

func TestSandboxRuntimeNameFromHashRejectsInvalidHash(t *testing.T) {
	for _, hash := range []string{"", "not-a-hash", strings.Repeat("a", 63), strings.Repeat("g", 64)} {
		if name := SandboxRuntimeNameFromHash(hash); name != "" {
			t.Errorf("invalid identity hash %q produced runtime name %q", hash, name)
		}
	}
}

func TestSandboxExecutionIdentityRejectsIncompleteOrCrossWorkspaceFacts(t *testing.T) {
	tests := []struct {
		name   string
		change func(*SandboxExecutionIdentity)
	}{
		{name: "missing effect", change: func(i *SandboxExecutionIdentity) { i.EffectID = "" }},
		{name: "missing attempt", change: func(i *SandboxExecutionIdentity) { i.AttemptID = "" }},
		{name: "zero fence", change: func(i *SandboxExecutionIdentity) { i.OperationFence = 0 }},
		{name: "zero tool attempt", change: func(i *SandboxExecutionIdentity) { i.ToolAttempt = 0 }},
		{name: "invalid tool request hash", change: func(i *SandboxExecutionIdentity) { i.ToolRequestHash = "raw request" }},
		{name: "task owner without fence", change: func(i *SandboxExecutionIdentity) { i.TaskOwnerID = "task-worker" }},
		{name: "agent run without fence", change: func(i *SandboxExecutionIdentity) { i.AgentRunID = "agent-run-a" }},
		{name: "unknown backend", change: func(i *SandboxExecutionIdentity) { i.Backend = "host-root" }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			identity := validSandboxExecutionIdentity()
			test.change(&identity)
			if err := identity.Normalize(); err == nil {
				t.Fatal("invalid sandbox identity was accepted")
			}
		})
	}
	agentIdentity := validSandboxExecutionIdentity()
	agentIdentity.AgentRunID, agentIdentity.AgentRunOwnerID, agentIdentity.AgentRunFence = "agent-run-a", "agent-worker-a", 3
	if err := agentIdentity.Normalize(); err != nil {
		t.Fatalf("complete agent-run fence rejected: %v", err)
	}
}

func TestSandboxAttemptObservationFailsClosedOnIdentityAndResultMismatch(t *testing.T) {
	identity := validSandboxExecutionIdentity()
	result := ToolResult{Status: ToolRunSucceeded, Stdout: "ok"}
	resultHash := result.Hash()
	observation := SandboxAttemptObservation{IdentityHash: identity.StableHash(), State: SandboxAttemptCompleted, RuntimeID: "opaque-runtime-id", ResultHash: resultHash, Result: &result}
	if err := observation.Normalize(identity); err != nil {
		t.Fatal(err)
	}

	observation.IdentityHash = HashStrings("wrong-identity")
	if err := observation.Normalize(identity); err == nil {
		t.Fatal("cross-identity observation was accepted")
	}
	observation = SandboxAttemptObservation{IdentityHash: identity.StableHash(), State: SandboxAttemptCompleted, ResultHash: HashStrings("wrong-result"), Result: &result}
	if err := observation.Normalize(identity); err == nil {
		t.Fatal("result hash mismatch was accepted")
	}
	observation = SandboxAttemptObservation{IdentityHash: identity.StableHash(), State: SandboxAttemptUnknown, Result: &result}
	if err := observation.Normalize(identity); err == nil {
		t.Fatal("unknown runtime state was allowed to claim a result")
	}
	oversized := ToolResult{Status: ToolRunSucceeded, Stdout: strings.Repeat("x", MaxToolOutputBytes+1)}
	observation = SandboxAttemptObservation{IdentityHash: identity.StableHash(), State: SandboxAttemptCompleted, ResultHash: oversized.Hash(), Result: &oversized}
	if err := observation.Normalize(identity); err == nil {
		t.Fatal("oversized reconciled output was accepted")
	}
}
