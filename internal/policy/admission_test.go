package policy

import (
	"strings"
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func admissionInput(t *testing.T, effect contracts.EffectClass) contracts.AdmissionInput {
	t.Helper()
	workspace := "workspace-a"
	connector := contracts.ConnectorRef{WorkspaceID: workspace, Name: "system", Version: "1"}
	capability := contracts.CapabilityDefinition{
		WorkspaceID: workspace,
		Ref:         contracts.CapabilityRef{WorkspaceID: workspace, Connector: connector, Name: "read", Version: "1"},
		Description: "bounded test capability", InputSchemaVersion: 1, InputSchemaHash: strings.Repeat("a", 64),
		OutputSchemaVersion: 1, OutputSchemaHash: strings.Repeat("b", 64), Effect: effect,
		Profile: contracts.DefaultExecutionProfile(), RetryPolicy: contracts.CapabilityRetryPolicy{MaxAttempts: 1, BackoffMS: 1, MaxBackoffMS: 1, Jitter: "none"},
		ResourceKinds: []string{"record"}, MaxRows: 10, RateLimitPerMinute: 10,
		Enabled: true, SupportsIdempotency: true, SupportsVerification: true,
	}
	if err := capability.Normalize(); err != nil {
		t.Fatal(err)
	}
	policy := contracts.AdmissionPolicy{
		WorkspaceID: workspace, PolicyID: "safe", Version: "1",
		AllowedConnectors: []contracts.ConnectorRef{connector}, AllowedResourceKinds: []string{"record"}, AllowedActorIDs: []string{"actor-a"},
		RequireEvidence: false, RequireTaskFence: true, MaxCostMicros: 100, MaxOperationsPerWindow: 4, MaxCostPerWindowMicros: 200,
	}
	if err := policy.Normalize(); err != nil {
		t.Fatal(err)
	}
	target := contracts.ResourceRef{WorkspaceID: workspace, System: contracts.SystemRef{WorkspaceID: workspace, Type: "service", ID: "service-a", Version: "1"}, Kind: "record", ID: "record-a", Version: "1"}
	return contracts.AdmissionInput{
		WorkspaceID: workspace, OperationID: "operation-a", OperationHash: strings.Repeat("c", 64), RequestID: "request-a", IdempotencyKey: "admit-a",
		Actor: contracts.ActorRef{ID: "actor-a", Kind: "human", WorkspaceID: workspace}, Capability: capability, Target: target, Policy: policy,
		ConnectorAvailable: true, ResourceAllowed: true, EvidenceSatisfied: true, TaskFenceValid: true,
		RequestedCostMicros: 10,
	}
}

func TestAdmissionReadIsDeterministicAndAutomatic(t *testing.T) {
	firstInput := admissionInput(t, contracts.EffectClassReadOnly)
	first, err := Evaluate(firstInput)
	if err != nil {
		t.Fatal(err)
	}
	secondInput := firstInput
	secondInput.RequestID = "different-transport-request"
	secondInput.ConnectorAvailable = true
	secondInput.ResourceAllowed = true
	secondInput.EvidenceSatisfied = true
	secondInput.CredentialStates = []contracts.CredentialState{{Reference: "credential-a", Status: contracts.CredentialStateActive}}
	secondInput.TaskOwnerID, secondInput.TaskFence, secondInput.TaskFenceValid = "takeover-worker", 9, true
	secondInput.QuotaOperations, secondInput.QuotaCostMicros = 3, 40
	if firstInput.StableHash() != secondInput.StableHash() {
		t.Fatal("runtime facts changed the logical admission hash")
	}
	second, err := Evaluate(secondInput)
	if err != nil {
		t.Fatal(err)
	}
	if first.Decision.Status != contracts.AdmissionAllowed || first.Decision.ReasonCode != contracts.AdmissionReasonApproved {
		t.Fatalf("read admission = %+v", first.Decision)
	}
	if first.Decision.DecisionHash != second.Decision.DecisionHash || first.Decision.InputHash != second.Decision.InputHash {
		t.Fatalf("decision is not deterministic: first=%+v second=%+v", first.Decision, second.Decision)
	}
	if first.Approval != nil {
		t.Fatal("read admission unexpectedly requested approval")
	}
}

func TestAdmissionWritesRequireExactApproval(t *testing.T) {
	evaluation, err := Evaluate(admissionInput(t, contracts.EffectClassIrreversibleWrite))
	if err != nil {
		t.Fatal(err)
	}
	if evaluation.Decision.Status != contracts.AdmissionAwaitingApproval || evaluation.Approval == nil {
		t.Fatalf("write admission = %+v approval=%+v", evaluation.Decision, evaluation.Approval)
	}
	if evaluation.Approval.DecisionHash != evaluation.Decision.DecisionHash || evaluation.Approval.OperationHash == "" || evaluation.Approval.InputHash == "" {
		t.Fatalf("approval is not bound to decision: %+v", evaluation.Approval)
	}
	if second, err := Evaluate(admissionInput(t, contracts.EffectClassIrreversibleWrite)); err != nil || second.Decision.DecisionHash != evaluation.Decision.DecisionHash {
		t.Fatalf("approval decision hash is not stable: second=%+v err=%v", second.Decision, err)
	}
}

func TestAdmissionFailsClosedForMissingFactsAndUnsafePolicy(t *testing.T) {
	tests := map[string]func(*contracts.AdmissionInput){
		"connector": func(in *contracts.AdmissionInput) { in.ConnectorAvailable = false },
		"resource":  func(in *contracts.AdmissionInput) { in.ResourceAllowed = false },
		"evidence": func(in *contracts.AdmissionInput) {
			in.Policy.RequireEvidence = true
			in.Policy.PolicyHash = ""
			in.EvidenceSatisfied = false
		},
		"fence": func(in *contracts.AdmissionInput) { in.TaskBound = true; in.TaskFenceValid = false },
		"quota": func(in *contracts.AdmissionInput) { in.QuotaOperations = in.Policy.MaxOperationsPerWindow },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			input := admissionInput(t, contracts.EffectClassReadOnly)
			mutate(&input)
			result, err := Evaluate(input)
			if err != nil {
				t.Fatal(err)
			}
			if result.Decision.Status != contracts.AdmissionDenied {
				t.Fatalf("decision = %+v", result.Decision)
			}
		})
	}
	input := admissionInput(t, contracts.EffectClassIrreversibleWrite)
	input.Policy.EffectRules = []contracts.AdmissionEffectRule{{Effect: contracts.EffectClassIrreversibleWrite, Mode: contracts.PolicyApprovalAutomatic}}
	if _, err := Evaluate(input); err == nil {
		t.Fatal("unsafe automatic irreversible-write policy was accepted")
	}
}

func TestAdmissionRejectsCredentialAndWorkspaceMismatches(t *testing.T) {
	input := admissionInput(t, contracts.EffectClassReadOnly)
	input.Capability.RequiredCredentialRefs = []string{"credential-a"}
	input.Capability.Ref.DefinitionHash = ""
	input.CredentialStates = []contracts.CredentialState{{Reference: "credential-a", Status: contracts.CredentialStateRevoked}}
	result, err := Evaluate(input)
	if err != nil {
		t.Fatal(err)
	}
	if result.Decision.ReasonCode != contracts.AdmissionReasonCredentialInvalid {
		t.Fatalf("credential decision = %+v", result.Decision)
	}
	input = admissionInput(t, contracts.EffectClassReadOnly)
	input.Target.WorkspaceID = "workspace-b"
	if _, err := Evaluate(input); err == nil {
		t.Fatal("cross-workspace target unexpectedly normalized")
	}
}
