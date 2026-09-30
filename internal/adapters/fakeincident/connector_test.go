package fakeincident

import (
	"context"
	"errors"
	"testing"

	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

func TestConnectorIsDeterministicAndApprovalGated(t *testing.T) {
	const workspace = "workspace-incident"
	adapter, err := NewConnector(workspace)
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	var remediation connector.Capability
	var read connector.Capability
	for _, candidate := range adapter.Capabilities() {
		switch candidate.Definition().Ref.Name {
		case "incident.remediate":
			remediation = candidate
		case "incident.read":
			read = candidate
		}
	}
	if remediation == nil || read == nil {
		t.Fatal("expected read and remediation capabilities")
	}
	request := testRequest(remediation.Definition(), ResourceKind)
	if _, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{}); !errors.Is(err, connector.ErrApprovalRequired) {
		t.Fatalf("remediation admission error=%v, want approval required", err)
	}
	admitted, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{ApprovalGranted: true})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := admitted.Capability.Plan(admitted.Request)
	if err != nil {
		t.Fatal(err)
	}
	one, err := admitted.Capability.Execute(context.Background(), admitted.Request, plan)
	if err != nil {
		t.Fatal(err)
	}
	two, err := admitted.Capability.Execute(context.Background(), admitted.Request, plan)
	if err != nil {
		t.Fatal(err)
	}
	if one.StableHash() != two.StableHash() || len(one.ExternalEffects) != 1 || !one.ExternalEffects[0].ProviderIdempotency {
		t.Fatalf("fake remediation is not deterministic/idempotent: one=%+v two=%+v", one, two)
	}
	cross := request
	cross.Target.WorkspaceID = "workspace-other"
	if _, err := registry.Admit(context.Background(), cross, connector.AdmissionOptions{ApprovalGranted: true}); err == nil {
		t.Fatal("cross-workspace incident request was admitted")
	}
}

func TestEffectfulExecutionUsesReservedAuthorityEffectID(t *testing.T) {
	adapter, err := NewConnector("workspace-authority-effect")
	if err != nil {
		t.Fatal(err)
	}
	var capability connector.Capability
	for _, candidate := range adapter.Capabilities() {
		if candidate.Definition().Ref.Name == "incident.remediate" {
			capability = candidate
			break
		}
	}
	if capability == nil {
		t.Fatal("remediation capability not found")
	}
	request := testRequest(capability.Definition(), ResourceKind)
	plan, err := capability.Plan(request)
	if err != nil {
		t.Fatal(err)
	}
	result, err := capability.(connector.AuthorityAwareCapability).ExecuteWithAuthority(context.Background(), request, plan, contracts.EffectAuthority{
		WorkspaceID: request.WorkspaceID, OperationID: request.ID, OperationOwnerID: "worker-1", OperationFence: 1, EffectID: "reserved-effect-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.ExternalEffects) != 1 || result.ExternalEffects[0].ID != "reserved-effect-1" || result.Steps[0].ExternalEffect.ID != "reserved-effect-1" {
		t.Fatalf("effect identity was not reserved by authority: %+v", result.ExternalEffects)
	}
}

func testRequest(definition contracts.CapabilityDefinition, kind string) contracts.OperationRequest {
	return contracts.OperationRequest{
		ID: "incident-operation", RequestID: "incident-request", IdempotencyKey: "incident-idempotency",
		WorkspaceID: definition.WorkspaceID, Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: definition.WorkspaceID},
		Capability: definition.Ref, Target: contracts.ResourceRef{WorkspaceID: definition.WorkspaceID, System: contracts.SystemRef{WorkspaceID: definition.WorkspaceID, Type: "monitoring", ID: "monitor", Version: "1"}, Kind: kind, ID: "incident-1", Version: "1"},
		InputType: InputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash,
		InputHash: contracts.HashStrings("incident-input"), Profile: definition.Profile,
	}
}
