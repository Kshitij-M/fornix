package connector_test

import (
	"context"
	"errors"
	"testing"

	"github.com/omaveda/fornix/internal/adapters/fakeincident"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

func TestTrustPolicyIsOrderIndependentAndPinsDefinitionHashes(t *testing.T) {
	adapter, err := fakeincident.NewConnector("workspace-trust")
	if err != nil {
		t.Fatal(err)
	}
	caps := adapter.Capabilities()
	definitions := make([]contracts.CapabilityDefinition, 0, len(caps))
	for _, capability := range caps {
		definitions = append(definitions, capability.Definition())
	}
	first, err := connector.NewTrustPolicy("workspace-trust", "revision-1", definitions)
	if err != nil {
		t.Fatal(err)
	}
	for left, right := 0, len(definitions)-1; left < right; left, right = left+1, right-1 {
		definitions[left], definitions[right] = definitions[right], definitions[left]
	}
	second, err := connector.NewTrustPolicy("workspace-trust", "revision-1", definitions)
	if err != nil {
		t.Fatal(err)
	}
	if first.StableHash() != second.StableHash() {
		t.Fatalf("trust hash depends on registration order: %s != %s", first.StableHash(), second.StableHash())
	}
	if err := first.Authorize(caps[0].Definition()); err != nil {
		t.Fatal(err)
	}
	altered := caps[0].Definition()
	altered.Ref.DefinitionHash = contracts.HashStrings("altered-definition")
	if first.Authorize(altered) == nil {
		t.Fatal("altered capability definition was trusted")
	}
	foreign := caps[0].Definition()
	foreign.WorkspaceID = "workspace-other"
	foreign.Ref.WorkspaceID = foreign.WorkspaceID
	foreign.Ref.Connector.WorkspaceID = foreign.WorkspaceID
	if first.Authorize(foreign) == nil {
		t.Fatal("cross-workspace capability was trusted")
	}
}

func TestRegistryTrustPolicyRejectsNewUntrustedRegistration(t *testing.T) {
	adapter, err := fakeincident.NewConnector("workspace-trust-registry")
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if err := registry.TrustWorkspace("workspace-trust-registry", "revision-1"); err != nil {
		t.Fatal(err)
	}
	registry.RequireTrustPolicy(true)
	capability, ok := registry.LookupIdentity("workspace-trust-registry", fakeincident.ConnectorName, fakeincident.ConnectorVersion, "incident.read", "1")
	if !ok {
		t.Fatal("trusted capability was not discoverable")
	}
	request := trustRequest(capability.Definition())
	if _, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{}); err != nil {
		t.Fatalf("trusted capability was rejected: %v", err)
	}
	// A second version can be registered for discovery, but it must not become
	// executable until the trust snapshot is explicitly replaced.
	second, err := fakeincident.NewConnector("workspace-trust-registry-2")
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(second); err != nil {
		t.Fatal(err)
	}
	foreignRequest := trustRequest(second.Capabilities()[0].Definition())
	if _, err := registry.Admit(context.Background(), foreignRequest, connector.AdmissionOptions{}); !errors.Is(err, connector.ErrTrustPolicyMissing) {
		t.Fatalf("untrusted workspace capability error=%v", err)
	}
}

func trustRequest(definition contracts.CapabilityDefinition) contracts.OperationRequest {
	hash := contracts.HashStrings("trust-input")
	return contracts.OperationRequest{
		ID: "trust-operation", RequestID: "trust-request", IdempotencyKey: "trust-key", WorkspaceID: definition.WorkspaceID,
		Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: definition.WorkspaceID}, Capability: definition.Ref,
		Target:    contracts.ResourceRef{WorkspaceID: definition.WorkspaceID, System: contracts.SystemRef{WorkspaceID: definition.WorkspaceID, Type: "monitoring", ID: "monitor", Version: "1"}, Kind: definition.ResourceKinds[0], ID: "resource-1", Version: "1"},
		InputType: "incident.workflow.step", InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: hash, Profile: definition.Profile,
	}
}
