package fakedomains

import (
	"context"
	"testing"

	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

func TestDomainsExposeDistinctTypedCapabilities(t *testing.T) {
	fake, err := NewConnector("workspace-a")
	if err != nil {
		t.Fatal(err)
	}
	if len(fake.Capabilities()) != 8 {
		t.Fatalf("capability count = %d, want 8", len(fake.Capabilities()))
	}
	seenSchemas := map[string]bool{}
	for _, capability := range fake.Capabilities() {
		definition := capability.Definition()
		seenSchemas[definition.InputSchemaHash] = true
	}
	if len(seenSchemas) < 2 {
		t.Fatal("pipeline and support capabilities must not share one input schema")
	}
}

func TestEffectCapabilityRequiresAuthorityAndUsesReservedIdentity(t *testing.T) {
	fake, err := NewConnector("workspace-a")
	if err != nil {
		t.Fatal(err)
	}
	var effectCapability connector.Capability
	for _, candidate := range fake.Capabilities() {
		if candidate.Definition().Ref.Name == "data_pipeline.publish" {
			effectCapability = candidate
			break
		}
	}
	if effectCapability == nil {
		t.Fatal("publish capability not registered")
	}
	definition := effectCapability.Definition()
	request := contracts.OperationRequest{ID: "operation-1", RequestID: "request-1", IdempotencyKey: "idempotency-1", WorkspaceID: "workspace-a", Actor: contracts.ActorRef{ID: "actor-1", Kind: "operator", WorkspaceID: "workspace-a"}, Capability: definition.Ref, Target: contracts.ResourceRef{WorkspaceID: "workspace-a", System: contracts.SystemRef{WorkspaceID: "workspace-a", Type: "pipeline", ID: "warehouse", Version: "1"}, Kind: PipelineKind, ID: "pipeline-1", Version: "1", ContentHash: contracts.HashStrings("target")}, InputType: PipelineInput, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: contracts.HashStrings("input"), Profile: definition.Profile}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	plan, err := effectCapability.Plan(request)
	if err != nil {
		t.Fatal(err)
	}
	authorityCapability, ok := effectCapability.(interface {
		ExecuteWithAuthority(context.Context, contracts.OperationRequest, contracts.OperationPlan, contracts.EffectAuthority) (contracts.OperationResult, error)
	})
	if !ok {
		t.Fatal("effect capability lacks authority-aware execution")
	}
	if _, err := authorityCapability.ExecuteWithAuthority(context.Background(), request, plan, contracts.EffectAuthority{WorkspaceID: request.WorkspaceID, OperationID: request.ID, OperationOwnerID: request.Actor.ID, OperationFence: 1}); err == nil {
		t.Fatal("expected missing effect identity to fail closed")
	}
	effectDescriber, ok := effectCapability.(interface {
		DescribeEffect(contracts.OperationRequest) (contracts.ExternalEffect, error)
	})
	if !ok {
		t.Fatal("effect capability lacks effect descriptor")
	}
	effect, err := effectDescriber.DescribeEffect(request)
	if err != nil {
		t.Fatal(err)
	}
	result, err := authorityCapability.ExecuteWithAuthority(context.Background(), request, plan, contracts.EffectAuthority{WorkspaceID: request.WorkspaceID, OperationID: request.ID, OperationOwnerID: request.Actor.ID, OperationFence: 1, EffectID: effect.ID})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.ExternalEffects) != 1 || result.ExternalEffects[0].ID != effect.ID {
		t.Fatalf("reserved effect identity was not preserved: %+v", result.ExternalEffects)
	}
}
