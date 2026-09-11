package repository_test

import (
	"context"
	"errors"
	"testing"

	"github.com/omaveda/fornix/internal/adapters/repository"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

func TestRepositoryConnectorFailsClosedWithoutInspector(t *testing.T) {
	adapter, err := repository.NewConnector("workspace-a", "1", nil)
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	request := requestFor(t, adapter.Capabilities()[0].Definition())
	if _, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{}); !errors.Is(err, connector.ErrConnectorUnavailable) {
		t.Fatalf("admission error = %v, want connector unavailable", err)
	}
}

func TestRepositoryConnectorValidatesTypedInputAndEvidence(t *testing.T) {
	adapter, err := repository.NewConnector("workspace-a", "1", func(_ context.Context, request contracts.OperationRequest) (repository.Inspection, error) {
		return repository.Inspection{
			OutputHash: repository.HashBytes([]byte("output")),
			Evidence:   []contracts.OperationEvidenceRef{{WorkspaceID: request.WorkspaceID, SourceReference: "repo:" + request.Target.ID, EvidenceHash: repository.HashBytes([]byte("evidence")), Role: "snapshot"}},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	definition := adapter.Capabilities()[0].Definition()
	request := requestFor(t, definition)
	request.InputType = "repository.unknown"
	if _, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{}); err == nil {
		t.Fatal("invalid repository input type was admitted")
	}

	request = requestFor(t, definition)
	capability, ok := registry.Lookup(request.Capability)
	if !ok {
		t.Fatal("registered repository capability was not found")
	}
	plan, err := capability.Plan(request)
	if err != nil {
		t.Fatal(err)
	}
	if plan.StableHash() == "" || len(plan.Steps) != 1 {
		t.Fatalf("unexpected repository plan: %+v", plan)
	}
	outcome, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), request, connector.AdmissionOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Result.StableHash() == "" || len(outcome.Result.Evidence) != 1 {
		t.Fatalf("unexpected repository result: %+v", outcome.Result)
	}
}

func TestRepositoryConnectorRejectsCrossWorkspaceEvidence(t *testing.T) {
	adapter, err := repository.NewConnector("workspace-a", "1", func(_ context.Context, _ contracts.OperationRequest) (repository.Inspection, error) {
		return repository.Inspection{
			OutputHash: repository.HashBytes([]byte("output")),
			Evidence:   []contracts.OperationEvidenceRef{{WorkspaceID: "workspace-b", SourceReference: "repo:other", EvidenceHash: repository.HashBytes([]byte("evidence"))}},
		}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	request := requestFor(t, adapter.Capabilities()[0].Definition())
	if _, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), request, connector.AdmissionOptions{}); err == nil {
		t.Fatal("cross-workspace evidence unexpectedly succeeded")
	}
}

func requestFor(t *testing.T, definition contracts.CapabilityDefinition) contracts.OperationRequest {
	t.Helper()
	return contracts.OperationRequest{
		ID: "operation-1", RequestID: "request-1", IdempotencyKey: "idempotency-1", WorkspaceID: definition.WorkspaceID,
		Actor: contracts.ActorRef{ID: "actor-1", Kind: "operator", WorkspaceID: definition.WorkspaceID}, Capability: definition.Ref,
		Target:    contracts.ResourceRef{WorkspaceID: definition.WorkspaceID, System: contracts.SystemRef{WorkspaceID: definition.WorkspaceID, Type: "repository", ID: "repo-1", Version: "1"}, Kind: "repository", ID: "repo-1", Version: "v1"},
		InputType: repository.CapabilityInputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: repository.HashBytes([]byte("input")), Profile: definition.Profile,
	}
}
