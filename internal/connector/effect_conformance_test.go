package connector_test

import (
	"context"
	"errors"
	"testing"

	"github.com/omaveda/fornix/internal/adapters/fakeincident"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

func TestEffectAuthorityConformanceRejectsDirectEffectfulAdmission(t *testing.T) {
	const workspace = "workspace-authority-conformance"
	adapter, err := fakeincident.NewConnector(workspace)
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	registry.RequireEffectAuthority(true)
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if err := registry.ValidateEffectAuthorityConformance(workspace); err != nil {
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
		t.Fatal("effectful fake incident capability not registered")
	}
	request := effectAuthorityTestRequest(capability.Definition())
	if _, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{ApprovalGranted: true}); err == nil {
		t.Fatalf("direct effectful admission error=%v, want authority rejection", err)
	}
	if _, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{ApprovalGranted: true, RequireAuthority: true, DeferEffectAuthority: true}); err == nil {
		t.Fatal("deferred metadata admission bypassed an explicit authority requirement")
	}
	deferred, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{ApprovalGranted: true, DeferEffectAuthority: true})
	if err != nil {
		t.Fatalf("pre-dispatch metadata admission: %v", err)
	}
	if deferred.Authority != nil {
		t.Fatal("pre-dispatch admission unexpectedly contains effect authority")
	}
	deferredPlan, err := deferred.Capability.Plan(request)
	if err != nil {
		t.Fatalf("pre-dispatch planning: %v", err)
	}
	if _, err := deferred.Capability.Execute(context.Background(), request, deferredPlan); !errors.Is(err, connector.ErrAuthorityExecution) {
		t.Fatalf("pre-dispatch plan execution error=%v, want ErrAuthorityExecution", err)
	}
	authority := contracts.EffectAuthority{WorkspaceID: workspace, OperationID: request.ID, OperationOwnerID: "worker-1", OperationFence: 1}
	admitted, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{ApprovalGranted: true, RequireAuthority: true, Authority: &authority})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := admitted.Capability.Plan(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admitted.Capability.Execute(context.Background(), request, plan); !errors.Is(err, connector.ErrAuthorityExecution) {
		t.Fatalf("ordinary effectful Execute error=%v, want ErrAuthorityExecution", err)
	}
}

func TestEffectAuthorityValidatorRunsBeforeEveryDispatch(t *testing.T) {
	const workspace = "workspace-authority-validator"
	adapter, err := fakeincident.NewConnector(workspace)
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	var capability connector.Capability
	for _, candidate := range adapter.Capabilities() {
		if candidate.Definition().Ref.Name == "incident.remediate" {
			capability = candidate
			break
		}
	}
	request := effectAuthorityTestRequest(capability.Definition())
	authority := contracts.EffectAuthority{WorkspaceID: workspace, OperationID: request.ID, OperationOwnerID: "worker-1", OperationFence: 1}
	var validations int
	_, err = (&connector.Executor{Registry: registry}).Execute(context.Background(), request, connector.AdmissionOptions{
		ApprovalGranted: true, RequireAuthority: true, Authority: &authority,
		ValidateAuthority: func(context.Context, contracts.EffectAuthority) error {
			validations++
			if validations == 2 {
				return errors.New("stale operation fence")
			}
			return nil
		},
	})
	if err == nil || validations != 2 {
		t.Fatalf("dispatch validation err=%v validations=%d, want second validation to fail", err, validations)
	}
}

func effectAuthorityTestRequest(definition contracts.CapabilityDefinition) contracts.OperationRequest {
	return contracts.OperationRequest{
		ID: "authority-operation", RequestID: "authority-request", IdempotencyKey: "authority-idempotency",
		WorkspaceID: definition.WorkspaceID,
		Actor:       contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: definition.WorkspaceID},
		Capability:  definition.Ref,
		Target:      contracts.ResourceRef{WorkspaceID: definition.WorkspaceID, System: contracts.SystemRef{WorkspaceID: definition.WorkspaceID, Type: "monitoring", ID: "monitor", Version: "1"}, Kind: fakeincident.ResourceKind, ID: "incident-1", Version: "1"},
		InputType:   fakeincident.InputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash,
		InputHash: contracts.HashStrings("authority-input"), Profile: definition.Profile,
	}
}
