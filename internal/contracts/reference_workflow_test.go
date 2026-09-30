package contracts

import "testing"

func referenceWorkflowTarget(workspace, kind, id string) ResourceRef {
	return ResourceRef{WorkspaceID: workspace, System: SystemRef{WorkspaceID: workspace, Type: kind, ID: "system-1", Version: "1"}, Kind: kind, ID: id, Version: "1"}
}

func TestReferenceWorkflowRequestDerivesStableDeliveryIdentity(t *testing.T) {
	request := ReferenceWorkflowRequest{WorkspaceID: "workspace-a", Actor: ActorRef{ID: "actor-1", Kind: "operator", WorkspaceID: "workspace-a"}, Kind: ReferenceWorkflowDataPipeline, Target: referenceWorkflowTarget("workspace-a", "pipeline", "pipeline-1"), IntentHash: HashStrings("intent")}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	firstHash := request.StableHash()
	if firstHash == "" || request.ID == "" || request.RequestID == "" || request.IdempotencyKey == "" {
		t.Fatalf("request identity was not derived: %+v", request)
	}
	repeated := request
	if err := repeated.Normalize(); err != nil {
		t.Fatal(err)
	}
	if repeated.StableHash() != firstHash || repeated.ID != request.ID || repeated.IdempotencyKey != request.IdempotencyKey {
		t.Fatalf("request identity was not stable: first=%+v repeated=%+v", request, repeated)
	}
}

func TestReferenceWorkflowRequestRejectsCrossWorkspaceAndUnapprovedEffect(t *testing.T) {
	request := ReferenceWorkflowRequest{WorkspaceID: "workspace-a", Actor: ActorRef{ID: "actor-1", Kind: "operator", WorkspaceID: "workspace-a"}, Kind: ReferenceWorkflowIncidentResponse, Target: referenceWorkflowTarget("workspace-b", "service", "service-1"), IntentHash: HashStrings("intent")}
	if err := request.Normalize(); err == nil {
		t.Fatal("cross-workspace target was accepted")
	}
	request.Target = referenceWorkflowTarget("workspace-a", "service", "service-1")
	request.Effectful = true
	if err := request.Normalize(); err == nil {
		t.Fatal("effectful workflow without approval was accepted")
	}
}

func TestReferenceWorkflowStepHashIsBoundedAndStable(t *testing.T) {
	step := OperationStep{ID: "step-1", Kind: WorkflowStepValidation, Effect: EffectClassObservation, InputHash: HashStrings("input")}
	first := ReferenceWorkflowStepHash(HashStrings("plan"), step)
	second := ReferenceWorkflowStepHash(HashStrings("plan"), step)
	if first == "" || first != second {
		t.Fatalf("step hash is unstable: %q %q", first, second)
	}
}
