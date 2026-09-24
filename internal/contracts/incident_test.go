package contracts

import (
	"strings"
	"testing"
)

func TestIncidentEventHashExcludesDeliveryIdentity(t *testing.T) {
	first := IncidentEvent{
		WorkspaceID: "workspace-incident", SourceSystem: "monitor", ExternalID: "event-1",
		Severity: IncidentSeverityWarning, Payload: []byte(`{"status":"degraded"}`),
		DeliveryMode: IncidentDeliveryFake, IdempotencyKey: "delivery-a", RequestID: "request-a",
		Actor: ActorRef{ID: "operator", Kind: "human", WorkspaceID: "workspace-incident"},
	}
	second := first
	second.DeliveryID, second.IdempotencyKey, second.RequestID = "delivery-b", "delivery-b", "request-b"
	second.CausationID, second.CorrelationID = "cause-b", "correlation-b"
	if err := first.Normalize(); err != nil {
		t.Fatal(err)
	}
	if err := second.Normalize(); err != nil {
		t.Fatal(err)
	}
	if first.StableHash() != second.StableHash() || first.PayloadHash == "" {
		t.Fatalf("delivery identity changed incident hash: %s != %s", first.StableHash(), second.StableHash())
	}
}

func TestSignedIncidentRequiresPreverifiedSignatureMetadata(t *testing.T) {
	event := IncidentEvent{
		WorkspaceID: "workspace-incident", SourceSystem: "monitor", ExternalID: "event-1",
		Severity: IncidentSeverityCritical, Payload: []byte(`{"status":"down"}`),
		DeliveryMode:   IncidentDeliverySigned,
		IdempotencyKey: "signed-event-1",
		Actor:          ActorRef{ID: "operator", Kind: "human", WorkspaceID: "workspace-incident"},
	}
	if err := event.Normalize(); err == nil {
		t.Fatal("signed event without signature metadata was accepted")
	}
	event.SignatureHash = strings.Repeat("a", 64)
	event.SignatureScheme = "preverified"
	if err := event.Normalize(); err != nil {
		t.Fatalf("preverified signed event was rejected: %v", err)
	}
}

func TestIncidentApprovalHashBindsDecisionToPlan(t *testing.T) {
	approval := IncidentApproval{
		WorkspaceID: "workspace-incident", RunID: "run-1", StepID: "run-1-approval",
		OperationHash: strings.Repeat("a", 64), PlanHash: strings.Repeat("b", 64),
		Decision: "approve", IdempotencyKey: "approval-1",
		Actor: ActorRef{ID: "operator", Kind: "human", WorkspaceID: "workspace-incident"},
	}
	if err := approval.Normalize(); err != nil {
		t.Fatal(err)
	}
	hash := approval.DecisionHash
	approval.Decision = "reject"
	if err := approval.Normalize(); err != nil {
		t.Fatal(err)
	}
	if hash == approval.DecisionHash {
		t.Fatal("approval decision hash did not change with decision")
	}
}
