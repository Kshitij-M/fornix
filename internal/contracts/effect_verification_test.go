package contracts

import (
	"strings"
	"testing"
)

func verificationTestRequest(t *testing.T, workspace string) EffectVerificationRequest {
	t.Helper()
	operation := domainTestOperation(t, workspace, "request-1", "idempotency-1")
	if err := operation.Normalize(); err != nil {
		t.Fatal(err)
	}
	operationHash := operation.StableHash()
	effect := ExternalEffect{
		ID: "effect-1", WorkspaceID: workspace, Boundary: "fixture.boundary",
		Class: EffectClassExternalCommunication, DeliveryGuarantee: ExternalDeliveryAtLeastOnce,
		IdempotencyKey: "effect-key", ProviderRequestID: "provider-1", ProviderIdempotency: true,
		VerificationRequired: true, VerificationStatus: ExternalVerificationPending,
		CompensationStatus: ExternalCompensationUnavailable,
	}
	if err := effect.Normalize(); err != nil {
		t.Fatal(err)
	}
	link := DomainEffectLink{
		WorkspaceID: workspace, OperationID: operation.ID, OperationHash: operationHash,
		StepID: "step-1", AttemptID: "attempt-1", EffectID: effect.ID,
		EffectReservationHash: effect.StableHash(), DomainKind: DomainEffectKindWorkflowStep,
		DomainID: "run-1:step-1", DomainHash: domainTestHash("domain"), LinkRole: DomainEffectLinkRolePrimary,
		RequestHash: domainTestHash("request"), Boundary: effect.Boundary, EffectClass: effect.Class,
		DeliveryGuarantee: effect.DeliveryGuarantee, ProviderIdempotency: effect.ProviderIdempotency,
		ProviderRequestID: effect.ProviderRequestID, VerificationStatus: ExternalVerificationPending,
		Status: DomainEffectLinkStatusLinked, Actor: operation.Actor, RequestID: "link-request",
		IdempotencyKey: "link-key",
	}
	if err := link.Normalize(); err != nil {
		t.Fatal(err)
	}
	return EffectVerificationRequest{
		WorkspaceID: workspace, OperationID: operation.ID, RunID: "run-1", StepID: "step-1",
		EffectID: effect.ID, OperationHash: operationHash, Operation: operation, Effect: effect,
		Link: link, EffectState: ExternalEffectAcknowledged, EffectVersion: 1, LinkVersion: 1,
		Actor: operation.Actor, IdempotencyKey: "verify-key",
	}
}

func TestEffectVerificationRequestBindsAllAuthorities(t *testing.T) {
	request := verificationTestRequest(t, "workspace-a")
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	request.Link.WorkspaceID = "workspace-b"
	if err := request.Normalize(); err == nil {
		t.Fatal("cross-workspace link was accepted")
	}

	request = verificationTestRequest(t, "workspace-a")
	request.OperationHash = strings.Repeat("a", 64)
	if err := request.Normalize(); err == nil {
		t.Fatal("operation hash mismatch was accepted")
	}
}

func TestEffectVerificationResultRequiresRedactedProof(t *testing.T) {
	verified := EffectVerificationResult{Status: EffectVerificationStatusVerified, ResultHash: domainTestHash("result"), VerificationHash: domainTestHash("proof")}
	if err := verified.Normalize(); err != nil {
		t.Fatal(err)
	}
	for _, result := range []EffectVerificationResult{
		{Status: EffectVerificationStatusVerified, ResultHash: domainTestHash("result")},
		{Status: EffectVerificationStatusVerified, VerificationHash: domainTestHash("proof")},
		{Status: EffectVerificationStatusFailed},
		{Status: EffectVerificationStatusUnknown},
	} {
		if err := result.Normalize(); err == nil {
			t.Fatalf("incomplete verification result was accepted: %+v", result)
		}
	}
	failed := EffectVerificationResult{Status: EffectVerificationStatusFailed, FailureCode: "result_mismatch"}
	if err := failed.Normalize(); err != nil {
		t.Fatal(err)
	}
	failed.FailureCode = "provider response contained arbitrary raw text"
	if err := failed.Normalize(); err == nil {
		t.Fatal("unbounded failure code was accepted")
	}
}
