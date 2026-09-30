package fakedomains

import (
	"context"
	"testing"

	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

func verificationCapability(t *testing.T) (connector.Capability, contracts.OperationRequest) {
	t.Helper()
	fake, err := NewConnector("workspace-a")
	if err != nil {
		t.Fatal(err)
	}
	for _, candidate := range fake.Capabilities() {
		if candidate.Definition().Ref.Name != "data_pipeline.publish" {
			continue
		}
		definition := candidate.Definition()
		request := contracts.OperationRequest{
			ID: "operation-1", RequestID: "request-1", IdempotencyKey: "operation-key", WorkspaceID: "workspace-a",
			Actor:      contracts.ActorRef{ID: "actor-1", Kind: "operator", WorkspaceID: "workspace-a"},
			Capability: definition.Ref,
			Target:     contracts.ResourceRef{WorkspaceID: "workspace-a", System: contracts.SystemRef{WorkspaceID: "workspace-a", Type: "pipeline", ID: "warehouse", Version: "1"}, Kind: PipelineKind, ID: "pipeline-1", Version: "1", ContentHash: contracts.HashStrings("target")},
			InputType:  PipelineInput, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash,
			InputHash: contracts.HashStrings("input"), Profile: definition.Profile,
		}
		if err := request.Normalize(); err != nil {
			t.Fatal(err)
		}
		return candidate, request
	}
	t.Fatal("publish capability not registered")
	return nil, contracts.OperationRequest{}
}

func verificationRequestForFake(t *testing.T, providerID string) contracts.EffectVerificationRequest {
	t.Helper()
	capability, operation := verificationCapability(t)
	describer := capability.(connector.EffectDescriber)
	effect, err := describer.DescribeEffect(operation)
	if err != nil {
		t.Fatal(err)
	}
	effect.ProviderRequestID = providerID
	if err := effect.Normalize(); err != nil {
		t.Fatal(err)
	}
	operationHash := operation.StableHash()
	link := contracts.DomainEffectLink{
		WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, OperationHash: operationHash,
		StepID: "step-1", AttemptID: "attempt-1", EffectID: effect.ID, EffectReservationHash: effect.StableHash(),
		DomainKind: contracts.DomainEffectKindWorkflowStep, DomainID: "run-1:step-1", DomainHash: contracts.HashStrings("domain"),
		LinkRole: contracts.DomainEffectLinkRolePrimary, RequestHash: operation.InputHash, Boundary: effect.Boundary,
		EffectClass: effect.Class, DeliveryGuarantee: effect.DeliveryGuarantee, ProviderIdempotency: effect.ProviderIdempotency,
		ProviderRequestID: effect.ProviderRequestID, VerificationStatus: contracts.ExternalVerificationPending,
		Status: contracts.DomainEffectLinkStatusLinked, Actor: operation.Actor, RequestID: "link-request", IdempotencyKey: "link-key",
	}
	if err := link.Normalize(); err != nil {
		t.Fatal(err)
	}
	request := contracts.EffectVerificationRequest{
		WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, RunID: "run-1", StepID: "step-1", EffectID: effect.ID,
		OperationHash: operationHash, Operation: operation, Effect: effect, Link: link,
		EffectState: contracts.ExternalEffectAcknowledged, EffectVersion: 1, LinkVersion: 1, Actor: operation.Actor, IdempotencyKey: "verify-key",
	}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	return request
}

func TestFakeDomainVerifierIsDeterministicAndHashOnly(t *testing.T) {
	capability, _ := verificationCapability(t)
	verifier := capability.(connector.EffectVerifier)
	verified, err := verifier.VerifyEffect(context.Background(), verificationRequestForFake(t, "provider-1"))
	if err != nil {
		t.Fatal(err)
	}
	if verified.Status != contracts.EffectVerificationStatusVerified || verified.ResultHash == "" || verified.VerificationHash == "" {
		t.Fatalf("unexpected verified outcome: %+v", verified)
	}
	repeated, err := verifier.VerifyEffect(context.Background(), verificationRequestForFake(t, "provider-1"))
	if err != nil {
		t.Fatal(err)
	}
	if verified != repeated {
		t.Fatalf("verification was not deterministic: %+v != %+v", verified, repeated)
	}
	mismatch, err := verifier.VerifyEffect(context.Background(), verificationRequestForFake(t, "provider-mismatch"))
	if err != nil || mismatch.Status != contracts.EffectVerificationStatusFailed || mismatch.FailureCode != "result_mismatch" {
		t.Fatalf("unexpected mismatch outcome=%+v err=%v", mismatch, err)
	}
	uncertain, err := verifier.VerifyEffect(context.Background(), verificationRequestForFake(t, "provider-uncertain"))
	if err != nil || uncertain.Status != contracts.EffectVerificationStatusUnknown || uncertain.FailureCode != "external_uncertain" {
		t.Fatalf("unexpected uncertain outcome=%+v err=%v", uncertain, err)
	}
}
