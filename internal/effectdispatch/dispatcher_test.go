package effectdispatch

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func TestExternalDispatchErrorDoesNotExposeWrappedPayload(t *testing.T) {
	secret := "authorization=super-secret"
	err := &ExternalDispatchError{Err: errors.New(secret), PossiblyStarted: true}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("uncertain error exposed wrapped payload: %q", err.Error())
	}
	if !errors.Is(err, err.Err) {
		t.Fatal("uncertain error did not preserve machine-readable cause")
	}
}

func TestDomainResultFinalizerRequiresLinkedVerifiedResult(t *testing.T) {
	var calls int
	dispatcher := &Dispatcher{}
	resultHash := contracts.HashStrings("tool-result")
	_, err := dispatcher.publishEffectLinkAndResult(
		context.Background(),
		contracts.ExternalEffectUpdate{WorkspaceID: "workspace", State: contracts.ExternalEffectVerified},
		nil,
		contracts.DomainEffectLinkStatusReconciled,
		resultHash,
		"",
		contracts.ActorRef{ID: "actor", Kind: "service", WorkspaceID: "workspace"},
		&store.OperationResultInput{Result: contracts.OperationResult{OutputHash: resultHash}},
		func(context.Context, pgx.Tx, string) error {
			calls++
			return nil
		},
	)
	if err == nil || calls != 0 {
		t.Fatalf("unlinked finalizer err=%v callback_calls=%d; want rejection before database work", err, calls)
	}
}

func TestAuthoritativeStepRejectsChangedCapabilityOrEffect(t *testing.T) {
	workspace := "workspace-dispatch-test"
	definition := contracts.CapabilityDefinition{
		WorkspaceID:        workspace,
		Ref:                contracts.CapabilityRef{WorkspaceID: workspace, Connector: contracts.ConnectorRef{WorkspaceID: workspace, Name: "fixture", Version: "1"}, Name: "write", Version: "1"},
		InputSchemaVersion: 1, InputSchemaHash: contracts.HashStrings("input"), OutputSchemaVersion: 1, OutputSchemaHash: contracts.HashStrings("output"),
		Effect: contracts.EffectClassReversibleWrite, Profile: contracts.DefaultExecutionProfile(), RetryPolicy: contracts.CapabilityRetryPolicy{MaxAttempts: 1, BackoffMS: 1, MaxBackoffMS: 1, Jitter: "none"}, ResourceKinds: []string{"record"}, Enabled: true,
	}
	if err := definition.Normalize(); err != nil {
		t.Fatal(err)
	}
	request := contracts.OperationRequest{ID: "operation-1", WorkspaceID: workspace, Capability: definition.Ref, Target: contracts.ResourceRef{WorkspaceID: workspace, System: contracts.SystemRef{WorkspaceID: workspace, Type: "fixture", ID: "system", Version: "1"}, Kind: "record", ID: "record", Version: "1"}, InputType: "fixture.input", InputSchemaVersion: 1, InputSchemaHash: definition.InputSchemaHash, InputHash: contracts.HashStrings("value"), Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspace}, Profile: definition.Profile, IdempotencyKey: "operation-key"}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	plan := contracts.OperationPlan{ID: "plan-1", WorkspaceID: workspace, OperationID: request.ID, OperationHash: request.StableHash(), Actor: request.Actor, Steps: []contracts.OperationStep{{ID: "step-1", Kind: "write", Capability: definition.Ref, Target: request.Target, Effect: definition.Effect, Profile: definition.Profile, InputHash: request.InputHash}}}
	if err := plan.Normalize(); err != nil {
		t.Fatal(err)
	}
	operation := store.Operation{WorkspaceID: workspace, ID: request.ID, Request: request, Plan: &plan}
	if step, err := authoritativeStep(operation, "step-1", definition); err != nil || step != "step-1" {
		t.Fatalf("valid authoritative step = %q, %v", step, err)
	}
	changed := definition
	changed.Effect = contracts.EffectClassIrreversibleWrite
	if _, err := authoritativeStep(operation, "step-1", changed); !errors.Is(err, ErrPlanMismatch) {
		t.Fatalf("changed effect error = %v, want plan mismatch", err)
	}
	if _, err := authoritativeStep(operation, "missing", definition); !errors.Is(err, ErrPlanMismatch) {
		t.Fatalf("missing step error = %v, want plan mismatch", err)
	}
}

func TestAdmissionMayDispatchRequiresApprovedPendingApproval(t *testing.T) {
	if !admissionMayDispatch(store.AdmissionResult{Decision: contracts.AdmissionDecision{Status: contracts.AdmissionAllowed}}) {
		t.Fatal("allowed admission was rejected")
	}
	if admissionMayDispatch(store.AdmissionResult{Decision: contracts.AdmissionDecision{Status: contracts.AdmissionAwaitingApproval}}) {
		t.Fatal("pending approval was dispatched")
	}
	expires := time.Now().UTC().Add(time.Minute)
	if !admissionMayDispatch(store.AdmissionResult{Decision: contracts.AdmissionDecision{Status: contracts.AdmissionAwaitingApproval}, Approval: &contracts.OperationApprovalRequest{Status: contracts.ApprovalRequestApproved, ExpiresAt: expires}}) {
		t.Fatal("approved pending admission was rejected")
	}
	expires = time.Now().UTC().Add(-time.Minute)
	if admissionMayDispatch(store.AdmissionResult{Decision: contracts.AdmissionDecision{Status: contracts.AdmissionAwaitingApproval}, Approval: &contracts.OperationApprovalRequest{Status: contracts.ApprovalRequestApproved, ExpiresAt: expires}}) {
		t.Fatal("expired approval was dispatched")
	}
}

func TestReservedEffectHashExcludesDeliveryIdentity(t *testing.T) {
	base := store.OperationEffect{WorkspaceID: "workspace", EffectClass: string(contracts.EffectClassReversibleWrite), Boundary: "fixture", DeliverySemantics: contracts.ExternalDeliveryAtLeastOnce, ProviderIdempotency: true, VerificationRequired: true, VerificationStatus: contracts.ExternalVerificationPending, CompensationStatus: contracts.ExternalCompensationAvailable, EffectID: "effect-a", IdempotencyKey: "key-a", ProviderRequestID: "provider-a"}
	other := base
	other.EffectID, other.IdempotencyKey, other.ProviderRequestID = "effect-b", "key-b", "provider-b"
	if reservedEffectHash(base) == "" || reservedEffectHash(base) != reservedEffectHash(other) {
		t.Fatalf("delivery identities changed effect reservation hash")
	}
}
