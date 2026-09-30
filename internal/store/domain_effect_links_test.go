package store

import (
	"context"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestDomainEffectLinkBindingIsIdempotentAndWorkspaceScoped(t *testing.T) {
	operationStore, pool, workspace := newOperationTestStore(t)
	ctx := context.Background()
	// This cleanup is registered after newOperationTestStore's cleanup, so the
	// relationship rows are removed before their generic parent rows.
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.domain_effect_link_transitions WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.domain_effect_links WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.operation_effect_transitions WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.operation_effect_state WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.operation_effects WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.operation_attempts WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.operation_steps WHERE workspace_id=$1`, workspace)
	})
	events := NewEventStore(pool)
	admission := NewAdmissionStore(pool, events)
	definition := admissionDefinition(t, workspace, contracts.EffectClassReversibleWrite, false)
	request := operationTestRequest(t, workspace, "domain-link-operation")
	request.Capability = definition.Ref
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	plan := contracts.OperationPlan{ID: request.ID + "-plan", WorkspaceID: workspace, OperationID: request.ID, Actor: request.Actor, Steps: []contracts.OperationStep{{ID: "step-1", Ordinal: 0, Kind: "fixture.write", Capability: definition.Ref, Target: request.Target, Effect: definition.Effect, Profile: definition.Profile, InputHash: request.InputHash}}}
	created, err := operationStore.Create(ctx, OperationCreateInput{Request: request, Plan: &plan})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admission.Admit(ctx, admissionInputForOperation(t, created.Operation, definition, admissionPolicy(t, workspace, definition), "domain-link-admission")); err != nil {
		t.Fatal(err)
	}
	lease, err := operationStore.AcquireLease(ctx, workspace, created.Operation.ID, "domain-link-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{contracts.OperationStatusPlanned, contracts.OperationStatusAdmitted, contracts.OperationStatusRunning} {
		if _, err := operationStore.Transition(ctx, OperationTransitionInput{WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: request.Actor, RequestID: "domain-link-" + status, IdempotencyKey: "domain-link-" + status, ToStatus: status}); err != nil {
			t.Fatal(err)
		}
	}
	requestHash := created.Operation.OperationHash
	attempt, _, err := operationStore.ReserveAttempt(ctx, OperationAttemptInput{WorkspaceID: workspace, OperationID: created.Operation.ID, StepID: "step-1", Attempt: 1, AttemptID: "domain-link-attempt", OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, RequestHash: requestHash, IdempotencyKey: "domain-link-attempt-key"})
	if err != nil {
		t.Fatal(err)
	}
	effect, _, err := operationStore.ReserveEffect(ctx, OperationEffectInput{WorkspaceID: workspace, OperationID: created.Operation.ID, StepID: "step-1", AttemptID: attempt.AttemptID, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, RequestHash: requestHash, Effect: contracts.ExternalEffect{WorkspaceID: workspace, Boundary: "fixture.write", Class: definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: "domain-link-effect-key", VerificationStatus: contracts.ExternalVerificationNotRequired, CompensationStatus: contracts.ExternalCompensationUnavailable}})
	if err != nil {
		t.Fatal(err)
	}
	links := NewDomainEffectLinkStore(pool)
	link := contracts.DomainEffectLink{WorkspaceID: workspace, OperationID: created.Operation.ID, OperationHash: created.Operation.OperationHash, StepID: effect.StepID, AttemptID: effect.AttemptID, EffectID: effect.EffectID, EffectReservationHash: storedEffectIdentityHash(effect), DomainKind: contracts.DomainEffectKindHTTPRequest, DomainID: "http-request-1", DomainHash: testHash("tool-request"), RequestHash: requestHash, Boundary: effect.Boundary, EffectClass: contracts.EffectClass(effect.EffectClass), DeliveryGuarantee: effect.DeliverySemantics, ProviderIdempotency: effect.ProviderIdempotency, VerificationStatus: effect.VerificationStatus, OperationOwnerID: lease.Lease.OwnerID, OperationFence: lease.Lease.Fence, Actor: request.Actor, RequestID: "domain-link-request", IdempotencyKey: "domain-link-key"}
	first, err := links.Bind(ctx, link)
	if err != nil {
		t.Fatal(err)
	}
	if first.Duplicate || first.Link.ID == "" || first.Link.LinkHash == "" {
		t.Fatalf("unexpected first binding: %+v", first)
	}
	second, err := links.Bind(ctx, link)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Duplicate || second.Link.ID != first.Link.ID || second.Link.LinkHash != first.Link.LinkHash {
		t.Fatalf("duplicate binding changed identity: first=%+v second=%+v", first, second)
	}
	got, err := links.GetByDomain(ctx, workspace, contracts.DomainEffectKindHTTPRequest, "http-request-1", contracts.DomainEffectLinkRolePrimary)
	if err != nil {
		t.Fatal(err)
	}
	if got.LinkHash != first.Link.LinkHash {
		t.Fatalf("read binding hash=%s want=%s", got.LinkHash, first.Link.LinkHash)
	}
	recovery, err := links.Transition(ctx, contracts.DomainEffectLinkTransitionRequest{
		WorkspaceID: workspace, LinkID: first.Link.ID, ExpectedVersion: 1,
		FromStatus: contracts.DomainEffectLinkStatusLinked, ToStatus: contracts.DomainEffectLinkStatusRecoveryRequired,
		FailureCode: "provider_uncertain", IdempotencyKey: "domain-link-recovery", Actor: request.Actor,
	})
	if err != nil || recovery.Duplicate || recovery.Transition.Version != 2 || recovery.Link.Status != contracts.DomainEffectLinkStatusRecoveryRequired {
		t.Fatalf("recovery transition=%+v err=%v", recovery, err)
	}
	duplicateRecovery, err := links.Transition(ctx, contracts.DomainEffectLinkTransitionRequest{
		WorkspaceID: workspace, LinkID: first.Link.ID, ExpectedVersion: 1,
		FromStatus: contracts.DomainEffectLinkStatusLinked, ToStatus: contracts.DomainEffectLinkStatusRecoveryRequired,
		FailureCode: "provider_uncertain", IdempotencyKey: "domain-link-recovery", Actor: request.Actor,
	})
	if err != nil || !duplicateRecovery.Duplicate || duplicateRecovery.Transition.Version != recovery.Transition.Version {
		t.Fatalf("recovery duplicate=%+v err=%v", duplicateRecovery, err)
	}
	if _, err := links.Transition(ctx, contracts.DomainEffectLinkTransitionRequest{
		WorkspaceID: workspace, LinkID: first.Link.ID, ExpectedVersion: 1,
		FromStatus: contracts.DomainEffectLinkStatusLinked, ToStatus: contracts.DomainEffectLinkStatusRecoveryRequired,
		FailureCode: "different_proof", IdempotencyKey: "domain-link-stale", Actor: request.Actor,
	}); err != ErrDomainEffectLinkFenced {
		t.Fatalf("stale transition err=%v, want=%v", err, ErrDomainEffectLinkFenced)
	}
	reconciled, err := links.Transition(ctx, contracts.DomainEffectLinkTransitionRequest{
		WorkspaceID: workspace, LinkID: first.Link.ID, ExpectedVersion: 2,
		FromStatus: contracts.DomainEffectLinkStatusRecoveryRequired, ToStatus: contracts.DomainEffectLinkStatusReconciled,
		ResultHash: testHash("verified-result"), IdempotencyKey: "domain-link-reconciled", Actor: request.Actor,
	})
	if err != nil || reconciled.Duplicate || reconciled.Link.Status != contracts.DomainEffectLinkStatusReconciled {
		t.Fatalf("reconciled transition=%+v err=%v", reconciled, err)
	}
	latest, err := links.Get(ctx, workspace, first.Link.ID)
	if err != nil || latest.Status != contracts.DomainEffectLinkStatusReconciled {
		t.Fatalf("latest link=%+v err=%v", latest, err)
	}
	foreign, err := links.GetByDomain(ctx, "other-workspace", contracts.DomainEffectKindHTTPRequest, "http-request-1", contracts.DomainEffectLinkRolePrimary)
	if err != nil && !errorsIsDomainLinkNotFound(err) {
		t.Fatal(err)
	}
	if foreign.ID != "" {
		t.Fatal("cross-workspace binding leaked")
	}
}

func errorsIsDomainLinkNotFound(err error) bool { return err == ErrDomainEffectLinkNotFound }

func TestDomainEffectLinkRejectsConflictingSourceAndEffectHashes(t *testing.T) {
	operationStore, pool, workspace := newOperationTestStore(t)
	ctx := context.Background()
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.domain_effect_link_transitions WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.domain_effect_links WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.operation_effect_transitions WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.operation_effect_state WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.operation_effects WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.operation_attempts WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.operation_steps WHERE workspace_id=$1`, workspace)
	})
	definition := admissionDefinition(t, workspace, contracts.EffectClassReversibleWrite, false)
	request := operationTestRequest(t, workspace, "domain-link-conflict")
	request.Capability = definition.Ref
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	plan := contracts.OperationPlan{ID: request.ID + "-plan", WorkspaceID: workspace, OperationID: request.ID, Actor: request.Actor, Steps: []contracts.OperationStep{{ID: "step-1", Ordinal: 0, Kind: "fixture.write", Capability: definition.Ref, Target: request.Target, Effect: definition.Effect, Profile: definition.Profile, InputHash: request.InputHash}}}
	created, err := operationStore.Create(ctx, OperationCreateInput{Request: request, Plan: &plan})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := NewAdmissionStore(pool, NewEventStore(pool)).Admit(ctx, admissionInputForOperation(t, created.Operation, definition, admissionPolicy(t, workspace, definition), "domain-link-conflict-admission")); err != nil {
		t.Fatal(err)
	}
	lease, err := operationStore.AcquireLease(ctx, workspace, created.Operation.ID, "domain-link-conflict-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{contracts.OperationStatusPlanned, contracts.OperationStatusAdmitted, contracts.OperationStatusRunning} {
		if _, err := operationStore.Transition(ctx, OperationTransitionInput{WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: request.Actor, RequestID: "conflict-" + status, IdempotencyKey: "conflict-" + status, ToStatus: status}); err != nil {
			t.Fatal(err)
		}
	}
	requestHash := created.Operation.OperationHash
	attempt, _, err := operationStore.ReserveAttempt(ctx, OperationAttemptInput{WorkspaceID: workspace, OperationID: created.Operation.ID, StepID: "step-1", Attempt: 1, AttemptID: "domain-link-conflict-attempt", OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, RequestHash: requestHash, IdempotencyKey: "domain-link-conflict-attempt-key"})
	if err != nil {
		t.Fatal(err)
	}
	effect, _, err := operationStore.ReserveEffect(ctx, OperationEffectInput{WorkspaceID: workspace, OperationID: created.Operation.ID, StepID: "step-1", AttemptID: attempt.AttemptID, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, RequestHash: requestHash, Effect: contracts.ExternalEffect{WorkspaceID: workspace, Boundary: "fixture.write", Class: definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: "domain-link-conflict-effect", VerificationStatus: contracts.ExternalVerificationNotRequired, CompensationStatus: contracts.ExternalCompensationUnavailable}})
	if err != nil {
		t.Fatal(err)
	}
	linkStore := NewDomainEffectLinkStore(pool)
	link := contracts.DomainEffectLink{WorkspaceID: workspace, OperationID: created.Operation.ID, OperationHash: created.Operation.OperationHash, StepID: effect.StepID, AttemptID: effect.AttemptID, EffectID: effect.EffectID, EffectReservationHash: storedEffectIdentityHash(effect), DomainKind: contracts.DomainEffectKindModelCall, DomainID: "model-call-1", DomainHash: testHash("model-request"), RequestHash: requestHash, Boundary: effect.Boundary, EffectClass: contracts.EffectClass(effect.EffectClass), DeliveryGuarantee: effect.DeliverySemantics, VerificationStatus: effect.VerificationStatus, OperationOwnerID: lease.Lease.OwnerID, OperationFence: lease.Lease.Fence, Actor: request.Actor, RequestID: "conflict-link-request", IdempotencyKey: "conflict-link-key"}
	if _, err := linkStore.Bind(ctx, link); err != nil {
		t.Fatal(err)
	}
	link.DomainHash = testHash("different-model-request")
	link.LinkHash = ""
	if _, err := linkStore.Bind(ctx, link); err == nil {
		t.Fatal("conflicting domain hash was accepted")
	}
}
