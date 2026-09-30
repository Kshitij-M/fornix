package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestEmbeddingRecoveryCoordinatorIsAtomicFencedAndReplayable(t *testing.T) {
	operationStore, pool, workspace := newOperationTestStore(t)
	ctx := context.Background()
	events := NewEventStore(pool)
	admission := NewAdmissionStore(pool, events)
	links := NewDomainEffectLinkStore(pool)
	embeddings := NewEmbeddingCallStore(pool)
	coordinator := NewEmbeddingRecoveryCoordinator(pool, embeddings, admission, links, events)
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.embedding_call_reconciliations WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.embedding_calls WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.domain_effect_link_transitions WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.domain_effect_links WHERE workspace_id=$1`, workspace)
	})

	definition := admissionDefinition(t, workspace, contracts.EffectClassExternalCommunication, false)
	request := operationTestRequest(t, workspace, "embedding-recovery-operation")
	request.Capability = definition.Ref
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	plan := operationTestPlan(t, request)
	created, err := operationStore.Create(ctx, OperationCreateInput{Request: request, Plan: &plan})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admission.Admit(ctx, admissionInputForOperation(t, created.Operation, definition, admissionPolicy(t, workspace, definition), "embedding-recovery-admission")); err != nil {
		t.Fatal(err)
	}
	operationLease, err := operationStore.AcquireLease(ctx, workspace, created.Operation.ID, "embedding-operation-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{contracts.OperationStatusPlanned, contracts.OperationStatusAdmitted, contracts.OperationStatusRunning} {
		if _, err := operationStore.Transition(ctx, OperationTransitionInput{WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: operationLease.Lease.OwnerID, Fence: operationLease.Lease.Fence, Actor: request.Actor, RequestID: "embedding-status-" + status, IdempotencyKey: "embedding-status-" + status, ToStatus: status}); err != nil {
			t.Fatal(err)
		}
	}
	requestHash := created.Operation.OperationHash
	attempt, _, err := operationStore.ReserveAttempt(ctx, OperationAttemptInput{WorkspaceID: workspace, OperationID: created.Operation.ID, StepID: "step-1", Attempt: 1, AttemptID: "embedding-recovery-attempt", OwnerID: operationLease.Lease.OwnerID, Fence: operationLease.Lease.Fence, RequestHash: requestHash, IdempotencyKey: "embedding-recovery-attempt"})
	if err != nil {
		t.Fatal(err)
	}
	effect, _, err := operationStore.ReserveEffect(ctx, OperationEffectInput{WorkspaceID: workspace, OperationID: created.Operation.ID, StepID: "step-1", AttemptID: attempt.AttemptID, OwnerID: operationLease.Lease.OwnerID, Fence: operationLease.Lease.Fence, RequestHash: requestHash, Effect: contracts.ExternalEffect{WorkspaceID: workspace, Boundary: "embedding-provider", Class: definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: "embedding-effect", VerificationStatus: contracts.ExternalVerificationPending, CompensationStatus: contracts.ExternalCompensationAvailable}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := admission.UpdateEffect(ctx, contracts.ExternalEffectUpdate{WorkspaceID: workspace, OperationID: created.Operation.ID, EffectID: effect.EffectID, OwnerID: operationLease.Lease.OwnerID, Fence: operationLease.Lease.Fence, RequestID: "embedding-dispatch", IdempotencyKey: "embedding-dispatch", State: contracts.ExternalEffectDispatching}); err != nil {
		t.Fatal(err)
	}
	if _, err := admission.UpdateEffect(ctx, contracts.ExternalEffectUpdate{WorkspaceID: workspace, OperationID: created.Operation.ID, EffectID: effect.EffectID, OwnerID: operationLease.Lease.OwnerID, Fence: operationLease.Lease.Fence, RequestID: "embedding-recovery-required", IdempotencyKey: "embedding-recovery-required", State: contracts.ExternalEffectRecoveryRequired, ProviderRequestID: "provider-receipt"}); err != nil {
		t.Fatal(err)
	}

	embeddingRequest := contracts.EmbeddingRequest{
		RequestID: "embedding-recovery-call", IdempotencyKey: "embedding-recovery-call-key", WorkspaceID: workspace,
		Actor: request.Actor, Provider: contracts.ProviderRef{Provider: "fake", Model: "fake-model"}, Model: "fake-model",
		SourceKind: "memo", SourceID: "memo-1", Text: "recovery source",
		Budget: contracts.EmbeddingBudget{MaxInputBytes: contracts.MaxEmbeddingInputBytes, Dimension: contracts.EmbeddingDimension, TimeoutMS: 1000},
	}
	if _, err := embeddings.Start(ctx, embeddingRequest, nil); err != nil {
		t.Fatal(err)
	}
	if err := embeddings.Attempt(ctx, workspace, embeddingRequest.RequestID); err != nil {
		t.Fatal(err)
	}
	if err := embeddings.Finish(ctx, contracts.EmbeddingCallResult{WorkspaceID: workspace, RequestID: embeddingRequest.RequestID, Status: contracts.EmbeddingCallRecoveryRequired, ProviderRequestID: "provider-receipt", Failure: &contracts.EmbeddingFailure{Code: contracts.EmbeddingFailureTransport, PossiblyStarted: true}}); err != nil {
		t.Fatal(err)
	}
	call, err := embeddings.Get(ctx, workspace, embeddingRequest.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	link, err := links.Bind(ctx, contracts.DomainEffectLink{
		WorkspaceID: workspace, OperationID: created.Operation.ID, OperationHash: requestHash, StepID: effect.StepID, AttemptID: effect.AttemptID,
		EffectID: effect.EffectID, EffectReservationHash: storedEffectIdentityHash(effect), DomainKind: contracts.DomainEffectKindEmbeddingCall,
		DomainID: embeddingRequest.IdempotencyKey, DomainHash: embeddingRequest.RequestHash(), RequestHash: requestHash,
		Boundary: effect.Boundary, EffectClass: contracts.EffectClass(effect.EffectClass), DeliveryGuarantee: effect.DeliverySemantics,
		ProviderIdempotency: effect.ProviderIdempotency, ProviderRequestID: effect.ProviderRequestID, VerificationStatus: effect.VerificationStatus,
		OperationOwnerID: operationLease.Lease.OwnerID, OperationFence: operationLease.Lease.Fence, Actor: request.Actor,
		RequestID: embeddingRequest.RequestID, IdempotencyKey: "embedding-recovery-link",
	})
	if err != nil {
		t.Fatal(err)
	}
	recoveryLease, err := admission.AcquireEffectLease(ctx, workspace, created.Operation.ID, effect.EffectID, "embedding-recovery-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	vector := make([]float32, contracts.EmbeddingDimension)
	for i := range vector {
		vector[i] = float32((i%17)+1) / 17
	}
	command := contracts.EmbeddingRecoveryFinalizeRequest{
		WorkspaceID: workspace, RequestID: embeddingRequest.RequestID, OwnerID: recoveryLease.Lease.OwnerID,
		Fence: recoveryLease.Lease.Fence, ExpectedEffectVersion: 3, ExpectedLinkVersion: 1,
		IdempotencyKey: "embedding-recovery-finalize", Actor: request.Actor,
		Result: contracts.EmbeddingReconciliationResult{WorkspaceID: workspace, RequestID: embeddingRequest.RequestID, Actor: request.Actor, Status: contracts.EmbeddingCallSucceeded, Provider: embeddingRequest.Provider, SourceHash: call.SourceHash, ProviderRequestID: "provider-receipt", Vector: vector},
	}
	coordinator.SetFailureHook(func(point string) error {
		if point == "embedding_recovery_effect_transition" {
			return errors.New("simulated crash")
		}
		return nil
	})
	if _, err := coordinator.Finalize(ctx, command); err == nil {
		t.Fatal("expected simulated crash")
	}
	coordinator.SetFailureHook(nil)
	unchangedEffect, err := admission.GetEffectState(ctx, workspace, effect.EffectID)
	if err != nil || unchangedEffect.State != contracts.ExternalEffectRecoveryRequired || unchangedEffect.Version != 3 {
		t.Fatalf("effect after rollback=%+v err=%v", unchangedEffect, err)
	}
	unchangedCall, err := embeddings.Get(ctx, workspace, embeddingRequest.RequestID)
	if err != nil || unchangedCall.Status != contracts.EmbeddingCallRecoveryRequired {
		t.Fatalf("embedding call after rollback=%+v err=%v", unchangedCall, err)
	}
	currentLink, err := links.CurrentByDomain(ctx, workspace, contracts.DomainEffectKindEmbeddingCall, embeddingRequest.IdempotencyKey, contracts.DomainEffectLinkRolePrimary)
	if err != nil || currentLink.Transition.Version != 1 || currentLink.Transition.ToStatus != contracts.DomainEffectLinkStatusLinked {
		t.Fatalf("link after rollback=%+v err=%v", currentLink, err)
	}
	if err := admission.ReleaseEffectLease(ctx, recoveryLease.Lease); err != nil {
		t.Fatal(err)
	}
	takeover, err := admission.AcquireEffectLease(ctx, workspace, created.Operation.ID, effect.EffectID, "embedding-recovery-takeover", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	staleCommand := command
	if _, err := coordinator.Finalize(ctx, staleCommand); !errors.Is(err, ErrEffectLeaseFenced) {
		t.Fatalf("stale recovery command error=%v, want ErrEffectLeaseFenced", err)
	}
	command.OwnerID = takeover.Lease.OwnerID
	command.Fence = takeover.Lease.Fence

	resolved, err := coordinator.Finalize(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Duplicate || resolved.Record.Status != contracts.EmbeddingCallSucceeded || resolved.Effect.State != contracts.ExternalEffectVerificationPending || resolved.Link.Status != contracts.DomainEffectLinkStatusReconciled {
		t.Fatalf("resolved=%+v", resolved)
	}
	duplicate, err := coordinator.Finalize(ctx, command)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Duplicate || duplicate.Record.VectorHash != resolved.Record.VectorHash {
		t.Fatalf("duplicate=%+v resolved=%+v", duplicate, resolved)
	}
	var concurrent sync.WaitGroup
	concurrentErrs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		concurrent.Add(1)
		go func() {
			defer concurrent.Done()
			result, callErr := coordinator.Finalize(ctx, command)
			if callErr == nil && !result.Duplicate {
				callErr = errors.New("concurrent replay was not classified as duplicate")
			}
			concurrentErrs <- callErr
		}()
	}
	concurrent.Wait()
	close(concurrentErrs)
	for callErr := range concurrentErrs {
		if callErr != nil {
			t.Fatal(callErr)
		}
	}
	var auditCount, eventCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.embedding_call_reconciliations WHERE workspace_id=$1 AND request_id=$2`, workspace, embeddingRequest.RequestID).Scan(&auditCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1 AND event_type='embedding.recovery_finalized'`, workspace).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if auditCount != 1 || eventCount != 1 {
		t.Fatalf("audit=%d events=%d, want one each", auditCount, eventCount)
	}
	queryUse := contracts.EmbeddingQueryUse{
		WorkspaceID: workspace, IdempotencyKey: "embedding-query-use-1", RequestID: "query-request-1", EmbeddingRequestID: embeddingRequest.RequestID,
		SourceHash: call.SourceHash, Provider: embeddingRequest.Provider, Actor: request.Actor, Route: "memo", GateReason: "cache_hit",
		CacheHit: true, Usage: resolved.Record.Usage, UsageEstimated: true,
	}
	if duplicateUse, err := embeddings.RecordQueryUse(ctx, queryUse); err != nil || duplicateUse {
		t.Fatalf("query use first duplicate=%v err=%v", duplicateUse, err)
	}
	if duplicateUse, err := embeddings.RecordQueryUse(ctx, queryUse); err != nil || !duplicateUse {
		t.Fatalf("query use replay duplicate=%v err=%v", duplicateUse, err)
	}
	conflictingUse := queryUse
	conflictingUse.Route = "symbol"
	if _, err := embeddings.RecordQueryUse(ctx, conflictingUse); !errors.Is(err, ErrEmbeddingQueryUseConflict) {
		t.Fatalf("conflicting query attribution error=%v, want ErrEmbeddingQueryUseConflict", err)
	}
	_ = link
}
