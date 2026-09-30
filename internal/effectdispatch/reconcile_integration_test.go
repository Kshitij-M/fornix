package effectdispatch

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func TestReconcileIsFencedAtomicCrashSafeAndDuplicateSafe(t *testing.T) {
	dispatcher, operations, workspace, request, definition, admission, pool := newDispatcherFixture(t)
	defer pool.Close()
	lease, err := prepareDispatcherOperation(operations, workspace, request)
	if err != nil {
		t.Fatal(err)
	}
	input := dispatcherRequest(workspace, request, definition, admission, lease, func(_ context.Context, _ contracts.EffectAuthority) (InvocationResult, error) {
		return InvocationResult{}, &ExternalDispatchError{Err: errors.New("provider unavailable"), ProviderRequestID: "provider-1", PossiblyStarted: true}
	})
	input.Effect.VerificationRequired = true
	input.Effect.VerificationStatus = contracts.ExternalVerificationPending
	input.AttemptID = "attempt-1"
	input.Effect.ID = "effect-1"
	_, dispatchErr := dispatcher.Dispatch(context.Background(), input)
	if !errors.Is(dispatchErr, ErrUncertainOutcome) {
		t.Fatalf("dispatch error=%v, want uncertain outcome", dispatchErr)
	}

	effectID := "effect-1"
	state, err := dispatcher.Admission.GetEffectState(context.Background(), workspace, effectID)
	if err != nil {
		t.Fatalf("read uncertain effect state: %v", err)
	}
	link, err := dispatcher.Links.CurrentByDomain(context.Background(), workspace, contracts.DomainEffectKindHTTPRequest, request.ID, contracts.DomainEffectLinkRolePrimary)
	if err != nil {
		t.Fatalf("read recovery link: %v", err)
	}
	if state.State != contracts.ExternalEffectRecoveryRequired || link.Transition.ToStatus != contracts.DomainEffectLinkStatusRecoveryRequired {
		t.Fatalf("uncertain authorities state=%+v link=%+v", state, link.Transition)
	}

	firstLease, err := dispatcher.Admission.AcquireEffectLease(context.Background(), workspace, request.ID, effectID, request.Actor.ID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Admission.ReleaseEffectLease(context.Background(), firstLease.Lease); err != nil {
		t.Fatal(err)
	}
	currentLease, err := dispatcher.Admission.AcquireEffectLease(context.Background(), workspace, request.ID, effectID, "recovery-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !currentLease.Takeover || currentLease.Lease.Fence <= firstLease.Lease.Fence {
		t.Fatalf("released-lease takeover did not advance its fence: first=%+v takeover=%+v", firstLease.Lease, currentLease)
	}
	if err := dispatcher.Admission.ReleaseEffectLease(context.Background(), currentLease.Lease); err != nil {
		t.Fatal(err)
	}
	expiringLease, err := dispatcher.Admission.AcquireEffectLease(context.Background(), workspace, request.ID, effectID, "short-lived-worker", 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(75 * time.Millisecond)
	if err := dispatcher.Admission.ValidateEffectLease(context.Background(), expiringLease.Lease); !errors.Is(err, store.ErrEffectLeaseExpired) {
		t.Fatalf("expired effect lease validation error=%v", err)
	}
	currentLease, err = dispatcher.Admission.AcquireEffectLease(context.Background(), workspace, request.ID, effectID, "recovery-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !currentLease.Takeover || currentLease.Lease.Fence <= expiringLease.Lease.Fence {
		t.Fatalf("expired-lease takeover did not advance its fence: expired=%+v takeover=%+v", expiringLease.Lease, currentLease)
	}

	reconcile := ReconcileRequest{
		WorkspaceID: workspace, OperationID: request.ID, EffectID: effectID, LinkID: link.Link.ID,
		OwnerID: currentLease.Lease.OwnerID, EffectFence: currentLease.Lease.Fence,
		ExpectedEffectVer: state.Version, ExpectedLinkVersion: link.Transition.Version,
		RequestID: "verify-request-1", IdempotencyKey: "verify-key-1", Actor: request.Actor,
		Outcome: contracts.EffectVerificationResult{Status: contracts.EffectVerificationStatusVerified, ResultHash: contracts.HashStrings("verified-result"), VerificationHash: contracts.HashStrings("verified-proof"), ProviderRequestID: "provider-1"},
	}
	stale := reconcile
	stale.OwnerID = firstLease.Lease.OwnerID
	stale.EffectFence = firstLease.Lease.Fence
	if _, err := dispatcher.Reconcile(context.Background(), stale); !errors.Is(err, store.ErrEffectLeaseFenced) && !errors.Is(err, store.ErrEffectLeaseExpired) && !errors.Is(err, store.ErrEffectLeaseReleased) {
		t.Fatalf("stale reconciliation error=%v", err)
	}
	expired := reconcile
	expired.OwnerID = expiringLease.Lease.OwnerID
	expired.EffectFence = expiringLease.Lease.Fence
	if _, err := dispatcher.Reconcile(context.Background(), expired); !errors.Is(err, store.ErrEffectLeaseFenced) && !errors.Is(err, store.ErrEffectLeaseExpired) {
		t.Fatalf("expired reconciliation error=%v", err)
	}

	dispatcher.Admission.SetFailureHook(func(point string) error {
		if point == "operation_effect_state_committed" {
			return fmt.Errorf("simulated reconciliation crash")
		}
		return nil
	})
	if _, err := dispatcher.Reconcile(context.Background(), reconcile); err == nil {
		t.Fatal("injected reconciliation crash did not surface")
	}
	dispatcher.Admission.SetFailureHook(nil)

	crashState, err := dispatcher.Admission.GetEffectState(context.Background(), workspace, effectID)
	if err != nil {
		t.Fatal(err)
	}
	if crashState.State != contracts.ExternalEffectVerificationPending || crashState.Version != state.Version+1 {
		t.Fatalf("crash did not preserve the committed pending checkpoint: before=%+v after=%+v", state, crashState)
	}
	linkAfterCrash, err := dispatcher.Links.CurrentByDomain(context.Background(), workspace, contracts.DomainEffectKindHTTPRequest, request.ID, contracts.DomainEffectLinkRolePrimary)
	if err != nil {
		t.Fatal(err)
	}
	if linkAfterCrash.Transition.Version != link.Transition.Version || linkAfterCrash.Transition.ToStatus != contracts.DomainEffectLinkStatusRecoveryRequired {
		t.Fatalf("crash partially committed the domain link: before=%+v after=%+v", link.Transition, linkAfterCrash.Transition)
	}
	const concurrentDeliveries = 6
	start := make(chan struct{})
	results := make([]ReconcileResult, concurrentDeliveries)
	errs := make([]error, concurrentDeliveries)
	var wg sync.WaitGroup
	for index := range results {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			<-start
			results[index], errs[index] = dispatcher.Reconcile(context.Background(), reconcile)
		}(index)
	}
	close(start)
	wg.Wait()
	var first ReconcileResult
	committed, duplicateCount := 0, 0
	for index, result := range results {
		if errs[index] != nil {
			t.Fatalf("concurrent reconciliation %d failed: %v", index, errs[index])
		}
		if result.Duplicate {
			duplicateCount++
		} else {
			committed++
			first = result
		}
	}
	if committed != 1 || duplicateCount != concurrentDeliveries-1 {
		t.Fatalf("concurrent duplicate outcomes: committed=%d duplicates=%d", committed, duplicateCount)
	}
	if first.Duplicate || first.State.State != contracts.ExternalEffectVerified || first.Link.Status != contracts.DomainEffectLinkStatusReconciled {
		t.Fatalf("first reconciliation=%+v", first)
	}
	second, err := dispatcher.Reconcile(context.Background(), reconcile)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Duplicate || second.State.Version != first.State.Version || second.LinkVersion != first.LinkVersion {
		t.Fatalf("duplicate reconciliation=%+v first=%+v", second, first)
	}
}

func TestUnknownVerificationCanBeRetriedWithFreshIdempotencyKey(t *testing.T) {
	dispatcher, operations, workspace, request, definition, admission, pool := newDispatcherFixture(t)
	defer pool.Close()
	lease, err := prepareDispatcherOperation(operations, workspace, request)
	if err != nil {
		t.Fatal(err)
	}
	input := dispatcherRequest(workspace, request, definition, admission, lease, func(_ context.Context, _ contracts.EffectAuthority) (InvocationResult, error) {
		return InvocationResult{}, &ExternalDispatchError{Err: errors.New("provider unavailable"), ProviderRequestID: "provider-unknown", PossiblyStarted: true}
	})
	input.Effect.VerificationRequired = true
	input.Effect.VerificationStatus = contracts.ExternalVerificationPending
	input.AttemptID = "attempt-unknown"
	input.Effect.ID = "effect-unknown-retry"
	_, dispatchErr := dispatcher.Dispatch(context.Background(), input)
	if !errors.Is(dispatchErr, ErrUncertainOutcome) {
		t.Fatalf("dispatch error=%v, want uncertain outcome", dispatchErr)
	}

	ctx := context.Background()
	effectID := input.Effect.ID
	state, err := dispatcher.Admission.GetEffectState(ctx, workspace, effectID)
	if err != nil {
		t.Fatal(err)
	}
	link, err := dispatcher.Links.CurrentByDomain(ctx, workspace, contracts.DomainEffectKindHTTPRequest, request.ID, contracts.DomainEffectLinkRolePrimary)
	if err != nil {
		t.Fatal(err)
	}
	leaseResult, err := dispatcher.Admission.AcquireEffectLease(ctx, workspace, request.ID, effectID, "recovery-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	unknown := ReconcileRequest{
		WorkspaceID: workspace, OperationID: request.ID, EffectID: effectID, LinkID: link.Link.ID,
		OwnerID: leaseResult.Lease.OwnerID, EffectFence: leaseResult.Lease.Fence,
		ExpectedEffectVer: state.Version, ExpectedLinkVersion: link.Transition.Version,
		RequestID: "verify-unknown-request", IdempotencyKey: "verify-unknown-key", Actor: request.Actor,
		Outcome: contracts.EffectVerificationResult{Status: contracts.EffectVerificationStatusUnknown, ProviderRequestID: "provider-unknown", FailureCode: "verification_inconclusive"},
	}
	unknownResult, err := dispatcher.Reconcile(ctx, unknown)
	if err != nil {
		t.Fatal(err)
	}
	if unknownResult.State.State != contracts.ExternalEffectRecoveryRequired || unknownResult.Duplicate {
		t.Fatalf("unknown verification was not durably recorded: %+v", unknownResult)
	}
	historical, err := dispatcher.Admission.GetEffectTransition(ctx, workspace, effectID, unknown.IdempotencyKey+":final")
	if err != nil {
		t.Fatalf("read immutable verification result: %v", err)
	}
	if historical.ToState != contracts.ExternalEffectRecoveryRequired || historical.FailureCode != unknown.Outcome.FailureCode {
		t.Fatalf("historical verification transition=%+v", historical)
	}

	verified := unknown
	verified.RequestID = "verify-success-request"
	verified.IdempotencyKey = "verify-success-key"
	verified.ExpectedEffectVer = unknownResult.State.Version
	verified.ExpectedLinkVersion = unknownResult.LinkVersion
	verified.Outcome = contracts.EffectVerificationResult{
		Status: contracts.EffectVerificationStatusVerified, ProviderRequestID: "provider-unknown",
		ResultHash: contracts.HashStrings("recovered-result"), VerificationHash: contracts.HashStrings("recovered-proof"),
	}
	verifiedResult, err := dispatcher.Reconcile(ctx, verified)
	if err != nil {
		t.Fatalf("fresh verification attempt could not recover unknown effect: %v", err)
	}
	if verifiedResult.State.State != contracts.ExternalEffectVerified || verifiedResult.Link.Status != contracts.DomainEffectLinkStatusReconciled || verifiedResult.Duplicate {
		t.Fatalf("fresh verification attempt did not reconcile effect: %+v", verifiedResult)
	}
}
