package effectdispatch

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func TestDispatcherReservesBeforeInvokesAndDeduplicates(t *testing.T) {
	dispatcher, operations, workspace, request, definition, admissionInput, pool := newDispatcherFixture(t)
	defer pool.Close()
	lease, err := operations.AcquireLease(context.Background(), workspace, request.ID, request.Actor.ID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, status := range []string{contracts.OperationStatusPlanned, contracts.OperationStatusAdmitted, contracts.OperationStatusRunning} {
		if _, err := operations.Transition(context.Background(), store.OperationTransitionInput{WorkspaceID: workspace, OperationID: request.ID, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: request.Actor, RequestID: "transition-" + status, IdempotencyKey: "transition-" + status, ToStatus: status}); err != nil {
			t.Fatalf("transition %s: %v", status, err)
		}
	}
	var calls atomic.Int32
	invoker := func(_ context.Context, _ contracts.EffectAuthority) (InvocationResult, error) {
		if calls.Add(1) != 1 {
			t.Fatal("invoker called more than once")
		}
		effect := contracts.ExternalEffect{WorkspaceID: workspace, Boundary: "fixture.dispatch", Class: definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: request.IdempotencyKey, ProviderIdempotency: true, VerificationStatus: contracts.ExternalVerificationNotRequired, CompensationStatus: contracts.ExternalCompensationUnavailable}
		outputHash := contracts.HashStrings("output")
		result := contracts.OperationResult{ID: request.ID + "-result", OperationID: request.ID, OperationHash: request.StableHash(), RequestID: request.RequestID, WorkspaceID: workspace, Actor: request.Actor, Status: contracts.OperationStatusSucceeded, OutputHash: outputHash, ExternalEffects: []contracts.ExternalEffect{effect}}
		return InvocationResult{Result: result, ResponseHash: outputHash}, nil
	}
	input := dispatcherRequest(workspace, request, definition, admissionInput, lease.Lease, invoker)
	var finalizerCalls atomic.Int32
	input.FinalizeTx = func(_ context.Context, _ pgx.Tx, resultHash string) error {
		if resultHash == "" {
			return errors.New("finalizer received no verified result hash")
		}
		finalizerCalls.Add(1)
		return nil
	}
	first, err := dispatcher.Dispatch(context.Background(), input)
	if err != nil {
		fatalDispatchWithFixtureCause(t, err)
	}
	if first.Duplicate || first.OperationResult == nil || first.State.State != contracts.ExternalEffectVerified || first.DomainLink == nil || first.DomainLink.Status != contracts.DomainEffectLinkStatusReconciled {
		t.Fatalf("first dispatch = %+v", first)
	}
	second, err := dispatcher.Dispatch(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Duplicate || calls.Load() != 1 || finalizerCalls.Load() != 1 || second.Effect.EffectID != first.Effect.EffectID {
		t.Fatalf("duplicate dispatch = %+v calls=%d finalizers=%d", second, calls.Load(), finalizerCalls.Load())
	}
	if second.OperationResult == nil || second.OperationResult.Record.ResultHash != first.OperationResult.Record.ResultHash {
		t.Fatalf("duplicate did not replay the committed operation result: second=%+v first=%+v", second.OperationResult, first.OperationResult)
	}
}

func TestDispatcherFinalEffectLinkAndResultRollbackTogether(t *testing.T) {
	dispatcher, operations, workspace, request, definition, admissionInput, pool := newDispatcherFixture(t)
	defer pool.Close()
	lease, err := prepareDispatcherOperation(operations, workspace, request)
	if err != nil {
		t.Fatal(err)
	}
	injectedFinalizationFailure := errors.New("injected specialized result commit failure")
	var calls atomic.Int32
	var effectID string
	input := dispatcherRequest(workspace, request, definition, admissionInput, lease, func(_ context.Context, authority contracts.EffectAuthority) (InvocationResult, error) {
		calls.Add(1)
		effectID = authority.EffectID
		effect := contracts.ExternalEffect{WorkspaceID: workspace, Boundary: "fixture.dispatch", Class: definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: request.IdempotencyKey, ProviderIdempotency: true, VerificationStatus: contracts.ExternalVerificationNotRequired, CompensationStatus: contracts.ExternalCompensationUnavailable}
		outputHash := contracts.HashStrings("atomic-result")
		result := contracts.OperationResult{ID: request.ID + "-result", OperationID: request.ID, OperationHash: request.StableHash(), RequestID: request.RequestID, WorkspaceID: workspace, Actor: request.Actor, Status: contracts.OperationStatusSucceeded, OutputHash: outputHash, ExternalEffects: []contracts.ExternalEffect{effect}}
		return InvocationResult{Result: result, ResponseHash: outputHash}, nil
	})
	var finalizerCalls atomic.Int32
	input.FinalizeTx = func(_ context.Context, _ pgx.Tx, resultHash string) error {
		if resultHash == "" {
			return errors.New("finalizer received no result hash")
		}
		finalizerCalls.Add(1)
		return injectedFinalizationFailure
	}
	if _, err := dispatcher.Dispatch(context.Background(), input); err == nil {
		t.Fatal("injected finalization failure unexpectedly committed")
	} else {
		var dispatchErr *ExternalDispatchError
		if errors.As(err, &dispatchErr) && !errors.Is(dispatchErr.Err, injectedFinalizationFailure) {
			t.Fatalf("dispatch failed before the injected finalizer: %v (fixture cause: %v)", err, dispatchErr.Err)
		}
	}
	if finalizerCalls.Load() != 1 {
		t.Fatalf("specialized finalizer calls=%d want 1", finalizerCalls.Load())
	}

	state, err := dispatcher.Admission.GetEffectState(context.Background(), workspace, effectID)
	if err != nil {
		t.Fatal(err)
	}
	if state.State != contracts.ExternalEffectAcknowledged {
		t.Fatalf("effect terminal state escaped rollback: %s", state.State)
	}
	link, err := dispatcher.Links.GetByDomain(context.Background(), workspace, input.DomainLink.DomainKind, input.DomainLink.DomainID, input.DomainLink.LinkRole)
	if err != nil {
		t.Fatal(err)
	}
	if link.Status != contracts.DomainEffectLinkStatusLinked {
		t.Fatalf("domain-link finalization escaped rollback: %s", link.Status)
	}
	if _, err := operations.GetResult(context.Background(), workspace, request.ID); !errors.Is(err, store.ErrOperationResultNotFound) {
		t.Fatalf("operation result escaped rollback: %v", err)
	}
	current, err := operations.Get(context.Background(), workspace, request.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != contracts.OperationStatusRunning {
		t.Fatalf("operation result transition escaped rollback: status=%s", current.Status)
	}
	if _, err := dispatcher.Dispatch(context.Background(), input); !errors.Is(err, ErrOperationResultRecoveryRequired) {
		t.Fatalf("retry did not expose the acknowledged result gap: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("lost in-memory result caused external reinvocation: calls=%d", calls.Load())
	}
}

func TestDispatcherLegacyTerminalEffectWithoutRequestedResultFailsClosed(t *testing.T) {
	dispatcher, operations, workspace, request, definition, admissionInput, pool := newDispatcherFixture(t)
	defer pool.Close()
	lease, err := prepareDispatcherOperation(operations, workspace, request)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	input := dispatcherRequest(workspace, request, definition, admissionInput, lease, func(_ context.Context, _ contracts.EffectAuthority) (InvocationResult, error) {
		calls.Add(1)
		effect := contracts.ExternalEffect{WorkspaceID: workspace, Boundary: "fixture.dispatch", Class: definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: request.IdempotencyKey, ProviderIdempotency: true, VerificationStatus: contracts.ExternalVerificationNotRequired, CompensationStatus: contracts.ExternalCompensationUnavailable}
		result := contracts.OperationResult{ID: request.ID + "-result", OperationID: request.ID, OperationHash: request.StableHash(), RequestID: request.RequestID, WorkspaceID: workspace, Actor: request.Actor, Status: contracts.OperationStatusSucceeded, OutputHash: contracts.HashStrings("legacy-result-gap"), ExternalEffects: []contracts.ExternalEffect{effect}}
		return InvocationResult{Result: result}, nil
	})
	legacyRequest := input
	legacyRequest.RecordResult = false
	first, err := dispatcher.Dispatch(context.Background(), legacyRequest)
	if err != nil || first.State.State != contracts.ExternalEffectVerified || first.OperationResult != nil {
		t.Fatalf("legacy terminal fixture dispatch=%+v err=%v", first, err)
	}
	if _, err := dispatcher.Dispatch(context.Background(), input); !errors.Is(err, ErrOperationResultRecoveryRequired) {
		t.Fatalf("terminal effect without result was reported as success: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("terminal duplicate re-invoked the external adapter: %d", calls.Load())
	}
}

func TestDispatcherConcurrentDuplicateDeliveryInvokesOnce(t *testing.T) {
	dispatcher, operations, workspace, request, definition, admissionInput, pool := newDispatcherFixture(t)
	defer pool.Close()
	lease, err := prepareDispatcherOperation(operations, workspace, request)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	invokerEntered := make(chan struct{})
	allowInvokerReturn := make(chan struct{})
	var releaseInvoker sync.Once
	allowInvocation := func() { releaseInvoker.Do(func() { close(allowInvokerReturn) }) }
	invoker := func(_ context.Context, _ contracts.EffectAuthority) (InvocationResult, error) {
		if calls.Add(1) == 1 {
			close(invokerEntered)
			<-allowInvokerReturn
		}
		effect := contracts.ExternalEffect{WorkspaceID: workspace, Boundary: "fixture.dispatch", Class: definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: request.IdempotencyKey, ProviderIdempotency: true, VerificationStatus: contracts.ExternalVerificationNotRequired, CompensationStatus: contracts.ExternalCompensationUnavailable}
		result := contracts.OperationResult{ID: request.ID + "-result", OperationID: request.ID, OperationHash: request.StableHash(), RequestID: request.RequestID, WorkspaceID: workspace, Actor: request.Actor, Status: contracts.OperationStatusSucceeded, OutputHash: contracts.HashStrings("concurrent-output"), ExternalEffects: []contracts.ExternalEffect{effect}}
		return InvocationResult{Result: result}, nil
	}
	input := dispatcherRequest(workspace, request, definition, admissionInput, lease, invoker)
	type dispatchResult struct {
		value Result
		err   error
	}
	firstResult := make(chan dispatchResult, 1)
	go func() {
		value, dispatchErr := dispatcher.Dispatch(context.Background(), input)
		firstResult <- dispatchResult{value: value, err: dispatchErr}
	}()
	select {
	case <-invokerEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("first delivery did not reach the external invocation boundary")
	}
	secondResult := make(chan dispatchResult, 1)
	go func() {
		value, dispatchErr := dispatcher.Dispatch(context.Background(), input)
		secondResult <- dispatchResult{value: value, err: dispatchErr}
	}()
	var second dispatchResult
	select {
	case second = <-secondResult:
	case <-time.After(5 * time.Second):
		allowInvocation()
		t.Fatal("overlapping duplicate delivery did not resolve while the first invocation was paused")
	}
	if !second.value.Duplicate || !errors.Is(second.err, ErrEffectDispatchInProgress) {
		allowInvocation()
		t.Fatalf("overlapping duplicate did not fail closed while result was not committed: %+v", second)
	}
	allowInvocation()
	first := <-firstResult
	if first.err != nil || first.value.OperationResult == nil {
		t.Fatalf("original dispatch failed after its invocation resumed: %+v", first)
	}
	if calls.Load() != 1 {
		t.Fatalf("concurrent duplicate delivery invoked %d times", calls.Load())
	}
	if _, err := operations.GetResult(context.Background(), workspace, request.ID); err != nil {
		t.Fatalf("committed dispatch did not persist its operation result: %v", err)
	}
}

func TestDispatcherDuplicateLosingReservedToDispatchingRaceFailsClosed(t *testing.T) {
	dispatcher, operations, workspace, request, definition, admissionInput, pool := newDispatcherFixture(t)
	defer pool.Close()
	lease, err := prepareDispatcherOperation(operations, workspace, request)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	invokerEntered := make(chan struct{})
	allowInvokerReturn := make(chan struct{})
	var releaseInvoker sync.Once
	allowInvocation := func() { releaseInvoker.Do(func() { close(allowInvokerReturn) }) }
	invoker := func(_ context.Context, _ contracts.EffectAuthority) (InvocationResult, error) {
		if calls.Add(1) != 1 {
			t.Fatal("duplicate reached the external invoker")
		}
		close(invokerEntered)
		<-allowInvokerReturn
		effect := contracts.ExternalEffect{WorkspaceID: workspace, Boundary: "fixture.dispatch", Class: definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: request.IdempotencyKey, ProviderIdempotency: true, VerificationStatus: contracts.ExternalVerificationNotRequired, CompensationStatus: contracts.ExternalCompensationUnavailable}
		result := contracts.OperationResult{ID: request.ID + "-result", OperationID: request.ID, OperationHash: request.StableHash(), RequestID: request.RequestID, WorkspaceID: workspace, Actor: request.Actor, Status: contracts.OperationStatusSucceeded, OutputHash: contracts.HashStrings("reserved-race-output"), ExternalEffects: []contracts.ExternalEffect{effect}}
		return InvocationResult{Result: result}, nil
	}
	input := dispatcherRequest(workspace, request, definition, admissionInput, lease, invoker)

	originalReady := make(chan struct{})
	duplicateReady := make(chan struct{})
	allowOriginal := make(chan struct{})
	allowDuplicate := make(chan struct{})
	var releaseOriginal, releaseDuplicate sync.Once
	dispatcher.beforeDispatchIntent = func(duplicate bool) {
		if duplicate {
			close(duplicateReady)
			<-allowDuplicate
			return
		}
		close(originalReady)
		<-allowOriginal
	}
	releaseOriginalGate := func() { releaseOriginal.Do(func() { close(allowOriginal) }) }
	releaseDuplicateGate := func() { releaseDuplicate.Do(func() { close(allowDuplicate) }) }
	defer func() {
		releaseOriginalGate()
		releaseDuplicateGate()
		allowInvocation()
	}()

	type dispatchResult struct {
		value Result
		err   error
	}
	originalResult := make(chan dispatchResult, 1)
	go func() {
		value, dispatchErr := dispatcher.Dispatch(context.Background(), input)
		originalResult <- dispatchResult{value: value, err: dispatchErr}
	}()
	select {
	case <-originalReady:
	case <-time.After(5 * time.Second):
		t.Fatal("original dispatch did not pause before dispatch intent")
	}
	duplicateResult := make(chan dispatchResult, 1)
	go func() {
		value, dispatchErr := dispatcher.Dispatch(context.Background(), input)
		duplicateResult <- dispatchResult{value: value, err: dispatchErr}
	}()
	select {
	case <-duplicateReady:
	case <-time.After(5 * time.Second):
		t.Fatal("duplicate did not read the reserved effect before dispatch intent")
	}

	// Let the original commit dispatching after the duplicate has observed the
	// reserved state, then allow the duplicate to exercise the idempotent
	// dispatch-intent transition against that now-live state.
	releaseOriginalGate()
	select {
	case <-invokerEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("original dispatch did not reach its external invocation")
	}
	releaseDuplicateGate()
	var duplicate dispatchResult
	select {
	case duplicate = <-duplicateResult:
	case <-time.After(5 * time.Second):
		t.Fatal("racing duplicate did not resolve")
	}
	if !duplicate.value.Duplicate || !errors.Is(duplicate.err, ErrEffectDispatchInProgress) || duplicate.value.OperationResult != nil {
		t.Fatalf("reserved-to-dispatching duplicate returned false success: %+v", duplicate)
	}
	allowInvocation()
	original := <-originalResult
	if original.err != nil || original.value.OperationResult == nil || calls.Load() != 1 {
		t.Fatalf("original delivery failed after duplicate race: result=%+v calls=%d", original, calls.Load())
	}
}

func TestDispatcherDuplicateFailsClosedWhenDomainLinkIsMissing(t *testing.T) {
	dispatcher, operations, workspace, request, definition, admissionInput, pool := newDispatcherFixture(t)
	defer pool.Close()
	lease, err := prepareDispatcherOperation(operations, workspace, request)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	input := dispatcherRequest(workspace, request, definition, admissionInput, lease, func(_ context.Context, _ contracts.EffectAuthority) (InvocationResult, error) {
		calls.Add(1)
		effect := contracts.ExternalEffect{WorkspaceID: workspace, Boundary: "fixture.dispatch", Class: definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: request.IdempotencyKey, ProviderIdempotency: true, VerificationStatus: contracts.ExternalVerificationNotRequired, CompensationStatus: contracts.ExternalCompensationUnavailable}
		result := contracts.OperationResult{ID: request.ID + "-result", OperationID: request.ID, OperationHash: request.StableHash(), RequestID: request.RequestID, WorkspaceID: workspace, Actor: request.Actor, Status: contracts.OperationStatusSucceeded, OutputHash: contracts.HashStrings("missing-link-output"), ExternalEffects: []contracts.ExternalEffect{effect}}
		return InvocationResult{Result: result}, nil
	})
	first, err := dispatcher.Dispatch(context.Background(), input)
	if err != nil || first.DomainLink == nil || first.OperationResult == nil {
		t.Fatalf("initial dispatch did not persist its domain link and result: %+v err=%v", first, err)
	}
	if _, err := pool.Exec(context.Background(), `DELETE FROM fornix.domain_effect_link_transitions WHERE workspace_id=$1 AND link_id=$2`, workspace, first.DomainLink.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `DELETE FROM fornix.domain_effect_links WHERE workspace_id=$1 AND id=$2`, workspace, first.DomainLink.ID); err != nil {
		t.Fatal(err)
	}
	duplicate, err := dispatcher.Dispatch(context.Background(), input)
	if !errors.Is(err, ErrDomainLinkRecoveryRequired) || !duplicate.Duplicate || duplicate.OperationResult != nil {
		t.Fatalf("missing domain link did not fail closed on replay: result=%+v err=%v", duplicate, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("domain-link lookup failure caused a second external call: %d", calls.Load())
	}
}

func TestDispatcherStaleFenceFailsBeforeInvoker(t *testing.T) {
	dispatcher, operations, workspace, request, definition, admissionInput, pool := newDispatcherFixture(t)
	defer pool.Close()
	first, err := operations.AcquireLease(context.Background(), workspace, request.ID, "worker-a", 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if _, err := operations.AcquireLease(context.Background(), workspace, request.ID, "worker-b", time.Minute); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	input := dispatcherRequest(workspace, request, definition, admissionInput, first.Lease, func(context.Context, contracts.EffectAuthority) (InvocationResult, error) {
		calls.Add(1)
		return InvocationResult{}, nil
	})
	if _, err := dispatcher.Dispatch(context.Background(), input); err == nil {
		t.Fatal("stale operation fence was accepted")
	}
	if calls.Load() != 0 {
		t.Fatal("stale worker reached the external invoker")
	}
}

func TestDispatcherStaleAgentRunFenceFailsBeforeInvoker(t *testing.T) {
	dispatcher, operations, workspace, request, definition, admissionInput, pool := newDispatcherFixture(t)
	defer pool.Close()
	operationLease, err := prepareDispatcherOperation(operations, workspace, request)
	if err != nil {
		t.Fatal(err)
	}

	agentRuns := store.NewAgentRunStore(pool, store.NewEventStore(pool))
	agentRun, _, err := agentRuns.Reserve(context.Background(), contracts.AgentRunRequest{
		RunID: "agent-run-stale-dispatch", RequestID: "agent-run-stale-request", IdempotencyKey: "agent-run-stale-key",
		WorkspaceID: workspace, Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspace},
		Goal: "exercise dispatch fencing", Provider: contracts.ProviderRef{Provider: "fake", Model: "fake-model"},
		Budget: contracts.AgentBudget{MaxTurns: 2, MaxModelSteps: 2, MaxToolCalls: 2, MaxContextBytes: 4096, MaxOutputTokens: 64, MaxWallTimeMS: 60_000, MaxCostUSD: 1, MaxToolAttempts: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.agent_run_worker_leases WHERE workspace_id=$1 AND run_id=$2`, workspace, agentRun.ID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.agent_runs WHERE workspace_id=$1 AND id=$2`, workspace, agentRun.ID)
	})
	firstOwner, err := agentRuns.AcquireAgentRunLease(context.Background(), workspace, agentRun.ID, "agent-worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	input := dispatcherRequest(workspace, request, definition, admissionInput, operationLease, func(context.Context, contracts.EffectAuthority) (InvocationResult, error) {
		calls.Add(1)
		return InvocationResult{}, nil
	})
	input.Authority.AgentRunID = agentRun.ID
	input.Authority.AgentRunOwnerID = firstOwner.Lease.OwnerID
	input.Authority.AgentRunFence = firstOwner.Lease.Fence

	// Pause after the effect is reserved but before the final authorization
	// transaction. Taking over during this window must make the atomic
	// authority+dispatch-intent operation fail without stranding dispatching.
	authorityCheckReached := make(chan struct{})
	continueDispatch := make(chan struct{})
	var releaseDispatch sync.Once
	unblockDispatch := func() { releaseDispatch.Do(func() { close(continueDispatch) }) }
	defer unblockDispatch()
	dispatcher.beforeDispatchIntent = func(bool) {
		close(authorityCheckReached)
		<-continueDispatch
	}
	type dispatchResult struct {
		value Result
		err   error
	}
	firstResult := make(chan dispatchResult, 1)
	go func() {
		value, dispatchErr := dispatcher.Dispatch(context.Background(), input)
		firstResult <- dispatchResult{value: value, err: dispatchErr}
	}()
	select {
	case <-authorityCheckReached:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch did not reach the final authorization boundary")
	}
	if _, err := pool.Exec(context.Background(), `UPDATE fornix.agent_run_worker_leases SET lease_until=clock_timestamp()-interval '1 second' WHERE workspace_id=$1 AND run_id=$2`, workspace, agentRun.ID); err != nil {
		unblockDispatch()
		t.Fatal(err)
	}
	secondOwner, err := agentRuns.AcquireAgentRunLease(context.Background(), workspace, agentRun.ID, "agent-worker-b", time.Minute)
	if err != nil || secondOwner.Lease.Fence <= firstOwner.Lease.Fence {
		unblockDispatch()
		t.Fatalf("agent-run takeover=%+v first=%+v err=%v", secondOwner.Lease, firstOwner.Lease, err)
	}
	unblockDispatch()
	staleResult := <-firstResult
	if !errors.Is(staleResult.err, store.ErrAgentRunLeaseFenced) {
		t.Fatalf("stale agent-run dispatch error=%v, want ErrAgentRunLeaseFenced", staleResult.err)
	}
	if calls.Load() != 0 {
		t.Fatalf("stale agent-run owner reached external invoker: calls=%d", calls.Load())
	}
	state, err := dispatcher.Admission.GetEffectState(context.Background(), workspace, staleResult.value.Effect.EffectID)
	if err != nil || state.State != contracts.ExternalEffectReserved {
		t.Fatalf("stale authorization stranded effect state=%+v err=%v; want reserved", state, err)
	}

	// The reservation is still safe to resume under the new live run owner.
	dispatcher.beforeDispatchIntent = nil
	input.Authority.AgentRunOwnerID = secondOwner.Lease.OwnerID
	input.Authority.AgentRunFence = secondOwner.Lease.Fence
	input.Invoker = func(_ context.Context, _ contracts.EffectAuthority) (InvocationResult, error) {
		calls.Add(1)
		effect := contracts.ExternalEffect{WorkspaceID: workspace, Boundary: "fixture.dispatch", Class: definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: request.IdempotencyKey, ProviderIdempotency: true, VerificationStatus: contracts.ExternalVerificationNotRequired, CompensationStatus: contracts.ExternalCompensationUnavailable}
		result := contracts.OperationResult{ID: request.ID + "-result", OperationID: request.ID, OperationHash: request.StableHash(), RequestID: request.RequestID, WorkspaceID: workspace, Actor: request.Actor, Status: contracts.OperationStatusSucceeded, OutputHash: contracts.HashStrings("fresh-agent-owner-result"), ExternalEffects: []contracts.ExternalEffect{effect}}
		return InvocationResult{Result: result}, nil
	}
	resumed, err := dispatcher.Dispatch(context.Background(), input)
	if err != nil || resumed.State.State != contracts.ExternalEffectVerified || calls.Load() != 1 {
		t.Fatalf("current owner could not safely resume stale pre-intent rejection: result=%+v calls=%d err=%v", resumed, calls.Load(), err)
	}
}

func TestDispatcherApprovalCannotBeSpoofedByTheInvoker(t *testing.T) {
	dispatcher, operations, workspace, request, definition, admissionInput, pool := newDispatcherFixture(t)
	defer pool.Close()
	lease, err := prepareDispatcherOperation(operations, workspace, request)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	input := dispatcherRequest(workspace, request, definition, admissionInput, lease, func(context.Context, contracts.EffectAuthority) (InvocationResult, error) {
		calls.Add(1)
		return InvocationResult{}, nil
	})
	input.Admission.Policy.EffectRules = []contracts.AdmissionEffectRule{{Effect: definition.Effect, Mode: contracts.PolicyApprovalRequired}}
	input.Admission.Policy.PolicyHash = ""
	if _, err := dispatcher.Dispatch(context.Background(), input); !errors.Is(err, ErrAdmissionNotAllowed) {
		t.Fatalf("pending approval error=%v, want ErrAdmissionNotAllowed", err)
	}
	if calls.Load() != 0 {
		t.Fatal("approval-pending effect reached the invoker")
	}
}

func TestDispatcherPreDispatchCrashLeavesRecoverableReservation(t *testing.T) {
	dispatcher, operations, workspace, request, definition, admissionInput, pool := newDispatcherFixture(t)
	defer pool.Close()
	lease, err := prepareDispatcherOperation(operations, workspace, request)
	if err != nil {
		t.Fatal(err)
	}
	dispatcher.Admission.SetFailureHook(func(point string) error {
		if point == "operation_effect_state_committed" {
			return errors.New("simulated crash before dispatch intent commit")
		}
		return nil
	})
	var calls atomic.Int32
	input := dispatcherRequest(workspace, request, definition, admissionInput, lease, func(_ context.Context, _ contracts.EffectAuthority) (InvocationResult, error) {
		calls.Add(1)
		effect := contracts.ExternalEffect{WorkspaceID: workspace, Boundary: "fixture.dispatch", Class: definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: request.IdempotencyKey, ProviderIdempotency: true, VerificationStatus: contracts.ExternalVerificationNotRequired, CompensationStatus: contracts.ExternalCompensationUnavailable}
		result := contracts.OperationResult{ID: request.ID + "-result", OperationID: request.ID, OperationHash: request.StableHash(), RequestID: request.RequestID, WorkspaceID: workspace, Actor: request.Actor, Status: contracts.OperationStatusSucceeded, OutputHash: contracts.HashStrings("recovery-output"), ExternalEffects: []contracts.ExternalEffect{effect}}
		return InvocationResult{Result: result}, nil
	})
	if _, err := dispatcher.Dispatch(context.Background(), input); err == nil {
		t.Fatal("injected pre-dispatch crash did not surface")
	}
	if calls.Load() != 0 {
		t.Fatal("pre-dispatch crash reached the external invoker")
	}
	dispatcher.Admission.SetFailureHook(nil)
	result, err := dispatcher.Dispatch(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || result.State.State != contracts.ExternalEffectVerified {
		t.Fatalf("reservation was not safely recoverable: result=%+v calls=%d", result, calls.Load())
	}
}

func TestDispatcherUncertainOutcomeIsNotBlindlyRetried(t *testing.T) {
	dispatcher, operations, workspace, request, definition, admissionInput, pool := newDispatcherFixture(t)
	defer pool.Close()
	lease, err := prepareDispatcherOperation(operations, workspace, request)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	input := dispatcherRequest(workspace, request, definition, admissionInput, lease, func(_ context.Context, _ contracts.EffectAuthority) (InvocationResult, error) {
		calls.Add(1)
		return InvocationResult{}, &ExternalDispatchError{Err: errors.New("provider response unavailable"), PossiblyStarted: true}
	})
	if _, err := dispatcher.Dispatch(context.Background(), input); !errors.Is(err, ErrUncertainOutcome) {
		t.Fatalf("uncertain outcome error=%v", err)
	}
	replay, err := dispatcher.Dispatch(context.Background(), input)
	if !errors.Is(err, ErrOperationResultRecoveryRequired) {
		t.Fatalf("uncertain duplicate did not report missing operation result: %v", err)
	}
	if !replay.Duplicate || replay.State.State != contracts.ExternalEffectRecoveryRequired || replay.DomainLink == nil || replay.DomainLink.Status != contracts.DomainEffectLinkStatusRecoveryRequired || calls.Load() != 1 {
		t.Fatalf("uncertain effect was retried: replay=%+v calls=%d", replay, calls.Load())
	}
}

func TestDispatcherBindsDomainEffectBeforeExternalInvocation(t *testing.T) {
	dispatcher, operations, workspace, request, definition, admission, _ := newDispatcherFixture(t)
	lease, err := prepareDispatcherOperation(operations, workspace, request)
	if err != nil {
		t.Fatal(err)
	}
	input := dispatcherRequest(workspace, request, definition, admission, lease, func(_ context.Context, authority contracts.EffectAuthority) (InvocationResult, error) {
		effect := contracts.ExternalEffect{SchemaVersion: contracts.DomainNeutralSchemaVersion, ID: authority.EffectID, WorkspaceID: workspace, Boundary: "fixture.dispatch", Class: definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: request.IdempotencyKey, ProviderIdempotency: true, VerificationStatus: contracts.ExternalVerificationNotRequired, CompensationStatus: contracts.ExternalCompensationUnavailable}
		result := contracts.OperationResult{ID: request.ID + "-result", OperationID: request.ID, OperationHash: request.StableHash(), RequestID: request.RequestID, WorkspaceID: workspace, Actor: request.Actor, Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: definition.OutputSchemaVersion, OutputSchemaHash: definition.OutputSchemaHash, OutputHash: contracts.HashStrings("domain-link-result"), ExternalEffects: []contracts.ExternalEffect{effect}}
		return InvocationResult{Result: result, ProviderRequestID: "fixture-provider-request"}, nil
	})
	input.DomainLink = &contracts.DomainEffectLink{DomainKind: contracts.DomainEffectKindHTTPRequest, DomainID: "fixture-request-1", DomainHash: contracts.HashStrings("fixture-domain-request"), LinkRole: contracts.DomainEffectLinkRolePrimary, IdempotencyKey: "fixture-domain-link"}
	first, err := dispatcher.Dispatch(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if first.DomainLink == nil || first.DomainLink.ID == "" || first.DomainLink.EffectID != first.Effect.EffectID {
		t.Fatalf("dispatcher did not return domain binding: %+v", first.DomainLink)
	}
	stored, err := dispatcher.Links.GetByDomain(context.Background(), workspace, contracts.DomainEffectKindHTTPRequest, "fixture-request-1", contracts.DomainEffectLinkRolePrimary)
	if err != nil {
		t.Fatal(err)
	}
	if stored.LinkHash != first.DomainLink.LinkHash || stored.OperationID != request.ID {
		t.Fatalf("stored binding mismatch: %+v", stored)
	}
	if stored.Status != contracts.DomainEffectLinkStatusReconciled || stored.ResultHash == "" || stored.ProviderRequestID != "fixture-provider-request" {
		t.Fatalf("successful dispatch did not reconcile domain link: %+v", stored)
	}
	second, err := dispatcher.Dispatch(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Duplicate {
		t.Fatalf("duplicate dispatch was not deduplicated: %+v", second)
	}
	if second.DomainLink == nil || second.DomainLink.Status != contracts.DomainEffectLinkStatusReconciled {
		t.Fatalf("duplicate dispatch did not replay reconciled link: %+v", second.DomainLink)
	}
	if second.OperationResult == nil || second.OperationResult.Record.ResultHash != first.OperationResult.Record.ResultHash {
		t.Fatalf("duplicate dispatch did not replay immutable result: first=%+v second=%+v", first.OperationResult, second.OperationResult)
	}
}

func TestChildRuntimeCreatesOneFencedEffectAndReplaysTerminalIdentity(t *testing.T) {
	dispatcher, operations, workspace, request, definition, _, pool := newDispatcherFixture(t)
	defer pool.Close()
	links := store.NewDomainEffectLinkStore(pool)
	runtime := &Runtime{Operations: operations, Admission: store.NewAdmissionStore(pool, store.NewEventStore(pool)), Links: links}
	definition.Ref.Name = "child-dispatch"
	definition.Ref.DefinitionHash = ""
	if err := definition.Normalize(); err != nil {
		t.Fatal(err)
	}
	target := request.Target
	policy := contracts.AdmissionPolicy{WorkspaceID: workspace, PolicyID: "child-runtime", Version: "1", EffectRules: []contracts.AdmissionEffectRule{{Effect: definition.Effect, Mode: contracts.PolicyApprovalAutomatic}}, AllowedConnectors: []contracts.ConnectorRef{definition.Ref.Connector}, AllowedResourceKinds: []string{target.Kind}, AllowedActorIDs: []string{request.Actor.ID}, MaxCostMicros: 100, MaxOperationsPerWindow: 10, MaxCostPerWindowMicros: 1000}
	var calls atomic.Int32
	inputHash := contracts.HashStrings("child-input")
	childRequestID := "child-request-1"
	child := ChildRequest{WorkspaceID: workspace, DomainKind: contracts.DomainEffectKindHTTPRequest, DomainID: "child-domain-1", DomainHash: inputHash, RequestID: childRequestID, IdempotencyKey: "child-runtime-1", Actor: request.Actor, Capability: definition, Target: target, InputType: "fixture.input", InputHash: inputHash, Policy: policy, Effect: contracts.ExternalEffect{Boundary: "fixture-child", Class: definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, ProviderIdempotency: true, VerificationStatus: contracts.ExternalVerificationNotRequired, CompensationStatus: contracts.ExternalCompensationUnavailable}, Invoker: func(_ context.Context, authority contracts.EffectAuthority) (InvocationResult, error) {
		calls.Add(1)
		effect := contracts.ExternalEffect{ID: authority.EffectID, WorkspaceID: workspace, Boundary: "fixture-child", Class: definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, ProviderIdempotency: true, VerificationStatus: contracts.ExternalVerificationNotRequired, CompensationStatus: contracts.ExternalCompensationUnavailable}
		outputHash := contracts.HashStrings("child-output")
		return InvocationResult{Result: contracts.OperationResult{ID: "child-result", OperationID: authority.OperationID, OperationHash: authority.RequestHash, RequestID: childRequestID, WorkspaceID: workspace, Actor: request.Actor, Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: definition.OutputSchemaVersion, OutputSchemaHash: definition.OutputSchemaHash, OutputHash: outputHash, Steps: []contracts.OperationStepResult{{StepID: "child-step-" + contracts.HashStrings(authority.OperationID)[:32], Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: definition.OutputSchemaVersion, OutputSchemaHash: definition.OutputSchemaHash, OutputHash: outputHash, ExternalEffect: &effect}}, ExternalEffects: []contracts.ExternalEffect{effect}}}, nil
	}}
	_ = dispatcher
	first, err := runtime.Run(context.Background(), child)
	if err != nil {
		t.Fatal(err)
	}
	if first.OperationResult == nil || calls.Load() != 1 {
		t.Fatalf("first child dispatch=%+v calls=%d", first, calls.Load())
	}
	second, err := runtime.Run(context.Background(), child)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Duplicate || calls.Load() != 1 {
		t.Fatalf("terminal child replay=%+v calls=%d", second, calls.Load())
	}
}

func prepareDispatcherOperation(operations *store.OperationStore, workspace string, request contracts.OperationRequest) (store.OperationLease, error) {
	lease, err := operations.AcquireLease(context.Background(), workspace, request.ID, request.Actor.ID, time.Minute)
	if err != nil {
		return store.OperationLease{}, err
	}
	for _, status := range []string{contracts.OperationStatusPlanned, contracts.OperationStatusAdmitted, contracts.OperationStatusRunning} {
		if _, err := operations.Transition(context.Background(), store.OperationTransitionInput{WorkspaceID: workspace, OperationID: request.ID, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: request.Actor, RequestID: "transition-" + status, IdempotencyKey: "transition-" + status, ToStatus: status}); err != nil {
			return store.OperationLease{}, err
		}
	}
	return lease.Lease, nil
}

func fatalDispatchWithFixtureCause(t *testing.T, err error) {
	t.Helper()
	var dispatchErr *ExternalDispatchError
	if errors.As(err, &dispatchErr) && dispatchErr.Err != nil {
		t.Fatalf("dispatch failed: %v (fixture cause: %v)", err, dispatchErr.Err)
	}
	t.Fatal(err)
}

func newDispatcherFixture(t *testing.T) (*Dispatcher, *store.OperationStore, string, contracts.OperationRequest, contracts.CapabilityDefinition, contracts.AdmissionInput, *pgxpool.Pool) {
	t.Helper()
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if err := store.ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	workspace := fmt.Sprintf("test-effect-dispatch-%d", time.Now().UnixNano())
	events := store.NewEventStore(pool)
	operations := store.NewOperationStore(pool, events)
	admission := store.NewAdmissionStore(pool, events)
	definition := dispatcherDefinition(workspace)
	request := dispatcherOperationRequest(workspace, definition)
	plan := contracts.OperationPlan{ID: request.ID + "-plan", WorkspaceID: workspace, OperationID: request.ID, Actor: request.Actor, Steps: []contracts.OperationStep{{ID: "dispatch-step", Ordinal: 0, Kind: "fixture.dispatch", Capability: request.Capability, Target: request.Target, Effect: definition.Effect, Profile: definition.Profile, InputHash: request.InputHash}}}
	created, err := operations.Create(ctx, store.OperationCreateInput{Request: request, Plan: &plan})
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	policy := contracts.AdmissionPolicy{WorkspaceID: workspace, PolicyID: "dispatcher-fixture", Version: "1", EffectRules: []contracts.AdmissionEffectRule{{Effect: definition.Effect, Mode: contracts.PolicyApprovalAutomatic}}, AllowedConnectors: []contracts.ConnectorRef{definition.Ref.Connector}, AllowedResourceKinds: []string{"record"}, AllowedActorIDs: []string{request.Actor.ID}, MaxCostMicros: 100, MaxOperationsPerWindow: 10, MaxCostPerWindowMicros: 1000}
	if err := policy.Normalize(); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	_ = created
	d := &Dispatcher{Operations: operations, Admission: admission, Links: store.NewDomainEffectLinkStore(pool)}
	input := contracts.AdmissionInput{WorkspaceID: workspace, OperationID: request.ID, OperationHash: created.Operation.OperationHash, RequestID: request.RequestID, IdempotencyKey: "dispatch-admission", Actor: request.Actor, Capability: definition, Target: request.Target, Policy: policy, ConnectorAvailable: true, ResourceAllowed: true, EvidenceSatisfied: true, TaskFenceValid: true, RequestedCostMicros: 1}
	// Store the policy on the request through a closure in dispatcherRequest;
	// the fixture uses the same deterministic key and input below.
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		for _, table := range []string{"domain_effect_link_transitions", "domain_effect_links", "operation_effect_transitions", "operation_effect_state", "operation_approval_transitions", "operation_approvals", "operation_results", "operation_authority_links", "operation_effects", "operation_attempts", "operation_transitions", "operation_leases", "operation_idempotency", "operation_steps", "operations", "control_events"} {
			_, _ = pool.Exec(cleanupCtx, "DELETE FROM fornix."+table+" WHERE workspace_id=$1", workspace)
		}
	})
	return d, operations, workspace, request, definition, input, pool
}

func dispatcherRequest(workspace string, request contracts.OperationRequest, definition contracts.CapabilityDefinition, admission contracts.AdmissionInput, lease store.OperationLease, invoker Invoker) Request {
	effect := contracts.ExternalEffect{WorkspaceID: workspace, Boundary: "fixture.dispatch", Class: definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: request.IdempotencyKey, ProviderIdempotency: true, VerificationStatus: contracts.ExternalVerificationNotRequired, CompensationStatus: contracts.ExternalCompensationUnavailable}
	return Request{Admission: admission, Operation: request, Definition: definition, OwnerID: lease.OwnerID, Fence: lease.Fence, StepID: "dispatch-step", AttemptKey: "dispatch-attempt", Effect: effect, DomainLink: &contracts.DomainEffectLink{DomainKind: contracts.DomainEffectKindHTTPRequest, DomainID: request.ID, DomainHash: request.StableHash(), LinkRole: contracts.DomainEffectLinkRolePrimary, IdempotencyKey: "fixture-domain-link"}, RecordResult: true, Invoker: invoker}
}

func dispatcherDefinition(workspace string) contracts.CapabilityDefinition {
	definition := contracts.CapabilityDefinition{WorkspaceID: workspace, Ref: contracts.CapabilityRef{WorkspaceID: workspace, Connector: contracts.ConnectorRef{WorkspaceID: workspace, Name: "fixture", Version: "1"}, Name: "dispatch", Version: "1"}, Description: "effect dispatcher fixture", InputSchemaVersion: 1, InputSchemaHash: contracts.HashStrings("input"), OutputSchemaVersion: 1, OutputSchemaHash: contracts.HashStrings("output"), Effect: contracts.EffectClassReversibleWrite, Profile: contracts.DefaultExecutionProfile(), RetryPolicy: contracts.CapabilityRetryPolicy{MaxAttempts: 1, BackoffMS: 1, MaxBackoffMS: 1, Jitter: "none"}, ResourceKinds: []string{"record"}, SupportsIdempotency: true, Enabled: true}
	return mustNormalizeDefinition(definition)
}

func dispatcherOperationRequest(workspace string, definition contracts.CapabilityDefinition) contracts.OperationRequest {
	request := contracts.OperationRequest{ID: "dispatch-operation", RequestID: "dispatch-request", IdempotencyKey: "dispatch-operation-key", WorkspaceID: workspace, Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspace}, Capability: definition.Ref, Target: contracts.ResourceRef{WorkspaceID: workspace, System: contracts.SystemRef{WorkspaceID: workspace, Type: "fixture", ID: "system", Version: "1"}, Kind: "record", ID: "record", Version: "1"}, InputType: "fixture.input", InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: contracts.HashStrings("input-value"), Profile: definition.Profile}
	if err := request.Normalize(); err != nil {
		panic(err)
	}
	return request
}

func mustNormalizeDefinition(definition contracts.CapabilityDefinition) contracts.CapabilityDefinition {
	if err := definition.Normalize(); err != nil {
		panic(err)
	}
	return definition
}
