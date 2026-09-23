package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestOperationLifecycleGraphIsExplicitAndTerminal(t *testing.T) {
	legal := [][2]string{
		{contracts.OperationStatusCreated, contracts.OperationStatusPlanned},
		{contracts.OperationStatusPlanned, contracts.OperationStatusAdmitted},
		{contracts.OperationStatusAdmitted, contracts.OperationStatusRunning},
		{contracts.OperationStatusRunning, contracts.OperationStatusAwaitingExternal},
		{contracts.OperationStatusAwaitingExternal, contracts.OperationStatusVerifying},
		{contracts.OperationStatusVerifying, contracts.OperationStatusSucceeded},
	}
	for _, pair := range legal {
		if !contracts.CanTransitionOperation(pair[0], pair[1]) {
			t.Fatalf("transition %s -> %s was rejected", pair[0], pair[1])
		}
	}
	for _, pair := range [][2]string{
		{contracts.OperationStatusCreated, contracts.OperationStatusRunning},
		{contracts.OperationStatusSucceeded, contracts.OperationStatusRunning},
		{contracts.OperationStatusFailed, contracts.OperationStatusSucceeded},
		{"unknown", contracts.OperationStatusRunning},
	} {
		if contracts.CanTransitionOperation(pair[0], pair[1]) {
			t.Fatalf("illegal transition %s -> %s was accepted", pair[0], pair[1])
		}
	}
	for _, status := range []string{contracts.OperationStatusSucceeded, contracts.OperationStatusFailed, contracts.OperationStatusCancelled, contracts.OperationStatusDeadLetter} {
		if !contracts.IsTerminalOperationStatus(status) {
			t.Fatalf("status %s is not terminal", status)
		}
	}
}

func TestOperationStateHashIsStableAndBounded(t *testing.T) {
	first, err := hashState(operationState{Status: contracts.OperationStatusRunning, StateVersion: 2, PlanHash: testHash("plan")})
	if err != nil {
		t.Fatal(err)
	}
	second, err := hashState(operationState{Status: contracts.OperationStatusRunning, StateVersion: 2, PlanHash: testHash("plan")})
	if err != nil {
		t.Fatal(err)
	}
	if first != second || !validHash(first) {
		t.Fatalf("state hash is not stable: %q %q", first, second)
	}
	if first == mustHashState(operationState{Status: contracts.OperationStatusRunning, StateVersion: 3, PlanHash: testHash("plan")}) {
		t.Fatal("state version did not contribute to state hash")
	}
}

func TestOperationRequestHashExcludesDeliveryIdentity(t *testing.T) {
	first := operationTestRequest(t, "unit-workspace", "key-a")
	first.ID, first.RequestID, first.CausationID, first.CorrelationID = "op-a", "request-a", "cause-a", "corr-a"
	second := first
	second.ID, second.RequestID, second.IdempotencyKey, second.CausationID, second.CorrelationID = "op-b", "request-b", "key-b", "cause-b", "corr-b"
	firstHash, err := first.CanonicalHash()
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := second.CanonicalHash()
	if err != nil {
		t.Fatal(err)
	}
	if firstHash != secondHash {
		t.Fatalf("delivery identity changed canonical hash: %s != %s", firstHash, secondHash)
	}
}

func TestOperationEventEscapesJSONPointerOperationIDs(t *testing.T) {
	request := operationTestRequest(t, "unit-workspace", "pointer-key")
	request.ID = "operation/with-token"
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	event, err := (&OperationStore{}).eventFor(request, request.StableHash(), contracts.OperationStatusCreated, 0, testHash("state"), "create")
	if err != nil {
		t.Fatal(err)
	}
	if len(event.StateDeltas) != 1 || event.StateDeltas[0].Path != "/operations/operation~1with-token/status" {
		t.Fatalf("operation path was not escaped: %+v", event.StateDeltas)
	}
}

func TestOperationIntegrationCreateDuplicateConflictAndLinks(t *testing.T) {
	store, _, workspace := newOperationTestStore(t)
	request := operationTestRequest(t, workspace, "create-key")
	plan := operationTestPlan(t, request)
	first, err := store.Create(context.Background(), OperationCreateInput{
		Request:   request,
		Plan:      &plan,
		Resources: []OperationResource{{WorkspaceID: workspace, ResourceKind: "record", ResourceID: "record-1", Role: "target"}},
		Links:     []OperationLink{{WorkspaceID: workspace, LinkKind: "evidence", SourceID: "evidence-1", SourceHash: testHash("evidence")}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first.Duplicate || first.Operation.Status != contracts.OperationStatusCreated || first.Event.Sequence == 0 {
		t.Fatalf("unexpected create result: %+v", first)
	}
	duplicate, err := store.Create(context.Background(), OperationCreateInput{Request: request, Plan: &plan})
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Duplicate || duplicate.Operation.ID != first.Operation.ID {
		t.Fatalf("duplicate create did not return original: %+v", duplicate)
	}
	conflicting := request
	conflicting.InputHash = testHash("different-input")
	if _, err := store.Create(context.Background(), OperationCreateInput{Request: conflicting}); !errors.Is(err, ErrOperationIdempotency) {
		t.Fatalf("conflicting create error=%v, want ErrOperationIdempotency", err)
	}
	var resourceCount, linkCount int
	if err := store.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.operation_resources WHERE workspace_id=$1 AND operation_id=$2`, workspace, first.Operation.ID).Scan(&resourceCount); err != nil {
		t.Fatal(err)
	}
	if err := store.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.operation_links WHERE workspace_id=$1 AND operation_id=$2`, workspace, first.Operation.ID).Scan(&linkCount); err != nil {
		t.Fatal(err)
	}
	if resourceCount != 1 || linkCount != 1 {
		t.Fatalf("children resources=%d links=%d", resourceCount, linkCount)
	}
}

func TestOperationIntegrationTransitionDuplicateReplayAndRollback(t *testing.T) {
	store, _, workspace := newOperationTestStore(t)
	created, err := store.Create(context.Background(), OperationCreateInput{Request: operationTestRequest(t, workspace, "transition-create"), Plan: func() *contracts.OperationPlan {
		value := operationTestPlan(t, operationTestRequest(t, workspace, "unused"))
		return &value
	}()})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireLease(context.Background(), workspace, created.Operation.ID, "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	transition := func(status, key string) OperationTransitionResult {
		t.Helper()
		result, transitionErr := store.Transition(context.Background(), OperationTransitionInput{WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, IdempotencyKey: key, ToStatus: status, ReasonCode: "test"})
		if transitionErr != nil {
			t.Fatal(transitionErr)
		}
		return result
	}
	transition(contracts.OperationStatusPlanned, "transition-planned")
	duplicate := transition(contracts.OperationStatusPlanned, "transition-planned")
	if !duplicate.Duplicate || duplicate.Transition.StateVersion != 1 {
		t.Fatalf("duplicate transition=%+v", duplicate)
	}
	if _, err := store.Transition(context.Background(), OperationTransitionInput{WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, IdempotencyKey: "illegal", ToStatus: contracts.OperationStatusSucceeded}); !errors.Is(err, ErrOperationTransition) {
		t.Fatalf("illegal transition error=%v", err)
	}
	store.SetFailureHook(func(point string) error {
		if point == "operation_transition_committed" {
			return errors.New("injected crash")
		}
		return nil
	})
	if _, err := store.Transition(context.Background(), OperationTransitionInput{WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, IdempotencyKey: "rollback-transition", ToStatus: contracts.OperationStatusAdmitted}); err == nil {
		t.Fatal("injected transition crash unexpectedly committed")
	}
	store.SetFailureHook(nil)
	if _, err := store.Transition(context.Background(), OperationTransitionInput{WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, IdempotencyKey: "after-rollback", ToStatus: contracts.OperationStatusAdmitted}); err != nil {
		t.Fatal(err)
	}
	current, err := store.Get(context.Background(), workspace, created.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != contracts.OperationStatusAdmitted || current.StateVersion != 2 {
		t.Fatalf("rollback changed operation: %+v", current)
	}
	replay, err := store.Replay(context.Background(), workspace, created.Operation.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !replay.Verified || replay.StateHash != current.StateHash || replay.TransitionCount != 2 {
		t.Fatalf("replay=%+v current=%+v", replay, current)
	}
	checkpoint, err := store.Replay(context.Background(), workspace, created.Operation.ID, 1, 100)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := store.Replay(context.Background(), workspace, created.Operation.ID, 1, 100)
	if err != nil || checkpoint.ReplayHash != repeated.ReplayHash || checkpoint.TransitionCount != 1 {
		t.Fatalf("checkpoint replay is not deterministic: first=%+v repeated=%+v err=%v", checkpoint, repeated, err)
	}
}

func TestOperationIntegrationConcurrentCreateHasOneEffect(t *testing.T) {
	store, _, workspace := newOperationTestStore(t)
	request := operationTestRequest(t, workspace, "concurrent-create")
	const workers = 12
	results := make(chan OperationCreateResult, workers)
	errorsCh := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := store.Create(context.Background(), OperationCreateInput{Request: request})
			if err != nil {
				errorsCh <- err
				return
			}
			results <- result
		}()
	}
	wg.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		t.Fatal(err)
	}
	var operationID string
	created, duplicates := 0, 0
	for result := range results {
		if operationID == "" {
			operationID = result.Operation.ID
		}
		if result.Operation.ID != operationID {
			t.Fatalf("concurrent create returned multiple IDs: %s %s", operationID, result.Operation.ID)
		}
		if result.Duplicate {
			duplicates++
		} else {
			created++
		}
	}
	if created != 1 || duplicates != workers-1 {
		t.Fatalf("created=%d duplicates=%d", created, duplicates)
	}
}

func TestOperationIntegrationStaleFenceAndWorkspaceIsolation(t *testing.T) {
	store, _, workspace := newOperationTestStore(t)
	created, err := store.Create(context.Background(), OperationCreateInput{Request: operationTestRequest(t, workspace, "lease-create")})
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.AcquireLease(context.Background(), workspace, created.Operation.ID, "worker-a", 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	second, err := store.AcquireLease(context.Background(), workspace, created.Operation.ID, "worker-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if second.Lease.Fence <= first.Lease.Fence {
		t.Fatalf("takeover did not fence old owner: first=%+v second=%+v", first, second)
	}
	_, err = store.Transition(context.Background(), OperationTransitionInput{WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: first.Lease.OwnerID, Fence: first.Lease.Fence, IdempotencyKey: "stale-transition", ToStatus: contracts.OperationStatusPlanned})
	if !errors.Is(err, ErrOperationLeaseFenced) {
		t.Fatalf("stale transition error=%v", err)
	}
	if _, err := store.Get(context.Background(), workspace+"-foreign", created.Operation.ID); !errors.Is(err, ErrOperationNotFound) {
		t.Fatalf("cross-workspace get error=%v", err)
	}
}

func TestOperationIntegrationCreateCrashRollsBack(t *testing.T) {
	store, _, workspace := newOperationTestStore(t)
	store.SetFailureHook(func(point string) error {
		if point == "operation_created" {
			return errors.New("injected crash")
		}
		return nil
	})
	request := operationTestRequest(t, workspace, "crash-create")
	if _, err := store.Create(context.Background(), OperationCreateInput{Request: request}); err == nil {
		t.Fatal("injected create crash unexpectedly committed")
	}
	store.SetFailureHook(nil)
	if _, err := store.Get(context.Background(), workspace, request.ID); !errors.Is(err, ErrOperationNotFound) {
		t.Fatalf("rolled-back operation lookup error=%v", err)
	}
	var count int
	if err := store.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.operation_idempotency WHERE workspace_id=$1 AND idempotency_key=$2`, workspace, request.IdempotencyKey).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("rolled-back idempotency rows=%d", count)
	}
}

func TestOperationIntegrationAttemptsEffectsAndCallbacksAreFencedAndIdempotent(t *testing.T) {
	store, _, workspace := newOperationTestStore(t)
	request := operationTestRequest(t, workspace, "attempt-create")
	plan := operationTestPlan(t, request)
	created, err := store.Create(context.Background(), OperationCreateInput{Request: request, Plan: &plan})
	if err != nil {
		t.Fatal(err)
	}
	firstLease, err := store.AcquireLease(context.Background(), workspace, created.Operation.ID, "worker-a", 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	attemptInput := OperationAttemptInput{
		WorkspaceID: workspace, OperationID: created.Operation.ID, StepID: "step-1", Attempt: 1,
		AttemptID: "attempt-a", OwnerID: firstLease.Lease.OwnerID, Fence: firstLease.Lease.Fence,
		RequestHash: testHash("attempt-request"), IdempotencyKey: "attempt-key",
	}
	attempt, createdAttempt, err := store.ReserveAttempt(context.Background(), attemptInput)
	if err != nil {
		t.Fatal(err)
	}
	if !createdAttempt || attempt.AttemptID != attemptInput.AttemptID {
		t.Fatalf("unexpected attempt reservation: %+v created=%v", attempt, createdAttempt)
	}
	duplicateInput := attemptInput
	duplicateInput.AttemptID = "attempt-delivery-retry"
	duplicate, createdDuplicate, err := store.ReserveAttempt(context.Background(), duplicateInput)
	if err != nil {
		t.Fatal(err)
	}
	if createdDuplicate || duplicate.AttemptID != attempt.AttemptID {
		t.Fatalf("duplicate attempt was not stable: %+v created=%v", duplicate, createdDuplicate)
	}

	effectInput := OperationEffectInput{
		WorkspaceID: workspace, OperationID: created.Operation.ID, StepID: "step-1", AttemptID: attempt.AttemptID,
		OwnerID: firstLease.Lease.OwnerID, Fence: firstLease.Lease.Fence, RequestHash: testHash("effect-request"),
		Effect: contracts.ExternalEffect{
			WorkspaceID: workspace, Boundary: "fixture-api", Class: contracts.EffectClassReversibleWrite,
			DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, VerificationRequired: true,
			VerificationStatus: contracts.ExternalVerificationPending, CompensationStatus: contracts.ExternalCompensationAvailable,
		},
	}
	effect, createdEffect, err := store.ReserveEffect(context.Background(), effectInput)
	if err != nil {
		t.Fatal(err)
	}
	if !createdEffect || effect.EffectID == "" {
		t.Fatalf("unexpected effect reservation: %+v created=%v", effect, createdEffect)
	}
	effectInput.Effect.ID = effect.EffectID
	duplicateEffect, createdDuplicateEffect, err := store.ReserveEffect(context.Background(), effectInput)
	if err != nil {
		t.Fatal(err)
	}
	if createdDuplicateEffect || duplicateEffect.EffectID != effect.EffectID {
		t.Fatalf("duplicate effect was not stable: %+v created=%v", duplicateEffect, createdDuplicateEffect)
	}

	callbackInput := OperationCallbackInput{
		WorkspaceID: workspace, OperationID: created.Operation.ID, CallbackID: "callback-1", CallbackKind: "fixture.verify",
		OwnerID: firstLease.Lease.OwnerID, Fence: firstLease.Lease.Fence,
		ExternalID: "provider-1", RequestHash: testHash("callback-request"), ResponseHash: testHash("callback-response"), Status: "verified",
	}
	callback, createdCallback, err := store.RecordCallback(context.Background(), callbackInput)
	if err != nil {
		t.Fatal(err)
	}
	if !createdCallback || callback.CallbackID != callbackInput.CallbackID {
		t.Fatalf("unexpected callback: %+v created=%v", callback, createdCallback)
	}
	duplicateCallback, createdDuplicateCallback, err := store.RecordCallback(context.Background(), callbackInput)
	if err != nil {
		t.Fatal(err)
	}
	if createdDuplicateCallback || duplicateCallback.CallbackID != callback.CallbackID {
		t.Fatalf("duplicate callback was not stable: %+v created=%v", duplicateCallback, createdDuplicateCallback)
	}

	time.Sleep(50 * time.Millisecond)
	secondLease, err := store.AcquireLease(context.Background(), workspace, created.Operation.ID, "worker-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.ReserveAttempt(context.Background(), OperationAttemptInput{
		WorkspaceID: workspace, OperationID: created.Operation.ID, StepID: "step-1", Attempt: 2,
		AttemptID: "stale-attempt", OwnerID: firstLease.Lease.OwnerID, Fence: firstLease.Lease.Fence,
		RequestHash: testHash("stale-attempt"), IdempotencyKey: "stale-attempt-key",
	}); !errors.Is(err, ErrOperationLeaseFenced) {
		t.Fatalf("stale attempt error=%v, want ErrOperationLeaseFenced", err)
	}
	if secondLease.Lease.Fence <= firstLease.Lease.Fence {
		t.Fatalf("takeover did not advance fence: first=%+v second=%+v", firstLease.Lease, secondLease.Lease)
	}
}

func TestOperationIntegrationUsesLiveTaskFenceAndOwnerBinding(t *testing.T) {
	store, pool, workspace := newOperationTestStore(t)
	if _, err := pool.Exec(context.Background(), `INSERT INTO fornix.sessions(workspace_id,id,host) VALUES($1,'worker-a','operation-test')`, workspace); err != nil {
		t.Fatal(err)
	}
	var taskID int64
	if err := pool.QueryRow(context.Background(), `
		INSERT INTO fornix.tasks(workspace_id,title,brief,created_by,status,assigned_session,execution_fence)
		VALUES($1,'operation task','operation task fence test','test','claimed',$2,7)
		RETURNING id`, workspace, "worker-a").Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
		INSERT INTO fornix.task_execution_leases(workspace_id,task_id,owner_id,fence,lease_until)
		VALUES($1,$2,'worker-a',7,clock_timestamp()+interval '1 minute')`, workspace, taskID); err != nil {
		t.Fatal(err)
	}
	request := operationTestRequest(t, workspace, "task-bound-create")
	request.Task = &contracts.EntityRef{ID: fmt.Sprint(taskID), Kind: "task", WorkspaceID: workspace}
	plan := operationTestPlan(t, request)
	created, err := store.Create(context.Background(), OperationCreateInput{Request: request, Plan: &plan, TaskOwnerID: "worker-a", TaskFence: 7})
	if err != nil {
		t.Fatal(err)
	}
	lease, err := store.AcquireLease(context.Background(), workspace, created.Operation.ID, "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transition(context.Background(), OperationTransitionInput{
		WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: "worker-a", Fence: lease.Lease.Fence,
		TaskOwnerID: "worker-a", TaskFence: 7, IdempotencyKey: "task-bound-transition", ToStatus: contracts.OperationStatusPlanned,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `
		UPDATE fornix.task_execution_leases SET owner_id='worker-b', fence=8, lease_until=clock_timestamp()+interval '1 minute'
		WHERE workspace_id=$1 AND task_id=$2`, workspace, taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `INSERT INTO fornix.sessions(workspace_id,id,host) VALUES($1,'worker-b','operation-test')`, workspace); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE fornix.tasks SET assigned_session='worker-b', execution_fence=8 WHERE workspace_id=$1 AND id=$2`, workspace, taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Transition(context.Background(), OperationTransitionInput{
		WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: "worker-a", Fence: lease.Lease.Fence,
		TaskOwnerID: "worker-a", TaskFence: 7, IdempotencyKey: "stale-task-transition", ToStatus: contracts.OperationStatusAdmitted,
	}); !errors.Is(err, ErrOperationTaskFence) {
		t.Fatalf("stale task fence error=%v, want ErrOperationTaskFence", err)
	}
	if _, err := store.Transition(context.Background(), OperationTransitionInput{
		WorkspaceID: workspace, OperationID: created.Operation.ID, OwnerID: "worker-b", Fence: lease.Lease.Fence,
		TaskOwnerID: "worker-b", TaskFence: 8, IdempotencyKey: "wrong-operation-owner", ToStatus: contracts.OperationStatusAdmitted,
	}); !errors.Is(err, ErrOperationTaskFence) && !errors.Is(err, ErrOperationLeaseOwned) {
		t.Fatalf("mismatched operation owner error=%v", err)
	}
}

func operationTestRequest(t *testing.T, workspace, key string) contracts.OperationRequest {
	t.Helper()
	hash := testHash("shared")
	return contracts.OperationRequest{
		WorkspaceID: workspace, IdempotencyKey: key,
		Actor:      contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspace},
		Capability: contracts.CapabilityRef{WorkspaceID: workspace, Connector: contracts.ConnectorRef{WorkspaceID: workspace, Name: "fixture", Version: "1"}, Name: "read", Version: "1", DefinitionHash: hash},
		Target:     contracts.ResourceRef{WorkspaceID: workspace, System: contracts.SystemRef{WorkspaceID: workspace, Type: "fixture", ID: "system", Version: "1"}, Kind: "record", ID: "record-1", Version: "1"},
		InputType:  "fixture.input", InputSchemaVersion: 1, InputSchemaHash: hash, InputHash: hash, Profile: contracts.DefaultExecutionProfile(),
	}
}

func operationTestPlan(t *testing.T, request contracts.OperationRequest) contracts.OperationPlan {
	t.Helper()
	return contracts.OperationPlan{ID: "plan-1", WorkspaceID: request.WorkspaceID, Actor: request.Actor, Steps: []contracts.OperationStep{{ID: "step-1", Ordinal: 0, Kind: "read", Capability: request.Capability, Target: request.Target, Effect: contracts.EffectClassReadOnly, Profile: contracts.DefaultExecutionProfile(), InputHash: testHash("step-input")}}}
}

func newOperationTestStore(t *testing.T) (*OperationStore, *pgxpool.Pool, string) {
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
	if err := ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	workspace := fmt.Sprintf("test-operations-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operations WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.task_execution_leases WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.tasks WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.idempotency_records WHERE workspace_id=$1`, workspace)
		pool.Close()
	})
	return NewOperationStore(pool, NewEventStore(pool)), pool, workspace
}

func testHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func mustHashState(state operationState) string {
	hash, err := hashState(state)
	if err != nil {
		panic(err)
	}
	return hash
}
