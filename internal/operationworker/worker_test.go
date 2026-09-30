package operationworker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func TestWorkerHandlesFencedClaimWithHeartbeatAndReleasesLease(t *testing.T) {
	operationStore, pool, workspace := newWorkerTestStore(t)
	request := workerRequest(workspace, "worker-success")
	created, err := operationStore.Create(context.Background(), store.OperationCreateInput{Request: request})
	if err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int32
	handler := HandlerFunc(func(ctx context.Context, claim store.OperationClaim) error {
		calls.Add(1)
		plan := workerPlan(claim.Operation.Request)
		planned, err := operationStore.AttachPlan(ctx, store.OperationPlanInput{
			WorkspaceID: workspace, OperationID: claim.Operation.ID, OwnerID: claim.Lease.OwnerID,
			Fence: claim.Lease.Fence, Actor: claim.Operation.Request.Actor,
			IdempotencyKey: "worker-plan-" + claim.Operation.ID, Plan: plan,
		})
		if err != nil {
			return err
		}
		for index, status := range []string{contracts.OperationStatusAdmitted, contracts.OperationStatusRunning} {
			if _, err := operationStore.Transition(ctx, store.OperationTransitionInput{
				WorkspaceID: workspace, OperationID: claim.Operation.ID, OwnerID: claim.Lease.OwnerID,
				Fence: claim.Lease.Fence, Actor: claim.Operation.Request.Actor,
				IdempotencyKey: fmt.Sprintf("worker-transition-%s-%d", claim.Operation.ID, index), ToStatus: status,
				TaskOwnerID: planned.Operation.TaskOwnerID, TaskFence: planned.Operation.TaskFence,
			}); err != nil {
				return err
			}
		}
		// Longer than one heartbeat interval: success proves the worker renewed
		// the same fence instead of relying on a process-local lease.
		select {
		case <-time.After(140 * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
		result := contracts.OperationResult{
			ID:          claim.Operation.ID + "-result",
			OperationID: claim.Operation.ID, OperationHash: claim.Operation.OperationHash,
			WorkspaceID: workspace, Actor: claim.Operation.Request.Actor,
			Status: contracts.OperationStatusSucceeded,
			Steps:  []contracts.OperationStepResult{{StepID: "step-1", Status: contracts.OperationStatusSucceeded, OutputHash: workerHash("output")}},
		}
		_, err = operationStore.RecordResult(ctx, store.OperationResultInput{
			WorkspaceID: workspace, OperationID: claim.Operation.ID, OwnerID: claim.Lease.OwnerID,
			Fence: claim.Lease.Fence, Actor: claim.Operation.Request.Actor,
			IdempotencyKey: "worker-result-" + claim.Operation.ID, Result: result,
		})
		return err
	})
	worker := New(operationStore, handler, "worker-success")
	worker.LeaseTTL = 300 * time.Millisecond
	worker.HeartbeatInterval = 40 * time.Millisecond
	worker.Limit = 1

	result, err := worker.RunOnce(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	if result.Claims != 1 || result.Handled != 1 || result.Released != 1 || calls.Load() != 1 {
		t.Fatalf("unexpected worker result: %+v calls=%d", result, calls.Load())
	}
	stored, err := operationStore.Get(context.Background(), workspace, created.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != contracts.OperationStatusSucceeded {
		t.Fatalf("operation status=%q, want succeeded", stored.Status)
	}
	second, err := worker.RunOnce(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	if second.Claims != 0 {
		t.Fatalf("released terminal operation was claimed again: %+v", second)
	}

	var released bool
	if err := pool.QueryRow(context.Background(), `SELECT released_at IS NOT NULL FROM fornix.operation_leases WHERE workspace_id=$1 AND operation_id=$2`, workspace, created.Operation.ID).Scan(&released); err != nil {
		t.Fatal(err)
	}
	if !released {
		t.Fatal("successful handler did not release its operation lease")
	}
}

func TestWorkerDoesNotHotLoopFailedHandlerAndExpiryIsRecoverable(t *testing.T) {
	operationStore, _, workspace := newWorkerTestStore(t)
	request := workerRequest(workspace, "worker-failure")
	created, err := operationStore.Create(context.Background(), store.OperationCreateInput{Request: request})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	worker := New(operationStore, HandlerFunc(func(context.Context, store.OperationClaim) error {
		calls.Add(1)
		return errors.New("bounded adapter failure")
	}), "worker-failure")
	worker.LeaseTTL = 80 * time.Millisecond
	worker.HeartbeatInterval = 20 * time.Millisecond

	first, err := worker.RunOnce(context.Background(), workspace)
	if err == nil || first.Claims != 1 || first.Failed != 1 || calls.Load() != 1 {
		t.Fatalf("failed handler result=%+v err=%v calls=%d", first, err, calls.Load())
	}
	second, err := worker.RunOnce(context.Background(), workspace)
	if err != nil {
		t.Fatal(err)
	}
	if second.Claims != 0 || calls.Load() != 1 {
		t.Fatalf("failed claim hot-looped: result=%+v calls=%d", second, calls.Load())
	}
	time.Sleep(120 * time.Millisecond)
	recovered, err := worker.RunOnce(context.Background(), workspace)
	if err == nil || recovered.Claims != 1 || calls.Load() != 2 {
		t.Fatalf("expired claim was not recoverable: result=%+v err=%v calls=%d", recovered, err, calls.Load())
	}
	if recovered.Outcomes[0].Fence <= first.Outcomes[0].Fence {
		t.Fatalf("takeover did not advance fence: first=%d recovered=%d", first.Outcomes[0].Fence, recovered.Outcomes[0].Fence)
	}
	_ = created
}

func TestWorkerConcurrentClaimHasOneHandler(t *testing.T) {
	operationStore, _, workspace := newWorkerTestStore(t)
	if _, err := operationStore.Create(context.Background(), store.OperationCreateInput{Request: workerRequest(workspace, "worker-concurrent")}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	finish := make(chan struct{})
	var calls atomic.Int32
	handler := HandlerFunc(func(ctx context.Context, claim store.OperationClaim) error {
		calls.Add(1)
		close(started)
		select {
		case <-finish:
			return errors.New("test recovery boundary")
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	first := New(operationStore, handler, "worker-concurrent-a")
	first.LeaseTTL = time.Second
	second := New(operationStore, handler, "worker-concurrent-b")
	second.LeaseTTL = time.Second
	firstResult := make(chan error, 1)
	go func() {
		_, err := first.RunOnce(context.Background(), workspace)
		firstResult <- err
	}()
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("first worker did not receive a claim")
	}
	secondResult, err := second.RunOnce(context.Background(), workspace)
	if err != nil || secondResult.Claims != 0 {
		t.Fatalf("concurrent worker claimed active operation: result=%+v err=%v", secondResult, err)
	}
	close(finish)
	if err := <-firstResult; err == nil {
		t.Fatal("failed test handler was reported as successful")
	}
	if calls.Load() != 1 {
		t.Fatalf("handler ran %d times, want one", calls.Load())
	}
}

func TestWorkerCancellationDoesNotReportFalseSuccess(t *testing.T) {
	operationStore, _, workspace := newWorkerTestStore(t)
	if _, err := operationStore.Create(context.Background(), store.OperationCreateInput{Request: workerRequest(workspace, "worker-cancel")}); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	worker := New(operationStore, HandlerFunc(func(ctx context.Context, _ store.OperationClaim) error {
		close(started)
		<-ctx.Done()
		return ctx.Err()
	}), "worker-cancel")
	worker.LeaseTTL = time.Second
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	type runResult struct {
		batch BatchResult
		err   error
	}
	resultCh := make(chan runResult, 1)
	go func() {
		batch, err := worker.RunOnce(ctx, workspace)
		resultCh <- runResult{batch: batch, err: err}
	}()
	select {
	case <-started:
		cancel()
	case <-time.After(2 * time.Second):
		t.Fatal("worker handler did not start")
	}
	result := <-resultCh
	if result.err == nil || result.batch.Claims != 1 || result.batch.Handled != 0 || result.batch.Released != 0 || result.batch.Failed != 1 {
		t.Fatalf("cancellation was reported as success: %+v", result)
	}
}

func workerRequest(workspace, key string) contracts.OperationRequest {
	hash := workerHash("shared")
	return contracts.OperationRequest{
		WorkspaceID: workspace, IdempotencyKey: key,
		Actor:      contracts.ActorRef{ID: "worker-actor", Kind: "worker", WorkspaceID: workspace},
		Capability: contracts.CapabilityRef{WorkspaceID: workspace, Connector: contracts.ConnectorRef{WorkspaceID: workspace, Name: "fixture", Version: "1"}, Name: "read", Version: "1", DefinitionHash: hash},
		Target:     contracts.ResourceRef{WorkspaceID: workspace, System: contracts.SystemRef{WorkspaceID: workspace, Type: "fixture", ID: "system", Version: "1"}, Kind: "record", ID: "record-1", Version: "1"},
		InputType:  "fixture.input", InputSchemaVersion: 1, InputSchemaHash: hash, InputHash: hash, Profile: contracts.DefaultExecutionProfile(),
	}
}

func workerPlan(request contracts.OperationRequest) contracts.OperationPlan {
	return contracts.OperationPlan{ID: "plan-1", WorkspaceID: request.WorkspaceID, Actor: request.Actor, Steps: []contracts.OperationStep{{ID: "step-1", Ordinal: 0, Kind: "read", Capability: request.Capability, Target: request.Target, Effect: contracts.EffectClassReadOnly, Profile: contracts.DefaultExecutionProfile(), InputHash: workerHash("step-input")}}}
}

func workerHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

func newWorkerTestStore(t *testing.T) (*store.OperationStore, *pgxpool.Pool, string) {
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
	workspace := fmt.Sprintf("test-operation-worker-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_authority_links WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operations WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.idempotency_records WHERE workspace_id=$1`, workspace)
		pool.Close()
	})
	return store.NewOperationStore(pool, store.NewEventStore(pool)), pool, workspace
}
