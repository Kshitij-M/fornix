package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

func newWorkflowTestStore(t *testing.T) (*WorkflowStore, *pgxpool.Pool, string) {
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
	workspace := fmt.Sprintf("test-workflow-%d", time.Now().UnixNano())
	events := NewEventStore(pool)
	operations := NewOperationStore(pool, events)
	workflows := NewWorkflowStore(pool, events, operations)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.workflow_transitions WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.workflow_idempotency WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.workflow_step_states WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.workflow_runs WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_effects WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_attempts WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_callbacks WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_transitions WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_links WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_resources WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_steps WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_leases WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_idempotency WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operations WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.idempotency_records WHERE workspace_id=$1`, workspace)
		pool.Close()
	})
	return workflows, pool, workspace
}

func createWorkflowFixture(t *testing.T, store *WorkflowStore, workspace, key string) contracts.WorkflowRun {
	return createWorkflowFixtureWithBudget(t, store, workspace, key, contracts.DefaultWorkflowBudget())
}

func createWorkflowFixtureWithBudget(t *testing.T, store *WorkflowStore, workspace, key string, budget contracts.WorkflowBudget) contracts.WorkflowRun {
	t.Helper()
	request := operationTestRequest(t, workspace, key)
	request.ID = "workflow-" + key
	request.RequestID = "request-" + key
	plan := operationTestPlan(t, request)
	plan.ID = "plan-" + key
	result, err := store.Create(context.Background(), WorkflowCreateInput{Operation: OperationCreateInput{Request: request, Plan: &plan}, Budget: budget})
	if err != nil {
		t.Fatal(err)
	}
	if result.Duplicate || result.Run.Status != contracts.WorkflowStatusCreated || result.Run.StateHash == "" {
		t.Fatalf("unexpected workflow create result: %+v", result)
	}
	duplicate, err := store.Create(context.Background(), WorkflowCreateInput{Operation: OperationCreateInput{Request: request, Plan: &plan}, Budget: budget})
	if err != nil || !duplicate.Duplicate || duplicate.Run.ID != result.Run.ID {
		t.Fatalf("workflow create was not idempotent: duplicate=%+v err=%v", duplicate, err)
	}
	return result.Run
}

func TestWorkflowBudgetsFailClosed(t *testing.T) {
	store, _, workspace := newWorkflowTestStore(t)
	budget := contracts.DefaultWorkflowBudget()
	budget.MaxOutputBytes = 8
	budget.MaxWallTimeMS = 1
	run := createWorkflowFixtureWithBudget(t, store, workspace, "budget", budget)
	lease, err := store.AcquireLease(context.Background(), workspace, run.ID, "budget-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	if _, err := store.StartStep(context.Background(), WorkflowStepStartInput{WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: run.Actor, IdempotencyKey: "budget-start"}); !errors.Is(err, ErrWorkflowBudget) {
		t.Fatalf("expired wall budget error=%v, want ErrWorkflowBudget", err)
	}
	// A separate run keeps the wall clock budget open long enough to prove the
	// output ceiling is enforced at completion rather than by the executor.
	budget.MaxWallTimeMS = contracts.WorkflowMaxWallTimeMS
	run = createWorkflowFixtureWithBudget(t, store, workspace, "output-budget", budget)
	lease, err = store.AcquireLease(context.Background(), workspace, run.ID, "output-budget-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	started, err := store.StartStep(context.Background(), WorkflowStepStartInput{WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: run.Actor, IdempotencyKey: "output-budget-start"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.CompleteStep(context.Background(), WorkflowStepCompleteInput{WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", Attempt: started.Step.Attempt, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: run.Actor, IdempotencyKey: "output-budget-complete", Result: contracts.WorkflowStepResult{Status: contracts.WorkflowStepSucceeded, OutputHash: testHash("too-large"), OutputBytes: 9}}); !errors.Is(err, ErrWorkflowBudget) {
		t.Fatalf("output budget error=%v, want ErrWorkflowBudget", err)
	}
}

func TestWorkflowLifecycleCrashDuplicateReplayAndFence(t *testing.T) {
	store, _, workspace := newWorkflowTestStore(t)
	run := createWorkflowFixture(t, store, workspace, "lifecycle")
	lease, err := store.AcquireLease(context.Background(), workspace, run.ID, "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	start := WorkflowStepStartInput{WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: run.Actor, IdempotencyKey: "start-lifecycle"}
	store.SetFailureHook(func(point string) error {
		if point == "workflow_step_started" {
			return errors.New("injected workflow crash")
		}
		return nil
	})
	if _, err := store.StartStep(context.Background(), start); err == nil {
		t.Fatal("expected injected workflow crash")
	}
	store.SetFailureHook(nil)
	unchanged, err := store.Get(context.Background(), workspace, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.StateVersion != 0 || unchanged.Steps[0].Status != contracts.WorkflowStepPlanned {
		t.Fatalf("crash changed initial checkpoint: %+v", unchanged)
	}
	started, err := store.StartStep(context.Background(), start)
	if err != nil || started.Run.Status != contracts.WorkflowStatusRunning || started.Step.Attempt != 1 {
		t.Fatalf("start result=%+v err=%v", started, err)
	}
	duplicate, err := store.StartStep(context.Background(), start)
	if err != nil || !duplicate.Duplicate || duplicate.Step.Attempt != 1 {
		t.Fatalf("duplicate start=%+v err=%v", duplicate, err)
	}
	completed, err := store.CompleteStep(context.Background(), WorkflowStepCompleteInput{WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", Attempt: 1, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: run.Actor, IdempotencyKey: "complete-lifecycle", Result: contracts.WorkflowStepResult{Status: contracts.WorkflowStepSucceeded, OutputHash: testHash("workflow-output"), OutputBytes: 16}})
	if err != nil || completed.Run.Status != contracts.WorkflowStatusSucceeded {
		t.Fatalf("complete result=%+v err=%v", completed, err)
	}
	completeDuplicate, err := store.CompleteStep(context.Background(), WorkflowStepCompleteInput{WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", Attempt: 1, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: run.Actor, IdempotencyKey: "complete-lifecycle", Result: contracts.WorkflowStepResult{Status: contracts.WorkflowStepSucceeded, OutputHash: testHash("workflow-output"), OutputBytes: 16}})
	if err != nil || !completeDuplicate.Duplicate {
		t.Fatalf("duplicate complete=%+v err=%v", completeDuplicate, err)
	}
	replay, err := store.Replay(context.Background(), workspace, run.ID, 0, 100)
	if err != nil || !replay.Verified || replay.TransitionCount != 2 {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
	repeated, err := store.Replay(context.Background(), workspace, run.ID, 0, 100)
	if err != nil || repeated.ReplayHash != replay.ReplayHash {
		t.Fatalf("replay is not deterministic: first=%+v repeated=%+v err=%v", replay, repeated, err)
	}
	if _, err := store.StartStep(context.Background(), WorkflowStepStartInput{WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", OwnerID: "stale-worker", Fence: lease.Lease.Fence, Actor: run.Actor, IdempotencyKey: "stale"}); !errors.Is(err, ErrOperationLeaseOwned) {
		t.Fatalf("stale owner error=%v, want ErrOperationLeaseOwned", err)
	}
}

func TestWorkflowWaitingResumeCancellationAndWorkspaceIsolation(t *testing.T) {
	store, _, workspace := newWorkflowTestStore(t)
	run := createWorkflowFixture(t, store, workspace, "wait")
	lease, err := store.AcquireLease(context.Background(), workspace, run.ID, "worker-wait", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	started, err := store.StartStep(context.Background(), WorkflowStepStartInput{WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: run.Actor, IdempotencyKey: "start-wait"})
	if err != nil {
		t.Fatal(err)
	}
	waiting, err := store.CompleteStep(context.Background(), WorkflowStepCompleteInput{WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", Attempt: started.Step.Attempt, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: run.Actor, IdempotencyKey: "await-approval", Result: contracts.WorkflowStepResult{Status: contracts.WorkflowStepAwaitingApproval, Wait: &contracts.WorkflowWait{Kind: contracts.WorkflowWaitApproval, Token: "approval-token"}}})
	if err != nil || waiting.Run.Status != contracts.WorkflowStatusAwaitingApproval {
		t.Fatalf("waiting result=%+v err=%v", waiting, err)
	}
	resumed, err := store.CompleteStep(context.Background(), WorkflowStepCompleteInput{WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", Attempt: started.Step.Attempt, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: run.Actor, IdempotencyKey: "resume-approval", Result: contracts.WorkflowStepResult{Status: contracts.WorkflowStepSucceeded, OutputHash: testHash("approved")}})
	if err != nil || resumed.Run.Status != contracts.WorkflowStatusSucceeded {
		t.Fatalf("resume result=%+v err=%v", resumed, err)
	}
	other, err := store.Get(context.Background(), "other-workspace", run.ID)
	if !errors.Is(err, ErrWorkflowNotFound) || other.ID != "" {
		t.Fatalf("cross-workspace read=%+v err=%v", other, err)
	}
}

func TestWorkflowConcurrentDuplicateStartHasOneCommittedTransition(t *testing.T) {
	store, pool, workspace := newWorkflowTestStore(t)
	run := createWorkflowFixture(t, store, workspace, "concurrent")
	lease, err := store.AcquireLease(context.Background(), workspace, run.ID, "worker-concurrent", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	input := WorkflowStepStartInput{WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: run.Actor, IdempotencyKey: "start-concurrent"}
	const workers = 8
	results := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.StartStep(context.Background(), input)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent duplicate start failed: %v", err)
		}
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.workflow_transitions WHERE workspace_id=$1 AND run_id=$2`, workspace, run.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("workflow transition count=%d, want 1", count)
	}
}
