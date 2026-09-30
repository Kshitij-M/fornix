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
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_authority_links WHERE workspace_id=$1`, workspace)
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

func TestWorkflowRetryDeadlineUsesEarliestPendingStep(t *testing.T) {
	now := time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC)
	later := now.Add(time.Minute)
	earlier := now.Add(-time.Minute)
	run := contracts.WorkflowRun{Steps: []contracts.WorkflowStepState{
		{StepID: "first", Status: contracts.WorkflowStepAwaitingRetry, NextRetryAt: &later},
		{StepID: "second", Status: contracts.WorkflowStepAwaitingRetry, NextRetryAt: &earlier},
		{StepID: "complete", Status: contracts.WorkflowStepSucceeded},
	}}
	if got := workflowRetryDeadline(run); got == nil || !got.Equal(earlier) {
		t.Fatalf("workflow retry deadline=%v, want earliest %s", got, earlier)
	}
	run.Steps[0].NextRetryAt = nil
	if got := workflowRetryDeadline(run); got != nil {
		t.Fatalf("immediately-eligible retry deadline=%v, want nil", got)
	}
}

func TestWorkflowStatusPreservesPendingWaits(t *testing.T) {
	completed := contracts.WorkflowStepState{Status: contracts.WorkflowStepSucceeded}
	laterDeadline := time.Date(2026, time.January, 2, 4, 4, 5, 0, time.UTC)
	earlierDeadline := laterDeadline.Add(-time.Minute)
	retryLater := contracts.WorkflowStepState{Ordinal: 1, StepID: "retry-later", Status: contracts.WorkflowStepAwaitingRetry, Wait: &contracts.WorkflowWait{Kind: contracts.WorkflowWaitRetry, Token: "later-token", ExpiresAt: &laterDeadline}, Failure: &contracts.WorkflowFailure{Code: "later_unavailable", Retryable: true}, NextRetryAt: &laterDeadline}
	retryEarlier := contracts.WorkflowStepState{Ordinal: 2, StepID: "retry-earlier", Status: contracts.WorkflowStepAwaitingRetry, Wait: &contracts.WorkflowWait{Kind: contracts.WorkflowWaitRetry, Token: "retry-token", ExpiresAt: &earlierDeadline}, Failure: &contracts.WorkflowFailure{Code: "temporarily_unavailable", Retryable: true}, NextRetryAt: &earlierDeadline}
	run := contracts.WorkflowRun{Steps: []contracts.WorkflowStepState{completed, retryLater, retryEarlier}}
	if got := workflowStatusAfterStep(run, contracts.WorkflowStepSucceeded); got != contracts.WorkflowStatusAwaitingRetry {
		t.Fatalf("run status after sibling retry wait=%q, want %q", got, contracts.WorkflowStatusAwaitingRetry)
	}
	if got := workflowRetryDeadline(run); got == nil || !got.Equal(earlierDeadline) {
		t.Fatalf("operation deadline=%v, want earliest retry %s", got, earlierDeadline)
	}
	wait, failure := workflowWaitDetailsForStatus(run, contracts.WorkflowStatusAwaitingRetry)
	if wait == nil || wait.Token != "retry-token" || wait.ExpiresAt == nil || !wait.ExpiresAt.Equal(earlierDeadline) || failure == nil || failure.Code != "temporarily_unavailable" {
		t.Fatalf("run-level retry wait details do not match pending step: wait=%+v failure=%+v", wait, failure)
	}
	retryImmediate := retryLater
	retryImmediate.NextRetryAt = nil
	retryImmediate.Wait = &contracts.WorkflowWait{Kind: contracts.WorkflowWaitRetry, Token: "later-token"}
	immediateRun := contracts.WorkflowRun{Steps: []contracts.WorkflowStepState{retryEarlier, retryImmediate}}
	wait, _ = workflowWaitDetailsForStatus(immediateRun, contracts.WorkflowStatusAwaitingRetry)
	if wait == nil || wait.Token != "later-token" {
		t.Fatalf("immediately eligible retry did not precede timed retry: %+v", wait)
	}
	approval := contracts.WorkflowStepState{Status: contracts.WorkflowStepAwaitingApproval, Wait: &contracts.WorkflowWait{Kind: contracts.WorkflowWaitApproval, Token: "approval-token"}}
	blocked := contracts.WorkflowRun{Steps: []contracts.WorkflowStepState{retryEarlier, approval}}
	if got := workflowStatusAfterStep(blocked, contracts.WorkflowStepSucceeded); got != contracts.WorkflowStatusAwaitingApproval {
		t.Fatalf("explicit approval wait was bypassed: run status=%q", got)
	}
	wait, _ = workflowWaitDetailsForStatus(blocked, contracts.WorkflowStatusAwaitingApproval)
	if wait == nil || wait.Token != "approval-token" {
		t.Fatalf("run-level approval wait does not match pending step: %+v", wait)
	}
	recovery := contracts.WorkflowStepState{Status: contracts.WorkflowStepRecoveryRequired}
	if got := workflowStatusAfterStep(contracts.WorkflowRun{Steps: []contracts.WorkflowStepState{retryEarlier, recovery}}, contracts.WorkflowStepSucceeded); got != contracts.WorkflowStatusRecoveryRequired {
		t.Fatalf("recovery wait was bypassed: run status=%q", got)
	}
}

func TestWorkflowRetryDeadlineProtectsQueueAndDirectStarts(t *testing.T) {
	workflows, _, workspace := newWorkflowTestStore(t)
	run := createWorkflowFixture(t, workflows, workspace, "retry-deadline")
	lease, err := workflows.AcquireLease(context.Background(), workspace, run.ID, "retry-worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	started, err := workflows.StartStep(context.Background(), WorkflowStepStartInput{
		WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", OwnerID: lease.Lease.OwnerID,
		Fence: lease.Lease.Fence, Actor: run.Actor, IdempotencyKey: "retry-deadline-first-start",
	})
	if err != nil {
		t.Fatalf("start initial attempt: %v", err)
	}
	var deadline time.Time
	if err := workflows.pool.QueryRow(context.Background(), `SELECT clock_timestamp() + interval '5 seconds'`).Scan(&deadline); err != nil {
		t.Fatal(err)
	}
	completed, err := workflows.CompleteStep(context.Background(), WorkflowStepCompleteInput{
		WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", Attempt: started.Step.Attempt,
		OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: run.Actor,
		IdempotencyKey: "retry-deadline-schedule",
		Result: contracts.WorkflowStepResult{
			Status:  contracts.WorkflowStepAwaitingRetry,
			Wait:    &contracts.WorkflowWait{Kind: contracts.WorkflowWaitRetry, Token: "retry-after-window", ExpiresAt: &deadline},
			Failure: &contracts.WorkflowFailure{Code: "temporary_unavailable", Retryable: true, Attempt: started.Step.Attempt},
		},
	})
	if err != nil {
		t.Fatalf("schedule retry: %v", err)
	}
	if completed.Run.Status != contracts.WorkflowStatusAwaitingRetry || completed.Step.NextRetryAt == nil {
		t.Fatalf("retry deadline missing from workflow checkpoint: run=%+v step=%+v", completed.Run, completed.Step)
	}
	operation, err := workflows.operations.Get(context.Background(), workspace, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	deadlineDifference := time.Duration(0)
	if operation.NextRetryAt != nil {
		deadlineDifference = operation.NextRetryAt.Sub(*completed.Step.NextRetryAt)
		if deadlineDifference < 0 {
			deadlineDifference = -deadlineDifference
		}
	}
	if operation.Status != contracts.OperationStatusAwaitingRetry || operation.NextRetryAt == nil || deadlineDifference > time.Microsecond {
		t.Fatalf("operation queue projection lost retry deadline: status=%s next_retry_at=%v workflow_deadline=%v", operation.Status, operation.NextRetryAt, completed.Step.NextRetryAt)
	}

	versionBefore := completed.Run.StateVersion
	var transitionsBefore int
	if err := workflows.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.workflow_transitions WHERE workspace_id=$1 AND run_id=$2`, workspace, run.ID).Scan(&transitionsBefore); err != nil {
		t.Fatal(err)
	}
	if _, err := workflows.StartStep(context.Background(), WorkflowStepStartInput{
		WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", OwnerID: lease.Lease.OwnerID,
		Fence: lease.Lease.Fence, Actor: run.Actor, IdempotencyKey: "retry-deadline-too-early",
	}); !errors.Is(err, ErrWorkflowRetryNotDue) {
		t.Fatalf("direct start before deadline error=%v, want ErrWorkflowRetryNotDue", err)
	}
	unchanged, err := workflows.Get(context.Background(), workspace, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.StateVersion != versionBefore || unchanged.Steps[0].Attempt != 1 || unchanged.Steps[0].Status != contracts.WorkflowStepAwaitingRetry {
		t.Fatalf("early retry attempt changed durable state: %+v", unchanged)
	}
	var transitionsAfter, earlyCommandCount int
	if err := workflows.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.workflow_transitions WHERE workspace_id=$1 AND run_id=$2`, workspace, run.ID).Scan(&transitionsAfter); err != nil {
		t.Fatal(err)
	}
	if err := workflows.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.workflow_idempotency WHERE workspace_id=$1 AND run_id=$2 AND idempotency_key='retry-deadline-too-early'`, workspace, run.ID).Scan(&earlyCommandCount); err != nil {
		t.Fatal(err)
	}
	if transitionsAfter != transitionsBefore || earlyCommandCount != 0 {
		t.Fatalf("early start appended durable history: transitions %d->%d, idempotency rows=%d", transitionsBefore, transitionsAfter, earlyCommandCount)
	}
	if err := workflows.ReleaseLease(context.Background(), lease.Lease); err != nil {
		t.Fatalf("release initial lease: %v", err)
	}
	claims, err := workflows.operations.ClaimReady(context.Background(), workspace, "retry-worker-b", 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 0 {
		t.Fatalf("operation queue claimed retry before deadline: %+v", claims)
	}
	var claimDeadline = time.Now().Add(10 * time.Second)
	for len(claims) == 0 && time.Now().Before(claimDeadline) {
		time.Sleep(100 * time.Millisecond)
		claims, err = workflows.operations.ClaimReady(context.Background(), workspace, "retry-worker-b", 1, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
	}
	if len(claims) != 1 || claims[0].Operation.ID != run.ID {
		t.Fatalf("due retry queue claims=%+v, want exactly workflow %s", claims, run.ID)
	}
	var claimedAt time.Time
	if err := workflows.pool.QueryRow(context.Background(), `SELECT clock_timestamp()`).Scan(&claimedAt); err != nil {
		t.Fatal(err)
	}
	if claimedAt.Before(*completed.Step.NextRetryAt) {
		t.Fatalf("operation queue claimed before the persisted retry deadline: claimed_at=%s retry_at=%s", claimedAt, completed.Step.NextRetryAt)
	}
	restarted, err := workflows.StartStep(context.Background(), WorkflowStepStartInput{
		WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", OwnerID: claims[0].Lease.OwnerID,
		Fence: claims[0].Lease.Fence, Actor: run.Actor, IdempotencyKey: "retry-deadline-second-start",
	})
	if err != nil {
		t.Fatalf("start due retry: %v", err)
	}
	if restarted.Step.Attempt != 2 || restarted.Step.Status != contracts.WorkflowStepRunning || restarted.Step.NextRetryAt != nil {
		t.Fatalf("due retry did not advance exactly once and clear its deadline: %+v", restarted.Step)
	}
}

func TestWorkflowRetryBudgetExhaustionClearsWait(t *testing.T) {
	workflows, _, workspace := newWorkflowTestStore(t)
	run := createWorkflowFixture(t, workflows, workspace, "retry-budget-exhaustion")
	lease, err := workflows.AcquireLease(context.Background(), workspace, run.ID, "retry-budget-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for expectedAttempt := 1; expectedAttempt <= run.Budget.MaxRetries+1; expectedAttempt++ {
		started, err := workflows.StartStep(context.Background(), WorkflowStepStartInput{
			WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", OwnerID: lease.Lease.OwnerID,
			Fence: lease.Lease.Fence, Actor: run.Actor, IdempotencyKey: fmt.Sprintf("retry-budget-start-%d", expectedAttempt),
		})
		if err != nil {
			t.Fatalf("start retry attempt %d: %v", expectedAttempt, err)
		}
		if started.Step.Attempt != expectedAttempt {
			t.Fatalf("attempt=%d, want %d", started.Step.Attempt, expectedAttempt)
		}
		var deadline time.Time
		if err := workflows.pool.QueryRow(context.Background(), `SELECT clock_timestamp() - interval '1 second'`).Scan(&deadline); err != nil {
			t.Fatal(err)
		}
		completed, err := workflows.CompleteStep(context.Background(), WorkflowStepCompleteInput{
			WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", Attempt: started.Step.Attempt,
			OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: run.Actor,
			IdempotencyKey: fmt.Sprintf("retry-budget-complete-%d", expectedAttempt),
			Result: contracts.WorkflowStepResult{
				Status:  contracts.WorkflowStepAwaitingRetry,
				Wait:    &contracts.WorkflowWait{Kind: contracts.WorkflowWaitRetry, Token: fmt.Sprintf("retry-budget-%d", expectedAttempt), ExpiresAt: &deadline},
				Failure: &contracts.WorkflowFailure{Code: "temporary_unavailable", Retryable: true, Attempt: started.Step.Attempt},
			},
		})
		if err != nil {
			t.Fatalf("complete retry attempt %d: %v", expectedAttempt, err)
		}
		if expectedAttempt < run.Budget.MaxRetries+1 {
			if completed.Run.Status != contracts.WorkflowStatusAwaitingRetry || completed.Step.Wait == nil || completed.Step.NextRetryAt == nil {
				t.Fatalf("retry attempt %d did not remain scheduled: run=%+v step=%+v", expectedAttempt, completed.Run, completed.Step)
			}
			continue
		}
		if completed.Run.Status != contracts.WorkflowStatusFailed || completed.Run.Wait != nil || completed.Run.Failure == nil || completed.Run.Failure.Code != "retry_budget_exhausted" {
			t.Fatalf("retry exhaustion did not produce a clean terminal failure: %+v", completed.Run)
		}
		if completed.Step.Status != contracts.WorkflowStepFailed || completed.Step.Wait != nil || completed.Step.NextRetryAt != nil || completed.Step.Failure == nil || completed.Step.Failure.Code != "retry_budget_exhausted" {
			t.Fatalf("exhausted step still advertises a retry: %+v", completed.Step)
		}
	}
	operation, err := workflows.operations.Get(context.Background(), workspace, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if operation.Status != contracts.OperationStatusFailed || operation.NextRetryAt != nil {
		t.Fatalf("terminal operation retained retry eligibility: status=%q next_retry_at=%v", operation.Status, operation.NextRetryAt)
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
