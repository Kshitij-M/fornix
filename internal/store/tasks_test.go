package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

func newTaskTestStore(t *testing.T) (*TaskStore, *pgxpool.Pool, string) {
	t.Helper()
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create test pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping test database: %v", err)
	}
	if err := ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatalf("apply migrations: %v", err)
	}
	workspaceID := fmt.Sprintf("test-tasks-%d", time.Now().UnixNano())
	events := NewEventStore(pool)
	store := NewTaskStore(pool, events)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.task_dependencies WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.task_execution_leases WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.task_state_projections WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_checkpoints WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.idempotency_records WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.tasks WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.sessions WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspaceID)
		pool.Close()
	})
	return store, pool, workspaceID
}

func addTaskSession(t *testing.T, pool *pgxpool.Pool, workspaceID, sessionID string, capabilities ...string) {
	t.Helper()
	if capabilities == nil {
		capabilities = []string{}
	}
	_, err := pool.Exec(context.Background(), `
		INSERT INTO fornix.sessions(workspace_id, id, host, capabilities, status)
		VALUES($1,$2,'task-test',$3,'idle')`, workspaceID, sessionID, capabilities)
	if err != nil {
		t.Fatalf("insert task session: %v", err)
	}
}

func TestTaskClaimCompleteDuplicateAndLifecycleEvents(t *testing.T) {
	store, pool, workspaceID := newTaskTestStore(t)
	addTaskSession(t, pool, workspaceID, "worker-a", "root")
	task, created, err := store.Create(context.Background(), TaskCreateInput{WorkspaceID: workspaceID, Title: "build", Brief: "compile", CreatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if created.EventType != "task.created" || task.Status != contracts.TaskStatusPending {
		t.Fatalf("created task=%+v event=%s", task, created.EventType)
	}
	claimed, err := store.ClaimNext(context.Background(), TaskClaimInput{WorkspaceID: workspaceID, SessionID: "worker-a", LeaseTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Task.Status != contracts.TaskStatusClaimed || claimed.Lease.Fence != 1 {
		t.Fatalf("claim=%+v", claimed)
	}
	result, err := store.Complete(context.Background(), TaskOutcomeInput{
		WorkspaceID: workspaceID, TaskID: task.ID, OwnerID: "worker-a", Fence: claimed.Lease.Fence,
		Result: "ok", IdempotencyKey: "complete-1", ActorID: "worker-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Task.Status != contracts.TaskStatusDone || result.Deduped || result.Event.EventType != "task.completed" {
		t.Fatalf("complete=%+v", result)
	}
	duplicate, err := store.Complete(context.Background(), TaskOutcomeInput{
		WorkspaceID: workspaceID, TaskID: task.ID, OwnerID: "worker-a", Fence: claimed.Lease.Fence,
		Result: "ok", IdempotencyKey: "complete-1", ActorID: "worker-a",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Deduped || duplicate.Event.Sequence != result.Event.Sequence {
		t.Fatalf("duplicate=%+v original=%+v", duplicate, result)
	}
	var eventCount int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1 AND task_ref->>'id'=$2`, workspaceID, fmt.Sprint(task.ID)).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 3 {
		t.Fatalf("task event count=%d want 3", eventCount)
	}
}

func TestTaskClaimCanTargetCreatedTask(t *testing.T) {
	store, pool, workspaceID := newTaskTestStore(t)
	addTaskSession(t, pool, workspaceID, "target-worker")
	first, _, err := store.Create(context.Background(), TaskCreateInput{WorkspaceID: workspaceID, Title: "first", Brief: "first", CreatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}
	target, _, err := store.Create(context.Background(), TaskCreateInput{WorkspaceID: workspaceID, Title: "target", Brief: "target", CreatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimNext(context.Background(), TaskClaimInput{WorkspaceID: workspaceID, SessionID: "target-worker", TaskID: target.ID, LeaseTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if claimed.Task.ID != target.ID || claimed.Lease.TaskID != target.ID {
		t.Fatalf("targeted claim returned task=%d lease_task=%d, want %d", claimed.Task.ID, claimed.Lease.TaskID, target.ID)
	}
	untouched, err := store.Get(context.Background(), workspaceID, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if untouched.Status != contracts.TaskStatusPending {
		t.Fatalf("non-target task status=%q, want pending", untouched.Status)
	}
}

func TestTaskConcurrentClaimsHaveOneWinner(t *testing.T) {
	store, pool, workspaceID := newTaskTestStore(t)
	addTaskSession(t, pool, workspaceID, "worker-a")
	addTaskSession(t, pool, workspaceID, "worker-b")
	created, _, err := store.Create(context.Background(), TaskCreateInput{WorkspaceID: workspaceID, Title: "one", Brief: "one", CreatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}
	type outcome struct {
		result TaskClaimResult
		err    error
	}
	results := make(chan outcome, 2)
	var wg sync.WaitGroup
	for _, worker := range []string{"worker-a", "worker-b"} {
		wg.Add(1)
		go func(worker string) {
			defer wg.Done()
			result, err := store.ClaimNext(context.Background(), TaskClaimInput{WorkspaceID: workspaceID, SessionID: worker, LeaseTTL: time.Second})
			results <- outcome{result: result, err: err}
		}(worker)
	}
	wg.Wait()
	close(results)
	winners := 0
	for result := range results {
		if result.err == nil {
			winners++
			if result.result.Task.ID != created.ID || result.result.Lease.Fence != 1 {
				t.Fatalf("unexpected winner=%+v", result.result)
			}
		} else if !errors.Is(result.err, ErrTaskNoReady) {
			t.Fatalf("unexpected loser error=%v", result.err)
		}
	}
	if winners != 1 {
		t.Fatalf("winners=%d want 1", winners)
	}
}

func TestTaskDependencySchedulingAndWorkspaceIsolation(t *testing.T) {
	store, pool, workspaceID := newTaskTestStore(t)
	otherWorkspace := workspaceID + "-other"
	addTaskSession(t, pool, workspaceID, "worker-a", "root")
	addTaskSession(t, pool, workspaceID, "worker-b")
	addTaskSession(t, pool, otherWorkspace, "worker-other")
	addTaskSession(t, pool, otherWorkspace, "worker-a")
	defer func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM fornix.task_execution_leases WHERE workspace_id=$1`, otherWorkspace)
		_, _ = pool.Exec(context.Background(), `DELETE FROM fornix.tasks WHERE workspace_id=$1`, otherWorkspace)
		_, _ = pool.Exec(context.Background(), `DELETE FROM fornix.sessions WHERE workspace_id=$1`, otherWorkspace)
		_, _ = pool.Exec(context.Background(), `DELETE FROM fornix.control_events WHERE workspace_id=$1`, otherWorkspace)
	}()
	root, _, err := store.Create(context.Background(), TaskCreateInput{WorkspaceID: workspaceID, Title: "root", Brief: "root", CreatedBy: "test", RequiredCapabilities: []string{"root"}})
	if err != nil {
		t.Fatal(err)
	}
	child, _, err := store.Create(context.Background(), TaskCreateInput{WorkspaceID: workspaceID, Title: "child", Brief: "child", CreatedBy: "test", DependsOn: []int64{root.ID}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNext(context.Background(), TaskClaimInput{WorkspaceID: workspaceID, SessionID: "worker-b", LeaseTTL: time.Second}); !errors.Is(err, ErrTaskNoReady) {
		t.Fatalf("child claimed before dependency completion: %v", err)
	}
	rootClaim, err := store.ClaimNext(context.Background(), TaskClaimInput{WorkspaceID: workspaceID, SessionID: "worker-a", LeaseTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Complete(context.Background(), TaskOutcomeInput{WorkspaceID: workspaceID, TaskID: root.ID, OwnerID: "worker-a", Fence: rootClaim.Lease.Fence, Result: "root", ActorID: "worker-a"}); err != nil {
		t.Fatal(err)
	}
	childClaim, err := store.ClaimNext(context.Background(), TaskClaimInput{WorkspaceID: workspaceID, SessionID: "worker-b", LeaseTTL: time.Second})
	if err != nil || childClaim.Task.ID != child.ID {
		t.Fatalf("child claim=%+v err=%v", childClaim, err)
	}
	other, _, err := store.Create(context.Background(), TaskCreateInput{WorkspaceID: otherWorkspace, Title: "isolated", Brief: "isolated", CreatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Get(context.Background(), workspaceID, other.ID); !errors.Is(err, ErrTaskNotFound) {
		t.Fatalf("cross-workspace get error=%v", err)
	}
	listed, err := store.List(context.Background(), workspaceID, "", "", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	for _, task := range listed {
		if task.WorkspaceID != workspaceID {
			t.Fatalf("cross-workspace task leaked: %+v", task)
		}
	}
}

func TestTaskLeaseTakeoverRejectsStaleWorker(t *testing.T) {
	store, pool, workspaceID := newTaskTestStore(t)
	addTaskSession(t, pool, workspaceID, "worker-old")
	addTaskSession(t, pool, workspaceID, "worker-new")
	task, _, err := store.Create(context.Background(), TaskCreateInput{WorkspaceID: workspaceID, Title: "fenced", Brief: "fenced", CreatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}
	old, err := store.ClaimNext(context.Background(), TaskClaimInput{WorkspaceID: workspaceID, SessionID: "worker-old", LeaseTTL: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	newOwner, err := store.ClaimNext(context.Background(), TaskClaimInput{WorkspaceID: workspaceID, SessionID: "worker-new", LeaseTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	if newOwner.Lease.Fence <= old.Lease.Fence {
		t.Fatalf("new fence=%d old=%d", newOwner.Lease.Fence, old.Lease.Fence)
	}
	_, err = store.Complete(context.Background(), TaskOutcomeInput{WorkspaceID: workspaceID, TaskID: task.ID, OwnerID: "worker-old", Fence: old.Lease.Fence, Result: "stale"})
	if !errors.Is(err, ErrTaskLeaseFenced) {
		t.Fatalf("stale completion error=%v want fence error", err)
	}
	if _, err := store.Fail(context.Background(), TaskFailureInput{WorkspaceID: workspaceID, TaskID: task.ID, OwnerID: "worker-old", Fence: old.Lease.Fence, Error: "stale", FailureClass: contracts.FailurePermanent}); !errors.Is(err, ErrTaskLeaseFenced) {
		t.Fatalf("stale failure error=%v want fence error", err)
	}
	if _, err := store.Cancel(context.Background(), TaskCancelInput{WorkspaceID: workspaceID, TaskID: task.ID, OwnerID: "worker-old", Fence: old.Lease.Fence, Reason: "stale"}); !errors.Is(err, ErrTaskLeaseFenced) {
		t.Fatalf("stale cancellation error=%v want fence error", err)
	}
	if _, err := store.Renew(context.Background(), workspaceID, task.ID, "worker-old", old.Lease.Fence, time.Second, "worker-old"); !errors.Is(err, ErrTaskLeaseFenced) {
		t.Fatalf("stale renewal error=%v want fence error", err)
	}
}

func TestTaskRetryCancellationAndDeadLetter(t *testing.T) {
	store, pool, workspaceID := newTaskTestStore(t)
	addTaskSession(t, pool, workspaceID, "worker-a")
	retryTask, _, err := store.Create(context.Background(), TaskCreateInput{WorkspaceID: workspaceID, Title: "retry", Brief: "retry", CreatedBy: "test", MaxAttempts: 2})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimNext(context.Background(), TaskClaimInput{WorkspaceID: workspaceID, SessionID: "worker-a", LeaseTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	zero := time.Duration(0)
	transient := contracts.FailureTransient
	failure, err := store.Fail(context.Background(), TaskFailureInput{WorkspaceID: workspaceID, TaskID: retryTask.ID, OwnerID: "worker-a", Fence: claim.Lease.Fence, Error: "temporary", FailureClass: transient, RetryAfter: &zero, IdempotencyKey: "retry-1", ActorID: "worker-a"})
	if err != nil || !failure.RetryScheduled || failure.Event.EventType != "task.retry_scheduled" {
		t.Fatalf("retry failure=%+v err=%v", failure, err)
	}
	duplicateFailure, err := store.Fail(context.Background(), TaskFailureInput{WorkspaceID: workspaceID, TaskID: retryTask.ID, OwnerID: "worker-a", Fence: claim.Lease.Fence, Error: "temporary", FailureClass: transient, RetryAfter: &zero, IdempotencyKey: "retry-1", ActorID: "worker-a"})
	if err != nil || !duplicateFailure.Deduped || duplicateFailure.Event.Sequence != failure.Event.Sequence {
		t.Fatalf("duplicate failure=%+v err=%v", duplicateFailure, err)
	}
	claim, err = store.ClaimNext(context.Background(), TaskClaimInput{WorkspaceID: workspaceID, SessionID: "worker-a", LeaseTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	failure, err = store.Fail(context.Background(), TaskFailureInput{WorkspaceID: workspaceID, TaskID: retryTask.ID, OwnerID: "worker-a", Fence: claim.Lease.Fence, Error: "still broken", FailureClass: transient, ActorID: "worker-a"})
	if err != nil || failure.Task.Status != contracts.TaskStatusDeadLetter || failure.Event.EventType != "task.deadlettered" {
		t.Fatalf("deadletter failure=%+v err=%v", failure, err)
	}
	cancelTask, _, err := store.Create(context.Background(), TaskCreateInput{WorkspaceID: workspaceID, Title: "cancel", Brief: "cancel", CreatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}
	cancelled, err := store.Cancel(context.Background(), TaskCancelInput{WorkspaceID: workspaceID, TaskID: cancelTask.ID, Reason: "superseded", IdempotencyKey: "cancel-1"})
	if err != nil || cancelled.Task.Status != contracts.TaskStatusCancelled {
		t.Fatalf("cancel=%+v err=%v", cancelled, err)
	}
	duplicateCancel, err := store.Cancel(context.Background(), TaskCancelInput{WorkspaceID: workspaceID, TaskID: cancelTask.ID, Reason: "superseded", IdempotencyKey: "cancel-1"})
	if err != nil || !duplicateCancel.Deduped || duplicateCancel.Event.Sequence != cancelled.Event.Sequence {
		t.Fatalf("duplicate cancel=%+v err=%v", duplicateCancel, err)
	}
	if _, err := store.ClaimNext(context.Background(), TaskClaimInput{WorkspaceID: workspaceID, SessionID: "worker-a", LeaseTTL: time.Second}); !errors.Is(err, ErrTaskNoReady) {
		t.Fatalf("cancelled task was claimable: %v", err)
	}
	_ = pool
}

func TestTaskMutationRollbackPreservesStateAndHistory(t *testing.T) {
	store, pool, workspaceID := newTaskTestStore(t)
	addTaskSession(t, pool, workspaceID, "worker-a")
	store.beforeCommit = func() error { return errors.New("simulated crash before commit") }
	if _, _, err := store.Create(context.Background(), TaskCreateInput{WorkspaceID: workspaceID, Title: "rollback", Brief: "rollback", CreatedBy: "test"}); err == nil {
		t.Fatal("create unexpectedly committed through crash hook")
	}
	store.beforeCommit = nil
	task, _, err := store.Create(context.Background(), TaskCreateInput{WorkspaceID: workspaceID, Title: "rollback", Brief: "rollback", CreatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimNext(context.Background(), TaskClaimInput{WorkspaceID: workspaceID, SessionID: "worker-a", LeaseTTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	store.beforeCommit = func() error { return errors.New("simulated crash before commit") }
	if _, err := store.Complete(context.Background(), TaskOutcomeInput{WorkspaceID: workspaceID, TaskID: task.ID, OwnerID: "worker-a", Fence: claim.Lease.Fence, Result: "not committed", IdempotencyKey: "rollback-complete"}); err == nil {
		t.Fatal("completion unexpectedly committed through crash hook")
	}
	store.beforeCommit = nil
	current, err := store.Get(context.Background(), workspaceID, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != contracts.TaskStatusClaimed {
		t.Fatalf("rollback changed task status=%s", current.Status)
	}
	if _, err := store.Complete(context.Background(), TaskOutcomeInput{WorkspaceID: workspaceID, TaskID: task.ID, OwnerID: "worker-a", Fence: claim.Lease.Fence, Result: "committed", IdempotencyKey: "rollback-complete"}); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1 AND event_type='task.completed'`, workspaceID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("completed event count=%d want 1", count)
	}
}

func TestTaskExecutionLatencyAndStorageImpact(t *testing.T) {
	store, pool, workspaceID := newTaskTestStore(t)
	addTaskSession(t, pool, workspaceID, "worker-metrics")
	samples := make([]time.Duration, 0, 20)
	for i := 0; i < 20; i++ {
		task, _, err := store.Create(context.Background(), TaskCreateInput{WorkspaceID: workspaceID, Title: fmt.Sprintf("measure-%d", i), Brief: "measure", CreatedBy: "test"})
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		claim, err := store.ClaimNext(context.Background(), TaskClaimInput{WorkspaceID: workspaceID, SessionID: "worker-metrics", LeaseTTL: time.Second})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.Complete(context.Background(), TaskOutcomeInput{WorkspaceID: workspaceID, TaskID: task.ID, OwnerID: "worker-metrics", Fence: claim.Lease.Fence, Result: "ok"}); err != nil {
			t.Fatal(err)
		}
		samples = append(samples, time.Since(started))
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
	p95 := samples[(len(samples)*95+99)/100-1]
	var taskBytes, dependencyBytes, eventBytes int64
	if err := pool.QueryRow(context.Background(), `SELECT COALESCE(sum(pg_column_size(t)),0) FROM fornix.tasks t WHERE workspace_id=$1`, workspaceID).Scan(&taskBytes); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT COALESCE(sum(pg_column_size(d)),0) FROM fornix.task_dependencies d WHERE workspace_id=$1`, workspaceID).Scan(&dependencyBytes); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT COALESCE(sum(pg_column_size(e)),0) FROM fornix.control_events e WHERE workspace_id=$1`, workspaceID).Scan(&eventBytes); err != nil {
		t.Fatal(err)
	}
	t.Logf("task claim+complete latency samples=%d p50=%s p95=%s max=%s", len(samples), samples[len(samples)/2], p95, samples[len(samples)-1])
	t.Logf("workspace logical storage task_rows=%dB dependency_rows=%dB event_rows=%dB; transitions use bounded transactions with claim/read+lease/session/event update shapes", taskBytes, dependencyBytes, eventBytes)
}
