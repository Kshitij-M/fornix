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

func newToolRunTestStore(t *testing.T) (*ToolRunStore, *pgxpool.Pool, string) {
	t.Helper()
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
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
	workspace := fmt.Sprintf("test-tools-%d", time.Now().UnixNano())
	store := NewToolRunStore(pool, NewEventStore(pool))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.tool_approvals WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.tool_runs WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.idempotency_records WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspace)
		pool.Close()
	})
	return store, pool, workspace
}

func durableToolRequest(workspace, key string) contracts.ToolRequest {
	return contracts.ToolRequest{WorkspaceID: workspace, RequestID: "request-" + key, IdempotencyKey: key, Actor: contracts.ActorRef{ID: "worker", Kind: "test"}, ToolID: "fornix.echo", Capability: "process.echo", Argv: []string{"/bin/echo", "stable"}, Budget: contracts.DefaultSandboxProfile()}
}

func TestToolRunStoreConcurrentReservationAndTerminalReplay(t *testing.T) {
	store, pool, workspace := newToolRunTestStore(t)
	request := durableToolRequest(workspace, "same-run")
	const workers = 10
	results := make(chan contracts.ToolRun, workers)
	errorsCh := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			run, _, err := store.Reserve(context.Background(), request, contracts.ToolModeAutomatic)
			if err != nil {
				errorsCh <- err
				return
			}
			results <- run
		}()
	}
	wg.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		t.Fatal(err)
	}
	var first contracts.ToolRun
	for run := range results {
		if first.ID == "" {
			first = run
		}
		if run.ID != first.ID {
			t.Fatalf("reservation returned multiple runs: %s and %s", first.ID, run.ID)
		}
	}
	started, err := store.MarkStarted(context.Background(), first)
	if err != nil {
		t.Fatal(err)
	}
	finished, err := store.Finish(context.Background(), started, contracts.ToolResult{Status: contracts.ToolRunSucceeded, Stdout: "stable"})
	if err != nil {
		t.Fatal(err)
	}
	replayed, existing, err := store.Reserve(context.Background(), request, contracts.ToolModeAutomatic)
	if err != nil || !existing || replayed.ID != finished.ID || replayed.Result == nil || replayed.Result.Stdout != "stable" {
		t.Fatalf("replayed=%+v existing=%t err=%v", replayed, existing, err)
	}
	var eventCount int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1 AND event_type LIKE 'tool.%'`, workspace).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 3 {
		t.Fatalf("tool event count=%d want 3", eventCount)
	}
	otherWorkspace := workspace + "-other"
	otherRequest := durableToolRequest(otherWorkspace, request.IdempotencyKey)
	otherRun, otherExisting, err := store.Reserve(context.Background(), otherRequest, contracts.ToolModeAutomatic)
	if err != nil || otherExisting || otherRun.ID == first.ID {
		t.Fatalf("workspace-scoped idempotency leaked: run=%+v existing=%t err=%v", otherRun, otherExisting, err)
	}
	defer func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM fornix.idempotency_records WHERE workspace_id=$1`, otherWorkspace)
		_, _ = pool.Exec(context.Background(), `DELETE FROM fornix.control_events WHERE workspace_id=$1`, otherWorkspace)
		_, _ = pool.Exec(context.Background(), `DELETE FROM fornix.tool_runs WHERE workspace_id=$1`, otherWorkspace)
	}()
}

func TestToolRunStoreInteractiveApprovalIsDurableAndAudited(t *testing.T) {
	store, pool, workspace := newToolRunTestStore(t)
	request := durableToolRequest(workspace, "approval-run")
	run, existing, err := store.Reserve(context.Background(), request, contracts.ToolModeInteractive)
	if err != nil || existing {
		t.Fatalf("reserve=%+v existing=%t err=%v", run, existing, err)
	}
	approval, err := store.CreateApproval(context.Background(), run, request, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if approval.Status != contracts.ApprovalPending {
		t.Fatalf("approval=%+v", approval)
	}
	decided, err := store.DecideApproval(context.Background(), contracts.ApprovalDecision{WorkspaceID: workspace, ApprovalID: approval.ID, Decision: contracts.ApprovalApproved, Actor: contracts.ActorRef{ID: "reviewer", Kind: "human"}, Reason: "safe test tool"})
	if err != nil {
		t.Fatal(err)
	}
	if decided.Status != contracts.ApprovalApproved || decided.DecidedAt == nil {
		t.Fatalf("decided=%+v", decided)
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1 AND event_type='tool.approval_decided'`, workspace).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("approval audit count=%d", count)
	}
}

func TestToolRunStoreTaskFenceRejectsStaleWorker(t *testing.T) {
	store, pool, workspace := newToolRunTestStore(t)
	_, err := pool.Exec(context.Background(), `INSERT INTO fornix.tasks(workspace_id,title,brief,created_by,status) VALUES($1,'fenced','fenced','test','claimed')`, workspace)
	if err != nil {
		t.Fatal(err)
	}
	var taskID int64
	if err := pool.QueryRow(context.Background(), `SELECT id FROM fornix.tasks WHERE workspace_id=$1`, workspace).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	_, err = pool.Exec(context.Background(), `INSERT INTO fornix.task_execution_leases(workspace_id,task_id,owner_id,fence,lease_until) VALUES($1,$2,'new-owner',2,clock_timestamp()+interval '1 minute')`, workspace, taskID)
	if err != nil {
		t.Fatal(err)
	}
	request := durableToolRequest(workspace, "stale-run")
	request.Task = &contracts.EntityRef{ID: fmt.Sprint(taskID), Kind: "task", WorkspaceID: workspace}
	request.TaskOwnerID, request.TaskFence = "old-owner", 1
	run, _, err := store.Reserve(context.Background(), request, contracts.ToolModeAutomatic)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkStarted(context.Background(), run); !errors.Is(err, ErrTaskLeaseFenced) {
		t.Fatalf("stale start error=%v", err)
	}
}
