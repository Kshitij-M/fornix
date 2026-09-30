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

func TestToolEffectFinalizerUsesCallerTransactionAndStableResultIdentity(t *testing.T) {
	store, pool, workspace := newToolRunTestStore(t)
	ctx := context.Background()
	request := durableToolRequest(workspace, "effect-finalizer-transaction")
	run, duplicate, err := store.Reserve(ctx, request, contracts.ToolModeAutomatic)
	if err != nil || duplicate {
		t.Fatalf("reserve run=%+v duplicate=%t err=%v", run, duplicate, err)
	}
	run, err = store.MarkStarted(ctx, run)
	if err != nil || run.Status != contracts.ToolRunRunning {
		t.Fatalf("start run=%+v err=%v", run, err)
	}
	result := contracts.ToolResult{Status: contracts.ToolRunSucceeded, Stdout: "verified result"}
	resultHash := result.Hash()

	rolledBack, err := beginWorkspaceTx(ctx, pool, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.FinalizeEffectResultTx(ctx, rolledBack, run, result, resultHash); err != nil {
		_ = rolledBack.Rollback(ctx)
		t.Fatalf("stage tool finalization: %v", err)
	}
	inside, err := readToolRunTx(ctx, rolledBack, workspace, run.IdempotencyKey)
	if err != nil || inside.Status != contracts.ToolRunSucceeded || inside.Result == nil || inside.Result.ContentHash != resultHash {
		_ = rolledBack.Rollback(ctx)
		t.Fatalf("uncommitted tool result=%+v err=%v", inside, err)
	}
	if err := rolledBack.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	afterRollback, err := store.Get(ctx, workspace, run.IdempotencyKey)
	if err != nil || afterRollback.Status != contracts.ToolRunRunning || afterRollback.Result != nil {
		t.Fatalf("tool result escaped caller rollback: run=%+v err=%v", afterRollback, err)
	}

	committed, err := beginWorkspaceTx(ctx, pool, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.FinalizeEffectResultTx(ctx, committed, run, result, resultHash); err != nil {
		_ = committed.Rollback(ctx)
		t.Fatalf("stage canonical tool finalization: %v", err)
	}
	if err := committed.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	final, err := store.Get(ctx, workspace, run.IdempotencyKey)
	if err != nil || final.Status != contracts.ToolRunSucceeded || final.Result == nil || final.Result.ContentHash != resultHash {
		t.Fatalf("committed tool result=%+v err=%v", final, err)
	}
	duplicateTx, err := beginWorkspaceTx(ctx, pool, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.FinalizeEffectResultTx(ctx, duplicateTx, run, result, resultHash); err != nil {
		_ = duplicateTx.Rollback(ctx)
		t.Fatalf("exact terminal duplicate was rejected: %v", err)
	}
	if err := duplicateTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	conflictTx, err := beginWorkspaceTx(ctx, pool, workspace)
	if err != nil {
		t.Fatal(err)
	}
	conflicting := contracts.ToolResult{Status: contracts.ToolRunSucceeded, Stdout: "different result"}
	if err := store.FinalizeEffectResultTx(ctx, conflictTx, run, conflicting, conflicting.Hash()); !errors.Is(err, ErrToolResultHashConflict) {
		_ = conflictTx.Rollback(ctx)
		t.Fatalf("conflicting terminal result error=%v, want hash conflict", err)
	}
	_ = conflictTx.Rollback(ctx)
	var succeededEvents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1 AND event_type=$2`, workspace, contracts.ToolEventSucceeded).Scan(&succeededEvents); err != nil {
		t.Fatal(err)
	}
	if succeededEvents != 1 {
		t.Fatalf("tool success events=%d want 1", succeededEvents)
	}
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

func TestToolRunStoreAgentRunFenceRejectsStaleWorker(t *testing.T) {
	store, pool, workspace := newToolRunTestStore(t)
	ctx := context.Background()
	runs := NewAgentRunStore(pool, NewEventStore(pool))
	run, _, err := runs.Reserve(ctx, durableAgentRequest(workspace, "tool-agent-fence"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.agent_run_worker_leases WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.agent_runs WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspace)
	})
	first, err := runs.AcquireAgentRunLease(ctx, workspace, run.ID, "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	request := durableToolRequest(workspace, "tool-agent-fence-effect")
	request.AgentRun = &contracts.EntityRef{ID: run.ID, Kind: "agent_run", WorkspaceID: workspace}
	request.AgentRunOwnerID, request.AgentRunFence = first.Lease.OwnerID, first.Lease.Fence
	reserved, existing, err := store.Reserve(ctx, request, contracts.ToolModeAutomatic)
	if err != nil || existing {
		t.Fatalf("reserve=%+v existing=%t err=%v", reserved, existing, err)
	}
	if err := runs.ReleaseAgentRunLease(ctx, first.Lease); err != nil {
		t.Fatal(err)
	}
	second, err := runs.AcquireAgentRunLease(ctx, workspace, run.ID, "worker-b", time.Second)
	if err != nil || second.Lease.Fence <= first.Lease.Fence {
		t.Fatalf("takeover=%+v err=%v", second, err)
	}
	replayRequest := request
	replayRequest.AgentRunOwnerID, replayRequest.AgentRunFence = second.Lease.OwnerID, second.Lease.Fence
	replayed, duplicate, err := store.Reserve(ctx, replayRequest, contracts.ToolModeAutomatic)
	if err != nil || !duplicate || replayed.RequestHash != reserved.RequestHash {
		t.Fatalf("takeover duplicate was not replayable: %+v duplicate=%t err=%v", replayed, duplicate, err)
	}
	if _, err := store.MarkStarted(ctx, reserved); !errors.Is(err, ErrAgentRunLeaseFenced) {
		t.Fatalf("stale tool start error=%v", err)
	}
	recorded, err := store.Get(ctx, workspace, request.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	if recorded.Status != contracts.ToolRunPending || recorded.AgentRunFence != first.Lease.Fence {
		t.Fatalf("stale worker changed tool ledger: %+v", recorded)
	}
}
