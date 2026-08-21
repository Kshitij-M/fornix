package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestAgentRunSchedulerClaimOrderingAndWorkspaceIsolation(t *testing.T) {
	runs, pool, workspace := newAgentRunTestStore(t)
	ctx := context.Background()
	first, _, err := runs.Reserve(ctx, durableAgentRequest(workspace, "queue-first"))
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := runs.Reserve(ctx, durableAgentRequest(workspace, "queue-second"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE fornix.agent_runs
		SET scheduler_priority=10, next_scheduled_at=clock_timestamp() - interval '1 second'
		WHERE workspace_id=$1 AND id=$2`, workspace, first.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE fornix.agent_runs
		SET scheduler_priority=0, next_scheduled_at=clock_timestamp() - interval '1 second'
		WHERE workspace_id=$1 AND id=$2`, workspace, second.ID); err != nil {
		t.Fatal(err)
	}
	claim, found, err := runs.ClaimNextAgentRun(ctx, workspace, "worker-a", time.Second)
	if err != nil || !found || claim.Run.ID != first.ID || claim.Lease.Fence != 1 {
		t.Fatalf("first claim=%+v found=%t err=%v", claim, found, err)
	}
	if _, found, err := runs.ClaimNextAgentRun(ctx, workspace, "worker-b", time.Second); err != nil || !found {
		t.Fatalf("second claim found=%t err=%v", found, err)
	}
	if _, found, err := runs.ClaimNextAgentRun(ctx, workspace, "worker-c", time.Second); err != nil || found {
		t.Fatalf("claimed an already-owned or empty queue: found=%t err=%v", found, err)
	}

	otherWorkspace := workspace + "-other"
	other, _, err := runs.Reserve(ctx, durableAgentRequest(otherWorkspace, "isolated"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.agent_run_worker_leases WHERE workspace_id=$1`, otherWorkspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.agent_runs WHERE workspace_id=$1`, otherWorkspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, otherWorkspace)
	})
	if _, found, err := runs.ClaimNextAgentRun(ctx, workspace, "worker-d", time.Second); err != nil || found {
		t.Fatalf("workspace claim leaked another workspace: found=%t err=%v", found, err)
	}
	otherClaim, found, err := runs.ClaimNextAgentRun(ctx, otherWorkspace, "worker-d", time.Second)
	if err != nil || !found || otherClaim.Run.ID != other.ID || otherClaim.Lease.WorkspaceID != otherWorkspace {
		t.Fatalf("other workspace claim=%+v found=%t err=%v", otherClaim, found, err)
	}
}

func TestAgentRunSchedulerExpiryTakeoverAndStaleWorkerFailsClosed(t *testing.T) {
	runs, pool, workspace := newAgentRunTestStore(t)
	ctx := context.Background()
	run, _, err := runs.Reserve(ctx, durableAgentRequest(workspace, "takeover"))
	if err != nil {
		t.Fatal(err)
	}
	first, found, err := runs.ClaimNextAgentRun(ctx, workspace, "worker-a", 20*time.Millisecond)
	if err != nil || !found {
		t.Fatalf("first claim=%+v found=%t err=%v", first, found, err)
	}
	time.Sleep(50 * time.Millisecond)
	second, found, err := runs.ClaimNextAgentRun(ctx, workspace, "worker-b", time.Second)
	if err != nil || !found || !second.Takeover || second.Lease.Fence != first.Lease.Fence+1 {
		t.Fatalf("takeover=%+v found=%t err=%v", second, found, err)
	}
	if err := runs.ValidateAgentRunLease(ctx, run, first.Lease); !errors.Is(err, ErrAgentRunLeaseFenced) {
		t.Fatalf("stale validation=%v, want fence error", err)
	}
	if _, err := runs.RenewAgentRunLease(ctx, first.Lease, time.Second); !errors.Is(err, ErrAgentRunLeaseFenced) {
		t.Fatalf("stale renewal=%v, want fence error", err)
	}
	if err := runs.ReleaseAgentRunLease(ctx, first.Lease); !errors.Is(err, ErrAgentRunLeaseFenced) {
		t.Fatalf("stale release=%v, want fence error", err)
	}
	if err := runs.ReleaseAgentRunLease(ctx, second.Lease); err != nil {
		t.Fatal(err)
	}
	if _, active, err := runs.GetAgentRunLease(ctx, workspace, run.ID); err != nil || active {
		t.Fatalf("released lease active=%t err=%v", active, err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM fornix.agent_run_worker_leases WHERE workspace_id=$1`, workspace); err != nil {
		t.Fatal(err)
	}
}

func TestAgentRunSchedulerConcurrentClaimsHaveOneOwnerPerRun(t *testing.T) {
	runs, _, workspace := newAgentRunTestStore(t)
	ctx := context.Background()
	run, _, err := runs.Reserve(ctx, durableAgentRequest(workspace, "concurrent-claim"))
	if err != nil {
		t.Fatal(err)
	}
	const workers = 12
	claims := make(chan contracts.AgentRunClaim, workers)
	errs := make(chan error, workers)
	var group sync.WaitGroup
	for i := 0; i < workers; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			claim, found, claimErr := runs.ClaimNextAgentRun(ctx, workspace, fmt.Sprintf("worker-%d", i), time.Second)
			if claimErr != nil {
				errs <- claimErr
				return
			}
			if found {
				claims <- claim
			}
		}(i)
	}
	group.Wait()
	close(claims)
	close(errs)
	var claimed int
	for claim := range claims {
		claimed++
		if claim.Run.ID != run.ID {
			t.Fatalf("claim crossed run identity: %+v", claim)
		}
	}
	for err := range errs {
		t.Fatalf("concurrent claim error: %v", err)
	}
	if claimed != 1 {
		t.Fatalf("claimed=%d want exactly one", claimed)
	}
}

func TestAgentRunSchedulerOwnedCheckpointRenewsAtomicallyAndRollsBack(t *testing.T) {
	runs, _, workspace := newAgentRunTestStore(t)
	ctx := context.Background()
	run, _, err := runs.Reserve(ctx, durableAgentRequest(workspace, "owned-commit"))
	if err != nil {
		t.Fatal(err)
	}
	claim, found, err := runs.ClaimNextAgentRun(ctx, workspace, "worker-a", time.Second)
	if err != nil || !found {
		t.Fatalf("claim=%+v found=%t err=%v", claim, found, err)
	}
	beforeLease, active, err := runs.GetAgentRunLease(ctx, workspace, run.ID)
	if err != nil || !active {
		t.Fatalf("before lease=%+v active=%t err=%v", beforeLease, active, err)
	}
	runs.beforeCommit = func() error { return errors.New("injected scheduler checkpoint crash") }
	next := run
	next.State, next.Phase = contracts.AgentRunRunning, contracts.AgentPhaseModel
	if _, err := runs.CommitOwned(ctx, run, next, contracts.AgentEventCheckpointed, map[string]any{"run_id": run.ID, "test": "owned-crash"}, claim.Lease); err == nil {
		t.Fatal("owned crash unexpectedly committed")
	}
	runs.beforeCommit = nil
	afterCrash, err := runs.Get(ctx, workspace, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterCrash.StateVersion != run.StateVersion || afterCrash.EventSequence != run.EventSequence || afterCrash.StateHash != run.StateHash {
		t.Fatalf("run changed across rolled-back owned commit: before=%+v after=%+v", run, afterCrash)
	}
	afterCrashLease, active, err := runs.GetAgentRunLease(ctx, workspace, run.ID)
	if err != nil || !active || !afterCrashLease.LeaseUntil.Equal(beforeLease.LeaseUntil) {
		t.Fatalf("lease changed across rolled-back owned commit: before=%+v after=%+v active=%t err=%v", beforeLease, afterCrashLease, active, err)
	}
	committed, err := runs.CommitOwned(ctx, afterCrash, next, contracts.AgentEventCheckpointed, map[string]any{"run_id": run.ID, "test": "owned-recovery"}, claim.Lease)
	if err != nil {
		t.Fatal(err)
	}
	if committed.State != contracts.AgentRunRunning || committed.StateVersion != run.StateVersion+1 {
		t.Fatalf("owned recovery=%+v", committed)
	}
	afterCommitLease, active, err := runs.GetAgentRunLease(ctx, workspace, run.ID)
	if err != nil || !active || !afterCommitLease.LeaseUntil.After(beforeLease.LeaseUntil) {
		t.Fatalf("checkpoint did not renew lease atomically: before=%+v after=%+v active=%t err=%v", beforeLease, afterCommitLease, active, err)
	}
}

func TestAgentRunSchedulerApprovalGateAndCancellation(t *testing.T) {
	runs, pool, workspace := newAgentRunTestStore(t)
	ctx := context.Background()
	run, _, err := runs.Reserve(ctx, durableAgentRequest(workspace, "approval-gate"))
	if err != nil {
		t.Fatal(err)
	}
	approvalID := "approval-" + run.ID
	toolRunID := "tool-run-" + run.ID
	if _, err := pool.Exec(ctx, `
		INSERT INTO fornix.tool_runs(
			id, workspace_id, request_id, idempotency_key, request_hash,
			tool_id, mode, status
		) VALUES($1,$2,$3,$4,$5,'fornix.echo','interactive','awaiting_approval')`,
		toolRunID, workspace, "tool-request-"+run.ID, "tool-key-"+run.ID, "tool-hash-"+run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		INSERT INTO fornix.tool_approvals(
			id, workspace_id, request_id, run_id, request_hash, tool_id,
			expires_at
		) VALUES($1,$2,$3,$4,$5,'fornix.echo',clock_timestamp()+interval '1 hour')`,
		approvalID, workspace, "tool-request-"+run.ID, toolRunID, "tool-hash-"+run.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
		UPDATE fornix.agent_runs
		SET state='awaiting_approval', phase='tool',
		    pending_tools=$3::jsonb,
		    next_scheduled_at=clock_timestamp()-interval '1 second'
		WHERE workspace_id=$1 AND id=$2`, workspace, run.ID,
		fmt.Sprintf(`[{"id":"call-1","tool_id":"fornix.echo","approval_id":%q,"arguments":"e30="}]`, approvalID)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.tool_approvals WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.tool_runs WHERE workspace_id=$1`, workspace)
	})
	if _, found, err := runs.ClaimNextAgentRun(ctx, workspace, "worker-before-approval", time.Second); err != nil || found {
		t.Fatalf("unapproved run was claimable: found=%t err=%v", found, err)
	}
	if _, err := pool.Exec(ctx, `UPDATE fornix.tool_approvals SET status='approved', decided_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2`, workspace, approvalID); err != nil {
		t.Fatal(err)
	}
	approved, found, err := runs.ClaimNextAgentRun(ctx, workspace, "worker-approved", time.Second)
	if err != nil || !found || approved.Run.ID != run.ID {
		t.Fatalf("approved run claim=%+v found=%t err=%v", approved, found, err)
	}
	if err := runs.ReleaseAgentRunLease(ctx, approved.Lease); err != nil {
		t.Fatal(err)
	}

	cancelWorkspace := workspace + "-cancel"
	cancelled, _, err := runs.Reserve(ctx, durableAgentRequest(cancelWorkspace, "cancelled-queue"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runs.Cancel(ctx, cancelled, "operator cancellation"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.agent_run_worker_leases WHERE workspace_id=$1`, cancelWorkspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.agent_runs WHERE workspace_id=$1`, cancelWorkspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, cancelWorkspace)
	})
	if _, found, err := runs.ClaimNextAgentRun(ctx, cancelWorkspace, "worker-cancelled", time.Second); err != nil || found {
		t.Fatalf("cancelled run was claimable: found=%t err=%v", found, err)
	}
}

func TestAgentRunSchedulerLatency(t *testing.T) {
	runs, _, workspace := newAgentRunTestStore(t)
	ctx := context.Background()
	if _, _, err := runs.Reserve(ctx, durableAgentRequest(workspace, "scheduler-latency")); err != nil {
		t.Fatal(err)
	}
	const samples = 20
	latencies := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		started := time.Now()
		claim, found, err := runs.ClaimNextAgentRun(ctx, workspace, fmt.Sprintf("latency-worker-%d", i), time.Second)
		if err != nil || !found {
			t.Fatalf("latency claim=%+v found=%t err=%v", claim, found, err)
		}
		latencies = append(latencies, time.Since(started))
		if err := runs.ReleaseAgentRunLease(ctx, claim.Lease); err != nil {
			t.Fatal(err)
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p95 := latencies[(len(latencies)*95+99)/100-1]
	t.Logf("agent-run scheduler claim latency samples=%d p50=%s p95=%s max=%s; claim SQL locks one run and one lease row, release SQL updates one lease row", samples, latencies[len(latencies)/2], p95, latencies[len(latencies)-1])
}
