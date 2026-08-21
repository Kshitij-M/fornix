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

func newAgentRunTestStore(t *testing.T) (*AgentRunStore, *pgxpool.Pool, string) {
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
	workspace := fmt.Sprintf("test-agent-runs-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.agent_run_worker_leases WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.agent_runs WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.idempotency_records WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspace)
		pool.Close()
	})
	return NewAgentRunStore(pool, NewEventStore(pool)), pool, workspace
}

func durableAgentRequest(workspace, key string) contracts.AgentRunRequest {
	return contracts.AgentRunRequest{
		RunID: "run-" + key, RequestID: "request-" + key, IdempotencyKey: key,
		WorkspaceID: workspace, Actor: contracts.ActorRef{ID: "worker", Kind: "test"},
		Goal: "durable agent goal", Provider: contracts.ProviderRef{Provider: "fake", Model: "fake-model"},
		Budget: contracts.AgentBudget{MaxTurns: 2, MaxModelSteps: 2, MaxToolCalls: 2, MaxContextBytes: 4096, MaxOutputTokens: 64, MaxWallTimeMS: 60_000, MaxCostUSD: 1, MaxToolAttempts: 2},
	}
}

func TestAgentRunStoreCrashBeforeCommitLeavesCheckpointUnchanged(t *testing.T) {
	runs, _, workspace := newAgentRunTestStore(t)
	ctx := context.Background()
	run, deduplicated, err := runs.Reserve(ctx, durableAgentRequest(workspace, "crash-boundary"))
	if err != nil || deduplicated {
		t.Fatalf("reserve=%+v deduplicated=%t err=%v", run, deduplicated, err)
	}
	before := run
	runs.beforeCommit = func() error { return errors.New("injected crash before transaction commit") }
	next := run
	next.State, next.Phase = contracts.AgentRunRunning, contracts.AgentPhaseModel
	if _, err := runs.Commit(ctx, run, next, contracts.AgentEventCheckpointed, map[string]any{"run_id": run.ID, "test": "crash"}); err == nil {
		t.Fatal("injected crash unexpectedly committed")
	}
	runs.beforeCommit = nil
	afterCrash, err := runs.Get(ctx, workspace, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterCrash.StateVersion != before.StateVersion || afterCrash.State != before.State || afterCrash.EventSequence != before.EventSequence || afterCrash.StateHash != before.StateHash {
		t.Fatalf("crash changed authoritative checkpoint: before=%+v after=%+v", before, afterCrash)
	}
	committed, err := runs.Commit(ctx, afterCrash, next, contracts.AgentEventCheckpointed, map[string]any{"run_id": run.ID, "test": "recovery"})
	if err != nil {
		t.Fatal(err)
	}
	if committed.State != contracts.AgentRunRunning || committed.StateVersion != before.StateVersion+1 {
		t.Fatalf("recovery commit=%+v", committed)
	}
}

func TestAgentRunStoreConcurrentCommitUsesCompareAndSwapAndReplayIsRunScoped(t *testing.T) {
	runs, _, workspace := newAgentRunTestStore(t)
	ctx := context.Background()
	first, _, err := runs.Reserve(ctx, durableAgentRequest(workspace, "concurrent-a"))
	if err != nil {
		t.Fatal(err)
	}
	second, _, err := runs.Reserve(ctx, durableAgentRequest(workspace, "concurrent-b"))
	if err != nil {
		t.Fatal(err)
	}
	next := first
	next.State = contracts.AgentRunRunning
	var group sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_, commitErr := runs.Commit(ctx, first, next, contracts.AgentEventCheckpointed, map[string]any{"run_id": first.ID})
			results <- commitErr
		}()
	}
	group.Wait()
	close(results)
	committed, conflicts := 0, 0
	for commitErr := range results {
		if commitErr == nil {
			committed++
		} else if errors.Is(commitErr, ErrAgentRunConflict) {
			conflicts++
		} else {
			t.Fatalf("unexpected concurrent commit error: %v", commitErr)
		}
	}
	if committed != 1 || conflicts != 1 {
		t.Fatalf("compare-and-swap results committed=%d conflicts=%d", committed, conflicts)
	}
	events, err := runs.Replay(ctx, workspace, first.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].EventType != contracts.AgentEventCreated || events[1].EventType != contracts.AgentEventCheckpointed {
		t.Fatalf("run replay included wrong events: %+v", events)
	}
	replayed, err := runs.ReplayCheckpoint(ctx, workspace, first.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	current, err := runs.Get(ctx, workspace, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.StateHash != current.StateHash || replayed.HistoryHash != current.Checkpoint().HistoryHash || replayed.EventSequence != current.EventSequence {
		t.Fatalf("replayed checkpoint differs from incremental state: replay=%+v current=%+v", replayed, current.Checkpoint())
	}
	fromCheckpoint, err := runs.ReplayCheckpoint(ctx, workspace, first.ID, events[0].Sequence, 100)
	if err != nil || fromCheckpoint.StateHash != current.StateHash {
		t.Fatalf("checkpoint replay differs from zero replay: replay=%+v err=%v", fromCheckpoint, err)
	}
	otherEvents, err := runs.Replay(ctx, workspace, second.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(otherEvents) != 1 || otherEvents[0].EventType != contracts.AgentEventCreated {
		t.Fatalf("second run replay leaked first run events: %+v", otherEvents)
	}
}
