package projection

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func newProjectionTestStore(t *testing.T) (*store.EventStore, *pgxpool.Pool, string) {
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
	if err := store.ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatalf("apply migrations: %v", err)
	}
	workspaceID := fmt.Sprintf("test-projection-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.task_state_projections WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_checkpoints WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.idempotency_records WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspaceID)
		pool.Close()
	})
	return store.NewEventStore(pool), pool, workspaceID
}

func makeTaskEvent(t *testing.T, workspaceID, taskID, status, result, sessionID string) contracts.EventEnvelope {
	t.Helper()
	eventType := "task.progressed"
	if status == TaskProjectionStatusDone {
		eventType = "task.completed"
	} else if status == TaskProjectionStatusFailed {
		eventType = "task.failed"
	}
	payload, err := json.Marshal(map[string]any{"task_id": taskID, "status": status, "result": result})
	if err != nil {
		t.Fatalf("marshal task event payload: %v", err)
	}
	event, err := contracts.NewEvent(eventType, payload)
	if err != nil {
		t.Fatalf("create task event: %v", err)
	}
	event.Scope.WorkspaceID = workspaceID
	event.Task = &contracts.EntityRef{ID: taskID, Kind: "task", WorkspaceID: workspaceID}
	statusValue, _ := json.Marshal(status)
	resultValue, _ := json.Marshal(result)
	event.StateDeltas = []contracts.StateDelta{
		{Op: contracts.DeltaSet, Path: "/tasks/" + taskID + "/status", Value: statusValue},
		{Op: contracts.DeltaSet, Path: "/tasks/" + taskID + "/result", Value: resultValue},
	}
	if sessionID != "" {
		event.Session = &contracts.EntityRef{ID: sessionID, Kind: "session", WorkspaceID: workspaceID}
	}
	return event
}

func appendTaskEvents(t *testing.T, eventStore *store.EventStore, events ...contracts.EventEnvelope) []store.AppendResult {
	t.Helper()
	results := make([]store.AppendResult, 0, len(events))
	for _, event := range events {
		result, err := eventStore.Append(context.Background(), event)
		if err != nil {
			t.Fatalf("append task event: %v", err)
		}
		results = append(results, result)
	}
	return results
}

func snapshotHash(t *testing.T, pool *pgxpool.Pool, workspaceID string) string {
	t.Helper()
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin snapshot transaction: %v", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	hash, err := SnapshotHashTx(context.Background(), tx, workspaceID)
	if err != nil {
		t.Fatalf("read snapshot hash: %v", err)
	}
	return hash
}

func checkpoint(t *testing.T, eventStore *store.EventStore, workspaceID, consumerID string) uint64 {
	t.Helper()
	value, err := eventStore.Checkpoint(context.Background(), workspaceID, consumerID)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	return value
}

func runUntilCaughtUp(t *testing.T, runner *Runner, workspaceID string) []BatchResult {
	t.Helper()
	results := make([]BatchResult, 0)
	for {
		result, err := runner.RunBatch(context.Background(), workspaceID)
		if err != nil {
			t.Fatalf("run projection batch: %v", err)
		}
		results = append(results, result)
		if !result.HasMore {
			return results
		}
	}
}

func TestRunnerIncrementalAndRebuildHaveSameHash(t *testing.T) {
	eventStore, pool, workspaceID := newProjectionTestStore(t)
	events := []contracts.EventEnvelope{
		makeTaskEvent(t, workspaceID, "task-a", TaskProjectionStatusActive, "started", "session-a"),
		makeTaskEvent(t, workspaceID, "task-b", TaskProjectionStatusFailed, "failed", "session-b"),
		makeTaskEvent(t, workspaceID, "task-a", TaskProjectionStatusDone, "complete", "session-a"),
		makeTaskEvent(t, workspaceID, "task-c", TaskProjectionStatusActive, "working", "session-c"),
		makeTaskEvent(t, workspaceID, "task-b", TaskProjectionStatusDone, "recovered", "session-b"),
		makeTaskEvent(t, workspaceID, "task-c", TaskProjectionStatusDone, "complete", "session-c"),
		makeTaskEvent(t, workspaceID, "task-a", TaskProjectionStatusDone, "verified", "session-a"),
	}
	appended := appendTaskEvents(t, eventStore, events...)
	runner, err := NewRunner(eventStore, NewTaskProjection("projection.incremental", 2))
	if err != nil {
		t.Fatal(err)
	}
	batches := runUntilCaughtUp(t, runner, workspaceID)
	if got := batches[len(batches)-1].CheckpointAfter; got != appended[len(appended)-1].Event.Sequence {
		t.Fatalf("incremental checkpoint=%d want=%d", got, appended[len(appended)-1].Event.Sequence)
	}
	incrementalHash := snapshotHash(t, pool, workspaceID)
	rebuild, err := runner.Rebuild(context.Background(), workspaceID)
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if rebuild.EventsRead != len(events) || rebuild.EventsApplied != len(events) {
		t.Fatalf("rebuild counts=%+v want read/applied=%d", rebuild, len(events))
	}
	rebuiltHash := snapshotHash(t, pool, workspaceID)
	if rebuiltHash != incrementalHash {
		t.Fatalf("rebuild hash=%s incremental hash=%s", rebuiltHash, incrementalHash)
	}
	if got := checkpoint(t, eventStore, workspaceID, "projection.incremental"); got != appended[len(appended)-1].Event.Sequence {
		t.Fatalf("rebuild checkpoint=%d want=%d", got, appended[len(appended)-1].Event.Sequence)
	}
}

func TestRunnerPreCommitFailureRollsBackProjectionAndCheckpoint(t *testing.T) {
	eventStore, pool, workspaceID := newProjectionTestStore(t)
	event := makeTaskEvent(t, workspaceID, "task-crash", TaskProjectionStatusDone, "committed-later", "session-crash")
	appended := appendTaskEvents(t, eventStore, event)[0]
	consumerID := "projection.crash"
	runner, err := NewRunner(eventStore, NewTaskProjection(consumerID, 10))
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("simulated crash before commit")
	if _, err := runner.RunBatchWithHook(context.Background(), workspaceID, func(BatchResult) error {
		return injected
	}); !errors.Is(err, injected) {
		t.Fatalf("run error=%v want=%v", err, injected)
	}
	if got := checkpoint(t, eventStore, workspaceID, consumerID); got != 0 {
		t.Fatalf("checkpoint after rollback=%d want=0", got)
	}
	if got := snapshotHash(t, pool, workspaceID); got != emptySnapshotHash(t) {
		t.Fatalf("projection changed after rollback: %s", got)
	}
	if _, err := runner.RunBatch(context.Background(), workspaceID); err != nil {
		t.Fatalf("retry after rollback: %v", err)
	}
	if got := checkpoint(t, eventStore, workspaceID, consumerID); got != appended.Event.Sequence {
		t.Fatalf("checkpoint after retry=%d want=%d", got, appended.Event.Sequence)
	}
}

func TestRunnerCommittedBatchIsSafeToReplayAndDuplicateConsumer(t *testing.T) {
	eventStore, pool, workspaceID := newProjectionTestStore(t)
	event := appendTaskEvents(t, eventStore,
		makeTaskEvent(t, workspaceID, "task-duplicate", TaskProjectionStatusDone, "once", "session-duplicate"),
	)[0]
	first, err := NewRunner(eventStore, NewTaskProjection("projection.first", 10))
	if err != nil {
		t.Fatal(err)
	}
	if result, err := first.RunBatch(context.Background(), workspaceID); err != nil || result.EventsApplied != 1 {
		t.Fatalf("first delivery result=%+v err=%v", result, err)
	}
	before := snapshotHash(t, pool, workspaceID)
	second, err := NewRunner(eventStore, NewTaskProjection("projection.replay", 10))
	if err != nil {
		t.Fatal(err)
	}
	result, err := second.RunBatch(context.Background(), workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	if result.EventsRead != 1 || result.EventsApplied != 0 || result.EventsDuplicate != 1 {
		t.Fatalf("duplicate result=%+v", result)
	}
	if got := snapshotHash(t, pool, workspaceID); got != before {
		t.Fatalf("duplicate changed projection hash: before=%s after=%s", before, got)
	}
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	state, found, err := ReadTaskStateTx(context.Background(), tx, workspaceID, "task-duplicate")
	_ = tx.Rollback(context.Background())
	if err != nil || !found {
		t.Fatalf("read duplicate state found=%v err=%v", found, err)
	}
	if state.AppliedEventCount != 1 || state.LastEventSequence != event.Event.Sequence {
		t.Fatalf("duplicate state=%+v", state)
	}
}

func TestRunnerStaleCheckpointDoesNotRegress(t *testing.T) {
	eventStore, _, workspaceID := newProjectionTestStore(t)
	appended := appendTaskEvents(t, eventStore,
		makeTaskEvent(t, workspaceID, "task-one", TaskProjectionStatusDone, "one", ""),
		makeTaskEvent(t, workspaceID, "task-two", TaskProjectionStatusDone, "two", ""),
	)
	consumerID := "projection.stale"
	if err := eventStore.AdvanceCheckpoint(context.Background(), workspaceID, consumerID, appended[1].Event.Sequence); err != nil {
		t.Fatal(err)
	}
	if err := eventStore.AdvanceCheckpoint(context.Background(), workspaceID, consumerID, appended[0].Event.Sequence); !errors.Is(err, store.ErrCheckpointRegression) {
		t.Fatalf("stale checkpoint error=%v", err)
	}
	if got := checkpoint(t, eventStore, workspaceID, consumerID); got != appended[1].Event.Sequence {
		t.Fatalf("checkpoint regressed to %d", got)
	}
}

func TestRunnerConcurrentConsumersPreserveIntegrityAndWorkspaceIsolation(t *testing.T) {
	eventStore, pool, workspaceID := newProjectionTestStore(t)
	otherWorkspace := workspaceID + "-other"
	t.Cleanup(func() {
		ctx := context.Background()
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.task_state_projections WHERE workspace_id=$1`, otherWorkspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.control_checkpoints WHERE workspace_id=$1`, otherWorkspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, otherWorkspace)
	})
	for i := 0; i < 12; i++ {
		appendTaskEvents(t, eventStore, makeTaskEvent(t, workspaceID, fmt.Sprintf("task-%02d", i), TaskProjectionStatusDone, fmt.Sprintf("result-%02d", i), ""))
	}
	for i := 0; i < 3; i++ {
		appendTaskEvents(t, eventStore, makeTaskEvent(t, otherWorkspace, fmt.Sprintf("task-%02d", i), TaskProjectionStatusDone, "other", ""))
	}

	const concurrentRuns = 6
	results := make(chan error, concurrentRuns)
	var wg sync.WaitGroup
	for i := 0; i < concurrentRuns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			runner, err := NewRunner(eventStore, NewTaskProjection("projection.concurrent", 2))
			if err == nil {
				_, err = runner.RunBatch(context.Background(), workspaceID)
			}
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent consumer error: %v", err)
		}
	}
	if got := checkpoint(t, eventStore, workspaceID, "projection.concurrent"); got == 0 {
		t.Fatal("concurrent consumer did not advance checkpoint")
	}
	if got := countProjectionRows(t, pool, workspaceID); got != 12 {
		t.Fatalf("workspace projection rows=%d want=12", got)
	}

	firstWorkspaceRunner, err := NewRunner(eventStore, NewTaskProjection("projection.isolation", 10))
	if err != nil {
		t.Fatal(err)
	}
	otherWorkspaceRunner, err := NewRunner(eventStore, NewTaskProjection("projection.isolation", 10))
	if err != nil {
		t.Fatal(err)
	}
	var isolation sync.WaitGroup
	isolation.Add(2)
	var isolationErrors [2]error
	go func() {
		defer isolation.Done()
		_, isolationErrors[0] = firstWorkspaceRunner.RunBatch(context.Background(), workspaceID)
	}()
	go func() {
		defer isolation.Done()
		_, isolationErrors[1] = otherWorkspaceRunner.RunBatch(context.Background(), otherWorkspace)
	}()
	isolation.Wait()
	for _, err := range isolationErrors {
		if err != nil {
			t.Fatalf("workspace isolation error: %v", err)
		}
	}
	if got := countProjectionRows(t, pool, otherWorkspace); got != 3 {
		t.Fatalf("other workspace projection rows=%d want=3", got)
	}
	if got := countProjectionRows(t, pool, workspaceID); got != 12 {
		t.Fatalf("workspace projection rows changed after isolation run=%d", got)
	}
}

func TestRunnerMalformedSupportedEventDoesNotAdvance(t *testing.T) {
	eventStore, pool, workspaceID := newProjectionTestStore(t)
	event := makeTaskEvent(t, workspaceID, "task-malformed", TaskProjectionStatusDone, "ignored", "")
	event.StateDeltas = []contracts.StateDelta{{
		Op: contracts.DeltaSet, Path: "/tasks/task-malformed/result", Value: json.RawMessage(`"missing-status"`),
	}}
	appendTaskEvents(t, eventStore, event)
	runner, err := NewRunner(eventStore, NewTaskProjection("projection.malformed", 10))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runner.RunBatch(context.Background(), workspaceID); err == nil {
		t.Fatal("malformed supported event unexpectedly succeeded")
	}
	if got := checkpoint(t, eventStore, workspaceID, "projection.malformed"); got != 0 {
		t.Fatalf("malformed event advanced checkpoint=%d", got)
	}
	if got := snapshotHash(t, pool, workspaceID); got != emptySnapshotHash(t) {
		t.Fatalf("malformed event wrote projection=%s", got)
	}
}

func TestRunnerLatencyAndReplayThroughput(t *testing.T) {
	eventStore, _, workspaceID := newProjectionTestStore(t)
	const eventCount = 100
	events := make([]contracts.EventEnvelope, 0, eventCount)
	for i := 0; i < eventCount; i++ {
		events = append(events, makeTaskEvent(t, workspaceID, fmt.Sprintf("benchmark-%03d", i), TaskProjectionStatusDone, "ok", ""))
	}
	tx, err := eventStore.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range events {
		if _, err := eventStore.AppendTx(context.Background(), tx, event); err != nil {
			_ = tx.Rollback(context.Background())
			t.Fatalf("append benchmark event: %v", err)
		}
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	runner, err := NewRunner(eventStore, NewTaskProjection("projection.benchmark", 10))
	if err != nil {
		t.Fatal(err)
	}
	latencies := make([]time.Duration, 0, eventCount/10)
	for {
		started := time.Now()
		result, err := runner.RunBatch(context.Background(), workspaceID)
		if err != nil {
			t.Fatal(err)
		}
		latencies = append(latencies, time.Since(started))
		if !result.HasMore {
			break
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p95 := latencies[(len(latencies)*95+99)/100-1]
	startRebuild := time.Now()
	rebuild, err := runner.Rebuild(context.Background(), workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	rebuildDuration := time.Since(startRebuild)
	throughput := float64(rebuild.EventsRead) / rebuildDuration.Seconds()
	t.Logf("projection batches=%d events=%d p50=%s p95=%s max=%s", len(latencies), eventCount, latencies[len(latencies)/2], p95, latencies[len(latencies)-1])
	t.Logf("projection rebuild events=%d duration=%s throughput=%.0f events/s", rebuild.EventsRead, rebuildDuration, throughput)
}

func countProjectionRows(t *testing.T, pool *pgxpool.Pool, workspaceID string) int {
	t.Helper()
	var count int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM fornix.task_state_projections WHERE workspace_id=$1`, workspaceID).Scan(&count); err != nil {
		t.Fatalf("count projection rows: %v", err)
	}
	return count
}

func emptySnapshotHash(t *testing.T) string {
	t.Helper()
	hash, err := hashJSON([]TaskState{})
	if err != nil {
		t.Fatal(err)
	}
	return hash
}
