package store

import (
	"bytes"
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
)

func newEventTestStore(t *testing.T) (*EventStore, *pgxpool.Pool, string) {
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
	workspaceID := fmt.Sprintf("test-events-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_checkpoints WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.idempotency_records WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspaceID)
		pool.Close()
	})
	return NewEventStore(pool), pool, workspaceID
}

func makeTestEvent(t *testing.T, workspaceID, key, payload string) contracts.EventEnvelope {
	t.Helper()
	event, err := contracts.NewEvent("test.control", json.RawMessage(payload))
	if err != nil {
		t.Fatalf("create event: %v", err)
	}
	event.Payload = append(json.RawMessage(nil), payload...)
	event.Scope.WorkspaceID = workspaceID
	event.Actor = contracts.ActorRef{ID: "test", Kind: "test"}
	event.IdempotencyKey = key
	event.StateDeltas = []contracts.StateDelta{{
		Op: contracts.DeltaSet, Path: "/tests/last_payload", Value: json.RawMessage(payload),
	}}
	return event
}

func TestEventStoreDuplicateDeliveryProducesOneEffect(t *testing.T) {
	store, pool, workspaceID := newEventTestStore(t)
	event := makeTestEvent(t, workspaceID, "duplicate-key", `{"attempt":1}`)

	const workers = 16
	results := make(chan AppendResult, workers)
	errorsCh := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := store.Append(context.Background(), event)
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
		t.Fatalf("concurrent append error: %v", err)
	}
	var first AppendResult
	duplicates := 0
	for result := range results {
		if first.Event.EventID == "" {
			first = result
		}
		if result.Duplicate {
			duplicates++
		}
		if result.Event.EventID != first.Event.EventID || result.Event.Sequence != first.Event.Sequence {
			t.Fatalf("duplicate returned a different event: first=%+v result=%+v", first.Event, result.Event)
		}
	}
	if duplicates != workers-1 {
		t.Fatalf("duplicate count = %d, want %d", duplicates, workers-1)
	}
	var count int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1 AND idempotency_key=$2`,
		workspaceID, event.IdempotencyKey).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("event count = %d, want 1", count)
	}
}

func TestEventStoreConcurrentWritersPreserveSequenceIntegrity(t *testing.T) {
	store, _, workspaceID := newEventTestStore(t)
	const writers = 24
	eventsToAppend := make([]contracts.EventEnvelope, writers)
	for i := range eventsToAppend {
		eventsToAppend[i] = makeTestEvent(t, workspaceID, fmt.Sprintf("writer-%d", i), fmt.Sprintf(`{"writer":%d}`, i))
	}
	results := make(chan AppendResult, writers)
	errorsCh := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			result, err := store.Append(context.Background(), eventsToAppend[index])
			if err != nil {
				errorsCh <- err
				return
			}
			results <- result
		}(i)
	}
	wg.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		t.Fatalf("concurrent writer error: %v", err)
	}
	sequences := make(map[uint64]struct{}, writers)
	for result := range results {
		if result.Duplicate {
			t.Fatal("unique writer was reported as duplicate")
		}
		if _, exists := sequences[result.Event.Sequence]; exists {
			t.Fatalf("duplicate event sequence %d", result.Event.Sequence)
		}
		sequences[result.Event.Sequence] = struct{}{}
	}
	if len(sequences) != writers {
		t.Fatalf("committed sequence count = %d, want %d", len(sequences), writers)
	}
	events, err := store.ReadAfterSequence(context.Background(), workspaceID, 0, writers+1)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != writers {
		t.Fatalf("read event count = %d, want %d", len(events), writers)
	}
	for i := 1; i < len(events); i++ {
		if events[i-1].Sequence >= events[i].Sequence {
			t.Fatalf("events are not strictly ordered: %d then %d", events[i-1].Sequence, events[i].Sequence)
		}
	}
}

func TestEventStoreIdempotencyConflictDoesNotAddHistory(t *testing.T) {
	store, pool, workspaceID := newEventTestStore(t)
	first := makeTestEvent(t, workspaceID, "conflict-key", `{"value":1}`)
	if _, err := store.Append(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := makeTestEvent(t, workspaceID, "conflict-key", `{"value":2}`)
	_, err := store.Append(context.Background(), second)
	if !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("error = %v, want ErrIdempotencyConflict", err)
	}
	var count int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1`, workspaceID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("event count = %d, want 1", count)
	}
}

func TestEventStoreReplayOrderingRawPayloadAndCheckpoint(t *testing.T) {
	store, pool, workspaceID := newEventTestStore(t)
	payloads := []string{` {"step":1} `, `{"step":2}`, `{"step":3}`}
	appended := make([]AppendResult, 0, len(payloads))
	for _, payload := range payloads {
		result, err := store.Append(context.Background(), makeTestEvent(t, workspaceID, "", payload))
		if err != nil {
			t.Fatal(err)
		}
		appended = append(appended, result)
	}
	if _, err := pool.Exec(context.Background(), `
		UPDATE fornix.control_events SET event_type='test.overwritten' WHERE sequence=$1`, appended[0].Event.Sequence); err == nil {
		t.Fatal("control event UPDATE unexpectedly succeeded")
	}
	if _, err := pool.Exec(context.Background(), `
		DELETE FROM fornix.control_events WHERE sequence=$1`, appended[0].Event.Sequence); err == nil {
		t.Fatal("control event DELETE unexpectedly succeeded")
	}

	events, err := store.ReadAfterSequence(context.Background(), workspaceID, 0, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != len(appended) {
		t.Fatalf("read %d events, want %d", len(events), len(appended))
	}
	for i, event := range events {
		if event.Sequence != appended[i].Event.Sequence || event.EventID != appended[i].Event.EventID {
			t.Fatalf("event %d ordering mismatch: got=%+v want=%+v", i, event, appended[i].Event)
		}
		if !bytes.Equal(event.Payload, []byte(payloads[i])) {
			t.Fatalf("raw payload %q, want exact %q", event.Payload, payloads[i])
		}
	}

	replayed, err := store.Replay(context.Background(), workspaceID, 0, appended[1].Event.Sequence, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 2 || replayed[0].EventID != appended[0].Event.EventID || replayed[1].EventID != appended[1].Event.EventID {
		t.Fatalf("unexpected replay: %+v", replayed)
	}
	if err := store.AdvanceCheckpoint(context.Background(), workspaceID, "replayer", appended[1].Event.Sequence); err != nil {
		t.Fatal(err)
	}
	if err := store.AdvanceCheckpoint(context.Background(), workspaceID, "replayer", appended[0].Event.Sequence); !errors.Is(err, ErrCheckpointRegression) {
		t.Fatalf("checkpoint regression error = %v", err)
	}
	checkpoint, err := store.Checkpoint(context.Background(), workspaceID, "replayer")
	if err != nil {
		t.Fatal(err)
	}
	if checkpoint != appended[1].Event.Sequence {
		t.Fatalf("checkpoint = %d, want %d", checkpoint, appended[1].Event.Sequence)
	}
	if err := store.AdvanceCheckpoint(context.Background(), workspaceID, "replayer", appended[2].Event.Sequence+100); !errors.Is(err, ErrInvalidCheckpoint) {
		t.Fatalf("invalid checkpoint error = %v", err)
	}
}

func TestEventStoreRollbackAllowsRetry(t *testing.T) {
	store, pool, workspaceID := newEventTestStore(t)
	event := makeTestEvent(t, workspaceID, "rollback-key", `{"retry":true}`)
	tx, err := pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendTx(context.Background(), tx, event); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	result, err := store.Append(context.Background(), event)
	if err != nil {
		t.Fatal(err)
	}
	if result.Duplicate {
		t.Fatal("retry was incorrectly reported as duplicate after rollback")
	}
}

func TestEventStoreAppendLatency(t *testing.T) {
	store, _, workspaceID := newEventTestStore(t)
	const samples = 20
	latencies := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		start := time.Now()
		if _, err := store.Append(context.Background(), makeTestEvent(t, workspaceID, "", fmt.Sprintf(`{"sample":%d}`, i))); err != nil {
			t.Fatal(err)
		}
		latencies = append(latencies, time.Since(start))
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p95 := latencies[(len(latencies)*95+99)/100-1]
	t.Logf("append latency samples=%d p50=%s p95=%s max=%s", samples, latencies[len(latencies)/2], p95, latencies[len(latencies)-1])
}
