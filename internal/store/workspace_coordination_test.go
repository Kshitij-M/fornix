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

func newWorkspaceCoordinationTestStore(t *testing.T) (*WorkspaceCoordinationStore, *pgxpool.Pool, string) {
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
	workspace := fmt.Sprintf("coordination-test-%d", time.Now().UnixNano())
	store := NewWorkspaceCoordinationStore(pool, NewEventStore(pool))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.workspace_router_observations WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.workspace_coordination_messages WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspace)
		pool.Close()
	})
	return store, pool, workspace
}

func coordinationMessage(workspace, key string) contracts.CoordinationMessage {
	return contracts.CoordinationMessage{WorkspaceID: workspace, RequestID: "request-" + key, IdempotencyKey: key, Sender: "worker-a", Recipient: "worker-b", Subject: "handoff", Body: "bounded body", Actor: contracts.ActorRef{ID: "actor-a", Kind: "worker", WorkspaceID: workspace}, OccurredAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)}
}

func routerObservation(workspace, key, model string) contracts.RouterObservation {
	return contracts.RouterObservation{WorkspaceID: workspace, RequestID: "request-" + key, IdempotencyKey: key, TaskCategory: "review", ModelID: model, CostUSD: 0.01, LatencyMS: 100, Outcome: "success", Actor: contracts.ActorRef{ID: "actor-a", Kind: "worker", WorkspaceID: workspace}, ObservedAt: time.Now().UTC()}
}

func TestWorkspaceCoordinationMessageLifecycleIsAtomicAndReplayable(t *testing.T) {
	store, pool, workspace := newWorkspaceCoordinationTestStore(t)
	ctx := context.Background()
	message := coordinationMessage(workspace, "coord-lifecycle")
	first, duplicate, err := store.AppendMessage(ctx, message)
	if err != nil || duplicate || first.Sequence == 0 {
		t.Fatalf("first append=%+v duplicate=%t err=%v", first, duplicate, err)
	}
	repeated, duplicate, err := store.AppendMessage(ctx, message)
	if err != nil || !duplicate || repeated.Sequence != first.Sequence {
		t.Fatalf("duplicate append=%+v duplicate=%t err=%v", repeated, duplicate, err)
	}
	conflict := message
	conflict.Body = "different"
	if _, _, err := store.AppendMessage(ctx, conflict); !errors.Is(err, ErrCoordinationConflict) {
		t.Fatalf("conflict error=%v", err)
	}
	read, err := store.ReadMessages(ctx, workspace, 0, "worker-b", 10)
	if err != nil || len(read) != 1 || read[0].RequestHash != first.RequestHash {
		t.Fatalf("read=%+v err=%v", read, err)
	}
	events, err := NewEventStore(pool).ReadAfterSequence(ctx, workspace, 0, 20)
	if err != nil || len(events) != 1 || events[0].EventType != contracts.CoordinationMessageEventType {
		t.Fatalf("events=%+v err=%v", events, err)
	}
}

func TestWorkspaceCoordinationCrashRollsBackRowAndEvent(t *testing.T) {
	store, pool, workspace := newWorkspaceCoordinationTestStore(t)
	ctx := context.Background()
	store.SetFailureHook(func(point string) error {
		if point == "workspace_coordination_before_commit" {
			return errors.New("simulated coordination crash")
		}
		return nil
	})
	if _, _, err := store.AppendMessage(ctx, coordinationMessage(workspace, "coord-crash")); err == nil {
		t.Fatal("crash hook unexpectedly succeeded")
	}
	store.SetFailureHook(nil)
	var rows, events int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.workspace_coordination_messages WHERE workspace_id=$1`, workspace).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1`, workspace).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if rows != 0 || events != 0 {
		t.Fatalf("crash left authority rows=%d events=%d", rows, events)
	}
}

func TestWorkspaceCoordinationConcurrentDuplicateDeliveryProducesOneEffect(t *testing.T) {
	store, pool, workspace := newWorkspaceCoordinationTestStore(t)
	ctx := context.Background()
	message := coordinationMessage(workspace, "coord-concurrent")
	results := make(chan error, 2)
	var wg sync.WaitGroup
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := store.AppendMessage(ctx, message)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatal(err)
		}
	}
	var rows, events int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.workspace_coordination_messages WHERE workspace_id=$1`, workspace).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1`, workspace).Scan(&events); err != nil {
		t.Fatal(err)
	}
	if rows != 1 || events != 1 {
		t.Fatalf("concurrent duplicate rows=%d events=%d", rows, events)
	}
}

func TestWorkspaceRouterObservationIsScopedAndDeterministic(t *testing.T) {
	store, _, workspace := newWorkspaceCoordinationTestStore(t)
	ctx := context.Background()
	if _, duplicate, err := store.AppendRouterObservation(ctx, routerObservation(workspace, "router-a", "cheap")); err != nil || duplicate {
		t.Fatalf("first observation duplicate=%t err=%v", duplicate, err)
	}
	second := routerObservation(workspace, "router-b", "reliable")
	second.CostUSD = 0.02
	if _, _, err := store.AppendRouterObservation(ctx, second); err != nil {
		t.Fatal(err)
	}
	duplicate, isDuplicate, err := store.AppendRouterObservation(ctx, second)
	if err != nil || !isDuplicate || duplicate.ID == 0 {
		t.Fatalf("duplicate observation=%+v duplicate=%t err=%v", duplicate, isDuplicate, err)
	}
	recommendations, err := store.Recommend(ctx, workspace, "review", 10)
	if err != nil || len(recommendations) != 2 {
		t.Fatalf("recommendations=%+v err=%v", recommendations, err)
	}
	for i := 1; i < len(recommendations); i++ {
		left := recommendations[i-1].SuccessRate / maxFloat(recommendations[i-1].CostUSDAvg, 1e-9)
		right := recommendations[i].SuccessRate / maxFloat(recommendations[i].CostUSDAvg, 1e-9)
		if left < right || (left == right && recommendations[i-1].ModelID > recommendations[i].ModelID) {
			t.Fatalf("recommendations are not deterministically ordered: %+v", recommendations)
		}
	}
	foreign := routerObservation(workspace+"-foreign", "router-a", "foreign")
	if _, _, err := store.AppendRouterObservation(ctx, foreign); err != nil {
		t.Fatal(err)
	}
	recommendations, err = store.Recommend(ctx, workspace, "review", 10)
	if err != nil || len(recommendations) != 2 {
		t.Fatalf("foreign observation leaked: %+v err=%v", recommendations, err)
	}
}
