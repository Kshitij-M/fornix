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
)

func TestTaskCreateIdempotencyAndConcurrentDuplicateDelivery(t *testing.T) {
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
	workspace := fmt.Sprintf("test-task-create-idempotency-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.task_dependencies WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.idempotency_records WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.tasks WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspace)
		pool.Close()
	})

	taskStore := NewTaskStore(pool, NewEventStore(pool))
	request := TaskCreateInput{WorkspaceID: workspace, IdempotencyKey: "create-once", Title: "inspect", Brief: "inspect repository", CreatedBy: "operator", MaxAttempts: 2}
	first, firstEvent, err := taskStore.Create(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	second, secondEvent, err := taskStore.Create(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if first.ID != second.ID || firstEvent.Sequence != secondEvent.Sequence {
		t.Fatalf("duplicate create changed effect: first=(%d,%d) second=(%d,%d)", first.ID, firstEvent.Sequence, second.ID, secondEvent.Sequence)
	}
	serverRestart := request
	serverRestart.OriginHost = "a-different-server-host"
	if restarted, restartedEvent, err := taskStore.Create(ctx, serverRestart); err != nil {
		t.Fatalf("server restart retry failed: %v", err)
	} else if restarted.ID != first.ID || restartedEvent.Sequence != firstEvent.Sequence {
		t.Fatalf("server metadata changed idempotent effect: got (%d,%d), want (%d,%d)", restarted.ID, restartedEvent.Sequence, first.ID, firstEvent.Sequence)
	}
	var taskCount, eventCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.tasks WHERE workspace_id=$1 AND create_idempotency_key=$2`, workspace, request.IdempotencyKey).Scan(&taskCount); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1 AND idempotency_key=$2`, workspace, request.IdempotencyKey).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if taskCount != 1 || eventCount != 1 {
		t.Fatalf("durable duplicate counts tasks=%d events=%d; want 1/1", taskCount, eventCount)
	}
	conflict := request
	conflict.Brief = "different request"
	if _, _, err := taskStore.Create(ctx, conflict); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("conflicting create error=%v, want ErrIdempotencyConflict", err)
	}

	concurrentRequest := TaskCreateInput{WorkspaceID: workspace, IdempotencyKey: "create-concurrent", Title: "parallel", Brief: "same request", CreatedBy: "operator"}
	results := make(chan struct {
		taskID int64
		seq    uint64
		err    error
	}, 2)
	var wait sync.WaitGroup
	for i := 0; i < 2; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			created, event, createErr := taskStore.Create(context.Background(), concurrentRequest)
			results <- struct {
				taskID int64
				seq    uint64
				err    error
			}{created.ID, event.Sequence, createErr}
		}()
	}
	wait.Wait()
	close(results)
	var concurrentID int64
	var concurrentSeq uint64
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if concurrentID == 0 {
			concurrentID, concurrentSeq = result.taskID, result.seq
		} else if result.taskID != concurrentID || result.seq != concurrentSeq {
			t.Fatalf("concurrent create changed effect: got (%d,%d), expected (%d,%d)", result.taskID, result.seq, concurrentID, concurrentSeq)
		}
	}
}
