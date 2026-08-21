package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

func newRetrievalSurfaceTestStore(t *testing.T) (*RetrievalSurfaceStore, *pgxpool.Pool, string) {
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
	workspace := fmt.Sprintf("test-retrieval-surface-%d", time.Now().UnixNano())
	// The table is intentionally append-only. A unique workspace per test keeps
	// cleanup from needing a privileged trigger bypass and mirrors production
	// retention semantics.
	t.Cleanup(pool.Close)
	return NewRetrievalSurfaceStore(pool), pool, workspace
}

func testSurface(workspace, id, key string) contracts.RetrievalSurface {
	id = workspace + "-" + id
	return contracts.RetrievalSurface{
		ID:             id,
		WorkspaceID:    workspace,
		RequestID:      "request-" + id,
		IdempotencyKey: key,
		RequestHash:    strings.Repeat("a", 64),
		PlanHash:       strings.Repeat("b", 64),
		ContextHash:    strings.Repeat("c", 64),
		Budget:         contracts.RetrievalBudget{MaxItems: 2, MaxBytes: 2048, MaxTokens: 256},
		Trace:          contracts.RetrievalTrace{PlanHash: strings.Repeat("b", 64), Stages: []contracts.RetrievalStageTrace{{Name: contracts.StageLexical, Status: "completed", Queries: 1}}, CompiledItems: 1},
		References: []contracts.RetrievalSurfaceReference{{
			SourceReference: "memo:1", Kind: "memo", EvidenceHash: strings.Repeat("d", 64), Score: 0.9, Stage: contracts.StageLexical,
		}},
		CapturedAt: time.Now().UTC(),
	}
}

func TestRetrievalSurfaceCaptureConcurrentDuplicateIsIdempotent(t *testing.T) {
	surfaces, pool, workspace := newRetrievalSurfaceTestStore(t)
	input := testSurface(workspace, "surface-concurrent", "capture-concurrent")
	const writers = 16
	results := make(chan contracts.RetrievalSurface, writers)
	created := make(chan bool, writers)
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stored, wasCreated, err := surfaces.Capture(context.Background(), input)
			if err != nil {
				errs <- err
				return
			}
			results <- stored
			created <- wasCreated
		}()
	}
	wg.Wait()
	close(results)
	close(created)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	createdCount := 0
	for wasCreated := range created {
		if wasCreated {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("concurrent duplicate capture created=%d, want 1", createdCount)
	}
	expected := input
	if err := expected.Normalize(); err != nil {
		t.Fatal(err)
	}
	for stored := range results {
		if stored.PayloadHash != expected.PayloadHash || stored.WorkspaceID != workspace {
			t.Fatalf("unstable duplicate result: %+v", stored)
		}
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.retrieval_surfaces WHERE workspace_id=$1 AND idempotency_key=$2`, workspace, input.IdempotencyKey).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("surface count=%d, want 1", count)
	}

	conflict := input
	conflict.ContextHash = strings.Repeat("e", 64)
	if _, _, err := surfaces.Capture(context.Background(), conflict); !errors.Is(err, ErrRetrievalSurfaceConflict) {
		t.Fatalf("conflict error=%v", err)
	}
}

func TestRetrievalSurfaceCaptureRollsBackOnFailureAndIsWorkspaceScoped(t *testing.T) {
	surfaces, pool, workspace := newRetrievalSurfaceTestStore(t)
	input := testSurface(workspace, "surface-crash", "capture-crash")
	surfaces.SetFailureHook(func(stage string) error {
		if stage == "inserted" {
			return errors.New("injected capture crash")
		}
		return nil
	})
	if _, _, err := surfaces.Capture(context.Background(), input); err == nil || !strings.Contains(err.Error(), "injected capture crash") {
		t.Fatalf("failure hook error=%v", err)
	}
	surfaces.SetFailureHook(nil)
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.retrieval_surfaces WHERE workspace_id=$1 AND id=$2`, workspace, input.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("failed capture left authoritative row count=%d", count)
	}

	stored, created, err := surfaces.Capture(context.Background(), input)
	if err != nil || !created {
		t.Fatalf("retry after rollback created=%v err=%v", created, err)
	}
	if _, err := surfaces.Get(context.Background(), workspace+"-foreign", stored.ID); !errors.Is(err, ErrRetrievalSurfaceNotFound) {
		t.Fatalf("cross-workspace read error=%v", err)
	}
	if _, err := surfaces.GetMany(context.Background(), workspace+"-foreign", []string{stored.ID}); !errors.Is(err, ErrRetrievalSurfaceNotFound) {
		t.Fatalf("cross-workspace batch error=%v", err)
	}
}

func TestRetrievalSurfaceListIsBoundedAndCursorOrdered(t *testing.T) {
	surfaces, _, workspace := newRetrievalSurfaceTestStore(t)
	for i := 0; i < 3; i++ {
		input := testSurface(workspace, fmt.Sprintf("surface-page-%d", i), fmt.Sprintf("capture-page-%d", i))
		input.CapturedAt = time.Unix(1_700_000_000+int64(i), 0).UTC()
		if _, _, err := surfaces.Capture(context.Background(), input); err != nil {
			t.Fatal(err)
		}
	}
	first, err := surfaces.List(context.Background(), workspace, 1, "")
	if err != nil || len(first.Items) != 1 || first.NextCursor == "" {
		t.Fatalf("first page=%+v err=%v", first, err)
	}
	second, err := surfaces.List(context.Background(), workspace, 1, first.NextCursor)
	if err != nil || len(second.Items) != 1 || second.Items[0].ID == first.Items[0].ID {
		t.Fatalf("second page=%+v err=%v", second, err)
	}
	if _, err := surfaces.List(context.Background(), workspace, 1, "not-a-cursor"); !errors.Is(err, ErrRetrievalSurfaceCursor) {
		t.Fatalf("invalid cursor error=%v", err)
	}
}

func TestRetrievalSurfaceAppendOnlyTriggerRejectsMutation(t *testing.T) {
	surfaces, pool, workspace := newRetrievalSurfaceTestStore(t)
	input := testSurface(workspace, "surface-immutable", "capture-immutable")
	stored, _, err := surfaces.Capture(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE fornix.retrieval_surfaces SET context_hash=$1 WHERE workspace_id=$2 AND id=$3`, strings.Repeat("e", 64), workspace, stored.ID); err == nil {
		t.Fatal("append-only update unexpectedly succeeded")
	}
	if _, err := pool.Exec(context.Background(), `DELETE FROM fornix.retrieval_surfaces WHERE workspace_id=$1 AND id=$2`, workspace, stored.ID); err == nil {
		t.Fatal("append-only delete unexpectedly succeeded")
	}
}
