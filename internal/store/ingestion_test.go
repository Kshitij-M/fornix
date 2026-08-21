package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/ingest"
)

func newIngestTestStore(t *testing.T) (*IngestStore, *pgxpool.Pool, string, string) {
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
	workspace := fmt.Sprintf("test-ingest-%d", time.Now().UnixNano())
	root := t.TempDir()
	events := NewEventStore(pool)
	store := NewIngestStore(pool, events, NewArtifactStore(pool))
	t.Cleanup(pool.Close)
	return store, pool, workspace, root
}

func ingestFixture(t *testing.T, root string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("# Fornix\n\nDeterministic ingestion.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "main.go"), []byte("package main\n\nfunc Main() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func submitIngest(t *testing.T, store *IngestStore, workspace, root string) contracts.IngestJob {
	t.Helper()
	source := contracts.RepositorySource{Repository: "fixture", SourceRoot: root, MountRoot: root, ExtractSymbols: true, ChunkBytes: 64, ChunkOverlap: 8}
	request := contracts.IngestJobRequest{WorkspaceID: workspace, IdempotencyKey: "fixture-ingest", Actor: contracts.ActorRef{ID: "test", Kind: "test", WorkspaceID: workspace}, Source: source, BatchSize: 1}
	discovery, err := ingest.Discover(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	job, created, err := store.Submit(context.Background(), request, discovery)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("first ingest was unexpectedly deduped")
	}
	return job
}

func TestIngestStoreDuplicateResumeAndCrashRecovery(t *testing.T) {
	store, pool, workspace, root := newIngestTestStore(t)
	ingestFixture(t, root)
	job := submitIngest(t, store, workspace, root)
	checkpointBefore := job.Checkpoint
	store.SetFailureHook(func() error { return errors.New("injected commit crash") })
	if _, err := store.ProcessBatch(context.Background(), contracts.IngestBatchRequest{WorkspaceID: workspace, JobID: job.ID, BatchSize: 1, Actor: job.Actor}); err == nil {
		t.Fatal("expected injected crash")
	}
	store.SetFailureHook(nil)
	afterCrash, _, err := store.Get(context.Background(), workspace, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterCrash.Checkpoint.NextOrdinal != checkpointBefore.NextOrdinal || afterCrash.Status != contracts.IngestQueued {
		t.Fatalf("crash changed authoritative state: before=%+v after=%+v", checkpointBefore, afterCrash.Checkpoint)
	}

	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.ProcessBatch(context.Background(), contracts.IngestBatchRequest{WorkspaceID: workspace, JobID: job.ID, BatchSize: 1, Actor: job.Actor})
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	progressed := 0
	for err := range results {
		if err == nil {
			progressed++
		} else if !errors.Is(err, ErrIngestCheckpoint) {
			t.Fatalf("concurrent batch error: %v", err)
		}
	}
	if progressed < 1 {
		t.Fatalf("concurrent writers progressed=%d", progressed)
	}
	for i := 0; i < 8; i++ {
		current, _, err := store.Get(context.Background(), workspace, job.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Status == contracts.IngestSucceeded {
			break
		}
		if _, err := store.ProcessBatch(context.Background(), contracts.IngestBatchRequest{WorkspaceID: workspace, JobID: job.ID, BatchSize: 1, Actor: job.Actor}); err != nil && !errors.Is(err, ErrIngestCheckpoint) {
			t.Fatal(err)
		}
	}
	final, _, err := store.Get(context.Background(), workspace, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != contracts.IngestSucceeded || final.Report == nil || final.Report.ReportHash == "" || final.Checkpoint.StateHash == "" {
		t.Fatalf("ingest did not complete: %+v", final)
	}
	var chunks, symbols int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.chunks WHERE workspace_id=$1`, workspace).Scan(&chunks); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.ingest_symbols WHERE workspace_id=$1`, workspace).Scan(&symbols); err != nil {
		t.Fatal(err)
	}
	if chunks == 0 || symbols == 0 {
		t.Fatalf("indexed counts chunks=%d symbols=%d", chunks, symbols)
	}

	discovery, err := ingest.Discover(context.Background(), contracts.RepositorySource{Repository: "fixture", SourceRoot: root, MountRoot: root, ExtractSymbols: true, ChunkBytes: 64, ChunkOverlap: 8})
	if err != nil {
		t.Fatal(err)
	}
	deduped, created, err := store.Submit(context.Background(), contracts.IngestJobRequest{WorkspaceID: workspace, IdempotencyKey: "fixture-ingest", Actor: job.Actor, Source: contracts.RepositorySource{Repository: "fixture", SourceRoot: root, MountRoot: root, ExtractSymbols: true, ChunkBytes: 64, ChunkOverlap: 8}, BatchSize: 1}, discovery)
	if err != nil {
		t.Fatal(err)
	}
	if created || deduped.ID != job.ID {
		t.Fatalf("duplicate submission created=%v job=%+v", created, deduped)
	}
}

func TestIngestStoreDeduplicatesIdenticalChunkContentAcrossFiles(t *testing.T) {
	store, pool, workspace, root := newIngestTestStore(t)
	content := []byte("same repository content\n")
	if err := os.WriteFile(filepath.Join(root, "a.txt"), content, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "b.txt"), content, 0o644); err != nil {
		t.Fatal(err)
	}
	source := contracts.RepositorySource{Repository: "duplicate-content", SourceRoot: root, MountRoot: root, ChunkBytes: 128}
	discovery, err := ingest.Discover(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	job, created, err := store.Submit(context.Background(), contracts.IngestJobRequest{
		WorkspaceID: workspace, IdempotencyKey: "duplicate-content-ingest",
		Actor:  contracts.ActorRef{ID: "test", Kind: "test", WorkspaceID: workspace},
		Source: source, BatchSize: 2,
	}, discovery)
	if err != nil || !created {
		t.Fatalf("submit created=%t err=%v", created, err)
	}
	result, err := store.ProcessBatch(context.Background(), contracts.IngestBatchRequest{WorkspaceID: workspace, JobID: job.ID, BatchSize: 2, Actor: job.Actor})
	if err != nil {
		t.Fatal(err)
	}
	if result.Job.Status != contracts.IngestSucceeded || result.Job.DedupedChunks != 1 {
		t.Fatalf("deduplication result=%+v", result.Job)
	}
	var chunks int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.chunks WHERE workspace_id=$1 AND content=$2`, workspace, string(content)).Scan(&chunks); err != nil {
		t.Fatal(err)
	}
	if chunks != 1 {
		t.Fatalf("identical content created %d chunk rows, want one", chunks)
	}
}

func TestIngestStoreRejectsSourceMutationAndCrossWorkspaceRead(t *testing.T) {
	store, _, workspace, root := newIngestTestStore(t)
	ingestFixture(t, root)
	job := submitIngest(t, store, workspace, root)
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("mutated\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ProcessBatch(context.Background(), contracts.IngestBatchRequest{WorkspaceID: workspace, JobID: job.ID, BatchSize: 1, Actor: job.Actor}); !errors.Is(err, ErrIngestPathChanged) {
		t.Fatalf("mutation error=%v", err)
	}
	if _, _, err := store.Get(context.Background(), workspace+"-foreign", job.ID); !errors.Is(err, ErrIngestJobNotFound) {
		t.Fatalf("cross-workspace read error=%v", err)
	}
}

func TestIngestStoreTaskFenceFailsClosed(t *testing.T) {
	store, pool, workspace, root := newIngestTestStore(t)
	ingestFixture(t, root)
	taskStore := NewTaskStore(pool, NewEventStore(pool))
	addTaskSession(t, pool, workspace, "ingest-worker-a")
	addTaskSession(t, pool, workspace, "ingest-worker-b")
	task, _, err := taskStore.Create(context.Background(), TaskCreateInput{WorkspaceID: workspace, Title: "ingest", Brief: "index mounted source", CreatedBy: "test"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := taskStore.ClaimNext(context.Background(), TaskClaimInput{WorkspaceID: workspace, SessionID: "ingest-worker-a", LeaseTTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	source := contracts.RepositorySource{Repository: "fixture-fenced", SourceRoot: root, MountRoot: root, ExtractSymbols: true}
	discovery, err := ingest.Discover(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.IngestJobRequest{WorkspaceID: workspace, IdempotencyKey: "fenced-ingest", Actor: contracts.ActorRef{ID: "test", WorkspaceID: workspace}, Task: &contracts.EntityRef{ID: fmt.Sprint(task.ID), Kind: "task", WorkspaceID: workspace}, TaskOwnerID: "ingest-worker-a", TaskFence: claim.Lease.Fence, Source: source}
	job, _, err := store.Submit(context.Background(), request, discovery)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ProcessBatch(context.Background(), contracts.IngestBatchRequest{WorkspaceID: workspace, JobID: job.ID, BatchSize: 1, Actor: request.Actor}); !errors.Is(err, ErrIngestFence) {
		t.Fatalf("missing batch fence was accepted: %v", err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE fornix.task_execution_leases SET lease_until=clock_timestamp() WHERE workspace_id=$1 AND task_id=$2`, workspace, task.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := taskStore.ClaimNext(context.Background(), TaskClaimInput{WorkspaceID: workspace, SessionID: "ingest-worker-b", LeaseTTL: time.Minute}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ProcessBatch(context.Background(), contracts.IngestBatchRequest{WorkspaceID: workspace, JobID: job.ID, BatchSize: 1, TaskOwnerID: "ingest-worker-a", TaskFence: claim.Lease.Fence, Actor: request.Actor}); !errors.Is(err, ErrIngestFence) {
		t.Fatalf("stale batch fence was accepted: %v", err)
	}
}

func TestIngestStorePreservesSupersessionAndRemovalHistory(t *testing.T) {
	store, pool, workspace, root := newIngestTestStore(t)
	ingestFixture(t, root)
	first := submitIngest(t, store, workspace, root)
	for i := 0; i < 4; i++ {
		current, _, err := store.Get(context.Background(), workspace, first.ID)
		if err != nil {
			t.Fatal(err)
		}
		if current.Status == contracts.IngestSucceeded {
			break
		}
		if _, err := store.ProcessBatch(context.Background(), contracts.IngestBatchRequest{WorkspaceID: workspace, JobID: first.ID, BatchSize: 8}); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Remove(filepath.Join(root, "main.go")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "README.md"), []byte("# Fornix\n\nUpdated source.\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	source := contracts.RepositorySource{Repository: "fixture", SourceRoot: root, MountRoot: root, ExtractSymbols: true, ChunkBytes: 64, ChunkOverlap: 8}
	discovery, err := ingest.Discover(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	second, created, err := store.Submit(context.Background(), contracts.IngestJobRequest{WorkspaceID: workspace, IdempotencyKey: "fixture-ingest-v2", Actor: first.Actor, Source: source, BatchSize: 8}, discovery)
	if err != nil {
		t.Fatal(err)
	}
	if !created || second.ID == first.ID {
		t.Fatalf("supersession did not create a new snapshot: first=%s second=%s created=%v", first.ID, second.ID, created)
	}
	var supersedes string
	if err := pool.QueryRow(context.Background(), `SELECT COALESCE(supersedes_file_id,'') FROM fornix.ingest_files WHERE job_id=$1 AND path='README.md'`, second.ID).Scan(&supersedes); err != nil {
		t.Fatal(err)
	}
	if supersedes == "" {
		t.Fatal("changed file has no supersession link")
	}
	var removed int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.ingest_files WHERE job_id=$1 AND path='main.go' AND state='removed'`, second.ID).Scan(&removed); err != nil {
		t.Fatal(err)
	}
	if removed != 1 {
		t.Fatalf("removed source history count=%d", removed)
	}
}
