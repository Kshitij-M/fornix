package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

func newWorkReceiptTestStore(t *testing.T) (*WorkReceiptStore, *pgxpool.Pool, string) {
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
	workspace := fmt.Sprintf("test-work-receipt-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanup, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanup, `DELETE FROM fornix.work_receipt_references WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanup, `DELETE FROM fornix.work_receipt_steps WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanup, `DELETE FROM fornix.work_receipts WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanup, `DELETE FROM fornix.artifact_refs WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanup, `DELETE FROM fornix.artifact_chunks WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanup, `DELETE FROM fornix.artifacts WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanup, `DELETE FROM fornix.evidence_records WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanup, `DELETE FROM fornix.tasks WHERE workspace_id=$1`, workspace)
		pool.Close()
	})
	return NewWorkReceiptStore(pool), pool, workspace
}

func insertCompletedReceiptTask(t *testing.T, pool *pgxpool.Pool, workspace string, fence int64) int64 {
	t.Helper()
	var taskID int64
	err := pool.QueryRow(context.Background(), `
		INSERT INTO fornix.tasks(workspace_id, title, brief, created_by, status, execution_fence, max_attempts)
		VALUES($1,'receipt test task','bounded receipt task','test','done',$2,1) RETURNING id`, workspace, fence).Scan(&taskID)
	if err != nil {
		t.Fatal(err)
	}
	return taskID
}

func testReceiptRequest(workspace string, taskID int64, fence uint64) contracts.WorkReceiptFinalizeRequest {
	taskRef := &contracts.EntityRef{ID: strconv.FormatInt(taskID, 10), Kind: "task", WorkspaceID: workspace}
	return contracts.WorkReceiptFinalizeRequest{
		ReceiptID: "receipt-test", RequestID: "receipt-request-test", IdempotencyKey: "receipt-idempotency-test",
		WorkspaceID: workspace, Actor: contracts.ActorRef{ID: "operator", Kind: "user", Name: "Test Operator", WorkspaceID: workspace},
		WorkKind: contracts.WorkReceiptReferenceTask, WorkID: strconv.FormatInt(taskID, 10), Task: taskRef,
		TaskOwnerID: "worker-1", TaskFence: fence,
		Steps: []contracts.WorkReceiptStep{
			{Ordinal: 0, ID: "task-complete", Name: "task completion", Kind: "task", Status: "succeeded", OutputHash: strings.Repeat("a", 64)},
		},
		References: []contracts.WorkReceiptReference{{WorkspaceID: workspace, Kind: contracts.WorkReceiptReferenceTask, SourceID: strconv.FormatInt(taskID, 10)}},
	}
}

func TestWorkReceiptFinalizeIsConcurrentAndIdempotent(t *testing.T) {
	store, pool, workspace := newWorkReceiptTestStore(t)
	taskID := insertCompletedReceiptTask(t, pool, workspace, 7)
	request := testReceiptRequest(workspace, taskID, 7)
	const writers = 12
	created := make(chan bool, writers)
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, wasCreated, err := store.Finalize(context.Background(), request)
			if err != nil {
				errs <- err
				return
			}
			created <- wasCreated
		}()
	}
	wg.Wait()
	close(created)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	createdCount := 0
	for value := range created {
		if value {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("concurrent finalization created=%d, want 1", createdCount)
	}
	var receipts, steps, refs int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.work_receipts WHERE workspace_id=$1`, workspace).Scan(&receipts); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.work_receipt_steps WHERE workspace_id=$1`, workspace).Scan(&steps); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.work_receipt_references WHERE workspace_id=$1`, workspace).Scan(&refs); err != nil {
		t.Fatal(err)
	}
	if receipts != 1 || steps != 1 || refs != 1 {
		t.Fatalf("durable effect counts receipts=%d steps=%d refs=%d", receipts, steps, refs)
	}

	conflict := request
	conflict.Steps = append([]contracts.WorkReceiptStep(nil), request.Steps...)
	conflict.Steps[0].OutputHash = strings.Repeat("b", 64)
	if _, _, err := store.Finalize(context.Background(), conflict); !errors.Is(err, ErrWorkReceiptConflict) {
		t.Fatalf("conflicting duplicate error=%v", err)
	}
	stale := request
	stale.TaskFence = 6
	if _, _, err := store.Finalize(context.Background(), stale); !errors.Is(err, ErrWorkReceiptStale) {
		t.Fatalf("stale fence error=%v", err)
	}
}

func TestWorkReceiptCrashBeforeCommitLeavesNoPartialHistory(t *testing.T) {
	store, pool, workspace := newWorkReceiptTestStore(t)
	taskID := insertCompletedReceiptTask(t, pool, workspace, 3)
	request := testReceiptRequest(workspace, taskID, 3)
	store.SetFailureHook(func(stage string) error {
		if stage == "links_inserted" {
			return errors.New("injected receipt crash")
		}
		return nil
	})
	if _, _, err := store.Finalize(context.Background(), request); err == nil || !strings.Contains(err.Error(), "injected receipt crash") {
		t.Fatalf("crash error=%v", err)
	}
	store.SetFailureHook(nil)
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.work_receipts WHERE workspace_id=$1`, workspace).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("crashed finalization left receipt count=%d", count)
	}
	if _, created, err := store.Finalize(context.Background(), request); err != nil || !created {
		t.Fatalf("retry created=%t err=%v", created, err)
	}
}

func TestWorkReceiptTypedEvidenceAndArtifactLinksAreIntegrityChecked(t *testing.T) {
	store, pool, workspace := newWorkReceiptTestStore(t)
	taskID := insertCompletedReceiptTask(t, pool, workspace, 4)
	artifacts := NewArtifactStore(pool)
	artifact, err := artifacts.Put(context.Background(), ArtifactPutInput{WorkspaceID: workspace, Kind: "receipt-test", MediaType: "text/plain", Raw: []byte("immutable report"), SourceKind: "task", SourceID: strconv.FormatInt(taskID, 10), Role: "report", IdempotencyKey: "receipt-artifact"})
	if err != nil {
		t.Fatal(err)
	}
	evidenceStore := NewEvidenceStore(pool)
	evidence, err := evidenceStore.Put(context.Background(), EvidencePutInput{WorkspaceID: workspace, SourceReference: "task:" + strconv.FormatInt(taskID, 10), DeduplicationKey: "receipt-evidence", Kind: "report", MediaType: "text/plain", Gist: "immutable report", Detail: "immutable report detail", RawPayload: []byte("immutable report")})
	if err != nil {
		t.Fatal(err)
	}
	request := testReceiptRequest(workspace, taskID, 4)
	request.ReceiptID, request.RequestID, request.IdempotencyKey = "receipt-linked", "receipt-linked-request", "receipt-linked-key"
	request.Artifacts = []contracts.WorkReceiptArtifact{{ID: artifact.Reference.ID, ArtifactID: artifact.Artifact.ID, WorkspaceID: workspace, ContentHash: artifact.Artifact.ContentHash, Role: "report"}}
	request.Evidence = []contracts.WorkReceiptEvidence{{ID: evidence.Record.ID, WorkspaceID: workspace, EvidenceHash: evidence.Record.EvidenceHash, SourceReference: evidence.Record.SourceReference, Role: "report"}}
	request.References = []contracts.WorkReceiptReference{{WorkspaceID: workspace, Kind: contracts.WorkReceiptReferenceTask, SourceID: strconv.FormatInt(taskID, 10)}}
	if _, created, err := store.Finalize(context.Background(), request); err != nil || !created {
		t.Fatalf("linked receipt created=%t err=%v", created, err)
	}
	foreign := request
	foreign.ReceiptID, foreign.RequestID, foreign.IdempotencyKey = "receipt-foreign", "receipt-foreign-request", "receipt-foreign-key"
	foreign.Evidence[0].WorkspaceID = workspace + "-other"
	if _, _, err := store.Finalize(context.Background(), foreign); err == nil {
		t.Fatal("cross-workspace evidence was accepted")
	}
}

func TestWorkReceiptDisclosurePreservesHashAndBudgets(t *testing.T) {
	store, pool, workspace := newWorkReceiptTestStore(t)
	taskID := insertCompletedReceiptTask(t, pool, workspace, 5)
	request := testReceiptRequest(workspace, taskID, 5)
	receipt, _, err := store.Finalize(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	gist, err := store.Disclose(context.Background(), contracts.WorkReceiptDisclosureRequest{WorkspaceID: workspace, ReceiptID: receipt.ID, Level: contracts.WorkReceiptDisclosureGist, MaxBytes: 4096, MaxTokens: 1024, MaxItems: 8})
	if err != nil {
		t.Fatal(err)
	}
	detail, err := store.Disclose(context.Background(), contracts.WorkReceiptDisclosureRequest{WorkspaceID: workspace, ReceiptID: receipt.ID, Level: contracts.WorkReceiptDisclosureDetail, MaxBytes: 4096, MaxTokens: 1024, MaxItems: 1})
	if err != nil {
		t.Fatal(err)
	}
	raw, err := store.Disclose(context.Background(), contracts.WorkReceiptDisclosureRequest{WorkspaceID: workspace, ReceiptID: receipt.ID, Level: contracts.WorkReceiptDisclosureRaw, MaxBytes: 64, MaxTokens: 16, MaxItems: 1})
	if err != nil {
		t.Fatal(err)
	}
	for _, hash := range []string{gist.CanonicalHash, detail.CanonicalHash, raw.CanonicalHash} {
		if hash != receipt.CanonicalHash {
			t.Fatalf("disclosure changed canonical hash: %s != %s", hash, receipt.CanonicalHash)
		}
	}
	if len(raw.Raw) > 64 || !raw.Truncated || detail.TotalItems > 1 {
		t.Fatalf("disclosure budgets were not enforced: raw=%d truncated=%t detail_items=%d", len(raw.Raw), raw.Truncated, detail.TotalItems)
	}
	if _, err := store.Get(context.Background(), workspace+"-other", receipt.ID); !errors.Is(err, ErrWorkReceiptNotFound) {
		t.Fatalf("cross-workspace receipt read error=%v", err)
	}
}

func TestWorkReceiptRowsAreAppendOnly(t *testing.T) {
	store, pool, workspace := newWorkReceiptTestStore(t)
	taskID := insertCompletedReceiptTask(t, pool, workspace, 6)
	receipt, _, err := store.Finalize(context.Background(), testReceiptRequest(workspace, taskID, 6))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `UPDATE fornix.work_receipts SET status='rejected' WHERE workspace_id=$1 AND id=$2`, workspace, receipt.ID); err == nil {
		t.Fatal("append-only receipt update unexpectedly succeeded")
	}
	if _, err := pool.Exec(context.Background(), `DELETE FROM fornix.work_receipts WHERE workspace_id=$1 AND id=$2`, workspace, receipt.ID); err == nil {
		t.Fatal("append-only receipt delete unexpectedly succeeded")
	}
}
