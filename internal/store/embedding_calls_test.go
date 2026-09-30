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

func TestEmbeddingCallStoreLifecycleAndReplay(t *testing.T) {
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}

	store := NewEmbeddingCallStore(pool)
	request := embeddingStoreTestRequest()
	start, err := store.Start(ctx, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	if start.Existing || start.Record.Status != contracts.EmbeddingCallPending {
		t.Fatalf("start = %+v", start)
	}
	if err := store.Attempt(ctx, request.WorkspaceID, request.RequestID); err != nil {
		t.Fatal(err)
	}
	vector := make([]float32, contracts.EmbeddingDimension)
	for i := range vector {
		vector[i] = float32(i%19) / 19
	}
	if err := store.Finish(ctx, contracts.EmbeddingCallResult{
		WorkspaceID: request.WorkspaceID, RequestID: request.RequestID,
		Status: contracts.EmbeddingCallSucceeded, AttemptCount: 1, Vector: vector,
		Usage:            contracts.EmbeddingUsage{InputBytes: int64(len(request.Text)), Dimension: len(vector), Source: "estimated"},
		ResponseEvidence: []byte(`{"vector":"redacted"}`),
	}); err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(ctx, request.WorkspaceID, request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Status != contracts.EmbeddingCallSucceeded || len(record.Vector) != contracts.EmbeddingDimension {
		t.Fatalf("record = %+v", record)
	}
	if record.Metadata["caller_note"] != "[REDACTED]" {
		t.Fatalf("embedding metadata was not redacted: %+v", record.Metadata)
	}
	hash, err := contracts.EmbeddingVectorHash(record.Vector)
	if err != nil || hash != record.VectorHash {
		t.Fatalf("vector hash = %s/%s err=%v", hash, record.VectorHash, err)
	}
	attachment := contracts.EmbeddingTargetAttachment{
		WorkspaceID: request.WorkspaceID, RequestID: request.RequestID,
		TargetKind: "chunk", TargetID: "chunk-1", SourceHash: record.SourceHash, VectorHash: hash,
	}
	if err := store.Attach(ctx, attachment); err != nil {
		t.Fatal(err)
	}
	if err := store.Attach(ctx, attachment); err != nil {
		t.Fatalf("duplicate attachment: %v", err)
	}
	conflicting := attachment
	conflicting.VectorHash = contracts.EmbeddingSourceHash("different-vector")
	if err := store.Attach(ctx, conflicting); !errors.Is(err, ErrEmbeddingAttachmentConflict) {
		t.Fatalf("conflicting attachment error = %v", err)
	}
	duplicate, err := store.Start(ctx, request, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Existing || duplicate.Record.VectorHash != record.VectorHash {
		t.Fatalf("duplicate = %+v", duplicate)
	}
	conflict := request
	conflict.Text = "different source"
	if _, err := store.Start(ctx, conflict, nil); !errors.Is(err, ErrEmbeddingCallConflict) {
		t.Fatalf("conflict error = %v", err)
	}
}

func TestEmbeddingCallStoreConcurrentStartDeduplicates(t *testing.T) {
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := NewEmbeddingCallStore(pool)
	request := embeddingStoreTestRequest()
	request.RequestID = "embedding-concurrent-request"
	request.IdempotencyKey = "embedding-concurrent-key"
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	starts := make(chan contracts.EmbeddingCallStart, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start, startErr := store.Start(ctx, request, nil)
			if startErr != nil {
				errs <- startErr
				return
			}
			starts <- start
		}()
	}
	wg.Wait()
	close(errs)
	close(starts)
	for err := range errs {
		t.Fatal(err)
	}
	if count := len(starts); count != 8 {
		t.Fatalf("start count = %d", count)
	}
	var recordCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.embedding_calls WHERE workspace_id=$1 AND request_id=$2`, request.WorkspaceID, request.RequestID).Scan(&recordCount); err != nil {
		t.Fatal(err)
	}
	if recordCount != 1 {
		t.Fatalf("durable record count = %d, want 1", recordCount)
	}
}

func TestEmbeddingCallStoreQueryDeduplicatesAcrossRoutes(t *testing.T) {
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := NewEmbeddingCallStore(pool)
	first := embeddingStoreTestRequest()
	first.SourceKind = "memo_query"
	first.RequestID = "memo-query-request-" + fmt.Sprint(time.Now().UnixNano())
	first.IdempotencyKey = first.RequestID + "-key"
	if _, err := store.Start(ctx, first, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Attempt(ctx, first.WorkspaceID, first.RequestID); err != nil {
		t.Fatal(err)
	}
	vector := make([]float32, contracts.EmbeddingDimension)
	if err := store.Finish(ctx, contracts.EmbeddingCallResult{WorkspaceID: first.WorkspaceID, RequestID: first.RequestID, Status: contracts.EmbeddingCallSucceeded, Vector: vector}); err != nil {
		t.Fatal(err)
	}
	second := first
	second.SourceKind = "rag_query"
	second.RequestID = "rag-query-request-" + fmt.Sprint(time.Now().UnixNano())
	second.IdempotencyKey = second.RequestID + "-key"
	start, err := store.Start(ctx, second, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !start.Existing || start.Record.RequestID != first.RequestID || start.Record.Status != contracts.EmbeddingCallSucceeded {
		t.Fatalf("cross-route query reuse = %+v", start)
	}
}

func TestEmbeddingCallStoreRecoversStaleInFlightCallWithoutRetry(t *testing.T) {
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := NewEmbeddingCallStore(pool)
	request := embeddingStoreTestRequest()
	request.WorkspaceID = fmt.Sprintf("embedding-recovery-%d", time.Now().UnixNano())
	request.Actor.WorkspaceID = request.WorkspaceID
	request.RequestID = "embedding-recovery-request"
	request.IdempotencyKey = "embedding-recovery-key"
	if _, err := store.Start(ctx, request, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Attempt(ctx, request.WorkspaceID, request.RequestID); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.RecoverStale(ctx, request.WorkspaceID, request.RequestID, time.Now().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Status != contracts.EmbeddingCallRecoveryRequired || recovered.Failure == nil || !recovered.Failure.PossiblyStarted {
		t.Fatalf("recovered = %+v", recovered)
	}
	if err := store.Attempt(ctx, request.WorkspaceID, request.RequestID); !errors.Is(err, ErrEmbeddingCallRecoveryRequired) {
		t.Fatalf("retry attempt error = %v, want recovery-required", err)
	}
}

func TestEmbeddingCallStoreResolvesProviderReconciliationAndIsIdempotent(t *testing.T) {
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := NewEmbeddingCallStore(pool)
	request := embeddingStoreTestRequest()
	request.WorkspaceID = fmt.Sprintf("embedding-reconcile-%d", time.Now().UnixNano())
	request.Actor.WorkspaceID = request.WorkspaceID
	request.RequestID = "embedding-reconcile-request"
	request.IdempotencyKey = "embedding-reconcile-key"
	if _, err := store.Start(ctx, request, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Attempt(ctx, request.WorkspaceID, request.RequestID); err != nil {
		t.Fatal(err)
	}
	if err := store.Finish(ctx, contracts.EmbeddingCallResult{WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, Status: contracts.EmbeddingCallRecoveryRequired, ProviderRequestID: "provider-receipt-1", Failure: &contracts.EmbeddingFailure{Code: contracts.EmbeddingFailureTransport, PossiblyStarted: true}}); err != nil {
		t.Fatal(err)
	}
	record, err := store.Get(ctx, request.WorkspaceID, request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	vector := make([]float32, contracts.EmbeddingDimension)
	for i := range vector {
		vector[i] = float32((i%13)+1) / 13
	}
	if err := store.ResolveRecovery(ctx, contracts.EmbeddingReconciliationResult{WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, Status: contracts.EmbeddingCallSucceeded, Provider: request.Provider, SourceHash: record.SourceHash, ProviderRequestID: "provider-receipt-1", Vector: vector}); err != nil {
		t.Fatal(err)
	}
	resolved, err := store.Get(ctx, request.WorkspaceID, request.RequestID)
	if err != nil || resolved.Status != contracts.EmbeddingCallSucceeded || len(resolved.Vector) != contracts.EmbeddingDimension {
		t.Fatalf("resolved = %+v err=%v", resolved, err)
	}
	if err := store.ResolveRecovery(ctx, contracts.EmbeddingReconciliationResult{WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, Status: contracts.EmbeddingCallSucceeded, Provider: request.Provider, SourceHash: record.SourceHash, ProviderRequestID: "provider-receipt-1", Vector: vector}); err != nil {
		t.Fatalf("duplicate reconciliation: %v", err)
	}
	var attempts int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.embedding_call_reconciliations WHERE workspace_id=$1 AND request_id=$2`, request.WorkspaceID, request.RequestID).Scan(&attempts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("reconciliation attempts = %d, want 1", attempts)
	}
}

func TestEmbeddingCallStoreExpiresOnlyUnreferencedQueryVectors(t *testing.T) {
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := NewEmbeddingCallStore(pool)
	request := embeddingStoreTestRequest()
	request.WorkspaceID = fmt.Sprintf("embedding-retention-%d", time.Now().UnixNano())
	request.Actor.WorkspaceID = request.WorkspaceID
	request.RequestID = "embedding-query-request"
	request.IdempotencyKey = "embedding-query-key"
	request.SourceKind = "memo_query"
	if _, err := store.Start(ctx, request, nil); err != nil {
		t.Fatal(err)
	}
	if err := store.Attempt(ctx, request.WorkspaceID, request.RequestID); err != nil {
		t.Fatal(err)
	}
	vector := make([]float32, contracts.EmbeddingDimension)
	for i := range vector {
		vector[i] = float32(i%7) / 7
	}
	if err := store.Finish(ctx, contracts.EmbeddingCallResult{WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, Status: contracts.EmbeddingCallSucceeded, Vector: vector}); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE fornix.embedding_calls SET retention_deadline=clock_timestamp()-interval '1 second' WHERE workspace_id=$1 AND request_id=$2`, request.WorkspaceID, request.RequestID); err != nil {
		t.Fatal(err)
	}
	dry, err := store.SweepExpiredQueryVectors(ctx, contracts.EmbeddingRetentionSweepRequest{WorkspaceID: request.WorkspaceID, Before: time.Now().UTC(), BatchSize: 8, DryRun: true})
	if err != nil || dry.Candidates != 1 || dry.Expired != 0 {
		t.Fatalf("dry sweep = %+v err=%v", dry, err)
	}
	expired, err := store.SweepExpiredQueryVectors(ctx, contracts.EmbeddingRetentionSweepRequest{WorkspaceID: request.WorkspaceID, Before: time.Now().UTC(), BatchSize: 8})
	if err != nil || expired.Expired != 1 {
		t.Fatalf("sweep = %+v err=%v", expired, err)
	}
	record, err := store.Get(ctx, request.WorkspaceID, request.RequestID)
	if err != nil || record.Status != contracts.EmbeddingCallExpired || record.Vector != nil || record.VectorHash == "" || record.TombstoneHash == "" {
		t.Fatalf("expired record = %+v err=%v", record, err)
	}
}

func embeddingStoreTestRequest() contracts.EmbeddingRequest {
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	workspace := "embedding-store-workspace-" + suffix
	return contracts.EmbeddingRequest{
		RequestID: "embedding-store-request-" + suffix, IdempotencyKey: "embedding-store-key-" + suffix, WorkspaceID: workspace,
		Actor:    contracts.ActorRef{ID: "embedding-store-actor", Kind: "test", WorkspaceID: workspace},
		Provider: contracts.ProviderRef{Provider: "fake", Model: "fake-model"}, Model: "fake-model",
		SourceKind: "test", SourceID: "source-1", Text: "durable embedding source",
		Metadata: map[string]string{"caller_note": "raw user text must not persist"},
		Budget:   contracts.EmbeddingBudget{MaxInputBytes: contracts.MaxEmbeddingInputBytes, Dimension: contracts.EmbeddingDimension, TimeoutMS: 1000},
	}
}
