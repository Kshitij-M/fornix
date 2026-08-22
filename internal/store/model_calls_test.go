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
	"github.com/omaveda/fornix/internal/model"
)

func newModelCallTestStore(t *testing.T) (*ModelCallStore, *pgxpool.Pool, string) {
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
	workspaceID := fmt.Sprintf("test-model-calls-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.model_calls WHERE workspace_id=$1`, workspaceID)
		pool.Close()
	})
	return NewModelCallStore(pool), pool, workspaceID
}

func modelCallRequest(workspaceID, key string) contracts.ModelRequest {
	request := contracts.NewModelRequest(workspaceID, "fake", "fake-model", "durable model call")
	request.RequestID = "request-" + key
	request.IdempotencyKey = key
	return request
}

func TestModelCallStoreConcurrentStartDeduplicates(t *testing.T) {
	store, pool, workspaceID := newModelCallTestStore(t)
	request := modelCallRequest(workspaceID, "concurrent-model-call")
	evidence := []byte(`{"prompt":"durable model call"}`)
	const workers = 12
	results := make(chan model.CallStart, workers)
	errorsCh := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := store.Start(context.Background(), request, evidence)
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
		t.Fatal(err)
	}
	created := 0
	var firstID int64
	for result := range results {
		if !result.Existing {
			created++
		}
		if firstID == 0 {
			firstID = result.Record.ID
		}
		if result.Record.ID != firstID || result.Record.RequestHash == "" {
			t.Fatalf("duplicate returned a different model call: %+v", result.Record)
		}
	}
	if created != 1 {
		t.Fatalf("new model call count = %d, want 1", created)
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.model_calls WHERE workspace_id=$1 AND idempotency_key=$2`, workspaceID, request.IdempotencyKey).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("durable model call rows = %d, want 1", count)
	}
}

func TestModelCallStoreRequestIDConflictResolvesAndRejectsHashMismatch(t *testing.T) {
	store, _, workspaceID := newModelCallTestStore(t)
	first := modelCallRequest(workspaceID, "request-identity-first")
	first.RequestID = "shared-request-id"
	if _, err := store.Start(context.Background(), first, []byte(`{"request":"stable"}`)); err != nil {
		t.Fatal(err)
	}

	// A retry may arrive with a regenerated idempotency key while retaining
	// the same logical request ID. The second unique index must resolve to the
	// original durable call rather than surfacing a raw constraint error.
	retry := first
	retry.IdempotencyKey = "request-identity-retry"
	replayed, err := store.Start(context.Background(), retry, []byte(`{"request":"stable"}`))
	if err != nil || !replayed.Existing || replayed.Record.RequestID != first.RequestID {
		t.Fatalf("request-id replay = %+v err=%v", replayed, err)
	}

	conflict := retry
	conflict.Metadata = map[string]string{"different": "payload"}
	if _, err := store.Start(context.Background(), conflict, []byte(`{"request":"stable"}`)); !errors.Is(err, ErrModelCallConflict) {
		t.Fatalf("request-id hash conflict = %v", err)
	}
}

func TestModelCallStoreTerminalReplayAndWorkspaceIsolation(t *testing.T) {
	store, _, workspaceID := newModelCallTestStore(t)
	ctx := context.Background()
	request := modelCallRequest(workspaceID, "terminal-model-call")
	request.Metadata = map[string]string{"purpose": "integration-test"}
	request.CausationID = "event-causation-1"
	request.CorrelationID = "run-correlation-1"
	start, err := store.Start(ctx, request, []byte(`{"request":"safe"}`))
	if err != nil || start.Existing {
		t.Fatalf("start = %+v err=%v", start, err)
	}
	response := contracts.ModelResponse{RequestID: request.RequestID, Provider: request.Provider, Content: "stable answer", FinishReason: "stop", Usage: contracts.ModelUsage{InputTokens: 4, OutputTokens: 2}, Cost: contracts.ModelCost{Currency: "USD", TotalCostUSD: 0.001}}
	if err := store.Attempt(ctx, workspaceID, request.RequestID); err != nil {
		t.Fatal(err)
	}
	result := contracts.ModelCallResult{WorkspaceID: workspaceID, RequestID: request.RequestID, Status: contracts.ModelCallSucceeded, AttemptCount: 1, Response: &response, Usage: response.Usage, Cost: response.Cost, ResponseEvidence: []byte(`{"provider":"fake"}`)}
	if err := store.Finish(ctx, result); err != nil {
		t.Fatal(err)
	}
	finished, err := store.Get(ctx, workspaceID, request.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	if finished.ResponseArtifact == nil || finished.ResponseArtifact.ContentHash != contracts.ArtifactContentHash(result.ResponseEvidence) || finished.ResponseArtifact.WorkspaceID != workspaceID {
		t.Fatalf("model response artifact was not durably linked: %+v", finished.ResponseArtifact)
	}
	if err := store.Finish(ctx, result); err != nil {
		t.Fatalf("idempotent terminal finish failed: %v", err)
	}
	replayed, err := store.Start(ctx, request, []byte(`{"request":"safe"}`))
	if err != nil || !replayed.Existing || replayed.Record.Response == nil || replayed.Record.Response.Content != "stable answer" || replayed.Record.Metadata["purpose"] != "integration-test" || replayed.Record.CausationID != request.CausationID || replayed.Record.CorrelationID != request.CorrelationID || replayed.Record.SchemaVersion != contracts.ModelSchemaVersion {
		t.Fatalf("replayed model call = %+v err=%v", replayed, err)
	}

	other := modelCallRequest(workspaceID+"-other", request.IdempotencyKey)
	otherStart, err := store.Start(ctx, other, []byte(`{"request":"safe"}`))
	if err != nil || otherStart.Existing {
		t.Fatalf("cross-workspace idempotency was not isolated: %+v err=%v", otherStart, err)
	}

	conflict := request
	conflict.RequestID = "request-conflict"
	conflict.Prompt = "different request"
	_, err = store.Start(ctx, conflict, []byte(`{"request":"different"}`))
	if !errors.Is(err, ErrModelCallConflict) {
		t.Fatalf("idempotency conflict error = %v", err)
	}
}

func TestModelCallResponseArtifactCrashRollsBackLedgerAndReference(t *testing.T) {
	store, pool, workspaceID := newModelCallTestStore(t)
	ctx := context.Background()
	request := modelCallRequest(workspaceID, "model-artifact-crash")
	if _, err := store.Start(ctx, request, []byte(`{"prompt":"crash-safe"}`)); err != nil {
		t.Fatal(err)
	}
	if err := store.Attempt(ctx, workspaceID, request.RequestID); err != nil {
		t.Fatal(err)
	}
	store.SetArtifactFailureHook(func(stage string) error {
		if stage == "reference_inserted" {
			return errors.New("injected model artifact crash")
		}
		return nil
	})
	err := store.Finish(ctx, contracts.ModelCallResult{
		WorkspaceID: workspaceID, RequestID: request.RequestID, Status: contracts.ModelCallSucceeded,
		Response:         &contracts.ModelResponse{RequestID: request.RequestID, Provider: request.Provider, Content: "crash-safe"},
		ResponseEvidence: []byte(`{"answer":"crash-safe"}`),
	})
	store.SetArtifactFailureHook(nil)
	if err == nil || !strings.Contains(err.Error(), "injected model artifact crash") {
		t.Fatalf("finish crash error=%v", err)
	}
	recovered, err := store.Get(ctx, workspaceID, request.IdempotencyKey)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Status != contracts.ModelCallRunning || recovered.ResponseArtifact != nil {
		t.Fatalf("model ledger changed despite rolled back artifact transaction: %+v", recovered)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.artifact_refs WHERE workspace_id=$1 AND source_kind='model_call' AND source_id=$2`, workspaceID, request.RequestID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("crash left model artifact reference count=%d", count)
	}
}
