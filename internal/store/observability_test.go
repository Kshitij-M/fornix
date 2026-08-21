package store

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

func newObservabilityTestStore(t *testing.T) (*ObservabilityStore, *pgxpool.Pool, string) {
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
	workspace := fmt.Sprintf("test-observability-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanup, `DELETE FROM fornix.eval_results WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanup, `DELETE FROM fornix.eval_runs WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanup, `DELETE FROM fornix.eval_datasets WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanup, `DELETE FROM fornix.metric_samples WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanup, `DELETE FROM fornix.cost_ledger WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanup, `DELETE FROM fornix.trace_spans WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanup, `DELETE FROM fornix.run_observations WHERE workspace_id=$1`, workspace)
		pool.Close()
	})
	return NewObservabilityStore(pool), pool, workspace
}

func testObservation(workspace, key string) contracts.RunObservation {
	return contracts.RunObservation{WorkspaceID: workspace, IdempotencyKey: key, Kind: contracts.ObservationTool, Component: "test", Operation: "execute", Outcome: contracts.OutcomeSucceeded, SourceKind: "test", SourceID: key, Evidence: []byte(`{"authorization":"should-not-persist","result":"stable"}`)}
}

func TestObservabilityDuplicateObservationIsIdempotentAndRedacted(t *testing.T) {
	obs, pool, workspace := newObservabilityTestStore(t)
	input := testObservation(workspace, "same-observation")
	const writers = 12
	results := make(chan contracts.RunObservation, writers)
	errorsCh := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stored, _, err := obs.RecordObservation(context.Background(), input)
			if err != nil {
				errorsCh <- err
				return
			}
			results <- stored
		}()
	}
	wg.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		t.Fatal(err)
	}
	for stored := range results {
		if stored.PayloadHash == "" || strings.Contains(string(stored.Evidence), "should-not-persist") {
			t.Fatalf("unsafe observation evidence: %+v", stored)
		}
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.run_observations WHERE workspace_id=$1 AND idempotency_key=$2`, workspace, input.IdempotencyKey).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("observation count=%d, want 1", count)
	}
	conflict := input
	conflict.Evidence = []byte(`{"result":"different"}`)
	if _, _, err := obs.RecordObservation(context.Background(), conflict); err == nil {
		t.Fatal("expected payload conflict")
	}
}

func TestObservabilityCostSnapshotIsWorkspaceScopedAndDistinguishesEstimate(t *testing.T) {
	obs, _, workspace := newObservabilityTestStore(t)
	other := workspace + "-other"
	_, _, err := obs.RecordCost(context.Background(), contracts.CostLedgerEntry{WorkspaceID: workspace, IdempotencyKey: "measured", Category: contracts.CostModel, Basis: "provider_usage", SourceKind: "model_call", SourceID: "call-1", AmountUSD: 0.25, AmountKnown: true, Measured: true})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = obs.RecordCost(context.Background(), contracts.CostLedgerEntry{WorkspaceID: workspace, IdempotencyKey: "estimated", Category: contracts.CostRetrieval, Basis: "db_queries", SourceKind: "retrieval", SourceID: "retrieval-1", AmountUSD: 0.10, AmountKnown: true, Estimated: true})
	if err != nil {
		t.Fatal(err)
	}
	_, _, err = obs.RecordCost(context.Background(), contracts.CostLedgerEntry{WorkspaceID: other, IdempotencyKey: "other", Category: contracts.CostTool, Basis: "duration_ms", SourceKind: "tool_run", SourceID: "tool-1"})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := obs.Snapshot(context.Background(), workspace, time.Now().UTC().Add(-time.Minute), time.Now().UTC().Add(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.MeasuredCostUSD != 0.25 || snapshot.EstimatedCostUSD != 0.10 || snapshot.UnknownCostEntries != 0 {
		t.Fatalf("unexpected cost snapshot: %+v", snapshot)
	}
	if len(snapshot.Costs) != 2 {
		t.Fatalf("workspace leaked cost categories: %+v", snapshot.Costs)
	}
}

func TestModelUsageReconcilesIntoDurableCostLedger(t *testing.T) {
	obs, pool, workspace := newObservabilityTestStore(t)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM fornix.model_calls WHERE workspace_id=$1`, workspace)
	})
	modelCalls := NewModelCallStore(pool)
	modelCalls.SetObservability(obs)
	request := contracts.NewModelRequest(workspace, "fake", "fake-model", "redacted test prompt")
	request.RequestID, request.IdempotencyKey = "model-observed-request", "model-observed-key"
	started, err := modelCalls.Start(context.Background(), request, []byte(`{"request":"redacted"}`))
	if err != nil || started.Existing {
		t.Fatalf("start=%+v err=%v", started, err)
	}
	if err := modelCalls.Attempt(context.Background(), workspace, request.RequestID); err != nil {
		t.Fatal(err)
	}
	usage := contracts.ModelUsage{InputTokens: 12, OutputTokens: 8, TotalTokens: 20, Source: "provider"}
	cost := contracts.ModelCost{Currency: "USD", TotalCostUSD: 0.004, Source: "configured_price"}
	if err := modelCalls.Finish(context.Background(), contracts.ModelCallResult{WorkspaceID: workspace, RequestID: request.RequestID, Status: contracts.ModelCallSucceeded, AttemptCount: 1, Usage: usage, Cost: cost, Response: &contracts.ModelResponse{RequestID: request.RequestID, Provider: request.Provider, Content: "offline"}, ResponseEvidence: []byte(`{"content":"offline"}`)}); err != nil {
		t.Fatal(err)
	}
	var amount float64
	var measured bool
	if err := pool.QueryRow(context.Background(), `SELECT amount_usd, measured FROM fornix.cost_ledger WHERE workspace_id=$1 AND idempotency_key=$2`, workspace, "model-cost:"+request.RequestID).Scan(&amount, &measured); err != nil {
		t.Fatal(err)
	}
	if amount != cost.TotalCostUSD || !measured {
		t.Fatalf("model cost did not reconcile: amount=%v measured=%t", amount, measured)
	}
	var inputTokens, outputTokens int
	if err := pool.QueryRow(context.Background(), `SELECT input_tokens, output_tokens FROM fornix.run_observations WHERE workspace_id=$1 AND idempotency_key=$2`, workspace, "model-observation:"+request.RequestID).Scan(&inputTokens, &outputTokens); err != nil {
		t.Fatal(err)
	}
	if inputTokens != usage.InputTokens || outputTokens != usage.OutputTokens {
		t.Fatalf("model usage did not reconcile: input=%d output=%d", inputTokens, outputTokens)
	}
}

func TestObservabilityMetricDimensionsRejectUnboundedInput(t *testing.T) {
	bad := contracts.MetricSample{WorkspaceID: "w", IdempotencyKey: "m", Name: strings.Repeat("x", contracts.MaxDimensionLength+1), Value: 1}
	if err := bad.Normalize(); err == nil {
		t.Fatal("expected bounded metric name rejection")
	}
	bad = contracts.MetricSample{WorkspaceID: "w", IdempotencyKey: "m", Name: "latency", Value: 1, Dimensions: contracts.MetricDimensions{Operation: "prompt should not be a dimension"}}
	if err := bad.Normalize(); err == nil {
		t.Fatal("expected prompt-like metric dimension rejection")
	}
}

func TestEvaluationStoreDatasetRunAndResultAreIdempotent(t *testing.T) {
	obs, pool, workspace := newObservabilityTestStore(t)
	evals := NewEvaluationStore(pool)
	dataset, created, err := evals.CreateDataset(context.Background(), contracts.EvalDataset{WorkspaceID: workspace, Name: "offline", Version: 1, Cases: []contracts.EvalCase{{ID: "case-1", ReplayRunID: "run-1", InputHash: strings.Repeat("a", 64)}}})
	if err != nil || !created {
		t.Fatalf("create dataset created=%t err=%v", created, err)
	}
	duplicate, duplicateCreated, err := evals.CreateDataset(context.Background(), dataset)
	if err != nil || duplicateCreated || duplicate.DatasetHash != dataset.DatasetHash {
		t.Fatalf("duplicate dataset created=%t duplicate=%+v err=%v", duplicateCreated, duplicate, err)
	}
	run, runCreated, err := evals.StartRun(context.Background(), contracts.EvalRun{WorkspaceID: workspace, DatasetID: dataset.ID, DatasetHash: dataset.DatasetHash, IdempotencyKey: "eval-run-1", RequestHash: "request-hash"})
	if err != nil || !runCreated {
		t.Fatalf("start eval run created=%t err=%v", runCreated, err)
	}
	result := contracts.EvalResult{WorkspaceID: workspace, EvalRunID: run.ID, CaseID: "case-1", ReplayRunID: "run-1", InputHash: strings.Repeat("a", 64), ReplayHash: strings.Repeat("b", 64), Passed: true, Gates: []contracts.QualityGate{{Name: "stable", Metric: "stable", Operator: "==", Threshold: 1, Actual: 1, Passed: true}}}
	_, resultCreated, err := evals.RecordResult(context.Background(), result)
	if err != nil || !resultCreated {
		t.Fatalf("record result created=%t err=%v", resultCreated, err)
	}
	_, resultDuplicate, err := evals.RecordResult(context.Background(), result)
	if err != nil || resultDuplicate {
		t.Fatalf("duplicate result created=%t err=%v", resultDuplicate, err)
	}
	stored, err := evals.GetRun(context.Background(), workspace, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.CasesCompleted != 1 || stored.CasesPassed != 1 {
		t.Fatalf("result count was not idempotent: %+v", stored)
	}
	_ = obs
}

func TestEvaluationStoreOversizedReportUsesStableArtifact(t *testing.T) {
	_, pool, workspace := newObservabilityTestStore(t)
	evals := NewEvaluationStore(pool)
	ctx := context.Background()
	dataset, _, err := evals.CreateDataset(ctx, contracts.EvalDataset{
		WorkspaceID: workspace, Name: "artifact-report", Version: 1,
		Cases: []contracts.EvalCase{{ID: "case-1", ReplayRunID: "surface-1", InputHash: strings.Repeat("a", 64)}},
	})
	if err != nil {
		t.Fatal(err)
	}
	run, _, err := evals.StartRun(ctx, contracts.EvalRun{
		WorkspaceID: workspace, DatasetID: dataset.ID, DatasetHash: dataset.DatasetHash,
		IdempotencyKey: "artifact-report-run", RequestHash: strings.Repeat("b", 64), BatchLimit: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	report := []byte(`{"report":"` + strings.Repeat("x", contracts.MaxEvalInlineBytes+1024) + `"}`)
	finished, err := evals.FinishRun(ctx, run, report)
	if err != nil {
		t.Fatal(err)
	}
	if finished.ReportArtifact == nil || len(finished.Report) != 0 {
		t.Fatalf("oversized report was not artifact-backed: %+v", finished)
	}
	stored, err := evals.GetRun(ctx, workspace, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.ReportArtifact == nil || stored.ReportArtifact.ContentHash != finished.ReportArtifact.ContentHash {
		t.Fatalf("report artifact reference was not stable: stored=%+v finished=%+v", stored.ReportArtifact, finished.ReportArtifact)
	}
	disclosed, err := NewArtifactStore(pool).Disclose(ctx, contracts.ArtifactDisclosureRequest{
		WorkspaceID: workspace, ArtifactID: stored.ReportArtifact.ArtifactID,
		Level: contracts.ArtifactDisclosureRaw, MaxBytes: len(report) + 128,
		MaxTokens: contracts.EstimateTokens(string(report)) + 128, MaxItems: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(disclosed.Raw) != string(report) || disclosed.ContentHash != stored.ReportArtifact.ContentHash || !disclosed.IntegrityVerified {
		t.Fatalf("artifact report disclosure changed content: hash=%s integrity=%t", disclosed.ContentHash, disclosed.IntegrityVerified)
	}
}
