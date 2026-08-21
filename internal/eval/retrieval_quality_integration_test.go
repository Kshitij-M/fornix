package eval

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func TestEvaluateRetrievalDatasetResolvesEvidenceAndPersistsQuality(t *testing.T) {
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if err := store.ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	workspace := fmt.Sprintf("test-retrieval-quality-%d", time.Now().UnixNano())
	otherWorkspace := workspace + "-other"
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.eval_results WHERE workspace_id=ANY($1)`, []string{workspace, otherWorkspace})
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.eval_runs WHERE workspace_id=ANY($1)`, []string{workspace, otherWorkspace})
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.eval_datasets WHERE workspace_id=ANY($1)`, []string{workspace, otherWorkspace})
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.evidence_records WHERE workspace_id=ANY($1)`, []string{workspace, otherWorkspace})
		pool.Close()
	})

	evidence := store.NewEvidenceStore(pool)
	goldRaw := []byte("authoritative gold evidence")
	goldHash := hashBytes(goldRaw)
	if _, err := evidence.Put(ctx, store.EvidencePutInput{WorkspaceID: workspace, SourceReference: "memo:gold", Kind: "memo", Gist: "gold evidence", RawPayload: goldRaw}); err != nil {
		t.Fatal(err)
	}
	otherRaw := []byte("other workspace evidence")
	otherHash := hashBytes(otherRaw)
	if _, err := evidence.Put(ctx, store.EvidencePutInput{WorkspaceID: otherWorkspace, SourceReference: "memo:other", Kind: "memo", Gist: "other evidence", RawPayload: otherRaw}); err != nil {
		t.Fatal(err)
	}
	resolved, err := evidence.ResolveEvidenceHashes(ctx, workspace, []string{goldHash, goldHash})
	if err != nil || len(resolved) != 1 || resolved[0].EvidenceHash != goldHash {
		t.Fatalf("gold resolution failed: resolved=%+v err=%v", resolved, err)
	}
	if _, err := evidence.ResolveEvidenceHashes(ctx, workspace, []string{otherHash}); err == nil {
		t.Fatal("cross-workspace gold unexpectedly resolved")
	}

	evals := store.NewEvaluationStore(pool)
	dataset, _, err := evals.CreateDataset(ctx, contracts.EvalDataset{WorkspaceID: workspace, Name: "retrieval-quality", Version: 1, Cases: []contracts.EvalCase{{ID: "case-1", ReplayRunID: "recorded-run", InputHash: fmt.Sprintf("%064d", 1), GoldEvidence: []string{goldHash}, ExpectedContextHash: "ctx-1", RetrievalK: 1}}})
	if err != nil {
		t.Fatal(err)
	}
	surface := RetrievalScoreInput{WorkspaceID: workspace, Pack: contracts.ContextPack{WorkspaceID: workspace, ContentHash: "ctx-1", Items: []contracts.ContextItem{{WorkspaceID: workspace, SourceReference: "memo:gold", EvidenceHash: goldHash, Score: 1, Text: "bounded"}}}, Measurement: RetrievalMeasurement{LatencyMS: 3, SQLQueries: 2, CostUSD: 0.01, CostKnown: true}}
	runner := Runner{Evaluations: evals, Evidence: evidence}
	gate := contracts.QualityGate{Name: "recall", Metric: "recall_at_k", Operator: ">=", Threshold: 1}
	dryRun, results, err := runner.EvaluateRetrievalDataset(ctx, dataset, contracts.EvalRun{WorkspaceID: workspace, DatasetID: dataset.ID, DatasetHash: dataset.DatasetHash, IdempotencyKey: "dry-quality", DryRun: true, BatchLimit: 1, Gates: []contracts.QualityGate{gate}}, map[string]RetrievalScoreInput{"case-1": surface}, RegressionPolicy{})
	if err != nil || dryRun.Status != contracts.EvalRunSucceeded || len(results) != 1 || results[0].RetrievalMetrics == nil {
		t.Fatalf("unexpected dry retrieval evaluation: run=%+v results=%+v err=%v", dryRun, results, err)
	}
	var dryRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.eval_runs WHERE workspace_id=$1 AND idempotency_key='dry-quality'`, workspace).Scan(&dryRows); err != nil {
		t.Fatal(err)
	}
	if dryRows != 0 {
		t.Fatal("dry retrieval evaluation wrote durable rows")
	}

	finished, persisted, err := runner.EvaluateRetrievalDataset(ctx, dataset, contracts.EvalRun{WorkspaceID: workspace, DatasetID: dataset.ID, DatasetHash: dataset.DatasetHash, IdempotencyKey: "persisted-quality", BatchLimit: 1, Gates: []contracts.QualityGate{gate}}, map[string]RetrievalScoreInput{"case-1": surface}, RegressionPolicy{})
	if err != nil || finished.Status != contracts.EvalRunSucceeded || len(persisted) != 1 {
		t.Fatalf("unexpected persisted retrieval evaluation: run=%+v results=%+v err=%v", finished, persisted, err)
	}
	var storedMetrics, storedEvidence []byte
	if err := pool.QueryRow(ctx, `SELECT retrieval_metrics, resolved_evidence FROM fornix.eval_results WHERE workspace_id=$1 AND eval_run_id=$2 AND case_id='case-1'`, workspace, finished.ID).Scan(&storedMetrics, &storedEvidence); err != nil {
		t.Fatal(err)
	}
	if len(storedMetrics) == 0 || len(storedEvidence) == 0 {
		t.Fatal("durable retrieval quality payload was empty")
	}
	_, duplicate, err := evals.RecordResult(ctx, persisted[0])
	if err != nil || duplicate {
		t.Fatalf("duplicate retrieval result was not idempotent: created=%t err=%v", duplicate, err)
	}
	repeated, repeatedResults, err := runner.EvaluateRetrievalDataset(ctx, dataset, contracts.EvalRun{WorkspaceID: workspace, DatasetID: dataset.ID, DatasetHash: dataset.DatasetHash, IdempotencyKey: "persisted-quality", BatchLimit: 1, Gates: []contracts.QualityGate{gate}}, map[string]RetrievalScoreInput{"case-1": surface}, RegressionPolicy{})
	if err != nil || repeated.ID != finished.ID || len(repeatedResults) != 1 {
		t.Fatalf("duplicate evaluation run was not safely replayable: run=%+v results=%+v err=%v", repeated, repeatedResults, err)
	}
}

func hashBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}
