package eval

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func TestReplayCaseIsStableAndDoesNotNeedProviders(t *testing.T) {
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
	workspace := fmt.Sprintf("test-eval-replay-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.eval_results WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.eval_runs WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.eval_datasets WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.agent_runs WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspace)
		pool.Close()
	})
	runs := store.NewAgentRunStore(pool, store.NewEventStore(pool))
	run, _, err := runs.Reserve(ctx, contracts.AgentRunRequest{RunID: "run-replay", RequestID: "request-replay", IdempotencyKey: "replay-run", WorkspaceID: workspace, Goal: "offline fake-provider replay", Provider: contracts.ProviderRef{Provider: "fake", Model: "fake-model"}, Budget: contracts.DefaultAgentBudget()})
	if err != nil {
		t.Fatal(err)
	}
	next := run
	next.State, next.Termination = contracts.AgentRunSucceeded, contracts.AgentTerminationCompleted
	next.History = []contracts.ModelMessage{{Role: "assistant", Content: "stable fake answer"}}
	next.LastOutput = "stable fake answer"
	committed, err := runs.Commit(ctx, run, next, contracts.AgentEventCompleted, map[string]any{"run_id": run.ID, "offline": true})
	if err != nil {
		t.Fatal(err)
	}
	caseInput := contracts.EvalCase{ID: "case-1", ReplayRunID: committed.ID, InputHash: committed.RequestHash, ExpectedTermination: contracts.AgentTerminationCompleted}
	first, err := ReplayCase(ctx, runs, workspace, caseInput)
	if err != nil {
		t.Fatal(err)
	}
	second, err := ReplayCase(ctx, runs, workspace, caseInput)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Passed || first.ReplayHash == "" || first.ReplayHash != second.ReplayHash || first.ReplayRunID != committed.ID {
		t.Fatalf("unstable replay: first=%+v second=%+v", first, second)
	}
	wrong := caseInput
	wrong.ExpectedTermination = contracts.AgentTerminationAbstained
	failed, err := ReplayCase(ctx, runs, workspace, wrong)
	if err != nil {
		t.Fatal(err)
	}
	if failed.Passed {
		t.Fatal("incorrect termination unexpectedly passed evaluation")
	}
}

func TestEvaluationRunnerIsBoundedAndDryRunIsReadOnly(t *testing.T) {
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
	workspace := fmt.Sprintf("test-eval-runner-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.eval_results WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.eval_runs WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.eval_datasets WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.agent_runs WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(ctx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspace)
		pool.Close()
	})
	events := store.NewEventStore(pool)
	runs := store.NewAgentRunStore(pool, events)
	recorded, _, err := runs.Reserve(ctx, contracts.AgentRunRequest{RunID: "runner-recorded", RequestID: "runner-request", IdempotencyKey: "runner-recorded-key", WorkspaceID: workspace, Goal: "offline evaluation", Provider: contracts.ProviderRef{Provider: "fake", Model: "fake-model"}, Budget: contracts.DefaultAgentBudget()})
	if err != nil {
		t.Fatal(err)
	}
	recorded.State, recorded.Termination = contracts.AgentRunSucceeded, contracts.AgentTerminationCompleted
	recorded.LastOutput = "stable"
	if _, err := runs.Commit(ctx, recorded, recorded, contracts.AgentEventCompleted, map[string]any{"offline": true}); err != nil {
		t.Fatal(err)
	}
	dataset, _, err := store.NewEvaluationStore(pool).CreateDataset(ctx, contracts.EvalDataset{WorkspaceID: workspace, Name: "runner", Version: 1, Cases: []contracts.EvalCase{{ID: "case-1", ReplayRunID: recorded.ID, InputHash: recorded.RequestHash, ExpectedTermination: contracts.AgentTerminationCompleted}}})
	if err != nil {
		t.Fatal(err)
	}
	evals := store.NewEvaluationStore(pool)
	runner := Runner{Runs: runs, Evaluations: evals}
	dryRun, results, err := runner.EvaluateDataset(ctx, dataset, contracts.EvalRun{WorkspaceID: workspace, DatasetID: dataset.ID, DatasetHash: dataset.DatasetHash, IdempotencyKey: "runner-dry", DryRun: true, BatchLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if dryRun.Status != contracts.EvalRunSucceeded || len(results) != 1 || dryRun.ReplayHash == "" {
		t.Fatalf("unexpected dry-run result: run=%+v results=%+v", dryRun, results)
	}
	var dryRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.eval_runs WHERE workspace_id=$1 AND idempotency_key='runner-dry'`, workspace).Scan(&dryRows); err != nil {
		t.Fatal(err)
	}
	if dryRows != 0 {
		t.Fatal("dry-run wrote an evaluation run")
	}
	finished, persisted, err := runner.EvaluateDataset(ctx, dataset, contracts.EvalRun{WorkspaceID: workspace, DatasetID: dataset.ID, DatasetHash: dataset.DatasetHash, IdempotencyKey: "runner-persisted", BatchLimit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if finished.Status != contracts.EvalRunSucceeded || len(persisted) != 1 || finished.ReplayHash == "" {
		t.Fatalf("unexpected persisted evaluation: %+v", finished)
	}
	var resultRows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.eval_results WHERE workspace_id=$1 AND eval_run_id=$2`, workspace, finished.ID).Scan(&resultRows); err != nil {
		t.Fatal(err)
	}
	if resultRows != 1 {
		t.Fatalf("expected one durable result, got %d", resultRows)
	}
}
