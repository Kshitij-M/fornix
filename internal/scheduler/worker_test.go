package scheduler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/agentloop"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/model"
	"github.com/omaveda/fornix/internal/store"
	"github.com/omaveda/fornix/internal/tool"
)

func newWorkerTestHarness(t *testing.T) (*store.AgentRunStore, *pgxpool.Pool, string) {
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
	if err := store.ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	workspace := fmt.Sprintf("test-scheduler-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.agent_run_worker_leases WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.agent_runs WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspace)
		pool.Close()
	})
	return store.NewAgentRunStore(pool, store.NewEventStore(pool)), pool, workspace
}

func workerTestRequest(workspace, key string) contracts.AgentRunRequest {
	return contracts.AgentRunRequest{
		RunID: "run-" + key, RequestID: "request-" + key, IdempotencyKey: key,
		WorkspaceID: workspace, Actor: contracts.ActorRef{ID: "scheduler-test", Kind: "worker"},
		Goal: "run scheduler test", Provider: contracts.ProviderRef{Provider: "fake", Model: "fake-model"},
		Budget: contracts.AgentBudget{MaxTurns: 2, MaxModelSteps: 2, MaxToolCalls: 2, MaxContextBytes: 4096, MaxOutputTokens: 64, MaxWallTimeMS: 60_000, MaxCostUSD: 1, MaxToolAttempts: 2},
	}
}

type workerTestTools struct{}

func (workerTestTools) Execute(context.Context, contracts.ToolRequest) (tool.Outcome, error) {
	return tool.Outcome{}, errors.New("no tool is registered in this worker test")
}

func (workerTestTools) Definition(string) (contracts.ToolDefinition, bool) {
	return contracts.ToolDefinition{}, false
}

func newFakeLoop(runs *store.AgentRunStore, response string) (*agentloop.Orchestrator, *model.FakeProvider) {
	registry := model.NewRegistry()
	fake := model.NewFakeProvider(model.FakeConfig{Response: response})
	_ = registry.Register(fake)
	return agentloop.New(runs, model.NewGateway(registry, nil), workerTestTools{}), fake
}

func TestWorkerRunOnceResumesDueRunAndReleasesLease(t *testing.T) {
	runs, _, workspace := newWorkerTestHarness(t)
	ctx := context.Background()
	if _, _, err := runs.Reserve(ctx, workerTestRequest(workspace, "run-once")); err != nil {
		t.Fatal(err)
	}
	loop, fake := newFakeLoop(runs, "scheduler response")
	worker := NewWorker(runs, loop, "worker-a")
	worker.LeaseTTL = 300 * time.Millisecond
	worker.HeartbeatInterval = 20 * time.Millisecond
	result, err := worker.RunOnce(ctx, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Claimed || result.Decision.Run.State != contracts.AgentRunSucceeded || fake.Calls() != 1 {
		t.Fatalf("worker result=%+v fake_calls=%d", result, fake.Calls())
	}
	if _, active, err := runs.GetAgentRunLease(ctx, workspace, result.Run.ID); err != nil || active {
		t.Fatalf("worker lease active after release=%t err=%v", active, err)
	}
	duplicate, err := worker.RunOnce(ctx, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Claimed {
		t.Fatalf("terminal run was delivered again: %+v", duplicate)
	}
}

func TestWorkerAutomaticallyResumesDueRetry(t *testing.T) {
	runs, _, workspace := newWorkerTestHarness(t)
	ctx := context.Background()
	if _, _, err := runs.Reserve(ctx, workerTestRequest(workspace, "retry-resume")); err != nil {
		t.Fatal(err)
	}
	registry := model.NewRegistry()
	fake := model.NewFakeProvider(model.FakeConfig{
		Response: "retry recovered",
		Failures: []contracts.ModelFailure{
			{Code: contracts.ModelFailureRateLimit, Message: "rate limited", Retryable: true},
			{Code: contracts.ModelFailureRateLimit, Message: "rate limited", Retryable: true},
			{Code: contracts.ModelFailureRateLimit, Message: "rate limited", Retryable: true},
		},
	})
	if err := registry.Register(fake); err != nil {
		t.Fatal(err)
	}
	loop := agentloop.New(runs, model.NewGateway(registry, nil), workerTestTools{})
	worker := NewWorker(runs, loop, "worker-retry")
	worker.LeaseTTL = time.Second
	worker.HeartbeatInterval = 20 * time.Millisecond
	first, err := worker.RunOnce(ctx, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Claimed || first.Decision.Run.State != contracts.AgentRunAwaitingRetry || first.Decision.Run.NextRetryAt == nil {
		t.Fatalf("first retry result=%+v", first)
	}
	// The provider performs bounded deterministic retries before persisting the
	// run's next retry time. Leave enough margin for race-instrumented builds
	// and a loaded Postgres container while still exercising automatic resume.
	time.Sleep(500 * time.Millisecond)
	second, err := worker.RunOnce(ctx, workspace)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Claimed || second.Decision.Run.State != contracts.AgentRunSucceeded || fake.Calls() != 4 {
		t.Fatalf("resumed retry result=%+v fake_calls=%d", second, fake.Calls())
	}
}

type blockingProvider struct {
	delegate *model.FakeProvider
	started  chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (p *blockingProvider) Name() string                      { return "blocking" }
func (p *blockingProvider) Aliases() []string                 { return []string{"fake"} }
func (p *blockingProvider) Endpoint() contracts.ModelEndpoint { return p.delegate.Endpoint() }
func (p *blockingProvider) Complete(ctx context.Context, request contracts.ModelRequest) (contracts.ModelResponse, error) {
	p.once.Do(func() { close(p.started) })
	select {
	case <-p.release:
		return p.delegate.Complete(ctx, request)
	case <-ctx.Done():
		return contracts.ModelResponse{}, ctx.Err()
	}
}
func (p *blockingProvider) Stream(ctx context.Context, request contracts.ModelRequest, sink model.StreamSink) (contracts.ModelResponse, error) {
	return p.delegate.Stream(ctx, request, sink)
}
func (p *blockingProvider) Embed(ctx context.Context, request model.EmbeddingRequest) ([]float32, error) {
	return p.delegate.Embed(ctx, request)
}

func TestWorkerCrashDuringModelExecutionCannotFinalizeAfterTakeover(t *testing.T) {
	runs, _, workspace := newWorkerTestHarness(t)
	ctx := context.Background()
	run, _, err := runs.Reserve(ctx, workerTestRequest(workspace, "stale-model"))
	if err != nil {
		t.Fatal(err)
	}
	delegate := model.NewFakeProvider(model.FakeConfig{Response: "late response"})
	blocking := &blockingProvider{delegate: delegate, started: make(chan struct{}), release: make(chan struct{})}
	registry := model.NewRegistry()
	if err := registry.Register(blocking); err != nil {
		t.Fatal(err)
	}
	loop := agentloop.New(runs, model.NewGateway(registry, nil), workerTestTools{})
	oldClaim, found, err := runs.ClaimNextAgentRun(ctx, workspace, "worker-old", 20*time.Millisecond)
	if err != nil || !found {
		t.Fatalf("old claim=%+v found=%t err=%v", oldClaim, found, err)
	}
	errCh := make(chan error, 1)
	go func() {
		_, runErr := loop.Run(agentloop.WithWorkerLease(ctx, oldClaim.Lease), workspace, run.ID)
		errCh <- runErr
	}()
	select {
	case <-blocking.started:
	case <-time.After(time.Second):
		t.Fatal("model call did not start")
	}
	time.Sleep(50 * time.Millisecond)
	newClaim, found, err := runs.ClaimNextAgentRun(ctx, workspace, "worker-new", time.Second)
	if err != nil || !found || !newClaim.Takeover || newClaim.Lease.Fence != oldClaim.Lease.Fence+1 {
		t.Fatalf("new claim=%+v found=%t err=%v", newClaim, found, err)
	}
	close(blocking.release)
	select {
	case runErr := <-errCh:
		if !errors.Is(runErr, store.ErrAgentRunLeaseFenced) {
			t.Fatalf("stale model worker error=%v, want lease fence error", runErr)
		}
	case <-time.After(time.Second):
		t.Fatal("stale model worker did not return")
	}
	current, err := runs.Get(ctx, workspace, run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.State != contracts.AgentRunPending || current.StateVersion != 1 {
		t.Fatalf("stale worker changed run=%+v", current)
	}
	if err := runs.ReleaseAgentRunLease(ctx, newClaim.Lease); err != nil {
		t.Fatal(err)
	}
}
