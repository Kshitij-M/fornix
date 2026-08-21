// Package scheduler runs durable repository work by claiming due Postgres rows
// and fencing every checkpoint with a workspace-scoped lease. It supplies the
// recovery guarantees needed before an agent result can be treated as
// verifiable.
package scheduler

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/omaveda/fornix/internal/agentloop"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

// ErrWorkerNotConfigured indicates that a worker lacks its Postgres store,
// orchestrator, or stable owner identity.
var ErrWorkerNotConfigured = errors.New("agent run worker is not configured")

// Result records one bounded pull/reconcile attempt. A false Claimed result
// is an ordinary empty queue, not an error.
type Result struct {
	Claimed  bool
	Lease    contracts.AgentRunLease
	Run      contracts.AgentRun
	Decision contracts.LoopDecision
	Takeover bool
}

// Worker is a small Postgres-backed pull worker. It intentionally has no
// in-memory queue: a process restart simply claims due rows again after lease
// expiry. The model/tool orchestrator is bounded by the run's persisted
// budgets.
type Worker struct {
	Store             *store.AgentRunStore
	Orchestrator      *agentloop.Orchestrator
	OwnerID           string
	LeaseTTL          time.Duration
	HeartbeatInterval time.Duration
	PollInterval      time.Duration
}

// NewWorker creates a pull worker with bounded lease and polling defaults.
func NewWorker(runs *store.AgentRunStore, orchestrator *agentloop.Orchestrator, ownerID string) *Worker {
	ownerID = strings.TrimSpace(ownerID)
	if ownerID == "" {
		ownerID = contracts.NewID("worker")
	}
	return &Worker{
		Store: runs, Orchestrator: orchestrator, OwnerID: ownerID,
		LeaseTTL:     contracts.DefaultAgentRunLeaseTTL,
		PollInterval: contracts.DefaultAgentRunPoll,
	}
}

// RunOnce claims at most one run in the requested workspace. An empty
// workspace selects all workspaces while preserving the workspace in every
// lease and orchestration request.
func (w *Worker) RunOnce(ctx context.Context, workspaceID string) (Result, error) {
	if w == nil || w.Store == nil || w.Orchestrator == nil || strings.TrimSpace(w.OwnerID) == "" {
		return Result{}, ErrWorkerNotConfigured
	}
	ttl := contracts.NormalizeAgentRunLeaseTTL(w.LeaseTTL)
	claim, found, err := w.Store.ClaimNextAgentRun(ctx, strings.TrimSpace(workspaceID), w.OwnerID, ttl)
	if err != nil || !found {
		return Result{Claimed: found}, err
	}
	result := Result{Claimed: true, Lease: claim.Lease, Run: claim.Run, Takeover: claim.Takeover}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	leaseCtx := agentloop.WithWorkerLease(runCtx, claim.Lease)

	interval := w.HeartbeatInterval
	if interval <= 0 || interval >= ttl {
		interval = ttl / 3
	}
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	hbDone := make(chan struct{})
	hbErr := make(chan error, 1)
	var hbOnce sync.Once
	var hbWG sync.WaitGroup
	hbWG.Add(1)
	go func() {
		defer hbWG.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-hbDone:
				return
			case <-ticker.C:
				if _, renewErr := w.Store.RenewAgentRunLease(runCtx, claim.Lease, ttl); renewErr != nil {
					hbOnce.Do(func() { hbErr <- renewErr })
					cancel()
					return
				}
			}
		}
	}()

	decision, runErr := w.Orchestrator.Run(leaseCtx, claim.Run.WorkspaceID, claim.Run.ID)
	close(hbDone)
	hbWG.Wait()
	var heartbeatErr error
	select {
	case heartbeatErr = <-hbErr:
	default:
	}
	if runErr == nil && heartbeatErr != nil {
		runErr = fmt.Errorf("agent run heartbeat failed: %w", heartbeatErr)
	}
	result.Decision = decision
	if decision.Run.ID != "" {
		result.Run = decision.Run
	}
	if releaseErr := w.Store.ReleaseAgentRunLease(context.Background(), claim.Lease); releaseErr != nil && runErr == nil {
		runErr = fmt.Errorf("release agent run lease: %w", releaseErr)
	}
	return result, runErr
}

// Run polls Postgres until cancellation. The empty-queue path waits for the
// configured interval; all correctness remains in the database claim.
func (w *Worker) Run(ctx context.Context, workspaceID string) error {
	if w == nil || w.Store == nil || w.Orchestrator == nil {
		return ErrWorkerNotConfigured
	}
	interval := w.PollInterval
	if interval <= 0 || interval > contracts.MaxAgentRunPoll {
		interval = contracts.DefaultAgentRunPoll
	}
	for {
		result, err := w.RunOnce(ctx, workspaceID)
		if err != nil && !errors.Is(err, context.Canceled) {
			// A failed run is durable state or a stale-owner error. Keep polling;
			// a supervisor can observe the returned error only when it cancels the
			// worker, while Postgres remains the recovery source.
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if result.Claimed {
			if err != nil {
				if !waitForPoll(ctx, interval) {
					return ctx.Err()
				}
			}
			continue
		}
		if !waitForPoll(ctx, interval) {
			return ctx.Err()
		}
	}
}

func waitForPoll(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
