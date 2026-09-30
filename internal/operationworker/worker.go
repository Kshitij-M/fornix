// Package operationworker consumes generic Postgres operation claims through
// an adapter-owned handler. It owns claim heartbeats and lease lifecycle, but
// it never dispatches a provider or mutates operation state on behalf of an
// adapter.
package operationworker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

var (
	ErrWorkerNotConfigured = errors.New("generic operation worker is not configured")
	ErrLeaseLost           = errors.New("generic operation worker lost its lease")
)

const (
	DefaultLeaseTTL          = 90 * time.Second
	DefaultHeartbeatInterval = 30 * time.Second
	DefaultPollInterval      = 2 * time.Second
	MaxClaimBatch            = 64
	MaxLeaseTTL              = 24 * time.Hour
	MaxPollInterval          = 5 * time.Minute
)

// Handler is the adapter-owned execution boundary. The handler receives the
// immutable request and the exact operation fence that authorized this
// delivery. It must persist any operation transition/result itself before
// returning nil. A handler must not treat nil as an exactly-once guarantee for
// an external provider.
type Handler interface {
	Handle(context.Context, store.OperationClaim) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(context.Context, store.OperationClaim) error

func (f HandlerFunc) Handle(ctx context.Context, claim store.OperationClaim) error {
	if f == nil {
		return ErrWorkerNotConfigured
	}
	return f(ctx, claim)
}

// ClaimOutcome is bounded process-local telemetry for one delivery. It is
// intentionally not a durable result; operation transitions and results are
// the authoritative records.
type ClaimOutcome struct {
	WorkspaceID string
	OperationID string
	Fence       uint64
	Handled     bool
	Released    bool
	Err         error
}

// BatchResult reports one bounded polling attempt.
type BatchResult struct {
	Claims   int
	Handled  int
	Released int
	Failed   int
	Outcomes []ClaimOutcome
}

// Worker owns only the generic claim lifecycle. The caller supplies the
// adapter handler and, when required, an explicit workspace. An empty
// workspace is rejected so that a process cannot accidentally become a global
// worker without a scheduler policy.
type Worker struct {
	Store     *store.OperationStore
	Handler   Handler
	OwnerID   string
	Limit     int
	MaxActive int
	// ReadOnlyOnly asks the Postgres claim to exclude unplanned, unknown, and
	// effectful operation plans. It is the required mode for a generic server
	// worker; adapter-owned workers may deliberately leave it false.
	ReadOnlyOnly      bool
	LeaseTTL          time.Duration
	HeartbeatInterval time.Duration
	PollInterval      time.Duration
}

// New creates a worker with bounded defaults.
func New(operationStore *store.OperationStore, handler Handler, ownerID string) *Worker {
	ownerID = strings.TrimSpace(ownerID)
	if ownerID == "" {
		ownerID = contracts.NewID("operation-worker")
	}
	return &Worker{
		Store: operationStore, Handler: handler, OwnerID: ownerID,
		Limit: 1, LeaseTTL: DefaultLeaseTTL,
		HeartbeatInterval: DefaultHeartbeatInterval, PollInterval: DefaultPollInterval,
	}
}

func (w *Worker) configured() bool {
	return w != nil && w.Store != nil && w.Handler != nil && strings.TrimSpace(w.OwnerID) != ""
}

func (w *Worker) leaseTTL() time.Duration {
	ttl := w.LeaseTTL
	if ttl <= 0 {
		ttl = DefaultLeaseTTL
	}
	if ttl > MaxLeaseTTL {
		ttl = MaxLeaseTTL
	}
	return ttl
}

func (w *Worker) heartbeatInterval(ttl time.Duration) time.Duration {
	interval := w.HeartbeatInterval
	if interval <= 0 || interval >= ttl {
		interval = ttl / 3
	}
	if interval < time.Millisecond {
		interval = time.Millisecond
	}
	return interval
}

func (w *Worker) limit() int {
	limit := w.Limit
	if limit <= 0 || limit > MaxClaimBatch {
		limit = MaxClaimBatch
	}
	return limit
}

// RunOnce claims one bounded batch in one explicit workspace and hands each
// claim to the adapter. An empty batch is a normal result. A handler error is
// reported and its lease is deliberately left to expire for takeover; this
// avoids a tight retry loop and preserves the crash-recovery boundary.
func (w *Worker) RunOnce(ctx context.Context, workspaceID string) (BatchResult, error) {
	if !w.configured() {
		return BatchResult{}, ErrWorkerNotConfigured
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return BatchResult{}, fmt.Errorf("%w: workspace_id is required", ErrWorkerNotConfigured)
	}
	claims, err := w.Store.ClaimReadyWithOptions(ctx, workspaceID, w.OwnerID, store.OperationClaimOptions{
		Limit: w.limit(), TTL: w.leaseTTL(), MaxActive: w.MaxActive, ReadOnlyOnly: w.ReadOnlyOnly,
	})
	if err != nil {
		return BatchResult{}, fmt.Errorf("claim generic operations: %w", err)
	}
	result := BatchResult{Claims: len(claims), Outcomes: make([]ClaimOutcome, 0, len(claims))}
	var firstErr error
	for _, claim := range claims {
		outcome := w.handleClaim(ctx, claim)
		result.Outcomes = append(result.Outcomes, outcome)
		if outcome.Handled {
			result.Handled++
		}
		if outcome.Released {
			result.Released++
		}
		if outcome.Err != nil {
			result.Failed++
			if firstErr == nil {
				firstErr = outcome.Err
			}
		}
	}
	return result, firstErr
}

func (w *Worker) handleClaim(parent context.Context, claim store.OperationClaim) ClaimOutcome {
	outcome := ClaimOutcome{WorkspaceID: claim.Operation.WorkspaceID, OperationID: claim.Operation.ID, Fence: claim.Lease.Fence}
	runCtx, cancel := context.WithCancel(parent)
	defer cancel()

	interval := w.heartbeatInterval(w.leaseTTL())
	heartbeatDone := make(chan struct{})
	heartbeatErr := make(chan error, 1)
	var heartbeatOnce sync.Once
	var heartbeatWG sync.WaitGroup
	heartbeatWG.Add(1)
	go func() {
		defer heartbeatWG.Done()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-heartbeatDone:
				return
			case <-ticker.C:
				if _, err := w.Store.RenewLease(runCtx, claim.Lease, w.leaseTTL()); err != nil {
					heartbeatOnce.Do(func() { heartbeatErr <- fmt.Errorf("%w: %v", ErrLeaseLost, err) })
					cancel()
					return
				}
			}
		}
	}()

	handlerErr := w.Handler.Handle(runCtx, claim)
	close(heartbeatDone)
	heartbeatWG.Wait()
	var leaseErr error
	select {
	case leaseErr = <-heartbeatErr:
	default:
	}
	if handlerErr == nil && leaseErr != nil {
		handlerErr = leaseErr
	}
	if handlerErr != nil {
		outcome.Err = handlerErr
		return outcome
	}

	releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer releaseCancel()
	if err := w.Store.ReleaseLease(releaseCtx, claim.Lease); err != nil {
		outcome.Err = fmt.Errorf("release operation claim: %w", err)
		return outcome
	}
	outcome.Handled = true
	outcome.Released = true
	return outcome
}

// Run polls until cancellation. A handler or lease error is returned to the
// supervisor; the durable lease remains the recovery mechanism for unfinished
// work. Cancellation is returned as context.Canceled for callers to classify.
func (w *Worker) Run(ctx context.Context, workspaceID string) error {
	if !w.configured() {
		return ErrWorkerNotConfigured
	}
	interval := w.PollInterval
	if interval <= 0 || interval > MaxPollInterval {
		interval = DefaultPollInterval
	}
	for {
		_, err := w.RunOnce(ctx, workspaceID)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		// Always yield after a claim batch. A misbehaving adapter that returns
		// success without advancing durable state must not turn the process into
		// a tight Postgres polling loop.
		if !wait(ctx, interval) {
			return ctx.Err()
		}
	}
}

func wait(ctx context.Context, interval time.Duration) bool {
	timer := time.NewTimer(interval)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
