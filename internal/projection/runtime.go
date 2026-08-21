// Package projection applies immutable control events to rebuildable,
// workspace-scoped read models. Projection writes and checkpoints commit in a
// single Postgres transaction under a consumer fence.
package projection

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

const (
	DefaultBatchSize = 100
	MaxBatchSize     = 1000
)

// ApplyResult makes duplicate delivery observable without forcing every
// subscriber to expose database-specific details.
type ApplyResult struct {
	Handled   bool
	Applied   bool
	Duplicate bool
}

// Projection is the deterministic derived-state contract. Apply must only
// mutate tables owned by the projection and must not mutate event history.
type Projection interface {
	Name() string
	Apply(context.Context, pgx.Tx, contracts.EventEnvelope) (ApplyResult, error)
	Reset(context.Context, pgx.Tx, string) error
}

// Subscriber adds the durable consumer identity and bounded batch policy to a
// projection. The identity is scoped by workspace in control_checkpoints.
type Subscriber interface {
	Projection
	ConsumerID() string
	BatchSize() int
}

// Runner owns one durable projection subscriber and coordinates lease-fenced,
// transactional batch application against the shared event store.
type Runner struct {
	events     *store.EventStore
	subscriber Subscriber
	ownerID    string
	leaseTTL   time.Duration
}

// NewRunner creates a projection runner with a generated process owner.
func NewRunner(events *store.EventStore, subscriber Subscriber) (*Runner, error) {
	return NewRunnerWithOwner(events, subscriber, contracts.NewID("consumer"), contracts.DefaultConsumerLeaseTTL)
}

// NewRunnerWithOwner is useful for durable worker identities and deterministic
// concurrency tests. Production processes should use a unique owner ID per
// process or worker instance.
func NewRunnerWithOwner(events *store.EventStore, subscriber Subscriber, ownerID string, leaseTTL time.Duration) (*Runner, error) {
	if events == nil {
		return nil, fmt.Errorf("event store is required")
	}
	if subscriber == nil {
		return nil, fmt.Errorf("subscriber is required")
	}
	if strings.TrimSpace(subscriber.ConsumerID()) == "" {
		return nil, fmt.Errorf("subscriber consumer ID is required")
	}
	if strings.TrimSpace(subscriber.Name()) == "" {
		return nil, fmt.Errorf("subscriber name is required")
	}
	ownerID = strings.TrimSpace(ownerID)
	if ownerID == "" {
		return nil, fmt.Errorf("consumer owner ID is required")
	}
	if leaseTTL <= 0 {
		leaseTTL = contracts.DefaultConsumerLeaseTTL
	}
	if leaseTTL > contracts.MaxConsumerLeaseTTL {
		leaseTTL = contracts.MaxConsumerLeaseTTL
	}
	return &Runner{events: events, subscriber: subscriber, ownerID: ownerID, leaseTTL: leaseTTL}, nil
}

// OwnerID returns the process identity used for consumer lease validation.
func (r *Runner) OwnerID() string {
	return r.ownerID
}

// AcquireLease obtains or reuses this runner's workspace-scoped consumer
// lease.
func (r *Runner) AcquireLease(ctx context.Context, workspaceID string) (contracts.ConsumerLeaseResult, error) {
	return r.events.AcquireConsumerLease(ctx, workspaceID, r.subscriber.ConsumerID(), r.ownerID, r.leaseTTL)
}

// RenewLease extends the exact current lease; stale owners fail closed.
func (r *Runner) RenewLease(ctx context.Context, lease contracts.ConsumerLease, ttl time.Duration) (contracts.ConsumerLease, error) {
	return r.events.RenewConsumerLease(ctx, lease, ttl)
}

// ReleaseLease relinquishes the exact current lease without moving the
// projection checkpoint backwards.
func (r *Runner) ReleaseLease(ctx context.Context, lease contracts.ConsumerLease) error {
	return r.events.ReleaseConsumerLease(ctx, lease)
}

// BatchResult reports one atomic projection application and checkpoint step.
type BatchResult struct {
	Projection       string
	ConsumerID       string
	WorkspaceID      string
	EventsRead       int
	EventsApplied    int
	EventsDuplicate  int
	CheckpointBefore uint64
	CheckpointAfter  uint64
	LeaseFence       uint64
	HasMore          bool
	Duration         time.Duration
}

// RunHook is a test seam executed after writes and before the transaction
// commits, allowing crash-boundary verification.
type RunHook func(BatchResult) error

// RunBatch acquires the consumer lease and processes one bounded event batch.
func (r *Runner) RunBatch(ctx context.Context, workspaceID string) (BatchResult, error) {
	acquired, err := r.AcquireLease(ctx, workspaceID)
	if err != nil {
		return BatchResult{Projection: r.subscriber.Name(), ConsumerID: r.subscriber.ConsumerID(), WorkspaceID: strings.TrimSpace(workspaceID)}, err
	}
	return r.runBatchWithLease(ctx, acquired.Lease, nil)
}

// RunBatchWithLease executes a batch only with the exact current owner and
// fence. It is the preferred entry point for callers that renew explicitly.
func (r *Runner) RunBatchWithLease(ctx context.Context, lease contracts.ConsumerLease) (BatchResult, error) {
	if err := r.validateLease(lease); err != nil {
		return BatchResult{}, err
	}
	return r.runBatchWithLease(ctx, lease, nil)
}

// RunBatchWithHook exists to make the pre-commit crash boundary testable. A
// production caller should use RunBatch; the hook executes after projection
// writes and checkpoint advancement but before COMMIT.
func (r *Runner) RunBatchWithHook(ctx context.Context, workspaceID string, hook RunHook) (BatchResult, error) {
	acquired, err := r.AcquireLease(ctx, workspaceID)
	if err != nil {
		return BatchResult{Projection: r.subscriber.Name(), ConsumerID: r.subscriber.ConsumerID(), WorkspaceID: strings.TrimSpace(workspaceID)}, err
	}
	return r.runBatchWithLease(ctx, acquired.Lease, hook)
}

func (r *Runner) runBatchWithLease(ctx context.Context, lease contracts.ConsumerLease, hook RunHook) (BatchResult, error) {
	if err := r.validateLease(lease); err != nil {
		return BatchResult{}, err
	}
	workspaceID := strings.TrimSpace(lease.WorkspaceID)
	started := time.Now()
	result := BatchResult{
		Projection:  r.subscriber.Name(),
		ConsumerID:  r.subscriber.ConsumerID(),
		WorkspaceID: workspaceID,
		LeaseFence:  lease.Fence,
	}
	tx, err := r.events.Begin(ctx)
	if err != nil {
		return result, fmt.Errorf("begin projection batch: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	if _, err := r.events.ValidateConsumerLeaseTx(ctx, tx, lease); err != nil {
		return result, err
	}
	before, err := r.events.EnsureCheckpointTx(ctx, tx, workspaceID, r.subscriber.ConsumerID())
	if err != nil {
		return result, err
	}
	result.CheckpointBefore = before
	batchSize := boundedBatchSize(r.subscriber.BatchSize())
	events, err := r.events.ReadAfterTx(ctx, tx, store.ReadRequest{
		WorkspaceID:   workspaceID,
		AfterSequence: before,
		Limit:         batchSize + 1,
	})
	if err != nil {
		return result, fmt.Errorf("read projection batch: %w", err)
	}
	if len(events) > batchSize {
		result.HasMore = true
		events = events[:batchSize]
	}
	result.EventsRead = len(events)
	for _, event := range events {
		outcome, applyErr := r.subscriber.Apply(ctx, tx, event)
		if applyErr != nil {
			return result, fmt.Errorf("apply %s event %d: %w", r.subscriber.Name(), event.Sequence, applyErr)
		}
		if outcome.Applied {
			result.EventsApplied++
		}
		if outcome.Duplicate {
			result.EventsDuplicate++
		}
	}
	after := before
	if len(events) > 0 {
		after = events[len(events)-1].Sequence
		if err := r.events.AdvanceCheckpointAtTxWithLease(ctx, tx, lease, before, after); err != nil {
			return result, err
		}
	}
	result.CheckpointAfter = after
	result.Duration = time.Since(started)
	if hook != nil {
		if err := hook(result); err != nil {
			return result, fmt.Errorf("projection pre-commit hook: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return result, fmt.Errorf("commit projection batch: %w", err)
	}
	return result, nil
}

// RebuildResult reports a projection reset and deterministic catch-up from
// sequence zero.
type RebuildResult struct {
	Projection      string
	ConsumerID      string
	WorkspaceID     string
	Batches         int
	EventsRead      int
	EventsApplied   int
	EventsDuplicate int
	Checkpoint      uint64
	LeaseFence      uint64
	Duration        time.Duration
}

// Rebuild resets the derived view and cursor atomically, then catches up using
// the same bounded runtime as incremental processing. If interrupted, the
// projection/cursor pair is still internally consistent and Rebuild can be
// retried from the beginning.
func (r *Runner) Rebuild(ctx context.Context, workspaceID string) (RebuildResult, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return RebuildResult{}, fmt.Errorf("workspace_id is required")
	}
	acquired, err := r.AcquireLease(ctx, workspaceID)
	if err != nil {
		return RebuildResult{Projection: r.subscriber.Name(), ConsumerID: r.subscriber.ConsumerID(), WorkspaceID: workspaceID}, err
	}
	lease := acquired.Lease
	started := time.Now()
	resetTx, err := r.events.Begin(ctx)
	if err != nil {
		return RebuildResult{}, fmt.Errorf("begin projection rebuild: %w", err)
	}
	defer func() { _ = resetTx.Rollback(ctx) }()
	if _, err := r.events.ValidateConsumerLeaseTx(ctx, resetTx, lease); err != nil {
		return RebuildResult{}, err
	}
	if _, err := r.events.EnsureCheckpointTx(ctx, resetTx, workspaceID, r.subscriber.ConsumerID()); err != nil {
		return RebuildResult{}, err
	}
	if err := r.subscriber.Reset(ctx, resetTx, workspaceID); err != nil {
		return RebuildResult{}, fmt.Errorf("reset %s projection: %w", r.subscriber.Name(), err)
	}
	if err := r.events.ResetCheckpointTx(ctx, resetTx, workspaceID, r.subscriber.ConsumerID()); err != nil {
		return RebuildResult{}, err
	}
	if err := resetTx.Commit(ctx); err != nil {
		return RebuildResult{}, fmt.Errorf("commit projection reset: %w", err)
	}

	result := RebuildResult{
		Projection:  r.subscriber.Name(),
		ConsumerID:  r.subscriber.ConsumerID(),
		WorkspaceID: workspaceID,
		LeaseFence:  lease.Fence,
	}
	for {
		batch, runErr := r.runBatchWithLease(ctx, lease, nil)
		if runErr != nil {
			return result, runErr
		}
		result.Batches++
		result.EventsRead += batch.EventsRead
		result.EventsApplied += batch.EventsApplied
		result.EventsDuplicate += batch.EventsDuplicate
		result.Checkpoint = batch.CheckpointAfter
		if !batch.HasMore {
			result.Duration = time.Since(started)
			return result, nil
		}
	}
}

func (r *Runner) validateLease(lease contracts.ConsumerLease) error {
	if err := contracts.ValidateConsumerLeaseIdentity(lease.WorkspaceID, lease.ConsumerID, lease.OwnerID); err != nil {
		return err
	}
	if lease.ConsumerID != r.subscriber.ConsumerID() {
		return fmt.Errorf("consumer lease consumer_id %q does not match subscriber %q", lease.ConsumerID, r.subscriber.ConsumerID())
	}
	if lease.OwnerID != r.ownerID {
		return fmt.Errorf("consumer lease owner_id %q does not match runner owner %q", lease.OwnerID, r.ownerID)
	}
	if lease.Fence == 0 {
		return fmt.Errorf("consumer lease fence is required")
	}
	return nil
}

func boundedBatchSize(size int) int {
	if size <= 0 {
		return DefaultBatchSize
	}
	if size > MaxBatchSize {
		return MaxBatchSize
	}
	return size
}
