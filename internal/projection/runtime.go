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

type Runner struct {
	events     *store.EventStore
	subscriber Subscriber
}

func NewRunner(events *store.EventStore, subscriber Subscriber) (*Runner, error) {
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
	return &Runner{events: events, subscriber: subscriber}, nil
}

type BatchResult struct {
	Projection       string
	ConsumerID       string
	WorkspaceID      string
	EventsRead       int
	EventsApplied    int
	EventsDuplicate  int
	CheckpointBefore uint64
	CheckpointAfter  uint64
	HasMore          bool
	Duration         time.Duration
}

type RunHook func(BatchResult) error

func (r *Runner) RunBatch(ctx context.Context, workspaceID string) (BatchResult, error) {
	return r.runBatch(ctx, workspaceID, nil)
}

// RunBatchWithHook exists to make the pre-commit crash boundary testable. A
// production caller should use RunBatch; the hook executes after projection
// writes and checkpoint advancement but before COMMIT.
func (r *Runner) RunBatchWithHook(ctx context.Context, workspaceID string, hook RunHook) (BatchResult, error) {
	return r.runBatch(ctx, workspaceID, hook)
}

func (r *Runner) runBatch(ctx context.Context, workspaceID string, hook RunHook) (BatchResult, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return BatchResult{}, fmt.Errorf("workspace_id is required")
	}
	started := time.Now()
	result := BatchResult{
		Projection:  r.subscriber.Name(),
		ConsumerID:  r.subscriber.ConsumerID(),
		WorkspaceID: workspaceID,
	}
	tx, err := r.events.Begin(ctx)
	if err != nil {
		return result, fmt.Errorf("begin projection batch: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

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
		if err := r.events.AdvanceCheckpointAtTx(ctx, tx, workspaceID, r.subscriber.ConsumerID(), before, after); err != nil {
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

type RebuildResult struct {
	Projection      string
	ConsumerID      string
	WorkspaceID     string
	Batches         int
	EventsRead      int
	EventsApplied   int
	EventsDuplicate int
	Checkpoint      uint64
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
	started := time.Now()
	resetTx, err := r.events.Begin(ctx)
	if err != nil {
		return RebuildResult{}, fmt.Errorf("begin projection rebuild: %w", err)
	}
	defer func() { _ = resetTx.Rollback(ctx) }()
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
	}
	for {
		batch, runErr := r.RunBatch(ctx, workspaceID)
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

func boundedBatchSize(size int) int {
	if size <= 0 {
		return DefaultBatchSize
	}
	if size > MaxBatchSize {
		return MaxBatchSize
	}
	return size
}
