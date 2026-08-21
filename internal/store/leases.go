package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrConsumerLeaseHeld      = errors.New("consumer lease is held by another owner")
	ErrConsumerLeaseMissing   = errors.New("consumer lease not found")
	ErrConsumerLeaseOwned     = errors.New("consumer lease is not owned by this owner")
	ErrConsumerLeaseFenced    = errors.New("consumer lease fence is stale")
	ErrConsumerLeaseExpired   = errors.New("consumer lease is expired")
	ErrConsumerLeaseReleased  = errors.New("consumer lease is released")
	ErrConsumerFenceExhausted = errors.New("consumer lease fence is exhausted")
)

const maxConsumerFence = uint64(1<<63 - 1)

// AcquireConsumerLease creates or takes over a workspace-scoped consumer
// lease. The returned fence is the only token that can authorize projection
// work for this owner.
func (s *EventStore) AcquireConsumerLease(
	ctx context.Context,
	workspaceID, consumerID, ownerID string,
	ttl time.Duration,
) (contracts.ConsumerLeaseResult, error) {
	tx, err := s.Begin(ctx)
	if err != nil {
		return contracts.ConsumerLeaseResult{}, fmt.Errorf("begin consumer lease acquire: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	result, err := s.AcquireConsumerLeaseTx(ctx, tx, workspaceID, consumerID, ownerID, ttl)
	if err != nil {
		return contracts.ConsumerLeaseResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ConsumerLeaseResult{}, fmt.Errorf("commit consumer lease acquire: %w", err)
	}
	return result, nil
}

// AcquireConsumerLeaseTx is the transaction-scoped form used by callers that
// need lease acquisition and another durable mutation to commit together.
func (s *EventStore) AcquireConsumerLeaseTx(
	ctx context.Context,
	tx pgx.Tx,
	workspaceID, consumerID, ownerID string,
	ttl time.Duration,
) (contracts.ConsumerLeaseResult, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	consumerID = strings.TrimSpace(consumerID)
	ownerID = strings.TrimSpace(ownerID)
	if err := contracts.ValidateConsumerLeaseIdentity(workspaceID, consumerID, ownerID); err != nil {
		return contracts.ConsumerLeaseResult{}, err
	}
	if tx == nil {
		return contracts.ConsumerLeaseResult{}, errors.New("consumer lease transaction is nil")
	}
	ttl = boundedConsumerLeaseTTL(ttl)

	insertedRow, err := tx.Exec(ctx, `
		INSERT INTO fornix.consumer_leases(
			workspace_id, consumer_id, owner_id, fence, lease_until,
			acquired_at, renewed_at, released_at
		) VALUES(
			$1, $2, $3, 1,
			clock_timestamp() + ($4::double precision * interval '1 millisecond'),
			clock_timestamp(), clock_timestamp(), NULL
		)
		ON CONFLICT (workspace_id, consumer_id) DO NOTHING`,
		workspaceID, consumerID, ownerID, ttl.Milliseconds())
	if err != nil {
		return contracts.ConsumerLeaseResult{}, fmt.Errorf("insert consumer lease: %w", err)
	}
	inserted := insertedRow.RowsAffected() == 1

	current, active, err := readConsumerLeaseTx(ctx, tx, workspaceID, consumerID, true)
	if err != nil {
		return contracts.ConsumerLeaseResult{}, err
	}
	if active {
		if current.OwnerID != ownerID {
			return contracts.ConsumerLeaseResult{}, fmt.Errorf("%w: workspace=%s consumer=%s owner=%s", ErrConsumerLeaseHeld, workspaceID, consumerID, current.OwnerID)
		}
		return contracts.ConsumerLeaseResult{
			Lease:    current,
			Acquired: inserted,
			Reused:   !inserted,
		}, nil
	}
	if current.Fence >= maxConsumerFence {
		return contracts.ConsumerLeaseResult{}, ErrConsumerFenceExhausted
	}

	if _, err := tx.Exec(ctx, `
		UPDATE fornix.consumer_leases
		SET owner_id=$3,
		    fence=fence+1,
		    lease_until=clock_timestamp() + ($4::double precision * interval '1 millisecond'),
		    acquired_at=clock_timestamp(),
		    renewed_at=clock_timestamp(),
		    released_at=NULL
		WHERE workspace_id=$1 AND consumer_id=$2 AND fence=$5`,
		workspaceID, consumerID, ownerID, ttl.Milliseconds(), int64(current.Fence)); err != nil {
		return contracts.ConsumerLeaseResult{}, fmt.Errorf("take over consumer lease: %w", err)
	}
	updated, updatedActive, err := readConsumerLeaseTx(ctx, tx, workspaceID, consumerID, true)
	if err != nil {
		return contracts.ConsumerLeaseResult{}, err
	}
	if !updatedActive || updated.OwnerID != ownerID || updated.Fence <= current.Fence {
		return contracts.ConsumerLeaseResult{}, errors.New("consumer lease takeover did not produce an active higher fence")
	}
	return contracts.ConsumerLeaseResult{
		Lease:    updated,
		Acquired: true,
		Takeover: !inserted,
	}, nil
}

// GetConsumerLease reads the current lease row without authorizing work.
func (s *EventStore) GetConsumerLease(ctx context.Context, workspaceID, consumerID string) (contracts.ConsumerLease, bool, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	consumerID = strings.TrimSpace(consumerID)
	if workspaceID == "" || consumerID == "" {
		return contracts.ConsumerLease{}, false, errors.New("workspace_id and consumer_id are required")
	}
	lease, active, err := readConsumerLease(ctx, s.pool, workspaceID, consumerID, false)
	if errors.Is(err, ErrConsumerLeaseMissing) {
		return contracts.ConsumerLease{}, false, nil
	}
	return lease, active, err
}

// ValidateConsumerLeaseTx locks and validates the current lease row. The row
// lock must be held through the caller's projection/checkpoint commit so an
// expiring lease cannot be taken over between validation and effect.
func (s *EventStore) ValidateConsumerLeaseTx(
	ctx context.Context,
	tx pgx.Tx,
	lease contracts.ConsumerLease,
) (contracts.ConsumerLease, error) {
	if err := contracts.ValidateConsumerLeaseIdentity(lease.WorkspaceID, lease.ConsumerID, lease.OwnerID); err != nil {
		return contracts.ConsumerLease{}, err
	}
	if lease.Fence == 0 || lease.Fence > maxConsumerFence {
		return contracts.ConsumerLease{}, ErrConsumerLeaseFenced
	}
	current, active, err := readConsumerLeaseTx(ctx, tx, lease.WorkspaceID, lease.ConsumerID, true)
	if err != nil {
		return contracts.ConsumerLease{}, err
	}
	if current.Fence != lease.Fence {
		return current, fmt.Errorf("%w: expected=%d current=%d", ErrConsumerLeaseFenced, lease.Fence, current.Fence)
	}
	if current.OwnerID != strings.TrimSpace(lease.OwnerID) {
		return current, fmt.Errorf("%w: expected=%s current=%s", ErrConsumerLeaseOwned, lease.OwnerID, current.OwnerID)
	}
	if current.ReleasedAt != nil {
		return current, ErrConsumerLeaseReleased
	}
	if !active {
		return current, ErrConsumerLeaseExpired
	}
	return current, nil
}

// RenewConsumerLease extends a still-valid lease without changing its fence.
func (s *EventStore) RenewConsumerLease(
	ctx context.Context,
	lease contracts.ConsumerLease,
	ttl time.Duration,
) (contracts.ConsumerLease, error) {
	tx, err := s.Begin(ctx)
	if err != nil {
		return contracts.ConsumerLease{}, fmt.Errorf("begin consumer lease renew: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	updated, err := s.RenewConsumerLeaseTx(ctx, tx, lease, ttl)
	if err != nil {
		return contracts.ConsumerLease{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ConsumerLease{}, fmt.Errorf("commit consumer lease renew: %w", err)
	}
	return updated, nil
}

// RenewConsumerLeaseTx renews the exact owner and fencing token inside the
// caller's transaction; stale or expired owners cannot extend themselves.
func (s *EventStore) RenewConsumerLeaseTx(
	ctx context.Context,
	tx pgx.Tx,
	lease contracts.ConsumerLease,
	ttl time.Duration,
) (contracts.ConsumerLease, error) {
	if _, err := s.ValidateConsumerLeaseTx(ctx, tx, lease); err != nil {
		return contracts.ConsumerLease{}, err
	}
	ttl = boundedConsumerLeaseTTL(ttl)
	if _, err := tx.Exec(ctx, `
		UPDATE fornix.consumer_leases
		SET lease_until=clock_timestamp() + ($3::double precision * interval '1 millisecond'),
		    renewed_at=clock_timestamp()
		WHERE workspace_id=$1 AND consumer_id=$2 AND owner_id=$4 AND fence=$5`,
		strings.TrimSpace(lease.WorkspaceID), strings.TrimSpace(lease.ConsumerID),
		ttl.Milliseconds(), strings.TrimSpace(lease.OwnerID), int64(lease.Fence)); err != nil {
		return contracts.ConsumerLease{}, fmt.Errorf("renew consumer lease: %w", err)
	}
	updated, active, err := readConsumerLeaseTx(ctx, tx, lease.WorkspaceID, lease.ConsumerID, true)
	if err != nil {
		return contracts.ConsumerLease{}, err
	}
	if !active {
		return contracts.ConsumerLease{}, ErrConsumerLeaseExpired
	}
	return updated, nil
}

// ReleaseConsumerLease makes a valid token unusable without deleting the
// current row. The next owner must take over with a larger fence.
func (s *EventStore) ReleaseConsumerLease(ctx context.Context, lease contracts.ConsumerLease) error {
	tx, err := s.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin consumer lease release: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.ReleaseConsumerLeaseTx(ctx, tx, lease); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit consumer lease release: %w", err)
	}
	return nil
}

// ReleaseConsumerLeaseTx releases the exact lease inside the caller's
// transaction while preserving its monotonic fence history.
func (s *EventStore) ReleaseConsumerLeaseTx(ctx context.Context, tx pgx.Tx, lease contracts.ConsumerLease) error {
	if _, err := s.ValidateConsumerLeaseTx(ctx, tx, lease); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE fornix.consumer_leases
		SET lease_until=clock_timestamp(), released_at=clock_timestamp(), renewed_at=clock_timestamp()
		WHERE workspace_id=$1 AND consumer_id=$2 AND owner_id=$3 AND fence=$4`,
		strings.TrimSpace(lease.WorkspaceID), strings.TrimSpace(lease.ConsumerID),
		strings.TrimSpace(lease.OwnerID), int64(lease.Fence)); err != nil {
		return fmt.Errorf("release consumer lease: %w", err)
	}
	return nil
}

// AdvanceCheckpointAtTxWithLease is the projection-only checkpoint boundary.
// It validates the fencing token while the lease row remains locked, then
// delegates to the monotonic checkpoint update.
func (s *EventStore) AdvanceCheckpointAtTxWithLease(
	ctx context.Context,
	tx pgx.Tx,
	lease contracts.ConsumerLease,
	current, sequence uint64,
) error {
	if _, err := s.ValidateConsumerLeaseTx(ctx, tx, lease); err != nil {
		return err
	}
	return s.AdvanceCheckpointAtTx(ctx, tx, lease.WorkspaceID, lease.ConsumerID, current, sequence)
}

type consumerLeaseQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func readConsumerLease(
	ctx context.Context,
	queryer consumerLeaseQueryer,
	workspaceID, consumerID string,
	lock bool,
) (contracts.ConsumerLease, bool, error) {
	query := `
		SELECT workspace_id, consumer_id, owner_id, fence, lease_until,
		       acquired_at, renewed_at, released_at,
		       (released_at IS NULL AND lease_until > clock_timestamp()) AS active
		FROM fornix.consumer_leases
		WHERE workspace_id=$1 AND consumer_id=$2`
	if lock {
		query += " FOR UPDATE"
	}
	var (
		lease  contracts.ConsumerLease
		fence  int64
		active bool
	)
	if err := queryer.QueryRow(ctx, query, workspaceID, consumerID).Scan(
		&lease.WorkspaceID, &lease.ConsumerID, &lease.OwnerID, &fence,
		&lease.LeaseUntil, &lease.AcquiredAt, &lease.RenewedAt,
		&lease.ReleasedAt, &active); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.ConsumerLease{}, false, ErrConsumerLeaseMissing
		}
		return contracts.ConsumerLease{}, false, fmt.Errorf("read consumer lease: %w", err)
	}
	if fence <= 0 {
		return contracts.ConsumerLease{}, false, errors.New("consumer lease has invalid fence")
	}
	lease.Fence = uint64(fence)
	return lease, active, nil
}

func readConsumerLeaseTx(ctx context.Context, tx pgx.Tx, workspaceID, consumerID string, lock bool) (contracts.ConsumerLease, bool, error) {
	if tx == nil {
		return contracts.ConsumerLease{}, false, errors.New("consumer lease transaction is nil")
	}
	return readConsumerLease(ctx, tx, strings.TrimSpace(workspaceID), strings.TrimSpace(consumerID), lock)
}

func boundedConsumerLeaseTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		ttl = contracts.DefaultConsumerLeaseTTL
	}
	if ttl > contracts.MaxConsumerLeaseTTL {
		ttl = contracts.MaxConsumerLeaseTTL
	}
	if ttl < time.Millisecond {
		ttl = time.Millisecond
	}
	return ttl
}
