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
	ErrAgentRunLeaseHeld      = errors.New("agent run worker lease is held by another owner")
	ErrAgentRunLeaseMissing   = errors.New("agent run worker lease not found")
	ErrAgentRunLeaseOwned     = errors.New("agent run worker lease is not owned by this owner")
	ErrAgentRunLeaseFenced    = errors.New("agent run worker lease fence is stale")
	ErrAgentRunLeaseExpired   = errors.New("agent run worker lease is expired")
	ErrAgentRunLeaseReleased  = errors.New("agent run worker lease is released")
	ErrAgentRunFenceExhausted = errors.New("agent run worker lease fence is exhausted")
)

const maxAgentRunFence = uint64(1<<63 - 1)

// ClaimNextAgentRun atomically selects one due run and creates or takes over
// its workspace-scoped worker lease. An empty workspaceID means that the
// caller is polling all workspaces; the returned row remains explicitly
// scoped and can only be used with the returned lease.
func (s *AgentRunStore) ClaimNextAgentRun(
	ctx context.Context,
	workspaceID, ownerID string,
	ttl time.Duration,
) (contracts.AgentRunClaim, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.AgentRunClaim{}, false, fmt.Errorf("agent run store is not configured")
	}
	workspaceID, ownerID = strings.TrimSpace(workspaceID), strings.TrimSpace(ownerID)
	if ownerID == "" {
		return contracts.AgentRunClaim{}, false, errors.New("owner_id is required")
	}
	ttl = contracts.NormalizeAgentRunLeaseTTL(ttl)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.AgentRunClaim{}, false, fmt.Errorf("begin agent run claim: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var selectedWorkspace, runID string
	err = tx.QueryRow(ctx, `
		SELECT ar.workspace_id, ar.id
		FROM fornix.agent_runs ar
		WHERE ($1 = '' OR ar.workspace_id = $1)
		  AND ar.state IN ('pending','running','awaiting_retry','awaiting_approval')
		  AND COALESCE(ar.next_retry_at, ar.next_scheduled_at, ar.created_at) <= clock_timestamp()
		  AND (
			ar.state <> 'awaiting_retry' OR ar.next_retry_at IS NULL OR ar.next_retry_at <= clock_timestamp()
		  )
		  AND (
			ar.state <> 'awaiting_approval' OR EXISTS (
				SELECT 1
				FROM fornix.tool_approvals approval
				WHERE approval.workspace_id = ar.workspace_id
				  AND approval.id = ar.pending_tools->0->>'approval_id'
				  AND approval.status = 'approved'
			)
		  )
		  AND NOT EXISTS (
			SELECT 1
			FROM fornix.agent_run_worker_leases lease
			WHERE lease.workspace_id = ar.workspace_id
			  AND lease.run_id = ar.id
			  AND lease.released_at IS NULL
			  AND lease.lease_until > clock_timestamp()
		  )
		ORDER BY
			ar.scheduler_priority DESC,
			COALESCE(ar.next_retry_at, ar.next_scheduled_at, ar.created_at) ASC,
			ar.created_at ASC,
			ar.workspace_id ASC,
			ar.id ASC
		FOR UPDATE OF ar SKIP LOCKED
		LIMIT 1`, workspaceID).Scan(&selectedWorkspace, &runID)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return contracts.AgentRunClaim{}, false, fmt.Errorf("commit empty agent run claim: %w", err)
		}
		return contracts.AgentRunClaim{}, false, nil
	}
	if err != nil {
		return contracts.AgentRunClaim{}, false, fmt.Errorf("select next agent run: %w", err)
	}

	run, err := readAgentRunTx(ctx, tx, selectedWorkspace, runID, true)
	if err != nil {
		return contracts.AgentRunClaim{}, false, fmt.Errorf("lock selected agent run: %w", err)
	}
	current, active, readErr := readAgentRunLeaseTx(ctx, tx, selectedWorkspace, runID, true)
	if readErr != nil && !errors.Is(readErr, ErrAgentRunLeaseMissing) {
		return contracts.AgentRunClaim{}, false, readErr
	}
	if readErr == nil && active {
		// All normal claimers lock the run before the lease. This branch is a
		// fail-closed guard for an out-of-band writer that created a lease after
		// the candidate snapshot was taken.
		if err := tx.Commit(ctx); err != nil {
			return contracts.AgentRunClaim{}, false, fmt.Errorf("commit held agent run claim: %w", err)
		}
		return contracts.AgentRunClaim{}, false, nil
	}

	takeover := readErr == nil
	if !takeover {
		if _, err := tx.Exec(ctx, `
			INSERT INTO fornix.agent_run_worker_leases(
				workspace_id, run_id, owner_id, fence, lease_until,
				acquired_at, renewed_at, released_at, updated_at
			) VALUES(
				$1, $2, $3, 1,
				clock_timestamp() + ($4::double precision * interval '1 millisecond'),
				clock_timestamp(), clock_timestamp(), NULL, clock_timestamp()
			)`, selectedWorkspace, runID, ownerID, ttl.Milliseconds()); err != nil {
			return contracts.AgentRunClaim{}, false, fmt.Errorf("insert agent run worker lease: %w", err)
		}
	} else {
		if current.Fence >= maxAgentRunFence {
			return contracts.AgentRunClaim{}, false, ErrAgentRunFenceExhausted
		}
		if _, err := tx.Exec(ctx, `
			UPDATE fornix.agent_run_worker_leases
			SET owner_id=$3,
			    fence=fence+1,
			    lease_until=clock_timestamp() + ($4::double precision * interval '1 millisecond'),
			    acquired_at=clock_timestamp(), renewed_at=clock_timestamp(),
			    released_at=NULL, updated_at=clock_timestamp()
			WHERE workspace_id=$1 AND run_id=$2 AND fence=$5`,
			selectedWorkspace, runID, ownerID, ttl.Milliseconds(), int64(current.Fence)); err != nil {
			return contracts.AgentRunClaim{}, false, fmt.Errorf("take over agent run worker lease: %w", err)
		}
	}
	lease, leaseActive, err := readAgentRunLeaseTx(ctx, tx, selectedWorkspace, runID, true)
	if err != nil {
		return contracts.AgentRunClaim{}, false, err
	}
	if !leaseActive || lease.OwnerID != ownerID || lease.Fence == 0 {
		return contracts.AgentRunClaim{}, false, errors.New("agent run worker lease acquisition did not produce an active owner")
	}
	lease.LeaseTTLMS = ttl.Milliseconds()
	if _, err := tx.Exec(ctx, `
		UPDATE fornix.agent_runs
		SET schedule_attempts=schedule_attempts+1, last_worker_id=$3
		WHERE workspace_id=$1 AND id=$2`, selectedWorkspace, runID, ownerID); err != nil {
		return contracts.AgentRunClaim{}, false, fmt.Errorf("record agent run scheduling attempt: %w", err)
	}
	if s.observability != nil {
		op := "claim"
		if takeover {
			op = "takeover"
		}
		if err := s.observability.recordObservationTx(ctx, tx, contracts.RunObservation{WorkspaceID: selectedWorkspace, IdempotencyKey: fmt.Sprintf("scheduler-observation:%s:%d", runID, lease.Fence), Kind: contracts.ObservationScheduler, Component: "agent_scheduler", Operation: op, Outcome: contracts.OutcomeSucceeded, Actor: contracts.ActorRef{ID: ownerID, Kind: "scheduler_worker", WorkspaceID: selectedWorkspace}, SourceKind: "agent_run_lease", SourceID: runID, StartedAt: time.Now().UTC(), Metadata: map[string]string{"fence": fmt.Sprintf("%d", lease.Fence)}}); err != nil {
			return contracts.AgentRunClaim{}, false, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.AgentRunClaim{}, false, fmt.Errorf("commit agent run claim: %w", err)
	}
	return contracts.AgentRunClaim{Run: run, Lease: lease, Takeover: takeover}, true, nil
}

// GetAgentRunLease reads ownership without authorizing a mutation.
func (s *AgentRunStore) GetAgentRunLease(ctx context.Context, workspaceID, runID string) (contracts.AgentRunLease, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.AgentRunLease{}, false, fmt.Errorf("agent run store is not configured")
	}
	workspaceID, runID = strings.TrimSpace(workspaceID), strings.TrimSpace(runID)
	if err := contracts.ValidateAgentRunLeaseIdentity(workspaceID, runID, "read"); err != nil {
		return contracts.AgentRunLease{}, false, err
	}
	lease, active, err := readAgentRunLease(ctx, s.pool, workspaceID, runID, false)
	if errors.Is(err, ErrAgentRunLeaseMissing) {
		return contracts.AgentRunLease{}, false, nil
	}
	return lease, active, err
}

// ValidateAgentRunLease is a non-locking admission check. CommitOwned repeats
// the same check while holding the lease row lock; callers must never rely on
// this read alone for an authoritative transition.
func (s *AgentRunStore) ValidateAgentRunLease(ctx context.Context, run contracts.AgentRun, lease contracts.AgentRunLease) error {
	if run.WorkspaceID != lease.WorkspaceID || run.ID != lease.RunID {
		return ErrAgentRunLeaseFenced
	}
	if err := contracts.ValidateAgentRunLeaseIdentity(lease.WorkspaceID, lease.RunID, lease.OwnerID); err != nil {
		return err
	}
	current, active, err := readAgentRunLease(ctx, s.pool, lease.WorkspaceID, lease.RunID, false)
	if err != nil {
		return err
	}
	if current.Fence != lease.Fence {
		return ErrAgentRunLeaseFenced
	}
	if current.OwnerID != lease.OwnerID {
		return ErrAgentRunLeaseOwned
	}
	if current.ReleasedAt != nil {
		return ErrAgentRunLeaseReleased
	}
	if !active {
		return ErrAgentRunLeaseExpired
	}
	return nil
}

// RenewAgentRunLease extends the exact live owner/fence. Expired or stale
// workers cannot revive a lease.
func (s *AgentRunStore) RenewAgentRunLease(ctx context.Context, lease contracts.AgentRunLease, ttl time.Duration) (contracts.AgentRunLease, error) {
	if s == nil || s.pool == nil {
		return contracts.AgentRunLease{}, fmt.Errorf("agent run store is not configured")
	}
	if err := contracts.ValidateAgentRunLeaseIdentity(lease.WorkspaceID, lease.RunID, lease.OwnerID); err != nil {
		return contracts.AgentRunLease{}, err
	}
	ttl = contracts.NormalizeAgentRunLeaseTTL(ttl)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.AgentRunLease{}, fmt.Errorf("begin agent run lease renewal: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := validateAgentRunLeaseTx(ctx, tx, lease); err != nil {
		return contracts.AgentRunLease{}, err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE fornix.agent_run_worker_leases
		SET lease_until=clock_timestamp() + ($5::double precision * interval '1 millisecond'),
		    renewed_at=clock_timestamp(), updated_at=clock_timestamp()
		WHERE workspace_id=$1 AND run_id=$2 AND owner_id=$3 AND fence=$4`,
		lease.WorkspaceID, lease.RunID, lease.OwnerID, int64(lease.Fence), ttl.Milliseconds()); err != nil {
		return contracts.AgentRunLease{}, fmt.Errorf("renew agent run worker lease: %w", err)
	}
	updated, active, err := readAgentRunLeaseTx(ctx, tx, lease.WorkspaceID, lease.RunID, true)
	if err != nil {
		return contracts.AgentRunLease{}, err
	}
	if !active {
		return contracts.AgentRunLease{}, ErrAgentRunLeaseExpired
	}
	updated.LeaseTTLMS = ttl.Milliseconds()
	if s.observability != nil {
		if err := s.observability.recordObservationTx(ctx, tx, contracts.RunObservation{WorkspaceID: lease.WorkspaceID, IdempotencyKey: fmt.Sprintf("scheduler-renew-observation:%s:%d", lease.RunID, lease.Fence), Kind: contracts.ObservationScheduler, Component: "agent_scheduler", Operation: "renew", Outcome: contracts.OutcomeSucceeded, Actor: contracts.ActorRef{ID: lease.OwnerID, Kind: "scheduler_worker", WorkspaceID: lease.WorkspaceID}, SourceKind: "agent_run_lease", SourceID: lease.RunID, StartedAt: time.Now().UTC(), Metadata: map[string]string{"fence": fmt.Sprintf("%d", lease.Fence)}}); err != nil {
			return contracts.AgentRunLease{}, err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.AgentRunLease{}, fmt.Errorf("commit agent run lease renewal: %w", err)
	}
	return updated, nil
}

// ReleaseAgentRunLease invalidates a live token without deleting its row.
func (s *AgentRunStore) ReleaseAgentRunLease(ctx context.Context, lease contracts.AgentRunLease) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("agent run store is not configured")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin agent run lease release: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := validateAgentRunLeaseTx(ctx, tx, lease); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE fornix.agent_run_worker_leases
		SET lease_until=clock_timestamp(), released_at=clock_timestamp(),
		    renewed_at=clock_timestamp(), updated_at=clock_timestamp()
		WHERE workspace_id=$1 AND run_id=$2 AND owner_id=$3 AND fence=$4`,
		lease.WorkspaceID, lease.RunID, lease.OwnerID, int64(lease.Fence)); err != nil {
		return fmt.Errorf("release agent run worker lease: %w", err)
	}
	if s.observability != nil {
		if err := s.observability.recordObservationTx(ctx, tx, contracts.RunObservation{WorkspaceID: lease.WorkspaceID, IdempotencyKey: fmt.Sprintf("scheduler-release-observation:%s:%d", lease.RunID, lease.Fence), Kind: contracts.ObservationScheduler, Component: "agent_scheduler", Operation: "release", Outcome: contracts.OutcomeSucceeded, Actor: contracts.ActorRef{ID: lease.OwnerID, Kind: "scheduler_worker", WorkspaceID: lease.WorkspaceID}, SourceKind: "agent_run_lease", SourceID: lease.RunID, StartedAt: time.Now().UTC(), Metadata: map[string]string{"fence": fmt.Sprintf("%d", lease.Fence)}}); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit agent run lease release: %w", err)
	}
	return nil
}

func validateAgentRunLeaseTx(ctx context.Context, tx pgx.Tx, lease contracts.AgentRunLease) (contracts.AgentRunLease, error) {
	if err := contracts.ValidateAgentRunLeaseIdentity(lease.WorkspaceID, lease.RunID, lease.OwnerID); err != nil {
		return contracts.AgentRunLease{}, err
	}
	if lease.Fence == 0 || lease.Fence > maxAgentRunFence {
		return contracts.AgentRunLease{}, ErrAgentRunLeaseFenced
	}
	current, active, err := readAgentRunLeaseTx(ctx, tx, lease.WorkspaceID, lease.RunID, true)
	if err != nil {
		return contracts.AgentRunLease{}, err
	}
	if current.Fence != lease.Fence {
		return current, fmt.Errorf("%w: expected=%d current=%d", ErrAgentRunLeaseFenced, lease.Fence, current.Fence)
	}
	if current.OwnerID != lease.OwnerID {
		return current, ErrAgentRunLeaseOwned
	}
	if current.ReleasedAt != nil {
		return current, ErrAgentRunLeaseReleased
	}
	if !active {
		return current, ErrAgentRunLeaseExpired
	}
	return current, nil
}

type agentRunLeaseQueryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func readAgentRunLease(ctx context.Context, queryer agentRunLeaseQueryer, workspaceID, runID string, lock bool) (contracts.AgentRunLease, bool, error) {
	query := `
		SELECT workspace_id, run_id, owner_id, fence, lease_until,
		       acquired_at, renewed_at, released_at,
		       (released_at IS NULL AND lease_until > clock_timestamp()) AS active
		FROM fornix.agent_run_worker_leases
		WHERE workspace_id=$1 AND run_id=$2`
	if lock {
		query += " FOR UPDATE"
	}
	var lease contracts.AgentRunLease
	var fence int64
	var active bool
	if err := queryer.QueryRow(ctx, query, workspaceID, runID).Scan(
		&lease.WorkspaceID, &lease.RunID, &lease.OwnerID, &fence,
		&lease.LeaseUntil, &lease.AcquiredAt, &lease.RenewedAt,
		&lease.ReleasedAt, &active); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.AgentRunLease{}, false, ErrAgentRunLeaseMissing
		}
		return contracts.AgentRunLease{}, false, fmt.Errorf("read agent run worker lease: %w", err)
	}
	if fence <= 0 {
		return contracts.AgentRunLease{}, false, errors.New("agent run worker lease has invalid fence")
	}
	lease.Fence = uint64(fence)
	return lease, active, nil
}

func readAgentRunLeaseTx(ctx context.Context, tx pgx.Tx, workspaceID, runID string, lock bool) (contracts.AgentRunLease, bool, error) {
	if tx == nil {
		return contracts.AgentRunLease{}, false, errors.New("agent run lease transaction is nil")
	}
	return readAgentRunLease(ctx, tx, strings.TrimSpace(workspaceID), strings.TrimSpace(runID), lock)
}
