package federation

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

const (
	defaultRetentionOwnerWorkspaceLimit = 100
	maxRetentionOwnerWorkspaceLimit     = 1000
	defaultRetentionOwnerBatchSize      = 100
)

// RetentionWorkspaceLister is deliberately narrower than OperatorStore so the
// owner can be qualified without a live database and deployments cannot
// accidentally substitute an unscoped workspace query.
type RetentionWorkspaceLister interface {
	ListWorkspaces(context.Context, int, string) (store.WorkspacePage, error)
}

// RetentionSweeper is the fenced store operation. Implementations must check
// OwnerID and Fence in the same transaction as their mutation.
type RetentionSweeper interface {
	RetentionSweep(context.Context, contracts.FederationRetentionRequest) (contracts.FederationRetentionResult, error)
}

// RetentionLeaseAuthority is the existing Postgres consumer-lease boundary.
type RetentionLeaseAuthority interface {
	AcquireConsumerLease(context.Context, string, string, string, time.Duration) (contracts.ConsumerLeaseResult, error)
	ReleaseConsumerLease(context.Context, contracts.ConsumerLease) error
}

// RetentionOwnerRequest controls one bounded, deterministic owner pass. It
// contains no raw payloads and is safe to construct from an operator or a
// deployment scheduler.
type RetentionOwnerRequest struct {
	Before         time.Time
	BatchSize      int
	WorkspaceLimit int
	DryRun         bool
	Actor          contracts.ActorRef
	RequestID      string
	IdempotencyKey string
	CausationID    string
	CorrelationID  string
}

// RetentionOwnerResult is a bounded operational report. FailureWorkspaces
// contains only workspace identifiers, never provider responses or payloads.
type RetentionOwnerResult struct {
	OwnerID           string
	Before            time.Time
	DryRun            bool
	WorkspacesScanned int
	LeasesAcquired    int
	LeasesBusy        int
	SweepsCommitted   int
	SweepsFailed      int
	PollExpired       int
	QuarantineExpired int
	FailureWorkspaces []string
}

// RetentionOwner acquires one workspace-scoped fence at a time and invokes
// the existing transactional retention store. It never holds a lease while
// scanning the workspace directory and never retries a held lease in a hot
// loop.
type RetentionOwner struct {
	Workspaces RetentionWorkspaceLister
	Leases     RetentionLeaseAuthority
	Sweeper    RetentionSweeper
	OwnerID    string
	LeaseTTL   time.Duration
}

func (o *RetentionOwner) RunOnce(ctx context.Context, request RetentionOwnerRequest) (RetentionOwnerResult, error) {
	if o == nil || o.Workspaces == nil || o.Leases == nil || o.Sweeper == nil || strings.TrimSpace(o.OwnerID) == "" {
		return RetentionOwnerResult{}, errors.New("retention owner is not configured")
	}
	if err := normalizeRetentionOwnerRequest(&request, o.OwnerID); err != nil {
		return RetentionOwnerResult{}, err
	}
	leaseTTL := o.LeaseTTL
	if leaseTTL <= 0 {
		leaseTTL = contracts.DefaultConsumerLeaseTTL
	}
	if leaseTTL > contracts.MaxConsumerLeaseTTL {
		leaseTTL = contracts.MaxConsumerLeaseTTL
	}
	result := RetentionOwnerResult{OwnerID: o.OwnerID, Before: request.Before, DryRun: request.DryRun}
	cursor := ""
	for result.WorkspacesScanned < request.WorkspaceLimit {
		pageLimit := request.WorkspaceLimit - result.WorkspacesScanned
		if pageLimit > defaultRetentionOwnerWorkspaceLimit {
			pageLimit = defaultRetentionOwnerWorkspaceLimit
		}
		page, err := o.Workspaces.ListWorkspaces(ctx, pageLimit, cursor)
		if err != nil {
			return result, fmt.Errorf("list retention workspaces: %w", err)
		}
		if len(page.Items) == 0 {
			break
		}
		for _, workspace := range page.Items {
			if result.WorkspacesScanned >= request.WorkspaceLimit {
				break
			}
			result.WorkspacesScanned++
			workspaceID := strings.TrimSpace(workspace.ID)
			if workspaceID == "" {
				result.SweepsFailed++
				continue
			}
			actor := request.Actor
			if actor.WorkspaceID != "" && actor.WorkspaceID != workspaceID {
				result.recordFailure(workspaceID)
				continue
			}
			actor.WorkspaceID = workspaceID
			if actor.ID == "" {
				actor.ID = o.OwnerID
			}
			if actor.Kind == "" {
				actor.Kind = "service"
			}
			leaseResult, err := o.Leases.AcquireConsumerLease(ctx, workspaceID, contracts.FederationRetentionConsumerID, o.OwnerID, leaseTTL)
			if err != nil {
				if errors.Is(err, store.ErrConsumerLeaseHeld) {
					result.LeasesBusy++
				} else {
					result.recordFailure(workspaceID)
				}
				continue
			}
			result.LeasesAcquired++
			retentionRequest := contracts.FederationRetentionRequest{
				RequestID:      request.RequestID + ":" + workspaceID,
				IdempotencyKey: request.IdempotencyKey + ":" + workspaceID,
				WorkspaceID:    workspaceID,
				Before:         request.Before,
				BatchSize:      request.BatchSize,
				DryRun:         request.DryRun,
				OwnerID:        leaseResult.Lease.OwnerID,
				Fence:          leaseResult.Lease.Fence,
				Actor:          actor,
				CausationID:    request.CausationID,
				CorrelationID:  request.CorrelationID,
			}
			sweep, sweepErr := o.Sweeper.RetentionSweep(ctx, retentionRequest)
			if sweepErr != nil {
				result.recordFailure(workspaceID)
			} else {
				result.SweepsCommitted++
				result.PollExpired += sweep.PollExpired
				result.QuarantineExpired += sweep.QuarantineExpired
			}
			releaseCtx, releaseCancel := context.WithTimeout(context.Background(), 10*time.Second)
			releaseErr := o.Leases.ReleaseConsumerLease(releaseCtx, leaseResult.Lease)
			releaseCancel()
			if releaseErr != nil {
				result.recordFailure(workspaceID)
			}
		}
		if page.NextCursor == "" {
			break
		}
		if page.NextCursor == cursor || (cursor != "" && page.NextCursor < cursor) {
			return result, errors.New("retention workspace cursor did not advance")
		}
		cursor = page.NextCursor
	}
	return result, nil
}

func normalizeRetentionOwnerRequest(request *RetentionOwnerRequest, ownerID string) error {
	if request == nil {
		return errors.New("retention owner request is nil")
	}
	if request.Before.IsZero() {
		request.Before = time.Now().UTC()
	}
	request.Before = request.Before.UTC()
	if request.BatchSize <= 0 {
		request.BatchSize = defaultRetentionOwnerBatchSize
	}
	if request.BatchSize > contracts.MaxFederationRetentionBatchSize {
		request.BatchSize = contracts.MaxFederationRetentionBatchSize
	}
	if request.WorkspaceLimit <= 0 {
		request.WorkspaceLimit = defaultRetentionOwnerWorkspaceLimit
	}
	if request.WorkspaceLimit > maxRetentionOwnerWorkspaceLimit {
		request.WorkspaceLimit = maxRetentionOwnerWorkspaceLimit
	}
	request.RequestID = strings.TrimSpace(request.RequestID)
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	if request.RequestID == "" {
		request.RequestID = "retention-owner-" + contracts.HashStrings(ownerID, request.Before.Format(time.RFC3339Nano))[:48]
	}
	if request.IdempotencyKey == "" {
		request.IdempotencyKey = request.RequestID
	}
	if len(request.RequestID) > contracts.MaxIdempotencyLength || len(request.IdempotencyKey) > contracts.MaxIdempotencyLength {
		return errors.New("retention owner request identity is too large")
	}
	if request.Actor.ID != "" {
		request.Actor.ID = strings.TrimSpace(request.Actor.ID)
	}
	if request.Actor.Kind != "" {
		request.Actor.Kind = strings.TrimSpace(request.Actor.Kind)
	}
	if request.Actor.ID == "" {
		request.Actor.ID = ownerID
	}
	if request.Actor.Kind == "" {
		request.Actor.Kind = "service"
	}
	return nil
}

func (r *RetentionOwnerResult) recordFailure(workspaceID string) {
	r.SweepsFailed++
	if len(r.FailureWorkspaces) < 64 {
		r.FailureWorkspaces = append(r.FailureWorkspaces, workspaceID)
	}
}
