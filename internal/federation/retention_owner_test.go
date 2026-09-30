package federation

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func TestRetentionOwnerIsBoundedWorkspaceScopedAndFenced(t *testing.T) {
	lister := &retentionOwnerLister{items: []contracts.Workspace{{ID: "workspace-a"}, {ID: "workspace-b"}, {ID: "workspace-c"}}}
	leases := &retentionOwnerLeases{busy: map[string]bool{"workspace-b": true}}
	sweeper := &retentionOwnerSweeper{}
	owner := &RetentionOwner{Workspaces: lister, Leases: leases, Sweeper: sweeper, OwnerID: "retention-worker", LeaseTTL: time.Second}
	result, err := owner.RunOnce(context.Background(), RetentionOwnerRequest{Before: time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC), WorkspaceLimit: 2, Actor: contracts.ActorRef{ID: "scheduler", Kind: "service"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.WorkspacesScanned != 2 || result.LeasesAcquired != 1 || result.LeasesBusy != 1 || result.SweepsCommitted != 1 || len(sweeper.requests) != 1 {
		t.Fatalf("unexpected owner result=%+v requests=%+v", result, sweeper.requests)
	}
	request := sweeper.requests[0]
	if request.WorkspaceID != "workspace-a" || request.OwnerID != "retention-worker" || request.Fence != 1 || request.Actor.WorkspaceID != "workspace-a" {
		t.Fatalf("retention request was not scoped/fenced: %+v", request)
	}
	if len(leases.released) != 1 || leases.released[0].Fence != request.Fence {
		t.Fatalf("lease release=%+v", leases.released)
	}
}

func TestRetentionOwnerFailsClosedOnCrossWorkspaceActorAndStalledCursor(t *testing.T) {
	lister := &retentionOwnerLister{items: []contracts.Workspace{{ID: "workspace-b"}}, stalled: true}
	owner := &RetentionOwner{Workspaces: lister, Leases: &retentionOwnerLeases{}, Sweeper: &retentionOwnerSweeper{}, OwnerID: "retention-worker"}
	result, err := owner.RunOnce(context.Background(), RetentionOwnerRequest{WorkspaceLimit: 1, Actor: contracts.ActorRef{ID: "actor", Kind: "service", WorkspaceID: "workspace-a"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.SweepsCommitted != 0 || result.SweepsFailed != 1 || len(result.FailureWorkspaces) != 1 {
		t.Fatalf("cross-workspace actor was not rejected: %+v", result)
	}

	stalled := &RetentionOwner{Workspaces: &retentionOwnerLister{items: []contracts.Workspace{{ID: "workspace-a"}, {ID: "workspace-b"}, {ID: "workspace-c"}}, stalled: true}, Leases: &retentionOwnerLeases{}, Sweeper: &retentionOwnerSweeper{}, OwnerID: "retention-worker"}
	if _, err := stalled.RunOnce(context.Background(), RetentionOwnerRequest{WorkspaceLimit: 2}); err == nil {
		t.Fatal("stalled workspace cursor was accepted")
	}
}

type retentionOwnerLister struct {
	items   []contracts.Workspace
	stalled bool
}

func (l *retentionOwnerLister) ListWorkspaces(_ context.Context, limit int, cursor string) (store.WorkspacePage, error) {
	start := 0
	for start < len(l.items) && l.items[start].ID <= cursor {
		start++
	}
	pageSize := limit
	if l.stalled && pageSize > 1 {
		pageSize = 1
	}
	end := start + pageSize
	if end > len(l.items) {
		end = len(l.items)
	}
	page := store.WorkspacePage{Items: append([]contracts.Workspace(nil), l.items[start:end]...)}
	if end < len(l.items) {
		if l.stalled && cursor != "" {
			page.NextCursor = cursor
		} else {
			page.NextCursor = l.items[end-1].ID
		}
	}
	return page, nil
}

type retentionOwnerLeases struct {
	busy     map[string]bool
	released []contracts.ConsumerLease
}

func (l *retentionOwnerLeases) AcquireConsumerLease(_ context.Context, workspaceID, consumerID, ownerID string, _ time.Duration) (contracts.ConsumerLeaseResult, error) {
	if l.busy != nil && l.busy[workspaceID] {
		return contracts.ConsumerLeaseResult{}, store.ErrConsumerLeaseHeld
	}
	return contracts.ConsumerLeaseResult{Acquired: true, Lease: contracts.ConsumerLease{WorkspaceID: workspaceID, ConsumerID: consumerID, OwnerID: ownerID, Fence: 1, LeaseUntil: time.Now().Add(time.Minute)}}, nil
}

func (l *retentionOwnerLeases) ReleaseConsumerLease(_ context.Context, lease contracts.ConsumerLease) error {
	l.released = append(l.released, lease)
	return nil
}

type retentionOwnerSweeper struct {
	requests []contracts.FederationRetentionRequest
}

func (s *retentionOwnerSweeper) RetentionSweep(_ context.Context, request contracts.FederationRetentionRequest) (contracts.FederationRetentionResult, error) {
	if request.OwnerID == "" || request.Fence == 0 {
		return contracts.FederationRetentionResult{}, errors.New("missing retention fence")
	}
	s.requests = append(s.requests, request)
	return contracts.FederationRetentionResult{WorkspaceID: request.WorkspaceID, PollExpired: 1}, nil
}
