package store

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestOperationQueueClaimIsBoundedWorkspaceScopedAndFenced(t *testing.T) {
	store, _, workspace := newOperationTestStore(t)
	created := make([]Operation, 0, 3)
	for index := 0; index < 3; index++ {
		result, err := store.Create(context.Background(), OperationCreateInput{Request: operationTestRequest(t, workspace, "queue-"+string(rune('a'+index)))})
		if err != nil {
			t.Fatal(err)
		}
		created = append(created, result.Operation)
	}

	first, err := store.ClaimReady(context.Background(), workspace, "queue-worker-a", 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 2 {
		t.Fatalf("claims=%d, want two", len(first))
	}
	for _, claim := range first {
		if claim.Operation.WorkspaceID != workspace || claim.Lease.WorkspaceID != workspace || claim.Lease.OwnerID != "queue-worker-a" || claim.Lease.Fence != 1 {
			t.Fatalf("invalid first claim: %+v", claim)
		}
	}

	second, err := store.ClaimReady(context.Background(), workspace, "queue-worker-b", 64, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 {
		t.Fatalf("second claims=%d, want one unleased operation", len(second))
	}
	if second[0].Lease.OwnerID != "queue-worker-b" || second[0].Lease.Fence != 1 {
		t.Fatalf("unexpected second claim lease: %+v", second[0].Lease)
	}
	claimed := map[string]bool{}
	for _, claim := range first {
		claimed[claim.Operation.ID] = true
	}
	if claimed[second[0].Operation.ID] {
		t.Fatalf("queue returned an already active operation: %s", second[0].Operation.ID)
	}
	if len(created) != 3 {
		t.Fatalf("created=%d, want three", len(created))
	}

	foreignWorkspace := workspace + "-foreign"
	foreign, err := store.Create(context.Background(), OperationCreateInput{Request: operationTestRequest(t, foreignWorkspace, "queue-foreign")})
	if err != nil {
		t.Fatal(err)
	}
	if claims, err := store.ClaimReady(context.Background(), workspace, "queue-worker-c", 64, time.Minute); err != nil {
		t.Fatal(err)
	} else if len(claims) != 0 {
		t.Fatalf("active workspace claim exposed unrelated work: %+v", claims)
	}
	if claims, err := store.ClaimReady(context.Background(), foreignWorkspace, "queue-worker-d", 1, time.Minute); err != nil {
		t.Fatal(err)
	} else if len(claims) != 1 || claims[0].Operation.ID != foreign.Operation.ID {
		t.Fatalf("foreign workspace claim mismatch: %+v", claims)
	}
}

func TestOperationQueueConcurrentClaimHasOneOwnerAndExpiryTakeover(t *testing.T) {
	store, _, workspace := newOperationTestStore(t)
	created, err := store.Create(context.Background(), OperationCreateInput{Request: operationTestRequest(t, workspace, "queue-concurrent")})
	if err != nil {
		t.Fatal(err)
	}

	start := make(chan struct{})
	results := make(chan []OperationClaim, 2)
	errorsCh := make(chan error, 2)
	var wait sync.WaitGroup
	for _, owner := range []string{"queue-owner-a", "queue-owner-b"} {
		owner := owner
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			claims, claimErr := store.ClaimReady(context.Background(), workspace, owner, 1, 25*time.Millisecond)
			if claimErr != nil {
				errorsCh <- claimErr
				return
			}
			results <- claims
		}()
	}
	close(start)
	wait.Wait()
	close(results)
	close(errorsCh)
	for err := range errorsCh {
		t.Fatal(err)
	}
	owners := make([]OperationClaim, 0, 2)
	for claims := range results {
		owners = append(owners, claims...)
	}
	if len(owners) != 1 || owners[0].Operation.ID != created.Operation.ID {
		t.Fatalf("concurrent claims=%+v, want one owner for %s", owners, created.Operation.ID)
	}

	time.Sleep(40 * time.Millisecond)
	takeover, err := store.ClaimReady(context.Background(), workspace, "queue-takeover", 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(takeover) != 1 || takeover[0].Operation.ID != created.Operation.ID {
		t.Fatalf("takeover claims=%+v, want expired operation", takeover)
	}
	if takeover[0].Lease.OwnerID != "queue-takeover" || takeover[0].Lease.Fence != 2 {
		t.Fatalf("takeover did not advance fence: %+v", takeover[0].Lease)
	}
	if _, err := store.RenewLease(context.Background(), owners[0].Lease, time.Minute); !errors.Is(err, ErrOperationLeaseFenced) && !errors.Is(err, ErrOperationLeaseExpired) && !errors.Is(err, ErrOperationLeaseOwned) {
		t.Fatalf("stale owner renewal error=%v", err)
	}
}
