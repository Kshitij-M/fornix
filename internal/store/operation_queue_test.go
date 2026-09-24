package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestOperationQueueClaimIsBoundedWorkspaceScopedAndFenced(t *testing.T) {
	store, _, workspace := newOperationTestStore(t)
	created := make([]Operation, 0, 3)
	for index := 0; index < 3; index++ {
		request := operationTestRequest(t, workspace, "queue-"+string(rune('a'+index)))
		request.Target.ID = fmt.Sprintf("record-%d", index)
		result, err := store.Create(context.Background(), OperationCreateInput{Request: request})
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

func TestOperationResourceLeaseSerializesDeclaredResourceAndAdvancesFence(t *testing.T) {
	store, pool, workspace := newOperationTestStore(t)
	resource := OperationResource{WorkspaceID: workspace, ResourceKind: "account", ResourceID: "account-1", Role: "target"}
	first, err := store.Create(context.Background(), OperationCreateInput{
		Request: operationTestRequest(t, workspace, "resource-first"), Resources: []OperationResource{resource},
	})
	if err != nil {
		t.Fatal(err)
	}
	secondRequest := operationTestRequest(t, workspace, "resource-second")
	secondRequest.Target.ID = "record-2"
	second, err := store.Create(context.Background(), OperationCreateInput{
		Request: secondRequest, Resources: []OperationResource{resource},
	})
	if err != nil {
		t.Fatal(err)
	}

	firstClaims, err := store.ClaimReady(context.Background(), workspace, "resource-owner-a", 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstClaims) != 1 || firstClaims[0].Operation.ID != first.Operation.ID || len(firstClaims[0].ResourceLeases) != 1 {
		t.Fatalf("first resource claim=%+v, want one resource lease", firstClaims)
	}
	secondClaims, err := store.ClaimReady(context.Background(), workspace, "resource-owner-b", 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondClaims) != 0 {
		t.Fatalf("resource-conflicting operation was claimed concurrently: %+v", secondClaims)
	}
	for index, status := range []string{contracts.OperationStatusPlanned, contracts.OperationStatusAdmitted, contracts.OperationStatusRunning, contracts.OperationStatusFailed} {
		if _, err := store.Transition(context.Background(), OperationTransitionInput{
			WorkspaceID: workspace, OperationID: first.Operation.ID, OwnerID: firstClaims[0].Lease.OwnerID,
			Fence: firstClaims[0].Lease.Fence, Actor: first.Operation.Request.Actor,
			IdempotencyKey: fmt.Sprintf("resource-terminal-%d", index), ToStatus: status,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.ReleaseLease(context.Background(), firstClaims[0].Lease); err != nil {
		t.Fatal(err)
	}

	secondClaims, err = store.ClaimReady(context.Background(), workspace, "resource-owner-b", 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondClaims) != 1 || secondClaims[0].Operation.ID != second.Operation.ID || len(secondClaims[0].ResourceLeases) != 1 {
		t.Fatalf("released resource was not reclaimed: %+v", secondClaims)
	}
	if secondClaims[0].ResourceLeases[0].Fence != 2 {
		t.Fatalf("resource fence=%d, want 2 after takeover", secondClaims[0].ResourceLeases[0].Fence)
	}

	var historyCount int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.operation_resource_lease_history WHERE workspace_id=$1 AND resource_key='account:account-1'`, workspace).Scan(&historyCount); err != nil {
		t.Fatal(err)
	}
	if historyCount != 3 {
		t.Fatalf("resource history rows=%d, want acquire, release, acquire", historyCount)
	}
}

func TestOperationQueueMaxActiveQuotaIsWorkspaceScoped(t *testing.T) {
	store, _, workspace := newOperationTestStore(t)
	firstRequest := operationTestRequest(t, workspace, "quota-first")
	firstRequest.Target.ID = "quota-record-1"
	first, err := store.Create(context.Background(), OperationCreateInput{Request: firstRequest})
	if err != nil {
		t.Fatal(err)
	}
	secondRequest := operationTestRequest(t, workspace, "quota-second")
	secondRequest.Target.ID = "quota-record-2"
	second, err := store.Create(context.Background(), OperationCreateInput{Request: secondRequest})
	if err != nil {
		t.Fatal(err)
	}

	claims, err := store.ClaimReadyWithOptions(context.Background(), workspace, "quota-owner-a", OperationClaimOptions{Limit: 2, MaxActive: 1, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(claims) != 1 || claims[0].Operation.ID != first.Operation.ID {
		t.Fatalf("quota claim=%+v, want one deterministic first operation", claims)
	}
	blocked, err := store.ClaimReadyWithOptions(context.Background(), workspace, "quota-owner-b", OperationClaimOptions{Limit: 2, MaxActive: 1, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(blocked) != 0 {
		t.Fatalf("workspace active quota was exceeded: %+v", blocked)
	}
	for index, status := range []string{contracts.OperationStatusPlanned, contracts.OperationStatusAdmitted, contracts.OperationStatusRunning, contracts.OperationStatusFailed} {
		if _, err := store.Transition(context.Background(), OperationTransitionInput{
			WorkspaceID: workspace, OperationID: first.Operation.ID, OwnerID: claims[0].Lease.OwnerID,
			Fence: claims[0].Lease.Fence, Actor: first.Operation.Request.Actor,
			IdempotencyKey: fmt.Sprintf("quota-terminal-%d", index), ToStatus: status,
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.ReleaseLease(context.Background(), claims[0].Lease); err != nil {
		t.Fatal(err)
	}
	available, err := store.ClaimReadyWithOptions(context.Background(), workspace, "quota-owner-b", OperationClaimOptions{Limit: 2, MaxActive: 1, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(available) != 1 || available[0].Operation.ID != second.Operation.ID {
		t.Fatalf("quota did not admit next operation after release: %+v", available)
	}

	foreignWorkspace := workspace + "-foreign"
	foreignRequest := operationTestRequest(t, foreignWorkspace, "quota-foreign")
	foreignRequest.Target.ID = "quota-record-foreign"
	foreign, err := store.Create(context.Background(), OperationCreateInput{Request: foreignRequest})
	if err != nil {
		t.Fatal(err)
	}
	foreignClaims, err := store.ClaimReadyWithOptions(context.Background(), foreignWorkspace, "quota-owner-c", OperationClaimOptions{Limit: 1, MaxActive: 1, TTL: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if len(foreignClaims) != 1 || foreignClaims[0].Operation.ID != foreign.Operation.ID {
		t.Fatalf("foreign workspace quota leaked: %+v", foreignClaims)
	}
}

func TestOperationResourceLeaseRejectsStaleOwnerAfterExpiryTakeover(t *testing.T) {
	store, pool, workspace := newOperationTestStore(t)
	created, err := store.Create(context.Background(), OperationCreateInput{
		Request:   operationTestRequest(t, workspace, "resource-stale"),
		Resources: []OperationResource{{WorkspaceID: workspace, ResourceKind: "deployment", ResourceID: "deployment-1"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	first, err := store.ClaimReady(context.Background(), workspace, "resource-stale-a", 1, 25*time.Millisecond)
	if err != nil || len(first) != 1 {
		t.Fatalf("first resource claim=%+v err=%v", first, err)
	}
	time.Sleep(45 * time.Millisecond)
	second, err := store.ClaimReady(context.Background(), workspace, "resource-stale-b", 1, time.Minute)
	if err != nil || len(second) != 1 || second[0].Operation.ID != created.Operation.ID {
		t.Fatalf("takeover resource claim=%+v err=%v", second, err)
	}
	if second[0].Lease.Fence != 2 || len(second[0].ResourceLeases) != 1 || second[0].ResourceLeases[0].Fence != 2 {
		t.Fatalf("operation/resource fences did not advance together: operation=%d resource=%+v", second[0].Lease.Fence, second[0].ResourceLeases)
	}
	if _, err := store.RenewLease(context.Background(), first[0].Lease, time.Minute); !errors.Is(err, ErrOperationLeaseFenced) && !errors.Is(err, ErrOperationLeaseExpired) {
		t.Fatalf("stale resource owner renewal error=%v", err)
	}
	if err := store.ReleaseLease(context.Background(), first[0].Lease); !errors.Is(err, ErrOperationLeaseFenced) && !errors.Is(err, ErrOperationLeaseExpired) {
		t.Fatalf("stale resource owner release error=%v", err)
	}
	var owner string
	if err := pool.QueryRow(context.Background(), `SELECT owner_id FROM fornix.operation_resource_leases WHERE workspace_id=$1 AND resource_key='deployment:deployment-1'`, workspace).Scan(&owner); err != nil {
		t.Fatal(err)
	}
	if owner != "resource-stale-b" {
		t.Fatalf("stale owner changed resource lease owner to %q", owner)
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
