package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestConsumerLeaseLifecycleAndFencing(t *testing.T) {
	store, _, workspaceID := newEventTestStore(t)
	first, err := store.AcquireConsumerLease(context.Background(), workspaceID, "projection.tasks", "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Acquired || first.Takeover || first.Lease.Fence != 1 {
		t.Fatalf("first acquire=%+v", first)
	}

	reused, err := store.AcquireConsumerLease(context.Background(), workspaceID, "projection.tasks", "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !reused.Reused || reused.Acquired || reused.Lease.Fence != first.Lease.Fence || !reused.Lease.LeaseUntil.Equal(first.Lease.LeaseUntil) {
		t.Fatalf("same-owner acquire=%+v first=%+v", reused, first)
	}

	if _, err := store.AcquireConsumerLease(context.Background(), workspaceID, "projection.tasks", "worker-b", time.Second); !errors.Is(err, ErrConsumerLeaseHeld) {
		t.Fatalf("active competing acquire error=%v, want ErrConsumerLeaseHeld", err)
	}

	renewed, err := store.RenewConsumerLease(context.Background(), first.Lease, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if renewed.Fence != first.Lease.Fence || !renewed.LeaseUntil.After(first.Lease.LeaseUntil) {
		t.Fatalf("renewed lease=%+v first=%+v", renewed, first.Lease)
	}

	if err := store.ReleaseConsumerLease(context.Background(), renewed); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RenewConsumerLease(context.Background(), renewed, time.Second); !errors.Is(err, ErrConsumerLeaseReleased) {
		t.Fatalf("released renew error=%v, want ErrConsumerLeaseReleased", err)
	}

	takeover, err := store.AcquireConsumerLease(context.Background(), workspaceID, "projection.tasks", "worker-b", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !takeover.Acquired || !takeover.Takeover || takeover.Lease.Fence != 2 {
		t.Fatalf("takeover=%+v", takeover)
	}
	if _, err := store.RenewConsumerLease(context.Background(), renewed, time.Second); !errors.Is(err, ErrConsumerLeaseFenced) {
		t.Fatalf("stale renew error=%v, want ErrConsumerLeaseFenced", err)
	}
	if err := store.ReleaseConsumerLease(context.Background(), renewed); !errors.Is(err, ErrConsumerLeaseFenced) {
		t.Fatalf("stale release error=%v, want ErrConsumerLeaseFenced", err)
	}
}

func TestConsumerLeaseExpiryTakeoverAndWorkspaceIsolation(t *testing.T) {
	store, _, workspaceID := newEventTestStore(t)
	first, err := store.AcquireConsumerLease(context.Background(), workspaceID, "projection.tasks", "worker-a", 20*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	takeover, err := store.AcquireConsumerLease(context.Background(), workspaceID, "projection.tasks", "worker-b", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if takeover.Lease.Fence != first.Lease.Fence+1 {
		t.Fatalf("expired takeover fence=%d want=%d", takeover.Lease.Fence, first.Lease.Fence+1)
	}

	otherWorkspace := workspaceID + "-other"
	t.Cleanup(func() {
		_, _ = store.pool.Exec(context.Background(), `DELETE FROM fornix.consumer_leases WHERE workspace_id=$1`, otherWorkspace)
	})
	other, err := store.AcquireConsumerLease(context.Background(), otherWorkspace, "projection.tasks", "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if other.Lease.Fence != 1 {
		t.Fatalf("isolated workspace fence=%d want=1", other.Lease.Fence)
	}
	if _, err := store.RenewConsumerLease(context.Background(), contracts.ConsumerLease{
		WorkspaceID: otherWorkspace,
		ConsumerID:  "projection.tasks",
		OwnerID:     "worker-a",
		Fence:       takeover.Lease.Fence,
	}, time.Second); !errors.Is(err, ErrConsumerLeaseFenced) {
		t.Fatalf("cross-workspace token error=%v, want ErrConsumerLeaseFenced", err)
	}
}

func TestConsumerLeaseConcurrentAcquireHasOneOwner(t *testing.T) {
	store, _, workspaceID := newEventTestStore(t)
	const workers = 12
	results := make(chan contracts.ConsumerLeaseResult, workers)
	errorsCh := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			result, err := store.AcquireConsumerLease(context.Background(), workspaceID, "projection.concurrent", fmt.Sprintf("worker-%d", i), time.Second)
			if err != nil {
				errorsCh <- err
				return
			}
			results <- result
		}(i)
	}
	wg.Wait()
	close(results)
	close(errorsCh)

	acquired := 0
	held := 0
	for result := range results {
		if result.Acquired {
			acquired++
		}
	}
	for err := range errorsCh {
		if errors.Is(err, ErrConsumerLeaseHeld) {
			held++
			continue
		}
		t.Fatalf("concurrent acquire error=%v", err)
	}
	if acquired != 1 || held != workers-1 {
		t.Fatalf("acquired=%d held=%d want acquired=1 held=%d", acquired, held, workers-1)
	}
}

func TestConsumerLeaseTransactionRollbackLeavesNoLease(t *testing.T) {
	store, _, workspaceID := newEventTestStore(t)
	tx, err := store.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.AcquireConsumerLeaseTx(context.Background(), tx, workspaceID, "projection.rollback", "worker-a", time.Second); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, found, err := store.GetConsumerLease(context.Background(), workspaceID, "projection.rollback"); err != nil {
		t.Fatal(err)
	} else if found {
		t.Fatal("rolled-back lease unexpectedly exists")
	}
}

func TestConsumerLeaseRenewAndReleaseRollbackPreserveCommittedState(t *testing.T) {
	store, _, workspaceID := newEventTestStore(t)
	acquired, err := store.AcquireConsumerLease(context.Background(), workspaceID, "projection.rollback-state", "worker-a", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	before, active, err := store.GetConsumerLease(context.Background(), workspaceID, "projection.rollback-state")
	if err != nil || !active {
		t.Fatalf("read before=%+v active=%v err=%v", before, active, err)
	}

	tx, err := store.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.RenewConsumerLeaseTx(context.Background(), tx, acquired.Lease, 10*time.Second); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	afterRenewRollback, active, err := store.GetConsumerLease(context.Background(), workspaceID, "projection.rollback-state")
	if err != nil || !active || !afterRenewRollback.LeaseUntil.Equal(before.LeaseUntil) {
		t.Fatalf("renew rollback changed lease before=%+v after=%+v active=%v err=%v", before, afterRenewRollback, active, err)
	}

	tx, err = store.pool.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.ReleaseConsumerLeaseTx(context.Background(), tx, acquired.Lease); err != nil {
		_ = tx.Rollback(context.Background())
		t.Fatal(err)
	}
	if err := tx.Rollback(context.Background()); err != nil {
		t.Fatal(err)
	}
	afterReleaseRollback, active, err := store.GetConsumerLease(context.Background(), workspaceID, "projection.rollback-state")
	if err != nil || !active || afterReleaseRollback.ReleasedAt != nil {
		t.Fatalf("release rollback changed lease=%+v active=%v err=%v", afterReleaseRollback, active, err)
	}
}

func TestConsumerLeaseLatency(t *testing.T) {
	store, _, workspaceID := newEventTestStore(t)
	const samples = 20
	if _, err := store.AcquireConsumerLease(context.Background(), workspaceID, "projection.latency", "worker-a", time.Minute); err != nil {
		t.Fatal(err)
	}
	latencies := make([]time.Duration, 0, samples)
	for i := 0; i < samples; i++ {
		started := time.Now()
		result, err := store.AcquireConsumerLease(context.Background(), workspaceID, "projection.latency", "worker-a", time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if !result.Reused {
			t.Fatalf("latency sample unexpectedly changed ownership: %+v", result)
		}
		latencies = append(latencies, time.Since(started))
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	p95 := latencies[(len(latencies)*95+99)/100-1]
	t.Logf("consumer lease acquire/reuse latency samples=%d p50=%s p95=%s max=%s", samples, latencies[len(latencies)/2], p95, latencies[len(latencies)-1])
}
