package store

import (
	"context"
	"fmt"
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

// TestFederationCapacityQualification is an opt-in disposable-database
// qualification for the workspace-scoped federation authority. It measures
// local database behavior only; it is never a production SLO claim.
func TestFederationCapacityQualification(t *testing.T) {
	dsn := os.Getenv("FORNIX_FEDERATION_CAPACITY_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_FEDERATION_CAPACITY_PG_DSN is not set")
	}
	operations := boundedEnvInt(t, "FORNIX_FEDERATION_CAPACITY_OPERATIONS", 64, 1, 512)
	workers := boundedEnvInt(t, "FORNIX_FEDERATION_CAPACITY_WORKERS", 4, 1, 16)
	maxP95Millis := boundedEnvInt(t, "FORNIX_FEDERATION_CAPACITY_MAX_P95_MS", 0, 0, 60_000)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create federation capacity pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	workspace := fmt.Sprintf("federation-capacity-%d", time.Now().UnixNano())
	store := NewFederationStore(pool, NewEventStore(pool), NewWorkspaceCoordinationStore(pool, NewEventStore(pool)))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		for _, table := range []string{"federation_retention_tombstones", "workspace_federation_poll_attempts", "workspace_federation_peer_leases", "workspace_federation_peer_commands", "workspace_federation_peers", "control_events"} {
			_, _ = pool.Exec(cleanupCtx, "DELETE FROM fornix."+table+" WHERE workspace_id=$1", workspace)
		}
	})
	actor := contracts.ActorRef{ID: "capacity-operator", Kind: "service", WorkspaceID: workspace}
	for ordinal := 0; ordinal < operations; ordinal++ {
		peer := contracts.FederationPeer{ID: fmt.Sprintf("peer-%04d", ordinal), WorkspaceID: workspace, RemoteWorkspaceID: fmt.Sprintf("remote-%04d", ordinal), EndpointURL: "https://peer.example.test/root", CredentialRef: "provider/federation", CreatedBy: actor}
		if _, _, err := store.CreatePeer(ctx, federationPeerCommand(workspace, peer, fmt.Sprintf("capacity-peer-%04d", ordinal))); err != nil {
			t.Fatalf("create peer %d: %v", ordinal, err)
		}
	}
	if err := flushCapacityDatabaseStats(ctx, pool); err != nil {
		t.Fatal(err)
	}
	beforeStats, err := readCapacityDatabaseStats(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	beforeBytes, err := readCapacityRelationBytes(ctx, pool, []string{"workspace_federation_peers", "workspace_federation_peer_leases", "workspace_federation_poll_attempts", "control_events"})
	if err != nil {
		t.Fatal(err)
	}
	type sample struct{ lease, poll time.Duration }
	samples := make(chan sample, operations)
	jobs := make(chan int)
	errs := make(chan error, operations)
	start := time.Now()
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wait.Add(1)
		go func() {
			defer wait.Done()
			for ordinal := range jobs {
				peerID := fmt.Sprintf("peer-%04d", ordinal)
				leaseStart := time.Now()
				lease, leaseErr := store.AcquirePeerLease(ctx, workspace, peerID, fmt.Sprintf("capacity-worker-%d", worker), time.Minute)
				leaseLatency := time.Since(leaseStart)
				if leaseErr != nil {
					errs <- leaseErr
					continue
				}
				pollStart := time.Now()
				_, _, pollErr := store.BeginPoll(ctx, contracts.FederationPollRequest{RequestID: fmt.Sprintf("capacity-request-%04d", ordinal), IdempotencyKey: fmt.Sprintf("capacity-poll-%04d", ordinal), WorkspaceID: workspace, PeerID: peerID, OwnerID: lease.OwnerID, Fence: lease.Fence, Actor: actor}, lease)
				pollLatency := time.Since(pollStart)
				if pollErr != nil {
					errs <- pollErr
					continue
				}
				if releaseErr := store.ReleasePeerLease(ctx, lease); releaseErr != nil {
					errs <- releaseErr
					continue
				}
				samples <- sample{lease: leaseLatency, poll: pollLatency}
			}
		}()
	}
	for ordinal := 0; ordinal < operations; ordinal++ {
		jobs <- ordinal
	}
	close(jobs)
	wait.Wait()
	close(samples)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	values := make([]federationCapacitySample, 0, operations)
	for sample := range samples {
		values = append(values, federationCapacitySample{Lease: sample.lease, Poll: sample.poll})
	}
	if len(values) != operations {
		t.Fatalf("capacity samples=%d want=%d", len(values), operations)
	}
	leaseP50, leaseP95, leaseP99 := federationPercentiles(values, func(value federationCapacitySample) time.Duration { return value.Lease })
	pollP50, pollP95, pollP99 := federationPercentiles(values, func(value federationCapacitySample) time.Duration { return value.Poll })
	if maxP95Millis > 0 && (leaseP95 > time.Duration(maxP95Millis)*time.Millisecond || pollP95 > time.Duration(maxP95Millis)*time.Millisecond) {
		t.Fatalf("federation p95 exceeded budget lease=%s poll=%s budget=%dms", leaseP95, pollP95, maxP95Millis)
	}
	if err := flushCapacityDatabaseStats(ctx, pool); err != nil {
		t.Fatal(err)
	}
	afterStats, err := readCapacityDatabaseStats(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	afterBytes, err := readCapacityRelationBytes(ctx, pool, []string{"workspace_federation_peers", "workspace_federation_peer_leases", "workspace_federation_poll_attempts", "control_events"})
	if err != nil {
		t.Fatal(err)
	}
	var waitingLocks int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM pg_locks WHERE database= (SELECT oid FROM pg_database WHERE datname=current_database()) AND NOT granted`).Scan(&waitingLocks); err != nil {
		t.Fatal(err)
	}
	t.Logf("federation capacity operations=%d workers=%d elapsed=%s lease_p50=%s lease_p95=%s lease_p99=%s poll_p50=%s poll_p95=%s poll_p99=%s tx_delta=%d rollback_delta=%d blocks_read_delta=%d blocks_hit_delta=%d relation_bytes_delta=%d waiting_locks=%d", operations, workers, time.Since(start), leaseP50, leaseP95, leaseP99, pollP50, pollP95, pollP99, afterStats.Transactions-beforeStats.Transactions, afterStats.Rollbacks-beforeStats.Rollbacks, afterStats.BlocksRead-beforeStats.BlocksRead, afterStats.BlocksHit-beforeStats.BlocksHit, afterBytes-beforeBytes, waitingLocks)
}

type federationCapacitySample struct{ Lease, Poll time.Duration }

func federationPercentiles(values []federationCapacitySample, selectDuration func(federationCapacitySample) time.Duration) (time.Duration, time.Duration, time.Duration) {
	durations := make([]time.Duration, 0, len(values))
	for _, value := range values {
		durations = append(durations, selectDuration(value))
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	percentile := func(value int) time.Duration {
		if len(durations) == 0 {
			return 0
		}
		index := (len(durations)*value + 99) / 100
		if index < 1 {
			index = 1
		}
		return durations[index-1]
	}
	return percentile(50), percentile(95), percentile(99)
}
