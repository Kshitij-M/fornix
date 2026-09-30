package store

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultCapacityOperations = 128
	maxCapacityOperations     = 2048
	defaultCapacityWorkers    = 4
	maxCapacityWorkers        = 32
)

type capacityDatabaseStats struct {
	Transactions int64
	Rollbacks    int64
	BlocksRead   int64
	BlocksHit    int64
}

type capacitySample struct {
	Create time.Duration
	Lease  time.Duration
}

// TestOperationCapacityQualification is an opt-in, bounded local capacity
// qualification. It is deliberately separate from ordinary integration tests
// because it creates a measurable workload and must use a disposable DSN.
func TestOperationCapacityQualification(t *testing.T) {
	dsn := os.Getenv("FORNIX_CAPACITY_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_CAPACITY_PG_DSN is not set")
	}
	operations := boundedEnvInt(t, "FORNIX_CAPACITY_OPERATIONS", defaultCapacityOperations, 1, maxCapacityOperations)
	workers := boundedEnvInt(t, "FORNIX_CAPACITY_WORKERS", defaultCapacityWorkers, 1, maxCapacityWorkers)
	maxP95Millis := boundedEnvInt(t, "FORNIX_CAPACITY_MAX_P95_MS", 0, 0, 60_000)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create capacity pool: %v", err)
	}
	// Register pool closure first. testing.T runs cleanup callbacks in reverse
	// registration order, so the workspace cleanup below runs while the pool is
	// still usable.
	t.Cleanup(pool.Close)
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping capacity database: %v", err)
	}
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("apply capacity migrations: %v", err)
	}

	workspace := fmt.Sprintf("capacity-qualification-%d", time.Now().UnixNano())
	operationStore := NewOperationStore(pool, NewEventStore(pool))
	tables := []string{
		"operations", "operation_idempotency", "operation_transitions",
		"operation_resources", "operation_steps", "operation_attempts",
		"operation_effects", "operation_callbacks", "operation_leases",
		"operation_links", "operation_results", "operation_admission_decisions",
		"operation_approvals", "operation_approval_transitions", "operation_effect_state",
		"operation_effect_transitions", "operation_effect_leases", "operation_authority_links", "control_events",
	}
	if err := flushCapacityDatabaseStats(ctx, pool); err != nil {
		t.Fatalf("flush capacity database stats before workload: %v", err)
	}
	beforeStats, err := readCapacityDatabaseStats(ctx, pool)
	if err != nil {
		t.Fatalf("read capacity database stats before workload: %v", err)
	}
	beforeBytes, err := readCapacityRelationBytes(ctx, pool, tables)
	if err != nil {
		t.Fatalf("read capacity relation sizes before workload: %v", err)
	}
	requests := make([]OperationCreateInput, operations)
	for ordinal := range requests {
		requests[ordinal] = OperationCreateInput{Request: operationTestRequest(t, workspace, fmt.Sprintf("capacity-%06d", ordinal))}
	}

	jobs := make(chan int)
	samples := make(chan capacitySample, operations)
	errors := make(chan error, operations)
	start := time.Now()
	var wait sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		worker := worker
		wait.Add(1)
		go func() {
			defer wait.Done()
			for ordinal := range jobs {
				createStart := time.Now()
				created, createErr := operationStore.Create(ctx, requests[ordinal])
				createLatency := time.Since(createStart)
				if createErr != nil {
					errors <- fmt.Errorf("worker %d create ordinal %d: %w", worker, ordinal, createErr)
					continue
				}
				duplicate, duplicateErr := operationStore.Create(ctx, requests[ordinal])
				if duplicateErr != nil {
					errors <- fmt.Errorf("worker %d duplicate ordinal %d: %w", worker, ordinal, duplicateErr)
					continue
				}
				if !duplicate.Duplicate || duplicate.Operation.ID != created.Operation.ID {
					errors <- fmt.Errorf("worker %d duplicate ordinal %d did not return original operation", worker, ordinal)
					continue
				}
				leaseStart := time.Now()
				lease, leaseErr := operationStore.AcquireLease(ctx, workspace, created.Operation.ID, fmt.Sprintf("capacity-worker-%d-op-%d", worker, ordinal), time.Minute)
				leaseLatency := time.Since(leaseStart)
				if leaseErr != nil {
					errors <- fmt.Errorf("worker %d lease ordinal %d: %w", worker, ordinal, leaseErr)
					continue
				}
				if releaseErr := operationStore.ReleaseLease(ctx, lease.Lease); releaseErr != nil {
					errors <- fmt.Errorf("worker %d release ordinal %d: %w", worker, ordinal, releaseErr)
					continue
				}
				samples <- capacitySample{Create: createLatency, Lease: leaseLatency}
			}
		}()
	}
	for ordinal := 0; ordinal < operations; ordinal++ {
		jobs <- ordinal
	}
	close(jobs)
	wait.Wait()
	close(samples)
	close(errors)
	if err := firstCapacityError(errors); err != nil {
		t.Fatal(err)
	}

	var operationCount, eventCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.operations WHERE workspace_id=$1`, workspace).Scan(&operationCount); err != nil {
		t.Fatalf("count capacity operations: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1`, workspace).Scan(&eventCount); err != nil {
		t.Fatalf("count capacity events: %v", err)
	}
	if operationCount != operations || eventCount != operations {
		t.Fatalf("capacity history count operations=%d events=%d, want %d each", operationCount, eventCount, operations)
	}

	if err := flushCapacityDatabaseStats(ctx, pool); err != nil {
		t.Fatalf("flush capacity database stats after workload: %v", err)
	}
	afterStats, err := readCapacityDatabaseStats(ctx, pool)
	if err != nil {
		t.Fatalf("read capacity database stats after workload: %v", err)
	}
	afterBytes, err := readCapacityRelationBytes(ctx, pool, tables)
	if err != nil {
		t.Fatalf("read capacity relation sizes after workload: %v", err)
	}
	allSamples := collectCapacitySamples(samples)
	if len(allSamples) != operations {
		t.Fatalf("recorded %d samples, want %d", len(allSamples), operations)
	}
	createP50, createP95, createMax := durationPercentiles(allSamples, func(sample capacitySample) time.Duration { return sample.Create })
	leaseP50, leaseP95, leaseMax := durationPercentiles(allSamples, func(sample capacitySample) time.Duration { return sample.Lease })
	if maxP95Millis > 0 && (createP95 > time.Duration(maxP95Millis)*time.Millisecond || leaseP95 > time.Duration(maxP95Millis)*time.Millisecond) {
		t.Fatalf("capacity p95 exceeded configured budget: create=%s lease=%s budget=%dms", createP95, leaseP95, maxP95Millis)
	}

	t.Logf("capacity qualification operations=%d workers=%d elapsed=%s create_p50=%s create_p95=%s create_max=%s lease_p50=%s lease_p95=%s lease_max=%s tx_delta=%d rollback_delta=%d blocks_read_delta=%d blocks_hit_delta=%d relation_bytes_delta=%d", operations, workers, time.Since(start), createP50, createP95, createMax, leaseP50, leaseP95, leaseMax, afterStats.Transactions-beforeStats.Transactions, afterStats.Rollbacks-beforeStats.Rollbacks, afterStats.BlocksRead-beforeStats.BlocksRead, afterStats.BlocksHit-beforeStats.BlocksHit, afterBytes-beforeBytes)
}

func boundedEnvInt(t *testing.T, name string, fallback, minimum, maximum int) int {
	t.Helper()
	value := fallback
	if raw := os.Getenv(name); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("%s must be an integer: %v", name, err)
		}
		value = parsed
	}
	if value < minimum || value > maximum {
		t.Fatalf("%s=%d is outside [%d,%d]", name, value, minimum, maximum)
	}
	return value
}

func firstCapacityError(errors <-chan error) error {
	for err := range errors {
		return err
	}
	return nil
}

func collectCapacitySamples(samples <-chan capacitySample) []capacitySample {
	values := make([]capacitySample, 0)
	for sample := range samples {
		values = append(values, sample)
	}
	return values
}

func durationPercentiles(samples []capacitySample, selectDuration func(capacitySample) time.Duration) (time.Duration, time.Duration, time.Duration) {
	values := make([]time.Duration, 0, len(samples))
	for _, sample := range samples {
		values = append(values, selectDuration(sample))
	}
	sort.Slice(values, func(i, j int) bool { return values[i] < values[j] })
	if len(values) == 0 {
		return 0, 0, 0
	}
	percentileIndex := func(percent int) int {
		index := (len(values)*percent + 99) / 100
		if index < 1 {
			index = 1
		}
		return index - 1
	}
	return values[percentileIndex(50)], values[percentileIndex(95)], values[len(values)-1]
}

func readCapacityDatabaseStats(ctx context.Context, pool *pgxpool.Pool) (capacityDatabaseStats, error) {
	var stats capacityDatabaseStats
	err := pool.QueryRow(ctx, `
		SELECT xact_commit, xact_rollback, blks_read, blks_hit
		FROM pg_stat_database
		WHERE datname = current_database()`).Scan(&stats.Transactions, &stats.Rollbacks, &stats.BlocksRead, &stats.BlocksHit)
	return stats, err
}

func flushCapacityDatabaseStats(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `SELECT pg_stat_force_next_flush()`)
	return err
}

func readCapacityRelationBytes(ctx context.Context, pool *pgxpool.Pool, tables []string) (int64, error) {
	var bytes int64
	err := pool.QueryRow(ctx, `
		SELECT COALESCE(sum(pg_total_relation_size(c.oid)), 0)::bigint
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'fornix' AND c.relname = ANY($1::text[])`, tables).Scan(&bytes)
	return bytes, err
}
