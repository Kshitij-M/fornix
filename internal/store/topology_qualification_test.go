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

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// TestPostgresTopologyQualification is opt-in because it applies the current
// migrations to the explicitly supplied topology. It measures authority and
// pool prerequisites; it does not claim to execute a failover or PITR drill.
func TestPostgresTopologyQualification(t *testing.T) {
	dsn := os.Getenv("FORNIX_TOPOLOGY_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TOPOLOGY_PG_DSN is not set")
	}
	operations := boundedQualificationInt(t, "FORNIX_TOPOLOGY_OPERATIONS", 32, 512)
	workers := boundedQualificationInt(t, "FORNIX_TOPOLOGY_WORKERS", 4, 16)
	maxConns := boundedQualificationInt(t, "FORNIX_TOPOLOGY_POOL_MAX", workers, 32)
	if maxConns < workers {
		maxConns = workers
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse topology DSN: %v", err)
	}
	config.MaxConns = int32(maxConns)
	config.MinConns = 0
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("create topology pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping topology: %v", err)
	}
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("apply topology migrations: %v", err)
	}
	facts, err := readPostgresTopologyFacts(ctx, pool)
	if err != nil {
		t.Fatalf("read topology facts: %v", err)
	}
	if facts.InRecovery {
		t.Fatalf("topology DSN points at a recovery/standby node; authority must use the writable primary")
	}
	if facts.ReadOnly {
		t.Fatalf("topology DSN is read-only; migrations and authority writes require a writable primary")
	}
	if os.Getenv("FORNIX_TOPOLOGY_REQUIRE_ARCHIVE") == "true" && facts.ArchiveMode != "on" && facts.ArchiveMode != "always" {
		t.Fatalf("archive requirement was enabled but archive_mode=%q", facts.ArchiveMode)
	}

	if err := qualifyWorkspaceContextConcurrency(ctx, pool, operations, workers); err != nil {
		t.Fatal(err)
	}
	acquireMillis, err := qualifyPoolAcquisition(ctx, pool, operations)
	if err != nil {
		t.Fatal(err)
	}
	stat := pool.Stat()
	fmt.Printf("postgres topology qualification: version=%s recovery=%t archive_mode=%s wal_level=%s operations=%d workers=%d pool_max=%d acquire_p50_ms=%.3f acquire_p95_ms=%.3f acquire_p99_ms=%.3f empty_acquires=%d canceled_acquires=%d ha_failover=not_executed pitr=deployment_owned\n", facts.ServerVersion, facts.InRecovery, facts.ArchiveMode, facts.WALLevel, operations, workers, maxConns, percentileMillis(acquireMillis, 0.50), percentileMillis(acquireMillis, 0.95), percentileMillis(acquireMillis, 0.99), stat.EmptyAcquireCount(), stat.CanceledAcquireCount())
}

type postgresTopologyFacts struct {
	ServerVersion  string
	InRecovery     bool
	ArchiveMode    string
	ArchiveCommand string
	WALLevel       string
	ReadOnly       bool
}

func readPostgresTopologyFacts(ctx context.Context, pool *pgxpool.Pool) (postgresTopologyFacts, error) {
	var facts postgresTopologyFacts
	err := pool.QueryRow(ctx, `SELECT current_setting('server_version_num'), pg_is_in_recovery(), current_setting('archive_mode'), current_setting('archive_command'), current_setting('wal_level'), current_setting('default_transaction_read_only')::boolean`).Scan(&facts.ServerVersion, &facts.InRecovery, &facts.ArchiveMode, &facts.ArchiveCommand, &facts.WALLevel, &facts.ReadOnly)
	return facts, err
}

func qualifyWorkspaceContextConcurrency(ctx context.Context, pool *pgxpool.Pool, operations, workers int) error {
	// Verify the rollback path directly as well as the public commit-owning
	// helper below. Both paths must clear the transaction-local setting.
	rollbackTx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin rollback qualification: %w", err)
	}
	if err := SetWorkspaceContext(ctx, rollbackTx, "topology-rollback"); err != nil {
		_ = rollbackTx.Rollback(ctx)
		return fmt.Errorf("set rollback workspace context: %w", err)
	}
	if err := assertWorkspaceContext(ctx, rollbackTx, "topology-rollback"); err != nil {
		_ = rollbackTx.Rollback(ctx)
		return fmt.Errorf("read rollback workspace context: %w", err)
	}
	if err := rollbackTx.Rollback(ctx); err != nil {
		return fmt.Errorf("rollback workspace context: %w", err)
	}
	if err := WithWorkspaceTx(ctx, pool, "topology-commit", func(tx pgx.Tx) error {
		return assertWorkspaceContext(ctx, tx, "topology-commit")
	}); err != nil {
		return fmt.Errorf("workspace commit qualification: %w", err)
	}
	var wg sync.WaitGroup
	errCh := make(chan error, workers)
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for iteration := worker; iteration < operations; iteration += workers {
				workspace := fmt.Sprintf("topology-worker-%d", worker%2)
				err := WithWorkspaceTx(ctx, pool, workspace, func(tx pgx.Tx) error {
					return assertWorkspaceContext(ctx, tx, workspace)
				})
				if err != nil {
					errCh <- err
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			return fmt.Errorf("concurrent workspace qualification: %w", err)
		}
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire context hygiene connection: %w", err)
	}
	defer conn.Release()
	var leaked string
	if err := conn.QueryRow(ctx, `SELECT current_setting('fornix.workspace_id', true)`).Scan(&leaked); err != nil {
		return fmt.Errorf("read pooled workspace context: %w", err)
	}
	if leaked != "" {
		return fmt.Errorf("workspace context leaked across pooled transactions")
	}
	return nil
}

func assertWorkspaceContext(ctx context.Context, tx pgx.Tx, expected string) error {
	var actual string
	if err := tx.QueryRow(ctx, `SELECT current_setting('fornix.workspace_id', true)`).Scan(&actual); err != nil {
		return err
	}
	if actual != expected {
		return fmt.Errorf("workspace context=%q, want %q", actual, expected)
	}
	return nil
}

func qualifyPoolAcquisition(ctx context.Context, pool *pgxpool.Pool, operations int) ([]time.Duration, error) {
	durations := make([]time.Duration, 0, operations)
	for index := 0; index < operations; index++ {
		started := time.Now()
		conn, err := pool.Acquire(ctx)
		if err != nil {
			return nil, err
		}
		conn.Release()
		durations = append(durations, time.Since(started))
	}
	return durations, nil
}

func percentileMillis(values []time.Duration, fraction float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), values...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	index := int(float64(len(sorted)-1) * fraction)
	return float64(sorted[index].Microseconds()) / 1000
}

func boundedQualificationInt(t *testing.T, name string, fallback, maximum int) int {
	t.Helper()
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 1 || value > maximum {
		t.Fatalf("%s must be between 1 and %d", name, maximum)
	}
	return value
}
