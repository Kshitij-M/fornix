package store

import (
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// The migration runner follows Orloj's numbered, embedded migration pattern:
// migrations are immutable, checksummed, serialized with a Postgres advisory
// lock, and recorded only after their transaction commits.
//
//go:embed migrations/*.sql
var migrationFS embed.FS

const migrationAdvisoryLockID int64 = 7_301_947

// ApplyMigrations applies embedded numbered migrations in order under a
// Postgres advisory lock. Checksums make applied migration files immutable and
// existing catalogs are preserved for compatibility with the legacy fabric
// schema name.
func ApplyMigrations(ctx context.Context, pool *pgxpool.Pool) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("acquire migration connection: %w", err)
	}
	defer conn.Release()

	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock($1)`, migrationAdvisoryLockID); err != nil {
		return fmt.Errorf("acquire migration lock: %w", err)
	}
	defer func() {
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, migrationAdvisoryLockID)
	}()

	if _, err := conn.Exec(ctx, `
DO $$
BEGIN
  IF to_regclass('fornix.schema_migrations') IS NULL THEN
    CREATE SCHEMA IF NOT EXISTS fabric;
    CREATE TABLE IF NOT EXISTS fabric.schema_migrations (
      version TEXT PRIMARY KEY,
      checksum TEXT NOT NULL,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  ELSE
    CREATE SCHEMA IF NOT EXISTS fornix;
    CREATE TABLE IF NOT EXISTS fornix.schema_migrations (
      version TEXT PRIMARY KEY,
      checksum TEXT NOT NULL,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  END IF;
END $$`); err != nil {
		return fmt.Errorf("create migration table: %w", err)
	}

	entries, err := fs.ReadDir(migrationFS, "migrations")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") {
			continue
		}
		if err := applyOne(ctx, conn, entry.Name()); err != nil {
			return err
		}
	}
	return nil
}

func applyOne(ctx context.Context, conn *pgxpool.Conn, name string) error {
	version := strings.TrimSuffix(name, ".sql")
	content, err := fs.ReadFile(migrationFS, "migrations/"+name)
	if err != nil {
		return fmt.Errorf("read migration %s: %w", version, err)
	}
	digest := sha256.Sum256(content)
	checksum := hex.EncodeToString(digest[:])

	catalog, err := migrationCatalog(ctx, conn)
	if err != nil {
		return fmt.Errorf("select migration catalog: %w", err)
	}
	var appliedChecksum string
	err = conn.QueryRow(ctx, migrationSelect(catalog), version).Scan(&appliedChecksum)
	if err == nil {
		if appliedChecksum != checksum {
			return fmt.Errorf("migration %s checksum mismatch: database=%s source=%s", version, appliedChecksum, checksum)
		}
		return nil
	}
	if err != pgx.ErrNoRows {
		return fmt.Errorf("check migration %s: %w", version, err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", version, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, string(content)); err != nil {
		return fmt.Errorf("apply migration %s: %w", version, err)
	}
	catalog, err = migrationCatalog(ctx, tx)
	if err != nil {
		return fmt.Errorf("select migration catalog after %s: %w", version, err)
	}
	if _, err := tx.Exec(ctx, migrationInsert(catalog), version, checksum); err != nil {
		return fmt.Errorf("record migration %s: %w", version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %s: %w", version, err)
	}
	return nil
}

func migrationCatalog(ctx context.Context, conn interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) (string, error) {
	var catalog string
	if err := conn.QueryRow(ctx, `
		SELECT CASE
			WHEN to_regclass('fornix.schema_migrations') IS NOT NULL THEN 'fornix'
			WHEN to_regclass('fabric.schema_migrations') IS NOT NULL THEN 'fabric'
			ELSE ''
		END`).Scan(&catalog); err != nil {
		return "", err
	}
	if catalog == "" {
		return "", fmt.Errorf("no migration catalog exists")
	}
	return catalog, nil
}

func migrationSelect(catalog string) string {
	if catalog == "fornix" {
		return `SELECT checksum FROM fornix.schema_migrations WHERE version=$1`
	}
	return `SELECT checksum FROM fabric.schema_migrations WHERE version=$1`
}

func migrationInsert(catalog string) string {
	if catalog == "fornix" {
		return `INSERT INTO fornix.schema_migrations(version, checksum) VALUES($1,$2)`
	}
	return `INSERT INTO fabric.schema_migrations(version, checksum) VALUES($1,$2)`
}
