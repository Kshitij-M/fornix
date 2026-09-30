package store

import (
	"context"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// TestPostgresWorkspaceIsolationQualification is intentionally opt-in. It must
// run with a dedicated NOBYPASSRLS, non-owner role; the ordinary development
// role is a superuser/table owner and therefore cannot prove enforcement.
//
// The test uses one transaction and rolls it back, so it creates no durable
// qualification rows. The inserted fixture is deliberately minimal because it
// exercises the database policy rather than the operation store contract.
func TestPostgresWorkspaceIsolationQualification(t *testing.T) {
	dsn := os.Getenv("FORNIX_RLS_TEST_DSN")
	if dsn == "" {
		t.Skip("FORNIX_RLS_TEST_DSN is not set")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create qualification pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping qualification database: %v", err)
	}

	var superuser, bypassRLS bool
	if err := pool.QueryRow(ctx, `
		SELECT r.rolsuper, r.rolbypassrls
		FROM pg_roles r
		WHERE r.rolname = current_user`).Scan(&superuser, &bypassRLS); err != nil {
		t.Fatalf("inspect runtime role: %v", err)
	}
	if superuser || bypassRLS {
		t.Fatalf("qualification requires a non-superuser NOBYPASSRLS role")
	}

	var total, protected int
	if err := pool.QueryRow(ctx, `
		SELECT count(*), count(*) FILTER (WHERE c.relrowsecurity)
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = 'fornix'
		  AND c.relkind IN ('r', 'p')
		  AND c.relname <> 'schema_migrations'
		  AND EXISTS (
			SELECT 1 FROM pg_attribute a
			WHERE a.attrelid = c.oid AND a.attname IN ('workspace_id', 'audit_workspace_id') AND NOT a.attisdropped
		  )`).Scan(&total, &protected); err != nil {
		t.Fatalf("inspect RLS policies: %v", err)
	}
	if total == 0 || protected != total {
		t.Fatalf("expected RLS on every workspace-scoped table, got %d/%d", protected, total)
	}
	var policies int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM pg_policies p
		JOIN pg_class c ON c.relname = p.tablename
		JOIN pg_namespace n ON n.oid = c.relnamespace AND n.nspname = p.schemaname
		WHERE p.schemaname = 'fornix' AND p.policyname = 'workspace_scope_isolation'
		  AND EXISTS (
			SELECT 1 FROM pg_attribute a
			WHERE a.attrelid = c.oid AND a.attname IN ('workspace_id', 'audit_workspace_id') AND NOT a.attisdropped
		  )`).Scan(&policies); err != nil {
		t.Fatalf("inspect workspace policies: %v", err)
	}
	if policies != total {
		t.Fatalf("expected one workspace policy per scoped table, got %d/%d", policies, total)
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatalf("begin qualification transaction: %v", err)
	}
	defer tx.Rollback(ctx) // the fixture must never become authoritative data

	var unscoped int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM fornix.operations`).Scan(&unscoped); err != nil {
		t.Fatalf("query without workspace context: %v", err)
	}
	if unscoped != 0 {
		t.Fatalf("unset workspace context exposed %d operation rows", unscoped)
	}
	if _, err := tx.Exec(ctx, `SELECT fornix.set_workspace_context('rls-workspace-a')`); err != nil {
		t.Fatalf("set workspace context: %v", err)
	}

	const hashA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const hashB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.operations (
			workspace_id, id, request_id, idempotency_key, request_hash,
			operation_hash, status, actor, request, state_hash
		) VALUES ('rls-workspace-a', 'rls-qualification-op', 'rls-request',
			'rls-idempotency', $1, $2, 'created', '{}'::jsonb, '{}'::jsonb, $2)`, hashA, hashB); err != nil {
		t.Fatalf("same-workspace insert was rejected: %v", err)
	}

	var visibleOther int
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM fornix.operations WHERE workspace_id = 'rls-workspace-b'`).Scan(&visibleOther); err != nil {
		t.Fatalf("query other workspace: %v", err)
	}
	if visibleOther != 0 {
		t.Fatalf("workspace context exposed %d foreign operation rows", visibleOther)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.operations (
			workspace_id, id, request_id, idempotency_key, request_hash,
			operation_hash, status, actor, request, state_hash
		) VALUES ('rls-workspace-b', 'rls-foreign-op', 'rls-foreign-request',
			'rls-foreign-idempotency', $1, $2, 'created', '{}'::jsonb, '{}'::jsonb, $2)`, hashA, hashB); err == nil {
		t.Fatalf("cross-workspace insert unexpectedly succeeded")
	}
}
