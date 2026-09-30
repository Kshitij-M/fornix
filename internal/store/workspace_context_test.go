package store

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestWithWorkspaceTxRequiresCallbackAndScope(t *testing.T) {
	if err := WithWorkspaceTx(context.Background(), nil, "workspace-a", nil); err == nil {
		t.Fatal("nil callback unexpectedly succeeded")
	}
	if err := WithWorkspaceTx(context.Background(), nil, "", func(pgx.Tx) error { return nil }); err == nil {
		t.Fatal("empty workspace unexpectedly succeeded")
	}
}

// TestWorkspaceContextClearsAcrossPooledTransactionReuse is opt-in because
// it needs Postgres. A single acquired connection makes leakage deterministic:
// commit and rollback must both clear the transaction-local setting before
// that connection can serve another workspace.
func TestWorkspaceContextClearsAcrossPooledTransactionReuse(t *testing.T) {
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse test pool config: %v", err)
	}
	config.MaxConns = 1
	config.MinConns = 0
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatalf("create test pool: %v", err)
	}
	defer pool.Close()
	ctx := context.Background()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping test pool: %v", err)
	}
	if err := ApplyMigrations(ctx, pool); err != nil {
		t.Fatalf("apply migrations: %v", err)
	}

	callbackErr := errors.New("synthetic scoped failure")
	if err := WithWorkspaceTx(ctx, pool, "workspace-c", func(tx pgx.Tx) error {
		var current string
		if err := tx.QueryRow(ctx, `SELECT current_setting('fornix.workspace_id', true)`).Scan(&current); err != nil {
			return err
		}
		if current != "workspace-c" {
			return errors.New("callback did not receive workspace context")
		}
		return callbackErr
	}); !errors.Is(err, callbackErr) {
		t.Fatalf("callback error=%v, want %v", err, callbackErr)
	}

	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire single test connection: %v", err)
	}
	defer conn.Release()

	assertEmptyContext := func(label string) {
		t.Helper()
		var value string
		if err := conn.QueryRow(ctx, `SELECT current_setting('fornix.workspace_id', true)`).Scan(&value); err != nil {
			t.Fatalf("read %s workspace context: %v", label, err)
		}
		if value != "" {
			t.Fatalf("workspace context leaked after %s: %q", label, value)
		}
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin committed transaction: %v", err)
	}
	if err := SetWorkspaceContext(ctx, tx, "workspace-a"); err != nil {
		t.Fatalf("set committed context: %v", err)
	}
	var value string
	if err := tx.QueryRow(ctx, `SELECT current_setting('fornix.workspace_id', true)`).Scan(&value); err != nil {
		t.Fatalf("read committed context: %v", err)
	}
	if value != "workspace-a" {
		t.Fatalf("committed transaction context=%q, want workspace-a", value)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("commit scoped transaction: %v", err)
	}
	assertEmptyContext("commit")

	tx, err = conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin rolled-back transaction: %v", err)
	}
	if err := SetWorkspaceContext(ctx, tx, "workspace-b"); err != nil {
		t.Fatalf("set rolled-back context: %v", err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback scoped transaction: %v", err)
	}
	assertEmptyContext("rollback")

	assertEmptyContext("callback rollback")
}

// TestRuntimeRoleWorkspaceRLSPoolReuseFailsClosed is run by the explicit
// role-separated qualification after the administrator has installed two
// synthetic federation rows. It proves actual RLS behavior as the non-owner
// application role while repeatedly reusing one physical connection.
func TestRuntimeRoleWorkspaceRLSPoolReuseFailsClosed(t *testing.T) {
	dsn := os.Getenv("FORNIX_RLS_TEST_DSN")
	if dsn == "" {
		t.Skip("FORNIX_RLS_TEST_DSN is not set")
	}
	expectedRole := os.Getenv("FORNIX_RLS_APP_ROLE")
	const workspaceA = "qualification-federation-workspace"
	const workspaceB = "qualification-federation-foreign"

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse runtime-role qualification DSN: %v", err)
	}
	config.MaxConns, config.MinConns = 1, 0
	ctx := context.Background()
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("create single-connection runtime pool: %v", err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("ping runtime-role qualification pool: %v", err)
	}
	conn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire runtime connection: %v", err)
	}
	defer conn.Release()

	type rowQueryer interface {
		QueryRow(context.Context, string, ...any) pgx.Row
	}

	var currentUser, tableOwner string
	var superuser, bypassRLS, rowSecurity bool
	err = conn.QueryRow(ctx, `
		SELECT current_user, runtime.rolsuper, runtime.rolbypassrls,
		       pg_get_userbyid(table_owner.relowner), table_owner.relrowsecurity
		FROM pg_roles runtime
	JOIN pg_class table_owner ON table_owner.oid='fornix.workspace_federation_peers'::regclass
		WHERE runtime.rolname=current_user`).Scan(&currentUser, &superuser, &bypassRLS, &tableOwner, &rowSecurity)
	if err != nil {
		t.Fatalf("inspect runtime role and fixture RLS: %v", err)
	}
	if expectedRole != "" && currentUser != expectedRole {
		t.Fatalf("connected role=%q, want configured runtime role", currentUser)
	}
	if superuser || bypassRLS || tableOwner == currentUser || !rowSecurity {
		t.Fatalf("runtime role is not independently subject to fixture RLS: role=%q superuser=%t bypass_rls=%t owner=%t rls=%t", currentUser, superuser, bypassRLS, tableOwner == currentUser, rowSecurity)
	}

	workspaceSetting := func(queryer rowQueryer, label string) {
		t.Helper()
		var setting string
		if err := queryer.QueryRow(ctx, `SELECT COALESCE(current_setting('fornix.workspace_id', true), '')`).Scan(&setting); err != nil {
			t.Fatalf("read workspace setting after %s: %v", label, err)
		}
		if setting != "" {
			t.Fatalf("workspace setting survived %s: %q", label, setting)
		}
	}
	visibleCount := func(queryer rowQueryer, workspaceID string) int {
		t.Helper()
		var count int
		if err := queryer.QueryRow(ctx, `SELECT count(*) FROM fornix.workspace_federation_peers WHERE workspace_id=$1 AND peer_id='qualification-peer'`, workspaceID).Scan(&count); err != nil {
			t.Fatalf("query fixture visibility for %q: %v", workspaceID, err)
		}
		return count
	}
	assertVisibleOnly := func(queryer rowQueryer, label, expectedWorkspace string) {
		t.Helper()
		wantA, wantB := 0, 0
		if expectedWorkspace == workspaceA {
			wantA = 1
		} else if expectedWorkspace == workspaceB {
			wantB = 1
		} else if expectedWorkspace != "" {
			t.Fatalf("test requested unknown expected workspace %q", expectedWorkspace)
		}
		if got := visibleCount(queryer, workspaceA); got != wantA {
			t.Fatalf("workspace A rows visible %s=%d, want %d", label, got, wantA)
		}
		if got := visibleCount(queryer, workspaceB); got != wantB {
			t.Fatalf("workspace B rows visible %s=%d, want %d", label, got, wantB)
		}
	}

	workspaceSetting(conn, "initial unscoped read")
	assertVisibleOnly(conn, "without context", "")

	commitWorkspace := func(workspaceID string) {
		t.Helper()
		tx, err := conn.Begin(ctx)
		if err != nil {
			t.Fatalf("begin transaction for %q: %v", workspaceID, err)
		}
		if err := SetWorkspaceContext(ctx, tx, workspaceID); err != nil {
			_ = tx.Rollback(ctx)
			t.Fatalf("set transaction-local workspace %q: %v", workspaceID, err)
		}
		gotA, gotB := visibleCount(tx, workspaceA), visibleCount(tx, workspaceB)
		wantA, wantB := 0, 0
		if workspaceID == workspaceA {
			wantA = 1
		} else if workspaceID == workspaceB {
			wantB = 1
		}
		if gotA != wantA || gotB != wantB {
			_ = tx.Rollback(ctx)
			t.Fatalf("workspace %q saw fixture counts A=%d B=%d", workspaceID, gotA, gotB)
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatalf("commit transaction for %q: %v", workspaceID, err)
		}
		workspaceSetting(conn, "commit for "+workspaceID)
		assertVisibleOnly(conn, "after commit for "+workspaceID, "")
	}
	commitWorkspace(workspaceA)
	commitWorkspace(workspaceB)

	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatalf("begin rollback transaction: %v", err)
	}
	if err := SetWorkspaceContext(ctx, tx, workspaceA); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("set rollback workspace: %v", err)
	}
	if gotA, gotB := visibleCount(tx, workspaceA), visibleCount(tx, workspaceB); gotA != 1 || gotB != 0 {
		_ = tx.Rollback(ctx)
		t.Fatalf("workspace A before rollback saw fixture counts A=%d B=%d", gotA, gotB)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatalf("rollback workspace A transaction: %v", err)
	}
	workspaceSetting(conn, "rollback for "+workspaceA)
	assertVisibleOnly(conn, "after rollback", "")
}
