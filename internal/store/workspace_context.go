package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// setWorkspaceContext binds one transaction to one workspace for the optional
// Postgres row-level-security policy. Application SQL must still include its
// explicit workspace predicates; this setting is defense in depth and is
// deliberately transaction-local so pooled connections cannot retain scope.
func setWorkspaceContext(ctx context.Context, tx pgx.Tx, workspaceID string) error {
	workspaceID = strings.TrimSpace(workspaceID)
	if tx == nil || workspaceID == "" {
		return fmt.Errorf("workspace context is required")
	}
	if _, err := tx.Exec(ctx, `SELECT fornix.set_workspace_context($1)`, workspaceID); err != nil {
		return fmt.Errorf("set postgres workspace context: %w", err)
	}
	return nil
}

// SetWorkspaceContext installs the transaction-local workspace context for
// packages that own a read-only store outside internal/store. It is a narrow
// boundary: callers still need an explicit workspace predicate in their SQL.
func SetWorkspaceContext(ctx context.Context, tx pgx.Tx, workspaceID string) error {
	return setWorkspaceContext(ctx, tx, workspaceID)
}

// beginWorkspaceTx starts a transaction with its tenant context already
// bound. Production application roles rely on this invariant when RLS is
// enabled; callers must not issue a workspace-scoped query before using this
// helper or an equivalent transaction boundary.
func beginWorkspaceTx(ctx context.Context, pool *pgxpool.Pool, workspaceID string) (pgx.Tx, error) {
	if pool == nil || strings.TrimSpace(workspaceID) == "" {
		return nil, fmt.Errorf("workspace transaction requires a pool and workspace")
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	if err := setWorkspaceContext(ctx, tx, workspaceID); err != nil {
		_ = tx.Rollback(ctx)
		return nil, err
	}
	return tx, nil
}

// BeginWorkspaceTx exposes the same transaction-local workspace boundary to
// domain adapters that must compose their own target write with a specialized
// store operation. Callers own commit/rollback and must keep all workspace
// predicates explicit in their SQL.
func BeginWorkspaceTx(ctx context.Context, pool *pgxpool.Pool, workspaceID string) (pgx.Tx, error) {
	return beginWorkspaceTx(ctx, pool, workspaceID)
}

// WithWorkspaceTx executes fn inside one transaction-local workspace scope.
// The helper owns commit and rollback so callers cannot accidentally commit a
// transaction whose context was never installed or return a connection with
// an application-owned transaction still open. fn must keep all SQL explicit
// about workspace predicates; RLS remains defense in depth.
func WithWorkspaceTx(ctx context.Context, pool *pgxpool.Pool, workspaceID string, fn func(pgx.Tx) error) error {
	if fn == nil {
		return fmt.Errorf("workspace transaction callback is required")
	}
	tx, err := beginWorkspaceTx(ctx, pool, workspaceID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func workspaceQueryRow(ctx context.Context, pool *pgxpool.Pool, workspaceID, query string, args []any, scan func(pgx.Row) error) error {
	tx, err := beginWorkspaceTx(ctx, pool, workspaceID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := scan(tx.QueryRow(ctx, query, args...)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func workspaceQueryRows(ctx context.Context, pool *pgxpool.Pool, workspaceID, query string, args []any, consume func(pgx.Rows) error) error {
	tx, err := beginWorkspaceTx(ctx, pool, workspaceID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, query, args...)
	if err != nil {
		return err
	}
	consumeErr := consume(rows)
	rows.Close()
	if consumeErr != nil {
		return consumeErr
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func workspaceExec(ctx context.Context, pool *pgxpool.Pool, workspaceID, query string, args ...any) (pgconn.CommandTag, error) {
	tx, err := beginWorkspaceTx(ctx, pool, workspaceID)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	tag, err := tx.Exec(ctx, query, args...)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return pgconn.CommandTag{}, err
	}
	return tag, nil
}
