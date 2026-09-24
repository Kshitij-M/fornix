package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
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
