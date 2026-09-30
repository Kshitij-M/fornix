package store

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/omaveda/fornix/internal/contracts"
)

// validateAgentRunEffectFenceTx validates the ownership tuple persisted on a
// model or tool effect while the caller holds the effect transaction. Empty
// values are the compatibility form for standalone calls; partial tuples are
// rejected so a stale or malformed worker cannot silently become unbound.
func validateAgentRunEffectFenceTx(ctx context.Context, tx pgx.Tx, workspaceID, runID, ownerID string, fence int64) error {
	workspaceID = strings.TrimSpace(workspaceID)
	runID = strings.TrimSpace(runID)
	ownerID = strings.TrimSpace(ownerID)
	if runID == "" && ownerID == "" && fence == 0 {
		return nil
	}
	if workspaceID == "" || runID == "" || ownerID == "" || fence <= 0 {
		return fmt.Errorf("%w: incomplete agent-run effect fence", ErrAgentRunLeaseFenced)
	}
	if uint64(fence) > maxAgentRunFence {
		return ErrAgentRunLeaseFenced
	}
	_, err := validateAgentRunLeaseTx(ctx, tx, contracts.AgentRunLease{
		WorkspaceID: workspaceID,
		RunID:       runID,
		OwnerID:     ownerID,
		Fence:       uint64(fence),
	})
	return err
}

func agentRunID(ref *contracts.EntityRef) string {
	if ref == nil {
		return ""
	}
	return strings.TrimSpace(ref.ID)
}
