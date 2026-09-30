package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/omaveda/fornix/internal/contracts"
)

// RetentionSweep expires only bounded operational federation rows. Peer
// commands, control events, recovery-required attempts, and active leases are
// authoritative or safety-critical and remain untouched. Every committed
// expiration is represented by an append-only tombstone before the source row
// is removed.
func (s *FederationStore) RetentionSweep(ctx context.Context, request contracts.FederationRetentionRequest) (contracts.FederationRetentionResult, error) {
	if s == nil || s.pool == nil || s.events == nil {
		return contracts.FederationRetentionResult{}, fmt.Errorf("federation store is not configured")
	}
	if err := request.Normalize(); err != nil {
		return contracts.FederationRetentionResult{}, err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.FederationRetentionResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result := contracts.FederationRetentionResult{WorkspaceID: request.WorkspaceID, DryRun: request.DryRun, BatchSize: request.BatchSize}
	// Scheduled owners must prove their exact workspace lease in this same
	// transaction as selection, tombstoning, deletion, and event append. This
	// closes the check-then-act window in which a stale owner could otherwise
	// continue a sweep after a takeover. Explicit operator sweeps may omit the
	// lease fields, but the retention owner never does.
	if request.OwnerID != "" {
		if _, err := s.events.ValidateConsumerLeaseTx(ctx, tx, contracts.ConsumerLease{
			WorkspaceID: request.WorkspaceID,
			ConsumerID:  contracts.FederationRetentionConsumerID,
			OwnerID:     request.OwnerID,
			Fence:       request.Fence,
		}); err != nil {
			return contracts.FederationRetentionResult{}, err
		}
	}
	lock := ""
	if !request.DryRun {
		lock = " FOR UPDATE SKIP LOCKED"
	}
	if err := tx.QueryRow(ctx, `
		SELECT count(*)
		FROM fornix.workspace_federation_poll_attempts p
		WHERE p.workspace_id=$1 AND p.retention_class='operational' AND p.retention_state='active'
		  AND p.retention_deadline IS NOT NULL AND p.retention_deadline <= $2
		  AND p.state='recovery_required'`, request.WorkspaceID, request.Before).Scan(&result.ProtectedRecovery); err != nil {
		return contracts.FederationRetentionResult{}, err
	}
	if err := tx.QueryRow(ctx, `
		SELECT count(*)
		FROM fornix.workspace_federation_poll_attempts p
		WHERE p.workspace_id=$1 AND p.retention_class='operational' AND p.retention_state='active'
		  AND p.retention_deadline IS NOT NULL AND p.retention_deadline <= $2
		  AND p.state IN ('succeeded','failed')
		  AND EXISTS (SELECT 1 FROM fornix.workspace_federation_peer_leases l
		              WHERE l.workspace_id=p.workspace_id AND l.peer_id=p.peer_id AND l.expires_at > $2)`, request.WorkspaceID, request.Before).Scan(&result.ProtectedLease); err != nil {
		return contracts.FederationRetentionResult{}, err
	}
	type pollCandidate struct{ id, hash, state string }
	pollRows, err := tx.Query(ctx, `
		SELECT p.id,p.request_hash,p.state
		FROM fornix.workspace_federation_poll_attempts p
		WHERE p.workspace_id=$1 AND p.retention_class='operational' AND p.retention_state='active'
		  AND p.retention_deadline IS NOT NULL AND p.retention_deadline <= $2
		  AND p.state IN ('succeeded','failed')
		  AND NOT EXISTS (SELECT 1 FROM fornix.workspace_federation_peer_leases l
		                  WHERE l.workspace_id=p.workspace_id AND l.peer_id=p.peer_id AND l.expires_at > $2)
		ORDER BY p.retention_deadline,p.id LIMIT $3`+lock, request.WorkspaceID, request.Before, request.BatchSize)
	if err != nil {
		return contracts.FederationRetentionResult{}, err
	}
	polls := make([]pollCandidate, 0, request.BatchSize)
	for pollRows.Next() {
		var candidate pollCandidate
		if err := pollRows.Scan(&candidate.id, &candidate.hash, &candidate.state); err != nil {
			pollRows.Close()
			return contracts.FederationRetentionResult{}, err
		}
		polls = append(polls, candidate)
	}
	if err := pollRows.Err(); err != nil {
		pollRows.Close()
		return contracts.FederationRetentionResult{}, err
	}
	pollRows.Close()
	result.PollCandidates = len(polls)
	if !request.DryRun {
		for _, candidate := range polls {
			tombstoneHash := contracts.HashStrings("federation.poll", request.WorkspaceID, candidate.id, candidate.hash, candidate.state)
			if err := insertFederationTombstone(ctx, tx, request, "workspace_federation_poll_attempts", candidate.id, tombstoneHash, "operational retention deadline"); err != nil {
				return contracts.FederationRetentionResult{}, err
			}
			if _, err := tx.Exec(ctx, `DELETE FROM fornix.workspace_federation_poll_attempts WHERE workspace_id=$1 AND id=$2 AND retention_state='active'`, request.WorkspaceID, candidate.id); err != nil {
				return contracts.FederationRetentionResult{}, err
			}
			result.PollExpired++
			result.TombstoneHashes = append(result.TombstoneHashes, tombstoneHash)
		}
	}
	type quarantineCandidate struct{ id, hash string }
	quarantineRows, err := tx.Query(ctx, `
		SELECT id,row_hash
		FROM fornix.federation_legacy_quarantine
		WHERE audit_workspace_id=$1 AND retention_class='operational' AND retention_state='active'
		  AND retention_deadline IS NOT NULL AND retention_deadline <= $2
		ORDER BY retention_deadline,id LIMIT $3`+lock, request.WorkspaceID, request.Before, request.BatchSize)
	if err != nil {
		return contracts.FederationRetentionResult{}, err
	}
	quarantine := make([]quarantineCandidate, 0, request.BatchSize)
	for quarantineRows.Next() {
		var candidate quarantineCandidate
		if err := quarantineRows.Scan(&candidate.id, &candidate.hash); err != nil {
			quarantineRows.Close()
			return contracts.FederationRetentionResult{}, err
		}
		quarantine = append(quarantine, candidate)
	}
	if err := quarantineRows.Err(); err != nil {
		quarantineRows.Close()
		return contracts.FederationRetentionResult{}, err
	}
	quarantineRows.Close()
	result.QuarantineCandidates = len(quarantine)
	if !request.DryRun {
		for _, candidate := range quarantine {
			tombstoneHash := contracts.HashStrings("federation.quarantine", request.WorkspaceID, candidate.id, candidate.hash)
			if err := insertFederationTombstone(ctx, tx, request, "federation_legacy_quarantine", candidate.id, tombstoneHash, "quarantine retention deadline"); err != nil {
				return contracts.FederationRetentionResult{}, err
			}
			if _, err := tx.Exec(ctx, `DELETE FROM fornix.federation_legacy_quarantine WHERE audit_workspace_id=$1 AND id=$2 AND retention_state='active'`, request.WorkspaceID, candidate.id); err != nil {
				return contracts.FederationRetentionResult{}, err
			}
			result.QuarantineExpired++
			result.TombstoneHashes = append(result.TombstoneHashes, tombstoneHash)
		}
	}
	if request.DryRun {
		if err := tx.Rollback(ctx); err != nil {
			return contracts.FederationRetentionResult{}, err
		}
		return result, nil
	}
	payload, _ := json.Marshal(map[string]any{
		"workspace_id": request.WorkspaceID, "poll_expired": result.PollExpired,
		"quarantine_expired": result.QuarantineExpired,
		"tombstone_hashes":   result.TombstoneHashes,
	})
	event := contracts.EventEnvelope{
		EventID: contracts.NewID("evt-federation-retention"), EventType: "federation.retention_expired",
		SchemaVersion: contracts.EventSchemaVersion, OccurredAt: request.Before,
		Scope: contracts.Scope{WorkspaceID: request.WorkspaceID}, Actor: request.Actor,
		CausationID: request.CausationID, CorrelationID: request.CorrelationID,
		IdempotencyKey: "federation-retention:" + request.WorkspaceID + ":" + request.IdempotencyKey, Payload: payload,
	}
	if _, err := s.events.AppendTx(ctx, tx, event); err != nil {
		return contracts.FederationRetentionResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.FederationRetentionResult{}, err
	}
	return result, nil
}

func insertFederationTombstone(ctx context.Context, tx pgx.Tx, request contracts.FederationRetentionRequest, sourceTable, sourceID, sourceHash, reason string) error {
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.federation_retention_tombstones(id,workspace_id,source_table,source_id,source_hash,reason,expired_at,actor)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb)
		ON CONFLICT DO NOTHING`,
		"retention-"+contracts.HashStrings(request.WorkspaceID, sourceTable, sourceID)[:48], request.WorkspaceID, sourceTable, sourceID, sourceHash, reason, request.Before, mustJSON(request.Actor)); err != nil {
		return err
	}
	return nil
}

// RetentionCutoff returns the default operational retention deadline for new
// federation records. Deployments may update deadlines explicitly through a
// policy migration or a future policy service; callers cannot extend an
// authoritative record through this helper.
func RetentionCutoff(now time.Time) time.Time {
	return now.UTC().Add(30 * 24 * time.Hour)
}
