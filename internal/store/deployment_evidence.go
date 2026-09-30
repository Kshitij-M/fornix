package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrDeploymentReleaseNotFound     = errors.New("deployment release not found")
	ErrDeploymentReleaseConflict     = errors.New("deployment release conflicts with existing state")
	ErrDeploymentEvidenceNotFound    = errors.New("deployment evidence link not found")
	ErrDeploymentEvidenceConflict    = errors.New("deployment evidence link conflicts with existing state")
	ErrDeploymentEvidenceStale       = errors.New("deployment evidence is stale for the release")
	ErrDeploymentEvidenceKind        = errors.New("deployment evidence kind is not admissible")
	ErrDeploymentBoundaryEvidence    = errors.New("external-effect evidence lacks one exact measured boundary observation")
	ErrDeploymentEvidenceLifecycle   = errors.New("deployment evidence lifecycle transition is invalid")
	ErrDeploymentQualificationCursor = errors.New("invalid deployment qualification cursor")
)

// DeploymentEvidenceStore indexes imported signed qualification evidence by
// release. It never owns or duplicates the imported raw bytes; the Task 79
// qualification import remains the evidence authority.
type DeploymentEvidenceStore struct {
	pool *pgxpool.Pool
}

func NewDeploymentEvidenceStore(pool *pgxpool.Pool) *DeploymentEvidenceStore {
	return &DeploymentEvidenceStore{pool: pool}
}

func (s *DeploymentEvidenceStore) RegisterRelease(ctx context.Context, request contracts.DeploymentReleaseRequest, now time.Time) (contracts.DeploymentRelease, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.DeploymentRelease{}, false, fmt.Errorf("deployment evidence store is not configured")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	if err := request.Normalize(); err != nil {
		return contracts.DeploymentRelease{}, false, err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.DeploymentRelease{}, false, fmt.Errorf("begin deployment release registration: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-deployment-release:' || $1 || ':' || $2, 0))`, request.WorkspaceID, request.DeploymentID); err != nil {
		return contracts.DeploymentRelease{}, false, err
	}
	if existing, existingErr := queryDeploymentReleaseByHash(ctx, tx, request.WorkspaceID, request.DeploymentID, request.ReleaseHash, true); existingErr == nil {
		if !sameDeploymentReleaseRequest(existing, request) {
			return contracts.DeploymentRelease{}, false, ErrDeploymentReleaseConflict
		}
		if request.DryRun {
			_ = tx.Rollback(ctx)
			return existing, false, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.DeploymentRelease{}, false, err
		}
		return existing, false, nil
	} else if !errors.Is(existingErr, ErrDeploymentReleaseNotFound) {
		return contracts.DeploymentRelease{}, false, existingErr
	}
	if existing, existingErr := queryDeploymentReleaseByIdempotency(ctx, tx, request.WorkspaceID, request.DeploymentID, request.IdempotencyKey, true); existingErr == nil {
		if !sameDeploymentReleaseRequest(existing, request) {
			return contracts.DeploymentRelease{}, false, ErrDeploymentReleaseConflict
		}
		if request.DryRun {
			_ = tx.Rollback(ctx)
			return existing, false, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.DeploymentRelease{}, false, err
		}
		return existing, false, nil
	} else if !errors.Is(existingErr, ErrDeploymentReleaseNotFound) {
		return contracts.DeploymentRelease{}, false, existingErr
	}
	snapshot, err := queryCurrentQualificationSnapshotTx(ctx, tx, request.WorkspaceID, request.DeploymentID, now)
	if err != nil {
		return contracts.DeploymentRelease{}, false, err
	}
	release := contracts.DeploymentRelease{
		SchemaVersion: contracts.DeploymentEvidenceSchemaVersion, ID: contracts.NewID("deployment-release"),
		WorkspaceID: request.WorkspaceID, DeploymentID: request.DeploymentID,
		ReleaseHash: request.ReleaseHash, TargetHash: request.TargetHash, Version: request.Version,
		CommitHash: request.CommitHash, TrustSnapshotRevision: snapshot.Snapshot.Revision,
		TrustSnapshotHash: snapshot.Snapshot.SnapshotHash, Status: contracts.DeploymentReleaseActive,
		Actor: request.Actor, CreatedAt: now,
	}
	if err := release.Normalize(); err != nil {
		return contracts.DeploymentRelease{}, false, err
	}
	if request.DryRun {
		_ = tx.Rollback(ctx)
		release.ID = "dry-run-" + release.ReleaseHash[:16]
		return release, false, nil
	}
	actorJSON, err := json.Marshal(request.Actor)
	if err != nil {
		return contracts.DeploymentRelease{}, false, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.qualification_deployment_releases
		  (id,workspace_id,deployment_id,release_hash,target_hash,version,commit_hash,trust_snapshot_revision,trust_snapshot_hash,status,actor,request_id,idempotency_key,causation_id,correlation_id)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'active',$10::jsonb,$11,$12,$13,$14)`,
		release.ID, release.WorkspaceID, release.DeploymentID, release.ReleaseHash, release.TargetHash, release.Version, nullableString(release.CommitHash), release.TrustSnapshotRevision, release.TrustSnapshotHash, actorJSON, nullableString(request.RequestID), request.IdempotencyKey, nullableString(request.CausationID), nullableString(request.CorrelationID)); err != nil {
		if isUniqueViolation(err) {
			return contracts.DeploymentRelease{}, false, ErrDeploymentReleaseConflict
		}
		return contracts.DeploymentRelease{}, false, fmt.Errorf("insert deployment release: %w", err)
	}
	if err := appendDeploymentReleaseEvent(ctx, tx, release, "registered", actorJSON, map[string]string{"release_hash": release.ReleaseHash, "trust_snapshot_hash": release.TrustSnapshotHash}); err != nil {
		return contracts.DeploymentRelease{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.DeploymentRelease{}, false, fmt.Errorf("commit deployment release: %w", err)
	}
	return release, true, nil
}

func (s *DeploymentEvidenceStore) ListReleases(ctx context.Context, workspaceID, deploymentID string, limit int, cursor string) (contracts.DeploymentReleasePage, error) {
	if s == nil || s.pool == nil {
		return contracts.DeploymentReleasePage{}, fmt.Errorf("deployment evidence store is not configured")
	}
	workspaceID, deploymentID, cursor = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(cursor)
	if workspaceID == "" || deploymentID == "" {
		return contracts.DeploymentReleasePage{}, ErrDeploymentReleaseNotFound
	}
	limit = boundedQualificationPageLimit(limit)
	if cursor != "" && !validQualificationCursor(cursor) {
		return contracts.DeploymentReleasePage{}, ErrDeploymentQualificationCursor
	}
	page := contracts.DeploymentReleasePage{Items: make([]contracts.DeploymentRelease, 0, limit)}
	err := workspaceQueryRows(ctx, s.pool, workspaceID, deploymentReleaseSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND id>$3 ORDER BY id LIMIT $4`, []any{workspaceID, deploymentID, cursor, limit + 1}, func(rows pgx.Rows) error {
		for rows.Next() {
			release, err := scanDeploymentRelease(rows)
			if err != nil {
				return err
			}
			page.Items = append(page.Items, release)
		}
		return rows.Err()
	})
	if err != nil {
		return contracts.DeploymentReleasePage{}, err
	}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, nil
}

// GetRelease returns one immutable release identity within the caller's
// workspace. The read is performed under the same workspace transaction
// policy as all other deployment-evidence reads.
func (s *DeploymentEvidenceStore) GetRelease(ctx context.Context, workspaceID, deploymentID, releaseID string) (contracts.DeploymentRelease, error) {
	if s == nil || s.pool == nil {
		return contracts.DeploymentRelease{}, fmt.Errorf("deployment evidence store is not configured")
	}
	workspaceID, deploymentID, releaseID = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(releaseID)
	if workspaceID == "" || deploymentID == "" || releaseID == "" {
		return contracts.DeploymentRelease{}, ErrDeploymentReleaseNotFound
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.DeploymentRelease{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	release, err := queryDeploymentRelease(ctx, tx, workspaceID, deploymentID, releaseID, false)
	if err != nil {
		return contracts.DeploymentRelease{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.DeploymentRelease{}, err
	}
	return release, nil
}

func (s *DeploymentEvidenceStore) LinkEvidence(ctx context.Context, request contracts.DeploymentEvidenceLinkRequest, now time.Time) (contracts.DeploymentEvidenceLink, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.DeploymentEvidenceLink{}, false, fmt.Errorf("deployment evidence store is not configured")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	if err := request.Normalize(); err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-deployment-evidence:' || $1 || ':' || $2, 0))`, request.WorkspaceID, request.DeploymentID); err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	link, created, err := s.linkEvidenceTx(ctx, tx, request, now.UTC())
	if err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	if request.DryRun {
		_ = tx.Rollback(ctx)
		return link, created, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	return link, created, nil
}

// linkEvidenceTx performs the existing link/replacement operation inside a
// caller-owned transaction. RefreshQualificationEvidence uses this helper so
// multiple replacements and their refresh report commit atomically.
func (s *DeploymentEvidenceStore) linkEvidenceTx(ctx context.Context, tx pgx.Tx, request contracts.DeploymentEvidenceLinkRequest, now time.Time) (contracts.DeploymentEvidenceLink, bool, error) {
	if existing, existingErr := queryDeploymentEvidenceByIdempotency(ctx, tx, request.WorkspaceID, request.DeploymentID, request.IdempotencyKey, true); existingErr == nil {
		if !sameDeploymentEvidenceRequest(existing, request) {
			return contracts.DeploymentEvidenceLink{}, false, ErrDeploymentEvidenceConflict
		}
		return existing, false, nil
	} else if !errors.Is(existingErr, ErrDeploymentEvidenceNotFound) {
		return contracts.DeploymentEvidenceLink{}, false, existingErr
	}
	var predecessor contracts.DeploymentEvidenceLink
	if existing, existingErr := queryDeploymentEvidenceByKind(ctx, tx, request.WorkspaceID, request.DeploymentID, request.ReleaseID, request.Kind, true); existingErr == nil {
		if !sameDeploymentEvidenceRequest(existing, request) {
			return contracts.DeploymentEvidenceLink{}, false, ErrDeploymentEvidenceConflict
		}
		if request.SupersedesLinkID == "" || request.SupersedesLinkID != existing.ID {
			return contracts.DeploymentEvidenceLink{}, false, ErrDeploymentEvidenceConflict
		}
		predecessor = existing
	} else if !errors.Is(existingErr, ErrDeploymentEvidenceNotFound) {
		return contracts.DeploymentEvidenceLink{}, false, existingErr
	} else if request.SupersedesLinkID != "" {
		return contracts.DeploymentEvidenceLink{}, false, ErrDeploymentEvidenceConflict
	}
	release, err := queryDeploymentRelease(ctx, tx, request.WorkspaceID, request.DeploymentID, request.ReleaseID, true)
	if err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	importRecord, signed, err := queryQualificationImportEvidence(ctx, tx, request.WorkspaceID, request.DeploymentID, request.ImportID, true)
	if err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	if importRecord.TrustSnapshotRevision != release.TrustSnapshotRevision || importRecord.TrustSnapshotHash != release.TrustSnapshotHash || importRecord.TargetHash != release.TargetHash || signed.Bundle.Report.TargetHash != release.TargetHash {
		return contracts.DeploymentEvidenceLink{}, false, ErrDeploymentEvidenceStale
	}
	externalBoundaryHash, boundaryEvidenceHash, boundaryEvidenceExpiresAt, err := qualificationBoundaryHashes(signed, request.Kind, now)
	if err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	link := contracts.DeploymentEvidenceLink{
		SchemaVersion: contracts.DeploymentEvidenceSchemaVersion, ID: contracts.NewID("deployment-evidence"),
		WorkspaceID: request.WorkspaceID, DeploymentID: request.DeploymentID, ReleaseID: release.ID,
		Kind: request.Kind, ImportID: request.ImportID, TargetHash: importRecord.TargetHash,
		SignedHash: importRecord.SignedHash, ObservationHash: importRecord.ObservationHash,
		ReportHash: signed.Bundle.Report.ReportHash, ManifestHash: signed.Bundle.Manifest.ManifestHash,
		SourceHash: importRecord.SourceHash, TrustSnapshotRevision: importRecord.TrustSnapshotRevision,
		TrustSnapshotHash: importRecord.TrustSnapshotHash, Outcome: signed.Bundle.Report.Outcome,
		ExternalBoundaryHash: externalBoundaryHash, BoundaryEvidenceHash: boundaryEvidenceHash, BoundaryEvidenceExpiresAt: boundaryEvidenceExpiresAt,
		RecoveryState: deploymentEvidenceRecoveryState(signed, request.Kind), Status: contracts.DeploymentEvidenceLinkActive,
		Actor: request.Actor, CreatedAt: now,
	}
	if predecessor.ID != "" {
		link.SupersedesLinkID = predecessor.ID
	}
	if err := link.Normalize(); err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	if request.DryRun {
		link.ID = "dry-run-" + link.SignedHash[:16]
		return link, false, nil
	}
	actorJSON, err := json.Marshal(request.Actor)
	if err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	if predecessor.ID != "" {
		if _, err := tx.Exec(ctx, `UPDATE fornix.qualification_deployment_evidence_links SET status='superseded', superseded_by_link_id=$1, actor=$2::jsonb WHERE workspace_id=$3 AND id=$4 AND status='active'`, link.ID, actorJSON, link.WorkspaceID, predecessor.ID); err != nil {
			return contracts.DeploymentEvidenceLink{}, false, fmt.Errorf("supersede deployment evidence link: %w", err)
		}
		predecessor.Status = contracts.DeploymentEvidenceLinkSuperseded
		predecessor.SupersededByLinkID = link.ID
		predecessor.Actor = request.Actor
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.qualification_deployment_evidence_links
		  (id,workspace_id,deployment_id,release_id,kind,import_id,target_hash,signed_hash,observation_hash,report_hash,manifest_hash,source_hash,external_boundary_hash,boundary_evidence_hash,boundary_evidence_expires_at,trust_snapshot_revision,trust_snapshot_hash,outcome,recovery_state,status,supersedes_link_id,superseded_by_link_id,revoked_at,revocation_reason,actor,request_id,idempotency_key,causation_id,correlation_id)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25::jsonb,$26,$27,$28,$29)`,
		link.ID, link.WorkspaceID, link.DeploymentID, link.ReleaseID, link.Kind, link.ImportID, link.TargetHash, link.SignedHash, link.ObservationHash, link.ReportHash, link.ManifestHash, link.SourceHash, link.ExternalBoundaryHash, link.BoundaryEvidenceHash, link.BoundaryEvidenceExpiresAt, link.TrustSnapshotRevision, link.TrustSnapshotHash, link.Outcome, link.RecoveryState, link.Status, nullableString(link.SupersedesLinkID), nil, nil, nil, actorJSON, nullableString(request.RequestID), request.IdempotencyKey, nullableString(request.CausationID), nullableString(request.CorrelationID)); err != nil {
		if isUniqueViolation(err) {
			return contracts.DeploymentEvidenceLink{}, false, ErrDeploymentEvidenceConflict
		}
		return contracts.DeploymentEvidenceLink{}, false, fmt.Errorf("insert deployment evidence link: %w", err)
	}
	if predecessor.ID != "" {
		if err := appendDeploymentEvidenceEvent(ctx, tx, predecessor, "superseded", actorJSON, request.RequestID, request.IdempotencyKey, request.CausationID, request.CorrelationID); err != nil {
			return contracts.DeploymentEvidenceLink{}, false, err
		}
	}
	if err := appendDeploymentEvidenceEvent(ctx, tx, link, "linked", actorJSON, request.RequestID, request.IdempotencyKey, request.CausationID, request.CorrelationID); err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	return link, true, nil
}

// RevokeEvidence moves one active evidence link to a durable revoked state.
// The imported signed bundle remains immutable and the lifecycle event is
// written in the same transaction as the current-state projection.
func (s *DeploymentEvidenceStore) RevokeEvidence(ctx context.Context, request contracts.DeploymentEvidenceRevocationRequest, now time.Time) (contracts.DeploymentEvidenceLink, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.DeploymentEvidenceLink{}, false, fmt.Errorf("deployment evidence store is not configured")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	if err := request.Normalize(); err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-deployment-evidence:' || $1 || ':' || $2, 0))`, request.WorkspaceID, request.DeploymentID); err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	var priorEventID, priorEvent string
	priorEventErr := tx.QueryRow(ctx, `SELECT evidence_id,event FROM fornix.qualification_deployment_evidence_events WHERE workspace_id=$1 AND deployment_id=$2 AND idempotency_key=$3 ORDER BY id LIMIT 1`, request.WorkspaceID, request.DeploymentID, request.IdempotencyKey).Scan(&priorEventID, &priorEvent)
	if priorEventErr == nil && (priorEventID != request.LinkID || priorEvent != "revoked") {
		return contracts.DeploymentEvidenceLink{}, false, ErrDeploymentEvidenceLifecycle
	}
	if priorEventErr != nil && !errors.Is(priorEventErr, pgx.ErrNoRows) {
		return contracts.DeploymentEvidenceLink{}, false, priorEventErr
	}
	link, err := queryDeploymentEvidenceByID(ctx, tx, request.WorkspaceID, request.DeploymentID, request.LinkID, true)
	if err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	if link.ReleaseID != request.ReleaseID || link.Kind != request.Kind {
		return contracts.DeploymentEvidenceLink{}, false, ErrDeploymentEvidenceLifecycle
	}
	if link.Status == contracts.DeploymentEvidenceLinkRevoked {
		if link.RevocationReason != request.Reason {
			return contracts.DeploymentEvidenceLink{}, false, ErrDeploymentEvidenceLifecycle
		}
		if request.DryRun {
			_ = tx.Rollback(ctx)
			return link, false, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.DeploymentEvidenceLink{}, false, err
		}
		return link, false, nil
	}
	if link.Status != contracts.DeploymentEvidenceLinkActive {
		return contracts.DeploymentEvidenceLink{}, false, ErrDeploymentEvidenceLifecycle
	}
	if request.DryRun {
		_ = tx.Rollback(ctx)
		link.Status = contracts.DeploymentEvidenceLinkRevoked
		link.RevokedAt = &now
		link.RevocationReason = request.Reason
		link.Actor = request.Actor
		return link, false, nil
	}
	actorJSON, err := json.Marshal(request.Actor)
	if err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	result, err := tx.Exec(ctx, `UPDATE fornix.qualification_deployment_evidence_links SET status='revoked', revoked_at=$1, revocation_reason=$2, actor=$3::jsonb WHERE workspace_id=$4 AND id=$5 AND status='active'`, now, request.Reason, actorJSON, request.WorkspaceID, request.LinkID)
	if err != nil {
		return contracts.DeploymentEvidenceLink{}, false, fmt.Errorf("revoke deployment evidence link: %w", err)
	}
	if result.RowsAffected() != 1 {
		return contracts.DeploymentEvidenceLink{}, false, ErrDeploymentEvidenceLifecycle
	}
	link.Status = contracts.DeploymentEvidenceLinkRevoked
	link.RevokedAt = &now
	link.RevocationReason = request.Reason
	link.Actor = request.Actor
	if err := appendDeploymentEvidenceEvent(ctx, tx, link, "revoked", actorJSON, request.RequestID, request.IdempotencyKey, request.CausationID, request.CorrelationID); err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.DeploymentEvidenceLink{}, false, err
	}
	return link, true, nil
}

func (s *DeploymentEvidenceStore) ListEvidence(ctx context.Context, workspaceID, deploymentID, releaseID string, limit int, cursor string) (contracts.DeploymentEvidencePage, error) {
	if s == nil || s.pool == nil {
		return contracts.DeploymentEvidencePage{}, fmt.Errorf("deployment evidence store is not configured")
	}
	workspaceID, deploymentID, releaseID, cursor = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(releaseID), strings.TrimSpace(cursor)
	if workspaceID == "" || deploymentID == "" || releaseID == "" {
		return contracts.DeploymentEvidencePage{}, ErrDeploymentEvidenceNotFound
	}
	limit = boundedQualificationPageLimit(limit)
	if cursor != "" && !validQualificationCursor(cursor) {
		return contracts.DeploymentEvidencePage{}, ErrDeploymentQualificationCursor
	}
	page := contracts.DeploymentEvidencePage{Items: make([]contracts.DeploymentEvidenceLink, 0, limit)}
	err := workspaceQueryRows(ctx, s.pool, workspaceID, deploymentEvidenceSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND release_id=$3 AND id>$4 ORDER BY id LIMIT $5`, []any{workspaceID, deploymentID, releaseID, cursor, limit + 1}, func(rows pgx.Rows) error {
		for rows.Next() {
			link, err := scanDeploymentEvidence(rows)
			if err != nil {
				return err
			}
			page.Items = append(page.Items, link)
		}
		return rows.Err()
	})
	if err != nil {
		return contracts.DeploymentEvidencePage{}, err
	}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, nil
}

// EvaluateGate is a read-only projection over release/evidence identities.
// It never replays or executes the imported deployment operation.
func (s *DeploymentEvidenceStore) EvaluateGate(ctx context.Context, workspaceID, deploymentID, releaseID string, requiredKinds []string, now time.Time) (contracts.DeploymentQualificationGate, error) {
	if s == nil || s.pool == nil {
		return contracts.DeploymentQualificationGate{}, fmt.Errorf("deployment evidence store is not configured")
	}
	workspaceID, deploymentID, releaseID = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(releaseID)
	if len(requiredKinds) == 0 {
		requiredKinds = append([]string(nil), contracts.DeploymentDefaultRequiredEvidenceKinds...)
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.DeploymentQualificationGate{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	release, err := queryDeploymentRelease(ctx, tx, workspaceID, deploymentID, releaseID, false)
	if err != nil {
		return contracts.DeploymentQualificationGate{}, err
	}
	gate, err := evaluateDeploymentGateTx(ctx, tx, release, requiredKinds, now.UTC())
	if err != nil {
		return contracts.DeploymentQualificationGate{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.DeploymentQualificationGate{}, err
	}
	return gate, nil
}

func evaluateDeploymentGateTx(ctx context.Context, tx pgx.Tx, release contracts.DeploymentRelease, requiredKinds []string, now time.Time) (contracts.DeploymentQualificationGate, error) {
	// Internal callers such as release-verification registration intentionally
	// pass nil to use the production default gate. Keep the default inside the
	// transaction helper so every caller evaluates the same authority snapshot
	// and required evidence set.
	if len(requiredKinds) == 0 {
		requiredKinds = append([]string(nil), contracts.DeploymentDefaultRequiredEvidenceKinds...)
	}
	gate := contracts.DeploymentQualificationGate{
		SchemaVersion: contracts.DeploymentEvidenceSchemaVersion, WorkspaceID: release.WorkspaceID,
		DeploymentID: release.DeploymentID, ReleaseID: release.ID, ReleaseHash: release.ReleaseHash,
		TrustSnapshotRevision: release.TrustSnapshotRevision, TrustSnapshotHash: release.TrustSnapshotHash,
		RequiredKinds: append([]string(nil), requiredKinds...), SnapshotCurrent: false,
	}
	current, currentErr := queryCurrentQualificationSnapshotTx(ctx, tx, release.WorkspaceID, release.DeploymentID, now.UTC())
	if currentErr == nil && current.Snapshot.Revision == release.TrustSnapshotRevision && current.Snapshot.SnapshotHash == release.TrustSnapshotHash {
		gate.SnapshotCurrent = true
	} else {
		gate.BlockedReasons = append(gate.BlockedReasons, "trust_snapshot_not_current")
	}
	rows, err := tx.Query(ctx, deploymentEvidenceSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND release_id=$3 ORDER BY kind`, release.WorkspaceID, release.DeploymentID, release.ID)
	if err != nil {
		return contracts.DeploymentQualificationGate{}, err
	}
	for rows.Next() {
		link, scanErr := scanDeploymentEvidence(rows)
		if scanErr != nil {
			rows.Close()
			return contracts.DeploymentQualificationGate{}, scanErr
		}
		gate.Links = append(gate.Links, link)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return contracts.DeploymentQualificationGate{}, err
	}
	rows.Close()
	if err := gate.Normalize(); err != nil {
		return contracts.DeploymentQualificationGate{}, err
	}
	byKind := make(map[string]contracts.DeploymentEvidenceLink, len(gate.Links))
	inactiveByKind := make(map[string]string, len(gate.Links))
	for _, link := range gate.Links {
		if link.Status != contracts.DeploymentEvidenceLinkActive {
			if _, exists := inactiveByKind[link.Kind]; !exists {
				inactiveByKind[link.Kind] = link.Status
			}
			continue
		}
		byKind[link.Kind] = link
		if link.Kind == contracts.DeploymentEvidenceExternalEffect {
			gate.ExternalBoundaryHash = link.ExternalBoundaryHash
			gate.BoundaryEvidenceHash = link.BoundaryEvidenceHash
			gate.BoundaryEvidenceExpiresAt = link.BoundaryEvidenceExpiresAt
			if link.ExternalBoundaryHash == "" || link.BoundaryEvidenceHash == "" || link.BoundaryEvidenceExpiresAt == nil {
				gate.BlockedReasons = append(gate.BlockedReasons, "external_boundary_evidence_missing")
			} else if !now.UTC().Before(link.BoundaryEvidenceExpiresAt.UTC()) {
				gate.BlockedReasons = append(gate.BlockedReasons, "external_boundary_evidence_expired")
			}
		}
		if link.TrustSnapshotHash != release.TrustSnapshotHash || link.TrustSnapshotRevision != release.TrustSnapshotRevision {
			gate.BlockedReasons = append(gate.BlockedReasons, "evidence_snapshot_mismatch:"+link.Kind)
		}
		if link.Outcome != contracts.QualificationOutcomePassed {
			gate.BlockedReasons = append(gate.BlockedReasons, "evidence_not_passed:"+link.Kind)
		}
		if link.RecoveryState == contracts.DeploymentEvidenceRecoveryRequired || link.RecoveryState == contracts.DeploymentEvidenceUnknown {
			gate.BlockedReasons = append(gate.BlockedReasons, "evidence_unresolved:"+link.Kind)
		}
	}
	for _, kind := range gate.RequiredKinds {
		if _, ok := byKind[kind]; !ok {
			if status, inactive := inactiveByKind[kind]; inactive {
				gate.BlockedReasons = append(gate.BlockedReasons, "evidence_"+status+":"+kind)
			} else {
				gate.MissingKinds = append(gate.MissingKinds, kind)
			}
		}
	}
	sortStrings(gate.BlockedReasons)
	sortStrings(gate.MissingKinds)
	gate.Ready = gate.SnapshotCurrent && len(gate.MissingKinds) == 0 && len(gate.BlockedReasons) == 0
	gate.GateHash = gate.StableHash()
	return gate, nil
}

func queryDeploymentRelease(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, releaseID string, lock bool) (contracts.DeploymentRelease, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	release, err := scanDeploymentRelease(tx.QueryRow(ctx, deploymentReleaseSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND id=$3`+lockClause, workspaceID, deploymentID, releaseID))
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.DeploymentRelease{}, ErrDeploymentReleaseNotFound
	}
	return release, err
}

func queryDeploymentReleaseByHash(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, releaseHash string, lock bool) (contracts.DeploymentRelease, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	release, err := scanDeploymentRelease(tx.QueryRow(ctx, deploymentReleaseSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND release_hash=$3`+lockClause, workspaceID, deploymentID, releaseHash))
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.DeploymentRelease{}, ErrDeploymentReleaseNotFound
	}
	return release, err
}

func queryDeploymentReleaseByIdempotency(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, key string, lock bool) (contracts.DeploymentRelease, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	release, err := scanDeploymentRelease(tx.QueryRow(ctx, deploymentReleaseSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND idempotency_key=$3`+lockClause, workspaceID, deploymentID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.DeploymentRelease{}, ErrDeploymentReleaseNotFound
	}
	return release, err
}

func queryDeploymentEvidenceByIdempotency(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, key string, lock bool) (contracts.DeploymentEvidenceLink, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	link, err := scanDeploymentEvidence(tx.QueryRow(ctx, deploymentEvidenceSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND idempotency_key=$3`+lockClause, workspaceID, deploymentID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.DeploymentEvidenceLink{}, ErrDeploymentEvidenceNotFound
	}
	return link, err
}

func queryDeploymentEvidenceByKind(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, releaseID, kind string, lock bool) (contracts.DeploymentEvidenceLink, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	link, err := scanDeploymentEvidence(tx.QueryRow(ctx, deploymentEvidenceSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND release_id=$3 AND kind=$4 AND status='active'`+lockClause, workspaceID, deploymentID, releaseID, kind))
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.DeploymentEvidenceLink{}, ErrDeploymentEvidenceNotFound
	}
	return link, err
}

func queryDeploymentEvidenceByID(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, linkID string, lock bool) (contracts.DeploymentEvidenceLink, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	link, err := scanDeploymentEvidence(tx.QueryRow(ctx, deploymentEvidenceSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND id=$3`+lockClause, workspaceID, deploymentID, linkID))
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.DeploymentEvidenceLink{}, ErrDeploymentEvidenceNotFound
	}
	return link, err
}

func queryQualificationImportEvidence(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, importID string, lock bool) (contracts.QualificationImportRecord, contracts.SignedQualificationBundle, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	row := tx.QueryRow(ctx, `SELECT id,workspace_id,deployment_id,target_hash,key_id,signer_record_id,signed_hash,observation_hash,source_hash,COALESCE(source_reference,''),COALESCE(request_id,''),idempotency_key,COALESCE(causation_id,''),COALESCE(correlation_id,''),COALESCE(trust_snapshot_revision,0),COALESCE(trust_snapshot_hash,''),status,actor,created_at,signed_bytes FROM fornix.qualification_imports WHERE workspace_id=$1 AND deployment_id=$2 AND id=$3`+lockClause, workspaceID, deploymentID, importID)
	record, raw, err := scanQualificationImportWithBytes(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.QualificationImportRecord{}, contracts.SignedQualificationBundle{}, ErrDeploymentEvidenceNotFound
	}
	if err != nil {
		return contracts.QualificationImportRecord{}, contracts.SignedQualificationBundle{}, err
	}
	signed, err := decodeQualificationSignedBytes(raw)
	if err != nil {
		return contracts.QualificationImportRecord{}, contracts.SignedQualificationBundle{}, err
	}
	return record, signed, nil
}

func deploymentReleaseSelect() string {
	return `SELECT id,workspace_id,deployment_id,release_hash,target_hash,version,COALESCE(commit_hash,''),trust_snapshot_revision,trust_snapshot_hash,status,actor,created_at FROM fornix.qualification_deployment_releases`
}

func deploymentEvidenceSelect() string {
	return `SELECT id,workspace_id,deployment_id,release_id,kind,import_id,target_hash,signed_hash,observation_hash,report_hash,manifest_hash,source_hash,external_boundary_hash,boundary_evidence_hash,boundary_evidence_expires_at,trust_snapshot_revision,trust_snapshot_hash,outcome,recovery_state,status,COALESCE(supersedes_link_id,''),COALESCE(superseded_by_link_id,''),revoked_at,COALESCE(revocation_reason,''),actor,created_at FROM fornix.qualification_deployment_evidence_links`
}

func scanDeploymentRelease(row pgx.Row) (contracts.DeploymentRelease, error) {
	var release contracts.DeploymentRelease
	var actorRaw []byte
	if err := row.Scan(&release.ID, &release.WorkspaceID, &release.DeploymentID, &release.ReleaseHash, &release.TargetHash, &release.Version, &release.CommitHash, &release.TrustSnapshotRevision, &release.TrustSnapshotHash, &release.Status, &actorRaw, &release.CreatedAt); err != nil {
		return contracts.DeploymentRelease{}, err
	}
	if err := json.Unmarshal(actorRaw, &release.Actor); err != nil {
		return contracts.DeploymentRelease{}, err
	}
	release.SchemaVersion = contracts.DeploymentEvidenceSchemaVersion
	return release, release.Normalize()
}

func scanDeploymentEvidence(row pgx.Row) (contracts.DeploymentEvidenceLink, error) {
	var link contracts.DeploymentEvidenceLink
	var actorRaw []byte
	if err := row.Scan(&link.ID, &link.WorkspaceID, &link.DeploymentID, &link.ReleaseID, &link.Kind, &link.ImportID, &link.TargetHash, &link.SignedHash, &link.ObservationHash, &link.ReportHash, &link.ManifestHash, &link.SourceHash, &link.ExternalBoundaryHash, &link.BoundaryEvidenceHash, &link.BoundaryEvidenceExpiresAt, &link.TrustSnapshotRevision, &link.TrustSnapshotHash, &link.Outcome, &link.RecoveryState, &link.Status, &link.SupersedesLinkID, &link.SupersededByLinkID, &link.RevokedAt, &link.RevocationReason, &actorRaw, &link.CreatedAt); err != nil {
		return contracts.DeploymentEvidenceLink{}, err
	}
	if err := json.Unmarshal(actorRaw, &link.Actor); err != nil {
		return contracts.DeploymentEvidenceLink{}, err
	}
	link.SchemaVersion = contracts.DeploymentEvidenceSchemaVersion
	return link, link.Normalize()
}

func appendDeploymentReleaseEvent(ctx context.Context, tx pgx.Tx, release contracts.DeploymentRelease, event string, actorJSON []byte, metadata map[string]string) error {
	metadataJSON, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_deployment_release_events(workspace_id,deployment_id,release_id,event,actor,metadata) VALUES($1,$2,$3,$4,$5::jsonb,$6::jsonb)`, release.WorkspaceID, release.DeploymentID, release.ID, event, actorJSON, metadataJSON); err != nil {
		return fmt.Errorf("record deployment release event: %w", err)
	}
	return nil
}

func appendDeploymentEvidenceEvent(ctx context.Context, tx pgx.Tx, link contracts.DeploymentEvidenceLink, event string, actorJSON []byte, requestID, idempotencyKey, causationID, correlationID string) error {
	metadata, err := json.Marshal(map[string]string{"kind": link.Kind, "import_id": link.ImportID, "signed_hash": link.SignedHash, "status": link.Status, "supersedes_link_id": link.SupersedesLinkID, "superseded_by_link_id": link.SupersededByLinkID, "external_boundary_hash": link.ExternalBoundaryHash, "boundary_evidence_hash": link.BoundaryEvidenceHash, "revocation_reason": link.RevocationReason})
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_deployment_evidence_events(workspace_id,deployment_id,release_id,evidence_id,event,actor,metadata,request_id,idempotency_key,causation_id,correlation_id) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb,$8,$9,$10,$11)`, link.WorkspaceID, link.DeploymentID, link.ReleaseID, link.ID, event, actorJSON, metadata, nullableString(requestID), nullableString(idempotencyKey), nullableString(causationID), nullableString(correlationID)); err != nil {
		return fmt.Errorf("record deployment evidence event: %w", err)
	}
	return nil
}

func sameDeploymentReleaseRequest(release contracts.DeploymentRelease, request contracts.DeploymentReleaseRequest) bool {
	return release.WorkspaceID == request.WorkspaceID && release.DeploymentID == request.DeploymentID && release.ReleaseHash == request.ReleaseHash && release.TargetHash == request.TargetHash && release.Version == request.Version && release.CommitHash == request.CommitHash
}

func sameDeploymentEvidenceRequest(link contracts.DeploymentEvidenceLink, request contracts.DeploymentEvidenceLinkRequest) bool {
	return link.WorkspaceID == request.WorkspaceID && link.DeploymentID == request.DeploymentID && link.ReleaseID == request.ReleaseID && link.Kind == request.Kind && link.ImportID == request.ImportID && link.SupersedesLinkID == request.SupersedesLinkID
}

func deploymentEvidenceRecoveryState(signed contracts.SignedQualificationBundle, kind string) string {
	for _, item := range signed.Bundle.Report.Cases {
		if item.ErrorCode == "recovery_required" || item.ErrorCode == "uncertain_recovery" {
			return contracts.DeploymentEvidenceRecoveryRequired
		}
	}
	for _, drill := range signed.Bundle.Report.RecoveryDrills {
		if drill.Outcome != contracts.QualificationOutcomePassed {
			return contracts.DeploymentEvidenceRecoveryRequired
		}
		if kind == contracts.DeploymentEvidenceBackupRestore || kind == contracts.DeploymentEvidenceTopology {
			return contracts.DeploymentEvidenceResolved
		}
	}
	if kind == contracts.DeploymentEvidenceExternalEffect && signed.Bundle.Report.Outcome == contracts.QualificationOutcomeBlocked {
		return contracts.DeploymentEvidenceUnknown
	}
	return contracts.DeploymentEvidenceNotApplicable
}

// qualificationBoundaryHashes derives the only external-boundary facts that
// may be persisted on a deployment evidence link. The caller cannot supply
// these values independently: they must be covered by the imported signed
// qualification bundle.
func qualificationBoundaryHashes(signed contracts.SignedQualificationBundle, kind string, now time.Time) (string, string, *time.Time, error) {
	if kind != contracts.DeploymentEvidenceExternalEffect {
		return "", "", nil, nil
	}
	if err := signed.Normalize(); err != nil {
		return "", "", nil, fmt.Errorf("normalize external-effect qualification: %w", err)
	}
	if len(signed.Bundle.Report.BoundaryEvidence) != 1 {
		return "", "", nil, ErrDeploymentBoundaryEvidence
	}
	evidence := signed.Bundle.Report.BoundaryEvidence[0]
	if evidence.Outcome != contracts.QualificationOutcomePassed || !evidence.Measured || evidence.BoundaryHash == "" || evidence.EvidenceHash == "" {
		return "", "", nil, ErrDeploymentBoundaryEvidence
	}
	boundaryEvidenceHash := signed.Bundle.Report.BoundaryEvidenceHash()
	if boundaryEvidenceHash == "" {
		return "", "", nil, ErrDeploymentBoundaryEvidence
	}
	if evidence.ExpiresAt.IsZero() || !now.UTC().Before(evidence.ExpiresAt.UTC()) {
		return "", "", nil, ErrDeploymentBoundaryEvidence
	}
	expiresAt := evidence.ExpiresAt.UTC()
	return evidence.BoundaryHash, boundaryEvidenceHash, &expiresAt, nil
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}
