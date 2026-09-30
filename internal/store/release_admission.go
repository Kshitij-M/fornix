package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrDeploymentVerificationNotFound = errors.New("deployment release verification not found")
	ErrDeploymentVerificationConflict = errors.New("deployment release verification conflicts with existing state")
	ErrDeploymentVerificationStale    = errors.New("deployment release verification is stale")
	ErrDeploymentAdmissionNotReady    = errors.New("deployment release is not ready for admission")
	ErrDeploymentAdmissionReference   = errors.New("deployment admission reference is not current")
)

// RegisterVerification stores only deployment-owned verification hashes and
// binds them to the exact current release gate in one transaction.
func (s *DeploymentEvidenceStore) RegisterVerification(ctx context.Context, request contracts.DeploymentReleaseVerificationRequest, now time.Time) (contracts.DeploymentReleaseVerification, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.DeploymentReleaseVerification{}, false, fmt.Errorf("deployment evidence store is not configured")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	if err := request.Normalize(); err != nil {
		return contracts.DeploymentReleaseVerification{}, false, err
	}
	if !request.ExpiresAt.After(now) {
		return contracts.DeploymentReleaseVerification{}, false, fmt.Errorf("release verification expires_at must be in the future")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.DeploymentReleaseVerification{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-release-verification:' || $1 || ':' || $2, 0))`, request.WorkspaceID, request.DeploymentID); err != nil {
		return contracts.DeploymentReleaseVerification{}, false, err
	}
	if existing, existingErr := queryReleaseVerificationByIdempotency(ctx, tx, request.WorkspaceID, request.DeploymentID, request.IdempotencyKey, true); existingErr == nil {
		if !sameReleaseVerificationRequest(existing, request) {
			return contracts.DeploymentReleaseVerification{}, false, ErrDeploymentVerificationConflict
		}
		if request.DryRun {
			_ = tx.Rollback(ctx)
			return existing, false, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.DeploymentReleaseVerification{}, false, err
		}
		return existing, false, nil
	} else if !errors.Is(existingErr, ErrDeploymentVerificationNotFound) {
		return contracts.DeploymentReleaseVerification{}, false, existingErr
	}
	if existing, existingErr := queryReleaseVerificationByKind(ctx, tx, request.WorkspaceID, request.DeploymentID, request.ReleaseID, request.ArtifactKind, true); existingErr == nil {
		if !sameReleaseVerificationRequest(existing, request) {
			return contracts.DeploymentReleaseVerification{}, false, ErrDeploymentVerificationConflict
		}
		if request.DryRun {
			_ = tx.Rollback(ctx)
			return existing, false, nil
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.DeploymentReleaseVerification{}, false, err
		}
		return existing, false, nil
	} else if !errors.Is(existingErr, ErrDeploymentVerificationNotFound) {
		return contracts.DeploymentReleaseVerification{}, false, existingErr
	}
	release, err := queryDeploymentRelease(ctx, tx, request.WorkspaceID, request.DeploymentID, request.ReleaseID, true)
	if err != nil {
		return contracts.DeploymentReleaseVerification{}, false, err
	}
	if release.ReleaseHash != request.ReleaseHash || release.TargetHash != request.TargetHash {
		return contracts.DeploymentReleaseVerification{}, false, ErrDeploymentVerificationStale
	}
	gate, err := evaluateDeploymentGateTx(ctx, tx, release, nil, now)
	if err != nil {
		return contracts.DeploymentReleaseVerification{}, false, err
	}
	if !gate.Ready {
		return contracts.DeploymentReleaseVerification{}, false, ErrDeploymentAdmissionNotReady
	}
	if gate.GateHash != request.GateHash {
		return contracts.DeploymentReleaseVerification{}, false, ErrDeploymentVerificationStale
	}
	if request.ExternalBoundaryHash != gate.ExternalBoundaryHash || request.BoundaryEvidenceHash != gate.BoundaryEvidenceHash {
		return contracts.DeploymentReleaseVerification{}, false, ErrDeploymentVerificationStale
	}
	verification := contracts.DeploymentReleaseVerification{
		SchemaVersion: contracts.ReleaseAdmissionSchemaVersion, ID: contracts.NewID("release-verification"),
		WorkspaceID: request.WorkspaceID, DeploymentID: request.DeploymentID, ReleaseID: release.ID,
		ReleaseHash: release.ReleaseHash, TargetHash: release.TargetHash, ArtifactKind: request.ArtifactKind,
		ArtifactHash: request.ArtifactHash, AttestationHash: request.AttestationHash, GateHash: gate.GateHash,
		ExternalBoundaryHash: gate.ExternalBoundaryHash, BoundaryEvidenceHash: gate.BoundaryEvidenceHash,
		BoundaryEvidenceExpiresAt: gate.BoundaryEvidenceExpiresAt,
		TrustSnapshotRevision:     release.TrustSnapshotRevision, TrustSnapshotHash: release.TrustSnapshotHash,
		SourceReference: request.SourceReference, Status: contracts.DeploymentVerificationVerified, Actor: request.Actor,
		CreatedAt: now, ExpiresAt: request.ExpiresAt,
	}
	if err := verification.Normalize(); err != nil {
		return contracts.DeploymentReleaseVerification{}, false, err
	}
	if request.DryRun {
		_ = tx.Rollback(ctx)
		verification.ID = "dry-run-" + verification.AttestationHash[:16]
		return verification, false, nil
	}
	actorJSON, err := json.Marshal(request.Actor)
	if err != nil {
		return contracts.DeploymentReleaseVerification{}, false, err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.qualification_deployment_release_verifications
		  (id,workspace_id,deployment_id,release_id,release_hash,target_hash,artifact_kind,artifact_hash,attestation_hash,gate_hash,external_boundary_hash,boundary_evidence_hash,boundary_evidence_expires_at,trust_snapshot_revision,trust_snapshot_hash,source_reference,status,actor,request_id,idempotency_key,causation_id,correlation_id,created_at,expires_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::jsonb,$19,$20,$21,$22,$23,$24)`,
		verification.ID, verification.WorkspaceID, verification.DeploymentID, verification.ReleaseID, verification.ReleaseHash, verification.TargetHash, verification.ArtifactKind, verification.ArtifactHash, verification.AttestationHash, verification.GateHash, verification.ExternalBoundaryHash, verification.BoundaryEvidenceHash, verification.BoundaryEvidenceExpiresAt, verification.TrustSnapshotRevision, verification.TrustSnapshotHash, nullableString(verification.SourceReference), verification.Status, actorJSON, nullableString(request.RequestID), request.IdempotencyKey, nullableString(request.CausationID), nullableString(request.CorrelationID), verification.CreatedAt, verification.ExpiresAt); err != nil {
		if isUniqueViolation(err) {
			return contracts.DeploymentReleaseVerification{}, false, ErrDeploymentVerificationConflict
		}
		return contracts.DeploymentReleaseVerification{}, false, fmt.Errorf("insert release verification: %w", err)
	}
	if err := appendReleaseVerificationEvent(ctx, tx, verification, "registered", actorJSON); err != nil {
		return contracts.DeploymentReleaseVerification{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.DeploymentReleaseVerification{}, false, err
	}
	return verification, true, nil
}

func (s *DeploymentEvidenceStore) GetVerification(ctx context.Context, workspaceID, deploymentID, releaseID, artifactKind string) (contracts.DeploymentReleaseVerification, error) {
	if s == nil || s.pool == nil {
		return contracts.DeploymentReleaseVerification{}, fmt.Errorf("deployment evidence store is not configured")
	}
	workspaceID, deploymentID, releaseID, artifactKind = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(releaseID), strings.TrimSpace(artifactKind)
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.DeploymentReleaseVerification{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	verification, err := queryReleaseVerificationByKind(ctx, tx, workspaceID, deploymentID, releaseID, artifactKind, false)
	if err != nil {
		return contracts.DeploymentReleaseVerification{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.DeploymentReleaseVerification{}, err
	}
	return verification, nil
}

// RevokeVerification preserves the verification row and appends a lifecycle
// event. Revocation is idempotent for the same current state.
func (s *DeploymentEvidenceStore) RevokeVerification(ctx context.Context, workspaceID, deploymentID, releaseID, artifactKind string, actor contracts.AuditActor, now time.Time) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("deployment evidence store is not configured")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	workspaceID, deploymentID, releaseID, artifactKind = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(releaseID), strings.TrimSpace(artifactKind)
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('fornix-release-verification:' || $1 || ':' || $2, 0))`, workspaceID, deploymentID); err != nil {
		return err
	}
	verification, err := queryReleaseVerificationByKind(ctx, tx, workspaceID, deploymentID, releaseID, artifactKind, true)
	if err != nil {
		return err
	}
	if verification.Status == contracts.DeploymentVerificationRevoked {
		return tx.Commit(ctx)
	}
	if err := normalizeDeploymentAuditActor(&actor, workspaceID); err != nil {
		return err
	}
	actorJSON, err := json.Marshal(actor)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.qualification_deployment_release_verifications SET status='revoked', revoked_at=$1, actor=$2::jsonb WHERE workspace_id=$3 AND deployment_id=$4 AND release_id=$5 AND artifact_kind=$6`, now, actorJSON, workspaceID, deploymentID, releaseID, artifactKind); err != nil {
		return err
	}
	verification.Status = contracts.DeploymentVerificationRevoked
	verification.RevokedAt = &now
	verification.Actor = actor
	if err := appendReleaseVerificationEvent(ctx, tx, verification, "revoked", actorJSON); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// EvaluateAdmission is read-only and does not mutate expired rows. Expiry is
// represented in the decision so history remains immutable and replayable.
func (s *DeploymentEvidenceStore) EvaluateAdmission(ctx context.Context, workspaceID, deploymentID, releaseID, artifactKind, artifactHash string, now time.Time) (contracts.DeploymentAdmissionDecision, error) {
	if s == nil || s.pool == nil {
		return contracts.DeploymentAdmissionDecision{}, fmt.Errorf("deployment evidence store is not configured")
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	now = now.UTC()
	workspaceID, deploymentID, releaseID, artifactKind, artifactHash = strings.TrimSpace(workspaceID), strings.TrimSpace(deploymentID), strings.TrimSpace(releaseID), strings.TrimSpace(artifactKind), strings.TrimSpace(artifactHash)
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.DeploymentAdmissionDecision{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	decision, err := evaluateAdmissionTx(ctx, tx, workspaceID, deploymentID, releaseID, artifactKind, artifactHash, now)
	if err != nil {
		return contracts.DeploymentAdmissionDecision{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.DeploymentAdmissionDecision{}, err
	}
	return decision, nil
}

// ValidateAdmissionReferenceTx re-evaluates a hash-only operation reference
// inside the caller's transaction. This is the only operation-facing bridge
// to release admission: it observes Postgres authority without opening a
// second transaction, so an effect reservation cannot race a trust/gate
// change between validation and durable reservation.
func (s *DeploymentEvidenceStore) ValidateAdmissionReferenceTx(ctx context.Context, tx pgx.Tx, reference contracts.DeploymentAdmissionReference, now time.Time) error {
	if s == nil || tx == nil {
		return fmt.Errorf("deployment admission authority is not configured")
	}
	if err := reference.Normalize(); err != nil {
		return fmt.Errorf("normalize deployment admission reference: %w", err)
	}
	if err := setWorkspaceContext(ctx, tx, reference.WorkspaceID); err != nil {
		return err
	}
	if now.IsZero() {
		now = time.Now().UTC()
	}
	decision, err := evaluateAdmissionTx(ctx, tx, reference.WorkspaceID, reference.DeploymentID, reference.ReleaseID, reference.ArtifactKind, reference.ArtifactHash, now.UTC())
	if err != nil {
		return err
	}
	if !decision.Ready || decision.DecisionHash != reference.DecisionHash || decision.ReleaseHash != reference.ReleaseHash || decision.ArtifactHash != reference.ArtifactHash || decision.GateHash != reference.GateHash || decision.ExternalBoundaryHash != reference.ExternalBoundaryHash || decision.BoundaryEvidenceHash != reference.BoundaryEvidenceHash || !sameOptionalTime(decision.BoundaryEvidenceExpiresAt, reference.BoundaryEvidenceExpiresAt) || decision.TrustSnapshotRevision != reference.TrustSnapshotRevision || decision.TrustSnapshotHash != reference.TrustSnapshotHash {
		return ErrDeploymentAdmissionReference
	}
	return nil
}

func evaluateAdmissionTx(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, releaseID, artifactKind, artifactHash string, now time.Time) (contracts.DeploymentAdmissionDecision, error) {
	release, err := queryDeploymentRelease(ctx, tx, workspaceID, deploymentID, releaseID, false)
	if err != nil {
		return contracts.DeploymentAdmissionDecision{}, err
	}
	gate, err := evaluateDeploymentGateTx(ctx, tx, release, nil, now)
	if err != nil {
		return contracts.DeploymentAdmissionDecision{}, err
	}
	decision := contracts.DeploymentAdmissionDecision{
		SchemaVersion: contracts.ReleaseAdmissionSchemaVersion, WorkspaceID: workspaceID, DeploymentID: deploymentID,
		ReleaseID: release.ID, ReleaseHash: release.ReleaseHash, ArtifactKind: artifactKind, ArtifactHash: artifactHash,
		TrustSnapshotRevision: release.TrustSnapshotRevision, TrustSnapshotHash: release.TrustSnapshotHash, GateHash: gate.GateHash,
		ExternalBoundaryHash: gate.ExternalBoundaryHash, BoundaryEvidenceHash: gate.BoundaryEvidenceHash,
		BoundaryEvidenceExpiresAt: gate.BoundaryEvidenceExpiresAt,
	}
	if !gate.Ready {
		decision.BlockedReasons = append(decision.BlockedReasons, "qualification_gate_not_ready")
		decision.BlockedReasons = append(decision.BlockedReasons, gate.MissingKinds...)
		decision.BlockedReasons = append(decision.BlockedReasons, gate.BlockedReasons...)
	}
	verification, verificationErr := queryReleaseVerificationByKind(ctx, tx, workspaceID, deploymentID, release.ID, artifactKind, false)
	if verificationErr != nil {
		if errors.Is(verificationErr, ErrDeploymentVerificationNotFound) {
			decision.BlockedReasons = append(decision.BlockedReasons, "release_verification_missing")
		} else {
			return contracts.DeploymentAdmissionDecision{}, verificationErr
		}
	} else {
		if decision.ArtifactHash == "" {
			decision.ArtifactHash = verification.ArtifactHash
		}
		artifactMatches := artifactHash == "" || verification.ArtifactHash == artifactHash
		currentBindingsMatch := verification.GateHash == gate.GateHash &&
			verification.ExternalBoundaryHash == gate.ExternalBoundaryHash &&
			verification.BoundaryEvidenceHash == gate.BoundaryEvidenceHash &&
			sameOptionalTime(verification.BoundaryEvidenceExpiresAt, gate.BoundaryEvidenceExpiresAt) &&
			verification.TrustSnapshotRevision == release.TrustSnapshotRevision &&
			verification.TrustSnapshotHash == release.TrustSnapshotHash
		// Only disclose a verification alongside the exact artifact and gate it
		// binds. A mismatch remains a deterministic blocked decision below; it
		// must not make normalization fail by pairing unrelated authority facts.
		if artifactMatches && currentBindingsMatch {
			decision.Verification = &verification
		}
		if verification.Status != contracts.DeploymentVerificationVerified {
			decision.BlockedReasons = append(decision.BlockedReasons, "release_verification_"+verification.Status)
		}
		if !now.Before(verification.ExpiresAt) {
			decision.BlockedReasons = append(decision.BlockedReasons, "release_verification_expired")
		}
		if artifactHash != "" && verification.ArtifactHash != artifactHash {
			decision.BlockedReasons = append(decision.BlockedReasons, "artifact_hash_mismatch")
		}
		if verification.GateHash != gate.GateHash {
			decision.BlockedReasons = append(decision.BlockedReasons, "gate_hash_mismatch")
		}
		if verification.ExternalBoundaryHash != gate.ExternalBoundaryHash || verification.BoundaryEvidenceHash != gate.BoundaryEvidenceHash || !sameOptionalTime(verification.BoundaryEvidenceExpiresAt, gate.BoundaryEvidenceExpiresAt) {
			decision.BlockedReasons = append(decision.BlockedReasons, "boundary_evidence_mismatch")
		}
		if verification.TrustSnapshotRevision != release.TrustSnapshotRevision || verification.TrustSnapshotHash != release.TrustSnapshotHash {
			decision.BlockedReasons = append(decision.BlockedReasons, "trust_snapshot_mismatch")
		}
	}
	if len(decision.BlockedReasons) == 0 {
		decision.Ready = true
	}
	if err := decision.Normalize(); err != nil {
		return contracts.DeploymentAdmissionDecision{}, err
	}
	decision.DecisionHash = decision.StableHash()
	return decision, nil
}

func queryReleaseVerificationByIdempotency(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, key string, lock bool) (contracts.DeploymentReleaseVerification, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	verification, err := scanReleaseVerification(tx.QueryRow(ctx, releaseVerificationSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND idempotency_key=$3`+lockClause, workspaceID, deploymentID, key))
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.DeploymentReleaseVerification{}, ErrDeploymentVerificationNotFound
	}
	return verification, err
}

func queryReleaseVerificationByKind(ctx context.Context, tx pgx.Tx, workspaceID, deploymentID, releaseID, artifactKind string, lock bool) (contracts.DeploymentReleaseVerification, error) {
	lockClause := ""
	if lock {
		lockClause = " FOR UPDATE"
	}
	verification, err := scanReleaseVerification(tx.QueryRow(ctx, releaseVerificationSelect()+` WHERE workspace_id=$1 AND deployment_id=$2 AND release_id=$3 AND artifact_kind=$4`+lockClause, workspaceID, deploymentID, releaseID, artifactKind))
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.DeploymentReleaseVerification{}, ErrDeploymentVerificationNotFound
	}
	return verification, err
}

func releaseVerificationSelect() string {
	return `SELECT id,workspace_id,deployment_id,release_id,release_hash,target_hash,artifact_kind,artifact_hash,attestation_hash,gate_hash,external_boundary_hash,boundary_evidence_hash,boundary_evidence_expires_at,trust_snapshot_revision,trust_snapshot_hash,COALESCE(source_reference,''),status,actor,created_at,expires_at,revoked_at FROM fornix.qualification_deployment_release_verifications`
}

func scanReleaseVerification(row pgx.Row) (contracts.DeploymentReleaseVerification, error) {
	var verification contracts.DeploymentReleaseVerification
	var actorRaw []byte
	if err := row.Scan(&verification.ID, &verification.WorkspaceID, &verification.DeploymentID, &verification.ReleaseID, &verification.ReleaseHash, &verification.TargetHash, &verification.ArtifactKind, &verification.ArtifactHash, &verification.AttestationHash, &verification.GateHash, &verification.ExternalBoundaryHash, &verification.BoundaryEvidenceHash, &verification.BoundaryEvidenceExpiresAt, &verification.TrustSnapshotRevision, &verification.TrustSnapshotHash, &verification.SourceReference, &verification.Status, &actorRaw, &verification.CreatedAt, &verification.ExpiresAt, &verification.RevokedAt); err != nil {
		return contracts.DeploymentReleaseVerification{}, err
	}
	if err := json.Unmarshal(actorRaw, &verification.Actor); err != nil {
		return contracts.DeploymentReleaseVerification{}, err
	}
	verification.SchemaVersion = contracts.ReleaseAdmissionSchemaVersion
	return verification, verification.Normalize()
}

func sameReleaseVerificationRequest(verification contracts.DeploymentReleaseVerification, request contracts.DeploymentReleaseVerificationRequest) bool {
	return verification.WorkspaceID == request.WorkspaceID && verification.DeploymentID == request.DeploymentID && verification.ReleaseID == request.ReleaseID && verification.ReleaseHash == request.ReleaseHash && verification.TargetHash == request.TargetHash && verification.ArtifactKind == request.ArtifactKind && verification.ArtifactHash == request.ArtifactHash && verification.AttestationHash == request.AttestationHash && verification.GateHash == request.GateHash && verification.ExternalBoundaryHash == request.ExternalBoundaryHash && verification.BoundaryEvidenceHash == request.BoundaryEvidenceHash && verification.ExpiresAt.Equal(request.ExpiresAt)
}

func appendReleaseVerificationEvent(ctx context.Context, tx pgx.Tx, verification contracts.DeploymentReleaseVerification, event string, actorJSON []byte) error {
	boundaryExpiry := ""
	if verification.BoundaryEvidenceExpiresAt != nil {
		boundaryExpiry = verification.BoundaryEvidenceExpiresAt.UTC().Format(time.RFC3339Nano)
	}
	metadata, err := json.Marshal(map[string]string{"artifact_kind": verification.ArtifactKind, "artifact_hash": verification.ArtifactHash, "gate_hash": verification.GateHash, "external_boundary_hash": verification.ExternalBoundaryHash, "boundary_evidence_hash": verification.BoundaryEvidenceHash, "boundary_evidence_expires_at": boundaryExpiry, "status": verification.Status})
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.qualification_deployment_release_verification_events(workspace_id,deployment_id,release_id,verification_id,event,actor,metadata) VALUES($1,$2,$3,$4,$5,$6::jsonb,$7::jsonb)`, verification.WorkspaceID, verification.DeploymentID, verification.ReleaseID, verification.ID, event, actorJSON, metadata); err != nil {
		return fmt.Errorf("record release verification event: %w", err)
	}
	return nil
}

func normalizeDeploymentAuditActor(actor *contracts.AuditActor, workspaceID string) error {
	if actor == nil || strings.TrimSpace(actor.ID) == "" || strings.TrimSpace(actor.WorkspaceID) != workspaceID {
		return fmt.Errorf("release verification actor crosses workspace boundary")
	}
	actor.ID = strings.TrimSpace(actor.ID)
	actor.WorkspaceID = strings.TrimSpace(actor.WorkspaceID)
	actor.Kind = strings.TrimSpace(actor.Kind)
	actor.APIKeyID = strings.TrimSpace(actor.APIKeyID)
	if len(actor.ID) > contracts.MaxDomainIDLength || len(actor.Kind) > contracts.MaxDomainNameLength || len(actor.APIKeyID) > contracts.MaxDomainIDLength || strings.ContainsAny(actor.ID+actor.Kind+actor.APIKeyID, "\x00\r\n") {
		return fmt.Errorf("release verification actor is invalid")
	}
	return nil
}
