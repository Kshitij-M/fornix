package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrAuthorityLinkNotFound  = errors.New("operation authority link not found")
	ErrAuthorityLinkConflict  = errors.New("operation authority link conflicts with existing state")
	ErrAuthorityLinkStale     = errors.New("operation authority link references stale authority")
	ErrAuthorityLinkWorkspace = errors.New("operation authority link workspace violation")
)

type authorityFacts struct {
	SchemaCatalogHash         string
	SchemaCatalogRevision     string
	CredentialLeaseID         string
	CredentialLeaseFence      uint64
	CredentialRevocationEpoch uint64
	CredentialSourceVersion   string
	CredentialSourceExpiresAt *time.Time
	ExternalBoundary          *contracts.ExternalBoundaryAuthority
}

func factsFromLink(link contracts.OperationAuthorityLink) authorityFacts {
	return authorityFacts{SchemaCatalogHash: link.SchemaCatalogHash, SchemaCatalogRevision: link.SchemaCatalogRevision, CredentialLeaseID: link.CredentialLeaseID, CredentialLeaseFence: link.CredentialLeaseFence, CredentialRevocationEpoch: link.CredentialRevocationEpoch, CredentialSourceVersion: link.CredentialSourceVersion, CredentialSourceExpiresAt: link.CredentialSourceExpiresAt, ExternalBoundary: contracts.CloneExternalBoundary(link.ExternalBoundary)}
}

func mergeAuthorityFacts(dst *authorityFacts, src authorityFacts) error {
	if dst == nil {
		return ErrAuthorityLinkConflict
	}
	if src.SchemaCatalogHash != "" {
		if dst.SchemaCatalogHash == "" {
			dst.SchemaCatalogHash, dst.SchemaCatalogRevision = src.SchemaCatalogHash, src.SchemaCatalogRevision
		} else if dst.SchemaCatalogHash != src.SchemaCatalogHash || dst.SchemaCatalogRevision != src.SchemaCatalogRevision {
			return ErrAuthorityLinkConflict
		}
	}
	if src.CredentialLeaseID != "" {
		if dst.CredentialLeaseID == "" {
			*dst = mergeCredentialFacts(*dst, src)
		} else if dst.CredentialLeaseID != src.CredentialLeaseID || dst.CredentialLeaseFence != src.CredentialLeaseFence || dst.CredentialRevocationEpoch != src.CredentialRevocationEpoch || dst.CredentialSourceVersion != src.CredentialSourceVersion || !sameOptionalTime(dst.CredentialSourceExpiresAt, src.CredentialSourceExpiresAt) {
			return ErrAuthorityLinkConflict
		}
	}
	if src.ExternalBoundary != nil {
		if dst.ExternalBoundary == nil {
			dst.ExternalBoundary = contracts.CloneExternalBoundary(src.ExternalBoundary)
		} else if !sameExternalBoundary(dst.ExternalBoundary, src.ExternalBoundary) {
			return ErrAuthorityLinkConflict
		}
	}
	return nil
}

func mergeCredentialFacts(dst, src authorityFacts) authorityFacts {
	dst.CredentialLeaseID = src.CredentialLeaseID
	dst.CredentialLeaseFence = src.CredentialLeaseFence
	dst.CredentialRevocationEpoch = src.CredentialRevocationEpoch
	dst.CredentialSourceVersion = src.CredentialSourceVersion
	dst.CredentialSourceExpiresAt = src.CredentialSourceExpiresAt
	return dst
}

func readAdmissionAuthorityFactsTx(ctx context.Context, tx pgx.Tx, workspaceID, operationID string) (authorityFacts, error) {
	var link contracts.OperationAuthorityLink
	row := tx.QueryRow(ctx, `SELECT `+authorityLinkColumns+` FROM fornix.operation_authority_links WHERE workspace_id=$1 AND operation_id=$2 AND stage=$3 ORDER BY created_at,link_id LIMIT 1`, workspaceID, operationID, contracts.AuthorityStageAdmission)
	value, err := scanAuthorityLink(row)
	if errors.Is(err, ErrAuthorityLinkNotFound) {
		return authorityFacts{}, nil
	}
	if err != nil {
		return authorityFacts{}, err
	}
	link = value
	return factsFromLink(link), nil
}

func requireAdmissionAuthorityFactsTx(ctx context.Context, tx pgx.Tx, workspaceID, operationID string) (authorityFacts, error) {
	facts, err := readAdmissionAuthorityFactsTx(ctx, tx, workspaceID, operationID)
	if err != nil {
		return authorityFacts{}, err
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM fornix.operation_authority_links WHERE workspace_id=$1 AND operation_id=$2 AND stage=$3)`, workspaceID, operationID, contracts.AuthorityStageAdmission).Scan(&exists); err != nil {
		return authorityFacts{}, err
	}
	if !exists {
		return authorityFacts{}, ErrAuthorityLinkStale
	}
	return facts, nil
}

// requireDispatchableAdmissionTx is used only by the shared effect
// dispatcher and strict effect-reservation HTTP path. The lower-level store
// APIs retain their historical recovery behavior for already-recorded test
// and reconciliation fixtures; new external dispatch must opt into this
// stronger check explicitly.
func requireDispatchableAdmissionTx(ctx context.Context, tx pgx.Tx, workspaceID, operationID string) error {
	var status string
	var approvalStatus *string
	var approvalExpiresAt *time.Time
	if err := tx.QueryRow(ctx, `
		SELECT d.status, a.status, a.expires_at
		FROM fornix.operation_authority_links l
		JOIN fornix.operation_admission_decisions d
		  ON d.workspace_id=l.workspace_id AND d.decision_id=l.admission_decision_id
		LEFT JOIN fornix.operation_approvals a
		  ON a.workspace_id=d.workspace_id AND a.approval_id=d.approval_id
		WHERE l.workspace_id=$1 AND l.operation_id=$2 AND l.stage=$3
		ORDER BY l.created_at,l.link_id LIMIT 1`, workspaceID, operationID, contracts.AuthorityStageAdmission).Scan(&status, &approvalStatus, &approvalExpiresAt); err != nil {
		return fmt.Errorf("validate admission decision status: %w", err)
	}
	status = strings.ToLower(strings.TrimSpace(status))
	allowed := status == contracts.AdmissionAllowed
	if !allowed && status == contracts.AdmissionAwaitingApproval && approvalStatus != nil && *approvalStatus == contracts.ApprovalRequestApproved && approvalExpiresAt != nil && approvalExpiresAt.After(time.Now().UTC()) {
		allowed = true
	}
	if !allowed {
		return fmt.Errorf("%w: admission status %s", ErrAuthorityLinkStale, status)
	}
	return nil
}

func mergeResultAuthorityFactsTx(ctx context.Context, tx pgx.Tx, input *OperationResultInput) error {
	if input == nil {
		return ErrAuthorityLinkConflict
	}
	facts, err := readAdmissionAuthorityFactsTx(ctx, tx, input.WorkspaceID, input.OperationID)
	if err != nil {
		return err
	}
	var effectFacts authorityFacts
	var credentialFence, credentialEpoch int64
	var sourceExpiry *time.Time
	var egressHash, destinationHash, networkBoundary, networkHash string
	if err := tx.QueryRow(ctx, `SELECT schema_catalog_hash,schema_catalog_revision,credential_lease_id,credential_lease_fence,credential_revocation_epoch,credential_source_version,credential_source_expires_at,egress_policy_hash,destination_policy_hash,network_boundary,network_boundary_hash FROM fornix.operation_effects WHERE workspace_id=$1 AND operation_id=$2 ORDER BY created_at,effect_id LIMIT 1`, input.WorkspaceID, input.OperationID).Scan(&effectFacts.SchemaCatalogHash, &effectFacts.SchemaCatalogRevision, &effectFacts.CredentialLeaseID, &credentialFence, &credentialEpoch, &effectFacts.CredentialSourceVersion, &sourceExpiry, &egressHash, &destinationHash, &networkBoundary, &networkHash); err == nil {
		if credentialFence < 0 || credentialEpoch < 0 {
			return ErrAuthorityLinkConflict
		}
		effectFacts.CredentialLeaseFence, effectFacts.CredentialRevocationEpoch, effectFacts.CredentialSourceExpiresAt = uint64(credentialFence), uint64(credentialEpoch), sourceExpiry
		effectFacts.ExternalBoundary, err = boundaryFromColumns(egressHash, destinationHash, networkBoundary, networkHash)
		if err != nil {
			return err
		}
		if err := mergeAuthorityFacts(&facts, effectFacts); err != nil {
			return err
		}
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return err
	}
	if err := mergeAuthorityFacts(&facts, authorityFacts{SchemaCatalogHash: input.SchemaCatalogHash, SchemaCatalogRevision: input.SchemaCatalogRevision, CredentialLeaseID: input.CredentialLeaseID, CredentialLeaseFence: input.CredentialLeaseFence, CredentialRevocationEpoch: input.CredentialRevocationEpoch, CredentialSourceVersion: input.CredentialSourceVersion, CredentialSourceExpiresAt: input.CredentialSourceExpiresAt, ExternalBoundary: input.ExternalBoundary}); err != nil {
		return err
	}
	input.SchemaCatalogHash, input.SchemaCatalogRevision = facts.SchemaCatalogHash, facts.SchemaCatalogRevision
	input.CredentialLeaseID, input.CredentialLeaseFence, input.CredentialRevocationEpoch = facts.CredentialLeaseID, facts.CredentialLeaseFence, facts.CredentialRevocationEpoch
	input.CredentialSourceVersion, input.CredentialSourceExpiresAt = facts.CredentialSourceVersion, facts.CredentialSourceExpiresAt
	input.ExternalBoundary = contracts.CloneExternalBoundary(facts.ExternalBoundary)
	return nil
}

func mergeEffectAuthorityFactsTx(ctx context.Context, tx pgx.Tx, input *OperationEffectInput) error {
	if input == nil {
		return ErrAuthorityLinkConflict
	}
	facts, err := requireAdmissionAuthorityFactsTx(ctx, tx, input.WorkspaceID, input.OperationID)
	if err != nil {
		return err
	}
	if err := mergeAuthorityFacts(&facts, authorityFacts{SchemaCatalogHash: input.SchemaCatalogHash, SchemaCatalogRevision: input.SchemaCatalogRevision, CredentialLeaseID: input.CredentialLeaseID, CredentialLeaseFence: input.CredentialLeaseFence, CredentialRevocationEpoch: input.CredentialRevocationEpoch, CredentialSourceVersion: input.CredentialSourceVersion, CredentialSourceExpiresAt: input.CredentialSourceExpiresAt, ExternalBoundary: input.ExternalBoundary}); err != nil {
		return err
	}
	input.SchemaCatalogHash, input.SchemaCatalogRevision = facts.SchemaCatalogHash, facts.SchemaCatalogRevision
	input.CredentialLeaseID, input.CredentialLeaseFence, input.CredentialRevocationEpoch = facts.CredentialLeaseID, facts.CredentialLeaseFence, facts.CredentialRevocationEpoch
	input.CredentialSourceVersion, input.CredentialSourceExpiresAt = facts.CredentialSourceVersion, facts.CredentialSourceExpiresAt
	input.ExternalBoundary = contracts.CloneExternalBoundary(facts.ExternalBoundary)
	return nil
}

const maxAuthorityLinkInspectionLimit = 256

// appendAuthorityLinkTx is shared by the admission, result, and receipt
// stores. The caller must keep the source mutation in this same transaction.
// It validates live fences and managed trust/credential references before the
// append, so a stale authority cannot leave an apparently valid join behind.
func appendAuthorityLinkTx(ctx context.Context, tx pgx.Tx, link contracts.OperationAuthorityLink) (contracts.OperationAuthorityLink, bool, error) {
	if tx == nil {
		return contracts.OperationAuthorityLink{}, false, fmt.Errorf("authority link transaction is nil")
	}
	if err := link.Normalize(); err != nil {
		return contracts.OperationAuthorityLink{}, false, err
	}
	if link.ID == "" {
		link.ID = contracts.NewID("authority")
	}
	if link.CreatedAt.IsZero() {
		link.CreatedAt = time.Now().UTC()
	}
	if err := link.Normalize(); err != nil {
		return contracts.OperationAuthorityLink{}, false, err
	}
	if encoded, err := json.Marshal(link); err != nil || len(encoded) > contracts.MaxAuthorityLinkJSONBytes {
		if err != nil {
			return contracts.OperationAuthorityLink{}, false, err
		}
		return contracts.OperationAuthorityLink{}, false, fmt.Errorf("authority link exceeds %d bytes", contracts.MaxAuthorityLinkJSONBytes)
	}
	if err := setWorkspaceContext(ctx, tx, link.WorkspaceID); err != nil {
		return contracts.OperationAuthorityLink{}, false, err
	}
	var operationHash, operationTaskOwner string
	var operationTaskFence int64
	if err := tx.QueryRow(ctx, `SELECT operation_hash, task_owner_id, task_fence FROM fornix.operations WHERE workspace_id=$1 AND id=$2 FOR SHARE`, link.WorkspaceID, link.OperationID).Scan(&operationHash, &operationTaskOwner, &operationTaskFence); errors.Is(err, pgx.ErrNoRows) {
		return contracts.OperationAuthorityLink{}, false, ErrOperationNotFound
	} else if err != nil {
		return contracts.OperationAuthorityLink{}, false, fmt.Errorf("validate authority operation: %w", err)
	}
	if strings.ToLower(operationHash) != link.OperationHash {
		return contracts.OperationAuthorityLink{}, false, ErrAuthorityLinkConflict
	}
	if link.TaskOwnerID != "" && (operationTaskOwner != link.TaskOwnerID || operationTaskFence <= 0 || uint64(operationTaskFence) != link.TaskFence) {
		return contracts.OperationAuthorityLink{}, false, ErrAuthorityLinkStale
	}
	if link.OperationFence != 0 {
		var owner string
		var fence int64
		var leaseUntil time.Time
		var releasedAt *time.Time
		if err := tx.QueryRow(ctx, `SELECT owner_id,fence,lease_until,released_at FROM fornix.operation_leases WHERE workspace_id=$1 AND operation_id=$2 FOR SHARE`, link.WorkspaceID, link.OperationID).Scan(&owner, &fence, &leaseUntil, &releasedAt); errors.Is(err, pgx.ErrNoRows) {
			return contracts.OperationAuthorityLink{}, false, ErrAuthorityLinkStale
		} else if err != nil {
			return contracts.OperationAuthorityLink{}, false, fmt.Errorf("validate authority operation lease: %w", err)
		}
		if owner != link.OperationOwnerID || fence <= 0 || uint64(fence) != link.OperationFence || releasedAt != nil || !leaseUntil.After(time.Now().UTC()) {
			return contracts.OperationAuthorityLink{}, false, ErrAuthorityLinkStale
		}
	}
	validateFacts := validateAuthorityFactsTx
	if link.Stage == contracts.AuthorityStageReceipt {
		validateFacts = validateHistoricalAuthorityFactsTx
	}
	if err := validateFacts(ctx, tx, link.WorkspaceID, link.SchemaCatalogHash, link.SchemaCatalogRevision, link.CredentialLeaseID, link.CredentialLeaseFence, link.CredentialRevocationEpoch, link.CredentialSourceVersion, link.CredentialSourceExpiresAt); err != nil {
		return contracts.OperationAuthorityLink{}, false, err
	}
	if link.CredentialLeaseID != "" && link.Stage != contracts.AuthorityStageReceipt {
		var status, refID, sourceVersion string
		var fence, epoch int64
		var expiresAt, sourceExpiresAt *time.Time
		if err := tx.QueryRow(ctx, `SELECT status,credential_ref_id,fence,revocation_epoch,source_version,expires_at,source_expires_at FROM fornix.credential_leases WHERE workspace_id=$1 AND id=$2 FOR SHARE`, link.WorkspaceID, link.CredentialLeaseID).Scan(&status, &refID, &fence, &epoch, &sourceVersion, &expiresAt, &sourceExpiresAt); errors.Is(err, pgx.ErrNoRows) {
			return contracts.OperationAuthorityLink{}, false, ErrAuthorityLinkStale
		} else if err != nil {
			return contracts.OperationAuthorityLink{}, false, fmt.Errorf("validate authority credential lease: %w", err)
		}
		if status != "active" || fence <= 0 || epoch <= 0 || uint64(fence) != link.CredentialLeaseFence || uint64(epoch) != link.CredentialRevocationEpoch || sourceVersion != link.CredentialSourceVersion || !sameOptionalTime(sourceExpiresAt, link.CredentialSourceExpiresAt) || expiresAt == nil || !expiresAt.After(time.Now().UTC()) || sourceExpiresAt != nil && !sourceExpiresAt.After(time.Now().UTC()) || refID == "" {
			return contracts.OperationAuthorityLink{}, false, ErrAuthorityLinkStale
		}
	}
	if link.TrustPolicyHash != "" {
		var signerStatus string
		var revision int64
		if err := tx.QueryRow(ctx, `
			SELECT s.status, p.revision
			FROM fornix.trust_policies p
			JOIN fornix.trust_signers s ON s.workspace_id=p.workspace_id AND s.signer_id=p.signer_id
			WHERE p.workspace_id=$1 AND p.policy_hash=$2 AND p.revision=$3 AND p.expires_at > clock_timestamp()
			ORDER BY p.revision DESC LIMIT 1`, link.WorkspaceID, link.TrustPolicyHash, parseAuthorityRevision(link.TrustPolicyRevision)).Scan(&signerStatus, &revision); errors.Is(err, pgx.ErrNoRows) {
			return contracts.OperationAuthorityLink{}, false, ErrAuthorityLinkStale
		} else if err != nil {
			return contracts.OperationAuthorityLink{}, false, fmt.Errorf("validate authority trust policy: %w", err)
		}
		if signerStatus != "active" || fmt.Sprintf("%d", revision) != link.TrustPolicyRevision {
			return contracts.OperationAuthorityLink{}, false, ErrAuthorityLinkStale
		}
	}
	if link.AdmissionDecisionID != "" {
		var decisionOperation, decisionHash, inputHash string
		if err := tx.QueryRow(ctx, `SELECT operation_id,operation_hash,input_hash FROM fornix.operation_admission_decisions WHERE workspace_id=$1 AND decision_id=$2 FOR SHARE`, link.WorkspaceID, link.AdmissionDecisionID).Scan(&decisionOperation, &decisionHash, &inputHash); errors.Is(err, pgx.ErrNoRows) {
			return contracts.OperationAuthorityLink{}, false, ErrAuthorityLinkStale
		} else if err != nil {
			return contracts.OperationAuthorityLink{}, false, fmt.Errorf("validate authority admission decision: %w", err)
		}
		if decisionOperation != link.OperationID || strings.ToLower(decisionHash) != link.OperationHash || (link.AdmissionInputHash != "" && strings.ToLower(inputHash) != link.AdmissionInputHash) {
			return contracts.OperationAuthorityLink{}, false, ErrAuthorityLinkConflict
		}
	}
	if link.ResultID != "" {
		var resultHash string
		if err := tx.QueryRow(ctx, `SELECT result_hash FROM fornix.operation_results WHERE workspace_id=$1 AND operation_id=$2 AND result_id=$3 FOR SHARE`, link.WorkspaceID, link.OperationID, link.ResultID).Scan(&resultHash); errors.Is(err, pgx.ErrNoRows) {
			return contracts.OperationAuthorityLink{}, false, ErrAuthorityLinkStale
		} else if err != nil {
			return contracts.OperationAuthorityLink{}, false, fmt.Errorf("validate authority result: %w", err)
		} else if strings.ToLower(resultHash) != link.ResultHash {
			return contracts.OperationAuthorityLink{}, false, ErrAuthorityLinkConflict
		}
	}
	if link.ReceiptID != "" {
		var receiptHash string
		if err := tx.QueryRow(ctx, `SELECT canonical_hash FROM fornix.work_receipts WHERE workspace_id=$1 AND id=$2 FOR SHARE`, link.WorkspaceID, link.ReceiptID).Scan(&receiptHash); errors.Is(err, pgx.ErrNoRows) {
			return contracts.OperationAuthorityLink{}, false, ErrAuthorityLinkStale
		} else if err != nil {
			return contracts.OperationAuthorityLink{}, false, fmt.Errorf("validate authority receipt: %w", err)
		} else if strings.ToLower(receiptHash) != link.ReceiptHash {
			return contracts.OperationAuthorityLink{}, false, ErrAuthorityLinkConflict
		}
	}
	effectIDs := link.EffectIDs
	if effectIDs == nil {
		effectIDs = []string{}
	}
	evidenceRefs := link.Evidence
	if evidenceRefs == nil {
		evidenceRefs = []contracts.AuthorityLinkReference{}
	}
	artifactRefs := link.Artifacts
	if artifactRefs == nil {
		artifactRefs = []contracts.AuthorityLinkReference{}
	}
	effectJSON, _ := json.Marshal(effectIDs)
	evidenceJSON, _ := json.Marshal(evidenceRefs)
	artifactJSON, _ := json.Marshal(artifactRefs)
	actorJSON, _ := json.Marshal(link.Actor)
	egressHash, destinationHash, networkBoundary, networkHash, err := boundaryColumns(link.ExternalBoundary)
	if err != nil {
		return contracts.OperationAuthorityLink{}, false, err
	}
	inserted, err := tx.Exec(ctx, `
		INSERT INTO fornix.operation_authority_links(
		 workspace_id,link_id,stage,operation_id,operation_hash,idempotency_key,
		 admission_decision_id,admission_input_hash,trust_policy_hash,trust_policy_revision,schema_catalog_hash,schema_catalog_revision,
		 connector_hash,capability_hash,policy_id,policy_version,policy_hash,
		 credential_lease_id,credential_lease_fence,credential_revocation_epoch,
		 credential_source_version,credential_source_expires_at,egress_policy_hash,destination_policy_hash,network_boundary,network_boundary_hash,
		 operation_owner_id,operation_fence,task_owner_id,task_fence,effect_reservation_hash,effect_ids,
		 result_id,result_hash,receipt_id,receipt_hash,evidence_refs,artifact_refs,actor,request_id,causation_id,correlation_id,link_hash,created_at
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32::jsonb,$33,$34,$35,$36,$37::jsonb,$38::jsonb,$39::jsonb,$40,$41,$42,$43,$44)
		ON CONFLICT (workspace_id,operation_id,stage,idempotency_key) DO NOTHING`,
		link.WorkspaceID, link.ID, link.Stage, link.OperationID, link.OperationHash, link.IdempotencyKey,
		link.AdmissionDecisionID, link.AdmissionInputHash, link.TrustPolicyHash, link.TrustPolicyRevision, link.SchemaCatalogHash, link.SchemaCatalogRevision,
		link.ConnectorHash, link.CapabilityHash, link.PolicyID, link.PolicyVersion, link.PolicyHash,
		link.CredentialLeaseID, int64(link.CredentialLeaseFence), int64(link.CredentialRevocationEpoch),
		link.CredentialSourceVersion, link.CredentialSourceExpiresAt, egressHash, destinationHash, networkBoundary, networkHash,
		link.OperationOwnerID, int64(link.OperationFence), link.TaskOwnerID, int64(link.TaskFence), link.EffectReservationHash, effectJSON,
		link.ResultID, link.ResultHash, link.ReceiptID, link.ReceiptHash, evidenceJSON, artifactJSON, actorJSON, link.RequestID, link.CausationID, link.CorrelationID, link.LinkHash, link.CreatedAt)
	if err != nil {
		return contracts.OperationAuthorityLink{}, false, fmt.Errorf("insert operation authority link: %w", err)
	}
	if inserted.RowsAffected() == 0 {
		existing, readErr := readAuthorityLinkByKey(ctx, tx, link.WorkspaceID, link.OperationID, link.Stage, link.IdempotencyKey)
		if readErr != nil {
			return contracts.OperationAuthorityLink{}, false, readErr
		}
		if existing.LinkHash != link.LinkHash {
			return contracts.OperationAuthorityLink{}, false, ErrAuthorityLinkConflict
		}
		return existing, false, nil
	}
	return link, true, nil
}

func parseAuthorityRevision(value string) int64 {
	var revision int64
	if _, err := fmt.Sscan(strings.TrimSpace(value), &revision); err != nil || revision <= 0 {
		return 0
	}
	return revision
}

// AuthorityLinks returns bounded, deterministic, reference-only linkage for
// one operation. It intentionally has no raw payload disclosure path.
func (s *OperationStore) AuthorityLinks(ctx context.Context, workspaceID, operationID string, limit int) ([]contracts.OperationAuthorityLink, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("operation store is not configured")
	}
	workspaceID, operationID = strings.TrimSpace(workspaceID), strings.TrimSpace(operationID)
	if limit <= 0 || limit > maxAuthorityLinkInspectionLimit {
		limit = maxAuthorityLinkInspectionLimit
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `SELECT `+authorityLinkColumns+` FROM fornix.operation_authority_links WHERE workspace_id=$1 AND operation_id=$2 ORDER BY created_at,link_id LIMIT $3`, workspaceID, operationID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]contracts.OperationAuthorityLink, 0)
	for rows.Next() {
		value, scanErr := scanAuthorityLink(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		result = append(result, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return result, nil
}

// validateAuthorityFactsTx validates only non-secret authority metadata. The
// caller owns the surrounding transaction, so catalog and lease expiry are
// checked against the same Postgres snapshot that records the effect/link.
func validateAuthorityFactsTx(ctx context.Context, tx pgx.Tx, workspaceID, catalogHash, catalogRevision, leaseID string, leaseFence, revocationEpoch uint64, sourceVersion string, sourceExpiresAt *time.Time) error {
	return validateAuthorityFactsWithBoundary(ctx, tx, workspaceID, catalogHash, catalogRevision, leaseID, leaseFence, revocationEpoch, sourceVersion, sourceExpiresAt, nil, contracts.EffectClassUnknown)
}

func validateAuthorityFactsWithBoundary(ctx context.Context, tx pgx.Tx, workspaceID, catalogHash, catalogRevision, leaseID string, leaseFence, revocationEpoch uint64, sourceVersion string, sourceExpiresAt *time.Time, boundary *contracts.ExternalBoundaryAuthority, effectClass contracts.EffectClass) error {
	if boundary != nil {
		if err := boundary.Normalize(); err != nil {
			return ErrAuthorityLinkConflict
		}
	} else if contracts.RequiresExternalBoundary(effectClass) {
		// Strictness is enforced by OperationStore. This structural function
		// remains compatible with historical fixtures and reports the absence
		// through the caller's strict admission check.
	}
	if (catalogHash == "") != (catalogRevision == "") {
		return ErrAuthorityLinkConflict
	}
	if catalogHash != "" {
		revision := parseAuthorityRevision(catalogRevision)
		if revision <= 0 {
			return ErrAuthorityLinkStale
		}
		var signerStatus string
		if err := tx.QueryRow(ctx, `
			SELECT s.status
			FROM fornix.trust_schema_catalogs c
			JOIN fornix.trust_signers s ON s.workspace_id=c.workspace_id AND s.signer_id=c.signer_id
			WHERE c.workspace_id=$1 AND c.catalog_hash=$2 AND c.revision=$3 AND c.expires_at > clock_timestamp()
			ORDER BY c.created_at DESC LIMIT 1`, workspaceID, catalogHash, revision).Scan(&signerStatus); errors.Is(err, pgx.ErrNoRows) {
			return ErrAuthorityLinkStale
		} else if err != nil {
			return fmt.Errorf("validate authority schema catalog: %w", err)
		}
		if signerStatus != "active" {
			return ErrAuthorityLinkStale
		}
	}
	if leaseID == "" {
		if leaseFence != 0 || revocationEpoch != 0 || sourceVersion != "" || sourceExpiresAt != nil {
			return ErrAuthorityLinkConflict
		}
		return nil
	}
	if leaseFence == 0 || revocationEpoch == 0 || strings.TrimSpace(sourceVersion) == "" {
		return ErrAuthorityLinkConflict
	}
	var status, storedVersion string
	var storedFence, storedEpoch int64
	var storedExpiry, storedSourceExpiry *time.Time
	if err := tx.QueryRow(ctx, `SELECT status,fence,revocation_epoch,source_version,expires_at,source_expires_at FROM fornix.credential_leases WHERE workspace_id=$1 AND id=$2 FOR SHARE`, workspaceID, leaseID).Scan(&status, &storedFence, &storedEpoch, &storedVersion, &storedExpiry, &storedSourceExpiry); errors.Is(err, pgx.ErrNoRows) {
		return ErrAuthorityLinkStale
	} else if err != nil {
		return fmt.Errorf("validate authority credential lease: %w", err)
	}
	now := time.Now().UTC()
	if status != "active" || storedFence <= 0 || storedEpoch <= 0 || uint64(storedFence) != leaseFence || uint64(storedEpoch) != revocationEpoch || storedVersion != sourceVersion || !sameOptionalTime(storedSourceExpiry, sourceExpiresAt) || storedExpiry == nil || !storedExpiry.After(now) || storedSourceExpiry != nil && !storedSourceExpiry.After(now) {
		return ErrAuthorityLinkStale
	}
	return nil
}

// validateHistoricalAuthorityFactsTx proves that a receipt lineage points to
// the exact immutable catalog/lease records that were used earlier. It does
// not require those authorities to remain live: a completed receipt must stay
// auditable after a lease expires or a signer is revoked.
func validateHistoricalAuthorityFactsTx(ctx context.Context, tx pgx.Tx, workspaceID, catalogHash, catalogRevision, leaseID string, leaseFence, revocationEpoch uint64, sourceVersion string, sourceExpiresAt *time.Time) error {
	if (catalogHash == "") != (catalogRevision == "") {
		return ErrAuthorityLinkConflict
	}
	if catalogHash != "" {
		if revision := parseAuthorityRevision(catalogRevision); revision <= 0 {
			return ErrAuthorityLinkStale
		} else {
			var exists bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM fornix.trust_schema_catalogs WHERE workspace_id=$1 AND catalog_hash=$2 AND revision=$3)`, workspaceID, catalogHash, revision).Scan(&exists); err != nil {
				return err
			}
			if !exists {
				return ErrAuthorityLinkStale
			}
		}
	}
	if leaseID == "" {
		if leaseFence != 0 || revocationEpoch != 0 || sourceVersion != "" || sourceExpiresAt != nil {
			return ErrAuthorityLinkConflict
		}
		return nil
	}
	if leaseFence == 0 || revocationEpoch == 0 || sourceVersion == "" {
		return ErrAuthorityLinkConflict
	}
	var exists bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM fornix.credential_leases WHERE workspace_id=$1 AND id=$2 AND fence=$3 AND revocation_epoch=$4 AND source_version=$5 AND source_expires_at IS NOT DISTINCT FROM $6::timestamptz)`, workspaceID, leaseID, int64(leaseFence), int64(revocationEpoch), sourceVersion, sourceExpiresAt).Scan(&exists); err != nil {
		return err
	}
	if !exists {
		return ErrAuthorityLinkStale
	}
	return nil
}

func readAuthorityLinkStageTx(ctx context.Context, tx pgx.Tx, workspaceID, operationID, stage string) (contracts.OperationAuthorityLink, error) {
	return scanAuthorityLink(tx.QueryRow(ctx, `SELECT `+authorityLinkColumns+` FROM fornix.operation_authority_links WHERE workspace_id=$1 AND operation_id=$2 AND stage=$3 ORDER BY created_at,link_id LIMIT 1`, workspaceID, operationID, stage))
}

func readAuthorityLinkByKey(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, operationID, stage, idempotencyKey string) (contracts.OperationAuthorityLink, error) {
	return scanAuthorityLink(queryer.QueryRow(ctx, `SELECT `+authorityLinkColumns+` FROM fornix.operation_authority_links WHERE workspace_id=$1 AND operation_id=$2 AND stage=$3 AND idempotency_key=$4`, workspaceID, operationID, stage, idempotencyKey))
}

const authorityLinkColumns = `workspace_id,link_id,stage,operation_id,operation_hash,idempotency_key,admission_decision_id,admission_input_hash,trust_policy_hash,trust_policy_revision,schema_catalog_hash,schema_catalog_revision,connector_hash,capability_hash,policy_id,policy_version,policy_hash,credential_lease_id,credential_lease_fence,credential_revocation_epoch,credential_source_version,credential_source_expires_at,egress_policy_hash,destination_policy_hash,network_boundary,network_boundary_hash,operation_owner_id,operation_fence,task_owner_id,task_fence,effect_reservation_hash,effect_ids,result_id,result_hash,receipt_id,receipt_hash,evidence_refs,artifact_refs,actor,request_id,causation_id,correlation_id,link_hash,created_at`

type authorityLinkScanner interface {
	Scan(...any) error
}

func scanAuthorityLink(row authorityLinkScanner) (contracts.OperationAuthorityLink, error) {
	var value contracts.OperationAuthorityLink
	var effectJSON, evidenceJSON, artifactJSON, actorJSON []byte
	var credentialFence, credentialEpoch, operationFence, taskFence int64
	var egressHash, destinationHash, networkBoundary, networkHash string
	if err := row.Scan(&value.WorkspaceID, &value.ID, &value.Stage, &value.OperationID, &value.OperationHash, &value.IdempotencyKey, &value.AdmissionDecisionID, &value.AdmissionInputHash, &value.TrustPolicyHash, &value.TrustPolicyRevision, &value.SchemaCatalogHash, &value.SchemaCatalogRevision, &value.ConnectorHash, &value.CapabilityHash, &value.PolicyID, &value.PolicyVersion, &value.PolicyHash, &value.CredentialLeaseID, &credentialFence, &credentialEpoch, &value.CredentialSourceVersion, &value.CredentialSourceExpiresAt, &egressHash, &destinationHash, &networkBoundary, &networkHash, &value.OperationOwnerID, &operationFence, &value.TaskOwnerID, &taskFence, &value.EffectReservationHash, &effectJSON, &value.ResultID, &value.ResultHash, &value.ReceiptID, &value.ReceiptHash, &evidenceJSON, &artifactJSON, &actorJSON, &value.RequestID, &value.CausationID, &value.CorrelationID, &value.LinkHash, &value.CreatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.OperationAuthorityLink{}, ErrAuthorityLinkNotFound
		}
		return contracts.OperationAuthorityLink{}, err
	}
	if credentialFence < 0 || credentialEpoch < 0 || operationFence < 0 || taskFence < 0 {
		return contracts.OperationAuthorityLink{}, ErrAuthorityLinkConflict
	}
	value.CredentialLeaseFence, value.CredentialRevocationEpoch = uint64(credentialFence), uint64(credentialEpoch)
	value.OperationFence, value.TaskFence = uint64(operationFence), uint64(taskFence)
	var boundaryErr error
	value.ExternalBoundary, boundaryErr = boundaryFromColumns(egressHash, destinationHash, networkBoundary, networkHash)
	if boundaryErr != nil {
		return contracts.OperationAuthorityLink{}, ErrAuthorityLinkConflict
	}
	if err := json.Unmarshal(effectJSON, &value.EffectIDs); err != nil || json.Unmarshal(evidenceJSON, &value.Evidence) != nil || json.Unmarshal(artifactJSON, &value.Artifacts) != nil || json.Unmarshal(actorJSON, &value.Actor) != nil {
		return contracts.OperationAuthorityLink{}, ErrAuthorityLinkConflict
	}
	if err := value.Normalize(); err != nil || value.LinkHash != strings.ToLower(value.LinkHash) {
		return contracts.OperationAuthorityLink{}, ErrAuthorityLinkConflict
	}
	return value, nil
}

func authorityEffectIDs(effects []contracts.ExternalEffect) ([]string, string) {
	ids := make([]string, 0, len(effects))
	hashes := make([]string, 0, len(effects))
	for _, effect := range effects {
		if effect.ID != "" {
			ids = append(ids, effect.ID)
		}
		if hash := effect.StableHash(); hash != "" {
			hashes = append(hashes, hash)
		}
	}
	sort.Strings(ids)
	sort.Strings(hashes)
	reservation := ""
	if len(hashes) > 0 {
		reservation = contracts.HashStrings(hashes...)
	}
	return ids, reservation
}

func authorityEvidenceRefs(refs []contracts.OperationEvidenceRef) []contracts.AuthorityLinkReference {
	result := make([]contracts.AuthorityLinkReference, 0, len(refs))
	for _, ref := range refs {
		result = append(result, contracts.AuthorityLinkReference{Kind: "evidence", ID: ref.SourceReference, Hash: ref.EvidenceHash, Role: ref.Role})
	}
	return result
}

func authorityResultEvidenceRefs(result contracts.OperationResult) []contracts.AuthorityLinkReference {
	refs := append([]contracts.OperationEvidenceRef(nil), result.Evidence...)
	for _, step := range result.Steps {
		refs = append(refs, step.Evidence...)
	}
	return authorityEvidenceRefs(refs)
}

func authorityAdmissionLink(input contracts.AdmissionInput, decision contracts.AdmissionDecision) contracts.OperationAuthorityLink {
	link := contracts.OperationAuthorityLink{
		WorkspaceID: input.WorkspaceID, Stage: contracts.AuthorityStageAdmission, OperationID: decision.OperationID, OperationHash: decision.OperationHash,
		AdmissionDecisionID: decision.ID, AdmissionInputHash: decision.InputHash, TrustPolicyHash: input.TrustPolicyHash, TrustPolicyRevision: input.TrustPolicyRevision,
		SchemaCatalogHash: input.SchemaCatalogHash, SchemaCatalogRevision: input.SchemaCatalogRevision,
		ExternalBoundary: contracts.CloneExternalBoundary(input.ExternalBoundary),
		ConnectorHash:    decision.Capability.Connector.StableHash(), CapabilityHash: decision.Capability.DefinitionHash,
		PolicyID: decision.PolicyID, PolicyVersion: decision.PolicyVersion, PolicyHash: decision.PolicyHash,
		CredentialLeaseID: input.CredentialLeaseID, CredentialLeaseFence: input.CredentialLeaseFence, CredentialRevocationEpoch: input.CredentialRevocationEpoch,
		CredentialSourceVersion: input.CredentialSourceVersion, CredentialSourceExpiresAt: input.CredentialSourceExpiresAt,
		Actor: decision.Actor, RequestID: decision.RequestID, IdempotencyKey: decision.IdempotencyKey,
		CausationID: "admission:" + decision.OperationID, CorrelationID: input.OperationID,
	}
	if input.TaskFenceValid {
		link.TaskOwnerID, link.TaskFence = input.TaskOwnerID, input.TaskFence
	}
	return link
}

func authorityResultLink(input OperationResultInput, operation Operation, result contracts.OperationResult, resultID, resultHash string, admission contracts.AdmissionDecision) contracts.OperationAuthorityLink {
	ids, reservation := authorityEffectIDs(result.ExternalEffects)
	requestID := input.RequestID
	if requestID == "" {
		requestID = operation.RequestID
	}
	link := contracts.OperationAuthorityLink{
		WorkspaceID: input.WorkspaceID, Stage: contracts.AuthorityStageResult, OperationID: input.OperationID, OperationHash: operation.OperationHash,
		AdmissionDecisionID: admission.ID, AdmissionInputHash: admission.InputHash, ConnectorHash: admission.Capability.Connector.StableHash(), CapabilityHash: admission.Capability.DefinitionHash,
		PolicyID: admission.PolicyID, PolicyVersion: admission.PolicyVersion, PolicyHash: admission.PolicyHash,
		TrustPolicyHash: input.TrustPolicyHash, TrustPolicyRevision: input.TrustPolicyRevision,
		SchemaCatalogHash: input.SchemaCatalogHash, SchemaCatalogRevision: input.SchemaCatalogRevision,
		ExternalBoundary:  contracts.CloneExternalBoundary(input.ExternalBoundary),
		CredentialLeaseID: input.CredentialLeaseID, CredentialLeaseFence: input.CredentialLeaseFence, CredentialRevocationEpoch: input.CredentialRevocationEpoch,
		CredentialSourceVersion: input.CredentialSourceVersion, CredentialSourceExpiresAt: input.CredentialSourceExpiresAt,
		OperationOwnerID: input.OwnerID, OperationFence: input.Fence, TaskOwnerID: input.TaskOwnerID, TaskFence: input.TaskFence,
		EffectReservationHash: reservation, EffectIDs: ids, ResultID: resultID, ResultHash: resultHash,
		Evidence: authorityResultEvidenceRefs(result), Actor: input.Actor, RequestID: requestID,
		IdempotencyKey: input.IdempotencyKey, CausationID: input.CausationID, CorrelationID: input.CorrelationID,
	}
	return link
}

func authorityReceiptLink(receipt contracts.WorkReceipt) contracts.OperationAuthorityLink {
	policyIDValue, policyVersionValue, policyHashValue := "", "", ""
	if receipt.Policy != nil {
		policyIDValue, policyVersionValue, policyHashValue = receipt.Policy.PolicyID, receipt.Policy.Version, receipt.Policy.PolicyHash
	}
	link := contracts.OperationAuthorityLink{
		WorkspaceID: receipt.WorkspaceID, Stage: contracts.AuthorityStageReceipt, OperationID: receipt.Operation.ID, OperationHash: receipt.Operation.Hash,
		PolicyID: policyIDValue, PolicyVersion: policyVersionValue, PolicyHash: policyHashValue,
		ReceiptID: receipt.ID, ReceiptHash: receipt.CanonicalHash,
		Actor: receipt.Actor, RequestID: receipt.RequestID, IdempotencyKey: receipt.IdempotencyKey,
		CausationID: "receipt:" + receipt.ID, CorrelationID: receipt.WorkID,
	}
	for _, evidence := range receipt.Evidence {
		link.Evidence = append(link.Evidence, contracts.AuthorityLinkReference{Kind: "evidence", ID: fmt.Sprintf("%d", evidence.ID), Hash: evidence.EvidenceHash, Role: evidence.Role})
	}
	for _, artifact := range receipt.Artifacts {
		link.Artifacts = append(link.Artifacts, contracts.AuthorityLinkReference{Kind: "artifact", ID: fmt.Sprintf("%d", artifact.ArtifactID), Hash: artifact.ContentHash, Role: artifact.Role})
	}
	return link
}

func admissionForOperationTx(ctx context.Context, tx pgx.Tx, workspaceID, operationID string) (contracts.AdmissionDecision, error) {
	var decision contracts.AdmissionDecision
	var actorJSON, capabilityJSON, targetJSON []byte
	err := tx.QueryRow(ctx, `SELECT decision_id,workspace_id,operation_id,operation_hash,request_id,idempotency_key,actor,capability,target,effect_class,policy_id,policy_version,policy_hash,input_hash,decision_hash,status,reason_code,approval_id,cost_micros,retry_at,created_at FROM fornix.operation_admission_decisions WHERE workspace_id=$1 AND operation_id=$2 ORDER BY created_at,decision_id LIMIT 1`, workspaceID, operationID).Scan(&decision.ID, &decision.WorkspaceID, &decision.OperationID, &decision.OperationHash, &decision.RequestID, &decision.IdempotencyKey, &actorJSON, &capabilityJSON, &targetJSON, &decision.Effect, &decision.PolicyID, &decision.PolicyVersion, &decision.PolicyHash, &decision.InputHash, &decision.DecisionHash, &decision.Status, &decision.ReasonCode, &decision.ApprovalID, &decision.CostMicros, &decision.RetryAt, &decision.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.AdmissionDecision{}, nil
	}
	if err != nil {
		return contracts.AdmissionDecision{}, err
	}
	if err := json.Unmarshal(actorJSON, &decision.Actor); err != nil || json.Unmarshal(capabilityJSON, &decision.Capability) != nil || json.Unmarshal(targetJSON, &decision.Target) != nil {
		return contracts.AdmissionDecision{}, ErrAuthorityLinkConflict
	}
	return decision, nil
}
