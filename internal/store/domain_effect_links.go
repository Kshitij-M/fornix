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
	ErrDomainEffectLinkNotFound          = errors.New("domain effect link not found")
	ErrDomainEffectLinkConflict          = errors.New("domain effect link conflicts with existing state")
	ErrDomainEffectLinkStale             = errors.New("domain effect link references stale authority")
	ErrDomainEffectLinkWorkspace         = errors.New("domain effect link workspace violation")
	ErrDomainEffectLinkTransition        = errors.New("domain effect link transition is invalid")
	ErrDomainEffectLinkEffectNotVerified = errors.New("sandbox effect must be verified before reconciliation")
	ErrDomainEffectLinkFenced            = errors.New("domain effect link transition version is stale")
)

// DomainEffectLinkStore persists the typed relationship between a specialized
// domain ledger and one generic operation effect. BindTx is intentionally
// exported so a domain store can create its source row and binding in one
// transaction without coupling the generic store to domain tables.
type DomainEffectLinkStore struct{ pool *pgxpool.Pool }

func NewDomainEffectLinkStore(pool *pgxpool.Pool) *DomainEffectLinkStore {
	return &DomainEffectLinkStore{pool: pool}
}

type DomainEffectLinkBindResult struct {
	Link      contracts.DomainEffectLink
	Duplicate bool
}

// DomainEffectLinkTransitionResult records one append-only link transition.
// Link.Status is the current status projected from the latest transition; the
// original link row remains immutable.
type DomainEffectLinkTransitionResult struct {
	Link       contracts.DomainEffectLink
	Transition contracts.DomainEffectLinkTransition
	Duplicate  bool
}

// DomainEffectLinkCurrent is a consistent read of the immutable link identity
// and its latest append-only transition. Callers use the version as an
// optimistic fence before a coordinator opens its final transaction.
type DomainEffectLinkCurrent struct {
	Link       contracts.DomainEffectLink
	Transition contracts.DomainEffectLinkTransition
}

func (s *DomainEffectLinkStore) Bind(ctx context.Context, link contracts.DomainEffectLink) (DomainEffectLinkBindResult, error) {
	if s == nil || s.pool == nil {
		return DomainEffectLinkBindResult{}, fmt.Errorf("domain effect link store is not configured")
	}
	if strings.TrimSpace(link.WorkspaceID) == "" {
		return DomainEffectLinkBindResult{}, ErrDomainEffectLinkWorkspace
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, link.WorkspaceID)
	if err != nil {
		return DomainEffectLinkBindResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := s.BindTx(ctx, tx, link)
	if err != nil {
		return DomainEffectLinkBindResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return DomainEffectLinkBindResult{}, fmt.Errorf("commit domain effect link: %w", err)
	}
	return result, nil
}

func (s *DomainEffectLinkStore) BindTx(ctx context.Context, tx pgx.Tx, link contracts.DomainEffectLink) (DomainEffectLinkBindResult, error) {
	if s == nil || tx == nil {
		return DomainEffectLinkBindResult{}, fmt.Errorf("domain effect link transaction is not configured")
	}
	link = cloneDomainEffectLink(link)
	if link.ID == "" {
		link.ID = contracts.NewID("effect-link")
	}
	if link.CreatedAt.IsZero() {
		link.CreatedAt = time.Now().UTC()
	}
	if err := link.Normalize(); err != nil {
		return DomainEffectLinkBindResult{}, err
	}
	encoded, err := json.Marshal(link)
	if err != nil || len(encoded) > 128<<10 {
		if err != nil {
			return DomainEffectLinkBindResult{}, err
		}
		return DomainEffectLinkBindResult{}, fmt.Errorf("domain effect link exceeds 131072 bytes")
	}
	if err := setWorkspaceContext(ctx, tx, link.WorkspaceID); err != nil {
		return DomainEffectLinkBindResult{}, err
	}
	effect, err := readEffectByID(ctx, tx, link.WorkspaceID, link.EffectID)
	if errors.Is(err, pgx.ErrNoRows) {
		return DomainEffectLinkBindResult{}, ErrDomainEffectLinkStale
	}
	if err != nil {
		return DomainEffectLinkBindResult{}, fmt.Errorf("read linked effect: %w", err)
	}
	operation, err := readOperationByID(ctx, tx, link.WorkspaceID, link.OperationID, false)
	if errors.Is(err, pgx.ErrNoRows) {
		return DomainEffectLinkBindResult{}, ErrOperationNotFound
	}
	if err != nil {
		return DomainEffectLinkBindResult{}, err
	}
	if effect.OperationID != link.OperationID || effect.StepID != link.StepID || effect.AttemptID != link.AttemptID || operation.OperationHash != link.OperationHash {
		return DomainEffectLinkBindResult{}, ErrDomainEffectLinkConflict
	}
	if storedEffectIdentityHash(effect) != link.EffectReservationHash || effect.RequestHash != link.RequestHash {
		return DomainEffectLinkBindResult{}, ErrDomainEffectLinkConflict
	}
	if effect.Boundary != link.Boundary || effect.EffectClass != string(link.EffectClass) || effect.DeliverySemantics != link.DeliveryGuarantee || effect.ProviderIdempotency != link.ProviderIdempotency || effect.ProviderRequestID != link.ProviderRequestID || effect.VerificationStatus != link.VerificationStatus || !sameExternalBoundary(effect.ExternalBoundary, link.ExternalBoundary) {
		return DomainEffectLinkBindResult{}, ErrDomainEffectLinkConflict
	}
	if effect.SchemaCatalogHash != link.SchemaCatalogHash || effect.SchemaCatalogRevision != link.SchemaCatalogRevision || effect.CredentialLeaseID != link.CredentialLeaseID || effect.CredentialLeaseFence != link.CredentialLeaseFence || effect.CredentialRevocationEpoch != link.CredentialRevocationEpoch || effect.CredentialSourceVersion != link.CredentialSourceVersion || !sameOptionalTime(effect.CredentialSourceExpiresAt, link.CredentialSourceExpiresAt) {
		return DomainEffectLinkBindResult{}, ErrDomainEffectLinkConflict
	}
	if operation.Request.Task != nil {
		if link.TaskOwnerID != operation.TaskOwnerID || link.TaskFence != operation.TaskFence || link.TaskFence == 0 {
			return DomainEffectLinkBindResult{}, ErrOperationTaskFence
		}
		if err := validateTaskFenceForOperationTx(ctx, tx, operation.Request.Task, link.TaskOwnerID, link.TaskFence); err != nil {
			return DomainEffectLinkBindResult{}, err
		}
	}
	if link.OperationOwnerID != "" {
		var owner string
		var fence int64
		var leaseUntil time.Time
		var releasedAt *time.Time
		if err := tx.QueryRow(ctx, `SELECT owner_id,fence,lease_until,released_at FROM fornix.operation_leases WHERE workspace_id=$1 AND operation_id=$2 FOR SHARE`, link.WorkspaceID, link.OperationID).Scan(&owner, &fence, &leaseUntil, &releasedAt); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return DomainEffectLinkBindResult{}, ErrDomainEffectLinkStale
			}
			return DomainEffectLinkBindResult{}, err
		}
		if owner != link.OperationOwnerID || fence <= 0 || uint64(fence) != link.OperationFence || releasedAt != nil || !leaseUntil.After(time.Now().UTC()) {
			return DomainEffectLinkBindResult{}, ErrDomainEffectLinkStale
		}
	}
	metadataJSON, _ := json.Marshal(link.Metadata)
	actorJSON, _ := json.Marshal(link.Actor)
	egressHash, destinationHash, networkBoundary, networkHash, err := boundaryColumns(link.ExternalBoundary)
	if err != nil {
		return DomainEffectLinkBindResult{}, err
	}
	operationFence, err := databaseCounter(link.OperationFence)
	if err != nil {
		return DomainEffectLinkBindResult{}, err
	}
	taskFence, err := databaseCounter(link.TaskFence)
	if err != nil {
		return DomainEffectLinkBindResult{}, err
	}
	credentialFence, err := databaseCounter(link.CredentialLeaseFence)
	if err != nil {
		return DomainEffectLinkBindResult{}, err
	}
	credentialEpoch, err := databaseCounter(link.CredentialRevocationEpoch)
	if err != nil {
		return DomainEffectLinkBindResult{}, err
	}
	command, err := tx.Exec(ctx, `
		INSERT INTO fornix.domain_effect_links(
		 workspace_id,link_id,schema_version,operation_id,operation_hash,step_id,attempt_id,effect_id,effect_reservation_hash,
		 domain_kind,domain_id,domain_hash,link_role,request_hash,result_hash,boundary,effect_class,delivery_guarantee,
		 provider_idempotency_supported,provider_request_id,verification_status,status,operation_owner_id,operation_fence,
		 task_owner_id,task_fence,schema_catalog_hash,schema_catalog_revision,credential_lease_id,credential_lease_fence,credential_revocation_epoch,credential_source_version,
		 credential_source_expires_at,actor,request_id,idempotency_key,causation_id,correlation_id,metadata,link_hash,created_at,egress_policy_hash,destination_policy_hash,network_boundary,network_boundary_hash
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34::jsonb,$35,$36,$37,$38,$39::jsonb,$40,$41,$42,$43,$44,$45)
		ON CONFLICT DO NOTHING`,
		link.WorkspaceID, link.ID, link.SchemaVersion, link.OperationID, link.OperationHash, link.StepID, link.AttemptID, link.EffectID, link.EffectReservationHash,
		link.DomainKind, link.DomainID, link.DomainHash, link.LinkRole, link.RequestHash, link.ResultHash, link.Boundary, link.EffectClass, link.DeliveryGuarantee,
		link.ProviderIdempotency, link.ProviderRequestID, link.VerificationStatus, link.Status, link.OperationOwnerID, operationFence, link.TaskOwnerID, taskFence,
		link.SchemaCatalogHash, link.SchemaCatalogRevision, link.CredentialLeaseID, credentialFence, credentialEpoch, link.CredentialSourceVersion, link.CredentialSourceExpiresAt,
		actorJSON, link.RequestID, link.IdempotencyKey, link.CausationID, link.CorrelationID, metadataJSON, link.LinkHash, link.CreatedAt, egressHash, destinationHash, networkBoundary, networkHash)
	if err != nil {
		return DomainEffectLinkBindResult{}, fmt.Errorf("insert domain effect link: %w", err)
	}
	if command.RowsAffected() == 0 {
		existing, readErr := readDomainEffectLinkByIdentityTx(ctx, tx, link)
		if readErr != nil {
			return DomainEffectLinkBindResult{}, readErr
		}
		if existing.LinkHash != link.LinkHash {
			return DomainEffectLinkBindResult{}, ErrDomainEffectLinkConflict
		}
		return DomainEffectLinkBindResult{Link: existing, Duplicate: true}, nil
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.domain_effect_link_transitions(workspace_id,link_id,version,from_status,to_status,provider_request_id,result_hash,failure_code,idempotency_key,actor) VALUES($1,$2,1,$3,$4,$5,$6,'',$7,$8::jsonb)`, link.WorkspaceID, link.ID, link.Status, link.Status, link.ProviderRequestID, link.ResultHash, link.IdempotencyKey+":linked", actorJSON); err != nil {
		return DomainEffectLinkBindResult{}, fmt.Errorf("append domain effect link transition: %w", err)
	}
	return DomainEffectLinkBindResult{Link: link}, nil
}

// Transition appends a bounded, idempotent recovery transition. It never
// updates the immutable link row and never authorizes an external effect.
// Callers must supply the exact observed version, which makes concurrent
// recovery workers fail closed rather than racing to invent an outcome.
func (s *DomainEffectLinkStore) Transition(ctx context.Context, request contracts.DomainEffectLinkTransitionRequest) (DomainEffectLinkTransitionResult, error) {
	if s == nil || s.pool == nil {
		return DomainEffectLinkTransitionResult{}, fmt.Errorf("domain effect link store is not configured")
	}
	if err := request.Normalize(); err != nil {
		return DomainEffectLinkTransitionResult{}, err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return DomainEffectLinkTransitionResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	result, err := s.TransitionTx(ctx, tx, request)
	if err != nil {
		return DomainEffectLinkTransitionResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return DomainEffectLinkTransitionResult{}, fmt.Errorf("commit domain effect link transition: %w", err)
	}
	return result, nil
}

// TransitionTx is the transaction-composition seam for a future reconciler.
// It only mutates append-only transition history and requires the link's
// current version to match the request.
func (s *DomainEffectLinkStore) TransitionTx(ctx context.Context, tx pgx.Tx, request contracts.DomainEffectLinkTransitionRequest) (DomainEffectLinkTransitionResult, error) {
	if s == nil || tx == nil {
		return DomainEffectLinkTransitionResult{}, fmt.Errorf("domain effect link transition transaction is not configured")
	}
	if err := request.Normalize(); err != nil {
		return DomainEffectLinkTransitionResult{}, err
	}
	if err := setWorkspaceContext(ctx, tx, request.WorkspaceID); err != nil {
		return DomainEffectLinkTransitionResult{}, err
	}
	if request.ToStatus == contracts.DomainEffectLinkStatusReconciled {
		if err := lockToolRunForLinkTransitionTx(ctx, tx, request.WorkspaceID, request.LinkID); err != nil {
			return DomainEffectLinkTransitionResult{}, err
		}
	}
	link, err := readDomainEffectLinkBaseTx(ctx, tx, request.WorkspaceID, request.LinkID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return DomainEffectLinkTransitionResult{}, ErrDomainEffectLinkNotFound
	}
	if err != nil {
		return DomainEffectLinkTransitionResult{}, err
	}
	current, err := readLatestDomainEffectLinkTransitionTx(ctx, tx, request.WorkspaceID, request.LinkID, true)
	if errors.Is(err, pgx.ErrNoRows) {
		return DomainEffectLinkTransitionResult{}, ErrDomainEffectLinkConflict
	}
	if err != nil {
		return DomainEffectLinkTransitionResult{}, err
	}
	if current.IdempotencyKey == request.IdempotencyKey {
		if !sameDomainEffectLinkTransition(current, request) {
			return DomainEffectLinkTransitionResult{}, ErrDomainEffectLinkConflict
		}
		if request.ToStatus == contracts.DomainEffectLinkStatusReconciled && link.DomainKind == contracts.DomainEffectKindToolRun {
			if err := validateSandboxLinkReconciliationTx(ctx, tx, request, link); err != nil {
				return DomainEffectLinkTransitionResult{}, err
			}
			if err := enqueueTerminalSandboxCleanupTx(ctx, tx, request.WorkspaceID, link.DomainID); err != nil {
				return DomainEffectLinkTransitionResult{}, err
			}
		}
		link.Status = current.ToStatus
		return DomainEffectLinkTransitionResult{Link: link, Transition: current, Duplicate: true}, nil
	}
	if current.Version != request.ExpectedVersion || current.ToStatus != request.FromStatus {
		return DomainEffectLinkTransitionResult{}, ErrDomainEffectLinkFenced
	}
	if request.ToStatus == contracts.DomainEffectLinkStatusReconciled && request.ResultHash == "" && request.FailureCode == "" {
		return DomainEffectLinkTransitionResult{}, fmt.Errorf("%w: reconciliation proof is required", ErrDomainEffectLinkTransition)
	}
	if request.ToStatus == contracts.DomainEffectLinkStatusReconciled && link.DomainKind == contracts.DomainEffectKindToolRun {
		if err := validateSandboxLinkReconciliationTx(ctx, tx, request, link); err != nil {
			return DomainEffectLinkTransitionResult{}, err
		}
	}
	actorJSON, _ := json.Marshal(request.Actor)
	transition := contracts.DomainEffectLinkTransition{
		WorkspaceID: request.WorkspaceID, LinkID: request.LinkID, Version: current.Version + 1,
		FromStatus: request.FromStatus, ToStatus: request.ToStatus,
		ProviderRequestID: request.ProviderRequestID, ResultHash: request.ResultHash,
		FailureCode: request.FailureCode, IdempotencyKey: request.IdempotencyKey,
		Actor: request.Actor,
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.domain_effect_link_transitions(
		 workspace_id,link_id,version,from_status,to_status,provider_request_id,result_hash,failure_code,idempotency_key,actor)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)`,
		transition.WorkspaceID, transition.LinkID, transition.Version, transition.FromStatus,
		transition.ToStatus, transition.ProviderRequestID, transition.ResultHash,
		transition.FailureCode, transition.IdempotencyKey, actorJSON); err != nil {
		return DomainEffectLinkTransitionResult{}, fmt.Errorf("append domain effect link transition: %w", err)
	}
	link.Status = transition.ToStatus
	if link.DomainKind == contracts.DomainEffectKindToolRun && transition.ToStatus == contracts.DomainEffectLinkStatusReconciled {
		if err := enqueueTerminalSandboxCleanupTx(ctx, tx, request.WorkspaceID, link.DomainID); err != nil {
			return DomainEffectLinkTransitionResult{}, err
		}
	}
	return DomainEffectLinkTransitionResult{Link: link, Transition: transition}, nil
}

// validateSandboxLinkReconciliationTx prevents a non-local tool link from
// becoming reconciled before its generic effect is verified. Otherwise a
// later verification commit could miss the cleanup enqueue seam permanently.
func validateSandboxLinkReconciliationTx(ctx context.Context, tx pgx.Tx, request contracts.DomainEffectLinkTransitionRequest, link contracts.DomainEffectLink) error {
	var idempotencyKey string
	if err := tx.QueryRow(ctx, `SELECT idempotency_key FROM fornix.tool_runs WHERE workspace_id=$1 AND id=$2`, request.WorkspaceID, link.DomainID).Scan(&idempotencyKey); errors.Is(err, pgx.ErrNoRows) {
		return ErrToolRunMissing
	} else if err != nil {
		return err
	}
	run, err := readToolRunTx(ctx, tx, request.WorkspaceID, idempotencyKey)
	if err != nil {
		return err
	}
	var toolRequest contracts.ToolRequest
	if len(run.RequestEvidence) == 0 || json.Unmarshal(run.RequestEvidence, &toolRequest) != nil {
		return ErrDomainEffectLinkConflict
	}
	if toolRequest.Budget.Backend == string(contracts.SandboxBackendLocalProcess) {
		return nil
	}
	if toolRequest.Budget.Backend != link.Boundary || request.ResultHash == "" {
		return ErrDomainEffectLinkEffectNotVerified
	}
	effect, err := readEffectState(ctx, tx, request.WorkspaceID, link.EffectID, false)
	if err != nil || effect.State != contracts.ExternalEffectVerified || effect.ResponseHash != request.ResultHash {
		return ErrDomainEffectLinkEffectNotVerified
	}
	return nil
}

// lockToolRunForLinkTransitionTx preserves run→link lock order shared with
// tool-result finalization, closing the race where concurrent transactions
// could both miss the other's terminal state and omit cleanup authority.
func lockToolRunForLinkTransitionTx(ctx context.Context, tx pgx.Tx, workspaceID, linkID string) error {
	var domainKind, domainID string
	if err := tx.QueryRow(ctx, `SELECT domain_kind,domain_id FROM fornix.domain_effect_links WHERE workspace_id=$1 AND link_id=$2`, workspaceID, linkID).Scan(&domainKind, &domainID); errors.Is(err, pgx.ErrNoRows) {
		return ErrDomainEffectLinkNotFound
	} else if err != nil {
		return err
	}
	if domainKind != contracts.DomainEffectKindToolRun {
		return nil
	}
	var runID string
	if err := tx.QueryRow(ctx, `SELECT id FROM fornix.tool_runs WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, workspaceID, domainID).Scan(&runID); errors.Is(err, pgx.ErrNoRows) {
		return ErrToolRunMissing
	} else if err != nil {
		return err
	}
	return nil
}

func cloneDomainEffectLink(value contracts.DomainEffectLink) contracts.DomainEffectLink {
	value.Metadata = cloneStringMap(value.Metadata)
	return value
}

func (s *DomainEffectLinkStore) Get(ctx context.Context, workspaceID, linkID string) (contracts.DomainEffectLink, error) {
	if s == nil || s.pool == nil {
		return contracts.DomainEffectLink{}, fmt.Errorf("domain effect link store is not configured")
	}
	workspaceID, linkID = strings.TrimSpace(workspaceID), strings.TrimSpace(linkID)
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.DomainEffectLink{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	value, err := readDomainEffectLinkTx(ctx, tx, workspaceID, linkID)
	if err != nil {
		return contracts.DomainEffectLink{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.DomainEffectLink{}, err
	}
	return value, nil
}

func (s *DomainEffectLinkStore) GetByDomain(ctx context.Context, workspaceID, kind, domainID, role string) (contracts.DomainEffectLink, error) {
	if s == nil || s.pool == nil {
		return contracts.DomainEffectLink{}, fmt.Errorf("domain effect link store is not configured")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.DomainEffectLink{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	value, err := readDomainEffectLinkByDomainTx(ctx, tx, workspaceID, kind, domainID, role)
	if err != nil {
		return contracts.DomainEffectLink{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.DomainEffectLink{}, err
	}
	return value, nil
}

func (s *DomainEffectLinkStore) CurrentByDomain(ctx context.Context, workspaceID, kind, domainID, role string) (DomainEffectLinkCurrent, error) {
	if s == nil || s.pool == nil {
		return DomainEffectLinkCurrent{}, fmt.Errorf("domain effect link store is not configured")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return DomainEffectLinkCurrent{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	link, err := readDomainEffectLinkBaseByDomainTx(ctx, tx, workspaceID, kind, domainID, role)
	if err != nil {
		return DomainEffectLinkCurrent{}, err
	}
	transition, err := readLatestDomainEffectLinkTransitionTx(ctx, tx, workspaceID, link.ID, false)
	if err != nil {
		return DomainEffectLinkCurrent{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return DomainEffectLinkCurrent{}, err
	}
	return DomainEffectLinkCurrent{Link: link, Transition: transition}, nil
}

// CurrentTx reads and locks one link's immutable identity plus latest
// transition inside a caller-owned transaction. Dispatchers use it to
// compose generic effect publication and link reconciliation atomically.
func (s *DomainEffectLinkStore) CurrentTx(ctx context.Context, tx pgx.Tx, workspaceID, linkID string) (DomainEffectLinkCurrent, error) {
	if s == nil || tx == nil {
		return DomainEffectLinkCurrent{}, fmt.Errorf("domain effect link transaction is not configured")
	}
	link, err := readDomainEffectLinkBaseTx(ctx, tx, workspaceID, linkID, true)
	if err != nil {
		return DomainEffectLinkCurrent{}, err
	}
	transition, err := readLatestDomainEffectLinkTransitionTx(ctx, tx, workspaceID, link.ID, true)
	if err != nil {
		return DomainEffectLinkCurrent{}, err
	}
	return DomainEffectLinkCurrent{Link: link, Transition: transition}, nil
}

func (s *DomainEffectLinkStore) List(ctx context.Context, workspaceID, operationID string, limit int) ([]contracts.DomainEffectLink, error) {
	if s == nil || s.pool == nil {
		return nil, fmt.Errorf("domain effect link store is not configured")
	}
	if limit <= 0 || limit > 256 {
		limit = 256
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, domainEffectLinkSelect+` WHERE workspace_id=$1 AND ($2='' OR operation_id=$2) ORDER BY created_at,link_id LIMIT $3`, workspaceID, strings.TrimSpace(operationID), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	values := make([]contracts.DomainEffectLink, 0, limit)
	for rows.Next() {
		value, scanErr := scanDomainEffectLink(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		if err := applyDomainEffectLinkCurrentStatus(ctx, tx, &value); err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, err
	}
	return values, nil
}

const domainEffectLinkSelect = `SELECT workspace_id,link_id,schema_version,operation_id,operation_hash,step_id,attempt_id,effect_id,effect_reservation_hash,domain_kind,domain_id,domain_hash,link_role,request_hash,result_hash,boundary,effect_class,delivery_guarantee,provider_idempotency_supported,provider_request_id,verification_status,status,operation_owner_id,operation_fence,task_owner_id,task_fence,schema_catalog_hash,schema_catalog_revision,credential_lease_id,credential_lease_fence,credential_revocation_epoch,credential_source_version,credential_source_expires_at,actor,request_id,idempotency_key,causation_id,correlation_id,metadata,link_hash,created_at,egress_policy_hash,destination_policy_hash,network_boundary,network_boundary_hash FROM fornix.domain_effect_links`

func readDomainEffectLinkTx(ctx context.Context, tx pgx.Tx, workspaceID, linkID string) (contracts.DomainEffectLink, error) {
	value, err := readDomainEffectLinkBaseTx(ctx, tx, workspaceID, linkID, false)
	if err != nil {
		return contracts.DomainEffectLink{}, err
	}
	if err := applyDomainEffectLinkCurrentStatus(ctx, tx, &value); err != nil {
		return contracts.DomainEffectLink{}, err
	}
	return value, nil
}
func readDomainEffectLinkBaseByDomainTx(ctx context.Context, tx pgx.Tx, workspaceID, kind, domainID, role string) (contracts.DomainEffectLink, error) {
	value, err := scanDomainEffectLink(tx.QueryRow(ctx, domainEffectLinkSelect+` WHERE workspace_id=$1 AND domain_kind=$2 AND domain_id=$3 AND link_role=$4`, workspaceID, strings.ToLower(strings.TrimSpace(kind)), strings.TrimSpace(domainID), strings.ToLower(strings.TrimSpace(role))))
	if err != nil {
		return contracts.DomainEffectLink{}, err
	}
	return value, nil
}

func readDomainEffectLinkByDomainTx(ctx context.Context, tx pgx.Tx, workspaceID, kind, domainID, role string) (contracts.DomainEffectLink, error) {
	value, err := readDomainEffectLinkBaseByDomainTx(ctx, tx, workspaceID, kind, domainID, role)
	if err != nil {
		return contracts.DomainEffectLink{}, err
	}
	if err := applyDomainEffectLinkCurrentStatus(ctx, tx, &value); err != nil {
		return contracts.DomainEffectLink{}, err
	}
	return value, nil
}
func readDomainEffectLinkByIdentityTx(ctx context.Context, tx pgx.Tx, link contracts.DomainEffectLink) (contracts.DomainEffectLink, error) {
	value, err := scanDomainEffectLink(tx.QueryRow(ctx, domainEffectLinkSelect+` WHERE workspace_id=$1 AND (idempotency_key=$2 OR (domain_kind=$3 AND domain_id=$4 AND link_role=$5) OR (effect_id=$6 AND link_role=$5)) ORDER BY created_at,link_id LIMIT 1`, link.WorkspaceID, link.IdempotencyKey, link.DomainKind, link.DomainID, link.EffectID, link.LinkRole))
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.DomainEffectLink{}, ErrDomainEffectLinkNotFound
	}
	if err == nil {
		err = applyDomainEffectLinkCurrentStatus(ctx, tx, &value)
	}
	return value, err
}

func readDomainEffectLinkBaseTx(ctx context.Context, tx pgx.Tx, workspaceID, linkID string, lock bool) (contracts.DomainEffectLink, error) {
	query := domainEffectLinkSelect + ` WHERE workspace_id=$1 AND link_id=$2`
	if lock {
		query += ` FOR UPDATE`
	}
	return scanDomainEffectLink(tx.QueryRow(ctx, query, workspaceID, linkID))
}

func applyDomainEffectLinkCurrentStatus(ctx context.Context, tx pgx.Tx, link *contracts.DomainEffectLink) error {
	if link == nil {
		return ErrDomainEffectLinkConflict
	}
	var status string
	if err := tx.QueryRow(ctx, `SELECT to_status FROM fornix.domain_effect_link_transitions WHERE workspace_id=$1 AND link_id=$2 ORDER BY version DESC LIMIT 1`, link.WorkspaceID, link.ID).Scan(&status); err != nil {
		return err
	}
	link.Status = status
	return nil
}

func readLatestDomainEffectLinkTransitionTx(ctx context.Context, tx pgx.Tx, workspaceID, linkID string, lock bool) (contracts.DomainEffectLinkTransition, error) {
	query := `SELECT workspace_id,link_id,version,from_status,to_status,provider_request_id,result_hash,failure_code,idempotency_key,actor,occurred_at FROM fornix.domain_effect_link_transitions WHERE workspace_id=$1 AND link_id=$2 ORDER BY version DESC LIMIT 1`
	if lock {
		query += ` FOR UPDATE`
	}
	var value contracts.DomainEffectLinkTransition
	var actorJSON []byte
	if err := tx.QueryRow(ctx, query, workspaceID, linkID).Scan(&value.WorkspaceID, &value.LinkID, &value.Version, &value.FromStatus, &value.ToStatus, &value.ProviderRequestID, &value.ResultHash, &value.FailureCode, &value.IdempotencyKey, &actorJSON, &value.OccurredAt); err != nil {
		return value, err
	}
	if err := json.Unmarshal(actorJSON, &value.Actor); err != nil {
		return value, ErrDomainEffectLinkConflict
	}
	return value, nil
}

func sameDomainEffectLinkTransition(stored contracts.DomainEffectLinkTransition, request contracts.DomainEffectLinkTransitionRequest) bool {
	return stored.LinkID == request.LinkID && stored.Version == request.ExpectedVersion+1 && stored.FromStatus == request.FromStatus && stored.ToStatus == request.ToStatus && stored.ProviderRequestID == request.ProviderRequestID && stored.ResultHash == request.ResultHash && stored.FailureCode == request.FailureCode && stored.Actor == request.Actor
}

type domainEffectLinkScanner interface{ Scan(...any) error }

func scanDomainEffectLink(row domainEffectLinkScanner) (contracts.DomainEffectLink, error) {
	var value contracts.DomainEffectLink
	var actorJSON, metadataJSON []byte
	var opFence, taskFence, credFence, credEpoch int64
	var egressHash, destinationHash, networkBoundary, networkHash string
	if err := row.Scan(&value.WorkspaceID, &value.ID, &value.SchemaVersion, &value.OperationID, &value.OperationHash, &value.StepID, &value.AttemptID, &value.EffectID, &value.EffectReservationHash, &value.DomainKind, &value.DomainID, &value.DomainHash, &value.LinkRole, &value.RequestHash, &value.ResultHash, &value.Boundary, &value.EffectClass, &value.DeliveryGuarantee, &value.ProviderIdempotency, &value.ProviderRequestID, &value.VerificationStatus, &value.Status, &value.OperationOwnerID, &opFence, &value.TaskOwnerID, &taskFence, &value.SchemaCatalogHash, &value.SchemaCatalogRevision, &value.CredentialLeaseID, &credFence, &credEpoch, &value.CredentialSourceVersion, &value.CredentialSourceExpiresAt, &actorJSON, &value.RequestID, &value.IdempotencyKey, &value.CausationID, &value.CorrelationID, &metadataJSON, &value.LinkHash, &value.CreatedAt, &egressHash, &destinationHash, &networkBoundary, &networkHash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.DomainEffectLink{}, ErrDomainEffectLinkNotFound
		}
		return contracts.DomainEffectLink{}, err
	}
	if opFence < 0 || taskFence < 0 || credFence < 0 || credEpoch < 0 {
		return contracts.DomainEffectLink{}, ErrDomainEffectLinkConflict
	}
	value.OperationFence, value.TaskFence, value.CredentialLeaseFence, value.CredentialRevocationEpoch = uint64(opFence), uint64(taskFence), uint64(credFence), uint64(credEpoch)
	var boundaryErr error
	value.ExternalBoundary, boundaryErr = boundaryFromColumns(egressHash, destinationHash, networkBoundary, networkHash)
	if boundaryErr != nil {
		return contracts.DomainEffectLink{}, ErrDomainEffectLinkConflict
	}
	if err := json.Unmarshal(actorJSON, &value.Actor); err != nil || json.Unmarshal(metadataJSON, &value.Metadata) != nil {
		return contracts.DomainEffectLink{}, ErrDomainEffectLinkConflict
	}
	if err := value.Normalize(); err != nil {
		return contracts.DomainEffectLink{}, ErrDomainEffectLinkConflict
	}
	return value, nil
}
