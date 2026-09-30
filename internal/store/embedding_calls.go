package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	pgvector "github.com/pgvector/pgvector-go"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrEmbeddingCallConflict           = errors.New("embedding call idempotency key reused with a different request")
	ErrEmbeddingCallMissing            = errors.New("embedding call not found")
	ErrEmbeddingCallInFlight           = errors.New("embedding call already in progress")
	ErrEmbeddingCallRecoveryRequired   = errors.New("embedding call requires external outcome recovery")
	ErrEmbeddingCallTerminal           = errors.New("embedding call is already terminal")
	ErrEmbeddingCallNotStale           = errors.New("embedding call is not stale")
	ErrEmbeddingCallLeaseFenced        = errors.New("embedding call task lease is stale")
	ErrEmbeddingAttachmentConflict     = errors.New("embedding target attachment conflicts with existing lineage")
	ErrEmbeddingReconciliationConflict = errors.New("embedding reconciliation conflicts with durable call")
	ErrEmbeddingQueryUseConflict       = errors.New("embedding query use conflicts with existing attribution")
)

// EmbeddingCallStore is the Postgres authority for provider-facing embedding
// attempts and replayable vector results. Domain rows remain responsible for
// their own derived vector projections.
type EmbeddingCallStore struct {
	pool *pgxpool.Pool
}

func NewEmbeddingCallStore(pool *pgxpool.Pool) *EmbeddingCallStore {
	return &EmbeddingCallStore{pool: pool}
}

// Start reserves a call before provider dispatch. A matching terminal call is
// replayable; a running call fails closed so duplicate delivery cannot invoke
// a provider twice.
func (s *EmbeddingCallStore) Start(ctx context.Context, request contracts.EmbeddingRequest, requestEvidence []byte) (contracts.EmbeddingCallStart, error) {
	if s == nil || s.pool == nil {
		return contracts.EmbeddingCallStart{}, fmt.Errorf("embedding call store is not configured")
	}
	if err := request.Normalize(); err != nil {
		return contracts.EmbeddingCallStart{}, fmt.Errorf("normalize embedding call: %w", err)
	}
	requestHash := request.RequestHash()
	requestEvidence = boundedEmbeddingEvidence(requestEvidence, request, requestHash)
	actorJSON, err := json.Marshal(request.Actor)
	if err != nil {
		return contracts.EmbeddingCallStart{}, fmt.Errorf("encode embedding actor: %w", err)
	}
	taskJSON, err := jsonOrEmpty(request.Task)
	if err != nil {
		return contracts.EmbeddingCallStart{}, err
	}
	sessionJSON, err := jsonOrEmpty(request.Session)
	if err != nil {
		return contracts.EmbeddingCallStart{}, err
	}
	metadataJSON, err := json.Marshal(redactedEmbeddingMetadata(request.Metadata))
	if err != nil {
		return contracts.EmbeddingCallStart{}, err
	}
	retentionClass, retentionDeadline := embeddingRetentionForSource(request.SourceKind)
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.EmbeddingCallStart{}, fmt.Errorf("begin embedding call start: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if retentionClass == contracts.EmbeddingRetentionQuery {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, contracts.HashStrings("query-embedding-key", request.WorkspaceID, request.SourceHash, request.Provider.Provider, request.Model)); err != nil {
			return contracts.EmbeddingCallStart{}, fmt.Errorf("lock reusable query embedding key: %w", err)
		}
		var reusableKey string
		lookupErr := tx.QueryRow(ctx, `
			SELECT idempotency_key
			FROM fornix.embedding_calls
			WHERE workspace_id=$1 AND source_hash=$2 AND provider=$3 AND model=$4
			  AND retention_class=$5 AND status IN ($6,$7,$8,$9)
			ORDER BY id
			LIMIT 1
			FOR UPDATE`, request.WorkspaceID, request.SourceHash, request.Provider.Provider, request.Model, contracts.EmbeddingRetentionQuery, contracts.EmbeddingCallPending, contracts.EmbeddingCallRunning, contracts.EmbeddingCallSucceeded, contracts.EmbeddingCallRecoveryRequired).Scan(&reusableKey)
		if lookupErr == nil {
			record, readErr := readEmbeddingCallTx(ctx, tx, request.WorkspaceID, reusableKey)
			if readErr != nil {
				return contracts.EmbeddingCallStart{}, readErr
			}
			if err := tx.Commit(ctx); err != nil {
				return contracts.EmbeddingCallStart{}, fmt.Errorf("commit reusable query embedding read: %w", err)
			}
			return contracts.EmbeddingCallStart{Record: record, Existing: true}, nil
		}
		if !errors.Is(lookupErr, pgx.ErrNoRows) {
			return contracts.EmbeddingCallStart{}, fmt.Errorf("resolve reusable query embedding: %w", lookupErr)
		}
	}
	inserted, err := tx.Exec(ctx, `
		INSERT INTO fornix.embedding_calls(
			workspace_id, request_id, idempotency_key, request_hash, schema_version,
			causation_id, correlation_id, source_kind, source_id, source_hash,
			provider, endpoint, model, actor, task_ref, session_ref,
			task_owner_id, task_fence, metadata, status, input_bytes,
			budget_max_input_bytes, budget_dimension, budget_timeout_ms, max_cost_usd,
			request_evidence, retention_class, retention_deadline
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::jsonb,$15::jsonb,$16::jsonb,$17,$18,$19::jsonb,$20,$21,$22,$23,$24,$25,$26::jsonb,$27,$28)
		ON CONFLICT DO NOTHING`,
		request.WorkspaceID, request.RequestID, request.IdempotencyKey, requestHash, request.SchemaVersion,
		request.CausationID, request.CorrelationID, request.SourceKind, request.SourceID, request.SourceHash,
		request.Provider.Provider, request.Provider.Endpoint, request.Model, actorJSON, taskJSON, sessionJSON,
		request.TaskOwnerID, request.TaskFence, metadataJSON, contracts.EmbeddingCallPending, len([]byte(request.Text)),
		request.Budget.MaxInputBytes, request.Budget.Dimension, request.Budget.TimeoutMS, request.Budget.MaxCostUSD,
		requestEvidence, retentionClass, retentionDeadline)
	if err != nil {
		return contracts.EmbeddingCallStart{}, fmt.Errorf("reserve embedding call: %w", err)
	}
	if inserted.RowsAffected() == 0 {
		var existingKey string
		lookupErr := tx.QueryRow(ctx, `
			SELECT idempotency_key FROM fornix.embedding_calls
			WHERE workspace_id=$1 AND (idempotency_key=$2 OR request_id=$3)
			ORDER BY id FOR UPDATE`, request.WorkspaceID, request.IdempotencyKey, request.RequestID).Scan(&existingKey)
		if errors.Is(lookupErr, pgx.ErrNoRows) {
			return contracts.EmbeddingCallStart{}, ErrEmbeddingCallMissing
		}
		if lookupErr != nil {
			return contracts.EmbeddingCallStart{}, fmt.Errorf("resolve duplicate embedding call: %w", lookupErr)
		}
		record, readErr := readEmbeddingCallTx(ctx, tx, request.WorkspaceID, existingKey)
		if readErr != nil {
			return contracts.EmbeddingCallStart{}, readErr
		}
		if record.RequestHash != requestHash {
			return contracts.EmbeddingCallStart{}, fmt.Errorf("%w: %s", ErrEmbeddingCallConflict, request.IdempotencyKey)
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.EmbeddingCallStart{}, fmt.Errorf("commit duplicate embedding call read: %w", err)
		}
		return contracts.EmbeddingCallStart{Record: record, Existing: true}, nil
	}
	record, err := readEmbeddingCallTx(ctx, tx, request.WorkspaceID, request.IdempotencyKey)
	if err != nil {
		return contracts.EmbeddingCallStart{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.EmbeddingCallStart{}, fmt.Errorf("commit embedding call start: %w", err)
	}
	return contracts.EmbeddingCallStart{Record: record}, nil
}

// Attempt changes a newly reserved call to running and increments its
// attempt count. A pending-to-running transition is intentionally strict.
func (s *EmbeddingCallStore) Attempt(ctx context.Context, workspaceID, requestID string) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("embedding call store is not configured")
	}
	workspaceID, requestID = strings.TrimSpace(workspaceID), strings.TrimSpace(requestID)
	if workspaceID == "" || requestID == "" {
		return fmt.Errorf("workspace_id and request_id are required")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return fmt.Errorf("begin embedding call attempt: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status string
	var taskJSON []byte
	var ownerID string
	var fence int64
	if err := tx.QueryRow(ctx, `
		SELECT status, task_ref, task_owner_id, task_fence
		FROM fornix.embedding_calls
		WHERE workspace_id=$1 AND request_id=$2
		FOR UPDATE`, workspaceID, requestID).Scan(&status, &taskJSON, &ownerID, &fence); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrEmbeddingCallMissing
		}
		return fmt.Errorf("lock embedding call before attempt: %w", err)
	}
	if status != contracts.EmbeddingCallPending {
		switch status {
		case contracts.EmbeddingCallRunning, contracts.EmbeddingCallPending:
			return ErrEmbeddingCallInFlight
		case contracts.EmbeddingCallRecoveryRequired:
			return ErrEmbeddingCallRecoveryRequired
		default:
			return ErrEmbeddingCallTerminal
		}
	}
	task, err := decodeEntityRef(taskJSON)
	if err != nil {
		return err
	}
	if err := validateEmbeddingTaskFenceTx(ctx, tx, workspaceID, task, ownerID, uint64(maxInt64(fence))); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE fornix.embedding_calls
		SET status=$1, attempt_count=attempt_count+1, started_at=COALESCE(started_at, now())
		WHERE workspace_id=$2 AND request_id=$3 AND status=$4`,
		contracts.EmbeddingCallRunning, workspaceID, requestID, contracts.EmbeddingCallPending); err != nil {
		return fmt.Errorf("record embedding call attempt: %w", err)
	}
	return tx.Commit(ctx)
}

// Finish validates and durably records a provider outcome. A terminal result
// is idempotent when the same status is delivered again.
func (s *EmbeddingCallStore) Finish(ctx context.Context, result contracts.EmbeddingCallResult) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("embedding call store is not configured")
	}
	result.WorkspaceID, result.RequestID = strings.TrimSpace(result.WorkspaceID), strings.TrimSpace(result.RequestID)
	if result.WorkspaceID == "" || result.RequestID == "" {
		return fmt.Errorf("workspace_id and request_id are required")
	}
	if result.Status != contracts.EmbeddingCallSucceeded && result.Status != contracts.EmbeddingCallFailed && result.Status != contracts.EmbeddingCallRecoveryRequired && result.Status != contracts.EmbeddingCallCancelled {
		return fmt.Errorf("invalid terminal embedding call status %q", result.Status)
	}
	var vectorValue any
	vectorHash := ""
	vectorDimension := 0
	if result.Status == contracts.EmbeddingCallSucceeded {
		if len(result.Vector) != contracts.EmbeddingDimension {
			return fmt.Errorf("successful embedding vector dimension must be %d", contracts.EmbeddingDimension)
		}
		var err error
		vectorHash, err = contracts.EmbeddingVectorHash(result.Vector)
		if err != nil {
			return err
		}
		vectorDimension = len(result.Vector)
		vectorValue = pgvector.NewVector(result.Vector)
	} else if len(result.Vector) > 0 {
		return fmt.Errorf("non-successful embedding call cannot contain a vector")
	}
	usageJSON, err := json.Marshal(result.Usage)
	if err != nil {
		return err
	}
	failureJSON := []byte(nil)
	if result.Failure != nil {
		failure := *result.Failure
		failure.Message = "embedding provider failure"
		failureJSON, err = json.Marshal(failure)
		if err != nil {
			return err
		}
	}
	responseEvidence := boundedEmbeddingEvidence(result.ResponseEvidence, contracts.EmbeddingRequest{WorkspaceID: result.WorkspaceID, RequestID: result.RequestID}, "")
	tx, err := beginWorkspaceTx(ctx, s.pool, result.WorkspaceID)
	if err != nil {
		return fmt.Errorf("begin embedding call finish: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var currentStatus string
	var startedAt *time.Time
	var taskJSON []byte
	var storedOwner string
	var storedFence int64
	var storedVectorHash string
	err = tx.QueryRow(ctx, `SELECT status, started_at, task_ref, task_owner_id, task_fence, vector_hash FROM fornix.embedding_calls WHERE workspace_id=$1 AND request_id=$2 FOR UPDATE`, result.WorkspaceID, result.RequestID).Scan(&currentStatus, &startedAt, &taskJSON, &storedOwner, &storedFence, &storedVectorHash)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrEmbeddingCallMissing
	}
	if err != nil {
		return fmt.Errorf("lock embedding call before finish: %w", err)
	}
	if currentStatus != contracts.EmbeddingCallRunning {
		if currentStatus == result.Status {
			if result.Status == contracts.EmbeddingCallSucceeded && storedVectorHash != vectorHash {
				return fmt.Errorf("%w: duplicate result vector hash differs", ErrEmbeddingCallConflict)
			}
			return nil
		}
		if currentStatus == contracts.EmbeddingCallRecoveryRequired {
			return ErrEmbeddingCallRecoveryRequired
		}
		return fmt.Errorf("%w: status=%s", ErrEmbeddingCallTerminal, currentStatus)
	}
	task, err := decodeEntityRef(taskJSON)
	if err != nil {
		return err
	}
	if result.TaskOwnerID == "" {
		result.TaskOwnerID = storedOwner
	}
	if result.TaskFence == 0 && storedFence > 0 {
		result.TaskFence = uint64(storedFence)
	}
	if err := validateEmbeddingTaskFenceTx(ctx, tx, result.WorkspaceID, task, result.TaskOwnerID, result.TaskFence); err != nil {
		return err
	}
	updated, err := tx.Exec(ctx, `
		UPDATE fornix.embedding_calls
		SET status=$1, attempt_count=GREATEST(attempt_count,$2), provider_request_id=$3,
			usage=$4::jsonb, failure=$5::jsonb, response_evidence=$6::jsonb,
			vector=$7, vector_hash=$8, vector_dimension=$9,
			duration_ms=GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (now()-COALESCE(started_at, now()))) * 1000)::bigint),
			finished_at=now()
		WHERE workspace_id=$10 AND request_id=$11 AND status=$12`,
		result.Status, result.AttemptCount, strings.TrimSpace(result.ProviderRequestID), usageJSON, nullJSON(failureJSON), responseEvidence,
		vectorValue, vectorHash, vectorDimension, result.WorkspaceID, result.RequestID, contracts.EmbeddingCallRunning)
	if err != nil {
		return fmt.Errorf("finish embedding call: %w", err)
	}
	if updated.RowsAffected() != 1 {
		return ErrEmbeddingCallTerminal
	}
	_ = startedAt
	return tx.Commit(ctx)
}

// ResolveRecovery finalizes an ambiguous call only from a provider-specific
// reconciliation result. It is deliberately separate from Finish so a normal
// worker cannot turn an uncertain call into a success with a late duplicate.
// The reconciliation audit row and specialized ledger transition commit
// together; generic operation/link reconciliation is owned by the caller's
// higher-level recovery coordinator.
func (s *EmbeddingCallStore) ResolveRecovery(ctx context.Context, result contracts.EmbeddingReconciliationResult) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("embedding call store is not configured")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, strings.TrimSpace(result.WorkspaceID))
	if err != nil {
		return fmt.Errorf("begin embedding recovery resolution: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.resolveRecoveryTx(ctx, tx, result, true); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// ResolveRecoveryTx applies the specialized embedding ledger transition in a
// caller-owned transaction. It intentionally does not commit; the recovery
// coordinator uses it with the generic effect and domain-link transitions so
// the complete local outcome is atomic.
func (s *EmbeddingCallStore) ResolveRecoveryTx(ctx context.Context, tx pgx.Tx, result contracts.EmbeddingReconciliationResult) error {
	return s.resolveRecoveryTx(ctx, tx, result, false)
}

type EmbeddingRecoveryAuditFacts struct {
	OperationID           string
	EffectID              string
	LinkID                string
	RequestHash           string
	RecoveryOwnerID       string
	RecoveryFence         uint64
	ExpectedEffectVersion int64
	ExpectedLinkVersion   int64
	CausationID           string
	CorrelationID         string
}

func (s *EmbeddingCallStore) resolveRecoveryTx(ctx context.Context, tx pgx.Tx, result contracts.EmbeddingReconciliationResult, requireTaskFence bool, auditFacts ...EmbeddingRecoveryAuditFacts) error {
	if s == nil || tx == nil {
		return fmt.Errorf("embedding recovery transaction is not configured")
	}
	result.WorkspaceID, result.RequestID = strings.TrimSpace(result.WorkspaceID), strings.TrimSpace(result.RequestID)
	result.SourceHash = strings.ToLower(strings.TrimSpace(result.SourceHash))
	result.ProviderRequestID = strings.TrimSpace(result.ProviderRequestID)
	if result.WorkspaceID == "" || result.RequestID == "" || result.ProviderRequestID == "" || result.Status != contracts.EmbeddingCallSucceeded && result.Status != contracts.EmbeddingCallFailed {
		return fmt.Errorf("embedding reconciliation identity or status is invalid")
	}
	vectorHash := ""
	vectorDimension := 0
	var vectorValue any
	if result.Status == contracts.EmbeddingCallSucceeded {
		var err error
		vectorHash, err = contracts.EmbeddingVectorHash(result.Vector)
		if err != nil {
			return err
		}
		vectorDimension = len(result.Vector)
		vectorValue = pgvector.NewVector(result.Vector)
	} else if len(result.Vector) != 0 {
		return fmt.Errorf("failed embedding reconciliation cannot contain a vector")
	}
	if len(result.ResponseEvidence) > 16<<10 {
		return fmt.Errorf("embedding reconciliation evidence exceeds 16KiB")
	}
	usageJSON, err := json.Marshal(result.Usage)
	if err != nil {
		return err
	}
	failureJSON := []byte(nil)
	if result.Failure != nil {
		failure := *result.Failure
		failure.Message = "embedding provider reconciliation failure"
		failureJSON, err = json.Marshal(failure)
		if err != nil {
			return err
		}
	}
	if err := setWorkspaceContext(ctx, tx, result.WorkspaceID); err != nil {
		return err
	}
	var currentStatus, sourceHash, provider, model, storedProviderRequestID, storedVectorHash string
	var requestHash string
	var taskJSON []byte
	var storedOwner string
	var storedFence int64
	var actorJSON []byte
	if err := tx.QueryRow(ctx, `
		SELECT status, source_hash, request_hash, provider, model, provider_request_id,
		       vector_hash, task_ref, task_owner_id, task_fence, actor
		FROM fornix.embedding_calls
		WHERE workspace_id=$1 AND request_id=$2
		FOR UPDATE`, result.WorkspaceID, result.RequestID).Scan(
		&currentStatus, &sourceHash, &requestHash, &provider, &model, &storedProviderRequestID,
		&storedVectorHash, &taskJSON, &storedOwner, &storedFence, &actorJSON); errors.Is(err, pgx.ErrNoRows) {
		return ErrEmbeddingCallMissing
	} else if err != nil {
		return fmt.Errorf("lock embedding call for recovery resolution: %w", err)
	}
	if currentStatus != contracts.EmbeddingCallRecoveryRequired {
		if currentStatus == result.Status && storedVectorHash == vectorHash && storedProviderRequestID == result.ProviderRequestID {
			return nil
		}
		return fmt.Errorf("%w: status=%s", ErrEmbeddingCallRecoveryRequired, currentStatus)
	}
	if sourceHash != result.SourceHash || strings.ToLower(provider) != strings.ToLower(result.Provider.Provider) || model != result.Provider.Model {
		return fmt.Errorf("%w: provider or source identity differs", ErrEmbeddingReconciliationConflict)
	}
	if storedProviderRequestID != "" && storedProviderRequestID != result.ProviderRequestID {
		return fmt.Errorf("%w: provider request identity differs", ErrEmbeddingReconciliationConflict)
	}
	task, err := decodeEntityRef(taskJSON)
	if err != nil {
		return err
	}
	owner := result.TaskOwnerID
	fence := result.TaskFence
	if owner == "" {
		owner = storedOwner
	}
	if fence == 0 && storedFence > 0 {
		fence = uint64(storedFence)
	}
	if requireTaskFence {
		if err := validateEmbeddingTaskFenceTx(ctx, tx, result.WorkspaceID, task, owner, fence); err != nil {
			return err
		}
	}
	if len(actorJSON) == 0 {
		actorJSON = []byte(`{}`)
	}
	resultHash := vectorHash
	if resultHash == "" {
		resultHash = contracts.HashStrings("embedding-reconciliation", requestHash, result.ProviderRequestID, result.Status)
	}
	attemptKey := "embedding-reconcile:" + result.RequestID + ":" + result.ProviderRequestID
	var audit EmbeddingRecoveryAuditFacts
	if len(auditFacts) > 0 {
		audit = auditFacts[0]
	}
	if audit.RequestHash == "" {
		audit.RequestHash = requestHash
	}
	insertedAudit, err := tx.Exec(ctx, `
		INSERT INTO fornix.embedding_call_reconciliations(
			workspace_id,request_id,attempt_key,provider,provider_request_id,source_hash,vector_hash,status,result_hash,response_evidence,actor,
			operation_id,effect_id,link_id,request_hash,recovery_owner_id,recovery_fence,expected_effect_version,expected_link_version,causation_id,correlation_id)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,NULLIF($12,''),NULLIF($13,''),NULLIF($14,''),NULLIF($15,''),NULLIF($16,''),NULLIF($17,0),NULLIF($18,0),NULLIF($19,0),NULLIF($20,''),NULLIF($21,''))
		ON CONFLICT (workspace_id,attempt_key) DO NOTHING`,
		result.WorkspaceID, result.RequestID, attemptKey, provider, result.ProviderRequestID,
		sourceHash, vectorHash, result.Status, resultHash, boundedEmbeddingEvidence(result.ResponseEvidence, contracts.EmbeddingRequest{WorkspaceID: result.WorkspaceID, RequestID: result.RequestID, SourceHash: sourceHash}, requestHash), actorJSON,
		audit.OperationID, audit.EffectID, audit.LinkID, audit.RequestHash, audit.RecoveryOwnerID, int64(audit.RecoveryFence), audit.ExpectedEffectVersion, audit.ExpectedLinkVersion, audit.CausationID, audit.CorrelationID)
	if err != nil {
		return fmt.Errorf("append embedding reconciliation audit: %w", err)
	}
	if insertedAudit.RowsAffected() == 0 {
		var existingProvider, existingProviderRequest, existingSource, existingVector, existingStatus, existingResult string
		if err := tx.QueryRow(ctx, `
			SELECT provider,provider_request_id,source_hash,vector_hash,status,result_hash
			FROM fornix.embedding_call_reconciliations
			WHERE workspace_id=$1 AND attempt_key=$2`, result.WorkspaceID, attemptKey).
			Scan(&existingProvider, &existingProviderRequest, &existingSource, &existingVector, &existingStatus, &existingResult); err != nil {
			return fmt.Errorf("read duplicate embedding reconciliation audit: %w", err)
		}
		if !strings.EqualFold(existingProvider, provider) || existingProviderRequest != result.ProviderRequestID || existingSource != sourceHash || existingVector != vectorHash || existingStatus != result.Status || existingResult != resultHash {
			return ErrEmbeddingReconciliationConflict
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE fornix.embedding_calls
		SET status=$1, provider_request_id=$2, usage=$3::jsonb, failure=$4::jsonb,
		    response_evidence=$5::jsonb, vector=$6, vector_hash=$7, vector_dimension=$8,
		    finished_at=clock_timestamp()
		WHERE workspace_id=$9 AND request_id=$10 AND status=$11`,
		result.Status, result.ProviderRequestID, usageJSON, nullJSON(failureJSON),
		boundedEmbeddingEvidence(result.ResponseEvidence, contracts.EmbeddingRequest{WorkspaceID: result.WorkspaceID, RequestID: result.RequestID, SourceHash: sourceHash}, requestHash),
		vectorValue, vectorHash, vectorDimension, result.WorkspaceID, result.RequestID, contracts.EmbeddingCallRecoveryRequired); err != nil {
		return fmt.Errorf("resolve embedding call recovery: %w", err)
	}
	return nil
}

// SweepExpiredQueryVectors removes only unreferenced derived query vectors.
// It preserves hashes and emits an expired tombstone, so replay cannot turn
// retention into an implicit provider retry.
func (s *EmbeddingCallStore) SweepExpiredQueryVectors(ctx context.Context, request contracts.EmbeddingRetentionSweepRequest) (contracts.EmbeddingRetentionSweepResult, error) {
	if s == nil || s.pool == nil {
		return contracts.EmbeddingRetentionSweepResult{}, fmt.Errorf("embedding call store is not configured")
	}
	request.WorkspaceID = strings.TrimSpace(request.WorkspaceID)
	if request.WorkspaceID == "" {
		return contracts.EmbeddingRetentionSweepResult{}, fmt.Errorf("workspace_id is required")
	}
	if request.Before.IsZero() {
		request.Before = time.Now().UTC()
	}
	if request.BatchSize <= 0 || request.BatchSize > 256 {
		request.BatchSize = 64
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		return contracts.EmbeddingRetentionSweepResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	rows, err := tx.Query(ctx, `
		SELECT c.request_id,c.source_hash,c.vector_hash
		FROM fornix.embedding_calls c
		WHERE c.workspace_id=$1 AND c.retention_class=$2 AND c.status=$3
		  AND c.retention_deadline <= $4 AND c.vector IS NOT NULL
		  AND NOT EXISTS (SELECT 1 FROM fornix.embedding_target_attachments a WHERE a.workspace_id=c.workspace_id AND a.request_id=c.request_id)
		ORDER BY c.retention_deadline,c.id
		LIMIT $5
		FOR UPDATE OF c SKIP LOCKED`, request.WorkspaceID, contracts.EmbeddingRetentionQuery, contracts.EmbeddingCallSucceeded, request.Before, request.BatchSize)
	if err != nil {
		return contracts.EmbeddingRetentionSweepResult{}, err
	}
	type candidate struct{ requestID, sourceHash, vectorHash string }
	var candidates []candidate
	for rows.Next() {
		var value candidate
		if err := rows.Scan(&value.requestID, &value.sourceHash, &value.vectorHash); err != nil {
			rows.Close()
			return contracts.EmbeddingRetentionSweepResult{}, err
		}
		candidates = append(candidates, value)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return contracts.EmbeddingRetentionSweepResult{}, err
	}
	rows.Close()
	result := contracts.EmbeddingRetentionSweepResult{WorkspaceID: request.WorkspaceID, BatchSize: request.BatchSize, Candidates: len(candidates), DryRun: request.DryRun}
	if request.DryRun {
		if err := tx.Commit(ctx); err != nil {
			return contracts.EmbeddingRetentionSweepResult{}, err
		}
		return result, nil
	}
	for _, value := range candidates {
		tombstone := contracts.HashStrings("embedding-query-expired", request.WorkspaceID, value.requestID, value.sourceHash, value.vectorHash)
		if _, err := tx.Exec(ctx, `
			UPDATE fornix.embedding_calls
			SET status=$1, vector=NULL, vector_dimension=0, expired_at=clock_timestamp(), tombstone_hash=$2
			WHERE workspace_id=$3 AND request_id=$4 AND status=$5`, contracts.EmbeddingCallExpired, tombstone, request.WorkspaceID, value.requestID, contracts.EmbeddingCallSucceeded); err != nil {
			return contracts.EmbeddingRetentionSweepResult{}, err
		}
		result.Expired++
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.EmbeddingRetentionSweepResult{}, err
	}
	return result, nil
}

// Attach records a successful embedding's relationship to a derived target in
// its own short transaction. Callers that already own a target transaction
// should use AttachTx so the target row and lineage link commit together.
func (s *EmbeddingCallStore) Attach(ctx context.Context, attachment contracts.EmbeddingTargetAttachment) error {
	if s == nil || s.pool == nil {
		return fmt.Errorf("embedding call store is not configured")
	}
	attachment, err := normalizeEmbeddingAttachment(attachment)
	if err != nil {
		return err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, attachment.WorkspaceID)
	if err != nil {
		return fmt.Errorf("begin embedding target attachment: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := s.AttachTx(ctx, tx, attachment); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// AttachTx verifies that the ledger has a successful result whose source and
// vector hashes match the target projection, then inserts an immutable link.
// It is deliberately idempotent for the same request/target identity and
// rejects a different lineage rather than overwriting history.
func (s *EmbeddingCallStore) AttachTx(ctx context.Context, tx pgx.Tx, attachment contracts.EmbeddingTargetAttachment) error {
	if s == nil {
		return fmt.Errorf("embedding call store is not configured")
	}
	attachment, err := normalizeEmbeddingAttachment(attachment)
	if err != nil {
		return err
	}
	var status, sourceHash, vectorHash string
	if err := tx.QueryRow(ctx, `
		SELECT status, source_hash, vector_hash
		FROM fornix.embedding_calls
		WHERE workspace_id=$1 AND request_id=$2
		FOR SHARE`, attachment.WorkspaceID, attachment.RequestID).Scan(&status, &sourceHash, &vectorHash); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrEmbeddingCallMissing
		}
		return fmt.Errorf("read embedding call for attachment: %w", err)
	}
	if status != contracts.EmbeddingCallSucceeded {
		return fmt.Errorf("embedding call is not successfully replayable: %s", status)
	}
	if sourceHash != attachment.SourceHash || vectorHash != attachment.VectorHash {
		return fmt.Errorf("%w: embedding hashes differ", ErrEmbeddingAttachmentConflict)
	}
	inserted, err := tx.Exec(ctx, `
		INSERT INTO fornix.embedding_target_attachments(workspace_id,request_id,target_kind,target_id,source_hash,vector_hash)
		VALUES($1,$2,$3,$4,$5,$6)
		ON CONFLICT (workspace_id,request_id,target_kind,target_id) DO NOTHING`,
		attachment.WorkspaceID, attachment.RequestID, attachment.TargetKind, attachment.TargetID, attachment.SourceHash, attachment.VectorHash)
	if err != nil {
		return fmt.Errorf("insert embedding target attachment: %w", err)
	}
	if inserted.RowsAffected() == 1 {
		return nil
	}
	var existingSource, existingVector string
	if err := tx.QueryRow(ctx, `
		SELECT source_hash, vector_hash
		FROM fornix.embedding_target_attachments
		WHERE workspace_id=$1 AND request_id=$2 AND target_kind=$3 AND target_id=$4`,
		attachment.WorkspaceID, attachment.RequestID, attachment.TargetKind, attachment.TargetID).Scan(&existingSource, &existingVector); err != nil {
		return fmt.Errorf("read duplicate embedding target attachment: %w", err)
	}
	if existingSource != attachment.SourceHash || existingVector != attachment.VectorHash {
		return fmt.Errorf("%w: duplicate target attachment differs", ErrEmbeddingAttachmentConflict)
	}
	return nil
}

func normalizeEmbeddingAttachment(attachment contracts.EmbeddingTargetAttachment) (contracts.EmbeddingTargetAttachment, error) {
	attachment.WorkspaceID = strings.TrimSpace(attachment.WorkspaceID)
	attachment.RequestID = strings.TrimSpace(attachment.RequestID)
	attachment.TargetKind = strings.ToLower(strings.TrimSpace(attachment.TargetKind))
	attachment.TargetID = strings.TrimSpace(attachment.TargetID)
	attachment.SourceHash = strings.ToLower(strings.TrimSpace(attachment.SourceHash))
	attachment.VectorHash = strings.ToLower(strings.TrimSpace(attachment.VectorHash))
	if attachment.WorkspaceID == "" || attachment.RequestID == "" || attachment.TargetKind == "" || attachment.TargetID == "" {
		return contracts.EmbeddingTargetAttachment{}, fmt.Errorf("embedding target attachment identity is required")
	}
	if len(attachment.TargetKind) > 64 || len(attachment.TargetID) > 512 || len(attachment.SourceHash) != 64 || len(attachment.VectorHash) != 64 {
		return contracts.EmbeddingTargetAttachment{}, fmt.Errorf("embedding target attachment identity is bounded")
	}
	for _, value := range []string{attachment.SourceHash, attachment.VectorHash} {
		decoded, err := hex.DecodeString(value)
		if err != nil || len(decoded) != sha256.Size {
			return contracts.EmbeddingTargetAttachment{}, fmt.Errorf("embedding target attachment hashes must be lowercase sha256")
		}
	}
	return attachment, nil
}

// RecoverStale moves an unfinished provider call into an explicit recovery
// state after its heartbeat window has elapsed. It never retries the provider:
// an operator or reconciler must establish the external outcome before a new
// request identity is allowed to run.
func (s *EmbeddingCallStore) RecoverStale(ctx context.Context, workspaceID, requestID string, cutoff time.Time) (contracts.EmbeddingCallRecord, error) {
	if s == nil || s.pool == nil {
		return contracts.EmbeddingCallRecord{}, fmt.Errorf("embedding call store is not configured")
	}
	workspaceID, requestID = strings.TrimSpace(workspaceID), strings.TrimSpace(requestID)
	if workspaceID == "" || requestID == "" {
		return contracts.EmbeddingCallRecord{}, fmt.Errorf("workspace_id and request_id are required")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.EmbeddingCallRecord{}, fmt.Errorf("begin embedding call recovery: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status string
	var updatedAt time.Time
	if err := tx.QueryRow(ctx, `SELECT status, updated_at FROM fornix.embedding_calls WHERE workspace_id=$1 AND request_id=$2 FOR UPDATE`, workspaceID, requestID).Scan(&status, &updatedAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.EmbeddingCallRecord{}, ErrEmbeddingCallMissing
		}
		return contracts.EmbeddingCallRecord{}, err
	}
	if status != contracts.EmbeddingCallPending && status != contracts.EmbeddingCallRunning {
		record, readErr := readEmbeddingCallTx(ctx, tx, workspaceID, requestID)
		if readErr != nil {
			return contracts.EmbeddingCallRecord{}, readErr
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.EmbeddingCallRecord{}, err
		}
		return record, nil
	}
	if !updatedAt.Before(cutoff) {
		return contracts.EmbeddingCallRecord{}, ErrEmbeddingCallNotStale
	}
	failure := contracts.EmbeddingFailure{Code: contracts.EmbeddingFailureProvider, Message: "embedding call became stale before durable completion", PossiblyStarted: status == contracts.EmbeddingCallRunning}
	failureJSON, _ := json.Marshal(failure)
	if _, err := tx.Exec(ctx, `
		UPDATE fornix.embedding_calls
		SET status=$1, failure=$2::jsonb, finished_at=clock_timestamp()
		WHERE workspace_id=$3 AND request_id=$4 AND status IN ($5,$6)`,
		contracts.EmbeddingCallRecoveryRequired, failureJSON, workspaceID, requestID, contracts.EmbeddingCallPending, contracts.EmbeddingCallRunning); err != nil {
		return contracts.EmbeddingCallRecord{}, fmt.Errorf("mark stale embedding call: %w", err)
	}
	record, err := readEmbeddingCallTx(ctx, tx, workspaceID, requestID)
	if err != nil {
		return contracts.EmbeddingCallRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.EmbeddingCallRecord{}, err
	}
	return record, nil
}

func validateEmbeddingTaskFenceTx(ctx context.Context, tx pgx.Tx, workspaceID string, task *contracts.EntityRef, ownerID string, fence uint64) error {
	if task == nil {
		if ownerID != "" || fence != 0 {
			return fmt.Errorf("%w: task fence supplied without a task", ErrEmbeddingCallLeaseFenced)
		}
		return nil
	}
	if !strings.EqualFold(strings.TrimSpace(task.Kind), "task") || strings.TrimSpace(task.WorkspaceID) != workspaceID {
		return fmt.Errorf("%w: task reference is outside workspace", ErrEmbeddingCallLeaseFenced)
	}
	taskID, err := strconv.ParseInt(strings.TrimSpace(task.ID), 10, 64)
	if err != nil || taskID <= 0 || strings.TrimSpace(ownerID) == "" || fence == 0 || fence > uint64(^uint64(0)>>1) {
		return fmt.Errorf("%w: task owner, numeric task id, and fence are required", ErrEmbeddingCallLeaseFenced)
	}
	var currentOwner string
	var currentFence int64
	var releasedAt *time.Time
	var active bool
	if err := tx.QueryRow(ctx, `SELECT owner_id, fence, released_at, (released_at IS NULL AND lease_until > clock_timestamp()) FROM fornix.task_execution_leases WHERE workspace_id=$1 AND task_id=$2 FOR UPDATE`, workspaceID, taskID).Scan(&currentOwner, &currentFence, &releasedAt, &active); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: task lease missing", ErrEmbeddingCallLeaseFenced)
		}
		return err
	}
	if currentOwner != ownerID || currentFence != int64(fence) || releasedAt != nil || !active {
		return fmt.Errorf("%w: expected owner=%s fence=%d", ErrEmbeddingCallLeaseFenced, ownerID, fence)
	}
	return nil
}

func maxInt64(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func embeddingRetentionForSource(sourceKind string) (string, *time.Time) {
	sourceKind = strings.ToLower(strings.TrimSpace(sourceKind))
	if sourceKind != "memo_query" && sourceKind != "symbol_query" && sourceKind != "rag_query" {
		return contracts.EmbeddingRetentionAuthoritative, nil
	}
	deadline := time.Now().UTC().Add(24 * time.Hour)
	return contracts.EmbeddingRetentionQuery, &deadline
}

func (s *EmbeddingCallStore) Get(ctx context.Context, workspaceID, requestID string) (contracts.EmbeddingCallRecord, error) {
	if s == nil || s.pool == nil {
		return contracts.EmbeddingCallRecord{}, fmt.Errorf("embedding call store is not configured")
	}
	workspaceID, requestID = strings.TrimSpace(workspaceID), strings.TrimSpace(requestID)
	if workspaceID == "" || requestID == "" {
		return contracts.EmbeddingCallRecord{}, fmt.Errorf("workspace_id and request_id are required")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.EmbeddingCallRecord{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	record, err := readEmbeddingCallTx(ctx, tx, workspaceID, requestID)
	if err != nil {
		return contracts.EmbeddingCallRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.EmbeddingCallRecord{}, err
	}
	return record, nil
}

// RecordQueryUse appends one hash-only retrieval attribution row. The
// canonical embedding call remains the provider-cost authority; this row
// records whether this actor/route reused it and is idempotent by request.
func (s *EmbeddingCallStore) RecordQueryUse(ctx context.Context, use contracts.EmbeddingQueryUse) (bool, error) {
	if s == nil || s.pool == nil {
		return false, fmt.Errorf("embedding call store is not configured")
	}
	if err := use.Normalize(); err != nil {
		return false, err
	}
	providerJSON, err := json.Marshal(use.Provider)
	if err != nil {
		return false, err
	}
	actorJSON, err := json.Marshal(use.Actor)
	if err != nil {
		return false, err
	}
	usageJSON, err := json.Marshal(use.Usage)
	if err != nil {
		return false, err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, use.WorkspaceID)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	inserted, err := tx.Exec(ctx, `
		INSERT INTO fornix.embedding_query_uses(
			schema_version,workspace_id,idempotency_key,request_id,embedding_request_id,source_hash,provider,actor,route,gate_reason,cache_hit,duplicate_work,usage,cost_usd,cost_known,usage_measured,usage_estimated)
		VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8::jsonb,$9,$10,$11,$12,$13::jsonb,$14,$15,$16,$17)
		ON CONFLICT (workspace_id,idempotency_key) DO NOTHING`,
		use.SchemaVersion, use.WorkspaceID, use.IdempotencyKey, use.RequestID, use.EmbeddingRequestID, use.SourceHash,
		providerJSON, actorJSON, use.Route, use.GateReason, use.CacheHit, use.DuplicateWork, usageJSON, use.CostUSD,
		use.CostKnown, use.UsageMeasured, use.UsageEstimated)
	if err != nil {
		return false, fmt.Errorf("insert embedding query use: %w", err)
	}
	if inserted.RowsAffected() == 0 {
		var sourceHash, route, gateReason, embeddingRequestID string
		if err := tx.QueryRow(ctx, `SELECT source_hash,route,gate_reason,embedding_request_id FROM fornix.embedding_query_uses WHERE workspace_id=$1 AND idempotency_key=$2`, use.WorkspaceID, use.IdempotencyKey).Scan(&sourceHash, &route, &gateReason, &embeddingRequestID); err != nil {
			return false, err
		}
		if sourceHash != use.SourceHash || route != use.Route || gateReason != use.GateReason || embeddingRequestID != use.EmbeddingRequestID {
			return false, ErrEmbeddingQueryUseConflict
		}
		if err := tx.Commit(ctx); err != nil {
			return false, err
		}
		return true, nil
	}
	if err := tx.Commit(ctx); err != nil {
		return false, err
	}
	return false, nil
}

// GetQueryCanonical resolves the workspace-scoped reusable query call by its
// source/provider/model identity. It is needed because a cross-route cache hit
// intentionally does not create a second embedding_calls row.
func (s *EmbeddingCallStore) GetQueryCanonical(ctx context.Context, workspaceID, sourceHash, provider, model string) (contracts.EmbeddingCallRecord, error) {
	if s == nil || s.pool == nil {
		return contracts.EmbeddingCallRecord{}, fmt.Errorf("embedding call store is not configured")
	}
	workspaceID, sourceHash, provider, model = strings.TrimSpace(workspaceID), strings.ToLower(strings.TrimSpace(sourceHash)), strings.ToLower(strings.TrimSpace(provider)), strings.TrimSpace(model)
	if workspaceID == "" || len(sourceHash) != 64 || provider == "" || model == "" {
		return contracts.EmbeddingCallRecord{}, fmt.Errorf("query embedding identity is required")
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.EmbeddingCallRecord{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var requestID string
	if err := tx.QueryRow(ctx, `
		SELECT request_id FROM fornix.embedding_calls
		WHERE workspace_id=$1 AND source_hash=$2 AND provider=$3 AND model=$4
		  AND retention_class=$5 AND status IN ($6,$7,$8,$9)
		ORDER BY id LIMIT 1`, workspaceID, sourceHash, provider, model, contracts.EmbeddingRetentionQuery,
		contracts.EmbeddingCallPending, contracts.EmbeddingCallRunning, contracts.EmbeddingCallSucceeded, contracts.EmbeddingCallRecoveryRequired).Scan(&requestID); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.EmbeddingCallRecord{}, ErrEmbeddingCallMissing
		}
		return contracts.EmbeddingCallRecord{}, err
	}
	record, err := readEmbeddingCallTx(ctx, tx, workspaceID, requestID)
	if err != nil {
		return contracts.EmbeddingCallRecord{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.EmbeddingCallRecord{}, err
	}
	return record, nil
}

func readEmbeddingCallTx(ctx context.Context, tx pgx.Tx, workspaceID, key string) (contracts.EmbeddingCallRecord, error) {
	var record contracts.EmbeddingCallRecord
	var actorJSON, taskJSON, sessionJSON, metadataJSON, usageJSON, failureJSON, requestEvidence, responseEvidence []byte
	var vectorText *string
	err := tx.QueryRow(ctx, `
		SELECT id, workspace_id, request_id, idempotency_key, request_hash, schema_version,
			causation_id, correlation_id, source_kind, source_id, source_hash,
			provider, endpoint, model, actor, task_ref, session_ref, task_owner_id, task_fence,
			metadata, status, attempt_count, provider_request_id, input_bytes,
			budget_max_input_bytes, budget_dimension, budget_timeout_ms, max_cost_usd,
			usage, failure, request_evidence, response_evidence, vector::text,
			vector_hash, vector_dimension, retention_class, retention_deadline, expired_at, tombstone_hash,
			created_at, started_at, finished_at, duration_ms, updated_at
		FROM fornix.embedding_calls
		WHERE workspace_id=$1 AND (request_id=$2 OR idempotency_key=$2)
		ORDER BY id LIMIT 1`, workspaceID, key).Scan(
		&record.ID, &record.WorkspaceID, &record.RequestID, &record.IdempotencyKey, &record.RequestHash, &record.SchemaVersion,
		&record.CausationID, &record.CorrelationID, &record.SourceKind, &record.SourceID, &record.SourceHash,
		&record.Provider.Provider, &record.Provider.Endpoint, &record.Provider.Model, &actorJSON, &taskJSON, &sessionJSON,
		&record.TaskOwnerID, &record.TaskFence, &metadataJSON, &record.Status, &record.AttemptCount, &record.ProviderRequestID, &record.Usage.InputBytes,
		&record.Budget.MaxInputBytes, &record.Budget.Dimension, &record.Budget.TimeoutMS, &record.Budget.MaxCostUSD,
		&usageJSON, &failureJSON, &requestEvidence, &responseEvidence, &vectorText,
		&record.VectorHash, &record.VectorDimension, &record.RetentionClass, &record.RetentionDeadline, &record.ExpiredAt, &record.TombstoneHash,
		&record.CreatedAt, &record.StartedAt, &record.FinishedAt, &record.DurationMS, &record.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.EmbeddingCallRecord{}, ErrEmbeddingCallMissing
	}
	if err != nil {
		return contracts.EmbeddingCallRecord{}, fmt.Errorf("read embedding call: %w", err)
	}
	if err := json.Unmarshal(actorJSON, &record.Actor); err != nil {
		return contracts.EmbeddingCallRecord{}, fmt.Errorf("decode embedding actor: %w", err)
	}
	record.Task, err = decodeEntityRef(taskJSON)
	if err != nil {
		return contracts.EmbeddingCallRecord{}, err
	}
	record.Session, err = decodeEntityRef(sessionJSON)
	if err != nil {
		return contracts.EmbeddingCallRecord{}, err
	}
	if len(metadataJSON) > 0 && string(metadataJSON) != "null" {
		if err := json.Unmarshal(metadataJSON, &record.Metadata); err != nil {
			return contracts.EmbeddingCallRecord{}, err
		}
	}
	if len(usageJSON) > 0 && string(usageJSON) != "null" {
		if err := json.Unmarshal(usageJSON, &record.Usage); err != nil {
			return contracts.EmbeddingCallRecord{}, err
		}
	}
	if len(failureJSON) > 0 && string(failureJSON) != "null" {
		record.Failure = &contracts.EmbeddingFailure{}
		if err := json.Unmarshal(failureJSON, record.Failure); err != nil {
			return contracts.EmbeddingCallRecord{}, err
		}
	}
	record.RequestEvidence = append([]byte(nil), requestEvidence...)
	if len(responseEvidence) > 0 {
		record.ResponseEvidence = append([]byte(nil), responseEvidence...)
	}
	if vectorText != nil && strings.TrimSpace(*vectorText) != "" {
		parsed := pgvector.Vector{}
		if err := parsed.Parse(*vectorText); err != nil {
			return contracts.EmbeddingCallRecord{}, fmt.Errorf("decode embedding vector: %w", err)
		}
		record.Vector = append([]float32(nil), parsed.Slice()...)
	}
	return record, nil
}

func boundedEmbeddingEvidence(raw []byte, request contracts.EmbeddingRequest, requestHash string) []byte {
	summary := map[string]any{
		"schema_version": contracts.EmbeddingSchemaVersion,
		"workspace_id":   request.WorkspaceID,
		"request_id":     request.RequestID,
		"source_kind":    request.SourceKind,
		"source_id":      request.SourceID,
		"source_hash":    request.SourceHash,
		"request_hash":   requestHash,
		"input_bytes":    len([]byte(request.Text)),
	}
	if len(raw) > 0 {
		digest := sha256.Sum256(raw)
		summary["evidence_hash"] = hex.EncodeToString(digest[:])
	}
	encoded, _ := json.Marshal(summary)
	return encoded
}

func redactedEmbeddingMetadata(metadata map[string]string) map[string]string {
	if len(metadata) == 0 {
		return map[string]string{}
	}
	redacted := make(map[string]string, len(metadata))
	for key := range metadata {
		redacted[key] = "[REDACTED]"
	}
	return redacted
}

// Keep database/sql linked in this file so pgx's nullable pointer behavior is
// checked by compile-time tooling on supported database/sql scanners.
var _ = sql.NullString{}
