package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrWorkReceiptNotFound  = errors.New("work receipt not found")
	ErrWorkReceiptConflict  = errors.New("work receipt identity conflict")
	ErrWorkReceiptIntegrity = errors.New("work receipt integrity check failed")
	ErrWorkReceiptStale     = errors.New("work receipt task or run fence is stale")
	ErrWorkReceiptWorkspace = errors.New("work receipt workspace violation")
)

// WorkReceiptStore is the Postgres authority for immutable derived work
// receipts. It validates existing authorities and writes the receipt plus all
// normalized links in one transaction.
type WorkReceiptStore struct {
	pool        *pgxpool.Pool
	failureHook func(string) error
}

// NewWorkReceiptStore creates a receipt store over the shared Postgres pool.
func NewWorkReceiptStore(pool *pgxpool.Pool) *WorkReceiptStore {
	return &WorkReceiptStore{pool: pool}
}

// SetFailureHook is a deterministic test seam for proving that a crash before
// commit leaves no receipt or partial step/reference links.
func (s *WorkReceiptStore) SetFailureHook(hook func(string) error) {
	if s != nil {
		s.failureHook = hook
	}
}

// Finalize validates and durably commits one verified receipt. A duplicate
// natural identity or idempotency key returns the existing immutable receipt;
// a changed request hash fails closed.
func (s *WorkReceiptStore) Finalize(ctx context.Context, request contracts.WorkReceiptFinalizeRequest) (contracts.WorkReceipt, bool, error) {
	if s == nil || s.pool == nil {
		return contracts.WorkReceipt{}, false, fmt.Errorf("work receipt store is not configured")
	}
	receipt, err := request.ToReceipt(time.Now().UTC())
	if err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.WorkReceipt{}, false, fmt.Errorf("begin work receipt finalization: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	stored, inserted, err := s.finalizeTx(ctx, tx, receipt)
	if err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.WorkReceipt{}, false, fmt.Errorf("commit work receipt: %w", err)
	}
	return stored, inserted, nil
}

// FinalizeTx verifies and stages a receipt in an existing transaction. The
// caller owns the transaction commit or rollback. This allows a receipt to be
// part of a larger authoritative mutation without a second commit boundary.
func (s *WorkReceiptStore) FinalizeTx(ctx context.Context, tx pgx.Tx, request contracts.WorkReceiptFinalizeRequest) (contracts.WorkReceipt, bool, error) {
	if s == nil || tx == nil {
		return contracts.WorkReceipt{}, false, fmt.Errorf("work receipt transaction is not configured")
	}
	receipt, err := request.ToReceipt(time.Now().UTC())
	if err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	return s.finalizeTx(ctx, tx, receipt)
}

func (s *WorkReceiptStore) finalizeTx(ctx context.Context, tx pgx.Tx, receipt contracts.WorkReceipt) (contracts.WorkReceipt, bool, error) {
	if err := receipt.Normalize(); err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	if err := s.validateAuthoritativeWorkTx(ctx, tx, &receipt); err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	if err := s.validateReferencesTx(ctx, tx, &receipt); err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	if err := s.validateTypedLinksTx(ctx, tx, receipt); err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	// Reference counts and integrity outcomes are part of the immutable
	// verification snapshot, so derive hashes only after validation completes.
	receipt.CanonicalHash = receipt.StableHash()
	receipt.Verification.ReceiptHash = receipt.CanonicalHash
	receipt.RequestHash = receipt.RequestContentHash()
	payload, err := receipt.CanonicalJSON()
	if err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	actorJSON, err := json.Marshal(receipt.Actor)
	if err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	taskJSON, err := jsonOrEmpty(receipt.Task)
	if err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	sessionJSON, err := jsonOrEmpty(receipt.Session)
	if err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	costJSON, err := json.Marshal(receipt.Cost)
	if err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	verificationJSON, err := json.Marshal(receipt.Verification)
	if err != nil {
		return contracts.WorkReceipt{}, false, err
	}

	inserted, err := tx.Exec(ctx, `
		INSERT INTO fornix.work_receipts(
			id, workspace_id, work_kind, work_id, request_id, idempotency_key,
			request_hash, canonical_hash, status, actor, task_ref, session_ref,
			task_owner_id, task_fence, source_manifest_hash, replay_hash, cost,
			verification, canonical_payload, policy_id, policy_version, policy_hash, created_at, verified_at
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb,$11::jsonb,$12::jsonb,
			$13,$14,$15,$16,$17::jsonb,$18::jsonb,$19,$20,$21,$22,$23,$24)
		ON CONFLICT DO NOTHING`, receipt.ID, receipt.WorkspaceID, receipt.WorkKind,
		receipt.WorkID, receipt.RequestID, receipt.IdempotencyKey, receipt.RequestHash,
		receipt.CanonicalHash, receipt.Status, actorJSON, taskJSON, sessionJSON,
		receipt.TaskOwnerID, int64(receipt.TaskFence), receipt.SourceManifestHash,
		receipt.ReplayHash, costJSON, verificationJSON, payload, policyID(receipt.Policy), policyVersion(receipt.Policy), policyHash(receipt.Policy), receipt.CreatedAt, receipt.VerifiedAt)
	if err != nil {
		return contracts.WorkReceipt{}, false, fmt.Errorf("insert work receipt: %w", err)
	}
	if inserted.RowsAffected() == 0 {
		existing, readErr := readWorkReceiptByIdentityTx(ctx, tx, receipt.WorkspaceID, receipt.WorkKind, receipt.WorkID, receipt.IdempotencyKey)
		if readErr != nil {
			return contracts.WorkReceipt{}, false, readErr
		}
		if existing.RequestHash != receipt.RequestHash || existing.CanonicalHash != receipt.CanonicalHash {
			return contracts.WorkReceipt{}, false, fmt.Errorf("%w: receipt identity already has a different canonical request", ErrWorkReceiptConflict)
		}
		return existing, false, nil
	}
	if err := s.fail("receipt_inserted"); err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	for _, step := range receipt.Steps {
		roles, _ := json.Marshal(step.ReferenceRoles)
		metadata, _ := json.Marshal(step.Metadata)
		if _, err := tx.Exec(ctx, `
			INSERT INTO fornix.work_receipt_steps(
				workspace_id, receipt_id, ordinal, step_id, name, kind, status,
				source_kind, source_id, source_hash, input_hash, output_hash,
				reference_roles, duration_ms, attempts, retry_count, duplicate_work,
				external_effect, external_boundary, metadata
			) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13::jsonb,$14,$15,$16,$17,$18,$19,$20::jsonb)`,
			receipt.WorkspaceID, receipt.ID, step.Ordinal, step.ID, step.Name, step.Kind,
			step.Status, step.SourceKind, step.SourceID, step.SourceHash, step.InputHash,
			step.OutputHash, roles, step.DurationMS, step.Attempts, step.RetryCount,
			step.DuplicateWork, step.ExternalEffect, step.ExternalBoundary, metadata); err != nil {
			return contracts.WorkReceipt{}, false, fmt.Errorf("insert work receipt step %d: %w", step.Ordinal, err)
		}
	}
	for ordinal, ref := range receipt.References {
		if _, err := tx.Exec(ctx, `
			INSERT INTO fornix.work_receipt_references(
				workspace_id, receipt_id, ordinal, reference_kind, source_id, role, source_hash
			) VALUES($1,$2,$3,$4,$5,$6,$7)`, receipt.WorkspaceID, receipt.ID, ordinal,
			ref.Kind, ref.SourceID, ref.Role, ref.Hash); err != nil {
			return contracts.WorkReceipt{}, false, fmt.Errorf("insert work receipt reference %d: %w", ordinal, err)
		}
	}
	if err := s.fail("links_inserted"); err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	stored, err := readWorkReceiptByIDTx(ctx, tx, receipt.WorkspaceID, receipt.ID)
	if err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	if err := s.fail("before_commit"); err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	return stored, true, nil
}

// Get returns only a receipt from the requested workspace. The database key
// and payload are both checked so a corrupted or cross-workspace row fails
// closed.
func (s *WorkReceiptStore) Get(ctx context.Context, workspaceID, receiptID string) (contracts.WorkReceipt, error) {
	if s == nil || s.pool == nil {
		return contracts.WorkReceipt{}, fmt.Errorf("work receipt store is not configured")
	}
	receipt, err := readWorkReceiptByIDTx(ctx, s.pool, strings.TrimSpace(workspaceID), strings.TrimSpace(receiptID))
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.WorkReceipt{}, ErrWorkReceiptNotFound
	}
	return receipt, err
}

// Disclose returns a bounded gist, detail, or redacted canonical JSON view.
// The authoritative canonical hash is included in every view.
func (s *WorkReceiptStore) Disclose(ctx context.Context, request contracts.WorkReceiptDisclosureRequest) (contracts.WorkReceiptDisclosureResult, error) {
	normalized, err := request.Normalize()
	if err != nil {
		return contracts.WorkReceiptDisclosureResult{}, err
	}
	receipt, err := s.Get(ctx, normalized.WorkspaceID, normalized.ReceiptID)
	if err != nil {
		return contracts.WorkReceiptDisclosureResult{}, err
	}
	fullPayload, err := receipt.CanonicalJSON()
	if err != nil {
		return contracts.WorkReceiptDisclosureResult{}, fmt.Errorf("canonicalize work receipt disclosure: %w", err)
	}
	result := contracts.WorkReceiptDisclosureResult{
		SchemaVersion: contracts.WorkReceiptSchemaVersion, WorkspaceID: receipt.WorkspaceID,
		ReceiptID: receipt.ID, Level: normalized.Level, CanonicalHash: receipt.CanonicalHash,
		TotalBytes: len(fullPayload), TotalItems: len(receipt.Steps) + len(receipt.References) + len(receipt.Evidence) + len(receipt.Artifacts),
	}
	maxBytes := normalized.MaxBytes
	if tokenBytes := normalized.MaxTokens * 4; tokenBytes < maxBytes {
		maxBytes = tokenBytes
	}
	switch normalized.Level {
	case contracts.WorkReceiptDisclosureGist:
		gist := &contracts.WorkReceiptGist{ID: receipt.ID, WorkspaceID: receipt.WorkspaceID, WorkKind: receipt.WorkKind, WorkID: receipt.WorkID, Status: receipt.Status, CanonicalHash: receipt.CanonicalHash, SourceManifestHash: receipt.SourceManifestHash, ReplayHash: receipt.ReplayHash, StepCount: len(receipt.Steps), ReferenceCount: len(receipt.References)}
		view, _ := json.Marshal(gist)
		if len(view) > maxBytes {
			view = view[:maxBytes]
			result.Truncated = true
		}
		result.Gist = gist
		result.TotalTokens = tokenEstimate(len(view))
		result.ContentViewHash = receiptViewHash(view)
	case contracts.WorkReceiptDisclosureDetail:
		bounded := boundedReceipt(receipt, normalized.MaxItems)
		view, marshalErr := bounded.CanonicalJSON()
		if marshalErr != nil || len(view) > maxBytes {
			result.Truncated = true
			bounded = minimalReceipt(receipt)
			view, _ = bounded.CanonicalJSON()
			if len(view) > maxBytes {
				view = view[:maxBytes]
			}
		}
		result.Detail = &bounded
		result.TotalItems = len(bounded.Steps) + len(bounded.References) + len(bounded.Evidence) + len(bounded.Artifacts)
		result.TotalTokens = tokenEstimate(len(view))
		result.ContentViewHash = receiptViewHash(view)
	case contracts.WorkReceiptDisclosureRaw:
		view := fullPayload
		if len(view) > maxBytes {
			view = view[:maxBytes]
			result.Truncated = true
		}
		result.Raw = append(json.RawMessage(nil), view...)
		result.TotalTokens = tokenEstimate(len(view))
		result.ContentViewHash = receiptViewHash(view)
	}
	return result, nil
}

func (s *WorkReceiptStore) fail(stage string) error {
	if s != nil && s.failureHook != nil {
		return s.failureHook(stage)
	}
	return nil
}

func (s *WorkReceiptStore) validateAuthoritativeWorkTx(ctx context.Context, tx pgx.Tx, receipt *contracts.WorkReceipt) error {
	if receipt.Task != nil {
		taskID, err := strconv.ParseInt(strings.TrimSpace(receipt.Task.ID), 10, 64)
		if err != nil || taskID <= 0 {
			return fmt.Errorf("%w: task reference is invalid", ErrWorkReceiptIntegrity)
		}
		var status string
		var fence int64
		if err := tx.QueryRow(ctx, `SELECT status, execution_fence FROM fornix.tasks WHERE workspace_id=$1 AND id=$2 FOR SHARE`, receipt.WorkspaceID, taskID).Scan(&status, &fence); errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: task is missing", ErrWorkReceiptIntegrity)
		} else if err != nil {
			return fmt.Errorf("validate receipt task: %w", err)
		} else if status != contracts.TaskStatusDone || fence <= 0 || uint64(fence) != receipt.TaskFence {
			return ErrWorkReceiptStale
		}
		if receipt.WorkKind == contracts.WorkReceiptReferenceTask && receipt.WorkID != strconv.FormatInt(taskID, 10) {
			return fmt.Errorf("%w: work_id does not match task", ErrWorkReceiptIntegrity)
		}
	}
	if receipt.WorkKind == contracts.WorkReceiptReferenceAgentRun {
		var state, stateHash, owner string
		var fence int64
		if err := tx.QueryRow(ctx, `SELECT state, state_hash, task_owner_id, task_fence FROM fornix.agent_runs WHERE workspace_id=$1 AND id=$2 FOR SHARE`, receipt.WorkspaceID, receipt.WorkID).Scan(&state, &stateHash, &owner, &fence); errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: agent run is missing", ErrWorkReceiptIntegrity)
		} else if err != nil {
			return fmt.Errorf("validate receipt agent run: %w", err)
		} else if state != contracts.AgentRunSucceeded {
			return fmt.Errorf("%w: agent run is not succeeded", ErrWorkReceiptIntegrity)
		} else if receipt.ReplayHash != "" && receipt.ReplayHash != strings.ToLower(stateHash) {
			return fmt.Errorf("%w: replay hash does not match agent state", ErrWorkReceiptIntegrity)
		} else if receipt.Task != nil && (owner != receipt.TaskOwnerID || fence <= 0 || uint64(fence) != receipt.TaskFence) {
			return ErrWorkReceiptStale
		}
	}
	if receipt.WorkKind == contracts.WorkReceiptReferenceChangeApplication {
		var status, packetHash string
		if err := tx.QueryRow(ctx, `SELECT status, packet_hash FROM fornix.change_applications WHERE workspace_id=$1 AND id=$2 FOR SHARE`, receipt.WorkspaceID, receipt.WorkID).Scan(&status, &packetHash); errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: change application is missing", ErrWorkReceiptIntegrity)
		} else if err != nil {
			return fmt.Errorf("validate receipt change application: %w", err)
		} else if status != contracts.ChangeApplied || (receipt.ReplayHash != "" && receipt.ReplayHash != packetHash) {
			return fmt.Errorf("%w: change application is not verified", ErrWorkReceiptIntegrity)
		}
	}
	if receipt.WorkKind == contracts.WorkReceiptReferenceChangeProposal {
		var status, packetHash string
		if err := tx.QueryRow(ctx, `SELECT status, packet_hash FROM fornix.change_proposals WHERE workspace_id=$1 AND id=$2 FOR SHARE`, receipt.WorkspaceID, receipt.WorkID).Scan(&status, &packetHash); errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: change proposal is missing", ErrWorkReceiptIntegrity)
		} else if err != nil {
			return fmt.Errorf("validate receipt change proposal: %w", err)
		} else if status != contracts.ChangeApplied || (receipt.ReplayHash != "" && receipt.ReplayHash != packetHash) {
			return fmt.Errorf("%w: change proposal is not applied", ErrWorkReceiptIntegrity)
		}
	}
	if receipt.WorkKind == contracts.WorkReceiptReferenceValidation {
		var status, reportHash, replayHash string
		if err := tx.QueryRow(ctx, `SELECT status, report_hash, replay_hash FROM fornix.validation_runs WHERE workspace_id=$1 AND id=$2 FOR SHARE`, receipt.WorkspaceID, receipt.WorkID).Scan(&status, &reportHash, &replayHash); errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: validation run is missing", ErrWorkReceiptIntegrity)
		} else if err != nil {
			return fmt.Errorf("validate receipt validation run: %w", err)
		} else if status != contracts.ValidationPassed {
			return fmt.Errorf("%w: validation run is not passed", ErrWorkReceiptIntegrity)
		} else if receipt.ReplayHash != "" && receipt.ReplayHash != replayHash {
			return fmt.Errorf("%w: validation replay hash does not match", ErrWorkReceiptIntegrity)
		}
	}
	return nil
}

func (s *WorkReceiptStore) validateReferencesTx(ctx context.Context, tx pgx.Tx, receipt *contracts.WorkReceipt) error {
	for _, ref := range receipt.References {
		if err := validateReceiptReferenceTx(ctx, tx, receipt.WorkspaceID, ref); err != nil {
			return err
		}
	}
	receipt.Verification.ReferencesChecked = len(receipt.References)
	receipt.Verification.ReferencesResolved = len(receipt.References)
	receipt.Verification.IntegrityChecks = len(receipt.References) + len(receipt.Evidence) + len(receipt.Artifacts)
	receipt.Verification.ReceiptHash = receipt.CanonicalHash
	return nil
}

func (s *WorkReceiptStore) validateTypedLinksTx(ctx context.Context, tx pgx.Tx, receipt contracts.WorkReceipt) error {
	for _, evidence := range receipt.Evidence {
		var hash, workspace string
		if err := tx.QueryRow(ctx, `SELECT workspace_id, evidence_hash FROM fornix.evidence_records WHERE workspace_id=$1 AND id=$2 FOR SHARE`, receipt.WorkspaceID, evidence.ID).Scan(&workspace, &hash); errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: evidence %d is missing", ErrWorkReceiptIntegrity, evidence.ID)
		} else if err != nil {
			return fmt.Errorf("validate receipt evidence: %w", err)
		} else if workspace != evidence.WorkspaceID || strings.ToLower(hash) != evidence.EvidenceHash {
			return fmt.Errorf("%w: evidence %d hash mismatch", ErrWorkReceiptIntegrity, evidence.ID)
		}
	}
	for _, artifact := range receipt.Artifacts {
		var hash, integrity, workspace string
		if err := tx.QueryRow(ctx, `SELECT workspace_id, content_hash, integrity_state FROM fornix.artifacts WHERE workspace_id=$1 AND id=$2 FOR SHARE`, receipt.WorkspaceID, artifact.ArtifactID).Scan(&workspace, &hash, &integrity); errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("%w: artifact %d is missing", ErrWorkReceiptIntegrity, artifact.ArtifactID)
		} else if err != nil {
			return fmt.Errorf("validate receipt artifact: %w", err)
		} else if workspace != artifact.WorkspaceID || strings.ToLower(hash) != artifact.ContentHash || integrity == contracts.ArtifactIntegrityCorrupt {
			return fmt.Errorf("%w: artifact %d integrity mismatch", ErrWorkReceiptIntegrity, artifact.ArtifactID)
		}
		var refHash string
		if err := tx.QueryRow(ctx, `SELECT a.content_hash FROM fornix.artifact_refs r JOIN fornix.artifacts a ON a.workspace_id=r.workspace_id AND a.id=r.artifact_id WHERE r.workspace_id=$1 AND r.id=$2 AND r.artifact_id=$3`, receipt.WorkspaceID, artifact.ID, artifact.ArtifactID).Scan(&refHash); err != nil {
			return fmt.Errorf("%w: artifact reference %d is missing", ErrWorkReceiptIntegrity, artifact.ID)
		}
		if strings.ToLower(refHash) != artifact.ContentHash {
			return fmt.Errorf("%w: artifact reference %d hash mismatch", ErrWorkReceiptIntegrity, artifact.ID)
		}
	}
	return nil
}

func validateReceiptReferenceTx(ctx context.Context, tx pgx.Tx, workspaceID string, ref contracts.WorkReceiptReference) error {
	var sourceHash string
	var found bool
	var err error
	switch ref.Kind {
	case contracts.WorkReceiptReferenceTask:
		var id int64
		id, err = strconv.ParseInt(ref.SourceID, 10, 64)
		if err == nil {
			err = tx.QueryRow(ctx, `SELECT true FROM fornix.tasks WHERE workspace_id=$1 AND id=$2`, workspaceID, id).Scan(&found)
		}
	case contracts.WorkReceiptReferenceAgentRun:
		err = tx.QueryRow(ctx, `SELECT true, state_hash FROM fornix.agent_runs WHERE workspace_id=$1 AND id=$2`, workspaceID, ref.SourceID).Scan(&found, &sourceHash)
	case contracts.WorkReceiptReferenceRetrievalSurface:
		var contextHash string
		err = tx.QueryRow(ctx, `SELECT true, payload_hash, context_hash FROM fornix.retrieval_surfaces WHERE workspace_id=$1 AND id=$2`, workspaceID, ref.SourceID).Scan(&found, &sourceHash, &contextHash)
		if err == nil && ref.Hash != "" && ref.Hash != strings.ToLower(sourceHash) && ref.Hash != strings.ToLower(contextHash) {
			return fmt.Errorf("%w: retrieval surface %s hash mismatch", ErrWorkReceiptIntegrity, ref.SourceID)
		}
	case contracts.WorkReceiptReferenceModelCall:
		err = tx.QueryRow(ctx, `SELECT true, request_hash FROM fornix.model_calls WHERE workspace_id=$1 AND (request_id=$2 OR id::text=$2) LIMIT 1`, workspaceID, ref.SourceID).Scan(&found, &sourceHash)
	case contracts.WorkReceiptReferenceToolRun:
		err = tx.QueryRow(ctx, `SELECT true, request_hash FROM fornix.tool_runs WHERE workspace_id=$1 AND id=$2`, workspaceID, ref.SourceID).Scan(&found, &sourceHash)
	case contracts.WorkReceiptReferenceEvidence:
		err = tx.QueryRow(ctx, `SELECT true, evidence_hash FROM fornix.evidence_records WHERE workspace_id=$1 AND id=$2`, workspaceID, ref.SourceID).Scan(&found, &sourceHash)
	case contracts.WorkReceiptReferenceArtifact:
		err = tx.QueryRow(ctx, `SELECT true, content_hash FROM fornix.artifacts WHERE workspace_id=$1 AND id=$2`, workspaceID, ref.SourceID).Scan(&found, &sourceHash)
	case contracts.WorkReceiptReferenceEvent:
		err = tx.QueryRow(ctx, `SELECT true, request_hash FROM fornix.control_events WHERE workspace_id=$1 AND event_id=$2`, workspaceID, ref.SourceID).Scan(&found, &sourceHash)
	case contracts.WorkReceiptReferenceObservation:
		err = tx.QueryRow(ctx, `SELECT true, payload_hash FROM fornix.run_observations WHERE workspace_id=$1 AND (id=$2 OR idempotency_key=$2) LIMIT 1`, workspaceID, ref.SourceID).Scan(&found, &sourceHash)
	case contracts.WorkReceiptReferenceCost:
		err = tx.QueryRow(ctx, `SELECT true, payload_hash FROM fornix.cost_ledger WHERE workspace_id=$1 AND (id=$2 OR idempotency_key=$2) LIMIT 1`, workspaceID, ref.SourceID).Scan(&found, &sourceHash)
	case contracts.WorkReceiptReferenceValidation:
		var reportHash, replayHash, requestHash string
		err = tx.QueryRow(ctx, `SELECT true, report_hash, replay_hash, request_hash FROM fornix.validation_runs WHERE workspace_id=$1 AND id=$2`, workspaceID, ref.SourceID).Scan(&found, &reportHash, &replayHash, &requestHash)
		if err == nil && ref.Hash != "" {
			switch strings.ToLower(ref.Hash) {
			case strings.ToLower(reportHash):
				sourceHash = reportHash
			case strings.ToLower(replayHash):
				sourceHash = replayHash
			case strings.ToLower(requestHash):
				sourceHash = requestHash
			default:
				return fmt.Errorf("%w: validation %s hash mismatch", ErrWorkReceiptIntegrity, ref.SourceID)
			}
		}
	case contracts.WorkReceiptReferenceReplay:
		// Replay is checked through the agent-run state hash or explicit hash.
		found = ref.Hash != ""
		sourceHash = ref.Hash
	case contracts.WorkReceiptReferenceChangeProposal:
		err = tx.QueryRow(ctx, `SELECT true, packet_hash FROM fornix.change_proposals WHERE workspace_id=$1 AND id=$2`, workspaceID, ref.SourceID).Scan(&found, &sourceHash)
	case contracts.WorkReceiptReferenceChangeApplication:
		err = tx.QueryRow(ctx, `SELECT true, packet_hash FROM fornix.change_applications WHERE workspace_id=$1 AND id=$2 AND status='applied'`, workspaceID, ref.SourceID).Scan(&found, &sourceHash)
	default:
		return fmt.Errorf("%w: unsupported reference kind %q", ErrWorkReceiptIntegrity, ref.Kind)
	}
	if errors.Is(err, pgx.ErrNoRows) || !found {
		return fmt.Errorf("%w: %s %s is missing", ErrWorkReceiptIntegrity, ref.Kind, ref.SourceID)
	}
	if err != nil {
		return fmt.Errorf("validate %s reference: %w", ref.Kind, err)
	}
	if ref.Hash != "" && strings.ToLower(sourceHash) != ref.Hash {
		return fmt.Errorf("%w: %s %s hash mismatch", ErrWorkReceiptIntegrity, ref.Kind, ref.SourceID)
	}
	return nil
}

func readWorkReceiptByIdentityTx(ctx context.Context, tx pgx.Tx, workspaceID, workKind, workID, idempotency string) (contracts.WorkReceipt, error) {
	var payload []byte
	err := tx.QueryRow(ctx, `SELECT canonical_payload FROM fornix.work_receipts WHERE workspace_id=$1 AND ((work_kind=$2 AND work_id=$3) OR idempotency_key=$4) ORDER BY id LIMIT 1`, workspaceID, workKind, workID, idempotency).Scan(&payload)
	return decodeWorkReceipt(payload, err)
}

func readWorkReceiptByIDTx(ctx context.Context, queryer interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, workspaceID, receiptID string) (contracts.WorkReceipt, error) {
	var payload []byte
	err := queryer.QueryRow(ctx, `SELECT canonical_payload FROM fornix.work_receipts WHERE workspace_id=$1 AND id=$2`, workspaceID, receiptID).Scan(&payload)
	return decodeWorkReceipt(payload, err)
}

func decodeWorkReceipt(payload []byte, err error) (contracts.WorkReceipt, error) {
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.WorkReceipt{}, ErrWorkReceiptNotFound
		}
		return contracts.WorkReceipt{}, err
	}
	var receipt contracts.WorkReceipt
	if err := json.Unmarshal(payload, &receipt); err != nil {
		return contracts.WorkReceipt{}, fmt.Errorf("decode work receipt: %w", err)
	}
	if err := receipt.Normalize(); err != nil {
		return contracts.WorkReceipt{}, fmt.Errorf("%w: %v", ErrWorkReceiptIntegrity, err)
	}
	if receipt.StableHash() != receipt.CanonicalHash {
		return contracts.WorkReceipt{}, fmt.Errorf("%w: canonical hash mismatch", ErrWorkReceiptIntegrity)
	}
	return receipt, nil
}

func boundedReceipt(receipt contracts.WorkReceipt, maxItems int) contracts.WorkReceipt {
	result := receipt
	result.Steps = append([]contracts.WorkReceiptStep(nil), receipt.Steps...)
	result.References = append([]contracts.WorkReceiptReference(nil), receipt.References...)
	result.Evidence = append([]contracts.WorkReceiptEvidence(nil), receipt.Evidence...)
	result.Artifacts = append([]contracts.WorkReceiptArtifact(nil), receipt.Artifacts...)
	remaining := maxItems
	if remaining < len(result.Steps) {
		result.Steps = result.Steps[:remaining]
		remaining = 0
	} else {
		remaining -= len(result.Steps)
	}
	if remaining < len(result.References) {
		result.References = result.References[:remaining]
		remaining = 0
	} else {
		remaining -= len(result.References)
	}
	if remaining < len(result.Evidence) {
		result.Evidence = result.Evidence[:remaining]
		remaining = 0
	} else {
		remaining -= len(result.Evidence)
	}
	if remaining < len(result.Artifacts) {
		result.Artifacts = result.Artifacts[:remaining]
	}
	return result
}

func minimalReceipt(receipt contracts.WorkReceipt) contracts.WorkReceipt {
	result := receipt
	result.Steps = append([]contracts.WorkReceiptStep(nil), receipt.Steps[:1]...)
	result.Steps[0].Metadata = nil
	result.Steps[0].ReferenceRoles = nil
	result.References = nil
	result.Evidence = nil
	result.Artifacts = nil
	return result
}

func tokenEstimate(bytes int) int {
	if bytes <= 0 {
		return 0
	}
	return (bytes + 3) / 4
}

func receiptViewHash(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}
