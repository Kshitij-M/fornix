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
	ErrChangeNotFound       = errors.New("repository change not found")
	ErrChangeConflict       = errors.New("repository change conflict")
	ErrChangeApproval       = errors.New("repository change approval is invalid")
	ErrChangeStaleFence     = errors.New("repository change task fence is stale")
	ErrChangeTerminal       = errors.New("repository change is terminal")
	ErrChangeInProgress     = errors.New("repository change application is in progress")
	ErrChangePacketMismatch = errors.New("repository change packet hash mismatch")
	ErrChangeBudget         = errors.New("repository change budget exceeded")
)

// ChangeProposalInput is the store boundary for a planned packet. Contents
// are transient and are moved to immutable ArtifactStore rows in the same
// transaction as the proposal, operation rows, and proposal event.
type ChangeProposalInput struct {
	Request  contracts.ChangeProposalRequest
	Packet   contracts.ChangePacket
	Contents map[string][]byte
	Diff     []byte
}

// ChangeApplicationFinalizeInput records the outcome after the external
// filesystem boundary. It is deliberately separate from BeginApplication so
// a crash between Postgres commit and filesystem verification is explicit.
type ChangeApplicationFinalizeInput struct {
	WorkspaceID    string
	ApplicationID  string
	ProposalID     string
	PacketHash     string
	Status         string
	ResultTreeHash string
	Conflict       *contracts.ChangeConflict
	Failure        *contracts.ChangeFailure
	Actor          contracts.ActorRef
	TaskOwnerID    string
	TaskFence      uint64
}

// RepositoryChangeStore owns the durable change ledger. Filesystem mutation
// is performed by internal/change only after this store admits the packet.
type RepositoryChangeStore struct {
	pool        *pgxpool.Pool
	events      *EventStore
	artifacts   *ArtifactStore
	policies    *PolicyStore
	failureHook func(string) error
}

// NewRepositoryChangeStore creates the Postgres-backed change authority.
func NewRepositoryChangeStore(pool *pgxpool.Pool, events *EventStore, artifacts *ArtifactStore) *RepositoryChangeStore {
	if events == nil {
		events = NewEventStore(pool)
	}
	if artifacts == nil {
		artifacts = NewArtifactStore(pool)
	}
	return &RepositoryChangeStore{pool: pool, events: events, artifacts: artifacts}
}

// SetFailureHook exposes deterministic transaction crash points to tests.
func (s *RepositoryChangeStore) SetFailureHook(hook func(string) error) {
	if s != nil {
		s.failureHook = hook
	}
}

// SetPolicyStore attaches the workspace policy authority. Existing callers
// without a policy store retain the pre-policy compatibility path.
func (s *RepositoryChangeStore) SetPolicyStore(policies *PolicyStore) {
	if s != nil {
		s.policies = policies
	}
}

func (s *RepositoryChangeStore) fail(stage string) error {
	if s != nil && s.failureHook != nil {
		return s.failureHook(stage)
	}
	return nil
}

// Propose persists one normalized packet and all content artifacts atomically.
// Repeating the same idempotency key and request hash returns the original
// proposal; reusing the key with different content fails closed.
func (s *RepositoryChangeStore) Propose(ctx context.Context, input ChangeProposalInput) (contracts.ChangeProposal, bool, error) {
	if s == nil || s.pool == nil || s.artifacts == nil || s.events == nil {
		return contracts.ChangeProposal{}, false, errors.New("repository change store is not configured")
	}
	if len(input.Packet.Source.Files) > 0 || input.Packet.Source.ManifestHash != "" || input.Packet.Source.SourceRoot != "" {
		input.Request.Source = input.Packet.Source
	}
	request, err := input.Request.Normalize()
	if err != nil {
		return contracts.ChangeProposal{}, false, err
	}
	requestedChangeBudget := input.Request.Budgets
	operationTypes := make([]string, 0, len(input.Packet.Operations))
	seenOperationTypes := make(map[string]struct{}, len(input.Packet.Operations))
	for _, operation := range input.Packet.Operations {
		operationType := strings.TrimSpace(operation.Type)
		if operationType == "" {
			continue
		}
		if _, exists := seenOperationTypes[operationType]; exists {
			continue
		}
		seenOperationTypes[operationType] = struct{}{}
		operationTypes = append(operationTypes, operationType)
	}
	if s.policies != nil {
		resolution, resolveErr := s.policies.Resolve(ctx, contracts.PolicyEvaluationRequest{
			WorkspaceID: request.WorkspaceID, Policy: request.Policy,
			RequestedBudget:       contracts.PolicyBudget{Change: requestedChangeBudget},
			RequestedApprovalMode: request.ApprovalMode, Operation: "change", OperationTypes: operationTypes,
		})
		if resolveErr != nil {
			return contracts.ChangeProposal{}, false, resolveErr
		}
		if resolution.Selected {
			request.Policy = contracts.ClonePolicyReference(resolution.Ref)
			request.Budgets = resolution.Budget.Change
			request.ApprovalMode = resolution.ApprovalMode
		}
	}
	packet := input.Packet
	if err := validateChangePacketBudget(packet, request.Budgets); err != nil {
		return contracts.ChangeProposal{}, false, err
	}
	packet.SchemaVersion = contracts.ChangeSchemaVersion
	packet.WorkspaceID = request.WorkspaceID
	packet.Repository = request.Repository
	packet.Source = request.Source
	packet.Budgets = request.Budgets
	packetHash := packet.StableHash()
	requestHash := request.RequestHash()
	actorJSON, err := json.Marshal(request.Actor)
	if err != nil {
		return contracts.ChangeProposal{}, false, fmt.Errorf("marshal change actor: %w", err)
	}
	sourceJSON, err := json.Marshal(packet.Source)
	if err != nil {
		return contracts.ChangeProposal{}, false, fmt.Errorf("marshal change source: %w", err)
	}
	budgetJSON, err := json.Marshal(packet.Budgets)
	if err != nil {
		return contracts.ChangeProposal{}, false, fmt.Errorf("marshal change budgets: %w", err)
	}
	taskJSON, _ := jsonStringOrEmpty(request.Task)
	sessionJSON, _ := jsonStringOrEmpty(request.Session)
	agentRunJSON, _ := jsonStringOrEmpty(request.AgentRun)
	status := contracts.ChangeAwaitingApproval
	if request.ApprovalMode == contracts.ChangeApprovalAutomatic {
		status = contracts.ChangeApproved
	} else if request.ApprovalMode == contracts.ChangeApprovalDenied {
		status = contracts.ChangeRejected
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ChangeProposal{}, false, fmt.Errorf("begin change proposal: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if s.policies != nil {
		if err := s.policies.LockActiveTx(ctx, tx, request.Policy); err != nil {
			return contracts.ChangeProposal{}, false, err
		}
	}
	var insertedID string
	err = tx.QueryRow(ctx, `
		INSERT INTO fornix.change_proposals(
			id,workspace_id,request_id,idempotency_key,request_hash,packet_hash,status,
			actor,task_ref,session_ref,agent_run_ref,task_owner_id,task_fence,repository,
			source,budgets,approval_mode,expected_tree_hash,policy_id,policy_version,policy_hash,created_at,updated_at
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10::jsonb,$11::jsonb,$12,$13,$14,$15::jsonb,$16::jsonb,$17,$18,$19,$20,$21,clock_timestamp(),clock_timestamp())
		ON CONFLICT (workspace_id,idempotency_key) DO NOTHING
		RETURNING id`, request.ID, request.WorkspaceID, request.RequestID, request.IdempotencyKey,
		requestHash, packetHash, status, actorJSON, taskJSON, sessionJSON, agentRunJSON,
		request.TaskOwnerID, int64(request.TaskFence), request.Repository, sourceJSON, budgetJSON,
		request.ApprovalMode, packet.ExpectedTreeHash, policyID(request.Policy), policyVersion(request.Policy), policyHash(request.Policy)).Scan(&insertedID)
	if errors.Is(err, pgx.ErrNoRows) {
		existing, readErr := readChangeProposalByKeyTx(ctx, tx, request.WorkspaceID, request.IdempotencyKey, true)
		if readErr != nil {
			return contracts.ChangeProposal{}, false, readErr
		}
		if existing.RequestHash != requestHash || existing.PacketHash != packetHash {
			return contracts.ChangeProposal{}, false, fmt.Errorf("%w: idempotency key", ErrIdempotencyConflict)
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.ChangeProposal{}, false, err
		}
		return existing, true, nil
	}
	if err != nil {
		return contracts.ChangeProposal{}, false, fmt.Errorf("insert change proposal: %w", err)
	}
	if err := s.fail("proposal_inserted"); err != nil {
		return contracts.ChangeProposal{}, false, err
	}
	for _, operation := range packet.Operations {
		var artifactID any
		if content := input.Contents[operation.ID]; len(content) > 0 {
			stored, putErr := s.artifacts.PutTx(ctx, tx, ArtifactPutInput{
				WorkspaceID: request.WorkspaceID, Kind: "change-content", MediaType: "application/octet-stream",
				Raw: content, SourceKind: "change_proposal", SourceID: request.ID,
				Role: "operation:" + operation.ID, IdempotencyKey: "change-content:" + request.WorkspaceID + ":" + request.ID + ":" + operation.ID,
				CausationID: request.CausationID, CorrelationID: request.CorrelationID, Actor: request.Actor,
			})
			if putErr != nil {
				return contracts.ChangeProposal{}, false, fmt.Errorf("store change content %s: %w", operation.ID, putErr)
			}
			operation.NewContentArtifact = &stored.Reference
			artifactID = stored.Reference.ArtifactID
			if _, err := tx.Exec(ctx, `INSERT INTO fornix.change_artifact_links(workspace_id,proposal_id,artifact_id,role) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, request.WorkspaceID, request.ID, stored.Reference.ArtifactID, "operation:"+operation.ID); err != nil {
				return contracts.ChangeProposal{}, false, fmt.Errorf("link change content %s: %w", operation.ID, err)
			}
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO fornix.change_operations(
				workspace_id,proposal_id,operation_id,ordinal,operation_type,path,destination,
				expected_hash,expected_mode,new_content_hash,new_content_artifact_id,new_byte_size,new_mode
			) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, request.WorkspaceID, request.ID,
			operation.ID, operation.Ordinal, operation.Type, operation.Path, operation.Destination,
			operation.ExpectedHash, int64(operation.ExpectedMode), operation.NewContentHash, artifactID,
			operation.NewByteSize, int64(operation.NewMode)); err != nil {
			return contracts.ChangeProposal{}, false, fmt.Errorf("insert change operation %s: %w", operation.ID, err)
		}
	}
	var diffArtifactID any
	if len(input.Diff) > 0 {
		stored, putErr := s.artifacts.PutTx(ctx, tx, ArtifactPutInput{
			WorkspaceID: request.WorkspaceID, Kind: "change-diff", MediaType: "application/json", Raw: input.Diff,
			SourceKind: "change_proposal", SourceID: request.ID, Role: "diff",
			IdempotencyKey: "change-diff:" + request.WorkspaceID + ":" + request.ID,
			CausationID:    request.CausationID, CorrelationID: request.CorrelationID, Actor: request.Actor,
		})
		if putErr != nil {
			return contracts.ChangeProposal{}, false, fmt.Errorf("store change diff: %w", putErr)
		}
		diffArtifactID = stored.Reference.ArtifactID
		if _, err := tx.Exec(ctx, `INSERT INTO fornix.change_artifact_links(workspace_id,proposal_id,artifact_id,role) VALUES($1,$2,$3,'diff') ON CONFLICT DO NOTHING`, request.WorkspaceID, request.ID, stored.Reference.ArtifactID); err != nil {
			return contracts.ChangeProposal{}, false, fmt.Errorf("link change diff: %w", err)
		}
		if _, err := tx.Exec(ctx, `UPDATE fornix.change_proposals SET diff_artifact_id=$3 WHERE workspace_id=$1 AND id=$2`, request.WorkspaceID, request.ID, stored.Reference.ArtifactID); err != nil {
			return contracts.ChangeProposal{}, false, fmt.Errorf("link change diff: %w", err)
		}
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.change_transitions(workspace_id,proposal_id,from_status,to_status,actor,request_id,reason) VALUES($1,$2,'',$3,$4::jsonb,$5,$6)`, request.WorkspaceID, request.ID, status, actorJSON, request.RequestID, "proposal created"); err != nil {
		return contracts.ChangeProposal{}, false, fmt.Errorf("record change proposal transition: %w", err)
	}
	if err := appendChangeEventTx(ctx, tx, s.events, request, "change.proposed", map[string]any{
		"proposal_id": request.ID, "status": status, "packet_hash": packetHash, "request_hash": requestHash, "diff_artifact_id": diffArtifactID,
	}); err != nil {
		return contracts.ChangeProposal{}, false, err
	}
	if err := s.fail("proposal_ready"); err != nil {
		return contracts.ChangeProposal{}, false, err
	}
	proposal, err := readChangeProposalTx(ctx, tx, request.WorkspaceID, request.ID, false)
	if err != nil {
		return contracts.ChangeProposal{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ChangeProposal{}, false, fmt.Errorf("commit change proposal: %w", err)
	}
	return proposal, false, nil
}

// validateChangePacketBudget is repeated after policy resolution because the
// planner may have run with a caller budget that is wider than the selected
// policy. The authoritative store must never persist a packet outside the
// effective policy envelope.
func validateChangePacketBudget(packet contracts.ChangePacket, budget contracts.ChangeBudgets) error {
	if len(packet.Operations) < 1 || len(packet.Operations) > budget.MaxOperations {
		return fmt.Errorf("%w: operation count", ErrChangeBudget)
	}
	var total int64
	for _, operation := range packet.Operations {
		if operation.NewByteSize < 0 || operation.NewByteSize > budget.MaxFileBytes {
			return fmt.Errorf("%w: file size", ErrChangeBudget)
		}
		if total > budget.MaxTotalBytes-operation.NewByteSize {
			return fmt.Errorf("%w: total bytes", ErrChangeBudget)
		}
		total += operation.NewByteSize
	}
	return nil
}

// Get reads a proposal and its immutable operation/artifact references.
func (s *RepositoryChangeStore) Get(ctx context.Context, workspaceID, proposalID string) (contracts.ChangeProposal, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ChangeProposal{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	proposal, err := readChangeProposalTx(ctx, tx, strings.TrimSpace(workspaceID), strings.TrimSpace(proposalID), false)
	if err != nil {
		return contracts.ChangeProposal{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ChangeProposal{}, err
	}
	return proposal, nil
}

// Approve records an exact packet decision and advances proposal state in the
// same transaction as its audit event. A changed packet can never reuse an
// earlier approval.
func (s *RepositoryChangeStore) Approve(ctx context.Context, request contracts.ChangeApprovalRequest) (contracts.ChangeApproval, contracts.ChangeProposal, bool, error) {
	if strings.TrimSpace(request.WorkspaceID) == "" || strings.TrimSpace(request.ProposalID) == "" || strings.TrimSpace(request.PacketHash) == "" || strings.TrimSpace(request.IdempotencyKey) == "" {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, errors.New("workspace, proposal, packet hash, and idempotency key are required")
	}
	request.WorkspaceID = strings.TrimSpace(request.WorkspaceID)
	request.ProposalID = strings.TrimSpace(request.ProposalID)
	request.PacketHash = strings.ToLower(strings.TrimSpace(request.PacketHash))
	request.Decision = strings.ToLower(strings.TrimSpace(request.Decision))
	if request.Decision != "approved" && request.Decision != "rejected" {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, ErrChangeApproval
	}
	if request.ID == "" {
		request.ID = contracts.NewID("approval")
	}
	if request.Actor.WorkspaceID == "" {
		request.Actor.WorkspaceID = request.WorkspaceID
	}
	actorJSON, _ := json.Marshal(request.Actor)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if existing, readErr := readChangeApprovalByKeyTx(ctx, tx, request.WorkspaceID, request.IdempotencyKey, false); readErr == nil {
		if existing.ProposalID != request.ProposalID || existing.PacketHash != request.PacketHash || existing.Decision != request.Decision {
			return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, fmt.Errorf("%w: approval idempotency key", ErrIdempotencyConflict)
		}
		proposal, proposalErr := readChangeProposalTx(ctx, tx, request.WorkspaceID, request.ProposalID, false)
		if proposalErr != nil {
			return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, proposalErr
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, err
		}
		return existing, proposal, true, nil
	} else if !errors.Is(readErr, ErrChangeNotFound) {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, readErr
	}
	var currentStatus, currentPacket string
	if err := tx.QueryRow(ctx, `SELECT status,packet_hash FROM fornix.change_proposals WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, request.WorkspaceID, request.ProposalID).Scan(&currentStatus, &currentPacket); errors.Is(err, pgx.ErrNoRows) {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, ErrChangeNotFound
	} else if err != nil {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, err
	}
	if currentPacket != request.PacketHash {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, ErrChangePacketMismatch
	}
	proposal, err := readChangeProposalTx(ctx, tx, request.WorkspaceID, request.ProposalID, true)
	if err != nil {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, err
	}
	if request.Policy != nil && !samePolicyReference(request.Policy, proposal.Policy) {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, ErrPolicyConflict
	}
	if currentStatus != contracts.ChangeAwaitingApproval && currentStatus != contracts.ChangeProposed {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, fmt.Errorf("%w: proposal status %s", ErrChangeApproval, currentStatus)
	}
	if request.ExpiresAt != nil && !request.ExpiresAt.After(time.Now().UTC()) {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, fmt.Errorf("%w: approval is already expired", ErrChangeApproval)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.change_approvals(id,workspace_id,proposal_id,packet_hash,decision,reason,actor,idempotency_key,expires_at,policy_id,policy_version,policy_hash) VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10,$11,$12)`, request.ID, request.WorkspaceID, request.ProposalID, request.PacketHash, request.Decision, strings.TrimSpace(request.Reason), actorJSON, request.IdempotencyKey, request.ExpiresAt, policyID(proposal.Policy), policyVersion(proposal.Policy), policyHash(proposal.Policy)); err != nil {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, fmt.Errorf("insert change approval: %w", err)
	}
	newStatus := contracts.ChangeApproved
	if request.Decision == "rejected" {
		newStatus = contracts.ChangeRejected
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.change_proposals SET status=$3,updated_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2`, request.WorkspaceID, request.ProposalID, newStatus); err != nil {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.change_transitions(workspace_id,proposal_id,from_status,to_status,actor,request_id,reason) VALUES($1,$2,$3,$4,$5::jsonb,$6,$7)`, request.WorkspaceID, request.ProposalID, currentStatus, newStatus, actorJSON, request.IdempotencyKey, strings.TrimSpace(request.Reason)); err != nil {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, err
	}
	if err := appendChangeEventTx(ctx, tx, s.events, contracts.ChangeProposalRequest{WorkspaceID: request.WorkspaceID, RequestID: request.ID, IdempotencyKey: "change-approval:" + request.WorkspaceID + ":" + request.IdempotencyKey, Actor: request.Actor, Policy: proposal.Policy, CausationID: request.ProposalID, CorrelationID: request.ProposalID}, "change.approval_decided", map[string]any{"proposal_id": request.ProposalID, "packet_hash": request.PacketHash, "decision": request.Decision}); err != nil {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, err
	}
	approval, err := readChangeApprovalByKeyTx(ctx, tx, request.WorkspaceID, request.IdempotencyKey, false)
	if err != nil {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, err
	}
	proposal, err = readChangeProposalTx(ctx, tx, request.WorkspaceID, request.ProposalID, false)
	if err != nil {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ChangeApproval{}, contracts.ChangeProposal{}, false, err
	}
	return approval, proposal, false, nil
}

// BeginApplication atomically admits an approved packet and records the
// applying state. No filesystem operation is performed in this transaction.
func (s *RepositoryChangeStore) BeginApplication(ctx context.Context, request contracts.ChangeApplicationRequest) (contracts.ChangeApplication, contracts.ChangeProposal, bool, error) {
	if strings.TrimSpace(request.WorkspaceID) == "" || strings.TrimSpace(request.ProposalID) == "" || strings.TrimSpace(request.PacketHash) == "" || strings.TrimSpace(request.IdempotencyKey) == "" {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, errors.New("workspace, proposal, packet hash, and idempotency key are required")
	}
	request.WorkspaceID, request.ProposalID, request.PacketHash, request.IdempotencyKey = strings.TrimSpace(request.WorkspaceID), strings.TrimSpace(request.ProposalID), strings.ToLower(strings.TrimSpace(request.PacketHash)), strings.TrimSpace(request.IdempotencyKey)
	if request.ID == "" {
		request.ID = contracts.NewID("application")
	}
	if request.Actor.WorkspaceID == "" {
		request.Actor.WorkspaceID = request.WorkspaceID
	}
	actorJSON, _ := json.Marshal(request.Actor)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if existing, readErr := readChangeApplicationByKeyTx(ctx, tx, request.WorkspaceID, request.IdempotencyKey, false); readErr == nil {
		if existing.ProposalID != request.ProposalID || existing.PacketHash != request.PacketHash {
			return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, fmt.Errorf("%w: application idempotency key", ErrIdempotencyConflict)
		}
		proposal, proposalErr := readChangeProposalTx(ctx, tx, request.WorkspaceID, request.ProposalID, false)
		if proposalErr != nil {
			return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, proposalErr
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, err
		}
		return existing, proposal, true, nil
	} else if !errors.Is(readErr, ErrChangeNotFound) {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, readErr
	}
	proposal, err := readChangeProposalTx(ctx, tx, request.WorkspaceID, request.ProposalID, true)
	if err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, err
	}
	if proposal.PacketHash != request.PacketHash {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, ErrChangePacketMismatch
	}
	if proposal.Status != contracts.ChangeApproved {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, fmt.Errorf("%w: proposal status %s", ErrChangeApproval, proposal.Status)
	}
	if proposal.Task != nil {
		if request.TaskOwnerID == "" || request.TaskFence == 0 || request.TaskOwnerID != proposal.TaskOwnerID || request.TaskFence != proposal.TaskFence {
			return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, ErrChangeStaleFence
		}
		if err := validateChangeTaskFenceTx(ctx, tx, request.WorkspaceID, proposal.Task.ID, request.TaskOwnerID, request.TaskFence); err != nil {
			return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, err
		}
	}
	var activeID string
	if err := tx.QueryRow(ctx, `SELECT id FROM fornix.change_applications WHERE workspace_id=$1 AND proposal_id=$2 AND status='applying' ORDER BY created_at DESC LIMIT 1 FOR UPDATE`, request.WorkspaceID, request.ProposalID).Scan(&activeID); err == nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, fmt.Errorf("%w: application %s", ErrChangeInProgress, activeID)
	} else if !errors.Is(err, pgx.ErrNoRows) {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, err
	}
	if request.Policy != nil && !samePolicyReference(request.Policy, proposal.Policy) {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, ErrPolicyConflict
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.change_applications(id,workspace_id,proposal_id,packet_hash,idempotency_key,status,expected_tree_hash,actor,task_owner_id,task_fence,policy_id,policy_version,policy_hash) VALUES($1,$2,$3,$4,$5,'applying',$6,$7::jsonb,$8,$9,$10,$11,$12)`, request.ID, request.WorkspaceID, request.ProposalID, request.PacketHash, request.IdempotencyKey, changeExpectedTreeHash(proposal), actorJSON, request.TaskOwnerID, int64(request.TaskFence), policyID(proposal.Policy), policyVersion(proposal.Policy), policyHash(proposal.Policy)); err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, fmt.Errorf("insert change application: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.change_proposals SET status='applying',updated_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2`, request.WorkspaceID, request.ProposalID); err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.change_transitions(workspace_id,proposal_id,application_id,from_status,to_status,actor,request_id,reason) VALUES($1,$2,$3,$4,'applying',$5::jsonb,$6,'application admitted')`, request.WorkspaceID, request.ProposalID, request.ID, proposal.Status, actorJSON, request.IdempotencyKey); err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, err
	}
	if err := appendChangeEventTx(ctx, tx, s.events, contracts.ChangeProposalRequest{WorkspaceID: request.WorkspaceID, RequestID: request.ID, IdempotencyKey: "change-application:" + request.WorkspaceID + ":" + request.IdempotencyKey, Actor: request.Actor, Policy: proposal.Policy, CausationID: request.ProposalID, CorrelationID: request.ProposalID}, "change.application_started", map[string]any{"proposal_id": request.ProposalID, "application_id": request.ID, "packet_hash": request.PacketHash}); err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, err
	}
	application, err := readChangeApplicationTx(ctx, tx, request.WorkspaceID, request.ID, false)
	if err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, err
	}
	proposal, err = readChangeProposalTx(ctx, tx, request.WorkspaceID, request.ProposalID, false)
	if err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, false, err
	}
	return application, proposal, false, nil
}

// FinalizeApplication records only a verified outcome. Task-bound callers
// must still hold the exact live fence, so a stale worker cannot claim that an
// external filesystem effect was durably applied.
func (s *RepositoryChangeStore) FinalizeApplication(ctx context.Context, input ChangeApplicationFinalizeInput) (contracts.ChangeApplication, contracts.ChangeProposal, error) {
	if input.Status != contracts.ChangeApplied && input.Status != contracts.ChangeConflicted && input.Status != contracts.ChangeFailed && input.Status != contracts.ChangeRecoveryRequired {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, errors.New("invalid change application terminal status")
	}
	if input.Actor.WorkspaceID == "" {
		input.Actor.WorkspaceID = input.WorkspaceID
	}
	actorJSON, _ := json.Marshal(input.Actor)
	conflictJSON, _ := jsonStringOrEmpty(input.Conflict)
	failureJSON, _ := jsonStringOrEmpty(input.Failure)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var currentStatus, proposalID, packetHash, owner string
	var currentFence int64
	if err := tx.QueryRow(ctx, `SELECT status,proposal_id,packet_hash,task_owner_id,task_fence FROM fornix.change_applications WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, input.WorkspaceID, input.ApplicationID).Scan(&currentStatus, &proposalID, &packetHash, &owner, &currentFence); errors.Is(err, pgx.ErrNoRows) {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, ErrChangeNotFound
	} else if err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
	}
	if input.ProposalID != "" && input.ProposalID != proposalID {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, ErrChangeConflict
	}
	if input.PacketHash != "" && strings.ToLower(input.PacketHash) != packetHash {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, ErrChangePacketMismatch
	}
	if currentStatus != contracts.ChangeApplying {
		application, readErr := readChangeApplicationTx(ctx, tx, input.WorkspaceID, input.ApplicationID, false)
		if readErr != nil {
			return contracts.ChangeApplication{}, contracts.ChangeProposal{}, readErr
		}
		proposal, readErr := readChangeProposalTx(ctx, tx, input.WorkspaceID, proposalID, false)
		if readErr != nil {
			return contracts.ChangeApplication{}, contracts.ChangeProposal{}, readErr
		}
		if err := tx.Commit(ctx); err != nil {
			return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
		}
		return application, proposal, nil
	}
	proposal, err := readChangeProposalTx(ctx, tx, input.WorkspaceID, proposalID, true)
	if err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
	}
	if proposal.Task != nil {
		if input.TaskOwnerID == "" || input.TaskFence == 0 || owner != input.TaskOwnerID || uint64(currentFence) != input.TaskFence {
			return contracts.ChangeApplication{}, contracts.ChangeProposal{}, ErrChangeStaleFence
		}
		if err := validateChangeTaskFenceTx(ctx, tx, input.WorkspaceID, proposal.Task.ID, input.TaskOwnerID, input.TaskFence); err != nil {
			return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.change_applications SET status=$3,result_tree_hash=$4,conflict=$5::jsonb,failure=$6::jsonb,updated_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2`, input.WorkspaceID, input.ApplicationID, input.Status, strings.ToLower(strings.TrimSpace(input.ResultTreeHash)), conflictJSON, failureJSON); err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.change_proposals SET status=$3,updated_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2`, input.WorkspaceID, proposalID, input.Status); err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
	}
	if _, err := tx.Exec(ctx, `INSERT INTO fornix.change_transitions(workspace_id,proposal_id,application_id,from_status,to_status,actor,request_id,reason) VALUES($1,$2,$3,'applying',$4,$5::jsonb,$6,$7)`, input.WorkspaceID, proposalID, input.ApplicationID, input.Status, actorJSON, input.ApplicationID, "application finalized"); err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
	}
	eventType := "change.application_failed"
	if input.Status == contracts.ChangeApplied {
		eventType = "change.applied"
	} else if input.Status == contracts.ChangeConflicted {
		eventType = "change.conflicted"
	} else if input.Status == contracts.ChangeRecoveryRequired {
		eventType = "change.recovery_required"
	}
	if err := appendChangeEventTx(ctx, tx, s.events, contracts.ChangeProposalRequest{WorkspaceID: input.WorkspaceID, RequestID: input.ApplicationID, IdempotencyKey: "change-finalize:" + input.WorkspaceID + ":" + input.ApplicationID, Actor: input.Actor, Policy: proposal.Policy, CausationID: input.ProposalID, CorrelationID: input.ProposalID}, eventType, map[string]any{"proposal_id": proposalID, "application_id": input.ApplicationID, "packet_hash": packetHash, "status": input.Status, "result_tree_hash": input.ResultTreeHash}); err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
	}
	if err := s.fail("application_finalized"); err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
	}
	application, err := readChangeApplicationTx(ctx, tx, input.WorkspaceID, input.ApplicationID, false)
	if err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
	}
	proposal, err = readChangeProposalTx(ctx, tx, input.WorkspaceID, proposalID, false)
	if err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
	}
	return application, proposal, nil
}

// Disclose returns a bounded hash-preserving change view. Raw proposed bytes
// are never copied into this view; they remain behind ArtifactStore's explicit
// disclosure and authorization boundary.
func (s *RepositoryChangeStore) Disclose(ctx context.Context, request contracts.ChangeDisclosureRequest) (contracts.ChangeDisclosureResult, error) {
	normalized, err := request.Normalize()
	if err != nil {
		return contracts.ChangeDisclosureResult{}, err
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.ChangeDisclosureResult{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var proposal *contracts.ChangeProposal
	var application *contracts.ChangeApplication
	if normalized.ProposalID != "" {
		value, readErr := readChangeProposalTx(ctx, tx, normalized.WorkspaceID, normalized.ProposalID, false)
		if readErr != nil {
			return contracts.ChangeDisclosureResult{}, readErr
		}
		proposal = &value
	} else {
		value, readErr := readChangeApplicationTx(ctx, tx, normalized.WorkspaceID, normalized.ApplicationID, false)
		if readErr != nil {
			return contracts.ChangeDisclosureResult{}, readErr
		}
		application = &value
		proposalValue, readErr := readChangeProposalTx(ctx, tx, normalized.WorkspaceID, application.ProposalID, false)
		if readErr != nil {
			return contracts.ChangeDisclosureResult{}, readErr
		}
		proposal = &proposalValue
	}
	view := contracts.ChangeDisclosureResult{WorkspaceID: normalized.WorkspaceID, Proposal: proposal, Application: application, PacketHash: proposal.PacketHash}
	if proposal.DiffArtifact != nil {
		view.DiffArtifactHash = proposal.DiffArtifact.ContentHash
	}
	if application != nil {
		view.ResultTreeHash = application.ResultTreeHash
	}
	if normalized.Level == string(contracts.DisclosureGist) {
		view.Proposal.Operations = nil
		view.Proposal.Source.Files = nil
	}
	if len(view.Proposal.Operations) > normalized.MaxItems {
		view.Proposal.Operations = view.Proposal.Operations[:normalized.MaxItems]
		view.Truncated = true
	}
	if len(view.Proposal.Source.Files) > normalized.MaxItems {
		view.Proposal.Source.Files = view.Proposal.Source.Files[:normalized.MaxItems]
		view.Truncated = true
	}
	raw, err := json.Marshal(view)
	if err != nil {
		return contracts.ChangeDisclosureResult{}, err
	}
	if len(raw) > normalized.MaxBytes {
		view.Truncated = true
		view.Proposal.Operations = nil
		view.Proposal.Source.Files = nil
		raw, err = json.Marshal(view)
		if err != nil {
			return contracts.ChangeDisclosureResult{}, err
		}
		if len(raw) > normalized.MaxBytes {
			raw = raw[:normalized.MaxBytes]
		}
	}
	view.TotalBytes = len(raw)
	view.ContentViewHash = hashBytes(raw)
	if err := tx.Commit(ctx); err != nil {
		return contracts.ChangeDisclosureResult{}, err
	}
	return view, nil
}

func appendChangeEventTx(ctx context.Context, tx pgx.Tx, events *EventStore, request contracts.ChangeProposalRequest, eventType string, payload map[string]any) error {
	event, err := contracts.NewEvent(eventType, payload)
	if err != nil {
		return err
	}
	event.Scope = contracts.Scope{WorkspaceID: request.WorkspaceID, Subject: request.Repository}
	event.Actor = request.Actor
	event.Policy = contracts.ClonePolicyReference(request.Policy)
	event.CausationID = request.CausationID
	event.CorrelationID = request.CorrelationID
	event.IdempotencyKey = request.IdempotencyKey
	event.Provenance = contracts.Provenance{SourcePaths: []string{"repository-change"}}
	if _, err := events.AppendTx(ctx, tx, event); err != nil {
		return fmt.Errorf("append %s: %w", eventType, err)
	}
	return nil
}

func validateChangeTaskFenceTx(ctx context.Context, tx pgx.Tx, workspaceID, taskID, owner string, fence uint64) error {
	parsed, err := strconv.ParseInt(strings.TrimSpace(taskID), 10, 64)
	if err != nil || parsed <= 0 || strings.TrimSpace(owner) == "" || fence == 0 {
		return ErrChangeStaleFence
	}
	var currentOwner string
	var currentFence int64
	var released *time.Time
	var active bool
	err = tx.QueryRow(ctx, `SELECT l.owner_id,l.fence,l.released_at,(l.released_at IS NULL AND l.lease_until > clock_timestamp()) FROM fornix.task_execution_leases l WHERE l.workspace_id=$1 AND l.task_id=$2 FOR UPDATE`, workspaceID, parsed).Scan(&currentOwner, &currentFence, &released, &active)
	if errors.Is(err, pgx.ErrNoRows) || err != nil {
		return ErrChangeStaleFence
	}
	if released != nil || !active || currentOwner != owner || currentFence <= 0 || uint64(currentFence) != fence {
		return ErrChangeStaleFence
	}
	return nil
}

func changeExpectedTreeHash(proposal contracts.ChangeProposal) string {
	return proposal.ExpectedTreeHash
}

func readChangeProposalByKeyTx(ctx context.Context, tx pgx.Tx, workspaceID, key string, forUpdate bool) (contracts.ChangeProposal, error) {
	query := `SELECT id FROM fornix.change_proposals WHERE workspace_id=$1 AND idempotency_key=$2`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var id string
	if err := tx.QueryRow(ctx, query, workspaceID, key).Scan(&id); errors.Is(err, pgx.ErrNoRows) {
		return contracts.ChangeProposal{}, ErrChangeNotFound
	} else if err != nil {
		return contracts.ChangeProposal{}, err
	}
	return readChangeProposalTx(ctx, tx, workspaceID, id, false)
}

func readChangeProposalTx(ctx context.Context, tx pgx.Tx, workspaceID, id string, forUpdate bool) (contracts.ChangeProposal, error) {
	query := `SELECT id,workspace_id,request_id,idempotency_key,request_hash,packet_hash,status,actor,task_ref,session_ref,agent_run_ref,task_owner_id,task_fence,repository,source,budgets,approval_mode,expected_tree_hash,diff_artifact_id,policy_id,policy_version,policy_hash,created_at,updated_at FROM fornix.change_proposals WHERE workspace_id=$1 AND id=$2`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var proposal contracts.ChangeProposal
	var actorJSON, taskJSON, sessionJSON, agentJSON, sourceJSON, budgetJSON []byte
	var fence int64
	var diffID *int64
	var policyID, policyVersion, policyHash *string
	err := tx.QueryRow(ctx, query, workspaceID, id).Scan(&proposal.ID, &proposal.WorkspaceID, &proposal.RequestID, &proposal.IdempotencyKey, &proposal.RequestHash, &proposal.PacketHash, &proposal.Status, &actorJSON, &taskJSON, &sessionJSON, &agentJSON, &proposal.TaskOwnerID, &fence, &proposal.Repository, &sourceJSON, &budgetJSON, &proposal.ApprovalMode, &proposal.ExpectedTreeHash, &diffID, &policyID, &policyVersion, &policyHash, &proposal.CreatedAt, &proposal.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.ChangeProposal{}, ErrChangeNotFound
	}
	if err != nil {
		return contracts.ChangeProposal{}, fmt.Errorf("read change proposal: %w", err)
	}
	proposal.SchemaVersion = contracts.ChangeSchemaVersion
	if fence < 0 {
		return contracts.ChangeProposal{}, ErrChangeConflict
	}
	proposal.TaskFence = uint64(fence)
	proposal.Policy = policyReference(policyID, policyVersion, policyHash, proposal.WorkspaceID)
	if err := json.Unmarshal(actorJSON, &proposal.Actor); err != nil {
		return contracts.ChangeProposal{}, err
	}
	if err := decodeChangeEntityRef(taskJSON, &proposal.Task); err != nil {
		return contracts.ChangeProposal{}, err
	}
	if err := decodeChangeEntityRef(sessionJSON, &proposal.Session); err != nil {
		return contracts.ChangeProposal{}, err
	}
	if err := decodeChangeEntityRef(agentJSON, &proposal.AgentRun); err != nil {
		return contracts.ChangeProposal{}, err
	}
	if err := json.Unmarshal(sourceJSON, &proposal.Source); err != nil {
		return contracts.ChangeProposal{}, err
	}
	if err := json.Unmarshal(budgetJSON, &proposal.Budgets); err != nil {
		return contracts.ChangeProposal{}, err
	}
	rows, err := tx.Query(ctx, `SELECT operation_id,ordinal,operation_type,path,destination,expected_hash,expected_mode,new_content_hash,new_content_artifact_id,new_byte_size,result_hash,new_mode FROM fornix.change_operations WHERE workspace_id=$1 AND proposal_id=$2 ORDER BY ordinal,operation_id`, workspaceID, id)
	if err != nil {
		return contracts.ChangeProposal{}, err
	}
	artifactIDs := make(map[string]*int64)
	for rows.Next() {
		var operation contracts.ChangeOperation
		var expectedMode, newMode int64
		var artifactID *int64
		if err := rows.Scan(&operation.ID, &operation.Ordinal, &operation.Type, &operation.Path, &operation.Destination, &operation.ExpectedHash, &expectedMode, &operation.NewContentHash, &artifactID, &operation.NewByteSize, &operation.ResultHash, &newMode); err != nil {
			return contracts.ChangeProposal{}, err
		}
		if expectedMode < 0 || newMode < 0 {
			return contracts.ChangeProposal{}, ErrChangeConflict
		}
		operation.ExpectedMode, operation.NewMode = uint32(expectedMode), uint32(newMode)
		artifactIDs[operation.ID] = artifactID
		proposal.Operations = append(proposal.Operations, operation)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return contracts.ChangeProposal{}, err
	}
	rows.Close()
	for index := range proposal.Operations {
		operation := &proposal.Operations[index]
		if artifactIDs[operation.ID] != nil {
			ref, refErr := readArtifactRefByIdentityTx(ctx, tx, workspaceID, "change_proposal", id, "operation:"+operation.ID)
			if refErr != nil {
				return contracts.ChangeProposal{}, refErr
			}
			operation.NewContentArtifact = &ref
		}
	}
	if diffID != nil {
		ref, refErr := readArtifactRefByIdentityTx(ctx, tx, workspaceID, "change_proposal", id, "diff")
		if refErr != nil {
			return contracts.ChangeProposal{}, refErr
		}
		proposal.DiffArtifact = &ref
	}
	return proposal, nil
}

func readChangeApprovalByKeyTx(ctx context.Context, tx pgx.Tx, workspaceID, key string, forUpdate bool) (contracts.ChangeApproval, error) {
	query := `SELECT id,workspace_id,proposal_id,packet_hash,decision,reason,actor,idempotency_key,expires_at,policy_id,policy_version,policy_hash,created_at FROM fornix.change_approvals WHERE workspace_id=$1 AND idempotency_key=$2`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	return scanChangeApproval(tx.QueryRow(ctx, query, workspaceID, key))
}

func scanChangeApproval(row pgx.Row) (contracts.ChangeApproval, error) {
	var approval contracts.ChangeApproval
	var actorJSON []byte
	var policyID, policyVersion, policyHash *string
	err := row.Scan(&approval.ID, &approval.WorkspaceID, &approval.ProposalID, &approval.PacketHash, &approval.Decision, &approval.Reason, &actorJSON, &approval.IdempotencyKey, &approval.ExpiresAt, &policyID, &policyVersion, &policyHash, &approval.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.ChangeApproval{}, ErrChangeNotFound
	}
	if err != nil {
		return contracts.ChangeApproval{}, err
	}
	if err := json.Unmarshal(actorJSON, &approval.Actor); err != nil {
		return contracts.ChangeApproval{}, err
	}
	approval.Policy = policyReference(policyID, policyVersion, policyHash, approval.WorkspaceID)
	return approval, nil
}

func readChangeApplicationByKeyTx(ctx context.Context, tx pgx.Tx, workspaceID, key string, forUpdate bool) (contracts.ChangeApplication, error) {
	query := `SELECT id FROM fornix.change_applications WHERE workspace_id=$1 AND idempotency_key=$2`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var id string
	if err := tx.QueryRow(ctx, query, workspaceID, key).Scan(&id); errors.Is(err, pgx.ErrNoRows) {
		return contracts.ChangeApplication{}, ErrChangeNotFound
	} else if err != nil {
		return contracts.ChangeApplication{}, err
	}
	return readChangeApplicationTx(ctx, tx, workspaceID, id, false)
}

func readChangeApplicationTx(ctx context.Context, tx pgx.Tx, workspaceID, id string, forUpdate bool) (contracts.ChangeApplication, error) {
	query := `SELECT id,workspace_id,proposal_id,packet_hash,status,expected_tree_hash,result_tree_hash,actor,task_owner_id,task_fence,conflict,failure,policy_id,policy_version,policy_hash,created_at,updated_at FROM fornix.change_applications WHERE workspace_id=$1 AND id=$2`
	if forUpdate {
		query += ` FOR UPDATE`
	}
	var application contracts.ChangeApplication
	var actorJSON, conflictJSON, failureJSON []byte
	var policyID, policyVersion, policyHash *string
	var fence int64
	err := tx.QueryRow(ctx, query, workspaceID, id).Scan(&application.ID, &application.WorkspaceID, &application.ProposalID, &application.PacketHash, &application.Status, &application.ExpectedTreeHash, &application.ResultTreeHash, &actorJSON, &application.TaskOwnerID, &fence, &conflictJSON, &failureJSON, &policyID, &policyVersion, &policyHash, &application.CreatedAt, &application.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.ChangeApplication{}, ErrChangeNotFound
	}
	if err != nil {
		return contracts.ChangeApplication{}, err
	}
	if fence < 0 {
		return contracts.ChangeApplication{}, ErrChangeConflict
	}
	application.TaskFence = uint64(fence)
	application.Policy = policyReference(policyID, policyVersion, policyHash, application.WorkspaceID)
	if err := json.Unmarshal(actorJSON, &application.Actor); err != nil {
		return contracts.ChangeApplication{}, err
	}
	if err := decodeOptionalJSON(conflictJSON, &application.Conflict); err != nil {
		return contracts.ChangeApplication{}, err
	}
	if err := decodeOptionalJSON(failureJSON, &application.Failure); err != nil {
		return contracts.ChangeApplication{}, err
	}
	return application, nil
}

func decodeChangeEntityRef(raw []byte, destination **contracts.EntityRef) error {
	if len(raw) == 0 || string(raw) == "{}" || string(raw) == "null" {
		return nil
	}
	var value contracts.EntityRef
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	if value.ID == "" {
		return nil
	}
	*destination = &value
	return nil
}

func decodeOptionalJSON(raw []byte, destination any) error {
	if len(raw) == 0 || string(raw) == "{}" || string(raw) == "null" {
		return nil
	}
	return json.Unmarshal(raw, destination)
}

func hashBytes(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func policyID(ref *contracts.ValidationPolicyRef) any {
	if ref == nil || strings.TrimSpace(ref.PolicyID) == "" {
		return nil
	}
	return ref.PolicyID
}

func policyVersion(ref *contracts.ValidationPolicyRef) any {
	if ref == nil || strings.TrimSpace(ref.Version) == "" {
		return nil
	}
	return ref.Version
}

func policyHash(ref *contracts.ValidationPolicyRef) any {
	if ref == nil || strings.TrimSpace(ref.PolicyHash) == "" {
		return nil
	}
	return ref.PolicyHash
}

func policyReference(id, version, hash *string, workspaceID string) *contracts.ValidationPolicyRef {
	if id == nil || version == nil || strings.TrimSpace(*id) == "" || strings.TrimSpace(*version) == "" {
		return nil
	}
	return &contracts.ValidationPolicyRef{SchemaVersion: contracts.PolicySchemaVersion, WorkspaceID: workspaceID, PolicyID: *id, Version: *version, PolicyHash: valueOrEmpty(hash)}
}

func valueOrEmpty(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func samePolicyReference(left, right *contracts.ValidationPolicyRef) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.WorkspaceID == right.WorkspaceID && left.PolicyID == right.PolicyID && left.Version == right.Version && (left.PolicyHash == "" || right.PolicyHash == "" || left.PolicyHash == right.PolicyHash)
}
