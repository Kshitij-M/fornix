package change

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

// Service composes the deterministic planner, durable change ledger, and
// content-addressed artifact authority. It never expands a request into a
// shell command or treats the filesystem as the source of approval truth.
type Service struct {
	Store     *store.RepositoryChangeStore
	Artifacts *store.ArtifactStore
	Receipts  *store.WorkReceiptStore
}

// SetReceiptStore enables derived Verified Change Packet output after an
// application has been durably verified. Receipt failure never rolls back an
// already-applied filesystem effect; the application remains authoritative.
func (s *Service) SetReceiptStore(receipts *store.WorkReceiptStore) {
	if s != nil {
		s.Receipts = receipts
	}
}

// NewService creates a repository-change service over existing Fornix stores.
func NewService(changeStore *store.RepositoryChangeStore, artifacts *store.ArtifactStore) *Service {
	return &Service{Store: changeStore, Artifacts: artifacts}
}

// PlanInput supplies the configured workspace mount and affected paths. The
// source snapshot is captured immediately before planning and is persisted as
// the packet precondition.
type PlanInput struct {
	Request contracts.ChangeProposalRequest
	Root    string
}

// DryRun plans and verifies a packet without writing proposals, artifacts,
// events, or any filesystem bytes.
func (s *Service) DryRun(ctx context.Context, input PlanInput) (PlannedChange, error) {
	return s.plan(ctx, input)
}

// Propose captures and durably persists one packet after deterministic
// planning. Content artifacts and the proposal event share one transaction.
func (s *Service) Propose(ctx context.Context, input PlanInput) (contracts.ChangeProposal, bool, PlannedChange, error) {
	planned, err := s.plan(ctx, input)
	if err != nil {
		return contracts.ChangeProposal{}, false, PlannedChange{}, err
	}
	proposal, duplicate, err := s.Store.Propose(ctx, store.ChangeProposalInput{
		Request: input.Request, Packet: planned.Packet, Contents: planned.Contents, Diff: planned.Diff,
	})
	if err != nil {
		return contracts.ChangeProposal{}, false, PlannedChange{}, err
	}
	return proposal, duplicate, planned, nil
}

// Approve persists an approval decision over an exact packet hash.
func (s *Service) Approve(ctx context.Context, request contracts.ChangeApprovalRequest) (contracts.ChangeApproval, contracts.ChangeProposal, bool, error) {
	return s.Store.Approve(ctx, request)
}

// Apply admits an approved packet, performs bounded structured filesystem
// operations, verifies the affected post-state, and finalizes the result.
// Multi-operation partial effects are classified as recovery_required.
func (s *Service) Apply(ctx context.Context, request contracts.ChangeApplicationRequest, root string) (contracts.ChangeApplication, contracts.ChangeProposal, error) {
	if request.DryRun {
		proposal, err := s.Store.Get(ctx, request.WorkspaceID, request.ProposalID)
		if err != nil {
			return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
		}
		if strings.ToLower(strings.TrimSpace(request.PacketHash)) != proposal.PacketHash {
			return contracts.ChangeApplication{}, contracts.ChangeProposal{}, store.ErrChangePacketMismatch
		}
		if filepath.Clean(strings.TrimSpace(root)) != filepath.Clean(strings.TrimSpace(proposal.Source.SourceRoot)) {
			return contracts.ChangeApplication{}, contracts.ChangeProposal{}, fmt.Errorf("repository mount does not match proposal source")
		}
		packet := proposalPacket(proposal)
		result, err := (Executor{}).Apply(ctx, root, packet, s.contentResolver(), true)
		return contracts.ChangeApplication{WorkspaceID: proposal.WorkspaceID, ProposalID: proposal.ID, PacketHash: proposal.PacketHash, Status: proposal.Status, ExpectedTreeHash: proposal.ExpectedTreeHash, ResultTreeHash: result.ResultTreeHash, Operations: proposal.Operations, Actor: request.Actor}, proposal, err
	}
	application, proposal, duplicate, err := s.Store.BeginApplication(ctx, request)
	if err != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, err
	}
	if duplicate && application.Status != contracts.ChangeApplying {
		return s.attachReceipt(ctx, application, proposal)
	}
	if filepath.Clean(strings.TrimSpace(root)) != filepath.Clean(strings.TrimSpace(proposal.Source.SourceRoot)) {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, fmt.Errorf("repository mount does not match proposal source")
	}
	packet := proposalPacket(proposal)
	resolver := s.contentResolver()
	result, applyErr := (Executor{}).Apply(ctx, root, packet, resolver, request.DryRun)
	if applyErr != nil {
		status := contracts.ChangeFailed
		failure := &contracts.ChangeFailure{Code: contracts.ChangeFailureFilesystem, Message: boundedFailure(applyErr)}
		if result.Conflict != nil || errors.Is(applyErr, ErrSourceConflict) {
			status = contracts.ChangeConflicted
			failure = &contracts.ChangeFailure{Code: contracts.ChangeFailureConflict, Message: "source or post-state precondition conflict"}
		} else if result.AppliedOperations > 0 {
			status = contracts.ChangeRecoveryRequired
			failure = &contracts.ChangeFailure{Code: contracts.ChangeFailureRecovery, Message: "filesystem effect may be partial; operator reconciliation required"}
		}
		finalized, finalProposal, finalizeErr := s.Store.FinalizeApplication(ctx, store.ChangeApplicationFinalizeInput{
			WorkspaceID: request.WorkspaceID, ApplicationID: application.ID, ProposalID: proposal.ID,
			PacketHash: request.PacketHash, Status: status, ResultTreeHash: result.ResultTreeHash,
			Conflict: result.Conflict, Failure: failure, Actor: request.Actor,
			TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence,
		})
		if finalizeErr != nil {
			return contracts.ChangeApplication{}, contracts.ChangeProposal{}, fmt.Errorf("finalize failed change application: %w (original: %v)", finalizeErr, applyErr)
		}
		return finalized, finalProposal, applyErr
	}
	finalized, finalProposal, finalizeErr := s.Store.FinalizeApplication(ctx, store.ChangeApplicationFinalizeInput{
		WorkspaceID: request.WorkspaceID, ApplicationID: application.ID, ProposalID: proposal.ID,
		PacketHash: request.PacketHash, Status: contracts.ChangeApplied, ResultTreeHash: result.ResultTreeHash,
		Actor: request.Actor, TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence,
	})
	if finalizeErr != nil {
		return contracts.ChangeApplication{}, contracts.ChangeProposal{}, finalizeErr
	}
	return s.attachReceipt(ctx, finalized, finalProposal)
}

func (s *Service) attachReceipt(ctx context.Context, application contracts.ChangeApplication, proposal contracts.ChangeProposal) (contracts.ChangeApplication, contracts.ChangeProposal, error) {
	if s.Receipts == nil || application.Status != contracts.ChangeApplied {
		return application, proposal, nil
	}
	request := contracts.WorkReceiptFinalizeRequest{
		ReceiptID:          "change-receipt-" + application.ID,
		RequestID:          "change-receipt-request-" + application.ID,
		IdempotencyKey:     "change-receipt:" + application.WorkspaceID + ":" + application.ID,
		WorkspaceID:        application.WorkspaceID,
		Actor:              application.Actor,
		WorkKind:           contracts.WorkReceiptReferenceChangeApplication,
		WorkID:             application.ID,
		SourceManifestHash: proposal.Source.ManifestHash,
		ReplayHash:         application.PacketHash,
		Steps: []contracts.WorkReceiptStep{{
			Ordinal: 0, ID: "filesystem-application", Name: "verified repository application", Kind: "change_application", Status: "succeeded",
			SourceKind: contracts.WorkReceiptReferenceChangeApplication, SourceID: application.ID, SourceHash: application.PacketHash,
			OutputHash: application.ResultTreeHash, ExternalEffect: true, ExternalBoundary: "configured-filesystem-mount",
		}},
		References: []contracts.WorkReceiptReference{
			{WorkspaceID: application.WorkspaceID, Kind: contracts.WorkReceiptReferenceChangeApplication, SourceID: application.ID, Hash: application.PacketHash},
			{WorkspaceID: application.WorkspaceID, Kind: contracts.WorkReceiptReferenceChangeProposal, SourceID: proposal.ID, Hash: proposal.PacketHash},
		},
	}
	for _, operation := range proposal.Operations {
		if operation.NewContentArtifact == nil {
			continue
		}
		artifact := operation.NewContentArtifact
		request.Artifacts = append(request.Artifacts, contracts.WorkReceiptArtifact{ID: artifact.ID, ArtifactID: artifact.ArtifactID, WorkspaceID: artifact.WorkspaceID, ContentHash: artifact.ContentHash, SourceKind: artifact.SourceKind, SourceID: artifact.SourceID, Role: artifact.Role})
		request.References = append(request.References, contracts.WorkReceiptReference{WorkspaceID: application.WorkspaceID, Kind: contracts.WorkReceiptReferenceArtifact, SourceID: strconv.FormatInt(artifact.ArtifactID, 10), Role: artifact.Role, Hash: artifact.ContentHash})
	}
	if proposal.DiffArtifact != nil {
		artifact := proposal.DiffArtifact
		request.Artifacts = append(request.Artifacts, contracts.WorkReceiptArtifact{ID: artifact.ID, ArtifactID: artifact.ArtifactID, WorkspaceID: artifact.WorkspaceID, ContentHash: artifact.ContentHash, SourceKind: artifact.SourceKind, SourceID: artifact.SourceID, Role: artifact.Role})
		request.References = append(request.References, contracts.WorkReceiptReference{WorkspaceID: application.WorkspaceID, Kind: contracts.WorkReceiptReferenceArtifact, SourceID: strconv.FormatInt(artifact.ArtifactID, 10), Role: artifact.Role, Hash: artifact.ContentHash})
	}
	receipt, _, err := s.Receipts.Finalize(ctx, request)
	if err != nil {
		return application, proposal, fmt.Errorf("finalize verified change packet: %w", err)
	}
	application.Receipt = &receipt
	return application, proposal, nil
}

func proposalPacket(proposal contracts.ChangeProposal) contracts.ChangePacket {
	return contracts.ChangePacket{
		SchemaVersion:    contracts.ChangeSchemaVersion,
		WorkspaceID:      proposal.WorkspaceID,
		Repository:       proposal.Repository,
		Source:           proposal.Source,
		Operations:       proposal.Operations,
		Budgets:          proposal.Budgets,
		ExpectedTreeHash: proposal.ExpectedTreeHash,
	}
}

// Get returns one workspace-scoped proposal.
func (s *Service) Get(ctx context.Context, workspaceID, proposalID string) (contracts.ChangeProposal, error) {
	return s.Store.Get(ctx, workspaceID, proposalID)
}

// Disclose returns a hash-preserving, bounded change view.
func (s *Service) Disclose(ctx context.Context, request contracts.ChangeDisclosureRequest) (contracts.ChangeDisclosureResult, error) {
	return s.Store.Disclose(ctx, request)
}

func (s *Service) plan(ctx context.Context, input PlanInput) (PlannedChange, error) {
	if s == nil || s.Store == nil || s.Artifacts == nil {
		return PlannedChange{}, errors.New("repository change service is not configured")
	}
	request, err := input.Request.Normalize()
	if err != nil {
		return PlannedChange{}, err
	}
	root := filepath.Clean(strings.TrimSpace(input.Root))
	if root == "" || !filepath.IsAbs(root) {
		return PlannedChange{}, fmt.Errorf("configured repository root must be absolute")
	}
	paths := make([]string, 0, len(request.Operations)*2)
	for _, operation := range request.Operations {
		paths = append(paths, operation.Path)
		if strings.TrimSpace(operation.Destination) != "" {
			paths = append(paths, operation.Destination)
		}
	}
	source, err := CaptureSnapshot(ctx, request.WorkspaceID, request.Repository, root, paths, request.Actor)
	if err != nil {
		return PlannedChange{}, err
	}
	source.Task, source.Session, source.AgentRun = request.Task, request.Session, request.AgentRun
	source.TaskOwnerID, source.TaskFence = request.TaskOwnerID, request.TaskFence
	request.Source = source
	return Plan(request, source)
}

func (s *Service) contentResolver() ContentResolver {
	return func(ctx context.Context, workspaceID, contentHash string) ([]byte, error) {
		if s.Artifacts == nil {
			return nil, errors.New("artifact store is not configured")
		}
		raw, err := s.Artifacts.ReadRawByHash(ctx, workspaceID, contentHash, contracts.MaxChangeFileBytes)
		if err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			return nil, fmt.Errorf("change content artifact disclosure is truncated")
		}
		return raw, nil
	}
}

func boundedFailure(err error) string {
	message := strings.TrimSpace(err.Error())
	if len(message) > 512 {
		message = message[:512]
	}
	return message
}
