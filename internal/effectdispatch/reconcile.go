package effectdispatch

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

var (
	ErrVerificationFence       = errors.New("effect verification fence is stale")
	ErrVerificationState       = errors.New("effect is not ready for verification")
	ErrVerificationConflict    = errors.New("effect verification outcome conflicts with durable state")
	ErrVerificationProofNeeded = errors.New("effect verification proof is required")
)

// ReconcileRequest is the local, fenced half of effect verification. The
// adapter proof is computed before this request is submitted; this method
// performs no external I/O and only appends authoritative local transitions.
type ReconcileRequest struct {
	WorkspaceID         string
	OperationID         string
	EffectID            string
	LinkID              string
	OwnerID             string
	EffectFence         uint64
	ExpectedEffectVer   int64
	ExpectedLinkVersion int64
	RequestID           string
	IdempotencyKey      string
	Actor               contracts.ActorRef
	Outcome             contracts.EffectVerificationResult
}

// ReconcileResult contains the effect projection and link projection after a
// local commit. It never includes provider payloads.
type ReconcileResult struct {
	State       store.EffectState
	Link        contracts.DomainEffectLink
	LinkVersion int64
	Duplicate   bool
}

func (r *ReconcileRequest) normalize() error {
	if r == nil {
		return fmt.Errorf("%w: request is nil", ErrVerificationFence)
	}
	if strings.TrimSpace(r.WorkspaceID) == "" || strings.TrimSpace(r.OperationID) == "" || strings.TrimSpace(r.EffectID) == "" || strings.TrimSpace(r.LinkID) == "" || strings.TrimSpace(r.OwnerID) == "" || strings.TrimSpace(r.RequestID) == "" || strings.TrimSpace(r.IdempotencyKey) == "" || r.EffectFence == 0 || r.ExpectedEffectVer < 1 || r.ExpectedLinkVersion < 1 {
		return fmt.Errorf("%w: identity, fence, version, and idempotency are required", ErrVerificationFence)
	}
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	r.OperationID, r.EffectID, r.LinkID, r.OwnerID, r.RequestID, r.IdempotencyKey = strings.TrimSpace(r.OperationID), strings.TrimSpace(r.EffectID), strings.TrimSpace(r.LinkID), strings.TrimSpace(r.OwnerID), strings.TrimSpace(r.RequestID), strings.TrimSpace(r.IdempotencyKey)
	if err := r.Outcome.Normalize(); err != nil {
		return fmt.Errorf("%w: %v", ErrVerificationProofNeeded, err)
	}
	if err := normalizeReconcileActor(&r.Actor, r.WorkspaceID); err != nil {
		return err
	}
	return nil
}

// Reconcile commits the generic effect transition and matching domain-link
// transition atomically. The effect recovery lease is checked by
// AdmissionStore.UpdateEffectTx inside the transaction, so a stale worker
// cannot publish a proof discovered under an older fence.
func (d *Dispatcher) Reconcile(ctx context.Context, request ReconcileRequest) (ReconcileResult, error) {
	if d == nil || d.Operations == nil || d.Admission == nil || d.Links == nil {
		return ReconcileResult{}, ErrNotConfigured
	}
	if err := request.normalize(); err != nil {
		return ReconcileResult{}, err
	}
	effect, err := d.Operations.GetEffect(ctx, request.WorkspaceID, request.EffectID)
	if err != nil {
		return ReconcileResult{}, err
	}
	if effect.OperationID != request.OperationID || effect.WorkspaceID != request.WorkspaceID {
		return ReconcileResult{}, store.ErrOperationWorkspace
	}
	state, err := d.Admission.GetEffectState(ctx, request.WorkspaceID, request.EffectID)
	if err != nil {
		return ReconcileResult{}, err
	}
	link, err := d.Links.Get(ctx, request.WorkspaceID, request.LinkID)
	if err != nil {
		return ReconcileResult{}, err
	}
	if link.OperationID != request.OperationID || link.EffectID != request.EffectID {
		return ReconcileResult{}, store.ErrDomainEffectLinkStale
	}
	currentLink, err := d.Links.CurrentByDomain(ctx, request.WorkspaceID, link.DomainKind, link.DomainID, link.LinkRole)
	if err != nil {
		return ReconcileResult{}, err
	}
	if currentLink.Link.ID != request.LinkID {
		return ReconcileResult{}, store.ErrDomainEffectLinkStale
	}
	if err := d.Admission.ValidateEffectLease(ctx, store.EffectLease{
		WorkspaceID: request.WorkspaceID, EffectID: request.EffectID,
		OwnerID: request.OwnerID, Fence: request.EffectFence,
	}); err != nil {
		return ReconcileResult{}, err
	}

	// A prior final commit is a safe duplicate only when the requested outcome
	// agrees with both durable authorities. No adapter is called here.
	if duplicate, ok := reconcileAlreadyCommitted(state, currentLink, request.Outcome); ok {
		return ReconcileResult{State: state, Link: duplicate, LinkVersion: currentLink.Transition.Version, Duplicate: true}, nil
	}
	if state.State == contracts.ExternalEffectVerificationPending {
		if state.Version != request.ExpectedEffectVer && state.Version != request.ExpectedEffectVer+1 {
			stale := fmt.Errorf("%w: pending effect version %d, expected %d or %d", ErrVerificationFence, state.Version, request.ExpectedEffectVer, request.ExpectedEffectVer+1)
			return d.duplicateOrStaleReconcile(ctx, link, request, stale)
		}
	} else if state.Version != request.ExpectedEffectVer {
		stale := fmt.Errorf("%w: effect version %d, expected %d", ErrVerificationFence, state.Version, request.ExpectedEffectVer)
		return d.duplicateOrStaleReconcile(ctx, link, request, stale)
	}
	if currentLink.Transition.Version != request.ExpectedLinkVersion {
		stale := fmt.Errorf("%w: link version %d, expected %d", ErrVerificationFence, currentLink.Transition.Version, request.ExpectedLinkVersion)
		return d.duplicateOrStaleReconcile(ctx, link, request, stale)
	}

	// Recovery and acknowledged states must first become verification_pending.
	// This is a separate durable step so a crash has an explicit, resumable
	// state rather than an implicit in-flight proof.
	if state.State == contracts.ExternalEffectAcknowledged || state.State == contracts.ExternalEffectRecoveryRequired {
		pending, pendingErr := d.Admission.UpdateEffect(ctx, contracts.ExternalEffectUpdate{
			WorkspaceID: request.WorkspaceID, OperationID: request.OperationID, EffectID: request.EffectID,
			OwnerID: request.OwnerID, Fence: request.EffectFence, LeaseKind: "effect",
			RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey + ":pending",
			State: contracts.ExternalEffectVerificationPending, ProviderRequestID: request.Outcome.ProviderRequestID,
		})
		if pendingErr != nil {
			return ReconcileResult{}, pendingErr
		}
		state = pending.State
	}
	if state.State != contracts.ExternalEffectVerificationPending {
		return ReconcileResult{}, fmt.Errorf("%w: state %q", ErrVerificationState, state.State)
	}

	toState, linkStatus, failureCode := reconcileStates(request.Outcome)
	finalUpdate := contracts.ExternalEffectUpdate{
		WorkspaceID: request.WorkspaceID, OperationID: request.OperationID, EffectID: request.EffectID,
		OwnerID: request.OwnerID, Fence: request.EffectFence, LeaseKind: "effect",
		RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey + ":final", State: toState,
		ProviderRequestID: request.Outcome.ProviderRequestID, ResponseHash: request.Outcome.ResultHash,
		VerificationHash: request.Outcome.VerificationHash, FailureCode: failureCode,
	}
	var finalState store.EffectState
	var finalLink contracts.DomainEffectLink
	var finalLinkVersion int64
	var finalDuplicate bool
	err = d.Admission.WithWorkspaceTx(ctx, request.WorkspaceID, func(tx pgx.Tx) error {
		updated, updateErr := d.Admission.UpdateEffectTx(ctx, tx, finalUpdate)
		if updateErr != nil {
			return updateErr
		}
		finalState = updated.State
		finalDuplicate = updated.Duplicate
		current, linkErr := d.Links.CurrentTx(ctx, tx, request.WorkspaceID, request.LinkID)
		if linkErr != nil {
			return linkErr
		}
		if updated.Duplicate {
			duplicateLink, ok := reconcileAlreadyCommitted(finalState, current, request.Outcome)
			if !ok {
				return ErrVerificationConflict
			}
			finalLink = duplicateLink
			finalLinkVersion = current.Transition.Version
			return nil
		}
		if current.Transition.Version != request.ExpectedLinkVersion || current.Transition.ToStatus != currentLink.Transition.ToStatus {
			return store.ErrDomainEffectLinkFenced
		}
		finalLink = current.Link
		if current.Transition.ToStatus == linkStatus {
			finalLink = current.Link
			finalLink.Status = current.Transition.ToStatus
			finalLinkVersion = current.Transition.Version
			return nil
		}
		if linkStatus == contracts.DomainEffectLinkStatusRecoveryRequired && current.Transition.ToStatus == contracts.DomainEffectLinkStatusRecoveryRequired {
			finalLink = current.Link
			finalLink.Status = current.Transition.ToStatus
			finalLinkVersion = current.Transition.Version
			return nil
		}
		transition, transitionErr := d.Links.TransitionTx(ctx, tx, contracts.DomainEffectLinkTransitionRequest{
			WorkspaceID: request.WorkspaceID, LinkID: request.LinkID,
			ExpectedVersion: current.Transition.Version, FromStatus: current.Transition.ToStatus,
			ToStatus: linkStatus, ProviderRequestID: request.Outcome.ProviderRequestID,
			ResultHash: request.Outcome.ResultHash, FailureCode: failureCode,
			IdempotencyKey: request.IdempotencyKey + ":link", Actor: request.Actor,
		})
		if transitionErr != nil {
			return transitionErr
		}
		finalLink.Status = transition.Link.Status
		finalLinkVersion = transition.Transition.Version
		return nil
	})
	if err != nil {
		return ReconcileResult{}, err
	}
	return ReconcileResult{State: finalState, Link: finalLink, LinkVersion: finalLinkVersion, Duplicate: finalDuplicate}, nil
}

// duplicateOrStaleReconcile closes a read-skew race between the independently
// read effect projection and domain-link projection. The final effect and link
// transitions commit atomically, but the initial preflight reads are separate
// transactions; a concurrent duplicate can therefore observe one projection
// before and the other after the commit. Only the exact final idempotency
// transition can turn that stale read into a duplicate success.
func (d *Dispatcher) duplicateOrStaleReconcile(ctx context.Context, link contracts.DomainEffectLink, request ReconcileRequest, stale error) (ReconcileResult, error) {
	transition, err := d.Admission.GetEffectTransition(ctx, request.WorkspaceID, request.EffectID, request.IdempotencyKey+":final")
	if errors.Is(err, store.ErrAdmissionNotFound) {
		return ReconcileResult{}, stale
	}
	if err != nil {
		return ReconcileResult{}, fmt.Errorf("check committed effect reconciliation: %w", err)
	}

	state, err := d.Admission.GetEffectState(ctx, request.WorkspaceID, request.EffectID)
	if err != nil {
		return ReconcileResult{}, err
	}
	current, err := d.Links.CurrentByDomain(ctx, request.WorkspaceID, link.DomainKind, link.DomainID, link.LinkRole)
	if err != nil {
		return ReconcileResult{}, err
	}
	if current.Link.ID != request.LinkID {
		return ReconcileResult{}, store.ErrDomainEffectLinkStale
	}
	toState, _, failureCode := reconcileStates(request.Outcome)
	if transition.ToState != toState || transition.ResponseHash != request.Outcome.ResultHash || transition.VerificationHash != request.Outcome.VerificationHash || transition.FailureCode != failureCode || (request.Outcome.ProviderRequestID != "" && transition.ProviderRequestID != request.Outcome.ProviderRequestID) {
		return ReconcileResult{}, ErrVerificationConflict
	}
	resultLink, committed := reconcileAlreadyCommitted(state, current, request.Outcome)
	if !committed {
		return ReconcileResult{}, ErrVerificationConflict
	}
	return ReconcileResult{State: state, Link: resultLink, LinkVersion: current.Transition.Version, Duplicate: true}, nil
}

func reconcileStates(outcome contracts.EffectVerificationResult) (effectState, linkStatus, failureCode string) {
	switch outcome.Status {
	case contracts.EffectVerificationStatusVerified:
		return contracts.ExternalEffectVerified, contracts.DomainEffectLinkStatusReconciled, ""
	case contracts.EffectVerificationStatusFailed:
		return contracts.ExternalEffectVerificationFailed, contracts.DomainEffectLinkStatusRecoveryRequired, outcome.FailureCode
	default:
		return contracts.ExternalEffectRecoveryRequired, contracts.DomainEffectLinkStatusRecoveryRequired, outcome.FailureCode
	}
}

func reconcileAlreadyCommitted(state store.EffectState, current store.DomainEffectLinkCurrent, outcome contracts.EffectVerificationResult) (contracts.DomainEffectLink, bool) {
	if outcome.Status == contracts.EffectVerificationStatusVerified && state.State == contracts.ExternalEffectVerified && current.Transition.ToStatus == contracts.DomainEffectLinkStatusReconciled && current.Transition.ResultHash == outcome.ResultHash && state.VerificationHash == outcome.VerificationHash && (outcome.ProviderRequestID == "" || state.ProviderRequestID == outcome.ProviderRequestID) {
		current.Link.Status = current.Transition.ToStatus
		return current.Link, true
	}
	if outcome.Status == contracts.EffectVerificationStatusFailed && state.State == contracts.ExternalEffectVerificationFailed && state.FailureCode == outcome.FailureCode && current.Transition.ToStatus == contracts.DomainEffectLinkStatusRecoveryRequired {
		current.Link.Status = current.Transition.ToStatus
		return current.Link, true
	}
	if outcome.Status == contracts.EffectVerificationStatusUnknown && state.State == contracts.ExternalEffectRecoveryRequired && state.FailureCode == outcome.FailureCode && current.Transition.ToStatus == contracts.DomainEffectLinkStatusRecoveryRequired {
		current.Link.Status = current.Transition.ToStatus
		return current.Link, true
	}
	return contracts.DomainEffectLink{}, false
}

func normalizeReconcileActor(actor *contracts.ActorRef, workspaceID string) error {
	if actor == nil || strings.TrimSpace(actor.ID) == "" || strings.TrimSpace(actor.WorkspaceID) != workspaceID {
		return fmt.Errorf("effect reconciliation actor is not workspace-scoped")
	}
	return nil
}
