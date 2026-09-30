package generic

import (
	"context"
	"errors"
	"fmt"

	connectorruntime "github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/effectdispatch"
	"github.com/omaveda/fornix/internal/store"
)

var (
	ErrVerifierNotConfigured = errors.New("workflow effect verifier is not configured")
	ErrVerifierUnavailable   = errors.New("workflow effect verifier is unavailable")
	ErrVerifierStep          = errors.New("workflow step is not awaiting effect verification")
)

// EffectVerifier is the provider-neutral proof seam. Implementations may
// inspect a remote system, but receive only bounded, hash-addressed facts and
// must return a normalized proof classification.
type EffectVerifier interface {
	VerifyEffect(context.Context, contracts.EffectVerificationRequest) (contracts.EffectVerificationResult, error)
}

// ConnectorEffectVerifier resolves a verifier from the already admitted
// capability catalog. It does not permit a request to select an arbitrary
// verifier or connector at runtime.
type ConnectorEffectVerifier struct {
	Registry *connectorruntime.Registry
}

func NewConnectorEffectVerifier(registry *connectorruntime.Registry) ConnectorEffectVerifier {
	return ConnectorEffectVerifier{Registry: registry}
}

func (v ConnectorEffectVerifier) VerifyEffect(ctx context.Context, request contracts.EffectVerificationRequest) (contracts.EffectVerificationResult, error) {
	if v.Registry == nil {
		return contracts.EffectVerificationResult{}, ErrVerifierNotConfigured
	}
	capability, ok := v.Registry.Lookup(request.Operation.Capability)
	if !ok {
		return contracts.EffectVerificationResult{}, ErrVerifierUnavailable
	}
	verifier, ok := capability.(connectorruntime.EffectVerifier)
	if !ok {
		return contracts.EffectVerificationResult{}, ErrVerifierUnavailable
	}
	result, err := verifier.VerifyEffect(ctx, request)
	if err != nil {
		// Do not surface provider error text. The adapter may wrap credentials,
		// URLs, or raw responses; the durable outcome is classified separately.
		return contracts.EffectVerificationResult{}, ErrVerifierUnavailable
	}
	if err := result.Normalize(); err != nil {
		return contracts.EffectVerificationResult{}, fmt.Errorf("normalize verifier result: %w", err)
	}
	return result, nil
}

// VerifyEffect proves one waiting workflow step, reconciles the local effect
// and domain link under the recovery fence, then resumes the workflow under
// its separate operation/task fence. The verifier is called before any local
// mutation; replay and duplicate finalization never call it again.
func (s *Service) VerifyEffect(ctx context.Context, workspaceID, runID string, actor contracts.ActorRef, workflowFence, taskFence uint64, request contracts.WorkflowVerifyRequest) (contracts.WorkflowVerificationResult, error) {
	if err := s.validate(); err != nil {
		return contracts.WorkflowVerificationResult{}, err
	}
	if s.Effects == nil || s.Verifier == nil {
		return contracts.WorkflowVerificationResult{}, ErrVerifierNotConfigured
	}
	if err := request.Normalize(); err != nil {
		return contracts.WorkflowVerificationResult{}, err
	}
	run, err := s.Store.Get(ctx, workspaceID, runID)
	if err != nil {
		return contracts.WorkflowVerificationResult{}, err
	}
	if actor.ID == "" || actor.WorkspaceID != run.WorkspaceID {
		return contracts.WorkflowVerificationResult{}, ErrActorRequired
	}
	var step contracts.WorkflowStepState
	found := false
	for _, candidate := range run.Steps {
		if candidate.StepID == request.StepID {
			step, found = candidate, true
			break
		}
	}
	if !found || (step.Status != contracts.WorkflowStepAwaitingExternal && step.Status != contracts.WorkflowStepRecoveryRequired) || step.Effect == nil {
		return contracts.WorkflowVerificationResult{}, ErrVerifierStep
	}
	currentLink, err := s.Effects.Links.CurrentByDomain(ctx, workspaceID, contracts.DomainEffectKindWorkflowStep, run.ID+":"+step.StepID, contracts.DomainEffectLinkRolePrimary)
	if err != nil {
		return contracts.WorkflowVerificationResult{}, err
	}
	state, err := s.Effects.Admission.GetEffectState(ctx, workspaceID, step.Effect.ID)
	if err != nil {
		return contracts.WorkflowVerificationResult{}, err
	}
	if err := s.Effects.Admission.ValidateEffectLease(ctx, store.EffectLease{
		WorkspaceID: workspaceID, EffectID: step.Effect.ID, OwnerID: actor.ID, Fence: request.EffectFence,
	}); err != nil {
		return contracts.WorkflowVerificationResult{}, err
	}
	prior, priorErr := s.Effects.Admission.GetEffectTransition(ctx, workspaceID, step.Effect.ID, request.IdempotencyKey+":final")
	if priorErr == nil {
		if prior.OperationID != run.Operation.ID || prior.LeaseKind != "effect" || prior.RequestID != "workflow-verify:"+request.IdempotencyKey || prior.Version > state.Version {
			return contracts.WorkflowVerificationResult{}, effectVerificationConflict("idempotency result is not bound to the current effect")
		}
		outcome, outcomeErr := effectVerificationOutcome(prior)
		if outcomeErr != nil {
			return contracts.WorkflowVerificationResult{}, outcomeErr
		}
		resumed := run
		// Reconcile commits the effect and link before the workflow checkpoint.
		// Resume only when this historical result is still the current effect
		// version; an older unknown result must never overwrite a later retry.
		if prior.Version == state.Version && (step.Status == contracts.WorkflowStepAwaitingExternal || step.Status == contracts.WorkflowStepRecoveryRequired) {
			resumedEffect := *step.Effect
			resumedEffect.VerificationStatus = outcome.Status
			if outcome.ProviderRequestID != "" {
				resumedEffect.ProviderRequestID = outcome.ProviderRequestID
			}
			stepResult := workflowStepResultForVerification(step, outcome)
			stepResult.Effect = &resumedEffect
			resumed, err = s.resume(ctx, workspaceID, runID, actor, workflowFence, taskFence, contracts.WorkflowResumeRequest{
				StepID: request.StepID, Result: stepResult,
			}, true)
			if err != nil {
				return contracts.WorkflowVerificationResult{}, err
			}
		}
		return contracts.WorkflowVerificationResult{
			Run: resumed, Outcome: outcome, EffectState: prior.ToState, EffectVersion: prior.Version,
			LinkID: currentLink.Link.ID, LinkStatus: currentLink.Transition.ToStatus,
			LinkVersion: currentLink.Transition.Version, Duplicate: true,
		}, nil
	} else if !errors.Is(priorErr, store.ErrAdmissionNotFound) {
		return contracts.WorkflowVerificationResult{}, priorErr
	}
	if durable, ok, durableErr := durableVerificationOutcome(state, currentLink); durableErr != nil {
		return contracts.WorkflowVerificationResult{}, durableErr
	} else if ok {
		resumed := run
		if step.Status == contracts.WorkflowStepAwaitingExternal {
			resumedEffect := *step.Effect
			resumedEffect.VerificationStatus = durable.Status
			if durable.ProviderRequestID != "" {
				resumedEffect.ProviderRequestID = durable.ProviderRequestID
			}
			stepResult := workflowStepResultForVerification(step, durable)
			stepResult.Effect = &resumedEffect
			var resumeErr error
			resumed, resumeErr = s.resume(ctx, workspaceID, runID, actor, workflowFence, taskFence, contracts.WorkflowResumeRequest{
				StepID: request.StepID, Result: stepResult,
			}, true)
			if resumeErr != nil {
				return contracts.WorkflowVerificationResult{}, resumeErr
			}
		}
		return contracts.WorkflowVerificationResult{
			Run: resumed, Outcome: durable, EffectState: state.State, EffectVersion: state.Version,
			LinkID: currentLink.Link.ID, LinkStatus: currentLink.Transition.ToStatus,
			LinkVersion: currentLink.Transition.Version, Duplicate: true,
		}, nil
	}
	reservation, err := s.Effects.Operations.GetEffect(ctx, workspaceID, step.Effect.ID)
	if err != nil {
		return contracts.WorkflowVerificationResult{}, err
	}
	operation, err := s.Effects.Operations.Get(ctx, workspaceID, run.Operation.ID)
	if err != nil {
		return contracts.WorkflowVerificationResult{}, err
	}
	if operation.OperationHash != currentLink.Link.OperationHash {
		return contracts.WorkflowVerificationResult{}, store.ErrDomainEffectLinkStale
	}
	effect := contracts.ExternalEffect{
		SchemaVersion:        contracts.DomainNeutralSchemaVersion,
		ID:                   reservation.EffectID,
		WorkspaceID:          reservation.WorkspaceID,
		Boundary:             reservation.Boundary,
		Class:                contracts.EffectClass(reservation.EffectClass),
		DeliveryGuarantee:    reservation.DeliverySemantics,
		IdempotencyKey:       reservation.IdempotencyKey,
		ProviderRequestID:    reservation.ProviderRequestID,
		ProviderIdempotency:  reservation.ProviderIdempotency,
		VerificationRequired: reservation.VerificationRequired,
		VerificationStatus:   reservation.VerificationStatus,
		CompensationStatus:   reservation.CompensationStatus,
	}
	if err := effect.Normalize(); err != nil {
		return contracts.WorkflowVerificationResult{}, fmt.Errorf("normalize reserved effect: %w", err)
	}
	proofRequest := contracts.EffectVerificationRequest{
		WorkspaceID: workspaceID, OperationID: run.Operation.ID, RunID: run.ID, StepID: step.StepID,
		EffectID: step.Effect.ID, OperationHash: currentLink.Link.OperationHash, OperationRequestHash: operation.RequestHash, Operation: operation.Request,
		Effect: effect, Link: currentLink.Link, EffectState: state.State, EffectVersion: state.Version,
		LinkVersion: currentLink.Transition.Version, Actor: actor, IdempotencyKey: request.IdempotencyKey,
	}
	if err := proofRequest.Normalize(); err != nil {
		return contracts.WorkflowVerificationResult{}, err
	}
	outcome, err := s.Verifier.VerifyEffect(ctx, proofRequest)
	if err != nil {
		return contracts.WorkflowVerificationResult{}, err
	}
	if err := outcome.Normalize(); err != nil {
		return contracts.WorkflowVerificationResult{}, err
	}
	reconciled, err := s.Effects.Reconcile(ctx, effectdispatchRequest(request, workspaceID, run, currentLink, actor, outcome))
	if err != nil {
		return contracts.WorkflowVerificationResult{}, err
	}
	resumedEffect := effect
	resumedEffect.VerificationStatus = outcome.Status
	if outcome.ProviderRequestID != "" {
		resumedEffect.ProviderRequestID = outcome.ProviderRequestID
	}
	stepResult := workflowStepResultForVerification(step, outcome)
	stepResult.Effect = &resumedEffect
	resumed, err := s.resume(ctx, workspaceID, runID, actor, workflowFence, taskFence, contracts.WorkflowResumeRequest{StepID: request.StepID, Result: stepResult}, true)
	if err != nil {
		return contracts.WorkflowVerificationResult{}, err
	}
	return contracts.WorkflowVerificationResult{Run: resumed, Outcome: outcome, EffectState: reconciled.State.State, EffectVersion: reconciled.State.Version, LinkID: reconciled.Link.ID, LinkStatus: reconciled.Link.Status, LinkVersion: reconciled.LinkVersion, Duplicate: reconciled.Duplicate}, nil
}

func workflowStepResultForVerification(step contracts.WorkflowStepState, outcome contracts.EffectVerificationResult) contracts.WorkflowStepResult {
	result := contracts.WorkflowStepResult{ExternalEffectStarted: true}
	switch outcome.Status {
	case contracts.EffectVerificationStatusVerified:
		result.Status = contracts.WorkflowStepSucceeded
		result.OutputHash = outcome.ResultHash
	case contracts.EffectVerificationStatusFailed:
		result.Status = contracts.WorkflowStepFailed
		result.Failure = &contracts.WorkflowFailure{Code: outcome.FailureCode, External: true, Attempt: step.Attempt}
	default:
		result.Status = contracts.WorkflowStepRecoveryRequired
		result.Failure = &contracts.WorkflowFailure{Code: outcome.FailureCode, External: true, Retryable: true, Attempt: step.Attempt}
	}
	return result
}

func durableVerificationOutcome(state store.EffectState, link store.DomainEffectLinkCurrent) (contracts.EffectVerificationResult, bool, error) {
	var result contracts.EffectVerificationResult
	switch state.State {
	case contracts.ExternalEffectVerified:
		if link.Transition.ToStatus != contracts.DomainEffectLinkStatusReconciled {
			return result, false, effectVerificationConflict("verified effect has no reconciled link")
		}
		result = contracts.EffectVerificationResult{Status: contracts.EffectVerificationStatusVerified, ResultHash: state.ResponseHash, VerificationHash: state.VerificationHash, ProviderRequestID: state.ProviderRequestID}
	case contracts.ExternalEffectVerificationFailed:
		if link.Transition.ToStatus != contracts.DomainEffectLinkStatusRecoveryRequired {
			return result, false, effectVerificationConflict("failed effect has no recovery link")
		}
		result = contracts.EffectVerificationResult{Status: contracts.EffectVerificationStatusFailed, ProviderRequestID: state.ProviderRequestID, FailureCode: state.FailureCode}
	default:
		return result, false, nil
	}
	if err := result.Normalize(); err != nil {
		return contracts.EffectVerificationResult{}, false, effectVerificationConflict(err.Error())
	}
	return result, true, nil
}

func effectVerificationOutcome(transition store.EffectTransition) (contracts.EffectVerificationResult, error) {
	var result contracts.EffectVerificationResult
	switch transition.ToState {
	case contracts.ExternalEffectVerified:
		result = contracts.EffectVerificationResult{
			Status: contracts.EffectVerificationStatusVerified, ResultHash: transition.ResponseHash,
			VerificationHash: transition.VerificationHash, ProviderRequestID: transition.ProviderRequestID,
		}
	case contracts.ExternalEffectVerificationFailed:
		result = contracts.EffectVerificationResult{
			Status: contracts.EffectVerificationStatusFailed, ProviderRequestID: transition.ProviderRequestID,
			FailureCode: transition.FailureCode,
		}
	case contracts.ExternalEffectRecoveryRequired:
		result = contracts.EffectVerificationResult{
			Status: contracts.EffectVerificationStatusUnknown, ProviderRequestID: transition.ProviderRequestID,
			FailureCode: transition.FailureCode,
		}
	default:
		return contracts.EffectVerificationResult{}, effectVerificationConflict("idempotency key does not identify a verification result")
	}
	if err := result.Normalize(); err != nil {
		return contracts.EffectVerificationResult{}, effectVerificationConflict(err.Error())
	}
	return result, nil
}

func effectVerificationConflict(reason string) error {
	return fmt.Errorf("%w: %s", effectdispatch.ErrVerificationConflict, reason)
}

func effectdispatchRequest(request contracts.WorkflowVerifyRequest, workspaceID string, run contracts.WorkflowRun, current store.DomainEffectLinkCurrent, actor contracts.ActorRef, outcome contracts.EffectVerificationResult) effectdispatch.ReconcileRequest {
	return effectdispatch.ReconcileRequest{
		WorkspaceID: workspaceID, OperationID: run.Operation.ID, EffectID: current.Link.EffectID, LinkID: current.Link.ID,
		OwnerID: actor.ID, EffectFence: request.EffectFence, ExpectedEffectVer: request.ExpectedEffectVer,
		ExpectedLinkVersion: request.ExpectedLinkVersion, RequestID: "workflow-verify:" + request.IdempotencyKey,
		IdempotencyKey: request.IdempotencyKey, Actor: actor, Outcome: outcome,
	}
}
