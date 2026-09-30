// Package effectdispatch composes Fornix's durable operation authorities into
// one provider-neutral external-effect boundary. It does not know whether an
// effect is an HTTP request, a tool process, a filesystem mutation, or a model
// call; the invoker is supplied by the owning domain adapter.
package effectdispatch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

var (
	ErrNotConfigured                   = errors.New("effect dispatcher is not configured")
	ErrAdmissionNotAllowed             = errors.New("effect admission is not allowed")
	ErrPlanMismatch                    = errors.New("effect does not match the authoritative operation plan")
	ErrEffectNotReturned               = errors.New("effectful adapter did not return its reserved effect")
	ErrUncertainOutcome                = errors.New("external effect outcome is uncertain")
	ErrDomainLinkRequired              = errors.New("effectful dispatch requires a domain-effect link")
	ErrDomainLinkRecoveryRequired      = errors.New("effect duplicate cannot verify its durable domain-effect link")
	ErrEffectDispatchInProgress        = errors.New("duplicate external effect dispatch is still in progress")
	ErrOperationResultRecoveryRequired = errors.New("operation result is missing for a finalized external effect; reconciliation is required")
)

// ExternalDispatchError is returned by an adapter when it cannot prove
// whether the external system accepted the request. The dispatcher records a
// recoverable state and never blindly retries that request.
type ExternalDispatchError struct {
	Err               error
	ProviderRequestID string
	ResponseHash      string
	PossiblyStarted   bool
	ContentEmitted    bool
}

func (e *ExternalDispatchError) Error() string {
	// The wrapped error is available to machine-readable callers through
	// Unwrap, but its text may contain provider payloads or credentials. Never
	// include it in a persisted or user-visible error string.
	return ErrUncertainOutcome.Error()
}

func (e *ExternalDispatchError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

// Is lets callers classify an uncertain external outcome without exposing or
// depending on provider error text.
func (e *ExternalDispatchError) Is(target error) bool {
	return target == ErrUncertainOutcome || (e != nil && errors.Is(e.Err, target))
}

// UncertainExternalOutcome reports whether the adapter could not prove that
// the external boundary was not crossed. The method is intentionally small so
// domain packages can classify recovery without importing this dispatcher and
// creating a package cycle.
func (e *ExternalDispatchError) UncertainExternalOutcome() bool {
	return e != nil && e.PossiblyStarted
}

// InvocationResult contains only normalized, hash-bound adapter facts. Raw
// payloads belong in the adapter's redacted evidence/artifact store.
type InvocationResult struct {
	Result                contracts.OperationResult
	ProviderRequestID     string
	ResponseHash          string
	ContentEmitted        bool
	ExternalEffectStarted bool
}

// Invoker is called only after the dispatcher has committed durable admission,
// attempt, effect reservation, and a dispatch intent atomically authorized by
// the live operation/task/agent-run fences.
type Invoker func(context.Context, contracts.EffectAuthority) (InvocationResult, error)

// TransactionFinalizer applies the specialized domain result in the same
// workspace transaction that verifies the generic effect and reconciles its
// domain link. It must only perform bounded local database work.
type TransactionFinalizer func(context.Context, pgx.Tx, string) error

// Request is the complete control-plane input. The caller may provide an
// operation lease fence and non-secret authority facts, but exact attempt and
// effect identities are derived and checked against Postgres by Dispatch.
type Request struct {
	Admission   contracts.AdmissionInput
	Operation   contracts.OperationRequest
	Definition  contracts.CapabilityDefinition
	OwnerID     string
	Fence       uint64
	TaskOwnerID string
	TaskFence   uint64
	StepID      string
	Attempt     int
	AttemptID   string
	AttemptKey  string
	Effect      contracts.ExternalEffect
	Authority   contracts.EffectAuthority
	// DomainLink is mandatory for effectful callers. The dispatcher derives
	// every generic identity and authority fact from Postgres; the caller may
	// provide only the specialized source identity and its hash.
	DomainLink   *contracts.DomainEffectLink
	Invoker      Invoker
	RecordResult bool
	FinalizeTx   TransactionFinalizer
}

// Result contains the durable prepare facts and, when requested, the
// operation result. Duplicate calls never invoke Invoker.
type Result struct {
	Admission       store.AdmissionResult
	Attempt         store.OperationAttempt
	Effect          store.OperationEffect
	State           store.EffectState
	OperationResult *store.OperationResultWrite
	Authority       contracts.EffectAuthority
	DomainLink      *contracts.DomainEffectLink
	Duplicate       bool
}

// Dispatcher is intentionally small. The stores remain the authorities; this
// type owns only sequencing and cross-domain invariants.
type Dispatcher struct {
	Operations *store.OperationStore
	Admission  *store.AdmissionStore
	Links      *store.DomainEffectLinkStore
	// beforeDispatchIntent is a nil-by-default scheduling seam used only by
	// deterministic concurrency tests. Production dispatchers leave it unset.
	beforeDispatchIntent func(duplicate bool)
}

// publishEffectAndLink commits one local effect transition and, when a link is
// supplied, its append-only terminal/recovery transition in one Postgres
// transaction. The external invocation has already completed before this
// helper is called.
func (d *Dispatcher) publishEffectAndLink(ctx context.Context, update contracts.ExternalEffectUpdate, link *contracts.DomainEffectLink, toStatus, resultHash, failureCode string, actor contracts.ActorRef) error {
	_, err := d.publishEffectLinkAndResult(ctx, update, link, toStatus, resultHash, failureCode, actor, nil, nil)
	return err
}

// publishEffectLinkAndResult commits the final generic effect state, optional
// domain-link transition, and optional operation result in one workspace
// transaction. The adapter has already returned, so this callback performs no
// external I/O and a rollback leaves the prior acknowledged state intact.
func (d *Dispatcher) publishEffectLinkAndResult(ctx context.Context, update contracts.ExternalEffectUpdate, link *contracts.DomainEffectLink, toStatus, resultHash, failureCode string, actor contracts.ActorRef, resultInput *store.OperationResultInput, finalizeTx TransactionFinalizer) (*store.OperationResultWrite, error) {
	if finalizeTx != nil && (link == nil || update.State != contracts.ExternalEffectVerified || toStatus != contracts.DomainEffectLinkStatusReconciled || resultHash == "") {
		return nil, fmt.Errorf("domain result finalizer requires a verified effect, reconciled link, and result hash")
	}
	if finalizeTx != nil && (resultInput == nil || resultInput.Result.OutputHash != resultHash) {
		return nil, fmt.Errorf("domain result finalizer requires the matching durable operation result")
	}
	var operationResult *store.OperationResultWrite
	err := d.Admission.WithWorkspaceTx(ctx, update.WorkspaceID, func(tx pgx.Tx) error {
		if _, err := d.Admission.UpdateEffectTx(ctx, tx, update); err != nil {
			return err
		}
		if link != nil && strings.TrimSpace(toStatus) != "" {
			current, err := d.Links.CurrentTx(ctx, tx, update.WorkspaceID, link.ID)
			if err != nil {
				return err
			}
			if _, err := d.Links.TransitionTx(ctx, tx, contracts.DomainEffectLinkTransitionRequest{
				WorkspaceID: update.WorkspaceID, LinkID: current.Link.ID,
				ExpectedVersion: current.Transition.Version, FromStatus: current.Transition.ToStatus,
				ToStatus: toStatus, ProviderRequestID: update.ProviderRequestID,
				ResultHash: resultHash, FailureCode: failureCode,
				IdempotencyKey: update.IdempotencyKey + ":domain-link", Actor: actor,
			}); err != nil {
				return err
			}
		}
		if resultInput != nil {
			written, err := d.Operations.RecordResultTx(ctx, tx, *resultInput)
			if err != nil {
				return err
			}
			operationResult = &written
		}
		if finalizeTx != nil {
			if err := finalizeTx(ctx, tx, resultHash); err != nil {
				return fmt.Errorf("finalize specialized effect result: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return operationResult, nil
}

func (d *Dispatcher) duplicateDomainLink(ctx context.Context, request Request, workspaceID, operationID, effectID string) (*contracts.DomainEffectLink, error) {
	if request.DomainLink == nil {
		return nil, ErrDomainLinkRequired
	}
	if request.DomainLink.ID != "" {
		value, err := d.Links.Get(ctx, workspaceID, request.DomainLink.ID)
		if err == nil {
			if !duplicateLinkMatches(value, request, workspaceID, operationID, effectID) {
				return nil, ErrDomainLinkRecoveryRequired
			}
			return &value, nil
		}
		if !errors.Is(err, store.ErrDomainEffectLinkNotFound) {
			return nil, fmt.Errorf("read duplicate domain-effect link by ID: %w", err)
		}
	}
	value, err := d.Links.GetByDomain(ctx, workspaceID, request.DomainLink.DomainKind, request.DomainLink.DomainID, request.DomainLink.LinkRole)
	if err != nil {
		return nil, fmt.Errorf("read duplicate domain-effect link by identity: %w", err)
	}
	if !duplicateLinkMatches(value, request, workspaceID, operationID, effectID) {
		return nil, ErrDomainLinkRecoveryRequired
	}
	return &value, nil
}

func duplicateLinkMatches(link contracts.DomainEffectLink, request Request, workspaceID, operationID, effectID string) bool {
	if request.DomainLink == nil {
		return false
	}
	want := request.DomainLink
	return link.WorkspaceID == workspaceID && link.OperationID == operationID && link.EffectID == effectID &&
		link.DomainKind == want.DomainKind && link.DomainID == want.DomainID && link.LinkRole == want.LinkRole
}

func (d *Dispatcher) replayDuplicate(ctx context.Context, request Request, operation store.Operation, admitted store.AdmissionResult, attempt store.OperationAttempt, effect store.OperationEffect, authority contracts.EffectAuthority) (Result, error) {
	state, err := d.Admission.GetEffectState(ctx, operation.WorkspaceID, effect.EffectID)
	if err != nil {
		return Result{Admission: admitted, Attempt: attempt, Effect: effect, Authority: authority, Duplicate: true}, err
	}
	link, err := d.duplicateDomainLink(ctx, request, operation.WorkspaceID, operation.ID, effect.EffectID)
	if err != nil {
		return Result{Admission: admitted, Attempt: attempt, Effect: effect, State: state, Authority: authority, Duplicate: true}, fmt.Errorf("%w: %w", ErrDomainLinkRecoveryRequired, err)
	}
	result := Result{Admission: admitted, Attempt: attempt, Effect: effect, State: state, Authority: authority, DomainLink: link, Duplicate: true}
	if !request.RecordResult {
		if state.State == contracts.ExternalEffectDispatching {
			return result, ErrEffectDispatchInProgress
		}
		return result, nil
	}
	record, err := d.Operations.GetResult(ctx, operation.WorkspaceID, operation.ID)
	if errors.Is(err, store.ErrOperationResultNotFound) {
		if state.State == contracts.ExternalEffectDispatching {
			return result, ErrEffectDispatchInProgress
		}
		return result, ErrOperationResultRecoveryRequired
	}
	if err != nil {
		return result, err
	}
	currentOperation, err := d.Operations.Get(ctx, operation.WorkspaceID, operation.ID)
	if err != nil {
		return result, err
	}
	result.OperationResult = &store.OperationResultWrite{Operation: currentOperation, Record: record, Duplicate: true}
	return result, nil
}

// Dispatch executes one effectful invocation through the durable boundary.
// It is at-least-once with respect to the external system and exactly-once
// with respect to the local reservation identity.
func (d *Dispatcher) Dispatch(ctx context.Context, request Request) (Result, error) {
	if d == nil || d.Operations == nil || d.Admission == nil || d.Links == nil || request.Invoker == nil {
		return Result{}, ErrNotConfigured
	}
	if request.DomainLink == nil {
		return Result{}, ErrDomainLinkRequired
	}
	if err := request.Operation.Normalize(); err != nil {
		return Result{}, fmt.Errorf("normalize operation: %w", err)
	}
	if err := request.Definition.Normalize(); err != nil {
		return Result{}, fmt.Errorf("normalize capability: %w", err)
	}
	if request.Definition.Effect == contracts.EffectClassReadOnly || request.Definition.Effect == contracts.EffectClassObservation {
		return Result{}, fmt.Errorf("%w: read-only capability cannot use effect dispatcher", ErrPlanMismatch)
	}
	if request.Operation.WorkspaceID != request.Definition.WorkspaceID {
		return Result{}, fmt.Errorf("%w: operation and capability workspace mismatch", ErrPlanMismatch)
	}
	if strings.TrimSpace(request.OwnerID) == "" || request.Fence == 0 {
		return Result{}, store.ErrOperationLeaseMissing
	}
	if request.Operation.Task != nil {
		if request.TaskOwnerID == "" || request.TaskFence == 0 {
			return Result{}, store.ErrOperationTaskFence
		}
	}
	operation, err := d.Operations.Get(ctx, request.Operation.WorkspaceID, request.Operation.ID)
	if err != nil {
		return Result{}, err
	}
	if operation.OperationHash == "" || operation.Request.ID != request.Operation.ID || operation.OperationHash != request.Operation.StableHash() && operation.Request.StableHash() != request.Operation.StableHash() {
		return Result{}, store.ErrOperationIdempotency
	}
	if operation.Request.Task != nil && (operation.TaskOwnerID != request.TaskOwnerID || operation.TaskFence != request.TaskFence) {
		return Result{}, store.ErrOperationTaskFence
	}
	stepID, err := authoritativeStep(operation, request.StepID, request.Definition)
	if err != nil {
		return Result{}, err
	}
	request.StepID = stepID
	if request.Attempt <= 0 {
		request.Attempt = 1
	}
	if request.AttemptID == "" {
		request.AttemptID = "attempt-" + contracts.HashStrings(operation.WorkspaceID, operation.ID, stepID, request.AttemptKey)[:40]
	}
	if request.AttemptKey == "" {
		request.AttemptKey = "effect-attempt-" + contracts.HashStrings(operation.WorkspaceID, operation.ID, stepID, request.Operation.IdempotencyKey)[:40]
	}
	if request.Effect.ID == "" {
		request.Effect.ID = "effect-" + contracts.HashStrings(operation.WorkspaceID, operation.ID, stepID, request.AttemptID)[:40]
	}
	if request.Effect.IdempotencyKey == "" {
		request.Effect.IdempotencyKey = "effect-" + contracts.HashStrings(operation.WorkspaceID, operation.ID, stepID, request.AttemptKey)[:40]
	}
	request.Effect.WorkspaceID = operation.WorkspaceID
	if request.Effect.Class != request.Definition.Effect {
		return Result{}, fmt.Errorf("%w: effect class %q does not match capability class %q", ErrPlanMismatch, request.Effect.Class, request.Definition.Effect)
	}
	if err := request.Effect.Normalize(); err != nil {
		return Result{}, fmt.Errorf("normalize effect: %w", err)
	}
	requestHash := operation.OperationHash
	if requestHash == "" {
		requestHash = request.Operation.StableHash()
	}
	request.Admission.WorkspaceID = operation.WorkspaceID
	request.Admission.OperationID = operation.ID
	request.Admission.OperationHash = operation.OperationHash
	request.Admission.Actor = operation.Request.Actor
	request.Admission.TaskBound = operation.Request.Task != nil
	request.Admission.TaskOwnerID = operation.TaskOwnerID
	request.Admission.TaskFence = operation.TaskFence
	request.Admission.TaskFenceValid = operation.Request.Task == nil
	admitted, err := d.Admission.Admit(ctx, request.Admission)
	if err != nil {
		return Result{}, err
	}
	if !admissionMayDispatch(admitted) {
		return Result{Admission: admitted}, fmt.Errorf("%w: %s", ErrAdmissionNotAllowed, admitted.Decision.Status)
	}
	authority := request.Authority
	authority.WorkspaceID = operation.WorkspaceID
	authority.OperationID = operation.ID
	authority.OperationOwnerID = request.OwnerID
	authority.OperationFence = request.Fence
	authority.TaskOwnerID = operation.TaskOwnerID
	authority.TaskFence = operation.TaskFence
	authority.AttemptID = request.AttemptID
	authority.EffectID = request.Effect.ID
	authority.RequestHash = requestHash
	authority.EffectReservationHash = request.Effect.StableHash()
	if authority.SchemaCatalogHash == "" {
		authority.SchemaCatalogHash = request.Admission.SchemaCatalogHash
		authority.SchemaCatalogRevision = request.Admission.SchemaCatalogRevision
	}
	if authority.CredentialLeaseID == "" {
		authority.CredentialLeaseID = request.Admission.CredentialLeaseID
		authority.CredentialLeaseFence = request.Admission.CredentialLeaseFence
		authority.CredentialRevocationEpoch = request.Admission.CredentialRevocationEpoch
		authority.CredentialSourceVersion = request.Admission.CredentialSourceVersion
		authority.CredentialSourceExpiresAt = request.Admission.CredentialSourceExpiresAt
	}
	if authority.ExternalBoundary != nil && request.Admission.ExternalBoundary != nil && authority.ExternalBoundary.StableHash() != request.Admission.ExternalBoundary.StableHash() {
		return Result{}, fmt.Errorf("%w: external boundary differs between admission and dispatch authority", ErrPlanMismatch)
	}
	if authority.ExternalBoundary == nil {
		authority.ExternalBoundary = contracts.CloneExternalBoundary(request.Admission.ExternalBoundary)
	}
	if err := authority.Normalize(); err != nil {
		return Result{}, fmt.Errorf("normalize dispatch authority: %w", err)
	}
	attempt, _, err := d.Operations.ReserveAttempt(ctx, store.OperationAttemptInput{WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, StepID: stepID, Attempt: request.Attempt, AttemptID: request.AttemptID, OwnerID: request.OwnerID, Fence: request.Fence, RequestHash: requestHash, IdempotencyKey: request.AttemptKey})
	if err != nil {
		return Result{Admission: admitted, Authority: authority}, err
	}
	effect, inserted, err := d.Operations.ReserveEffect(ctx, store.OperationEffectInput{WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, StepID: stepID, AttemptID: attempt.AttemptID, OwnerID: request.OwnerID, Fence: request.Fence, Effect: request.Effect, RequestHash: requestHash, SchemaCatalogHash: authority.SchemaCatalogHash, SchemaCatalogRevision: authority.SchemaCatalogRevision, CredentialLeaseID: authority.CredentialLeaseID, CredentialLeaseFence: authority.CredentialLeaseFence, CredentialRevocationEpoch: authority.CredentialRevocationEpoch, CredentialSourceVersion: authority.CredentialSourceVersion, CredentialSourceExpiresAt: authority.CredentialSourceExpiresAt, ExternalBoundary: authority.ExternalBoundary, RequireAllowedAdmission: true})
	if err != nil {
		return Result{Admission: admitted, Attempt: attempt, Authority: authority}, err
	}
	authority.EffectID = effect.EffectID
	authority.AttemptID = effect.AttemptID
	authority.EffectReservationHash = reservedEffectHash(effect)
	duplicate := !inserted
	if duplicate {
		state, stateErr := d.Admission.GetEffectState(ctx, operation.WorkspaceID, effect.EffectID)
		if stateErr != nil {
			return Result{Admission: admitted, Attempt: attempt, Effect: effect, Authority: authority, Duplicate: true}, stateErr
		}
		// A reservation is the only safe duplicate state to resume: no
		// dispatch intent was committed, so no external system could have
		// observed this attempt. Every later state is owned by the original
		// dispatcher or is explicitly uncertain and must be replayed/reconciled.
		if state.State != contracts.ExternalEffectReserved {
			return d.replayDuplicate(ctx, request, operation, admitted, attempt, effect, authority)
		}
	}
	var domainLink *contracts.DomainEffectLink
	if request.DomainLink != nil {
		link := *request.DomainLink
		link.WorkspaceID = operation.WorkspaceID
		link.OperationID, link.OperationHash = operation.ID, operation.OperationHash
		link.StepID, link.AttemptID, link.EffectID = effect.StepID, effect.AttemptID, effect.EffectID
		link.EffectReservationHash, link.RequestHash = reservedEffectHash(effect), requestHash
		link.Boundary, link.EffectClass, link.DeliveryGuarantee = effect.Boundary, contracts.EffectClass(effect.EffectClass), effect.DeliverySemantics
		if link.ExternalBoundary != nil && authority.ExternalBoundary != nil && link.ExternalBoundary.StableHash() != authority.ExternalBoundary.StableHash() {
			return Result{Admission: admitted, Attempt: attempt, Effect: effect, Authority: authority}, fmt.Errorf("%w: external boundary differs between effect and domain link", ErrPlanMismatch)
		}
		link.ExternalBoundary = contracts.CloneExternalBoundary(authority.ExternalBoundary)
		link.ProviderIdempotency, link.ProviderRequestID, link.VerificationStatus = effect.ProviderIdempotency, effect.ProviderRequestID, effect.VerificationStatus
		link.OperationOwnerID, link.OperationFence = request.OwnerID, request.Fence
		link.TaskOwnerID, link.TaskFence = operation.TaskOwnerID, operation.TaskFence
		link.SchemaCatalogHash, link.SchemaCatalogRevision = authority.SchemaCatalogHash, authority.SchemaCatalogRevision
		link.CredentialLeaseID, link.CredentialLeaseFence, link.CredentialRevocationEpoch = authority.CredentialLeaseID, authority.CredentialLeaseFence, authority.CredentialRevocationEpoch
		link.CredentialSourceVersion, link.CredentialSourceExpiresAt = authority.CredentialSourceVersion, authority.CredentialSourceExpiresAt
		link.Actor, link.RequestID, link.CausationID, link.CorrelationID = operation.Request.Actor, operation.Request.RequestID, operation.Request.CausationID, operation.Request.CorrelationID
		if link.IdempotencyKey == "" {
			link.IdempotencyKey = "domain-link-" + contracts.HashStrings(operation.WorkspaceID, effect.EffectID, link.DomainKind, link.DomainID)[:48]
		}
		bound, bindErr := d.Links.Bind(ctx, link)
		if bindErr != nil {
			return Result{Admission: admitted, Attempt: attempt, Effect: effect, Authority: authority}, fmt.Errorf("bind domain effect: %w", bindErr)
		}
		domainLink = &bound.Link
	}
	if d.beforeDispatchIntent != nil {
		d.beforeDispatchIntent(duplicate)
	}
	dispatching, err := d.Operations.BeginEffectDispatch(ctx, authority, contracts.ExternalEffectUpdate{WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, EffectID: effect.EffectID, OwnerID: request.OwnerID, Fence: request.Fence, RequestID: operation.Request.RequestID, IdempotencyKey: request.Effect.IdempotencyKey + ":dispatching", State: contracts.ExternalEffectDispatching})
	if err != nil {
		if duplicate {
			// A concurrent recovery caller can race the original transaction
			// between its idempotency lookup and state update. If the state is
			// already dispatching, the other caller owns the external boundary;
			// never issue a second provider call.
			if current, readErr := d.Admission.GetEffectState(ctx, operation.WorkspaceID, effect.EffectID); readErr == nil && current.State != contracts.ExternalEffectReserved {
				return d.replayDuplicate(ctx, request, operation, admitted, attempt, effect, authority)
			}
		}
		return Result{Admission: admitted, Attempt: attempt, Effect: effect, Authority: authority}, err
	}
	if dispatching.Duplicate {
		return d.replayDuplicate(ctx, request, operation, admitted, attempt, effect, authority)
	}
	invocation, invokeErr := request.Invoker(ctx, authority)
	if invokeErr != nil {
		uncertain := &ExternalDispatchError{Err: invokeErr, PossiblyStarted: true}
		if classified, ok := invokeErr.(*ExternalDispatchError); ok {
			uncertain = classified
		}
		recoveryUpdate := contracts.ExternalEffectUpdate{WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, EffectID: effect.EffectID, OwnerID: request.OwnerID, Fence: request.Fence, RequestID: operation.Request.RequestID, IdempotencyKey: request.Effect.IdempotencyKey + ":recovery", State: contracts.ExternalEffectRecoveryRequired, ProviderRequestID: uncertain.ProviderRequestID, ResponseHash: uncertain.ResponseHash, FailureCode: "external_uncertain"}
		if finalizeErr := d.publishEffectAndLink(ctx, recoveryUpdate, domainLink, contracts.DomainEffectLinkStatusRecoveryRequired, "", "external_uncertain", operation.Request.Actor); finalizeErr != nil {
			return Result{Admission: admitted, Attempt: attempt, Effect: effect, Authority: authority, DomainLink: domainLink}, fmt.Errorf("%w: recovery publication failed: %v", uncertain, finalizeErr)
		}
		if domainLink != nil {
			domainLink.Status = contracts.DomainEffectLinkStatusRecoveryRequired
		}
		return Result{Admission: admitted, Attempt: attempt, Effect: effect, Authority: authority, DomainLink: domainLink}, uncertain
	}
	if err := invocation.Result.Normalize(); err != nil {
		return Result{Admission: admitted, Attempt: attempt, Effect: effect, Authority: authority, DomainLink: domainLink}, &ExternalDispatchError{Err: fmt.Errorf("normalize invoked result: %w", err), PossiblyStarted: true, ProviderRequestID: invocation.ProviderRequestID, ResponseHash: invocation.ResponseHash}
	}
	// The provider call happened outside Postgres. Re-read the operation,
	// task, effect reservation, and live authority facts before publishing any
	// acknowledged/verified state or durable result.
	if err := d.Operations.ValidateEffectAuthority(ctx, authority); err != nil {
		return Result{Admission: admitted, Attempt: attempt, Effect: effect, Authority: authority, DomainLink: domainLink}, &ExternalDispatchError{Err: fmt.Errorf("validate authority before finalization: %w", err), PossiblyStarted: true, ProviderRequestID: invocation.ProviderRequestID, ResponseHash: invocation.ResponseHash}
	}
	// Adapters validate the logical request hash. The durable operation result
	// is additionally bound to the persisted operation hash, which includes
	// the authoritative plan identity.
	invocation.Result.OperationHash = operation.OperationHash
	if !containsEffect(invocation.Result.ExternalEffects, effect) {
		recoveryUpdate := contracts.ExternalEffectUpdate{WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, EffectID: effect.EffectID, OwnerID: request.OwnerID, Fence: request.Fence, RequestID: operation.Request.RequestID, IdempotencyKey: request.Effect.IdempotencyKey + ":recovery", State: contracts.ExternalEffectRecoveryRequired, FailureCode: "effect_not_returned"}
		if finalizeErr := d.publishEffectAndLink(ctx, recoveryUpdate, domainLink, contracts.DomainEffectLinkStatusRecoveryRequired, "", "effect_not_returned", operation.Request.Actor); finalizeErr != nil {
			return Result{Admission: admitted, Attempt: attempt, Effect: effect, Authority: authority, DomainLink: domainLink}, fmt.Errorf("effect-not-returned recovery publication failed: %w", finalizeErr)
		}
		if domainLink != nil {
			domainLink.Status = contracts.DomainEffectLinkStatusRecoveryRequired
		}
		return Result{Admission: admitted, Attempt: attempt, Effect: effect, Authority: authority, DomainLink: domainLink}, &ExternalDispatchError{Err: ErrEffectNotReturned, PossiblyStarted: true, ProviderRequestID: invocation.ProviderRequestID, ResponseHash: invocation.ResponseHash}
	}
	responseHash := invocation.ResponseHash
	if responseHash == "" {
		responseHash = invocation.Result.StableHash()
	}
	providerRequestID := invocation.ProviderRequestID
	if providerRequestID == "" {
		providerRequestID = effect.ProviderRequestID
	}
	if _, err := d.Admission.UpdateEffect(ctx, contracts.ExternalEffectUpdate{WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, EffectID: effect.EffectID, OwnerID: request.OwnerID, Fence: request.Fence, RequestID: operation.Request.RequestID, IdempotencyKey: request.Effect.IdempotencyKey + ":dispatched", State: contracts.ExternalEffectDispatched, ProviderRequestID: providerRequestID, ResponseHash: responseHash}); err != nil {
		return Result{Admission: admitted, Attempt: attempt, Effect: effect, Authority: authority, DomainLink: domainLink}, &ExternalDispatchError{Err: err, PossiblyStarted: true, ProviderRequestID: providerRequestID, ResponseHash: responseHash}
	}
	stateKey := request.Effect.IdempotencyKey + ":acknowledged"
	if _, err := d.Admission.UpdateEffect(ctx, contracts.ExternalEffectUpdate{WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, EffectID: effect.EffectID, OwnerID: request.OwnerID, Fence: request.Fence, RequestID: operation.Request.RequestID, IdempotencyKey: stateKey, State: contracts.ExternalEffectAcknowledged, ProviderRequestID: providerRequestID, ResponseHash: responseHash}); err != nil {
		return Result{Admission: admitted, Attempt: attempt, Effect: effect, Authority: authority, DomainLink: domainLink}, &ExternalDispatchError{Err: err, PossiblyStarted: true, ProviderRequestID: providerRequestID, ResponseHash: responseHash}
	}
	finalState := contracts.ExternalEffectVerified
	finalStatus := contracts.OperationStatusSucceeded
	if effect.VerificationRequired {
		finalState = contracts.ExternalEffectVerificationPending
		finalStatus = contracts.OperationStatusAwaitingExternal
	}
	finalUpdate := contracts.ExternalEffectUpdate{WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, EffectID: effect.EffectID, OwnerID: request.OwnerID, Fence: request.Fence, RequestID: operation.Request.RequestID, IdempotencyKey: request.Effect.IdempotencyKey + ":final", State: finalState, ProviderRequestID: providerRequestID, ResponseHash: responseHash}
	linkStatus := ""
	if finalState == contracts.ExternalEffectVerified {
		linkStatus = contracts.DomainEffectLinkStatusReconciled
	}
	var resultInput *store.OperationResultInput
	if request.RecordResult {
		if invocation.Result.Status == contracts.OperationStatusFailed {
			finalStatus = contracts.OperationStatusFailed
		}
		invocation.Result.Status = finalStatus
		resultInput = &store.OperationResultInput{WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, OwnerID: request.OwnerID, Fence: request.Fence, TaskOwnerID: operation.TaskOwnerID, TaskFence: operation.TaskFence, Actor: operation.Request.Actor, RequestID: operation.Request.RequestID, IdempotencyKey: request.Effect.IdempotencyKey + ":result", CausationID: operation.Request.CausationID, CorrelationID: operation.Request.CorrelationID, SchemaCatalogHash: authority.SchemaCatalogHash, SchemaCatalogRevision: authority.SchemaCatalogRevision, CredentialLeaseID: authority.CredentialLeaseID, CredentialLeaseFence: authority.CredentialLeaseFence, CredentialRevocationEpoch: authority.CredentialRevocationEpoch, CredentialSourceVersion: authority.CredentialSourceVersion, CredentialSourceExpiresAt: authority.CredentialSourceExpiresAt, Result: invocation.Result}
	}
	operationResult, err := d.publishEffectLinkAndResult(ctx, finalUpdate, domainLink, linkStatus, responseHash, "", operation.Request.Actor, resultInput, request.FinalizeTx)
	if err != nil {
		return Result{Admission: admitted, Attempt: attempt, Effect: effect, Authority: authority, DomainLink: domainLink}, &ExternalDispatchError{Err: err, PossiblyStarted: true, ProviderRequestID: providerRequestID, ResponseHash: responseHash}
	}
	if domainLink != nil && linkStatus != "" {
		domainLink.Status = linkStatus
	}
	state, err := d.Admission.GetEffectState(ctx, operation.WorkspaceID, effect.EffectID)
	if err != nil {
		return Result{Admission: admitted, Attempt: attempt, Effect: effect, Authority: authority}, err
	}
	result := Result{Admission: admitted, Attempt: attempt, Effect: effect, State: state, Authority: authority, DomainLink: domainLink, Duplicate: duplicate}
	result.OperationResult = operationResult
	return result, nil
}

func admissionMayDispatch(value store.AdmissionResult) bool {
	if value.Decision.Status == contracts.AdmissionAllowed {
		return true
	}
	return value.Decision.Status == contracts.AdmissionAwaitingApproval && value.Approval != nil && value.Approval.Status == contracts.ApprovalRequestApproved && value.Approval.ExpiresAt.After(now())
}

var now = func() time.Time { return time.Now().UTC() }

func authoritativeStep(operation store.Operation, requested string, definition contracts.CapabilityDefinition) (string, error) {
	if operation.Plan == nil || len(operation.Plan.Steps) == 0 {
		return "", fmt.Errorf("%w: operation plan is required", ErrPlanMismatch)
	}
	requested = strings.TrimSpace(requested)
	for _, step := range operation.Plan.Steps {
		if requested != "" && step.ID != requested {
			continue
		}
		if step.Capability.StableHash() != definition.Ref.StableHash() || step.Effect != definition.Effect {
			return "", fmt.Errorf("%w: step capability or effect mismatch", ErrPlanMismatch)
		}
		return step.ID, nil
	}
	return "", fmt.Errorf("%w: operation step not found", ErrPlanMismatch)
}

func containsEffect(effects []contracts.ExternalEffect, reserved store.OperationEffect) bool {
	for _, candidate := range effects {
		if candidate.WorkspaceID == reserved.WorkspaceID && candidate.Class == contracts.EffectClass(reserved.EffectClass) && candidate.Boundary == reserved.Boundary && candidate.StableHash() == reservedEffectHash(reserved) {
			return true
		}
	}
	return false
}

func reservedEffectHash(effect store.OperationEffect) string {
	return (contracts.ExternalEffect{WorkspaceID: effect.WorkspaceID, Boundary: effect.Boundary, Class: contracts.EffectClass(effect.EffectClass), DeliveryGuarantee: effect.DeliverySemantics, ProviderIdempotency: effect.ProviderIdempotency, VerificationRequired: effect.VerificationRequired, VerificationStatus: effect.VerificationStatus, CompensationStatus: effect.CompensationStatus}).StableHash()
}
