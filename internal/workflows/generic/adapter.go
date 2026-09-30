package generic

import (
	"context"
	"errors"
	"fmt"
	"strings"

	connectorruntime "github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/effectdispatch"
	"github.com/omaveda/fornix/internal/store"
)

var ErrAdmissionAuthorityNeeded = errors.New("workflow connector step requires durable admission authority")

// ConnectorStepExecutor adapts the existing domain-neutral connector
// registry into the workflow runtime. It executes only read/observation
// steps. Effectful steps use the existing durable effect dispatcher when it is
// configured; otherwise they pause for an explicit authority adapter. This
// prevents a generic workflow from turning a connector capability into an
// unreserved external call.
type ConnectorStepExecutor struct {
	Registry   *connectorruntime.Registry
	Executor   *connectorruntime.Executor
	Operations *store.OperationStore
	Admission  *store.AdmissionStore
	Dispatcher *effectdispatch.Dispatcher
}

func NewConnectorStepExecutor(registry *connectorruntime.Registry, executor *connectorruntime.Executor, operations *store.OperationStore) *ConnectorStepExecutor {
	return &ConnectorStepExecutor{Registry: registry, Executor: executor, Operations: operations}
}

// SetEffectBoundary injects the existing Postgres-backed admission and effect
// authorities. It is intentionally separate from construction so offline
// tests can use the same connector executor without configuring a database.
func (e *ConnectorStepExecutor) SetEffectBoundary(admission *store.AdmissionStore, dispatcher *effectdispatch.Dispatcher) {
	if e == nil {
		return
	}
	e.Admission = admission
	e.Dispatcher = dispatcher
}

func (e *ConnectorStepExecutor) Execute(ctx context.Context, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep) (contracts.WorkflowStepResult, error) {
	return e.execute(ctx, run, state, step, store.WorkflowLease{}, "", 0)
}

func (e *ConnectorStepExecutor) ExecuteWithLease(ctx context.Context, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep, lease store.WorkflowLease, taskOwnerID string, taskFence uint64) (contracts.WorkflowStepResult, error) {
	return e.execute(ctx, run, state, step, lease, taskOwnerID, taskFence)
}

func (e *ConnectorStepExecutor) execute(ctx context.Context, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep, lease store.WorkflowLease, taskOwnerID string, taskFence uint64) (contracts.WorkflowStepResult, error) {
	if e == nil || e.Registry == nil || e.Executor == nil {
		return contracts.WorkflowStepResult{}, ErrNotConfigured
	}
	readOnly := step.Effect == contracts.EffectClassReadOnly || step.Effect == contracts.EffectClassObservation
	if readOnly && e.Admission == nil {
		return contracts.WorkflowStepResult{}, ErrAdmissionAuthorityNeeded
	}
	if e.Operations == nil {
		return contracts.WorkflowStepResult{}, ErrNotConfigured
	}
	operation, err := e.Operations.Get(ctx, run.WorkspaceID, run.Operation.ID)
	if err != nil {
		return contracts.WorkflowStepResult{}, err
	}
	capability, ok := e.Registry.Lookup(step.Capability)
	if !ok {
		return contracts.WorkflowStepResult{}, fmt.Errorf("unknown workflow capability %s", step.Capability.Name)
	}
	definition := capability.Definition()
	if step.Effect != contracts.EffectClassReadOnly && step.Effect != contracts.EffectClassObservation {
		if e.Dispatcher == nil || e.Admission == nil {
			return contracts.WorkflowStepResult{Status: contracts.WorkflowStepAwaitingExternal, Wait: &contracts.WorkflowWait{Kind: contracts.WorkflowWaitExternal, Token: contracts.HashStrings("generic-effect-authority", run.ID, step.ID, fmt.Sprint(state.Attempt+1)), Reason: ErrEffectAuthorityNeeded.Error()}}, nil
		}
		return e.executeEffect(ctx, run, state, step, operation, capability, definition, lease, taskOwnerID, taskFence)
	}
	request := operation.Request
	request.ID = contracts.HashStrings("workflow-operation", run.ID, step.ID)[:32]
	request.RequestID = contracts.HashStrings("workflow-request", run.ID, step.ID)[:32]
	request.IdempotencyKey = contracts.HashStrings("workflow-step", run.ID, step.ID, fmt.Sprint(state.Attempt))
	request.CausationID = run.Operation.ID
	request.CorrelationID = run.ID
	request.Actor = run.Actor
	request.Capability = step.Capability
	request.Target = step.Target
	request.InputSchemaVersion = definition.InputSchemaVersion
	request.InputSchemaHash = definition.InputSchemaHash
	request.InputHash = step.InputHash
	request.Profile = step.Profile
	if err := request.Normalize(); err != nil {
		return contracts.WorkflowStepResult{}, fmt.Errorf("normalize workflow connector request: %w", err)
	}
	principal := contracts.Principal{ID: run.Actor.ID, WorkspaceID: run.WorkspaceID, Subject: run.Actor.ID, Kind: run.Actor.Kind, DisplayName: run.Actor.Name, Authenticated: true}
	options := connectorruntime.AdmissionOptions{Principal: &principal, Authorize: func(_ context.Context, value contracts.Principal, candidate contracts.OperationRequest, _ contracts.CapabilityDefinition) (bool, error) {
		return value.WorkspaceID == candidate.WorkspaceID && value.ID == candidate.Actor.ID, nil
	}}
	runtimeAdmission, err := e.Registry.Admit(ctx, request, options)
	if err != nil {
		return contracts.WorkflowStepResult{}, err
	}
	input := workflowStepAdmissionInput(run, state, step, operation, request, runtimeAdmission, principal, taskOwnerID, taskFence)
	durableAdmission, err := e.Admission.Admit(ctx, input)
	if err != nil {
		return contracts.WorkflowStepResult{}, fmt.Errorf("persist workflow connector admission: %w", err)
	}
	if durableAdmission.Decision.Status != contracts.AdmissionAllowed {
		return workflowAdmissionResult(durableAdmission.Decision), nil
	}
	outcome, err := e.Executor.Execute(ctx, request, options)
	if err != nil {
		return contracts.WorkflowStepResult{}, err
	}
	if outcome.Result.Status != contracts.OperationStatusSucceeded {
		return contracts.WorkflowStepResult{Status: contracts.WorkflowStepFailed, OutputHash: outcome.Result.OutputHash, Evidence: outcome.Result.Evidence, Failure: &contracts.WorkflowFailure{Code: strings.ToLower(outcome.Result.Status), Retryable: false}}, nil
	}
	return contracts.WorkflowStepResult{Status: contracts.WorkflowStepSucceeded, OutputHash: outcome.Result.OutputHash, Evidence: outcome.Result.Evidence}, nil
}

func workflowStepAdmissionInput(run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep, operation store.Operation, request contracts.OperationRequest, admission connectorruntime.Admission, principal contracts.Principal, taskOwnerID string, taskFence uint64) contracts.AdmissionInput {
	definition := admission.Definition
	input := contracts.AdmissionInput{
		WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, OperationHash: operation.OperationHash,
		RequestID:      request.RequestID,
		IdempotencyKey: workflowStepAdmissionKey(run.ID, step.ID, state.Attempt),
		Actor:          principal.Actor(), Capability: definition, Target: request.Target,
		SchemaCatalogHash: admission.SchemaCatalogHash, SchemaCatalogRevision: admission.SchemaCatalogRevision,
		Policy: contracts.AdmissionPolicy{
			WorkspaceID: operation.WorkspaceID, PolicyID: "generic-workflow-step", Version: "1",
			AllowedConnectors:    []contracts.ConnectorRef{definition.Ref.Connector},
			AllowedResourceKinds: definition.ResourceKinds, AllowedActorIDs: []string{principal.ID},
			RequireTaskFence: operation.Request.Task != nil,
		},
		ConnectorAvailable: true, ResourceAllowed: true, EvidenceSatisfied: true,
		TaskBound: operation.Request.Task != nil, TaskFenceValid: operation.Request.Task == nil,
	}
	if operation.Request.Task != nil {
		input.TaskOwnerID, input.TaskFence = taskOwnerID, taskFence
	}
	return input
}

func workflowStepAdmissionKey(runID, stepID string, attempt int) string {
	return contracts.HashStrings("workflow-step-admission", runID, stepID, fmt.Sprint(attempt))
}

func workflowAdmissionResult(decision contracts.AdmissionDecision) contracts.WorkflowStepResult {
	if decision.ReasonCode == contracts.AdmissionReasonRateLimited && decision.RetryAt != nil {
		retryAt := decision.RetryAt.UTC()
		return contracts.WorkflowStepResult{
			Status: contracts.WorkflowStepAwaitingRetry,
			Wait: &contracts.WorkflowWait{
				Kind:   contracts.WorkflowWaitRetry,
				Token:  contracts.HashStrings("capability-rate-retry", decision.WorkspaceID, decision.OperationID, decision.IdempotencyKey, decision.DecisionHash),
				Reason: "capability rate window is full", ExpiresAt: &retryAt,
			},
			Failure: &contracts.WorkflowFailure{Code: "admission_" + decision.ReasonCode, Retryable: true},
		}
	}
	return contracts.WorkflowStepResult{
		Status: contracts.WorkflowStepFailed,
		// Historical rate-limit decisions have no persisted deadline. They are
		// intentionally not converted to immediate retries.
		Failure: &contracts.WorkflowFailure{Code: "admission_" + decision.ReasonCode, Retryable: false},
	}
}

// RecoverStepResult reconstructs only the no-effect outcome proven by a
// durable rate-limit denial. An allowed admission is not enough: the worker
// may have crashed during the connector call, so normal conservative recovery
// remains in force for every other decision.
func (e *ConnectorStepExecutor) RecoverStepResult(ctx context.Context, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep) (contracts.WorkflowStepResult, bool, error) {
	if e == nil || e.Admission == nil || e.Operations == nil ||
		(step.Effect != contracts.EffectClassReadOnly && step.Effect != contracts.EffectClassObservation) {
		return contracts.WorkflowStepResult{}, false, nil
	}
	operation, err := e.Operations.Get(ctx, run.WorkspaceID, run.Operation.ID)
	if err != nil {
		return contracts.WorkflowStepResult{}, false, err
	}
	key := workflowStepAdmissionKey(run.ID, step.ID, state.Attempt)
	decision, err := e.Admission.GetDecisionByIdempotencyKey(ctx, run.WorkspaceID, key)
	if errors.Is(err, store.ErrAdmissionNotFound) {
		return contracts.WorkflowStepResult{}, false, nil
	}
	if err != nil {
		return contracts.WorkflowStepResult{}, false, err
	}
	if decision.WorkspaceID != run.WorkspaceID || decision.OperationID != operation.ID || decision.OperationHash != operation.OperationHash ||
		decision.IdempotencyKey != key || decision.Capability.StableHash() != step.Capability.StableHash() ||
		decision.Target.StableHash() != step.Target.StableHash() || decision.Actor != run.Actor || decision.Status != contracts.AdmissionDenied ||
		decision.ReasonCode != contracts.AdmissionReasonRateLimited || decision.RetryAt == nil {
		return contracts.WorkflowStepResult{}, false, nil
	}
	return workflowAdmissionResult(decision), true, nil
}

func (e *ConnectorStepExecutor) executeEffect(ctx context.Context, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep, operation store.Operation, capability connectorruntime.Capability, definition contracts.CapabilityDefinition, lease store.WorkflowLease, taskOwnerID string, taskFence uint64) (contracts.WorkflowStepResult, error) {
	if operation.Request.Capability.StableHash() != step.Capability.StableHash() {
		return contracts.WorkflowStepResult{}, fmt.Errorf("%w: effectful workflow step is not the persisted operation capability", ErrEffectAuthorityNeeded)
	}
	if lease.OwnerID == "" || lease.Fence == 0 {
		return contracts.WorkflowStepResult{}, store.ErrWorkflowLease
	}
	if operation.Request.Task != nil && (taskOwnerID == "" || taskFence == 0) {
		return contracts.WorkflowStepResult{}, store.ErrOperationTaskFence
	}
	request := operation.Request
	principal := contracts.Principal{ID: run.Actor.ID, WorkspaceID: run.WorkspaceID, Subject: run.Actor.ID, Kind: run.Actor.Kind, DisplayName: run.Actor.Name, Authenticated: true}
	approved := false
	for _, candidate := range run.Steps {
		if candidate.Status == contracts.WorkflowStepSucceeded && strings.Contains(candidate.Kind, contracts.WorkflowStepApproval) {
			approved = true
			break
		}
	}
	options := connectorruntime.AdmissionOptions{Principal: &principal, ApprovalGranted: approved, Authorize: func(_ context.Context, value contracts.Principal, candidate contracts.OperationRequest, _ contracts.CapabilityDefinition) (bool, error) {
		return value.WorkspaceID == candidate.WorkspaceID && value.ID == candidate.Actor.ID, nil
	}}
	admission, err := e.Registry.Admit(ctx, request, options)
	if err != nil {
		return contracts.WorkflowStepResult{}, err
	}
	if descriptor, ok := capability.(connectorruntime.EffectDescriber); !ok {
		return contracts.WorkflowStepResult{}, ErrEffectAuthorityNeeded
	} else {
		effect, describeErr := descriptor.DescribeEffect(request)
		if describeErr != nil {
			return contracts.WorkflowStepResult{}, describeErr
		}
		return e.dispatchEffect(ctx, run, state, step, operation, definition, effect, admission, principal, options, lease, taskOwnerID, taskFence)
	}
}

func (e *ConnectorStepExecutor) dispatchEffect(ctx context.Context, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep, operation store.Operation, definition contracts.CapabilityDefinition, effect contracts.ExternalEffect, admission connectorruntime.Admission, principal contracts.Principal, options connectorruntime.AdmissionOptions, lease store.WorkflowLease, taskOwnerID string, taskFence uint64) (contracts.WorkflowStepResult, error) {
	input := contracts.AdmissionInput{
		WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, OperationHash: operation.OperationHash,
		RequestID: operation.Request.RequestID, IdempotencyKey: contracts.HashStrings("workflow-admission", run.ID, step.ID),
		Actor: principal.Actor(), Capability: definition, Target: operation.Request.Target,
		Policy:             contracts.AdmissionPolicy{WorkspaceID: operation.WorkspaceID, PolicyID: "generic-workflow-connector", Version: "1", AllowedConnectors: []contracts.ConnectorRef{definition.Ref.Connector}, AllowedResourceKinds: definition.ResourceKinds, AllowedActorIDs: []string{principal.ID}, RequireEvidence: false, RequireTaskFence: operation.Request.Task != nil},
		ConnectorAvailable: true, ResourceAllowed: true, EvidenceSatisfied: true, TaskBound: operation.Request.Task != nil,
		TaskOwnerID: operation.TaskOwnerID, TaskFence: operation.TaskFence, TaskFenceValid: operation.Request.Task == nil,
		SchemaCatalogHash: admission.SchemaCatalogHash, SchemaCatalogRevision: admission.SchemaCatalogRevision,
	}
	if operation.Request.Task != nil {
		input.TaskOwnerID, input.TaskFence, input.TaskFenceValid = taskOwnerID, taskFence, false
	}
	link := &contracts.DomainEffectLink{DomainKind: contracts.DomainEffectKindWorkflowStep, DomainID: run.ID + ":" + step.ID, DomainHash: contracts.HashStrings(run.ID, step.ID, step.InputHash), LinkRole: contracts.DomainEffectLinkRolePrimary, IdempotencyKey: contracts.HashStrings("workflow-domain-link", run.ID, step.ID, fmt.Sprint(state.Attempt))}
	result, err := e.Dispatcher.Dispatch(ctx, effectdispatch.Request{
		Admission: input, Operation: operation.Request, Definition: definition, OwnerID: lease.OwnerID, Fence: lease.Fence,
		TaskOwnerID: taskOwnerID, TaskFence: taskFence, StepID: step.ID, Attempt: state.Attempt,
		AttemptKey: contracts.HashStrings("workflow-attempt", run.ID, step.ID, fmt.Sprint(state.Attempt)), Effect: effect,
		DomainLink: link, RecordResult: false,
		Invoker: func(invokeCtx context.Context, authority contracts.EffectAuthority) (effectdispatch.InvocationResult, error) {
			outcome, executeErr := e.Executor.Execute(invokeCtx, operation.Request, connectorruntime.AdmissionOptions{Principal: &principal, ApprovalGranted: options.ApprovalGranted, Authority: &authority, RequireAuthority: true, ValidateAuthority: e.Operations.ValidateEffectAuthority, Authorize: options.Authorize})
			if executeErr != nil {
				return effectdispatch.InvocationResult{}, executeErr
			}
			return effectdispatch.InvocationResult{Result: outcome.Result}, nil
		},
	})
	if err != nil {
		var uncertain *effectdispatch.ExternalDispatchError
		if errors.As(err, &uncertain) {
			return contracts.WorkflowStepResult{Status: contracts.WorkflowStepRecoveryRequired, Effect: &effect, Failure: &contracts.WorkflowFailure{Code: "external_uncertain", External: true}}, nil
		}
		return contracts.WorkflowStepResult{}, err
	}
	effect.ID = result.Effect.EffectID
	effect.ProviderRequestID = result.Effect.ProviderRequestID
	effect.VerificationStatus = result.Effect.VerificationStatus
	if result.State.State == contracts.ExternalEffectVerificationPending || result.State.State == contracts.ExternalEffectAcknowledged || result.State.State == contracts.ExternalEffectDispatched {
		return contracts.WorkflowStepResult{Status: contracts.WorkflowStepAwaitingExternal, Effect: &effect}, nil
	}
	if result.State.State == contracts.ExternalEffectVerified {
		return contracts.WorkflowStepResult{Status: contracts.WorkflowStepSucceeded, Effect: &effect, OutputHash: contracts.HashStrings("workflow-effect-result", result.Effect.EffectID)}, nil
	}
	return contracts.WorkflowStepResult{Status: contracts.WorkflowStepRecoveryRequired, Effect: &effect, Failure: &contracts.WorkflowFailure{Code: "effect_recovery_required", External: true}}, nil
}
