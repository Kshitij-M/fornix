package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"github.com/omaveda/fornix/internal/change"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/effectdispatch"
	"github.com/omaveda/fornix/internal/model"
	"github.com/omaveda/fornix/internal/store"
)

// domainEffectAdapters is the production composition layer for built-in
// effectful domains. It contains no provider or process logic; it only builds
// a typed child operation and hands the actual boundary to the owning domain.
type domainEffectAdapters struct {
	runtime             *effectdispatch.Runtime
	toolRuns            *store.ToolRunStore
	deploymentAdmission func(context.Context, string) (*contracts.DeploymentAdmissionReference, error)
}

func effectEntityID(reference *contracts.EntityRef) string {
	if reference == nil {
		return ""
	}
	return reference.ID
}

func (a *domainEffectAdapters) admissionReference(ctx context.Context, workspaceID string) (*contracts.DeploymentAdmissionReference, error) {
	if a == nil || a.deploymentAdmission == nil {
		return nil, nil
	}
	return a.deploymentAdmission(ctx, workspaceID)
}

// embeddingRecoveryFinalizer keeps the provider gateway independent from the
// Postgres store while routing production recovery through one local commit.
type embeddingRecoveryFinalizer struct {
	coordinator *store.EmbeddingRecoveryCoordinator
}

func (f *embeddingRecoveryFinalizer) FinalizeEmbeddingRecovery(ctx context.Context, request contracts.EmbeddingRecoveryFinalizeRequest) (contracts.EmbeddingCallRecord, error) {
	if f == nil || f.coordinator == nil {
		return contracts.EmbeddingCallRecord{}, model.ErrEmbeddingReconciliationUnsupported
	}
	result, err := f.coordinator.Finalize(ctx, request)
	if err != nil {
		return contracts.EmbeddingCallRecord{}, err
	}
	return result.Record, nil
}

// RunEmbedding routes vector generation through the same fenced child-effect
// authority as chat calls and tools. The vector itself stays in the
// specialized embedding ledger; generic operation records receive hashes and
// references only.
func (a *domainEffectAdapters) RunEmbedding(ctx context.Context, request contracts.EmbeddingRequest, provider contracts.ProviderRef, invoke func(context.Context) ([]float32, error)) ([]float32, error) {
	requestHash := request.RequestHash()
	definition := embeddingCapability(request.WorkspaceID, provider)
	target := embeddingTarget(request.WorkspaceID, provider)
	effect := contracts.ExternalEffect{WorkspaceID: request.WorkspaceID, Boundary: "embedding-provider", Class: contracts.EffectClassExternalCommunication, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, ProviderIdempotency: false}
	deploymentAdmission, err := a.admissionReference(ctx, request.WorkspaceID)
	if err != nil {
		return nil, err
	}
	var vector []float32
	dispatchResult, dispatchErr := a.runtime.Run(ctx, effectdispatch.ChildRequest{
		WorkspaceID: request.WorkspaceID, DomainKind: contracts.DomainEffectKindEmbeddingCall,
		DomainID: request.IdempotencyKey, DomainHash: requestHash,
		RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey + ":embedding",
		CausationID: request.CausationID, CorrelationID: request.CorrelationID, Actor: request.Actor,
		Task: request.Task, TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence, Session: request.Session,
		DeploymentAdmission: deploymentAdmission,
		ExternalBoundary:    provider.ExternalBoundary,
		Capability:          definition, Target: target, InputType: "fornix.embedding.request", InputHash: requestHash,
		Policy: autonomousPolicy(request.WorkspaceID, request.Actor, definition, target, true), Effect: effect,
		Invoker: func(invokeCtx context.Context, authority contracts.EffectAuthority) (effectdispatch.InvocationResult, error) {
			called, invokeErr := invoke(invokeCtx)
			vector = called
			if invokeErr != nil {
				return effectdispatch.InvocationResult{}, &effectdispatch.ExternalDispatchError{Err: invokeErr, PossiblyStarted: true}
			}
			return embeddingInvocation(authority, request, definition, effect, vector)
		},
	})
	if dispatchErr != nil {
		return nil, dispatchErr
	}
	if dispatchResult.Duplicate && len(vector) == 0 {
		return nil, &effectdispatch.ExternalDispatchError{PossiblyStarted: true}
	}
	return vector, nil
}

// RunEmbeddingReconciliation is a separate, hash-only provider outcome query.
// Its operation and effect identities cannot collide with generation. A
// duplicate reconciliation remains fail-closed because the specialized
// embedding ledger is finalized by the caller after this boundary returns.
func (a *domainEffectAdapters) RunEmbeddingReconciliation(ctx context.Context, request contracts.EmbeddingReconciliationRequest, provider contracts.ProviderRef, invoke func(context.Context) (contracts.EmbeddingResponse, error)) (contracts.EmbeddingResponse, error) {
	requestHash := contracts.HashStrings(request.RequestHash, request.ProviderRequestID, request.SourceHash, "embedding-reconciliation")
	definition := embeddingCapability(request.WorkspaceID, provider)
	definition.Ref.Name = "reconcile"
	target := embeddingTarget(request.WorkspaceID, provider)
	effect := contracts.ExternalEffect{WorkspaceID: request.WorkspaceID, Boundary: "embedding-provider-reconciliation", Class: contracts.EffectClassExternalCommunication, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, ProviderIdempotency: true}
	deploymentAdmission, err := a.admissionReference(ctx, request.WorkspaceID)
	if err != nil {
		return contracts.EmbeddingResponse{}, err
	}
	var response contracts.EmbeddingResponse
	dispatchResult, dispatchErr := a.runtime.Run(ctx, effectdispatch.ChildRequest{
		WorkspaceID: request.WorkspaceID, DomainKind: contracts.DomainEffectKindEmbeddingCall,
		DomainID: request.RequestID + ":reconciliation", DomainHash: requestHash,
		RequestID: "embedding-reconcile-request:" + request.RequestID, IdempotencyKey: "embedding-reconcile:" + request.RequestID + ":" + request.ProviderRequestID,
		CausationID: request.CausationID, CorrelationID: request.CorrelationID, Actor: request.Actor,
		Task: request.Task, TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence, Session: request.Session,
		DeploymentAdmission: deploymentAdmission,
		ExternalBoundary:    provider.ExternalBoundary,
		Capability:          definition, Target: target, InputType: "fornix.embedding.reconciliation", InputHash: requestHash,
		Policy: autonomousPolicy(request.WorkspaceID, request.Actor, definition, target, true), Effect: effect,
		Invoker: func(invokeCtx context.Context, authority contracts.EffectAuthority) (effectdispatch.InvocationResult, error) {
			called, invokeErr := invoke(invokeCtx)
			response = called
			if invokeErr != nil {
				return effectdispatch.InvocationResult{}, &effectdispatch.ExternalDispatchError{Err: invokeErr, PossiblyStarted: true}
			}
			return embeddingReconciliationInvocation(authority, request, definition, effect, response), nil
		},
	})
	if dispatchErr != nil {
		return contracts.EmbeddingResponse{}, dispatchErr
	}
	if dispatchResult.Duplicate && len(response.Vector) == 0 {
		return contracts.EmbeddingResponse{}, effectdispatch.ErrUncertainOutcome
	}
	return response, nil
}

func (a *domainEffectAdapters) RunComplete(ctx context.Context, request contracts.ModelRequest, provider contracts.ProviderRef, attempt int, invoke func(context.Context) (contracts.ModelResponse, error)) (contracts.ModelResponse, error) {
	requestHash, err := request.RequestHash()
	if err != nil {
		return contracts.ModelResponse{}, err
	}
	definition := modelCapability(request.WorkspaceID, provider)
	target := modelTarget(request.WorkspaceID, provider)
	effect := contracts.ExternalEffect{WorkspaceID: request.WorkspaceID, Boundary: "model-provider", Class: contracts.EffectClassExternalCommunication, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, ProviderIdempotency: provider.Provider == "openai"}
	deploymentAdmission, err := a.admissionReference(ctx, request.WorkspaceID)
	if err != nil {
		return contracts.ModelResponse{}, err
	}
	var response contracts.ModelResponse
	_, dispatchErr := a.runtime.Run(ctx, effectdispatch.ChildRequest{
		WorkspaceID: request.WorkspaceID, DomainKind: contracts.DomainEffectKindModelCall,
		DomainID: request.IdempotencyKey, DomainHash: requestHash,
		RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey + ":model:" + fmt.Sprint(attempt),
		CausationID: request.CausationID, CorrelationID: request.CorrelationID, Actor: request.Actor,
		Task: request.Task, TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence, Session: request.Session,
		AgentRunID: effectEntityID(request.AgentRun), AgentRunOwnerID: request.AgentRunOwnerID, AgentRunFence: request.AgentRunFence,
		DeploymentAdmission: deploymentAdmission,
		ExternalBoundary:    provider.ExternalBoundary,
		Capability:          definition, Target: target, InputType: "fornix.model.request", InputHash: requestHash,
		Policy: autonomousPolicy(request.WorkspaceID, request.Actor, definition, target, true), Effect: effect,
		Invoker: func(invokeCtx context.Context, authority contracts.EffectAuthority) (effectdispatch.InvocationResult, error) {
			called, invokeErr := invoke(invokeCtx)
			response = called
			if invokeErr != nil {
				return effectdispatch.InvocationResult{}, modelDispatchError(invokeErr)
			}
			return modelInvocation(authority, request, definition, effect, response), nil
		},
	})
	if dispatchErr != nil {
		return contracts.ModelResponse{}, unwrapModelDispatchError(dispatchErr)
	}
	return response, nil
}

func (a *domainEffectAdapters) RunStream(ctx context.Context, request contracts.ModelRequest, provider contracts.ProviderRef, attempt int, sink model.StreamSink, invoke func(context.Context, model.StreamSink) (contracts.ModelResponse, error)) (contracts.ModelResponse, error) {
	requestHash, err := request.RequestHash()
	if err != nil {
		return contracts.ModelResponse{}, err
	}
	definition := modelCapability(request.WorkspaceID, provider)
	target := modelTarget(request.WorkspaceID, provider)
	effect := contracts.ExternalEffect{WorkspaceID: request.WorkspaceID, Boundary: "model-provider", Class: contracts.EffectClassExternalCommunication, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, ProviderIdempotency: provider.Provider == "openai"}
	deploymentAdmission, err := a.admissionReference(ctx, request.WorkspaceID)
	if err != nil {
		return contracts.ModelResponse{}, err
	}
	var response contracts.ModelResponse
	_, dispatchErr := a.runtime.Run(ctx, effectdispatch.ChildRequest{
		WorkspaceID: request.WorkspaceID, DomainKind: contracts.DomainEffectKindModelCall,
		DomainID: request.IdempotencyKey, DomainHash: requestHash,
		RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey + ":model-stream:" + fmt.Sprint(attempt),
		CausationID: request.CausationID, CorrelationID: request.CorrelationID, Actor: request.Actor,
		Task: request.Task, TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence, Session: request.Session,
		AgentRunID: effectEntityID(request.AgentRun), AgentRunOwnerID: request.AgentRunOwnerID, AgentRunFence: request.AgentRunFence,
		DeploymentAdmission: deploymentAdmission,
		ExternalBoundary:    provider.ExternalBoundary,
		Capability:          definition, Target: target, InputType: "fornix.model.request", InputHash: requestHash,
		Policy: autonomousPolicy(request.WorkspaceID, request.Actor, definition, target, true), Effect: effect,
		Invoker: func(invokeCtx context.Context, authority contracts.EffectAuthority) (effectdispatch.InvocationResult, error) {
			called, invokeErr := invoke(invokeCtx, sink)
			response = called
			if invokeErr != nil {
				return effectdispatch.InvocationResult{}, modelDispatchError(invokeErr)
			}
			return modelInvocation(authority, request, definition, effect, response), nil
		},
	})
	if dispatchErr != nil {
		return contracts.ModelResponse{}, unwrapModelDispatchError(dispatchErr)
	}
	return response, nil
}

func (a *domainEffectAdapters) Run(ctx context.Context, request contracts.ToolRequest, definition contracts.ToolDefinition, run contracts.ToolRun, invoke func(context.Context, contracts.EffectAuthority) (contracts.ToolResult, error)) (contracts.ToolResult, error) {
	requestHash := run.RequestHash
	if requestHash == "" {
		var err error
		requestHash, err = request.RequestHash()
		if err != nil {
			return contracts.ToolResult{}, err
		}
	}
	capability := toolCapability(request.WorkspaceID, definition)
	target := toolTarget(request.WorkspaceID, definition)
	effect := contracts.ExternalEffect{WorkspaceID: request.WorkspaceID, Boundary: definition.Sandbox.Backend, Class: contracts.EffectClassReversibleWrite, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce}
	deploymentAdmission, err := a.admissionReference(ctx, request.WorkspaceID)
	if err != nil {
		return contracts.ToolResult{}, err
	}
	var output contracts.ToolResult
	dispatchResult, dispatchErr := a.runtime.Run(ctx, effectdispatch.ChildRequest{
		WorkspaceID: request.WorkspaceID, DomainKind: contracts.DomainEffectKindToolRun,
		DomainID: run.ID, DomainHash: requestHash, RequestID: request.RequestID,
		IdempotencyKey: request.IdempotencyKey + ":tool", CausationID: request.CausationID, CorrelationID: request.CorrelationID,
		Actor: request.Actor, Task: request.Task, TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence, Session: request.Session,
		AgentRunID: effectEntityID(request.AgentRun), AgentRunOwnerID: request.AgentRunOwnerID, AgentRunFence: request.AgentRunFence,
		DeploymentAdmission: deploymentAdmission,
		Capability:          capability, Target: target, InputType: "fornix.tool.request", InputHash: requestHash,
		Policy: autonomousPolicy(request.WorkspaceID, request.Actor, capability, target, false), Effect: effect,
		Invoker: func(invokeCtx context.Context, authority contracts.EffectAuthority) (effectdispatch.InvocationResult, error) {
			called, invokeErr := invoke(invokeCtx, authority)
			output = called
			if invokeErr != nil {
				return effectdispatch.InvocationResult{}, &effectdispatch.ExternalDispatchError{Err: invokeErr, PossiblyStarted: true}
			}
			if output.Status == "" {
				output.Status = contracts.ToolRunFailed
			}
			if output.Status == contracts.ToolRunSucceeded && output.Failure != nil {
				output.Status = contracts.ToolRunFailed
			}
			return toolInvocation(authority, request, definition, effect, output), nil
		},
		FinalizeTx: func(finalizeCtx context.Context, tx pgx.Tx, resultHash string) error {
			if a == nil || a.toolRuns == nil {
				return fmt.Errorf("tool result authority is not configured")
			}
			return a.toolRuns.FinalizeEffectResultTx(finalizeCtx, tx, run, output, resultHash)
		},
	})
	if dispatchErr != nil {
		return contracts.ToolResult{}, dispatchErr
	}
	if dispatchResult.Duplicate && output.Status == "" {
		if a.toolRuns == nil {
			return contracts.ToolResult{}, effectdispatch.ErrUncertainOutcome
		}
		canonical, readErr := a.toolRuns.GetByID(ctx, request.WorkspaceID, run.ID)
		if readErr != nil {
			return contracts.ToolResult{}, readErr
		}
		if canonical.Result == nil || !contracts.IsToolTerminal(canonical.Status) {
			return contracts.ToolResult{}, effectdispatch.ErrUncertainOutcome
		}
		output = *canonical.Result
	}
	return output, nil
}

func (a *domainEffectAdapters) RunChange(ctx context.Context, request contracts.ChangeApplicationRequest, proposal contracts.ChangeProposal, application contracts.ChangeApplication, root string, packet contracts.ChangePacket, resolve change.ContentResolver, invoke func(context.Context) (change.AppliedChange, error)) (change.AppliedChange, error) {
	capability := changeCapability(request.WorkspaceID)
	target := changeTarget(request.WorkspaceID, proposal.Repository)
	packetHash := packet.StableHash()
	taskOwnerID, taskFence := request.TaskOwnerID, request.TaskFence
	if taskOwnerID == "" {
		taskOwnerID, taskFence = proposal.TaskOwnerID, proposal.TaskFence
	}
	effect := contracts.ExternalEffect{WorkspaceID: request.WorkspaceID, Boundary: "configured-filesystem-mount", Class: contracts.EffectClassReversibleWrite, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce}
	deploymentAdmission, err := a.admissionReference(ctx, request.WorkspaceID)
	if err != nil {
		return change.AppliedChange{}, err
	}
	var applied change.AppliedChange
	_, dispatchErr := a.runtime.Run(ctx, effectdispatch.ChildRequest{
		WorkspaceID: request.WorkspaceID, DomainKind: contracts.DomainEffectKindChangeApplication,
		DomainID: application.ID, DomainHash: packetHash, RequestID: request.ID,
		IdempotencyKey: request.IdempotencyKey + ":change", Actor: request.Actor, Task: proposal.Task,
		TaskOwnerID: taskOwnerID, TaskFence: taskFence, Capability: capability, Target: target,
		DeploymentAdmission: deploymentAdmission,
		InputType:           "fornix.change.packet", InputHash: packetHash,
		Policy: autonomousPolicy(request.WorkspaceID, request.Actor, capability, target, false), Effect: effect,
		Invoker: func(invokeCtx context.Context, authority contracts.EffectAuthority) (effectdispatch.InvocationResult, error) {
			called, invokeErr := invoke(invokeCtx)
			applied = called
			if invokeErr != nil {
				return effectdispatch.InvocationResult{}, &effectdispatch.ExternalDispatchError{Err: invokeErr, PossiblyStarted: called.AppliedOperations > 0}
			}
			return changeInvocation(authority, request, capability, effect, called, packetHash), nil
		},
	})
	if dispatchErr != nil {
		return change.AppliedChange{}, dispatchErr
	}
	return applied, nil
}

func modelInvocation(authority contracts.EffectAuthority, request contracts.ModelRequest, definition contracts.CapabilityDefinition, effect contracts.ExternalEffect, response contracts.ModelResponse) effectdispatch.InvocationResult {
	responseHash := contracts.HashStrings(response.RequestID, response.ProviderRequestID, response.Content, response.FinishReason)
	returned := effect
	returned.ID, returned.IdempotencyKey, returned.ProviderRequestID = authority.EffectID, authority.EffectID, response.ProviderRequestID
	operationResult := contracts.OperationResult{
		ID: "model-result-" + responseHash[:32], OperationID: authority.OperationID, OperationHash: authority.RequestHash,
		RequestID: request.RequestID, WorkspaceID: request.WorkspaceID, Actor: request.Actor, Status: contracts.OperationStatusSucceeded,
		OutputSchemaVersion: definition.OutputSchemaVersion, OutputSchemaHash: definition.OutputSchemaHash, OutputHash: responseHash,
		Steps:           []contracts.OperationStepResult{{StepID: "child-step-" + contracts.HashStrings(authority.OperationID)[:32], Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: definition.OutputSchemaVersion, OutputSchemaHash: definition.OutputSchemaHash, OutputHash: responseHash, ExternalEffect: &returned}},
		ExternalEffects: []contracts.ExternalEffect{returned},
	}
	return effectdispatch.InvocationResult{Result: operationResult, ProviderRequestID: response.ProviderRequestID, ResponseHash: responseHash, ContentEmitted: response.Content != ""}
}

func toolInvocation(authority contracts.EffectAuthority, request contracts.ToolRequest, definition contracts.ToolDefinition, effect contracts.ExternalEffect, output contracts.ToolResult) effectdispatch.InvocationResult {
	outputHash := output.Hash()
	returned := effect
	returned.ID, returned.IdempotencyKey = authority.EffectID, authority.EffectID
	outputSchemaHash := contracts.HashStrings("fornix.tool.output", definition.ID)
	operationStatus := contracts.OperationStatusSucceeded
	stepStatus := contracts.OperationStatusSucceeded
	var operationFailure *contracts.OperationFailure
	if output.Status == contracts.ToolRunFailed {
		operationStatus, stepStatus = contracts.OperationStatusFailed, contracts.OperationStatusFailed
		failureCode := "tool_failed"
		if output.Failure != nil && output.Failure.Code != "" {
			failureCode = output.Failure.Code
		}
		operationFailure = &contracts.OperationFailure{SchemaVersion: contracts.DomainNeutralSchemaVersion, WorkspaceID: request.WorkspaceID, Code: contracts.OperationFailureAdapter, Retryable: output.Failure != nil && output.Failure.Retryable, DetailHash: contracts.HashStrings("fornix.tool.failure", failureCode, outputHash)}
	}
	operationResult := contracts.OperationResult{
		ID: "tool-result-" + outputHash[:32], OperationID: authority.OperationID, OperationHash: authority.RequestHash,
		RequestID: request.RequestID, WorkspaceID: request.WorkspaceID, Actor: request.Actor, Status: operationStatus, Failure: operationFailure,
		OutputSchemaVersion: contracts.ToolSchemaVersion, OutputSchemaHash: outputSchemaHash, OutputHash: outputHash,
		Steps:           []contracts.OperationStepResult{{StepID: "child-step-" + contracts.HashStrings(authority.OperationID)[:32], Status: stepStatus, OutputSchemaVersion: contracts.ToolSchemaVersion, OutputSchemaHash: outputSchemaHash, OutputHash: outputHash, ExternalEffect: &returned}},
		ExternalEffects: []contracts.ExternalEffect{returned},
	}
	return effectdispatch.InvocationResult{Result: operationResult, ResponseHash: outputHash, ProviderRequestID: "process:" + definition.ID}
}

func changeInvocation(authority contracts.EffectAuthority, request contracts.ChangeApplicationRequest, definition contracts.CapabilityDefinition, effect contracts.ExternalEffect, applied change.AppliedChange, packetHash string) effectdispatch.InvocationResult {
	outputHash := contracts.HashStrings(applied.ResultTreeHash, packetHash)
	returned := effect
	returned.ID, returned.IdempotencyKey = authority.EffectID, authority.EffectID
	operationResult := contracts.OperationResult{
		ID: "change-result-" + outputHash[:32], OperationID: authority.OperationID, OperationHash: authority.RequestHash,
		RequestID: request.ID, WorkspaceID: request.WorkspaceID, Actor: request.Actor, Status: contracts.OperationStatusSucceeded,
		OutputSchemaVersion: definition.OutputSchemaVersion, OutputSchemaHash: definition.OutputSchemaHash, OutputHash: outputHash,
		Steps:           []contracts.OperationStepResult{{StepID: "child-step-" + contracts.HashStrings(authority.OperationID)[:32], Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: definition.OutputSchemaVersion, OutputSchemaHash: definition.OutputSchemaHash, OutputHash: outputHash, ExternalEffect: &returned}},
		ExternalEffects: []contracts.ExternalEffect{returned},
	}
	return effectdispatch.InvocationResult{Result: operationResult, ResponseHash: outputHash}
}

func embeddingInvocation(authority contracts.EffectAuthority, request contracts.EmbeddingRequest, definition contracts.CapabilityDefinition, effect contracts.ExternalEffect, vector []float32) (effectdispatch.InvocationResult, error) {
	outputHash, err := contracts.EmbeddingVectorHash(vector)
	if err != nil {
		return effectdispatch.InvocationResult{}, err
	}
	returned := effect
	returned.ID, returned.IdempotencyKey = authority.EffectID, authority.EffectID
	operationResult := contracts.OperationResult{
		ID: "embedding-result-" + outputHash[:32], OperationID: authority.OperationID, OperationHash: authority.RequestHash,
		RequestID: request.RequestID, WorkspaceID: request.WorkspaceID, Actor: request.Actor, Status: contracts.OperationStatusSucceeded,
		OutputSchemaVersion: definition.OutputSchemaVersion, OutputSchemaHash: definition.OutputSchemaHash, OutputHash: outputHash,
		Steps:           []contracts.OperationStepResult{{StepID: "child-step-" + contracts.HashStrings(authority.OperationID)[:32], Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: definition.OutputSchemaVersion, OutputSchemaHash: definition.OutputSchemaHash, OutputHash: outputHash, ExternalEffect: &returned}},
		ExternalEffects: []contracts.ExternalEffect{returned},
	}
	return effectdispatch.InvocationResult{Result: operationResult, ResponseHash: outputHash}, nil
}

func embeddingReconciliationInvocation(authority contracts.EffectAuthority, request contracts.EmbeddingReconciliationRequest, definition contracts.CapabilityDefinition, effect contracts.ExternalEffect, response contracts.EmbeddingResponse) effectdispatch.InvocationResult {
	vectorHash, _ := contracts.EmbeddingVectorHash(response.Vector)
	outputHash := contracts.HashStrings("embedding-reconciliation", request.RequestHash, response.ProviderRequestID, response.SourceHash, vectorHash, responseFailureCode(response.Failure))
	returned := effect
	returned.ID, returned.IdempotencyKey, returned.ProviderRequestID = authority.EffectID, authority.EffectID, response.ProviderRequestID
	status := contracts.OperationStatusSucceeded
	var failure *contracts.OperationFailure
	if response.Failure != nil {
		status = contracts.OperationStatusFailed
		failure = &contracts.OperationFailure{SchemaVersion: contracts.DomainNeutralSchemaVersion, WorkspaceID: request.WorkspaceID, Code: contracts.OperationFailureAdapter, Retryable: response.Failure.Retryable, DetailHash: contracts.HashStrings("embedding-reconciliation-failure", response.Failure.Code, response.ProviderRequestID)}
	}
	operationResult := contracts.OperationResult{
		ID: "embedding-reconciliation-result-" + outputHash[:32], OperationID: authority.OperationID, OperationHash: authority.RequestHash,
		RequestID: "embedding-reconcile-request:" + request.RequestID, WorkspaceID: request.WorkspaceID, Actor: request.Actor, Status: status, Failure: failure,
		OutputSchemaVersion: definition.OutputSchemaVersion, OutputSchemaHash: definition.OutputSchemaHash, OutputHash: outputHash,
		Steps:           []contracts.OperationStepResult{{StepID: "child-step-" + contracts.HashStrings(authority.OperationID)[:32], Status: status, OutputSchemaVersion: definition.OutputSchemaVersion, OutputSchemaHash: definition.OutputSchemaHash, OutputHash: outputHash, ExternalEffect: &returned}},
		ExternalEffects: []contracts.ExternalEffect{returned},
	}
	return effectdispatch.InvocationResult{Result: operationResult, ProviderRequestID: response.ProviderRequestID, ResponseHash: outputHash}
}

func responseFailureCode(failure *contracts.EmbeddingFailure) string {
	if failure == nil {
		return ""
	}
	return failure.Code
}

func modelDispatchError(err error) error {
	var failure *model.FailureError
	if errors.As(err, &failure) {
		possiblyStarted := failure.Failure.ContentEmitted || failure.Failure.Code == contracts.ModelFailureTransport || failure.Failure.Code == contracts.ModelFailureTimeout
		return &effectdispatch.ExternalDispatchError{Err: err, PossiblyStarted: possiblyStarted, ContentEmitted: failure.Failure.ContentEmitted}
	}
	return &effectdispatch.ExternalDispatchError{Err: err, PossiblyStarted: true}
}

func unwrapModelDispatchError(err error) error {
	var uncertain *effectdispatch.ExternalDispatchError
	if errors.As(err, &uncertain) && !uncertain.PossiblyStarted && uncertain.Err != nil {
		return uncertain.Err
	}
	return err
}

func modelCapability(workspace string, provider contracts.ProviderRef) contracts.CapabilityDefinition {
	return effectCapability(workspace, "model", "complete", contracts.EffectClassExternalCommunication, contracts.HashStrings("model-input", provider.Provider, provider.Model), contracts.HashStrings("model-output"))
}

func toolCapability(workspace string, definition contracts.ToolDefinition) contracts.CapabilityDefinition {
	return effectCapability(workspace, "tool", definition.Capability, contracts.EffectClassReversibleWrite, contracts.HashStrings("tool-input", definition.ID), contracts.HashStrings("tool-output", definition.ID))
}

func changeCapability(workspace string) contracts.CapabilityDefinition {
	return effectCapability(workspace, "change", "apply", contracts.EffectClassReversibleWrite, contracts.HashStrings("change-input"), contracts.HashStrings("change-output"))
}

func embeddingCapability(workspace string, provider contracts.ProviderRef) contracts.CapabilityDefinition {
	return effectCapability(workspace, "embedding", "generate", contracts.EffectClassExternalCommunication, contracts.HashStrings("embedding-input", provider.Provider, provider.Model), contracts.HashStrings("embedding-output"))
}

func effectCapability(workspace, connectorName, name string, effect contracts.EffectClass, inputHash, outputHash string) contracts.CapabilityDefinition {
	return contracts.CapabilityDefinition{WorkspaceID: workspace, Ref: contracts.CapabilityRef{WorkspaceID: workspace, Connector: contracts.ConnectorRef{WorkspaceID: workspace, Name: connectorName, Version: "1"}, Name: name, Version: "1"}, Description: "Fornix durable domain effect", InputSchemaVersion: 1, InputSchemaHash: inputHash, OutputSchemaVersion: 1, OutputSchemaHash: outputHash, Effect: effect, Profile: contracts.DefaultExecutionProfile(), RetryPolicy: contracts.CapabilityRetryPolicy{MaxAttempts: 1, BackoffMS: 1, MaxBackoffMS: 1, Jitter: "none"}, ResourceKinds: []string{"domain", "model", "tool", "filesystem"}, MaxRows: 1, RateLimitPerMinute: 1000, SupportsIdempotency: true, SupportsCancellation: true, SupportsVerification: true, Enabled: true}
}

func autonomousPolicy(workspace string, actor contracts.ActorRef, definition contracts.CapabilityDefinition, target contracts.ResourceRef, externalCommunication bool) contracts.AdmissionPolicy {
	return contracts.AdmissionPolicy{WorkspaceID: workspace, PolicyID: "fornix-domain-effect", Version: "1", AllowAutonomousExternalCommunication: externalCommunication, EffectRules: []contracts.AdmissionEffectRule{{Effect: definition.Effect, Mode: contracts.PolicyApprovalAutomatic}}, AllowedConnectors: []contracts.ConnectorRef{definition.Ref.Connector}, AllowedResourceKinds: []string{target.Kind}, AllowedActorIDs: []string{actor.ID}, MaxCostMicros: 1_000_000, MaxOperationsPerWindow: 1000, MaxCostPerWindowMicros: 10_000_000}
}

func modelTarget(workspace string, provider contracts.ProviderRef) contracts.ResourceRef {
	return contracts.ResourceRef{WorkspaceID: workspace, System: contracts.SystemRef{WorkspaceID: workspace, Type: "model_provider", ID: strings.TrimSpace(provider.Provider), Version: "1"}, Kind: "model", ID: strings.TrimSpace(provider.Provider) + ":" + strings.TrimSpace(provider.Model), Version: "1"}
}

func toolTarget(workspace string, definition contracts.ToolDefinition) contracts.ResourceRef {
	return contracts.ResourceRef{WorkspaceID: workspace, System: contracts.SystemRef{WorkspaceID: workspace, Type: "local_process", ID: definition.ID, Version: definition.Version}, Kind: "tool", ID: definition.ID, Version: definition.Version}
}

func changeTarget(workspace, repository string) contracts.ResourceRef {
	return contracts.ResourceRef{WorkspaceID: workspace, System: contracts.SystemRef{WorkspaceID: workspace, Type: "filesystem_mount", ID: strings.TrimSpace(repository), Version: "1"}, Kind: "filesystem", ID: strings.TrimSpace(repository), Version: "1"}
}

func embeddingTarget(workspace string, provider contracts.ProviderRef) contracts.ResourceRef {
	return contracts.ResourceRef{WorkspaceID: workspace, System: contracts.SystemRef{WorkspaceID: workspace, Type: "embedding_provider", ID: strings.TrimSpace(provider.Provider), Version: "1"}, Kind: "embedding", ID: strings.TrimSpace(provider.Provider) + ":" + strings.TrimSpace(provider.Model), Version: "1"}
}
