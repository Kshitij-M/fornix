// Package reference contains offline qualification fixtures for Fornix's
// domain-neutral operation and workflow seams. It intentionally performs no
// provider, filesystem, network, model, or database work.
package reference

import (
	"context"
	"fmt"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
)

const connectorName = "fornix-reference"

// Build creates a deterministic generic plan for one reference domain. A
// real adapter may submit the resulting operation to Postgres; this builder
// does not persist or execute it.
func Build(request contracts.ReferenceWorkflowRequest) (contracts.ReferenceWorkflowPlan, error) {
	if err := request.Normalize(); err != nil {
		return contracts.ReferenceWorkflowPlan{}, err
	}
	inputHash := request.IntentHash
	profile := contracts.DefaultExecutionProfile()
	profile.MaxSteps = 8
	if err := profile.Normalize(); err != nil {
		return contracts.ReferenceWorkflowPlan{}, err
	}
	observation, err := capability(request, "observe", contracts.EffectClassObservation, inputHash, request.Target.Kind, false)
	if err != nil {
		return contracts.ReferenceWorkflowPlan{}, err
	}
	validation, err := capability(request, "validate", contracts.EffectClassObservation, inputHash, request.Target.Kind, false)
	if err != nil {
		return contracts.ReferenceWorkflowPlan{}, err
	}
	verification, err := capability(request, "verify", contracts.EffectClassObservation, inputHash, request.Target.Kind, false)
	if err != nil {
		return contracts.ReferenceWorkflowPlan{}, err
	}
	steps := []contracts.OperationStep{
		{ID: request.ID + "-observe", Ordinal: 0, Kind: contracts.WorkflowStepConnector, Capability: observation, Target: request.Target, Effect: contracts.EffectClassObservation, Profile: profile, InputHash: inputHash},
		{ID: request.ID + "-validate", Ordinal: 1, Kind: contracts.WorkflowStepValidation, Capability: validation, Target: request.Target, DependsOn: []string{request.ID + "-observe"}, Effect: contracts.EffectClassObservation, Profile: profile, InputHash: inputHash},
	}
	if request.Effectful {
		effect, err := capability(request, "execute", contracts.EffectClassApprovalRequiredWrite, inputHash, request.Target.Kind, true)
		if err != nil {
			return contracts.ReferenceWorkflowPlan{}, err
		}
		steps = append(steps,
			contracts.OperationStep{ID: request.ID + "-approval", Ordinal: 2, Kind: contracts.WorkflowStepApproval, Capability: validation, Target: request.Target, DependsOn: []string{request.ID + "-validate"}, Effect: contracts.EffectClassObservation, Profile: profile, InputHash: inputHash},
			contracts.OperationStep{ID: request.ID + "-execute", Ordinal: 3, Kind: contracts.WorkflowStepConnector, Capability: effect, Target: request.Target, DependsOn: []string{request.ID + "-approval"}, Effect: contracts.EffectClassApprovalRequiredWrite, Profile: profile, InputHash: inputHash},
			contracts.OperationStep{ID: request.ID + "-verify", Ordinal: 4, Kind: contracts.WorkflowStepValidation, Capability: verification, Target: request.Target, DependsOn: []string{request.ID + "-execute"}, Effect: contracts.EffectClassObservation, Profile: profile, InputHash: inputHash},
		)
	} else {
		steps = append(steps, contracts.OperationStep{ID: request.ID + "-verify", Ordinal: 2, Kind: contracts.WorkflowStepValidation, Capability: verification, Target: request.Target, DependsOn: []string{request.ID + "-validate"}, Effect: contracts.EffectClassObservation, Profile: profile, InputHash: inputHash})
	}
	operation := contracts.OperationRequest{ID: request.ID + "-operation", RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey, WorkspaceID: request.WorkspaceID, Actor: request.Actor, Capability: observation, Target: request.Target, InputType: "fornix.reference." + request.Kind, InputSchemaVersion: 1, InputSchemaHash: inputHash, InputHash: inputHash, Profile: profile, Metadata: map[string]string{"reference_workflow": request.Kind}}
	if err := operation.Normalize(); err != nil {
		return contracts.ReferenceWorkflowPlan{}, err
	}
	plan := contracts.OperationPlan{ID: request.ID + "-plan", OperationID: operation.ID, OperationHash: operation.StableHash(), WorkspaceID: request.WorkspaceID, Actor: request.Actor, Steps: steps}
	if err := plan.Normalize(); err != nil {
		return contracts.ReferenceWorkflowPlan{}, err
	}
	return contracts.ReferenceWorkflowPlan{SchemaVersion: contracts.ReferenceWorkflowSchemaVersion, Request: request, Operation: operation, Plan: plan, Budget: contracts.DefaultWorkflowBudget(), PlanHash: plan.StableHash()}, nil
}

func capability(request contracts.ReferenceWorkflowRequest, name string, effect contracts.EffectClass, inputHash, resourceKind string, requiresApproval bool) (contracts.CapabilityRef, error) {
	profile := contracts.DefaultExecutionProfile()
	definition := contracts.CapabilityDefinition{WorkspaceID: request.WorkspaceID, Ref: contracts.CapabilityRef{WorkspaceID: request.WorkspaceID, Connector: contracts.ConnectorRef{WorkspaceID: request.WorkspaceID, Name: connectorName, Version: "1"}, Name: name, Version: "1"}, Description: "deterministic reference qualification capability", InputSchemaVersion: 1, InputSchemaHash: inputHash, OutputSchemaVersion: 1, OutputSchemaHash: contracts.HashStrings("fornix-reference-output", request.Kind, name), Effect: effect, Profile: profile, MaxRows: 1, RateLimitPerMinute: 60, RequiresApproval: requiresApproval, SupportsCancellation: true, SupportsIdempotency: true, SupportsVerification: true, Enabled: true, ResourceKinds: []string{resourceKind}}
	if err := definition.Normalize(); err != nil {
		return contracts.CapabilityRef{}, err
	}
	return definition.Ref, nil
}

// Replay runs the plan using deterministic synthetic output hashes. It never
// invokes a connector. Effectful plans remain recovery_required until the
// caller supplies a recorded external effect hash from a real authority.
func Replay(_ context.Context, plan contracts.ReferenceWorkflowPlan, recordedEffectHash string) (contracts.ReferenceWorkflowTrace, error) {
	if plan.SchemaVersion != contracts.ReferenceWorkflowSchemaVersion || plan.PlanHash == "" {
		return contracts.ReferenceWorkflowTrace{}, fmt.Errorf("reference workflow plan is invalid")
	}
	if err := plan.Request.Normalize(); err != nil {
		return contracts.ReferenceWorkflowTrace{}, err
	}
	if err := plan.Plan.Normalize(); err != nil {
		return contracts.ReferenceWorkflowTrace{}, err
	}
	if plan.Plan.StableHash() != plan.PlanHash {
		return contracts.ReferenceWorkflowTrace{}, fmt.Errorf("reference workflow plan hash does not match normalized plan")
	}
	stepHashes := make([]string, 0, len(plan.Plan.Steps))
	terminal := contracts.OperationStatusSucceeded
	effectState := contracts.ReferenceWorkflowEffectNotApplicable
	for _, step := range plan.Plan.Steps {
		stepHashes = append(stepHashes, contracts.ReferenceWorkflowStepHash(plan.PlanHash, step))
		if step.Effect == contracts.EffectClassApprovalRequiredWrite || step.Effect == contracts.EffectClassReversibleWrite || step.Effect == contracts.EffectClassIrreversibleWrite || step.Effect == contracts.EffectClassExternalCommunication {
			effectState = contracts.ReferenceWorkflowEffectUnresolved
			terminal = contracts.OperationStatusRecoveryRequired
		}
	}
	if effectState != contracts.ReferenceWorkflowEffectNotApplicable {
		if strings.TrimSpace(recordedEffectHash) != "" {
			if _, err := normalizeHash(recordedEffectHash); err != nil {
				return contracts.ReferenceWorkflowTrace{}, err
			}
			effectState, terminal = contracts.ReferenceWorkflowEffectVerified, contracts.OperationStatusSucceeded
		}
	}
	trace := contracts.ReferenceWorkflowTrace{SchemaVersion: contracts.ReferenceWorkflowSchemaVersion, WorkspaceID: plan.Request.WorkspaceID, WorkflowID: plan.Request.ID, PlanHash: plan.PlanHash, StepOutputHashes: stepHashes, TerminalStatus: terminal, ExternalEffectState: effectState}
	trace.ReplayHash = contracts.HashStrings("fornix-reference-replay", trace.PlanHash, strings.Join(trace.StepOutputHashes, ","), trace.TerminalStatus, trace.ExternalEffectState)
	return trace, nil
}

func normalizeHash(value string) (string, error) {
	value = strings.TrimSpace(value)
	if len(value) != 64 {
		return "", fmt.Errorf("recorded effect hash must be a sha256 hex digest")
	}
	for _, char := range value {
		if !((char >= '0' && char <= '9') || (char >= 'a' && char <= 'f') || (char >= 'A' && char <= 'F')) {
			return "", fmt.Errorf("recorded effect hash must be hexadecimal")
		}
	}
	return strings.ToLower(value), nil
}
