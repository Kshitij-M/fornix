package generic

import "github.com/omaveda/fornix/internal/contracts"

// workflowApprovalSatisfied reports whether every approval step that is an
// ancestor of an effectful step has durably succeeded. Unrelated approvals in
// the same plan cannot authorize this effect.
func workflowApprovalSatisfied(run contracts.WorkflowRun, effectStepID string) bool {
	planSteps := make(map[string]contracts.OperationStep, len(run.Plan.Steps))
	states := make(map[string]contracts.WorkflowStepState, len(run.Steps))
	for _, step := range run.Plan.Steps {
		planSteps[step.ID] = step
	}
	for _, state := range run.Steps {
		states[state.StepID] = state
	}
	target, ok := planSteps[effectStepID]
	if !ok || !workflowStepHasExternalEffect(target) {
		return false
	}
	approvalCount := 0
	for _, candidate := range run.Plan.Steps {
		if candidate.Kind != contracts.WorkflowStepApproval || !workflowStepDependsOn(planSteps, target.ID, candidate.ID, map[string]bool{}) {
			continue
		}
		approvalCount++
		if states[candidate.ID].Status != contracts.WorkflowStepSucceeded {
			return false
		}
	}
	return approvalCount > 0
}

// workflowApprovalGatesEffect distinguishes approvals that authorize a write
// from informational workflow pauses.
func workflowApprovalGatesEffect(run contracts.WorkflowRun, approvalStepID string) bool {
	planSteps := make(map[string]contracts.OperationStep, len(run.Plan.Steps))
	for _, step := range run.Plan.Steps {
		planSteps[step.ID] = step
	}
	approval, ok := planSteps[approvalStepID]
	if !ok || approval.Kind != contracts.WorkflowStepApproval {
		return false
	}
	for _, candidate := range run.Plan.Steps {
		if workflowStepHasExternalEffect(candidate) && workflowStepDependsOn(planSteps, candidate.ID, approvalStepID, map[string]bool{}) {
			return true
		}
	}
	return false
}

func workflowStepHasExternalEffect(step contracts.OperationStep) bool {
	switch step.Effect {
	case contracts.EffectClassReversibleWrite, contracts.EffectClassApprovalRequiredWrite,
		contracts.EffectClassIrreversibleWrite, contracts.EffectClassExternalCommunication:
		return true
	default:
		return false
	}
}

func workflowStepDependsOn(steps map[string]contracts.OperationStep, stepID, dependencyID string, visited map[string]bool) bool {
	if visited[stepID] {
		return false
	}
	visited[stepID] = true
	step, ok := steps[stepID]
	if !ok {
		return false
	}
	for _, dependency := range step.DependsOn {
		if dependency == dependencyID || workflowStepDependsOn(steps, dependency, dependencyID, visited) {
			return true
		}
	}
	return false
}
