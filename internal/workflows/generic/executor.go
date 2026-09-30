package generic

import (
	"context"
	"fmt"

	"github.com/omaveda/fornix/internal/contracts"
)

// DeterministicExecutor is the safe offline default. It proves workflow
// scheduling, checkpointing, replay, and workspace isolation without making a
// provider or external-system call. Read and observation steps receive a
// stable hash-only result. Approval and effectful steps pause for an explicit
// authority-aware adapter instead of silently performing side effects.
type DeterministicExecutor struct{}

func (DeterministicExecutor) Execute(_ context.Context, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep) (contracts.WorkflowStepResult, error) {
	switch {
	case step.Kind == contracts.WorkflowStepApproval:
		return contracts.WorkflowStepResult{Status: contracts.WorkflowStepAwaitingApproval, Wait: &contracts.WorkflowWait{Kind: contracts.WorkflowWaitApproval, Token: contracts.HashStrings("generic-approval", run.ID, step.ID, fmt.Sprint(state.Attempt+1)), Reason: "explicit approval is required"}}, nil
	case step.Effect != contracts.EffectClassReadOnly && step.Effect != contracts.EffectClassObservation:
		return contracts.WorkflowStepResult{Status: contracts.WorkflowStepAwaitingExternal, Wait: &contracts.WorkflowWait{Kind: contracts.WorkflowWaitExternal, Token: contracts.HashStrings("generic-authority", run.ID, step.ID, fmt.Sprint(state.Attempt+1)), Reason: "an authority-aware domain adapter is required"}}, nil
	default:
		return contracts.WorkflowStepResult{Status: contracts.WorkflowStepSucceeded, OutputHash: contracts.HashStrings("generic-step-output", run.ID, step.ID, step.InputHash, run.PlanHash)}, nil
	}
}
