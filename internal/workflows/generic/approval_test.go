package generic

import (
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestWorkflowApprovalRequiresSucceededPlanAncestors(t *testing.T) {
	run := contracts.WorkflowRun{
		Plan: contracts.OperationPlan{Steps: []contracts.OperationStep{
			{ID: "approval-a", Kind: contracts.WorkflowStepApproval},
			{ID: "approval-b", Kind: contracts.WorkflowStepApproval},
			{ID: "prepare", Kind: contracts.WorkflowStepValidation, DependsOn: []string{"approval-a"}},
			{ID: "publish", Kind: contracts.WorkflowStepConnector, Effect: contracts.EffectClassApprovalRequiredWrite, DependsOn: []string{"prepare", "approval-b"}},
		}},
		Steps: []contracts.WorkflowStepState{
			{StepID: "approval-a", Status: contracts.WorkflowStepSucceeded},
			{StepID: "approval-b", Status: contracts.WorkflowStepAwaitingApproval},
		},
	}
	if workflowApprovalSatisfied(run, "publish") {
		t.Fatal("effect was authorized before every ancestor approval succeeded")
	}
	if !workflowApprovalGatesEffect(run, "approval-a") || !workflowApprovalGatesEffect(run, "approval-b") {
		t.Fatal("transitive and direct approval dependencies must both gate the write")
	}
	run.Steps[1].Status = contracts.WorkflowStepSucceeded
	if !workflowApprovalSatisfied(run, "publish") {
		t.Fatal("effect was not authorized after all plan-bound approval ancestors succeeded")
	}
	if workflowApprovalSatisfied(run, "prepare") {
		t.Fatal("an unrelated observation step was treated as an effect")
	}
}

func TestUnrelatedApprovalCannotAuthorizeEffect(t *testing.T) {
	run := contracts.WorkflowRun{
		Plan: contracts.OperationPlan{Steps: []contracts.OperationStep{
			{ID: "approval", Kind: contracts.WorkflowStepApproval},
			{ID: "publish", Kind: contracts.WorkflowStepConnector, Effect: contracts.EffectClassApprovalRequiredWrite},
		}},
		Steps: []contracts.WorkflowStepState{{StepID: "approval", Status: contracts.WorkflowStepSucceeded}},
	}
	if workflowApprovalSatisfied(run, "publish") {
		t.Fatal("unrelated approval authorized an effect")
	}
	if workflowApprovalGatesEffect(run, "approval") {
		t.Fatal("unrelated approval was reported as an effect gate")
	}
}
