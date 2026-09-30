package store

import (
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestWorkflowTransitionActorAllowsOnlyScopedApprovalReviewer(t *testing.T) {
	requester := contracts.ActorRef{ID: "requester", Kind: "user", WorkspaceID: "workspace-a"}
	operation := Operation{WorkspaceID: "workspace-a", Request: contracts.OperationRequest{Actor: requester}}
	previous := contracts.WorkflowRun{WorkspaceID: "workspace-a", Plan: contracts.OperationPlan{Steps: []contracts.OperationStep{
		{ID: "approval", Kind: contracts.WorkflowStepApproval},
		{ID: "work", Kind: contracts.WorkflowStepValidation},
	}}}
	reviewer := contracts.ActorRef{ID: "reviewer", Kind: "user", WorkspaceID: "workspace-a"}

	tests := []struct {
		name   string
		stepID string
		from   string
		to     string
		actor  contracts.ActorRef
		want   bool
	}{
		{name: "requester can perform ordinary transition", stepID: "work", from: contracts.WorkflowStepRunning, to: contracts.WorkflowStepSucceeded, actor: requester, want: true},
		{name: "distinct workspace reviewer can approve approval step", stepID: "approval", from: contracts.WorkflowStepAwaitingApproval, to: contracts.WorkflowStepSucceeded, actor: reviewer, want: true},
		{name: "reviewer cannot complete ordinary step", stepID: "work", from: contracts.WorkflowStepAwaitingApproval, to: contracts.WorkflowStepSucceeded, actor: reviewer},
		{name: "reviewer cannot change workspace", stepID: "approval", from: contracts.WorkflowStepAwaitingApproval, to: contracts.WorkflowStepSucceeded, actor: contracts.ActorRef{ID: "reviewer", Kind: "user", WorkspaceID: "workspace-b"}},
		{name: "reviewer cannot fail or skip approval", stepID: "approval", from: contracts.WorkflowStepAwaitingApproval, to: contracts.WorkflowStepFailed, actor: reviewer},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := workflowTransitionActorAllowed(operation, previous, test.stepID, test.from, test.to, test.actor)
			if got != test.want {
				t.Fatalf("workflowTransitionActorAllowed()=%t, want %t", got, test.want)
			}
		})
	}
}
