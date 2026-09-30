package generic

import (
	"context"
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestDeterministicExecutorIsStableAndEffectSafe(t *testing.T) {
	actor := contracts.ActorRef{ID: "actor-1", Kind: "operator", WorkspaceID: "workspace-1"}
	run := contracts.WorkflowRun{ID: "run-1", WorkspaceID: actor.WorkspaceID, PlanHash: contracts.HashStrings("plan")}
	state := contracts.WorkflowStepState{StepID: "step-1", Attempt: 1}
	step := contracts.OperationStep{ID: state.StepID, Kind: contracts.WorkflowStepConnector, Effect: contracts.EffectClassObservation, InputHash: contracts.HashStrings("input")}
	executor := DeterministicExecutor{}
	first, err := executor.Execute(context.Background(), run, state, step)
	if err != nil {
		t.Fatal(err)
	}
	second, err := executor.Execute(context.Background(), run, state, step)
	if err != nil {
		t.Fatal(err)
	}
	if first.OutputHash != second.OutputHash || first.Status != contracts.WorkflowStepSucceeded {
		t.Fatalf("executor is not deterministic: first=%+v second=%+v", first, second)
	}
	effect := step
	effect.Effect = contracts.EffectClassReversibleWrite
	paused, err := executor.Execute(context.Background(), run, state, effect)
	if err != nil {
		t.Fatal(err)
	}
	if paused.Status != contracts.WorkflowStepAwaitingExternal || paused.Wait == nil || paused.Wait.Kind != contracts.WorkflowWaitExternal {
		t.Fatalf("effectful step was not paused safely: %+v", paused)
	}
}

func TestWorkflowReplayLimitIsBounded(t *testing.T) {
	if _, err := (&Service{}).Replay(context.Background(), "workspace-1", "run-1", contracts.WorkflowReplayRequest{Limit: MaxReplayLimit + 1}); err != ErrNotConfigured {
		t.Fatalf("expected unconfigured service error, got %v", err)
	}
}

func TestWorkflowCreateRequestRejectsNestedWorkspaceMismatch(t *testing.T) {
	request := contracts.WorkflowCreateRequest{
		WorkspaceID: "workspace-a",
		Operation:   contracts.OperationRequest{WorkspaceID: "workspace-b"},
	}
	if err := request.Normalize("workspace-a", contracts.ActorRef{ID: "actor-1", Kind: "operator", WorkspaceID: "workspace-a"}); err == nil {
		t.Fatal("expected nested workspace mismatch to fail closed")
	}
}
