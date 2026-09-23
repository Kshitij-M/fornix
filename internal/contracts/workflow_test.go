package contracts

import (
	"strings"
	"testing"
	"time"
)

func TestWorkflowRunNormalizesAndHashesWithoutDeliveryFacts(t *testing.T) {
	workspace := "workflow-a"
	hash := strings.Repeat("a", 64)
	plan := workflowTestPlan(t, workspace, hash)
	planHash := plan.StableHash()
	run := WorkflowRun{
		WorkspaceID: workspace, ID: "workflow-1",
		Operation: OperationReference{WorkspaceID: workspace, ID: "operation-1", Hash: hash},
		Plan:      plan, PlanHash: planHash, Actor: ActorRef{ID: "actor-1", Kind: "operator", WorkspaceID: workspace}, Status: WorkflowStatusCreated,
		Budget: WorkflowBudget{MaxParallelRead: 1}, Steps: []WorkflowStepState{{WorkspaceID: workspace, RunID: "workflow-1", StepID: "step-1", Ordinal: 0, Kind: WorkflowStepValidation, Status: WorkflowStepPlanned}, {WorkspaceID: workspace, RunID: "workflow-1", StepID: "step-2", Ordinal: 1, Kind: WorkflowStepValidation, Status: WorkflowStepPlanned}},
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := run.Normalize(); err != nil {
		t.Fatal(err)
	}
	first := run.StableHash()
	run.StateVersion, run.StateHash = 4, hash
	run.CreatedAt, run.UpdatedAt = time.Now().Add(time.Hour), time.Now().Add(2*time.Hour)
	if second := run.StableHash(); second != first {
		t.Fatalf("delivery facts changed workflow hash: %s != %s", second, first)
	}
}

func TestWorkflowRunnableStepsRespectDependenciesAndFanout(t *testing.T) {
	workspace := "workflow-a"
	hash := strings.Repeat("b", 64)
	plan := workflowTestPlan(t, workspace, hash)
	plan.Steps[1].DependsOn = []string{"step-1"}
	if err := plan.Normalize(); err != nil {
		t.Fatal(err)
	}
	run := WorkflowRun{WorkspaceID: workspace, ID: "workflow-1", Operation: OperationReference{WorkspaceID: workspace, ID: "operation-1", Hash: hash}, Plan: plan, PlanHash: plan.StableHash(), Actor: ActorRef{ID: "actor-1", Kind: "operator", WorkspaceID: workspace}, Status: WorkflowStatusRunning, Budget: WorkflowBudget{MaxParallelRead: 1}, Steps: []WorkflowStepState{{WorkspaceID: workspace, RunID: "workflow-1", StepID: "step-1", Ordinal: 0, Kind: WorkflowStepValidation, Status: WorkflowStepPlanned}, {WorkspaceID: workspace, RunID: "workflow-1", StepID: "step-2", Ordinal: 1, Kind: WorkflowStepValidation, Status: WorkflowStepPlanned}}}
	if err := run.Normalize(); err != nil {
		t.Fatal(err)
	}
	ready := run.WorkflowRunnableSteps()
	if len(ready) != 1 || ready[0].StepID != "step-1" {
		t.Fatalf("ready steps=%+v, want step-1 only", ready)
	}
	run.Steps[0].Status = WorkflowStepSucceeded
	ready = run.WorkflowRunnableSteps()
	if len(ready) != 1 || ready[0].StepID != "step-2" {
		t.Fatalf("joined ready steps=%+v, want step-2 only", ready)
	}
}

func TestWorkflowStepResultRejectsCrossWorkspaceArtifact(t *testing.T) {
	result := WorkflowStepResult{Status: WorkflowStepSucceeded, Artifacts: []ArtifactRef{{ID: 1, ArtifactID: 1, WorkspaceID: "workspace-b", ContentHash: strings.Repeat("c", 64), SourceKind: "workflow", SourceID: "run-1"}}}
	if err := result.Normalize("workspace-a"); err == nil {
		t.Fatal("cross-workspace artifact was accepted")
	}
}

func workflowTestPlan(t *testing.T, workspace, hash string) OperationPlan {
	t.Helper()
	plan := OperationPlan{ID: "plan-1", OperationID: "operation-1", OperationHash: hash, WorkspaceID: workspace, Actor: ActorRef{ID: "actor-1", Kind: "operator", WorkspaceID: workspace}, Steps: []OperationStep{
		{ID: "step-1", Ordinal: 0, Kind: WorkflowStepValidation, Capability: CapabilityRef{WorkspaceID: workspace, Connector: ConnectorRef{WorkspaceID: workspace, Name: "fixture", Version: "1"}, Name: "validate", Version: "1", DefinitionHash: hash}, Target: ResourceRef{WorkspaceID: workspace, System: SystemRef{WorkspaceID: workspace, Type: "fixture", ID: "system", Version: "1"}, Kind: "record", ID: "record-1", Version: "1"}, Effect: EffectClassObservation, Profile: DefaultExecutionProfile(), InputHash: hash},
		{ID: "step-2", Ordinal: 1, Kind: WorkflowStepValidation, Capability: CapabilityRef{WorkspaceID: workspace, Connector: ConnectorRef{WorkspaceID: workspace, Name: "fixture", Version: "1"}, Name: "validate", Version: "1", DefinitionHash: hash}, Target: ResourceRef{WorkspaceID: workspace, System: SystemRef{WorkspaceID: workspace, Type: "fixture", ID: "system", Version: "1"}, Kind: "record", ID: "record-2", Version: "1"}, Effect: EffectClassObservation, Profile: DefaultExecutionProfile(), InputHash: hash},
	}}
	if err := plan.Normalize(); err != nil {
		t.Fatal(err)
	}
	return plan
}
