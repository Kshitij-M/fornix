package reference

import (
	"context"
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func scenarioRequest(kind string, effectful bool) contracts.ReferenceWorkflowRequest {
	workspace := "workspace-a"
	return contracts.ReferenceWorkflowRequest{WorkspaceID: workspace, Actor: contracts.ActorRef{ID: "actor-1", Kind: "operator", WorkspaceID: workspace}, Kind: kind, Target: contracts.ResourceRef{WorkspaceID: workspace, System: contracts.SystemRef{WorkspaceID: workspace, Type: kind, ID: "system-1", Version: "1"}, Kind: "resource", ID: "resource-1", Version: "1"}, IntentHash: contracts.HashStrings("intent", kind), Effectful: effectful, RequiresApproval: effectful}
}

func TestAllReferenceScenariosBuildGenericPlans(t *testing.T) {
	for _, kind := range []string{contracts.ReferenceWorkflowIncidentResponse, contracts.ReferenceWorkflowDataPipeline, contracts.ReferenceWorkflowCustomerSupport, contracts.ReferenceWorkflowInfrastructureMaintenance} {
		plan, err := Build(scenarioRequest(kind, false))
		if err != nil {
			t.Fatalf("%s: %v", kind, err)
		}
		if len(plan.Plan.Steps) != 3 || plan.PlanHash == "" || plan.Operation.Capability.Connector.Name != "fornix-reference" {
			t.Fatalf("%s: unexpected plan %+v", kind, plan)
		}
	}
}

func TestReferenceReplayIsDeterministicAndDoesNotExecuteEffects(t *testing.T) {
	plan, err := Build(scenarioRequest(contracts.ReferenceWorkflowInfrastructureMaintenance, true))
	if err != nil {
		t.Fatal(err)
	}
	first, err := Replay(context.Background(), plan, "")
	if err != nil {
		t.Fatal(err)
	}
	second, err := Replay(context.Background(), plan, "")
	if err != nil {
		t.Fatal(err)
	}
	if first.ReplayHash == "" || first.ReplayHash != second.ReplayHash || first.TerminalStatus != contracts.OperationStatusRecoveryRequired || first.ExternalEffectState != contracts.ReferenceWorkflowEffectUnresolved {
		t.Fatalf("unresolved replay was not deterministic/fail-closed: first=%+v second=%+v", first, second)
	}
	verified, err := Replay(context.Background(), plan, contracts.HashStrings("recorded-effect"))
	if err != nil {
		t.Fatal(err)
	}
	if verified.TerminalStatus != contracts.OperationStatusSucceeded || verified.ExternalEffectState != contracts.ReferenceWorkflowEffectVerified {
		t.Fatalf("recorded effect did not resolve replay: %+v", verified)
	}
}

func TestReferenceReplayReadOnlyScenarioHasNoExternalEffect(t *testing.T) {
	plan, err := Build(scenarioRequest(contracts.ReferenceWorkflowCustomerSupport, false))
	if err != nil {
		t.Fatal(err)
	}
	trace, err := Replay(context.Background(), plan, "")
	if err != nil {
		t.Fatal(err)
	}
	if trace.TerminalStatus != contracts.OperationStatusSucceeded || trace.ExternalEffectState != contracts.ReferenceWorkflowEffectNotApplicable {
		t.Fatalf("read-only reference scenario unexpectedly requires effect recovery: %+v", trace)
	}
}

func TestReferenceReplayRejectsTamperedPlanHash(t *testing.T) {
	plan, err := Build(scenarioRequest(contracts.ReferenceWorkflowDataPipeline, false))
	if err != nil {
		t.Fatal(err)
	}
	plan.PlanHash = contracts.HashStrings("tampered-plan")
	if _, err := Replay(context.Background(), plan, ""); err == nil {
		t.Fatal("tampered plan hash was accepted")
	}
}
