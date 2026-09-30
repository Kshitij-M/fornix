package connector_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/omaveda/fornix/internal/adapters/fakeincident"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

func TestConformanceReportIsStableRedactedAndWorkspaceScoped(t *testing.T) {
	const workspace = "workspace-conformance-report"
	adapter, err := fakeincident.NewConnector(workspace)
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	var capability connector.Capability
	for _, candidate := range adapter.Capabilities() {
		if candidate.Definition().Ref.Name == "incident.read" {
			capability = candidate
			break
		}
	}
	if capability == nil {
		t.Fatal("read capability is missing")
	}
	request := conformanceReportRequest(capability.Definition())
	first := connector.RunConformanceReport(context.Background(), registry, request, connector.ConformanceReportOptions{})
	second := connector.RunConformanceReport(context.Background(), registry, request, connector.ConformanceReportOptions{})
	if first.Outcome != "passed" || first.ReportHash == "" || first.ReportHash != second.ReportHash || len(first.Cases) != 1 || first.Cases[0].Name != "suite" {
		t.Fatalf("unstable conformance report: first=%+v second=%+v", first, second)
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"password", "authorization", "prompt", "raw_input"} {
		if strings.Contains(strings.ToLower(string(encoded)), forbidden) {
			t.Fatalf("conformance report contains forbidden field %q: %s", forbidden, encoded)
		}
	}

	crossWorkspace := request
	crossWorkspace.Target.WorkspaceID = workspace + "-other"
	cross := connector.RunConformanceReport(context.Background(), registry, crossWorkspace, connector.ConformanceReportOptions{})
	if cross.Outcome != "failed" || cross.ReportHash == "" {
		t.Fatalf("cross-workspace conformance was not failed closed: %+v", cross)
	}
}

func TestConformanceReportBlocksEffectfulExecutionUnlessExplicitlyEnabled(t *testing.T) {
	const workspace = "workspace-conformance-effect"
	adapter, err := fakeincident.NewConnector(workspace)
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	var capability connector.Capability
	for _, candidate := range adapter.Capabilities() {
		if candidate.Definition().Ref.Name == "incident.remediate" {
			capability = candidate
			break
		}
	}
	request := conformanceReportRequest(capability.Definition())
	report := connector.RunConformanceReport(context.Background(), registry, request, connector.ConformanceReportOptions{})
	if report.Outcome != "blocked" || len(report.Cases) != 1 || report.Cases[0].ErrorCode != "external_effect_opt_in" {
		t.Fatalf("effectful conformance was not blocked: %+v", report)
	}
}

func conformanceReportRequest(definition contracts.CapabilityDefinition) contracts.OperationRequest {
	workspace := definition.WorkspaceID
	return contracts.OperationRequest{
		ID: "conformance-operation", RequestID: "conformance-request", IdempotencyKey: "conformance-idempotency",
		WorkspaceID: workspace, Actor: contracts.ActorRef{ID: "operator", Kind: "service", WorkspaceID: workspace},
		Capability: definition.Ref,
		Target:     contracts.ResourceRef{WorkspaceID: workspace, System: contracts.SystemRef{WorkspaceID: workspace, Type: "monitoring", ID: "monitor", Version: "1"}, Kind: definition.ResourceKinds[0], ID: "incident-1", Version: "1"},
		InputType:  fakeincident.InputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash,
		InputHash: contracts.HashStrings("conformance-input"), Profile: definition.Profile,
	}
}
