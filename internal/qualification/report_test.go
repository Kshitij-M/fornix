package qualification

import (
	"context"
	"testing"

	"github.com/omaveda/fornix/internal/adapters/fakeincident"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

func TestBuilderAdaptsConnectorReportWithoutCrossWorkspaceData(t *testing.T) {
	const workspace = "qualification-workspace"
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
	request := contracts.OperationRequest{
		ID: "qualification-operation", RequestID: "qualification-request", IdempotencyKey: "qualification-idempotency",
		WorkspaceID: workspace, Actor: contracts.ActorRef{ID: "operator", Kind: "service", WorkspaceID: workspace},
		Capability: capability.Definition().Ref,
		Target:     contracts.ResourceRef{WorkspaceID: workspace, System: contracts.SystemRef{WorkspaceID: workspace, Type: "monitoring", ID: "monitor", Version: "1"}, Kind: capability.Definition().ResourceKinds[0], ID: "incident-1", Version: "1"},
		InputType:  fakeincident.InputType, InputSchemaVersion: capability.Definition().InputSchemaVersion, InputSchemaHash: capability.Definition().InputSchemaHash,
		InputHash: contracts.HashStrings("qualification-input"), Profile: capability.Definition().Profile,
	}
	conformance := connector.RunConformanceReport(context.Background(), registry, request, connector.ConformanceReportOptions{})
	if conformance.Outcome != "passed" {
		t.Fatalf("conformance outcome=%+v", conformance)
	}
	builder, err := NewBuilder("qualification-run", workspace, contracts.HashStrings("deployment", "test"))
	if err != nil {
		t.Fatal(err)
	}
	if err := builder.AddConnectorReport(conformance); err != nil {
		t.Fatal(err)
	}
	report, err := builder.Finalize()
	if err != nil {
		t.Fatal(err)
	}
	if report.ReportHash == "" || len(report.Cases) != 1 || report.Cases[0].Category != contracts.QualificationCategoryAdapter {
		t.Fatalf("unexpected qualification report=%+v", report)
	}
	if report.Cases[0].EvidenceHash != conformance.ReportHash {
		t.Fatalf("connector report hash was not preserved: %+v", report.Cases[0])
	}
	cross := conformance
	cross.WorkspaceID = workspace + "-other"
	if err := builder.AddConnectorReport(cross); err == nil {
		t.Fatal("cross-workspace connector report was accepted")
	}
}
