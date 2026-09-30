package connector_test

import (
	"testing"

	"github.com/omaveda/fornix/internal/adapters/fakeincident"
	"github.com/omaveda/fornix/internal/connector"
)

func TestAuthorityConformanceCoversAllRegisteredWorkspaces(t *testing.T) {
	firstAdapter, err := fakeincident.NewConnector("workspace-authority-one")
	if err != nil {
		t.Fatal(err)
	}
	secondAdapter, err := fakeincident.NewConnector("workspace-authority-two")
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(firstAdapter); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(secondAdapter); err != nil {
		t.Fatal(err)
	}
	reports := registry.AuthorityConformanceAll(true)
	if len(reports) != 2 || reports[0].WorkspaceID != "workspace-authority-one" || reports[1].WorkspaceID != "workspace-authority-two" {
		t.Fatalf("workspace reports=%+v", reports)
	}
	for _, report := range reports {
		if !report.Ready || report.ReportHash == "" {
			t.Fatalf("workspace report is not ready: %+v", report)
		}
	}
	if err := registry.ValidateEffectAuthorityConformanceAll(true); err != nil {
		t.Fatal(err)
	}
}
