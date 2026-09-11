package server

import (
	"testing"

	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

func TestRegisterRepositoryConnectorIsWorkspaceScopedAndIdempotent(t *testing.T) {
	registry := connector.NewRegistry()
	if err := registerRepositoryConnector(registry, "workspace-a"); err != nil {
		t.Fatal(err)
	}
	if err := registerRepositoryConnector(registry, "workspace-a"); err != nil {
		t.Fatal(err)
	}
	if err := registerRepositoryConnector(registry, "workspace-b"); err != nil {
		t.Fatal(err)
	}
	if got := len(registry.ConnectorRefs("workspace-a")); got != 1 {
		t.Fatalf("workspace-a connector count = %d, want 1", got)
	}
	if got := len(registry.ConnectorRefs("workspace-b")); got != 1 {
		t.Fatalf("workspace-b connector count = %d, want 1", got)
	}
	if got := registry.Capabilities("workspace-a"); len(got) != 1 || got[0].WorkspaceID != "workspace-a" {
		t.Fatalf("workspace-a capabilities = %+v", got)
	}
	if registry.HasConnector(contracts.ConnectorRef{WorkspaceID: "workspace-c", Name: "repository", Version: "1"}) {
		t.Fatal("unregistered workspace connector was reported")
	}
}
