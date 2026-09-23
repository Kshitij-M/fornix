package server

import (
	"fmt"
	"strings"

	"github.com/omaveda/fornix/internal/adapters/repository"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

// registerRepositoryConnector installs the repository adapter for a durable
// workspace identity. The current HTTP workflows continue to use their
// existing authoritative ingestion/change services; this registry entry is
// the shared capability catalog used by the forthcoming generic operation
// authority. A nil inspector makes discovery safe while preventing an
// unconfigured process from executing repository work through this seam.
func registerRepositoryConnector(registry *connector.Registry, workspaceID string) error {
	if registry == nil {
		return fmt.Errorf("connector registry is nil")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	ref := contracts.ConnectorRef{WorkspaceID: workspaceID, Name: repository.ConnectorName, Version: repository.ConnectorVersion}
	if registry.HasConnector(ref) {
		return nil
	}
	adapter, err := repository.NewConnector(workspaceID, repository.ConnectorVersion, nil)
	if err != nil {
		return err
	}
	return registry.Register(adapter)
}
