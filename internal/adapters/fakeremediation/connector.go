// Package fakeremediation exposes the approval-gated fake remediation
// capability as a separately registerable adapter. Keeping it separate makes
// the external-effect boundary visible in discovery and tests.
package fakeremediation

import (
	"context"

	"github.com/omaveda/fornix/internal/adapters/fakeincident"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

// Connector narrows fakeincident to the one approval-gated effect capability.
// The underlying implementation remains deterministic and offline.
type Connector struct{ parent *fakeincident.Connector }

func NewConnector(workspaceID string) (*Connector, error) {
	parent, err := fakeincident.NewConnector(workspaceID)
	if err != nil {
		return nil, err
	}
	return &Connector{parent: parent}, nil
}

func (c *Connector) Definition() contracts.ConnectorRef {
	if c == nil || c.parent == nil {
		return contracts.ConnectorRef{}
	}
	// The capability definition is intentionally still owned by the shared
	// fakeincident contract. This wrapper is used in tests and workflow
	// composition where only the effect capability should be exposed.
	return c.parent.Definition()
}

func (c *Connector) Capabilities() []connector.Capability {
	if c == nil || c.parent == nil {
		return nil
	}
	for _, capability := range c.parent.Capabilities() {
		if capability.Definition().Ref.Name == "incident.remediate" {
			return []connector.Capability{capability}
		}
	}
	return nil
}

func (c *Connector) Health(ctx context.Context) connector.HealthStatus {
	if c == nil || c.parent == nil {
		return connector.HealthStatus{Status: connector.HealthUnavailable, Reason: "fake remediation connector is not configured"}
	}
	return c.parent.Health(ctx)
}
