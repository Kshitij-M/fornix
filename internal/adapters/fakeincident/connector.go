// Package fakeincident provides deterministic incident and remediation
// capabilities for offline qualification. It is intentionally not a live
// monitoring or deployment integration; production adapters must add their
// own ingress verification, credentials, egress, and reconciliation.
package fakeincident

import (
	"context"
	"fmt"
	"strings"

	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

const (
	ConnectorName    = "fakeincident"
	ConnectorVersion = "1"
	ResourceKind     = "incident"
	RunbookKind      = "runbook"
	InputType        = "incident.workflow.step"
)

// Connector is an explicit in-memory adapter. It returns hashes and bounded
// evidence identities only; no external system is contacted.
type Connector struct {
	workspace string
	ref       contracts.ConnectorRef
	caps      []connector.Capability
}

// NewConnector creates the fake monitoring/remediation adapter for one
// workspace. Registration remains explicit and process-local.
func NewConnector(workspaceID string) (*Connector, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	ref := contracts.ConnectorRef{WorkspaceID: workspaceID, Name: ConnectorName, Version: ConnectorVersion}
	if err := ref.Normalize(); err != nil {
		return nil, err
	}
	definitions := []struct {
		name, kind string
		effect     contracts.EffectClass
		approval   bool
	}{
		{name: "incident.read", kind: ResourceKind, effect: contracts.EffectClassReadOnly},
		{name: "runbook.read", kind: RunbookKind, effect: contracts.EffectClassReadOnly},
		{name: "incident.verify", kind: ResourceKind, effect: contracts.EffectClassObservation},
		{name: "incident.remediate", kind: ResourceKind, effect: contracts.EffectClassApprovalRequiredWrite, approval: true},
	}
	c := &Connector{workspace: workspaceID, ref: ref}
	for _, spec := range definitions {
		definition := contracts.CapabilityDefinition{
			WorkspaceID:        workspaceID,
			Ref:                contracts.CapabilityRef{WorkspaceID: workspaceID, Connector: ref, Name: spec.name, Version: "1"},
			Description:        "deterministic fake incident workflow capability",
			InputSchemaVersion: 1, InputSchemaHash: schemaHash(InputType),
			OutputSchemaVersion: 1, OutputSchemaHash: schemaHash(spec.name + ".output.v1"),
			Effect: spec.effect, Profile: fakeProfile(spec.effect),
			Evidence:         []contracts.EvidenceRequirement{{WorkspaceID: workspaceID, Kind: "incident_observation", MinItems: 1, MaxItems: 2, RequireHash: true, RequireProvenance: true}},
			ResourceKinds:    []string{spec.kind},
			RetryPolicy:      contracts.CapabilityRetryPolicy{MaxAttempts: 2, BackoffMS: 10, MaxBackoffMS: 100, Jitter: "none", RetryableCodes: []string{"transport", "timeout"}},
			RequiresApproval: spec.approval, SupportsCancellation: true, SupportsIdempotency: true, SupportsVerification: true, Enabled: true,
		}
		if err := definition.Normalize(); err != nil {
			return nil, fmt.Errorf("fakeincident capability %s: %w", spec.name, err)
		}
		c.caps = append(c.caps, &capability{parent: c, definition: definition})
	}
	return c, nil
}

func (c *Connector) Definition() contracts.ConnectorRef {
	if c == nil {
		return contracts.ConnectorRef{}
	}
	return c.ref
}

func (c *Connector) Capabilities() []connector.Capability {
	if c == nil {
		return nil
	}
	return append([]connector.Capability(nil), c.caps...)
}

func (c *Connector) Health(context.Context) connector.HealthStatus {
	if c == nil || c.workspace == "" {
		return connector.HealthStatus{Status: connector.HealthUnavailable, Reason: "fake incident connector is not configured"}
	}
	return connector.HealthStatus{Status: connector.HealthReady}
}

type capability struct {
	parent     *Connector
	definition contracts.CapabilityDefinition
}

func (c *capability) Definition() contracts.CapabilityDefinition {
	if c == nil {
		return contracts.CapabilityDefinition{}
	}
	return c.definition
}

func (c *capability) Validate(request contracts.OperationRequest) error {
	if c == nil || c.parent == nil {
		return fmt.Errorf("fake incident capability is not configured")
	}
	if err := contracts.ValidateOperationRequest(request, c.definition); err != nil {
		return err
	}
	if request.InputType != InputType || request.Target.Kind != c.definition.ResourceKinds[0] {
		return fmt.Errorf("fake incident request does not match capability")
	}
	return nil
}

func (c *capability) Plan(request contracts.OperationRequest) (contracts.OperationPlan, error) {
	if err := c.Validate(request); err != nil {
		return contracts.OperationPlan{}, err
	}
	plan := contracts.OperationPlan{ID: request.ID + "-plan", OperationID: request.ID, OperationHash: request.StableHash(), WorkspaceID: request.WorkspaceID, Actor: request.Actor, Steps: []contracts.OperationStep{{ID: request.ID + "-step", Ordinal: 0, Kind: request.Capability.Name, Capability: request.Capability, Target: request.Target, Effect: c.definition.Effect, Profile: request.Profile, Evidence: c.definition.Evidence, InputHash: request.InputHash}}}
	if err := plan.Normalize(); err != nil {
		return contracts.OperationPlan{}, err
	}
	return plan, nil
}

func (c *capability) Execute(ctx context.Context, request contracts.OperationRequest, plan contracts.OperationPlan) (contracts.OperationResult, error) {
	if err := c.Validate(request); err != nil {
		return contracts.OperationResult{}, err
	}
	if err := plan.Normalize(); err != nil {
		return contracts.OperationResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return contracts.OperationResult{}, err
	}
	outputHash := contracts.HashStrings(c.definition.Ref.Name, request.Target.StableHash(), request.InputHash)
	evidence := contracts.OperationEvidenceRef{WorkspaceID: request.WorkspaceID, SourceReference: "fakeincident:" + c.definition.Ref.Name + ":" + request.Target.ID, EvidenceHash: contracts.HashStrings("fakeincident-evidence", outputHash), Role: "observation"}
	result := contracts.OperationResult{ID: request.ID + "-result", OperationID: request.ID, OperationHash: request.StableHash(), RequestID: request.RequestID, WorkspaceID: request.WorkspaceID, Actor: request.Actor, Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: c.definition.OutputSchemaVersion, OutputSchemaHash: c.definition.OutputSchemaHash, OutputHash: outputHash, Evidence: []contracts.OperationEvidenceRef{evidence}, Steps: []contracts.OperationStepResult{{StepID: plan.Steps[0].ID, Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: c.definition.OutputSchemaVersion, OutputSchemaHash: c.definition.OutputSchemaHash, OutputHash: outputHash, Evidence: []contracts.OperationEvidenceRef{evidence}}}}
	if c.definition.RequiresApproval {
		result.ExternalEffects = []contracts.ExternalEffect{{WorkspaceID: request.WorkspaceID, Boundary: "fakeincident.remediation", Class: contracts.EffectClassApprovalRequiredWrite, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: request.IdempotencyKey, ProviderRequestID: "fake-" + outputHash[:16], ProviderIdempotency: true, VerificationRequired: true, VerificationStatus: contracts.ExternalVerificationPending, CompensationStatus: contracts.ExternalCompensationUnavailable}}
		result.Steps[0].ExternalEffect = &result.ExternalEffects[0]
	}
	if err := result.Normalize(); err != nil {
		return contracts.OperationResult{}, err
	}
	return result, nil
}

func fakeProfile(effect contracts.EffectClass) contracts.ExecutionProfile {
	p := contracts.DefaultExecutionProfile()
	p.MaxSteps = 1
	p.MaxConcurrency = 1
	p.MaxInputBytes = 64 << 10
	p.MaxOutputBytes = 64 << 10
	p.MaxOutputTokens = 2048
	p.MaxCostUSD = 0
	p.MaxRetries = 1
	p.AllowExternalEffects = effect != contracts.EffectClassReadOnly && effect != contracts.EffectClassObservation
	return p
}

func schemaHash(value string) string { return contracts.HashStrings("fakeincident-schema", value) }
