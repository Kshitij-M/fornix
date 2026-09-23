// Package repository adapts bounded, read-only repository inspection to the
// domain-neutral connector contract. It returns hashes and evidence links;
// repository bytes remain owned by the existing ingestion/evidence plane.
package repository

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

const (
	ConnectorName       = "repository"
	ConnectorVersion    = "1"
	CapabilityName      = "inspect"
	CapabilityVersion   = "1"
	CapabilityInputType = "repository.inspect"
	ResourceKind        = "repository"
)

// Inspection is the bounded output accepted from the repository-specific
// inspection implementation. It contains no source bytes or arbitrary
// diagnostic text.
type Inspection struct {
	OutputHash string
	ReportHash string
	Evidence   []contracts.OperationEvidenceRef
}

// Inspector is supplied by a repository runtime that already knows how to
// read a configured mount safely. The registry never discovers paths or
// invokes a shell on its own.
type Inspector func(context.Context, contracts.OperationRequest) (Inspection, error)

// Connector advertises the repository inspection capability for one
// workspace. A nil inspector is intentionally reported as unavailable,
// allowing discovery without permitting execution.
type Connector struct {
	ref        contracts.ConnectorRef
	inspector  Inspector
	definition contracts.CapabilityDefinition
}

// NewConnector creates a workspace-scoped repository connector. The caller
// must provide a bounded inspector before execution can be admitted.
func NewConnector(workspaceID, version string, inspector Inspector) (*Connector, error) {
	ref := contracts.ConnectorRef{WorkspaceID: strings.TrimSpace(workspaceID), Name: ConnectorName, Version: strings.TrimSpace(version)}
	if err := ref.Normalize(); err != nil {
		return nil, err
	}
	definition := contracts.CapabilityDefinition{
		WorkspaceID: workspaceID,
		Ref: contracts.CapabilityRef{
			WorkspaceID: workspaceID, Connector: ref, Name: CapabilityName, Version: CapabilityVersion,
		},
		Description:          "read a bounded repository state through a configured adapter",
		InputSchemaVersion:   1,
		InputSchemaHash:      schemaHash("repository.inspect.input.v1"),
		OutputSchemaVersion:  1,
		OutputSchemaHash:     schemaHash("repository.inspect.output.v1"),
		Effect:               contracts.EffectClassReadOnly,
		Profile:              contracts.DefaultExecutionProfile(),
		Evidence:             []contracts.EvidenceRequirement{{WorkspaceID: workspaceID, Kind: "repository_snapshot", MinItems: 1, RequireHash: true, RequireProvenance: true}},
		ResourceKinds:        []string{ResourceKind},
		RetryPolicy:          contracts.CapabilityRetryPolicy{MaxAttempts: 1, BackoffMS: 1, MaxBackoffMS: 1, Jitter: "none"},
		SupportsCancellation: true,
		SupportsIdempotency:  true,
		SupportsVerification: true,
		Enabled:              true,
	}
	if err := definition.Normalize(); err != nil {
		return nil, fmt.Errorf("repository capability definition: %w", err)
	}
	return &Connector{ref: ref, inspector: inspector, definition: definition}, nil
}

// Definition returns the immutable connector identity.
func (c *Connector) Definition() contracts.ConnectorRef {
	if c == nil {
		return contracts.ConnectorRef{}
	}
	return c.ref
}

// Capabilities returns the explicitly registered repository capability.
func (c *Connector) Capabilities() []connector.Capability {
	if c == nil {
		return nil
	}
	return []connector.Capability{&inspectionCapability{connector: c, definition: cloneDefinition(c.definition)}}
}

// Health reports unavailable until a safe inspector is configured. No health
// check performs filesystem access or accepts a caller-provided path.
func (c *Connector) Health(ctx context.Context) connector.HealthStatus {
	if err := ctx.Err(); err != nil {
		return connector.HealthStatus{Status: connector.HealthUnavailable, Reason: err.Error()}
	}
	if c == nil || c.inspector == nil {
		return connector.HealthStatus{Status: connector.HealthUnavailable, Reason: "repository inspector is not configured"}
	}
	return connector.HealthStatus{Status: connector.HealthReady}
}

type inspectionCapability struct {
	connector  *Connector
	definition contracts.CapabilityDefinition
}

func (c *inspectionCapability) Definition() contracts.CapabilityDefinition {
	if c == nil {
		return contracts.CapabilityDefinition{}
	}
	return cloneDefinition(c.definition)
}

func (c *inspectionCapability) Validate(request contracts.OperationRequest) error {
	if c == nil || c.connector == nil {
		return fmt.Errorf("repository capability is not configured")
	}
	if err := contracts.ValidateOperationRequest(request, c.definition); err != nil {
		return err
	}
	if request.InputType != CapabilityInputType || request.InputSchemaVersion != c.definition.InputSchemaVersion {
		return fmt.Errorf("repository inspection input schema is incompatible")
	}
	if request.Target.System.Type != "repository" {
		return fmt.Errorf("repository inspection requires a repository system")
	}
	return nil
}

func (c *inspectionCapability) Plan(request contracts.OperationRequest) (contracts.OperationPlan, error) {
	if err := c.Validate(request); err != nil {
		return contracts.OperationPlan{}, err
	}
	plan := contracts.OperationPlan{
		ID:            request.ID + "-plan",
		OperationID:   request.ID,
		OperationHash: request.StableHash(),
		WorkspaceID:   request.WorkspaceID,
		Actor:         request.Actor,
		Steps: []contracts.OperationStep{{
			ID:      request.ID + "-inspect",
			Ordinal: 0, Kind: CapabilityInputType, Capability: request.Capability,
			Target: request.Target, Effect: c.definition.Effect, Profile: request.Profile,
			Evidence: c.definition.Evidence, InputHash: request.InputHash,
		}},
	}
	if err := plan.Normalize(); err != nil {
		return contracts.OperationPlan{}, err
	}
	return plan, nil
}

func (c *inspectionCapability) Execute(ctx context.Context, request contracts.OperationRequest, plan contracts.OperationPlan) (contracts.OperationResult, error) {
	if err := c.Validate(request); err != nil {
		return contracts.OperationResult{}, err
	}
	if err := plan.Normalize(); err != nil {
		return contracts.OperationResult{}, err
	}
	if c.connector.inspector == nil {
		return contracts.OperationResult{}, &connector.FailureError{Code: "unavailable", Retryable: false}
	}
	inspection, err := c.connector.inspector(ctx, request)
	if err != nil {
		return contracts.OperationResult{}, err
	}
	if err := normalizeInspection(&inspection, request.WorkspaceID); err != nil {
		return contracts.OperationResult{}, err
	}
	result := contracts.OperationResult{
		ID: request.ID + "-result", OperationID: request.ID, OperationHash: request.StableHash(),
		RequestID: request.RequestID, WorkspaceID: request.WorkspaceID, Actor: request.Actor,
		Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: c.definition.OutputSchemaVersion, OutputSchemaHash: c.definition.OutputSchemaHash,
		OutputHash: inspection.OutputHash, ReportHash: inspection.ReportHash,
		Evidence: inspection.Evidence,
		Steps:    []contracts.OperationStepResult{{StepID: plan.Steps[0].ID, Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: c.definition.OutputSchemaVersion, OutputSchemaHash: c.definition.OutputSchemaHash, OutputHash: inspection.OutputHash, Evidence: inspection.Evidence}},
	}
	if err := result.Normalize(); err != nil {
		return contracts.OperationResult{}, err
	}
	return result, nil
}

func normalizeInspection(value *Inspection, workspaceID string) error {
	if value == nil {
		return fmt.Errorf("repository inspection is nil")
	}
	if !isSHA256(value.OutputHash) {
		return fmt.Errorf("repository inspection output_hash must be a sha256")
	}
	if value.ReportHash != "" && !isSHA256(value.ReportHash) {
		return fmt.Errorf("repository inspection report_hash must be a sha256")
	}
	if len(value.Evidence) == 0 {
		return fmt.Errorf("repository inspection must return evidence")
	}
	for i := range value.Evidence {
		if err := value.Evidence[i].Normalize(); err != nil {
			return fmt.Errorf("repository inspection evidence[%d]: %w", i, err)
		}
		if value.Evidence[i].WorkspaceID != workspaceID {
			return fmt.Errorf("repository inspection evidence crosses workspace boundary")
		}
	}
	return nil
}

// HashBytes is the canonical content hash helper for repository adapter
// implementations. It returns identity only; callers remain responsible for
// preserving raw bytes in the existing artifact/evidence authorities.
func HashBytes(value []byte) string {
	digest := sha256.Sum256(value)
	return hex.EncodeToString(digest[:])
}

func schemaHash(value string) string { return HashBytes([]byte(value)) }

func isSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

func cloneDefinition(value contracts.CapabilityDefinition) contracts.CapabilityDefinition {
	value.Evidence = append([]contracts.EvidenceRequirement(nil), value.Evidence...)
	for i := range value.Evidence {
		value.Evidence[i].RequiredFields = append([]string(nil), value.Evidence[i].RequiredFields...)
	}
	value.ResourceKinds = append([]string(nil), value.ResourceKinds...)
	value.RequiredCredentialRefs = append([]string(nil), value.RequiredCredentialRefs...)
	value.RetryPolicy.RetryableCodes = append([]string(nil), value.RetryPolicy.RetryableCodes...)
	return value
}
