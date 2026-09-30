// Package fakedomains provides deterministic, offline capabilities for two
// materially different operational domains. It is a qualification adapter,
// not a live pipeline or ticketing integration: every result is hash-only and
// every write declares at-least-once external semantics.
package fakedomains

import (
	"context"
	"fmt"
	"strings"

	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

const (
	ConnectorName    = "fakedomains"
	ConnectorVersion = "1"
	PipelineInput    = "fornix.data_pipeline.v1"
	SupportInput     = "fornix.customer_support.v1"
	PipelineKind     = "data_pipeline"
	SupportKind      = "customer_case"
)

type spec struct {
	name, inputType, resourceKind string
	effect                        contracts.EffectClass
	approval                      bool
}

// Connector advertises distinct schemas and capabilities for data-pipeline
// and customer-support work while sharing the same typed adapter boundary.
type Connector struct {
	workspace string
	ref       contracts.ConnectorRef
	caps      []connector.Capability
}

func NewConnector(workspaceID string) (*Connector, error) {
	workspaceID = strings.TrimSpace(workspaceID)
	ref := contracts.ConnectorRef{WorkspaceID: workspaceID, Name: ConnectorName, Version: ConnectorVersion}
	if err := ref.Normalize(); err != nil {
		return nil, err
	}
	definitions := []spec{
		{name: "data_pipeline.observe", inputType: PipelineInput, resourceKind: PipelineKind, effect: contracts.EffectClassObservation},
		{name: "data_pipeline.validate", inputType: PipelineInput, resourceKind: PipelineKind, effect: contracts.EffectClassObservation},
		{name: "data_pipeline.publish", inputType: PipelineInput, resourceKind: PipelineKind, effect: contracts.EffectClassApprovalRequiredWrite, approval: true},
		{name: "data_pipeline.verify", inputType: PipelineInput, resourceKind: PipelineKind, effect: contracts.EffectClassObservation},
		{name: "customer_support.case_read", inputType: SupportInput, resourceKind: SupportKind, effect: contracts.EffectClassReadOnly},
		{name: "customer_support.classify", inputType: SupportInput, resourceKind: SupportKind, effect: contracts.EffectClassObservation},
		{name: "customer_support.reply_send", inputType: SupportInput, resourceKind: SupportKind, effect: contracts.EffectClassApprovalRequiredWrite, approval: true},
		{name: "customer_support.reply_verify", inputType: SupportInput, resourceKind: SupportKind, effect: contracts.EffectClassObservation},
	}
	value := &Connector{workspace: workspaceID, ref: ref}
	for _, item := range definitions {
		definition := contracts.CapabilityDefinition{
			WorkspaceID:        workspaceID,
			Ref:                contracts.CapabilityRef{WorkspaceID: workspaceID, Connector: ref, Name: item.name, Version: "1"},
			Description:        "deterministic offline qualification capability",
			InputSchemaVersion: 1, InputSchemaHash: schemaHash(item.inputType, item.name),
			OutputSchemaVersion: 1, OutputSchemaHash: schemaHash(item.name, "output"),
			Effect: item.effect, Profile: profile(item.effect),
			Evidence:         []contracts.EvidenceRequirement{{WorkspaceID: workspaceID, Kind: item.inputType, MinItems: 1, MaxItems: 2, RequireHash: true, RequireProvenance: true}},
			ResourceKinds:    []string{item.resourceKind},
			RetryPolicy:      contracts.CapabilityRetryPolicy{MaxAttempts: 2, BackoffMS: 10, MaxBackoffMS: 100, Jitter: "none", RetryableCodes: []string{"transport", "timeout"}},
			RequiresApproval: item.approval, SupportsCancellation: true, SupportsIdempotency: true, SupportsVerification: true, Enabled: true,
		}
		if err := definition.Normalize(); err != nil {
			return nil, fmt.Errorf("fakedomains capability %s: %w", item.name, err)
		}
		value.caps = append(value.caps, &capability{parent: value, definition: definition})
	}
	return value, nil
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
		return connector.HealthStatus{Status: connector.HealthUnavailable, Reason: "fakedomains connector is not configured"}
	}
	return connector.HealthStatus{Status: connector.HealthReady}
}

type capability struct {
	parent     *Connector
	definition contracts.CapabilityDefinition
}

func (c *capability) Definition() contracts.CapabilityDefinition { return c.definition }

func (c *capability) Validate(request contracts.OperationRequest) error {
	if c == nil || c.parent == nil {
		return fmt.Errorf("fakedomains capability is not configured")
	}
	if err := contracts.ValidateOperationRequest(request, c.definition); err != nil {
		return err
	}
	if request.InputType != inputTypeFor(c.definition) || request.Target.Kind != c.definition.ResourceKinds[0] {
		return fmt.Errorf("fakedomains request does not match capability schema")
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

func (c *capability) DescribeEffect(request contracts.OperationRequest) (contracts.ExternalEffect, error) {
	if err := c.Validate(request); err != nil {
		return contracts.ExternalEffect{}, err
	}
	if !c.definition.RequiresApproval {
		return contracts.ExternalEffect{}, fmt.Errorf("capability %q does not produce an external effect", c.definition.Ref.Name)
	}
	outputHash := contracts.HashStrings("fakedomains-effect", c.definition.Ref.Name, request.Target.StableHash(), request.InputHash)
	effect := contracts.ExternalEffect{ID: "effect-" + outputHash[:32], WorkspaceID: request.WorkspaceID, Boundary: "fakedomains." + strings.ReplaceAll(c.definition.Ref.Name, ".", "-"), Class: c.definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: request.IdempotencyKey, ProviderRequestID: "fake-" + outputHash[:16], ProviderIdempotency: true, VerificationRequired: true, VerificationStatus: contracts.ExternalVerificationPending, CompensationStatus: contracts.ExternalCompensationUnavailable}
	if err := effect.Normalize(); err != nil {
		return contracts.ExternalEffect{}, err
	}
	return effect, nil
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
	outputHash := contracts.HashStrings("fakedomains-output", c.definition.Ref.Name, request.Target.StableHash(), request.InputHash)
	evidence := contracts.OperationEvidenceRef{WorkspaceID: request.WorkspaceID, SourceReference: "fakedomains:" + c.definition.Ref.Name + ":" + request.Target.ID, EvidenceHash: contracts.HashStrings("fakedomains-evidence", outputHash), Role: "observation"}
	result := contracts.OperationResult{ID: request.ID + "-result", OperationID: request.ID, OperationHash: request.StableHash(), RequestID: request.RequestID, WorkspaceID: request.WorkspaceID, Actor: request.Actor, Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: c.definition.OutputSchemaVersion, OutputSchemaHash: c.definition.OutputSchemaHash, OutputHash: outputHash, Evidence: []contracts.OperationEvidenceRef{evidence}, Steps: []contracts.OperationStepResult{{StepID: plan.Steps[0].ID, Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: c.definition.OutputSchemaVersion, OutputSchemaHash: c.definition.OutputSchemaHash, OutputHash: outputHash, Evidence: []contracts.OperationEvidenceRef{evidence}}}}
	if c.definition.RequiresApproval {
		effect, err := c.DescribeEffect(request)
		if err != nil {
			return contracts.OperationResult{}, err
		}
		result.ExternalEffects = []contracts.ExternalEffect{effect}
		result.Steps[0].ExternalEffect = &result.ExternalEffects[0]
	}
	if err := result.Normalize(); err != nil {
		return contracts.OperationResult{}, err
	}
	return result, nil
}

func (c *capability) ExecuteWithAuthority(ctx context.Context, request contracts.OperationRequest, plan contracts.OperationPlan, authority contracts.EffectAuthority) (contracts.OperationResult, error) {
	if err := authority.Normalize(); err != nil {
		return contracts.OperationResult{}, fmt.Errorf("fakedomains authority: %w", err)
	}
	if authority.WorkspaceID != request.WorkspaceID || authority.OperationID != request.ID || authority.EffectID == "" || !c.definition.RequiresApproval {
		return contracts.OperationResult{}, connector.ErrAuthorityExecution
	}
	result, err := c.Execute(ctx, request, plan)
	if err != nil {
		return contracts.OperationResult{}, err
	}
	if len(result.ExternalEffects) != 1 || result.Steps[0].ExternalEffect == nil {
		return contracts.OperationResult{}, connector.ErrAuthorityExecution
	}
	result.ExternalEffects[0].ID = authority.EffectID
	result.Steps[0].ExternalEffect = &result.ExternalEffects[0]
	return result, result.Normalize()
}

// VerifyEffect is a deterministic offline proof adapter. The marker-based
// outcomes let qualification exercise mismatch and uncertain recovery without
// contacting a provider or persisting raw payloads.
func (c *capability) VerifyEffect(_ context.Context, request contracts.EffectVerificationRequest) (contracts.EffectVerificationResult, error) {
	if c == nil || c.parent == nil || !c.definition.RequiresApproval {
		return contracts.EffectVerificationResult{}, connector.ErrAuthorityExecution
	}
	if err := request.Normalize(); err != nil {
		return contracts.EffectVerificationResult{}, err
	}
	providerID := strings.ToLower(request.Effect.ProviderRequestID)
	if strings.Contains(providerID, "uncertain") {
		return contracts.EffectVerificationResult{Status: contracts.EffectVerificationStatusUnknown, ProviderRequestID: request.Effect.ProviderRequestID, FailureCode: "external_uncertain"}, nil
	}
	if strings.Contains(providerID, "mismatch") {
		return contracts.EffectVerificationResult{Status: contracts.EffectVerificationStatusFailed, ProviderRequestID: request.Effect.ProviderRequestID, FailureCode: "result_mismatch"}, nil
	}
	resultHash := contracts.HashStrings("fakedomains-verified", c.definition.Ref.Name, request.Operation.Target.StableHash(), request.Operation.InputHash)
	return contracts.EffectVerificationResult{
		Status:            contracts.EffectVerificationStatusVerified,
		ResultHash:        resultHash,
		VerificationHash:  contracts.HashStrings("fakedomains-proof", request.Effect.ProviderRequestID, resultHash, request.Link.DomainHash),
		ProviderRequestID: request.Effect.ProviderRequestID,
	}, nil
}

func inputTypeFor(definition contracts.CapabilityDefinition) string {
	if strings.HasPrefix(definition.Ref.Name, "customer_support.") {
		return SupportInput
	}
	return PipelineInput
}

func schemaHash(parts ...string) string {
	return contracts.HashStrings(append([]string{"fakedomains-schema"}, parts...)...)
}

func profile(effect contracts.EffectClass) contracts.ExecutionProfile {
	value := contracts.DefaultExecutionProfile()
	value.MaxSteps = 1
	value.MaxConcurrency = 1
	value.MaxInputBytes = 64 << 10
	value.MaxOutputBytes = 64 << 10
	value.MaxOutputTokens = 2048
	value.MaxRetries = 1
	value.AllowExternalEffects = effect != contracts.EffectClassReadOnly && effect != contracts.EffectClassObservation
	return value
}
