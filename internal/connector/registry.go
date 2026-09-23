// Package connector owns Fornix's explicit, deterministic connector and
// capability catalog. It admits typed adapter requests but does not own
// durable operation state, authorization records, credentials, or external
// exactly-once semantics.
package connector

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrRegistryNil           = errors.New("connector registry is nil")
	ErrConnectorNil          = errors.New("connector is nil")
	ErrConnectorDuplicate    = errors.New("connector is already registered")
	ErrCapabilityDuplicate   = errors.New("capability is already registered")
	ErrConnectorNotFound     = errors.New("connector is not registered")
	ErrCapabilityNotFound    = errors.New("capability is not registered")
	ErrDefinitionConflict    = errors.New("capability definition conflicts with registration")
	ErrConnectorUnavailable  = errors.New("connector is unavailable")
	ErrCapabilityDisabled    = errors.New("capability is disabled")
	ErrApprovalRequired      = errors.New("capability approval is required")
	ErrCredentialUnavailable = errors.New("required credential reference is unavailable")
	ErrUnauthorized          = errors.New("connector operation is unauthorized")
)

const (
	HealthReady       = "ready"
	HealthDegraded    = "degraded"
	HealthUnavailable = "unavailable"
)

// HealthStatus is a bounded runtime observation. It is intentionally not a
// durable health record; callers that need auditability must persist the
// admission decision through the operation authority.
type HealthStatus struct {
	Status string
	Reason string
}

// Capability is the narrow adapter-owned execution seam. Implementations
// validate typed request identity, construct a bounded plan, and optionally
// execute a capability. They must not mutate Fornix authority behind the
// registry or return raw credentials and unbounded payloads.
type Capability interface {
	contracts.OperationAdapter
	Execute(context.Context, contracts.OperationRequest, contracts.OperationPlan) (contracts.OperationResult, error)
}

// Connector advertises immutable capabilities for one workspace-scoped
// adapter version. Registration is explicit and process-local in this slice.
type Connector interface {
	Definition() contracts.ConnectorRef
	Capabilities() []Capability
	Health(context.Context) HealthStatus
}

type connectorEntry struct {
	connector Connector
	ref       contracts.ConnectorRef
}

type capabilityEntry struct {
	connector  connectorEntry
	capability Capability
	definition contracts.CapabilityDefinition
}

// Registry stores explicit connector and capability registrations. Lookups
// are exact and stable; a model or request cannot add an entry at runtime.
type Registry struct {
	mu                   sync.RWMutex
	connectors           map[string]connectorEntry
	capabilities         map[string]capabilityEntry
	capabilityIdentities map[string]capabilityEntry
}

// NewRegistry creates an empty connector registry.
func NewRegistry() *Registry {
	return &Registry{
		connectors:           make(map[string]connectorEntry),
		capabilities:         make(map[string]capabilityEntry),
		capabilityIdentities: make(map[string]capabilityEntry),
	}
}

// Register validates and atomically adds a connector and all of its
// capabilities. Duplicate or partially valid registrations are rejected
// without changing the existing catalog.
func (r *Registry) Register(value Connector) error {
	if r == nil {
		return ErrRegistryNil
	}
	if value == nil {
		return ErrConnectorNil
	}
	ref := value.Definition()
	if err := ref.Normalize(); err != nil {
		return fmt.Errorf("connector definition: %w", err)
	}
	capabilities := value.Capabilities()
	if len(capabilities) == 0 {
		return fmt.Errorf("connector %s has no capabilities", ref.Name)
	}

	entries := make([]capabilityEntry, 0, len(capabilities))
	seen := make(map[string]struct{}, len(capabilities))
	seenIdentities := make(map[string]struct{}, len(capabilities))
	for index, capability := range capabilities {
		if capability == nil {
			return fmt.Errorf("connector capability %d is nil", index)
		}
		definition := capability.Definition()
		if err := definition.Normalize(); err != nil {
			return fmt.Errorf("capability %d definition: %w", index, err)
		}
		if definition.WorkspaceID != ref.WorkspaceID || definition.Ref.Connector != ref {
			return fmt.Errorf("capability %d does not belong to connector %s@%s", index, ref.Name, ref.Version)
		}
		key := capabilityKey(definition.Ref)
		identityKey := capabilityIdentityKey(definition.Ref)
		if _, exists := seen[key]; exists {
			return fmt.Errorf("%w: %s", ErrCapabilityDuplicate, key)
		}
		if _, exists := seenIdentities[identityKey]; exists {
			return fmt.Errorf("%w: %s", ErrCapabilityDuplicate, identityKey)
		}
		seen[key] = struct{}{}
		seenIdentities[identityKey] = struct{}{}
		entries = append(entries, capabilityEntry{
			connector: connectorEntry{connector: value, ref: ref}, capability: capability,
			definition: cloneDefinition(definition),
		})
	}

	connectorKey := connectorKey(ref)
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.connectors[connectorKey]; exists {
		return fmt.Errorf("%w: %s", ErrConnectorDuplicate, connectorKey)
	}
	for _, entry := range entries {
		if _, exists := r.capabilityIdentities[capabilityIdentityKey(entry.definition.Ref)]; exists {
			return fmt.Errorf("%w: %s", ErrCapabilityDuplicate, capabilityIdentityKey(entry.definition.Ref))
		}
	}
	r.connectors[connectorKey] = connectorEntry{connector: value, ref: ref}
	for _, entry := range entries {
		r.capabilities[capabilityKey(entry.definition.Ref)] = entry
		r.capabilityIdentities[capabilityIdentityKey(entry.definition.Ref)] = entry
	}
	return nil
}

// HasConnector reports whether an exact connector identity is registered.
func (r *Registry) HasConnector(ref contracts.ConnectorRef) bool {
	if r == nil || ref.Normalize() != nil {
		return false
	}
	r.mu.RLock()
	_, ok := r.connectors[connectorKey(ref)]
	r.mu.RUnlock()
	return ok
}

// Lookup returns a capability only when its definition hash exactly matches
// the registered immutable definition.
func (r *Registry) Lookup(ref contracts.CapabilityRef) (Capability, bool) {
	if r == nil || ref.Normalize() != nil {
		return nil, false
	}
	r.mu.RLock()
	entry, ok := r.capabilities[capabilityKey(ref)]
	r.mu.RUnlock()
	if !ok || entry.definition.Ref.DefinitionHash != ref.DefinitionHash {
		return nil, false
	}
	return &boundCapability{entry: entry}, true
}

// LookupIdentity resolves a capability for discovery. Execution must use the
// returned definition's hash in a CapabilityRef and call Lookup instead.
func (r *Registry) LookupIdentity(workspaceID, connectorName, connectorVersion, name, version string) (Capability, bool) {
	ref := contracts.CapabilityRef{
		WorkspaceID: workspaceID,
		Connector:   contracts.ConnectorRef{WorkspaceID: workspaceID, Name: connectorName, Version: connectorVersion},
		Name:        name, Version: version,
	}
	if ref.Connector.Normalize() != nil {
		return nil, false
	}
	ref.DefinitionHash = ""
	if ref.NormalizeIdentity() != nil {
		return nil, false
	}
	key := capabilityIdentityKey(ref)
	r.mu.RLock()
	entry, ok := r.capabilityIdentities[key]
	r.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return &boundCapability{entry: entry}, true
}

// Capabilities returns normalized definitions in stable identity order. The
// returned slices are defensive copies and cannot mutate the registry.
func (r *Registry) Capabilities(workspaceID string) []contracts.CapabilityDefinition {
	if r == nil {
		return nil
	}
	workspaceID = strings.TrimSpace(workspaceID)
	r.mu.RLock()
	definitions := make([]contracts.CapabilityDefinition, 0, len(r.capabilities))
	for _, entry := range r.capabilities {
		if entry.definition.WorkspaceID == workspaceID {
			definitions = append(definitions, cloneDefinition(entry.definition))
		}
	}
	r.mu.RUnlock()
	sort.Slice(definitions, func(i, j int) bool {
		return capabilityKey(definitions[i].Ref) < capabilityKey(definitions[j].Ref)
	})
	return definitions
}

// ConnectorRefs returns connector identities in stable order.
func (r *Registry) ConnectorRefs(workspaceID string) []contracts.ConnectorRef {
	if r == nil {
		return nil
	}
	workspaceID = strings.TrimSpace(workspaceID)
	r.mu.RLock()
	refs := make([]contracts.ConnectorRef, 0, len(r.connectors))
	for _, entry := range r.connectors {
		if entry.ref.WorkspaceID == workspaceID {
			refs = append(refs, entry.ref)
		}
	}
	r.mu.RUnlock()
	sort.Slice(refs, func(i, j int) bool { return connectorKey(refs[i]) < connectorKey(refs[j]) })
	return refs
}

// Health reports the current connector health. Unknown connectors are
// unavailable rather than implicitly healthy.
func (r *Registry) Health(ctx context.Context, ref contracts.ConnectorRef) HealthStatus {
	if r == nil || ref.Normalize() != nil {
		return HealthStatus{Status: HealthUnavailable, Reason: "connector is not registered"}
	}
	r.mu.RLock()
	entry, ok := r.connectors[connectorKey(ref)]
	r.mu.RUnlock()
	if !ok {
		return HealthStatus{Status: HealthUnavailable, Reason: "connector is not registered"}
	}
	status := entry.connector.Health(ctx)
	status.Status = strings.ToLower(strings.TrimSpace(status.Status))
	if status.Status != HealthReady && status.Status != HealthDegraded && status.Status != HealthUnavailable {
		return HealthStatus{Status: HealthUnavailable, Reason: "connector returned unknown health"}
	}
	return status
}

type boundCapability struct {
	entry capabilityEntry
}

func (c *boundCapability) Definition() contracts.CapabilityDefinition {
	if c == nil {
		return contracts.CapabilityDefinition{}
	}
	return cloneDefinition(c.entry.definition)
}

func (c *boundCapability) Validate(request contracts.OperationRequest) error {
	if c == nil {
		return ErrCapabilityNotFound
	}
	if err := c.validateRegisteredDefinition(); err != nil {
		return err
	}
	if err := contracts.ValidateOperationRequest(request, c.entry.definition); err != nil {
		return err
	}
	return c.entry.capability.Validate(request)
}

func (c *boundCapability) Plan(request contracts.OperationRequest) (contracts.OperationPlan, error) {
	if err := c.Validate(request); err != nil {
		return contracts.OperationPlan{}, err
	}
	plan, err := c.entry.capability.Plan(request)
	if err != nil {
		return contracts.OperationPlan{}, err
	}
	if err := validatePlan(request, plan); err != nil {
		return contracts.OperationPlan{}, err
	}
	if err := c.validateRegisteredDefinition(); err != nil {
		return contracts.OperationPlan{}, err
	}
	return plan, nil
}

func (c *boundCapability) Execute(ctx context.Context, request contracts.OperationRequest, plan contracts.OperationPlan) (contracts.OperationResult, error) {
	if c == nil {
		return contracts.OperationResult{}, ErrCapabilityNotFound
	}
	if err := c.Validate(request); err != nil {
		return contracts.OperationResult{}, err
	}
	if err := validatePlan(request, plan); err != nil {
		return contracts.OperationResult{}, err
	}
	result, err := c.entry.capability.Execute(ctx, request, plan)
	if definitionErr := c.validateRegisteredDefinition(); definitionErr != nil {
		return contracts.OperationResult{}, definitionErr
	}
	if err == nil {
		if schemaErr := validateResultSchema(result, c.entry.definition); schemaErr != nil {
			return contracts.OperationResult{}, schemaErr
		}
	}
	return result, err
}

func (c *boundCapability) validateRegisteredDefinition() error {
	if c == nil || c.entry.capability == nil {
		return ErrCapabilityNotFound
	}
	live := c.entry.capability.Definition()
	if err := live.Normalize(); err != nil {
		return fmt.Errorf("%w: registered capability became invalid", ErrDefinitionConflict)
	}
	if live.Ref != c.entry.definition.Ref || live.StableHash() != c.entry.definition.StableHash() {
		return ErrDefinitionConflict
	}
	return nil
}

func connectorKey(ref contracts.ConnectorRef) string {
	return ref.WorkspaceID + "\x00" + ref.Name + "\x00" + ref.Version
}

func capabilityIdentityKey(ref contracts.CapabilityRef) string {
	return ref.WorkspaceID + "\x00" + ref.Connector.Name + "\x00" + ref.Connector.Version + "\x00" + ref.Name + "\x00" + ref.Version
}

func capabilityKey(ref contracts.CapabilityRef) string {
	return capabilityIdentityKey(ref) + "\x00" + ref.DefinitionHash
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

func validatePlan(request contracts.OperationRequest, plan contracts.OperationPlan) error {
	if err := plan.Normalize(); err != nil {
		return fmt.Errorf("operation plan: %w", err)
	}
	if plan.WorkspaceID != request.WorkspaceID || plan.OperationID != request.ID || plan.OperationHash != request.StableHash() || plan.Actor.ID != request.Actor.ID {
		return fmt.Errorf("operation plan is not bound to the admitted request")
	}
	if len(plan.Steps) > request.Profile.MaxSteps {
		return fmt.Errorf("operation plan exceeds request step budget")
	}
	return nil
}

func validateResultSchema(result contracts.OperationResult, definition contracts.CapabilityDefinition) error {
	if result.Status != contracts.OperationStatusSucceeded {
		return nil
	}
	if result.OutputSchemaVersion != definition.OutputSchemaVersion || result.OutputSchemaHash != definition.OutputSchemaHash {
		return fmt.Errorf("operation result output schema does not match capability definition")
	}
	for index, step := range result.Steps {
		if step.Status == contracts.OperationStatusSucceeded && (step.OutputSchemaVersion != definition.OutputSchemaVersion || step.OutputSchemaHash != definition.OutputSchemaHash) {
			return fmt.Errorf("operation result step %d output schema does not match capability definition", index)
		}
	}
	return nil
}
