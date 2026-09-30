package tool

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/qualification"
)

// SandboxProvider executes a tool inside exactly one named backend and reports
// its enforced and missing controls. It must never substitute another backend.
type SandboxProvider interface {
	ProcessExecutor
	Backend() contracts.SandboxBackend
	Capabilities() contracts.SandboxBackendCapabilities
}

// AttemptAwareSandboxProvider is mandatory for runtime-backed backends. It
// receives the exact durable effect identity before creating or reusing a
// runtime object, and must be able to reconcile that same attempt after a
// control-plane crash. LocalExecutor remains a separate trusted-host tier.
type AttemptAwareSandboxProvider interface {
	SandboxProvider
	RunAttempt(ctx context.Context, def contracts.ToolDefinition, req contracts.ToolRequest, identity contracts.SandboxExecutionIdentity) (contracts.ToolResult, error)
	ReconcileAttempt(ctx context.Context, identity contracts.SandboxExecutionIdentity) (contracts.SandboxAttemptObservation, error)
}

// QualifiedSandboxProvider exposes the exact runtime fingerprint and signed
// conformance proof required before a non-local backend can be registered.
// Identity hashes must be recomputed from the active provider/runtime
// configuration whenever this method is called. Implementations must also
// revalidate the same qualification atomically immediately before runtime
// creation in RunAttempt; a registry preflight is not runtime attestation.
type QualifiedSandboxProvider interface {
	SandboxProvider
	RuntimeIdentity() contracts.SandboxRuntimeIdentity
	QualificationProof() contracts.SandboxQualificationProof
}

// SandboxRegistry owns the process-local provider catalog. PostgreSQL remains
// authoritative for tool admission, run identity, approvals, and fencing.
type SandboxRegistry struct {
	mu             sync.RWMutex
	providers      map[contracts.SandboxBackend]SandboxProvider
	qualifications map[contracts.SandboxBackend]contracts.SandboxQualificationProof
	trust          *qualification.SandboxTrust
}

// NewSandboxRegistry constructs an empty registry. Providers must be added
// explicitly so a backend name cannot be mistaken for an installed runtime.
func NewSandboxRegistry() *SandboxRegistry {
	return &SandboxRegistry{
		providers:      make(map[contracts.SandboxBackend]SandboxProvider),
		qualifications: make(map[contracts.SandboxBackend]contracts.SandboxQualificationProof),
	}
}

// NewSandboxRegistryWithQualificationTrust enables non-local provider
// registration only for the deployment target signed by the configured key.
// Local-process execution remains a separate, explicitly limited tier.
func NewSandboxRegistryWithQualificationTrust(trust qualification.SandboxTrust) (*SandboxRegistry, error) {
	if err := qualification.ValidateSandboxTrust(trust); err != nil {
		return nil, err
	}
	registry := NewSandboxRegistry()
	trust.PublicKey = append([]byte(nil), trust.PublicKey...)
	registry.trust = &trust
	return registry, nil
}

// NewDefaultSandboxRegistry registers only the bounded local-process provider.
// OCI, gVisor, and microVM execution stay unavailable until separately
// implemented and qualified.
func NewDefaultSandboxRegistry() *SandboxRegistry {
	registry := NewSandboxRegistry()
	registry.providers[contracts.SandboxBackendLocalProcess] = LocalExecutor{}
	return registry
}

// Register adds one provider and rejects duplicate or inconsistent backend
// declarations.
func (r *SandboxRegistry) Register(provider SandboxProvider) error {
	if r == nil || provider == nil {
		return fmt.Errorf("sandbox registry and provider are required")
	}
	backend := provider.Backend()
	capabilities := provider.Capabilities()
	if capabilities.Backend != backend {
		return fmt.Errorf("sandbox provider backend declaration is inconsistent")
	}
	if backend != contracts.SandboxBackendLocalProcess {
		if _, ok := provider.(AttemptAwareSandboxProvider); !ok {
			return fmt.Errorf("non-local sandbox provider must support fenced attempt execution and reconciliation")
		}
		if !containsSandboxCapability(capabilities.Enforced, contracts.SandboxCapabilityDurableAttemptIdentity) || !containsSandboxCapability(capabilities.Enforced, contracts.SandboxCapabilityCrashReconciliation) {
			return fmt.Errorf("non-local sandbox provider must declare durable attempt identity and crash reconciliation")
		}
	}
	if err := capabilities.Normalize(); err != nil {
		return fmt.Errorf("invalid sandbox provider capabilities: %w", err)
	}
	var proof contracts.SandboxQualificationProof
	if backend != contracts.SandboxBackendLocalProcess {
		if r.trust == nil {
			return fmt.Errorf("non-local sandbox provider requires deployment qualification trust")
		}
		qualified, ok := provider.(QualifiedSandboxProvider)
		if !ok {
			return fmt.Errorf("non-local sandbox provider must provide signed qualification evidence and runtime identity")
		}
		proof = qualified.QualificationProof()
		if err := qualification.VerifySandboxProof(proof, qualified.RuntimeIdentity(), capabilities, *r.trust); err != nil {
			return fmt.Errorf("non-local sandbox provider qualification failed: %w", err)
		}
		var err error
		proof, err = cloneSandboxQualificationProof(proof)
		if err != nil {
			return fmt.Errorf("copy sandbox qualification proof: %w", err)
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.providers[backend]; exists {
		return fmt.Errorf("sandbox backend %q is already registered", backend)
	}
	r.providers[backend] = provider
	if backend != contracts.SandboxBackendLocalProcess {
		r.qualifications[backend] = proof
	}
	return nil
}

// Resolve returns only the exact requested backend after checking every
// required capability. It never silently falls back to local execution.
func (r *SandboxRegistry) Resolve(profile contracts.SandboxProfile, workspaceIDs ...string) (SandboxProvider, error) {
	if err := profile.Normalize(); err != nil {
		return nil, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "sandbox profile is invalid")
	}
	if r == nil {
		return nil, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "requested sandbox backend is unavailable")
	}
	r.mu.RLock()
	provider, ok := r.providers[contracts.SandboxBackend(profile.Backend)]
	proof, hasProof := r.qualifications[contracts.SandboxBackend(profile.Backend)]
	trust := r.trust
	r.mu.RUnlock()
	if !ok {
		return nil, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "requested sandbox backend is unavailable")
	}
	capabilities := provider.Capabilities()
	if capabilities.Backend != provider.Backend() || capabilities.Normalize() != nil || !capabilities.Available {
		return nil, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "requested sandbox backend is unavailable")
	}
	if provider.Backend() != contracts.SandboxBackendLocalProcess {
		if _, ok := provider.(AttemptAwareSandboxProvider); !ok || !containsSandboxCapability(capabilities.Enforced, contracts.SandboxCapabilityDurableAttemptIdentity) || !containsSandboxCapability(capabilities.Enforced, contracts.SandboxCapabilityCrashReconciliation) {
			return nil, sandboxFailure(contracts.ToolFailureSandboxCapability, "non-local sandbox backend lacks durable attempt reconciliation")
		}
		qualified, ok := provider.(QualifiedSandboxProvider)
		if !ok || !hasProof || trust == nil || qualification.VerifySandboxProof(proof, qualified.RuntimeIdentity(), capabilities, *trust) != nil ||
			proof.Evidence.Runtime.ImageDigest != profile.ImageDigest || proof.Evidence.Runtime.ImagePlatform != profile.ImagePlatform || !proof.Evidence.Limits.Supports(profile) {
			return nil, sandboxFailure(contracts.ToolFailureSandboxCapability, "non-local sandbox qualification is missing, stale, or does not match the active runtime")
		}
		if trust.WorkspaceID != "" && (len(workspaceIDs) != 1 || strings.TrimSpace(workspaceIDs[0]) != trust.WorkspaceID) {
			return nil, sandboxFailure(contracts.ToolFailureSandboxCapability, "sandbox qualification is not authorized for this workspace")
		}
	}
	requiredCapabilities, err := profile.RequiredEnforcements()
	if err != nil {
		return nil, sandboxFailure(contracts.ToolFailureSandboxCapability, "sandbox profile has invalid enforcement requirements")
	}
	for _, required := range requiredCapabilities {
		if !containsSandboxCapability(capabilities.Enforced, required) {
			return nil, sandboxFailure(contracts.ToolFailureSandboxCapability, "sandbox backend does not enforce a required capability")
		}
	}
	return provider, nil
}

// QualificationHash resolves and validates the exact backend/profile before
// returning the signed proof identity for durable request evidence.
func (r *SandboxRegistry) QualificationHash(profile contracts.SandboxProfile, workspaceIDs ...string) (string, error) {
	provider, err := r.Resolve(profile, workspaceIDs...)
	if err != nil {
		return "", err
	}
	if provider.Backend() == contracts.SandboxBackendLocalProcess {
		return "", nil
	}
	r.mu.RLock()
	proof, ok := r.qualifications[provider.Backend()]
	r.mu.RUnlock()
	if !ok {
		return "", sandboxFailure(contracts.ToolFailureSandboxCapability, "sandbox qualification is unavailable")
	}
	hash := proof.Evidence.StableHash()
	if hash == "" {
		return "", sandboxFailure(contracts.ToolFailureSandboxCapability, "sandbox qualification evidence is invalid")
	}
	return hash, nil
}

// attemptProvider resolves an already-recorded runtime backend without
// constructing an execution profile. It is intentionally separate from
// Resolve: reconciliation may inspect an existing attempt, but must not gain
// authority to start one or substitute another backend.
func (r *SandboxRegistry) attemptProvider(backend contracts.SandboxBackend) (AttemptAwareSandboxProvider, error) {
	if r == nil {
		return nil, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "recorded sandbox backend is unavailable")
	}
	r.mu.RLock()
	provider, ok := r.providers[backend]
	proof, hasProof := r.qualifications[backend]
	trust := r.trust
	r.mu.RUnlock()
	if !ok || provider == nil || provider.Backend() != backend {
		return nil, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "recorded sandbox backend is unavailable")
	}
	capabilities := provider.Capabilities()
	if capabilities.Backend != backend || capabilities.Normalize() != nil || !capabilities.Available {
		return nil, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "recorded sandbox backend is unavailable")
	}
	attemptProvider, ok := provider.(AttemptAwareSandboxProvider)
	if !ok || !containsSandboxCapability(capabilities.Enforced, contracts.SandboxCapabilityDurableAttemptIdentity) || !containsSandboxCapability(capabilities.Enforced, contracts.SandboxCapabilityCrashReconciliation) {
		return nil, sandboxFailure(contracts.ToolFailureSandboxCapability, "recorded sandbox backend cannot reconcile durable attempts")
	}
	qualified, ok := provider.(QualifiedSandboxProvider)
	if !ok || !hasProof || trust == nil || qualification.VerifySandboxProof(proof, qualified.RuntimeIdentity(), capabilities, *trust) != nil {
		return nil, sandboxFailure(contracts.ToolFailureSandboxCapability, "recorded sandbox qualification is missing, stale, or changed")
	}
	return attemptProvider, nil
}

func cloneSandboxQualificationProof(proof contracts.SandboxQualificationProof) (contracts.SandboxQualificationProof, error) {
	raw, err := json.Marshal(proof)
	if err != nil {
		return contracts.SandboxQualificationProof{}, err
	}
	var clone contracts.SandboxQualificationProof
	if err := json.Unmarshal(raw, &clone); err != nil {
		return contracts.SandboxQualificationProof{}, err
	}
	if err := clone.Bundle.Normalize(); err != nil {
		return contracts.SandboxQualificationProof{}, err
	}
	return clone, nil
}

// ReconcileSandboxAttempt asks only the exact recorded non-local backend to
// inspect an existing attempt. It never invokes RunAttempt and never treats an
// absent/unknown observation as permission to execute again.
func (e *Executor) ReconcileSandboxAttempt(ctx context.Context, run contracts.ToolRun, link contracts.DomainEffectLink) (contracts.SandboxExecutionIdentity, contracts.SandboxAttemptObservation, error) {
	if e == nil || e.Sandboxes == nil {
		return contracts.SandboxExecutionIdentity{}, contracts.SandboxAttemptObservation{}, sandboxFailure(contracts.ToolFailureSandboxUnavailable, "sandbox recovery is unavailable")
	}
	if err := ctx.Err(); err != nil {
		return contracts.SandboxExecutionIdentity{}, contracts.SandboxAttemptObservation{}, err
	}
	if run.Status != contracts.ToolRunRecoveryRequired || run.ID == "" || run.WorkspaceID == "" || run.RequestHash == "" || run.Attempt < 1 ||
		link.WorkspaceID != run.WorkspaceID || link.DomainKind != contracts.DomainEffectKindToolRun || link.DomainID != run.ID ||
		link.DomainHash != run.RequestHash || link.LinkRole != contracts.DomainEffectLinkRolePrimary ||
		link.Status != contracts.DomainEffectLinkStatusRecoveryRequired || link.Boundary == string(contracts.SandboxBackendLocalProcess) {
		return contracts.SandboxExecutionIdentity{}, contracts.SandboxAttemptObservation{}, sandboxFailure(contracts.ToolFailureSandboxCapability, "tool recovery identity is incomplete or inconsistent")
	}
	var recorded contracts.ToolRequest
	if len(run.RequestEvidence) == 0 || json.Unmarshal(run.RequestEvidence, &recorded) != nil {
		return contracts.SandboxExecutionIdentity{}, contracts.SandboxAttemptObservation{}, sandboxFailure(contracts.ToolFailureSandboxCapability, "durable tool recovery evidence is unavailable")
	}
	if recorded.WorkspaceID != run.WorkspaceID || recorded.RequestID != run.RequestID || recorded.IdempotencyKey != run.IdempotencyKey ||
		recorded.ToolID != run.ToolID || recorded.TaskOwnerID != run.TaskOwnerID || recorded.TaskFence != run.TaskFence ||
		recorded.AgentRunOwnerID != run.AgentRunOwnerID || recorded.AgentRunFence != run.AgentRunFence ||
		entityRefID(recorded.Task) != entityRefID(run.Task) || entityRefID(recorded.AgentRun) != entityRefID(run.AgentRun) ||
		recorded.ToolDefinitionHash == "" || recorded.SandboxProfileHash == "" || recorded.SandboxQualificationHash == "" || recorded.Budget.Backend != link.Boundary {
		return contracts.SandboxExecutionIdentity{}, contracts.SandboxAttemptObservation{}, sandboxFailure(contracts.ToolFailureSandboxCapability, "durable tool recovery evidence does not match the reserved attempt")
	}
	backend := contracts.SandboxBackend(link.Boundary)
	qualificationHash, err := e.Sandboxes.QualificationHash(recorded.Budget, run.WorkspaceID)
	if err != nil || qualificationHash == "" || qualificationHash != recorded.SandboxQualificationHash {
		return contracts.SandboxExecutionIdentity{}, contracts.SandboxAttemptObservation{}, sandboxFailure(contracts.ToolFailureSandboxCapability, "recorded sandbox qualification no longer matches the attempt")
	}
	provider, err := e.Sandboxes.attemptProvider(backend)
	if err != nil {
		return contracts.SandboxExecutionIdentity{}, contracts.SandboxAttemptObservation{}, err
	}
	identity := contracts.SandboxExecutionIdentity{
		SchemaVersion: contracts.SandboxExecutionIdentitySchemaVersion,
		WorkspaceID:   run.WorkspaceID, ToolRunID: run.ID, ToolAttempt: run.Attempt, Backend: backend,
		OperationID: link.OperationID, OperationOwnerID: link.OperationOwnerID, OperationFence: link.OperationFence,
		AttemptID: link.AttemptID, EffectID: link.EffectID, ToolRequestHash: run.RequestHash,
		OperationRequestHash: link.OperationHash, EffectReservationHash: link.EffectReservationHash,
		TaskOwnerID: link.TaskOwnerID, TaskFence: link.TaskFence,
		AgentRunID: entityRefID(run.AgentRun), AgentRunOwnerID: run.AgentRunOwnerID, AgentRunFence: run.AgentRunFence,
		ToolDefinitionHash: recorded.ToolDefinitionHash, SandboxProfileHash: recorded.SandboxProfileHash,
		QualificationHash: recorded.SandboxQualificationHash,
	}
	if err := identity.Normalize(); err != nil {
		return contracts.SandboxExecutionIdentity{}, contracts.SandboxAttemptObservation{}, sandboxFailure(contracts.ToolFailureSandboxCapability, "durable tool recovery identity is invalid")
	}
	observation, err := provider.ReconcileAttempt(ctx, identity)
	if err != nil {
		// Provider diagnostics can contain paths, commands, or credentials.
		return contracts.SandboxExecutionIdentity{}, contracts.SandboxAttemptObservation{}, sandboxFailure(contracts.ToolFailureExternalUncertain, "sandbox outcome could not be reconciled")
	}
	if err := observation.Normalize(identity); err != nil {
		return contracts.SandboxExecutionIdentity{}, contracts.SandboxAttemptObservation{}, sandboxFailure(contracts.ToolFailureSandboxCapability, "sandbox observation does not match the reserved attempt")
	}
	return identity, observation, nil
}

func entityRefID(reference *contracts.EntityRef) string {
	if reference == nil {
		return ""
	}
	return reference.ID
}

func sandboxExecutionIdentity(run contracts.ToolRun, request contracts.ToolRequest, definition contracts.ToolDefinition, authority contracts.EffectAuthority) (contracts.SandboxExecutionIdentity, error) {
	if err := authority.Normalize(); err != nil {
		return contracts.SandboxExecutionIdentity{}, err
	}
	toolRequestHash, err := request.RequestHash()
	if err != nil {
		return contracts.SandboxExecutionIdentity{}, err
	}
	if run.ID == "" || run.WorkspaceID != request.WorkspaceID || authority.WorkspaceID != request.WorkspaceID ||
		run.RequestID != request.RequestID || run.ToolID != request.ToolID || run.ToolID != definition.ID ||
		run.RequestHash == "" || run.RequestHash != toolRequestHash || authority.RequestHash == "" ||
		run.TaskOwnerID != request.TaskOwnerID || run.TaskFence != request.TaskFence ||
		sandboxEntityID(run.Task) != sandboxEntityID(request.Task) ||
		authority.TaskOwnerID != request.TaskOwnerID || authority.TaskFence != request.TaskFence ||
		sandboxEntityID(run.AgentRun) != sandboxEntityID(request.AgentRun) ||
		run.AgentRunOwnerID != request.AgentRunOwnerID || run.AgentRunFence != request.AgentRunFence {
		return contracts.SandboxExecutionIdentity{}, fmt.Errorf("tool run, request, and effect authority do not match")
	}
	definitionHash, err := definition.Hash()
	if err != nil {
		return contracts.SandboxExecutionIdentity{}, err
	}
	profileHash, err := definition.Sandbox.Hash()
	if err != nil {
		return contracts.SandboxExecutionIdentity{}, err
	}
	if request.ToolDefinitionHash != "" && request.ToolDefinitionHash != definitionHash || request.SandboxProfileHash != "" && request.SandboxProfileHash != profileHash {
		return contracts.SandboxExecutionIdentity{}, fmt.Errorf("request execution hashes do not match the resolved definition")
	}
	identity := contracts.SandboxExecutionIdentity{
		SchemaVersion: contracts.SandboxExecutionIdentitySchemaVersion,
		WorkspaceID:   request.WorkspaceID, ToolRunID: run.ID, ToolAttempt: run.Attempt,
		Backend:     contracts.SandboxBackend(definition.Sandbox.Backend),
		OperationID: authority.OperationID, OperationOwnerID: authority.OperationOwnerID,
		OperationFence: authority.OperationFence, AttemptID: authority.AttemptID, EffectID: authority.EffectID,
		ToolRequestHash: toolRequestHash, OperationRequestHash: authority.RequestHash,
		EffectReservationHash: authority.EffectReservationHash,
		TaskOwnerID:           authority.TaskOwnerID, TaskFence: authority.TaskFence,
		AgentRunID: sandboxEntityID(run.AgentRun), AgentRunOwnerID: run.AgentRunOwnerID, AgentRunFence: run.AgentRunFence,
		ToolDefinitionHash: definitionHash, SandboxProfileHash: profileHash,
		QualificationHash: request.SandboxQualificationHash,
	}
	if err := identity.Normalize(); err != nil {
		return contracts.SandboxExecutionIdentity{}, err
	}
	return identity, nil
}

func sandboxEntityID(reference *contracts.EntityRef) string {
	if reference == nil {
		return ""
	}
	return reference.ID
}

// Capabilities returns a deterministic snapshot of registered provider
// declarations. Returned slices cannot mutate provider state held by the registry.
func (r *SandboxRegistry) Capabilities() []contracts.SandboxBackendCapabilities {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	providers := make([]SandboxProvider, 0, len(r.providers))
	for _, provider := range r.providers {
		providers = append(providers, provider)
	}
	r.mu.RUnlock()
	result := make([]contracts.SandboxBackendCapabilities, 0, len(providers))
	for _, provider := range providers {
		capabilities := provider.Capabilities()
		capabilities.Enforced = append([]contracts.SandboxCapability(nil), capabilities.Enforced...)
		capabilities.NotEnforced = append([]contracts.SandboxCapability(nil), capabilities.NotEnforced...)
		capabilities.Limitations = append([]string(nil), capabilities.Limitations...)
		_ = capabilities.Normalize()
		result = append(result, capabilities)
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Backend < result[j].Backend })
	return result
}

// CapabilityMatrix returns every known backend in stable order. Unregistered
// runtimes are reported unavailable rather than being represented by an empty
// capability claim.
func CapabilityMatrix(registry *SandboxRegistry) []contracts.SandboxBackendCapabilities {
	if registry == nil {
		registry = NewDefaultSandboxRegistry()
	}
	registered := make(map[contracts.SandboxBackend]contracts.SandboxBackendCapabilities)
	for _, capabilities := range registry.Capabilities() {
		registered[capabilities.Backend] = capabilities
	}
	result := make([]contracts.SandboxBackendCapabilities, 0, len(contracts.KnownSandboxBackends()))
	for _, backend := range contracts.KnownSandboxBackends() {
		if capabilities, ok := registered[backend]; ok {
			result = append(result, capabilities)
			continue
		}
		result = append(result, contracts.SandboxBackendCapabilities{
			Backend: backend, Available: false,
			Limitations: []string{"no sandbox provider is registered"},
		})
	}
	return result
}

func containsSandboxCapability(capabilities []contracts.SandboxCapability, required contracts.SandboxCapability) bool {
	for _, capability := range capabilities {
		if capability == required {
			return true
		}
	}
	return false
}

func sandboxFailure(code, message string) *FailureError {
	return &FailureError{Failure: contracts.ToolFailure{Code: code, Message: message}}
}
