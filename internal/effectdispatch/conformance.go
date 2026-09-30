package effectdispatch

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/omaveda/fornix/internal/contracts"
)

const AdapterConformanceSchemaVersion = 1

// AdapterConformance declares the authority composition required by one
// production-composed dynamic effect adapter. It is qualification metadata,
// not a capability grant and not proof of remote-provider behavior.
type AdapterConformance struct {
	ID                      string                `json:"id"`
	DomainKind              string                `json:"domain_kind"`
	Boundary                string                `json:"boundary"`
	Effect                  contracts.EffectClass `json:"effect"`
	UsesOperationAdmission  bool                  `json:"uses_operation_admission"`
	UsesEffectReservation   bool                  `json:"uses_effect_reservation"`
	UsesOperationFence      bool                  `json:"uses_operation_fence"`
	UsesTaskFenceWhenBound  bool                  `json:"uses_task_fence_when_bound"`
	UsesDeploymentAdmission bool                  `json:"uses_deployment_admission"`
	UsesExternalBoundary    bool                  `json:"uses_external_boundary_authority"`
	PreservesAtLeastOnce    bool                  `json:"preserves_at_least_once"`
	SupportsRecovery        bool                  `json:"supports_recovery"`
}

// ConformanceRegistry is a deterministic process-local manifest of dynamic
// effect adapters. The actual operation/effect authority is still Postgres.
type ConformanceRegistry struct {
	mu      sync.RWMutex
	entries map[string]AdapterConformance
}

// NewConformanceRegistry creates an empty adapter manifest.
func NewConformanceRegistry() *ConformanceRegistry {
	return &ConformanceRegistry{entries: make(map[string]AdapterConformance)}
}

// Register adds one bounded adapter manifest and rejects incomplete authority
// composition instead of allowing an unsafe partial declaration.
func (r *ConformanceRegistry) Register(entry AdapterConformance) error {
	if r == nil {
		return fmt.Errorf("adapter conformance registry is nil")
	}
	entry.ID = strings.TrimSpace(entry.ID)
	entry.DomainKind = strings.TrimSpace(entry.DomainKind)
	entry.Boundary = strings.TrimSpace(entry.Boundary)
	if entry.ID == "" || entry.DomainKind == "" || entry.Boundary == "" {
		return fmt.Errorf("adapter conformance id, domain_kind, and boundary are required")
	}
	if entry.Effect == contracts.EffectClassReadOnly || entry.Effect == contracts.EffectClassObservation || !validEffectClass(entry.Effect) {
		return fmt.Errorf("adapter conformance %s must be effectful", entry.ID)
	}
	if !entry.UsesOperationAdmission || !entry.UsesEffectReservation || !entry.UsesOperationFence || !entry.UsesTaskFenceWhenBound || !entry.UsesDeploymentAdmission || !entry.PreservesAtLeastOnce || !entry.SupportsRecovery || entry.Effect == contracts.EffectClassExternalCommunication && !entry.UsesExternalBoundary {
		return fmt.Errorf("adapter conformance %s is missing an authority requirement", entry.ID)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.entries[entry.ID]; exists {
		return fmt.Errorf("adapter conformance %s is already registered", entry.ID)
	}
	r.entries[entry.ID] = entry
	return nil
}

func validEffectClass(effect contracts.EffectClass) bool {
	switch effect {
	case contracts.EffectClassReversibleWrite, contracts.EffectClassApprovalRequiredWrite,
		contracts.EffectClassIrreversibleWrite, contracts.EffectClassExternalCommunication:
		return true
	default:
		return false
	}
}

// Entries returns a stable defensive copy of the manifest.
func (r *ConformanceRegistry) Entries() []AdapterConformance {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	entries := make([]AdapterConformance, 0, len(r.entries))
	for _, entry := range r.entries {
		entries = append(entries, entry)
	}
	r.mu.RUnlock()
	sort.Slice(entries, func(i, j int) bool { return entries[i].ID < entries[j].ID })
	return entries
}

// Validate returns a stable hash of a complete manifest. It never performs a
// provider, tool, database, broker, or network operation.
func (r *ConformanceRegistry) Validate() (string, error) {
	if r == nil {
		return "", fmt.Errorf("adapter conformance registry is nil")
	}
	entries := r.Entries()
	if len(entries) == 0 {
		return "", fmt.Errorf("adapter conformance registry is empty")
	}
	raw, err := json.Marshal(struct {
		SchemaVersion int                  `json:"schema_version"`
		Entries       []AdapterConformance `json:"entries"`
	}{AdapterConformanceSchemaVersion, entries})
	if err != nil {
		return "", fmt.Errorf("hash adapter conformance manifest: %w", err)
	}
	return contracts.HashStrings("effect-adapter-conformance", string(raw)), nil
}

// BuiltinConformanceRegistry declares the dynamic effect paths composed by
// the server. All of them enter effectdispatch.Runtime, which supplies the
// operation/effect fences and revalidates deployment admission in Postgres.
func BuiltinConformanceRegistry() (*ConformanceRegistry, error) {
	r := NewConformanceRegistry()
	entries := []AdapterConformance{
		{ID: "model.complete", DomainKind: contracts.DomainEffectKindModelCall, Boundary: "model-provider", Effect: contracts.EffectClassExternalCommunication, UsesOperationAdmission: true, UsesEffectReservation: true, UsesOperationFence: true, UsesTaskFenceWhenBound: true, UsesDeploymentAdmission: true, UsesExternalBoundary: true, PreservesAtLeastOnce: true, SupportsRecovery: true},
		{ID: "model.stream", DomainKind: contracts.DomainEffectKindModelCall, Boundary: "model-provider", Effect: contracts.EffectClassExternalCommunication, UsesOperationAdmission: true, UsesEffectReservation: true, UsesOperationFence: true, UsesTaskFenceWhenBound: true, UsesDeploymentAdmission: true, UsesExternalBoundary: true, PreservesAtLeastOnce: true, SupportsRecovery: true},
		{ID: "embedding.generate", DomainKind: contracts.DomainEffectKindEmbeddingCall, Boundary: "embedding-provider", Effect: contracts.EffectClassExternalCommunication, UsesOperationAdmission: true, UsesEffectReservation: true, UsesOperationFence: true, UsesTaskFenceWhenBound: true, UsesDeploymentAdmission: true, UsesExternalBoundary: true, PreservesAtLeastOnce: true, SupportsRecovery: true},
		{ID: "embedding.reconcile", DomainKind: contracts.DomainEffectKindEmbeddingCall, Boundary: "embedding-provider-reconciliation", Effect: contracts.EffectClassExternalCommunication, UsesOperationAdmission: true, UsesEffectReservation: true, UsesOperationFence: true, UsesTaskFenceWhenBound: true, UsesDeploymentAdmission: true, UsesExternalBoundary: true, PreservesAtLeastOnce: true, SupportsRecovery: true},
		{ID: "tool.run", DomainKind: contracts.DomainEffectKindToolRun, Boundary: "local-process", Effect: contracts.EffectClassReversibleWrite, UsesOperationAdmission: true, UsesEffectReservation: true, UsesOperationFence: true, UsesTaskFenceWhenBound: true, UsesDeploymentAdmission: true, PreservesAtLeastOnce: true, SupportsRecovery: true},
		{ID: "change.apply", DomainKind: contracts.DomainEffectKindChangeApplication, Boundary: "configured-filesystem-mount", Effect: contracts.EffectClassReversibleWrite, UsesOperationAdmission: true, UsesEffectReservation: true, UsesOperationFence: true, UsesTaskFenceWhenBound: true, UsesDeploymentAdmission: true, PreservesAtLeastOnce: true, SupportsRecovery: true},
	}
	for _, entry := range entries {
		if err := r.Register(entry); err != nil {
			return nil, err
		}
	}
	return r, nil
}
