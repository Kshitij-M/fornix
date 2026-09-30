package connector

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
)

const AuthorityConformanceSchemaVersion = 1

// AuthorityConformanceEntry is a bounded, hash-only inventory item. It
// describes registration-time facts; it does not grant authority or prove
// that a remote provider honors its protocol.
type AuthorityConformanceEntry struct {
	Capability contracts.CapabilityRef `json:"capability"`
	Effect     contracts.EffectClass   `json:"effect"`
	Ready      bool                    `json:"ready"`
	Missing    []string                `json:"missing,omitempty"`
}

// AuthorityConformanceReport is a deterministic inventory of effectful
// connector capabilities for one workspace. Missing contains bounded control
// codes rather than raw adapter errors or provider payloads.
type AuthorityConformanceReport struct {
	SchemaVersion int                         `json:"schema_version"`
	WorkspaceID   string                      `json:"workspace_id"`
	Strict        bool                        `json:"strict"`
	Entries       []AuthorityConformanceEntry `json:"entries"`
	Ready         bool                        `json:"ready"`
	ReportHash    string                      `json:"report_hash"`
}

// WorkspaceIDs returns all registered workspace identities in stable order.
// The registry is process-local; durable workspace existence remains owned by
// Postgres and callers must bound any durable workspace inventory separately.
func (r *Registry) WorkspaceIDs() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	seen := make(map[string]struct{}, len(r.connectors)+len(r.capabilities))
	for _, entry := range r.connectors {
		seen[entry.ref.WorkspaceID] = struct{}{}
	}
	for _, entry := range r.capabilities {
		seen[entry.definition.WorkspaceID] = struct{}{}
	}
	r.mu.RUnlock()
	workspaces := make([]string, 0, len(seen))
	for workspaceID := range seen {
		workspaces = append(workspaces, workspaceID)
	}
	sort.Strings(workspaces)
	return workspaces
}

// AuthorityConformance inventories one workspace without invoking an adapter,
// provider, database, or external system. Strict controls are reported for
// effectful capabilities; read-only and observation capabilities are omitted
// because they do not cross the external-effect reservation boundary.
func (r *Registry) AuthorityConformance(workspaceID string, strict bool) AuthorityConformanceReport {
	workspaceID = strings.TrimSpace(workspaceID)
	report := AuthorityConformanceReport{
		SchemaVersion: AuthorityConformanceSchemaVersion,
		WorkspaceID:   workspaceID,
		Strict:        strict,
		Entries:       make([]AuthorityConformanceEntry, 0),
		Ready:         workspaceID != "",
	}
	for _, definition := range r.Capabilities(workspaceID) {
		if definition.Effect == contracts.EffectClassReadOnly || definition.Effect == contracts.EffectClassObservation {
			continue
		}
		entry := AuthorityConformanceEntry{
			Capability: definition.Ref,
			Effect:     definition.Effect,
			Ready:      true,
			Missing:    make([]string, 0),
		}
		// Look up the entry while holding the registry's immutable registration
		// identity. The capability may not be reconstructed from a name alone.
		r.mu.RLock()
		registered, found := r.capabilities[capabilityKey(definition.Ref)]
		r.mu.RUnlock()
		if !found || registered.capability == nil {
			entry.Missing = append(entry.Missing, "capability_registration")
		} else {
			if _, ok := registered.capability.(AuthorityAwareCapability); !ok {
				entry.Missing = append(entry.Missing, "authority_execution")
			}
			if _, ok := registered.capability.(EffectDescriber); !ok {
				entry.Missing = append(entry.Missing, "effect_descriptor")
			}
			if definition.Effect == contracts.EffectClassExternalCommunication {
				if _, ok := registered.capability.(BoundaryDescriber); !ok {
					entry.Missing = append(entry.Missing, "boundary_descriptor")
				}
			}
		}
		if !definition.SupportsIdempotency {
			entry.Missing = append(entry.Missing, "provider_idempotency")
		}
		if !definition.SupportsVerification {
			entry.Missing = append(entry.Missing, "verification")
		}
		if !definition.Profile.AllowExternalEffects {
			entry.Missing = append(entry.Missing, "external_effect_profile")
		}
		sort.Strings(entry.Missing)
		entry.Ready = len(entry.Missing) == 0
		if strict && !entry.Ready {
			report.Ready = false
		}
		report.Entries = append(report.Entries, entry)
	}
	sort.Slice(report.Entries, func(i, j int) bool {
		return capabilityIdentityKey(report.Entries[i].Capability) < capabilityIdentityKey(report.Entries[j].Capability)
	})
	report.ReportHash = authorityConformanceHash(report)
	return report
}

// AuthorityConformanceAll returns one deterministic report for each
// registered workspace. It is intentionally bounded by the process-local
// registry; server startup applies its own durable workspace limit.
func (r *Registry) AuthorityConformanceAll(strict bool) []AuthorityConformanceReport {
	workspaces := r.WorkspaceIDs()
	reports := make([]AuthorityConformanceReport, 0, len(workspaces))
	for _, workspaceID := range workspaces {
		reports = append(reports, r.AuthorityConformance(workspaceID, strict))
	}
	return reports
}

// ValidateEffectAuthorityConformanceAll validates every registered workspace
// and returns only bounded capability identities. It never executes adapters.
func (r *Registry) ValidateEffectAuthorityConformanceAll(strict bool) error {
	if r == nil {
		return ErrRegistryNil
	}
	reports := r.AuthorityConformanceAll(strict)
	failures := make([]string, 0)
	for _, report := range reports {
		if report.Ready {
			continue
		}
		for _, entry := range report.Entries {
			if !entry.Ready {
				failures = append(failures, report.WorkspaceID+":"+capabilityIdentityKey(entry.Capability)+":"+strings.Join(entry.Missing, "+"))
			}
		}
	}
	if len(failures) == 0 {
		return nil
	}
	sort.Strings(failures)
	return fmt.Errorf("effect authority conformance failed: %s", strings.Join(failures, ","))
}

// ValidateEffectAuthorityConformance verifies the process composition rule
// that every registered effectful capability accepts the durable authority
// envelope. It is intentionally side-effect free and should run at startup
// and in CI. A missing method is a hard composition error, not a reason to
// downgrade the capability to the ordinary Execute seam.
func (r *Registry) ValidateEffectAuthorityConformance(workspaceID string) error {
	if r == nil {
		return ErrRegistryNil
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return fmt.Errorf("workspace_id is required")
	}
	report := r.AuthorityConformance(workspaceID, true)
	if report.Ready {
		return nil
	}
	failures := make([]string, 0)
	for _, entry := range report.Entries {
		if !entry.Ready {
			failures = append(failures, capabilityIdentityKey(entry.Capability)+":"+strings.Join(entry.Missing, "+"))
		}
	}
	if len(failures) == 0 {
		failures = append(failures, "empty_inventory")
	}
	sort.Strings(failures)
	return fmt.Errorf("effectful capability conformance failed: %s", strings.Join(failures, ","))
}

func authorityConformanceHash(report AuthorityConformanceReport) string {
	copyReport := report
	copyReport.ReportHash = ""
	raw, _ := json.Marshal(copyReport)
	return contracts.HashStrings("connector-authority-conformance", string(raw))
}
