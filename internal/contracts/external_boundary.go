package contracts

import (
	"fmt"
	"strings"
)

// ExternalBoundarySchemaVersion versions the redacted network-boundary
// envelope independently from operation and provider request schemas.
const ExternalBoundarySchemaVersion = 1

const (
	// NetworkBoundaryControlledTransport identifies Fornix's bounded HTTP
	// transport and resolver policy. It is an input fact, not proof of host
	// firewall or network-namespace isolation.
	NetworkBoundaryControlledTransport = "controlled_transport"
	// NetworkBoundaryDeploymentAttested is reserved for a deployment-owned
	// attestation that binds the process to a known network boundary.
	NetworkBoundaryDeploymentAttested = "deployment_attested"
)

// ExternalBoundaryAuthority is the hash-only envelope required for a network
// effect. Policy payloads, URLs with credentials, proxy tokens, and provider
// responses never cross this contract. Credential lease identity and source
// facts remain on EffectAuthority because they are validated by the durable
// credential authority.
type ExternalBoundaryAuthority struct {
	SchemaVersion         int    `json:"schema_version"`
	EgressPolicyHash      string `json:"egress_policy_hash"`
	DestinationPolicyHash string `json:"destination_policy_hash"`
	NetworkBoundary       string `json:"network_boundary"`
	NetworkBoundaryHash   string `json:"network_boundary_hash"`
}

// IsZero reports whether no boundary facts were supplied. A zero pointer is
// the compatibility representation for fake/read-only paths; a non-nil
// partial value is invalid and must fail closed.
func (a ExternalBoundaryAuthority) IsZero() bool {
	return strings.TrimSpace(a.EgressPolicyHash) == "" &&
		strings.TrimSpace(a.DestinationPolicyHash) == "" &&
		strings.TrimSpace(a.NetworkBoundary) == "" &&
		strings.TrimSpace(a.NetworkBoundaryHash) == ""
}

// Normalize validates a complete, secret-free boundary envelope.
func (a *ExternalBoundaryAuthority) Normalize() error {
	if a == nil {
		return fmt.Errorf("external boundary authority is nil")
	}
	if a.SchemaVersion == 0 {
		a.SchemaVersion = ExternalBoundarySchemaVersion
	}
	if a.SchemaVersion != ExternalBoundarySchemaVersion {
		return fmt.Errorf("unsupported external boundary schema_version %d", a.SchemaVersion)
	}
	var err error
	if a.EgressPolicyHash, err = normalizeDomainHash(a.EgressPolicyHash, "egress policy hash", true); err != nil {
		return err
	}
	if a.DestinationPolicyHash, err = normalizeDomainHash(a.DestinationPolicyHash, "destination policy hash", true); err != nil {
		return err
	}
	a.NetworkBoundary = strings.ToLower(strings.TrimSpace(a.NetworkBoundary))
	switch a.NetworkBoundary {
	case NetworkBoundaryControlledTransport, NetworkBoundaryDeploymentAttested:
	default:
		return fmt.Errorf("unsupported network boundary %q", a.NetworkBoundary)
	}
	if a.NetworkBoundaryHash, err = normalizeDomainHash(a.NetworkBoundaryHash, "network boundary hash", true); err != nil {
		return err
	}
	return nil
}

// StableHash returns the canonical identity of the redacted boundary facts.
func (a ExternalBoundaryAuthority) StableHash() string {
	if err := a.Normalize(); err != nil {
		return ""
	}
	return HashStrings("external-boundary-authority", fmt.Sprint(a.SchemaVersion), a.EgressPolicyHash, a.DestinationPolicyHash, a.NetworkBoundary, a.NetworkBoundaryHash)
}

// CloneExternalBoundary returns a defensive copy suitable for an authority
// envelope. No secret-bearing value is represented by this type.
func CloneExternalBoundary(value *ExternalBoundaryAuthority) *ExternalBoundaryAuthority {
	if value == nil {
		return nil
	}
	clone := *value
	return &clone
}

// RequiresExternalBoundary identifies network communication effects. Local
// process and filesystem effects still require operation/effect authority but
// do not pretend to have a network policy.
func RequiresExternalBoundary(effect EffectClass) bool {
	return effect == EffectClassExternalCommunication
}
