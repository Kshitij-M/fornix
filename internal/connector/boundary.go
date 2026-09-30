package connector

import "github.com/omaveda/fornix/internal/contracts"

// BoundaryAuthority derives the portable hash-only envelope from the exact
// egress policy used by a connector's controlled client.
func BoundaryAuthority(policy EgressPolicy) (contracts.ExternalBoundaryAuthority, error) {
	if err := policy.Normalize(); err != nil {
		return contracts.ExternalBoundaryAuthority{}, err
	}
	egressHash := policy.StableHash()
	destinationHash := policy.Destination.StableHash()
	if egressHash == "" || destinationHash == "" {
		return contracts.ExternalBoundaryAuthority{}, ErrDestinationPolicy
	}
	return contracts.ExternalBoundaryAuthority{
		SchemaVersion:         contracts.ExternalBoundarySchemaVersion,
		EgressPolicyHash:      egressHash,
		DestinationPolicyHash: destinationHash,
		NetworkBoundary:       contracts.NetworkBoundaryControlledTransport,
		NetworkBoundaryHash:   contracts.HashStrings("fornix-network-boundary", contracts.NetworkBoundaryControlledTransport, egressHash, destinationHash),
	}, nil
}
