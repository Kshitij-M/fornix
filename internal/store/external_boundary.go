package store

import (
	"fmt"

	"github.com/omaveda/fornix/internal/contracts"
)

func boundaryFromColumns(egressHash, destinationHash, networkBoundary, networkHash string) (*contracts.ExternalBoundaryAuthority, error) {
	if egressHash == "" && destinationHash == "" && networkBoundary == "" && networkHash == "" {
		return nil, nil
	}
	value := &contracts.ExternalBoundaryAuthority{
		SchemaVersion:         contracts.ExternalBoundarySchemaVersion,
		EgressPolicyHash:      egressHash,
		DestinationPolicyHash: destinationHash,
		NetworkBoundary:       networkBoundary,
		NetworkBoundaryHash:   networkHash,
	}
	if err := value.Normalize(); err != nil {
		return nil, fmt.Errorf("normalize stored external boundary: %w", err)
	}
	return value, nil
}

func boundaryColumns(value *contracts.ExternalBoundaryAuthority) (egressHash, destinationHash, networkBoundary, networkHash string, err error) {
	if value == nil {
		return "", "", "", "", nil
	}
	copyValue := contracts.CloneExternalBoundary(value)
	if err := copyValue.Normalize(); err != nil {
		return "", "", "", "", err
	}
	return copyValue.EgressPolicyHash, copyValue.DestinationPolicyHash, copyValue.NetworkBoundary, copyValue.NetworkBoundaryHash, nil
}

func sameExternalBoundary(left, right *contracts.ExternalBoundaryAuthority) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.StableHash() != "" && left.StableHash() == right.StableHash()
}
