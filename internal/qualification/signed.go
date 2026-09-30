package qualification

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"sort"

	"github.com/omaveda/fornix/internal/contracts"
)

// ValidateSignedBundle verifies cryptographic integrity and optional external
// trust/scope constraints. It performs no network lookup or durable write.
func ValidateSignedBundle(value contracts.SignedQualificationBundle, workspaceID, targetHash, expectedKeyID string, expectedPublicKey ed25519.PublicKey) error {
	if err := value.VerifyWithKey(expectedKeyID, expectedPublicKey); err != nil {
		return err
	}
	if workspaceID != "" && value.Bundle.Report.WorkspaceID != workspaceID {
		return fmt.Errorf("signed qualification crosses workspace scope")
	}
	if targetHash != "" && value.Bundle.Report.TargetHash != targetHash {
		return fmt.Errorf("signed qualification crosses target scope")
	}
	return nil
}

// MergeSignedBundles verifies each source and deterministically merges their
// redacted reports. Source signatures are not copied onto the aggregate: a
// caller must explicitly sign the returned bundle with an authorized key.
func MergeSignedBundles(ctx context.Context, runID, workspaceID, targetHash string, signed []contracts.SignedQualificationBundle, options RunnerOptions, expectedKeyID string, expectedPublicKey ed25519.PublicKey) (Result, error) {
	if len(signed) == 0 || len(signed) > contracts.MaxQualificationManifestChecks {
		return Result{}, fmt.Errorf("signed qualification merge requires between 1 and %d bundles", contracts.MaxQualificationManifestChecks)
	}
	ordered := append([]contracts.SignedQualificationBundle(nil), signed...)
	for index := range ordered {
		if err := ValidateSignedBundle(ordered[index], workspaceID, targetHash, expectedKeyID, expectedPublicKey); err != nil {
			return Result{}, fmt.Errorf("signed qualification input %d is invalid", index)
		}
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Signature.SignedHash != ordered[j].Signature.SignedHash {
			return ordered[i].Signature.SignedHash < ordered[j].Signature.SignedHash
		}
		return ordered[i].Signature.KeyID < ordered[j].Signature.KeyID
	})
	seenSigned := make(map[string]struct{}, len(ordered))
	seenBundle := make(map[string]string, len(ordered))
	bundles := make([]contracts.QualificationBundle, 0, len(ordered))
	for _, value := range ordered {
		if _, exists := seenSigned[value.Signature.SignedHash]; exists {
			continue
		}
		manifestHash := value.Bundle.Manifest.ManifestHash
		if prior, exists := seenBundle[manifestHash]; exists && prior != value.Signature.SignedHash {
			return Result{}, fmt.Errorf("qualification bundle has conflicting signatures")
		}
		seenSigned[value.Signature.SignedHash] = struct{}{}
		seenBundle[manifestHash] = value.Signature.SignedHash
		bundles = append(bundles, value.Bundle)
	}
	return MergeBundles(ctx, runID, workspaceID, targetHash, bundles, options)
}
