package qualification

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func signedQualificationResult(t *testing.T, workspaceID, targetHash, keyID string, seed byte) contracts.SignedQualificationBundle {
	t.Helper()
	result, err := Run(context.Background(), "signed-run", workspaceID, targetHash, OfflineChecks(), RunnerOptions{RunnerVersion: "1", EnvironmentNames: []string{"FORNIX_MODE"}})
	if err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{seed}, ed25519.SeedSize))
	signed, err := contracts.SignQualificationBundle(result.Bundle, keyID, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func TestMergeSignedBundlesIsIdempotentAndDeterministic(t *testing.T) {
	targetHash := contracts.HashStrings("signed-merge-target")
	signed := signedQualificationResult(t, "workspace-a", targetHash, "deployment-key-v1", 0x42)
	options := RunnerOptions{RunnerVersion: "1", EnvironmentNames: []string{"FORNIX_MODE"}}
	first, err := MergeSignedBundles(context.Background(), "merged-run", "workspace-a", targetHash, []contracts.SignedQualificationBundle{signed, signed}, options, "deployment-key-v1", privatePublicKey(t, 0x42))
	if err != nil {
		t.Fatal(err)
	}
	second, err := MergeSignedBundles(context.Background(), "merged-run", "workspace-a", targetHash, []contracts.SignedQualificationBundle{signed}, options, "deployment-key-v1", privatePublicKey(t, 0x42))
	if err != nil {
		t.Fatal(err)
	}
	if first.Bundle.Report.ReportHash != second.Bundle.Report.ReportHash || first.Bundle.Manifest.ManifestHash != second.Bundle.Manifest.ManifestHash {
		t.Fatalf("duplicate merge changed output: first=%+v second=%+v", first.Bundle, second.Bundle)
	}
}

func TestMergeSignedBundlesRejectsScopeAndConflictingEvidence(t *testing.T) {
	targetHash := contracts.HashStrings("signed-merge-target")
	valid := signedQualificationResult(t, "workspace-a", targetHash, "deployment-key-v1", 0x42)
	if _, err := MergeSignedBundles(context.Background(), "merged-run", "workspace-b", targetHash, []contracts.SignedQualificationBundle{valid}, RunnerOptions{}, "deployment-key-v1", privatePublicKey(t, 0x42)); err == nil {
		t.Fatal("cross-workspace signed bundle was accepted")
	}
	if _, err := MergeSignedBundles(context.Background(), "merged-run", "workspace-a", contracts.HashStrings("other-target"), []contracts.SignedQualificationBundle{valid}, RunnerOptions{}, "deployment-key-v1", privatePublicKey(t, 0x42)); err == nil {
		t.Fatal("cross-target signed bundle was accepted")
	}
	other := signedQualificationWithEvidence(t, "workspace-a", targetHash, "deployment-key-v2", 0x24, "different-evidence")
	if _, err := MergeSignedBundles(context.Background(), "merged-run", "workspace-a", targetHash, []contracts.SignedQualificationBundle{valid, other}, RunnerOptions{}, "", nil); err == nil {
		t.Fatal("conflicting signed evidence was accepted")
	}
	tampered := valid
	tampered.ObservationHash = contracts.HashStrings("tampered")
	if _, err := MergeSignedBundles(context.Background(), "merged-run", "workspace-a", targetHash, []contracts.SignedQualificationBundle{tampered}, RunnerOptions{}, "deployment-key-v1", privatePublicKey(t, 0x42)); err == nil {
		t.Fatal("tampered signed bundle was accepted")
	}
}

func signedQualificationWithEvidence(t *testing.T, workspaceID, targetHash, keyID string, seed byte, evidence string) contracts.SignedQualificationBundle {
	t.Helper()
	result, err := Run(context.Background(), "signed-run", workspaceID, targetHash, []Check{{
		Name: "qualification-contract", Version: "1", Category: contracts.QualificationCategoryAuthority, Offline: true,
		Run: func(context.Context) (contracts.QualificationCase, error) {
			return contracts.QualificationCase{Outcome: contracts.QualificationOutcomePassed, EvidenceHash: contracts.HashStrings(evidence)}, nil
		},
	}}, RunnerOptions{RunnerVersion: "1", EnvironmentNames: []string{"FORNIX_MODE"}})
	if err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{seed}, ed25519.SeedSize))
	signed, err := contracts.SignQualificationBundle(result.Bundle, keyID, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return signed
}

func privatePublicKey(t *testing.T, seed byte) ed25519.PublicKey {
	t.Helper()
	return ed25519.NewKeyFromSeed(bytes.Repeat([]byte{seed}, ed25519.SeedSize)).Public().(ed25519.PublicKey)
}
