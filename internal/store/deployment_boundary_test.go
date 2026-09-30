package store

import (
	"bytes"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestQualificationBoundaryHashesRequireOneMeasuredSignedObservation(t *testing.T) {
	targetHash := contracts.HashStrings("deployment", "boundary-store-test")
	boundaryHash := contracts.ExternalBoundaryAuthority{
		EgressPolicyHash:      contracts.HashStrings("egress"),
		DestinationPolicyHash: contracts.HashStrings("destination"),
		NetworkBoundary:       contracts.NetworkBoundaryControlledTransport,
		NetworkBoundaryHash:   contracts.HashStrings("network"),
	}.StableHash()
	report := contracts.QualificationReport{
		RunID: "boundary-store-test", WorkspaceID: "workspace-a", TargetHash: targetHash, Outcome: contracts.QualificationOutcomePassed,
		Cases: []contracts.QualificationCase{{Name: "boundary", Category: contracts.QualificationCategoryExternalBoundary, Outcome: contracts.QualificationOutcomePassed}},
		BoundaryEvidence: []contracts.BoundaryQualificationEvidence{{
			ID: "provider-idempotency", Kind: contracts.BoundaryQualificationProviderIdempotency,
			Outcome: contracts.QualificationOutcomePassed, BoundaryHash: boundaryHash, Measured: true,
			ProviderRequestHash: contracts.HashStrings("provider-request"), ProviderResponseHash: contracts.HashStrings("provider-response"),
			ObservedAt: time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC), ExpiresAt: time.Date(2026, 9, 27, 13, 0, 0, 0, time.UTC),
		}},
	}
	if err := report.Normalize(); err != nil {
		t.Fatal(err)
	}
	bundle := contracts.QualificationBundle{
		Report: report,
		Manifest: contracts.QualificationManifest{
			RunID: report.RunID, WorkspaceID: report.WorkspaceID, TargetHash: targetHash, ReportHash: report.ReportHash,
			RunnerVersion: "test", Checks: []contracts.QualificationCheckManifest{{
				Name: "boundary", Version: "1", Category: contracts.QualificationCategoryExternalBoundary,
				ExecutionMode: "external", InputHash: contracts.HashStrings("boundary-input"), Outcome: contracts.QualificationOutcomePassed,
			}},
		},
	}
	if err := bundle.Normalize(); err != nil {
		t.Fatal(err)
	}
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x61}, ed25519.SeedSize))
	signed, err := contracts.SignQualificationBundle(bundle, "deployment-key", private)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 9, 27, 12, 30, 0, 0, time.UTC)
	gotBoundary, gotEvidence, gotExpiry, err := qualificationBoundaryHashes(signed, contracts.DeploymentEvidenceExternalEffect, now)
	if err != nil {
		t.Fatal(err)
	}
	if gotBoundary != boundaryHash || gotEvidence != report.BoundaryEvidenceHash() || gotExpiry == nil || !gotExpiry.Equal(time.Date(2026, 9, 27, 13, 0, 0, 0, time.UTC)) {
		t.Fatalf("derived boundary hashes=%s/%s expiry=%v want=%s/%s", gotBoundary, gotEvidence, gotExpiry, boundaryHash, report.BoundaryEvidenceHash())
	}
	if gotBoundary, gotEvidence, gotExpiry, err := qualificationBoundaryHashes(signed, contracts.DeploymentEvidenceProvider, now); err != nil || gotBoundary != "" || gotEvidence != "" || gotExpiry != nil {
		t.Fatalf("non-external evidence derived boundary=%s/%s err=%v", gotBoundary, gotEvidence, err)
	}
	if _, _, _, err := qualificationBoundaryHashes(signed, contracts.DeploymentEvidenceExternalEffect, time.Date(2026, 9, 27, 13, 0, 0, 0, time.UTC)); err != ErrDeploymentBoundaryEvidence {
		t.Fatalf("expired boundary evidence error=%v, want=%v", err, ErrDeploymentBoundaryEvidence)
	}

	missing := bundle
	missing.Report.BoundaryEvidence = nil
	missing.Report.ReportHash = ""
	if err := missing.Report.Normalize(); err != nil {
		t.Fatal(err)
	}
	missing.Manifest.ReportHash = missing.Report.ReportHash
	missing.Manifest.ManifestHash = ""
	if err := missing.Manifest.Normalize(); err != nil {
		t.Fatal(err)
	}
	missingSigned, err := contracts.SignQualificationBundle(missing, "deployment-key", private)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := qualificationBoundaryHashes(missingSigned, contracts.DeploymentEvidenceExternalEffect, now); err != ErrDeploymentBoundaryEvidence {
		t.Fatalf("missing boundary evidence error=%v, want=%v", err, ErrDeploymentBoundaryEvidence)
	}
}
