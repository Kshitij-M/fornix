package qualification

import (
	"bytes"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func boundaryPublisherBundle(t *testing.T, count int, expiresAt time.Time) contracts.QualificationBundle {
	t.Helper()
	targetHash := contracts.HashStrings("publisher-target")
	boundaryHash := contracts.ExternalBoundaryAuthority{
		EgressPolicyHash:      contracts.HashStrings("egress"),
		DestinationPolicyHash: contracts.HashStrings("destination"),
		NetworkBoundary:       contracts.NetworkBoundaryDeploymentAttested,
		NetworkBoundaryHash:   contracts.HashStrings("network"),
	}.StableHash()
	evidence := make([]contracts.BoundaryQualificationEvidence, 0, count)
	for index := 0; index < count; index++ {
		evidence = append(evidence, contracts.BoundaryQualificationEvidence{
			ID:                   "provider-" + string(rune('a'+index)),
			Kind:                 contracts.BoundaryQualificationProviderIdempotency,
			Outcome:              contracts.QualificationOutcomePassed,
			BoundaryHash:         boundaryHash,
			ProviderRequestHash:  contracts.HashStrings("request", string(rune('a'+index))),
			ProviderResponseHash: contracts.HashStrings("response", string(rune('a'+index))),
			Measured:             true,
			ObservedAt:           time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC),
			ExpiresAt:            expiresAt,
		})
	}
	report := contracts.QualificationReport{
		RunID: "publisher-run", WorkspaceID: "workspace-a", TargetHash: targetHash,
		Outcome:          contracts.QualificationOutcomePassed,
		Cases:            []contracts.QualificationCase{{Name: "provider-idempotency", Category: contracts.QualificationCategoryExternalBoundary, Outcome: contracts.QualificationOutcomePassed}},
		BoundaryEvidence: evidence,
	}
	if err := report.Normalize(); err != nil {
		t.Fatal(err)
	}
	bundle := contracts.QualificationBundle{
		Report: report,
		Manifest: contracts.QualificationManifest{
			RunID: report.RunID, WorkspaceID: report.WorkspaceID, TargetHash: report.TargetHash,
			ReportHash: report.ReportHash, RunnerVersion: "publisher-test",
			Checks: []contracts.QualificationCheckManifest{{
				Name: "provider-idempotency", Version: "1", Category: contracts.QualificationCategoryExternalBoundary,
				ExecutionMode: "external", InputHash: contracts.HashStrings("publisher-input"), Outcome: contracts.QualificationOutcomePassed,
			}},
		},
	}
	if err := bundle.Normalize(); err != nil {
		t.Fatal(err)
	}
	return bundle
}

func TestPublishBoundaryBundleIsDeterministicAndRedacted(t *testing.T) {
	bundle := boundaryPublisherBundle(t, 1, time.Date(2026, 9, 27, 13, 0, 0, 0, time.UTC))
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x47}, ed25519.SeedSize))
	options := BoundaryPublisherOptions{RequireExternalEffect: true, AsOf: time.Date(2026, 9, 27, 12, 30, 0, 0, time.UTC)}
	first, err := PublishBoundaryBundle(bundle, "deployment-publisher", key, options)
	if err != nil {
		t.Fatal(err)
	}
	second, err := PublishBoundaryBundle(bundle, "deployment-publisher", key, options)
	if err != nil {
		t.Fatal(err)
	}
	if first.Signature.SignedHash != second.Signature.SignedHash || first.ObservationHash != second.ObservationHash {
		t.Fatalf("repeated publication changed identity: first=%s/%s second=%s/%s", first.Signature.SignedHash, first.ObservationHash, second.Signature.SignedHash, second.ObservationHash)
	}
	if err := ValidateBoundaryBundle(first, options); err != nil {
		t.Fatal(err)
	}
	summary, err := Summary(first, true)
	if err != nil {
		t.Fatal(err)
	}
	if summary.EvidenceCount != 1 || summary.ExpiresAtRFC3339 == "" {
		t.Fatalf("unexpected publisher summary: %+v", summary)
	}
	if bytes.Contains([]byte(summary.SignedHash), []byte("deployment-publisher")) {
		t.Fatal("publisher identity leaked into hash output")
	}
}

func TestPublishBoundaryBundleRejectsAmbiguousAndExpiredExternalEvidence(t *testing.T) {
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x48}, ed25519.SeedSize))
	asOf := time.Date(2026, 9, 27, 12, 30, 0, 0, time.UTC)
	if _, err := PublishBoundaryBundle(boundaryPublisherBundle(t, 2, asOf.Add(time.Hour)), "publisher", key, BoundaryPublisherOptions{RequireExternalEffect: true, AsOf: asOf}); err == nil {
		t.Fatal("ambiguous external boundary evidence was accepted")
	}
	if _, err := PublishBoundaryBundle(boundaryPublisherBundle(t, 1, asOf), "publisher", key, BoundaryPublisherOptions{RequireExternalEffect: true, AsOf: asOf}); err == nil {
		t.Fatal("expired external boundary evidence was accepted")
	}
	if _, err := PublishBoundaryBundle(boundaryPublisherBundle(t, 1, asOf.Add(time.Hour)), "publisher", key, BoundaryPublisherOptions{RequireExternalEffect: true}); err == nil {
		t.Fatal("implicit as_of was accepted by the library")
	}
}

func TestValidateBoundaryBundleRejectsTampering(t *testing.T) {
	bundle := boundaryPublisherBundle(t, 1, time.Date(2026, 9, 27, 13, 0, 0, 0, time.UTC))
	key := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x49}, ed25519.SeedSize))
	signed, err := PublishBoundaryBundle(bundle, "publisher", key, BoundaryPublisherOptions{RequireExternalEffect: true, AsOf: time.Date(2026, 9, 27, 12, 30, 0, 0, time.UTC)})
	if err != nil {
		t.Fatal(err)
	}
	signed.Bundle.Report.BoundaryEvidence[0].ProviderResponseHash = contracts.HashStrings("tampered")
	if err := ValidateBoundaryBundle(signed, BoundaryPublisherOptions{RequireExternalEffect: true, AsOf: time.Date(2026, 9, 27, 12, 30, 0, 0, time.UTC)}); err == nil {
		t.Fatal("tampered boundary bundle was accepted")
	}
}
