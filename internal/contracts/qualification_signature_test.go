package contracts

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func signedQualificationFixture(t *testing.T) (QualificationBundle, ed25519.PrivateKey) {
	t.Helper()
	targetHash := HashStrings("deployment", "signed-fixture")
	report := QualificationReport{
		RunID:       "signed-qualification",
		WorkspaceID: "workspace-a",
		TargetHash:  targetHash,
		Outcome:     QualificationOutcomePassed,
		Cases: []QualificationCase{{
			Name:         "authority",
			Category:     QualificationCategoryAuthority,
			Outcome:      QualificationOutcomePassed,
			EvidenceHash: HashStrings("authority-evidence"),
			Measurements: []QualificationMeasurement{{Name: "effect_count", Value: 1, Unit: "count", Present: true}},
		}},
		RecoveryDrills: []RecoveryDrill{{
			ID:                 "restore-1",
			Kind:               RecoveryDrillBackupRestore,
			Outcome:            QualificationOutcomePassed,
			TargetHash:         targetHash,
			EvidenceHash:       HashStrings("restore-evidence"),
			SourceFingerprint:  HashStrings("source"),
			RestoreFingerprint: HashStrings("restore"),
			ObservedAt:         time.Date(2026, 9, 27, 10, 0, 0, 0, time.UTC),
		}},
	}
	if err := report.Normalize(); err != nil {
		t.Fatal(err)
	}
	bundle := QualificationBundle{
		Report: report,
		Manifest: QualificationManifest{
			RunID:         report.RunID,
			WorkspaceID:   report.WorkspaceID,
			TargetHash:    report.TargetHash,
			ReportHash:    report.ReportHash,
			RunnerVersion: "1",
			CommitHash:    HashStrings("commit"),
			EnvironmentNames: []string{
				"FORNIX_MODE",
				"FORNIX_VERSION",
			},
			Checks: []QualificationCheckManifest{{
				Name:          "authority",
				Version:       "1",
				Category:      QualificationCategoryAuthority,
				ExecutionMode: "offline",
				InputHash:     HashStrings("authority-input"),
				Outcome:       QualificationOutcomePassed,
				EvidenceHash:  HashStrings("authority-evidence"),
			}},
		},
	}
	if err := bundle.Normalize(); err != nil {
		t.Fatal(err)
	}
	seed := bytes.Repeat([]byte{0x42}, ed25519.SeedSize)
	return bundle, ed25519.NewKeyFromSeed(seed)
}

func TestQualificationSignatureIsDeterministicAndBoundToEvidence(t *testing.T) {
	bundle, privateKey := signedQualificationFixture(t)
	first, err := SignQualificationBundle(bundle, "deployment-key-v1", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	second, err := SignQualificationBundle(bundle, "deployment-key-v1", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	if first.Signature.SignedHash != second.Signature.SignedHash || first.Signature.Signature != second.Signature.Signature || first.ObservationHash != second.ObservationHash {
		t.Fatalf("same bundle received different signature identity: first=%+v second=%+v", first, second)
	}
	if err := first.Verify(); err != nil {
		t.Fatal(err)
	}
	if subject, err := first.SigningSubjectHash(); err != nil || subject != first.Signature.SignedHash {
		t.Fatalf("subject mismatch: subject=%s err=%v signed=%s", subject, err, first.Signature.SignedHash)
	}

	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "42424242") || strings.Contains(strings.ToLower(string(encoded)), "private_key") {
		t.Fatalf("signed evidence contains private-key material or field: %s", encoded)
	}

	tampered := first
	tampered.Bundle.Report.TargetHash = HashStrings("different-target")
	if err := tampered.Verify(); err == nil {
		t.Fatal("target tampering was accepted")
	}
	tampered = first
	tampered.Bundle.Manifest.EnvironmentNames[0] = "FORNIX_OTHER"
	if err := tampered.Verify(); err == nil {
		t.Fatal("environment-name tampering was accepted")
	}
	tampered = first
	tampered.Signature.Signature = strings.Repeat("0", ed25519.SignatureSize*2)
	if err := tampered.Verify(); err == nil {
		t.Fatal("signature tampering was accepted")
	}
}

func TestQualificationSignatureTrustBindingAndMalformedValuesFailClosed(t *testing.T) {
	bundle, privateKey := signedQualificationFixture(t)
	signed, err := SignQualificationBundle(bundle, "deployment-key-v1", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	publicKey := privateKey.Public().(ed25519.PublicKey)
	if err := signed.VerifyWithKey("other-key", publicKey); err == nil {
		t.Fatal("wrong key id was accepted")
	}
	wrongPublic := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x24}, ed25519.SeedSize)).Public().(ed25519.PublicKey)
	if err := signed.VerifyWithKey("deployment-key-v1", wrongPublic); err == nil {
		t.Fatal("wrong public key was accepted")
	}
	malformed := signed
	malformed.Signature.Algorithm = "rsa"
	if err := malformed.Verify(); err == nil {
		t.Fatal("unsupported signature algorithm was accepted")
	}
}
