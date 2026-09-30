package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/qualification"
)

func TestReadQualificationReportVerifiesStableHashAndRejectsUnknownFields(t *testing.T) {
	targetHash := contracts.HashStrings("deployment", "test")
	report := contracts.QualificationReport{
		RunID: "qualification-run", TargetHash: targetHash, Outcome: contracts.QualificationOutcomePassed,
		Cases: []contracts.QualificationCase{{Name: "adapter", Category: contracts.QualificationCategoryAdapter, Outcome: contracts.QualificationOutcomePassed}},
	}
	if err := report.Normalize(); err != nil {
		t.Fatal(err)
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "qualification.json")
	encoded, err := json.Marshal(report)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	loaded, err := readQualificationReport(path)
	if err != nil || loaded.ReportHash != report.ReportHash {
		t.Fatalf("loaded report=%+v err=%v", loaded, err)
	}
	unknownPath := filepath.Join(directory, "unknown.json")
	if err := os.WriteFile(unknownPath, append(encoded[:len(encoded)-1], []byte(`,"secret":"must-not-be-accepted"}`)...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readQualificationReport(unknownPath); err == nil {
		t.Fatal("unknown qualification field was accepted")
	}
}

func TestQualificationBundleRoundTripIsAtomicAndValidated(t *testing.T) {
	result, err := qualification.Run(context.Background(), "cli-run", "workspace", contracts.HashStrings("target"), qualification.OfflineChecks(), qualification.RunnerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "bundle.json")
	if err := writeQualificationBundle(path, result.Bundle); err != nil {
		t.Fatal(err)
	}
	loaded, err := readQualificationBundle(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Report.ReportHash != result.Bundle.Report.ReportHash || loaded.Manifest.ManifestHash != result.Bundle.Manifest.ManifestHash {
		t.Fatalf("bundle changed on round trip: loaded=%+v result=%+v", loaded, result.Bundle)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("bundle permissions=%o want 600", info.Mode().Perm())
	}
}

func TestSignedQualificationBundleRoundTripIsAtomicAndSecretSafe(t *testing.T) {
	result, err := qualification.Run(context.Background(), "cli-signed-run", "workspace", contracts.HashStrings("target"), qualification.OfflineChecks(), qualification.RunnerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x42}, ed25519.SeedSize))
	signed, err := contracts.SignQualificationBundle(result.Bundle, "deployment-key-v1", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "signed.json")
	if err := writeSignedQualificationBundle(path, signed); err != nil {
		t.Fatal(err)
	}
	loaded, err := readSignedQualificationBundle(path)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Signature.SignedHash != signed.Signature.SignedHash || loaded.ObservationHash != signed.ObservationHash {
		t.Fatalf("signed bundle changed on round trip: loaded=%+v signed=%+v", loaded, signed)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("signed bundle permissions=%o want 600", info.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, privateKey.Seed()) || bytes.Contains(bytes.ToLower(raw), []byte("private_key")) {
		t.Fatalf("signed bundle contains private-key material or field: %s", raw)
	}
	unknownPath := filepath.Join(t.TempDir(), "unknown.json")
	if err := os.WriteFile(unknownPath, append(raw[:len(raw)-1], []byte(`,"private_key":"must-not-be-accepted"}`)...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readSignedQualificationBundle(unknownPath); err == nil {
		t.Fatal("private-key-shaped unknown field was accepted")
	}
}

func TestQualificationKeyReadersAcceptBoundedRawAndHexForms(t *testing.T) {
	directory := t.TempDir()
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x24}, ed25519.SeedSize))
	rawPrivate := filepath.Join(directory, "private.raw")
	if err := os.WriteFile(rawPrivate, privateKey, 0o600); err != nil {
		t.Fatal(err)
	}
	loadedPrivate, err := readQualificationPrivateKey(rawPrivate)
	if err != nil || !bytes.Equal(loadedPrivate, privateKey) {
		t.Fatalf("raw private key mismatch: err=%v", err)
	}
	rawPublic := filepath.Join(directory, "public.hex")
	if err := os.WriteFile(rawPublic, []byte(hex.EncodeToString(privateKey.Public().(ed25519.PublicKey))), 0o600); err != nil {
		t.Fatal(err)
	}
	loadedPublic, err := readQualificationPublicKey(rawPublic)
	if err != nil || !bytes.Equal(loadedPublic, privateKey.Public().(ed25519.PublicKey)) {
		t.Fatalf("hex public key mismatch: err=%v", err)
	}
}

func TestBoundaryQualificationOutputRequiresExplicitReplacement(t *testing.T) {
	path := filepath.Join(t.TempDir(), "signed-boundary.json")
	if err := os.WriteFile(path, []byte("existing"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := refuseExistingQualificationOutput(path, false); err == nil {
		t.Fatal("boundary publisher implicitly accepted replacing an existing output")
	}
	if err := refuseExistingQualificationOutput(path, true); err != nil {
		t.Fatalf("explicit replacement was rejected: %v", err)
	}
}

func TestQualificationTrustSnapshotRoundTripIsAtomicAndSecretSafe(t *testing.T) {
	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x25}, ed25519.SeedSize))
	now := time.Now().UTC()
	snapshot, err := contracts.SignQualificationTrustSnapshot(contracts.QualificationTrustSnapshot{
		WorkspaceID: "workspace", DeploymentID: "deployment", Revision: 1,
		IssuedAt: now.Add(-time.Hour), ExpiresAt: now.Add(time.Hour), Entries: []contracts.QualificationTrustSnapshotEntry{{
			DeploymentID: "deployment", KeyID: "qualification-key", PublicKey: hex.EncodeToString(privateKey.Public().(ed25519.PublicKey)), ValidFrom: now.Add(-time.Hour), ValidUntil: now.Add(time.Hour),
		}},
	}, "publisher-key", privateKey)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "snapshot.json")
	if err := writeQualificationTrustSnapshot(path, snapshot); err != nil {
		t.Fatal(err)
	}
	loaded, raw, err := readQualificationTrustSnapshot(path)
	if err != nil || loaded.SnapshotHash != snapshot.SnapshotHash || len(raw) == 0 {
		t.Fatalf("snapshot round trip loaded=%+v err=%v", loaded, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("snapshot permissions=%o want 600", info.Mode().Perm())
	}
	if bytes.Contains(raw, privateKey.Seed()) || bytes.Contains(bytes.ToLower(raw), []byte("private_key")) {
		t.Fatalf("snapshot contains private-key material: %s", raw)
	}
}

func TestReadQualificationRefreshItemsRejectsUnknownFieldsAndBounds(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "items.json")
	if err := os.WriteFile(path, []byte(`{"items":[{"kind":"provider","import_id":"import-1"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	items, err := readQualificationRefreshItems(path)
	if err != nil || len(items) != 1 || items[0].Kind != contracts.DeploymentEvidenceProvider {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	unknownPath := filepath.Join(directory, "unknown.json")
	if err := os.WriteFile(unknownPath, []byte(`{"items":[{"kind":"provider","import_id":"import-1","secret":"no"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readQualificationRefreshItems(unknownPath); err == nil {
		t.Fatal("unknown refresh item field was accepted")
	}
}

func TestReadQualificationRefreshScheduleRejectsUnknownFieldsAndNormalizes(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "schedule.json")
	contents := `{"deployment_id":"deployment","release_id":"release","required_evidence_kinds":["provider"],"first_due_at":"2026-09-27T12:00:00Z"}`
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	request, err := readQualificationRefreshScheduleRequest(path)
	if err != nil || request.DeploymentID != "deployment" || len(request.RequiredEvidenceKinds) != 1 {
		t.Fatalf("request=%+v err=%v", request, err)
	}
	unknownPath := filepath.Join(directory, "unknown-schedule.json")
	if err := os.WriteFile(unknownPath, []byte(`{"deployment_id":"deployment","release_id":"release","required_evidence_kinds":["provider"],"first_due_at":"2026-09-27T12:00:00Z","credential":"no"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readQualificationRefreshScheduleRequest(unknownPath); err == nil {
		t.Fatal("unknown schedule field was accepted")
	}
}
