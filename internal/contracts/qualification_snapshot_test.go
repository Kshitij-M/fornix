package contracts

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"testing"
	"time"
)

func TestQualificationTrustSnapshotNormalizesAndSignsDeterministically(t *testing.T) {
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x61}, ed25519.SeedSize))
	validFrom := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	validUntil := validFrom.Add(24 * time.Hour)
	base := QualificationTrustSnapshot{
		WorkspaceID: "workspace-a", DeploymentID: "deployment-a", Revision: 7,
		IssuedAt: validFrom, ExpiresAt: validUntil,
		Entries: []QualificationTrustSnapshotEntry{
			{DeploymentID: "deployment-a", KeyID: "key-b", PublicKey: stringHex(private.Public().(ed25519.PublicKey)), ValidFrom: validFrom, ValidUntil: validUntil},
			{DeploymentID: "deployment-a", KeyID: "key-a", PublicKey: stringHex(private.Public().(ed25519.PublicKey)), ValidFrom: validFrom, ValidUntil: validUntil},
		},
	}
	first, err := SignQualificationTrustSnapshot(base, "publisher", private)
	if err != nil {
		t.Fatal(err)
	}
	base.Entries[0], base.Entries[1] = base.Entries[1], base.Entries[0]
	second, err := SignQualificationTrustSnapshot(base, "publisher", private)
	if err != nil {
		t.Fatal(err)
	}
	if first.SnapshotHash != second.SnapshotHash || first.Signature != second.Signature {
		t.Fatalf("entry ordering changed signed identity: first=%+v second=%+v", first, second)
	}
	if err := first.VerifyWithKey("publisher", private.Public().(ed25519.PublicKey), validFrom); err != nil {
		t.Fatalf("verify at validity boundary: %v", err)
	}
	if !first.AuthorizesSigner("key-a", first.Entries[0].PublicKey, validFrom) {
		t.Fatal("snapshot did not authorize signer at valid_from boundary")
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(bytes.ToLower(encoded), []byte("private_key")) {
		t.Fatalf("snapshot contains private-key-shaped disclosure: %s", encoded)
	}
}

func TestQualificationTrustSnapshotRejectsWrongPublisherAndInvalidWindow(t *testing.T) {
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x62}, ed25519.SeedSize))
	other := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x63}, ed25519.SeedSize))
	from := time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC)
	snapshot, err := SignQualificationTrustSnapshot(QualificationTrustSnapshot{
		WorkspaceID: "workspace-a", DeploymentID: "deployment-a", Revision: 1,
		IssuedAt: from, ExpiresAt: from.Add(time.Hour), Entries: []QualificationTrustSnapshotEntry{{
			DeploymentID: "deployment-a", KeyID: "key-a", PublicKey: stringHex(private.Public().(ed25519.PublicKey)), ValidFrom: from, ValidUntil: from.Add(time.Hour),
		}},
	}, "publisher", private)
	if err != nil {
		t.Fatal(err)
	}
	if err := snapshot.VerifyWithKey("publisher", other.Public().(ed25519.PublicKey), from); err == nil {
		t.Fatal("wrong publisher key was accepted")
	}
	if snapshot.AuthorizesSigner("key-a", snapshot.Entries[0].PublicKey, from.Add(time.Hour)) {
		t.Fatal("expired snapshot entry was accepted")
	}
	if _, err := SignQualificationTrustSnapshot(snapshot, "publisher", nil); err == nil {
		t.Fatal("missing private key produced a signed snapshot")
	}
}
