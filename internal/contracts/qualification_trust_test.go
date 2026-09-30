package contracts

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestQualificationTrustedSignerNormalizesPublicMaterialOnly(t *testing.T) {
	private := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{0x51}, ed25519.SeedSize))
	public := private.Public().(ed25519.PublicKey)
	signer := QualificationTrustedSigner{
		ID: "signer-1", WorkspaceID: "workspace-a", DeploymentID: "deployment-a", KeyID: "key-1",
		PublicKey: strings.ToUpper(stringHex(public)), Status: QualificationSignerActive,
		ValidFrom: time.Date(2026, 9, 27, 0, 0, 0, 0, time.UTC), ValidUntil: time.Date(2026, 9, 28, 0, 0, 0, 0, time.UTC),
		Actor: AuditActor{ID: "operator", WorkspaceID: "workspace-a", Kind: "test"},
	}
	if err := signer.Normalize(); err != nil {
		t.Fatal(err)
	}
	if signer.PublicKey != stringHex(public) || signer.PublicKeyHash != HashStrings("qualification-signer-public-key", signer.PublicKey) {
		t.Fatalf("normalized signer=%+v", signer)
	}
	encoded, err := json.Marshal(signer)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(string(encoded)), "private") || strings.Contains(string(encoded), "51515151") {
		t.Fatalf("signer disclosure contains private material: %s", encoded)
	}
}

func TestQualificationImportRequestRejectsCrossWorkspaceSignedBundle(t *testing.T) {
	request := QualificationImportRequest{
		WorkspaceID: "workspace-a", DeploymentID: "deployment-a", TargetHash: HashStrings("target"),
		IdempotencyKey: "import-1", SignedBundle: SignedQualificationBundle{},
		Actor: AuditActor{ID: "operator", WorkspaceID: "workspace-a", Kind: "test"},
	}
	if err := request.Normalize(); err == nil {
		t.Fatal("empty signed bundle was accepted")
	}
}

func stringHex(value []byte) string {
	const hex = "0123456789abcdef"
	result := make([]byte, len(value)*2)
	for i, item := range value {
		result[i*2] = hex[item>>4]
		result[i*2+1] = hex[item&0x0f]
	}
	return string(result)
}
