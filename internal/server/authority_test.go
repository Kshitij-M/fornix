package server

import (
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/adapters/fakeincident"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

func TestInstallSignedAuthorityPublishesOneMonotonicGeneration(t *testing.T) {
	const workspace = "workspace-authority-generation"
	adapter, err := fakeincident.NewConnector(workspace)
	if err != nil {
		t.Fatal(err)
	}
	definitions := make([]contracts.CapabilityDefinition, 0, len(adapter.Capabilities()))
	for _, capability := range adapter.Capabilities() {
		definitions = append(definitions, capability.Definition())
	}
	policy, err := connector.NewTrustPolicy(workspace, "1", definitions)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := connector.NewSchemaCatalog(workspace, "1", definitions)
	if err != nil {
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Add(-time.Minute)
	if err := policy.Sign("release-1", privateKey, now, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Sign("release-1", privateKey, now, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if err := installSignedAuthority(registry, workspace, policy, privateKey.Public().(ed25519.PublicKey), catalog, privateKey.Public().(ed25519.PublicKey), time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if err := installSignedAuthority(registry, workspace, policy, privateKey.Public().(ed25519.PublicKey), catalog, privateKey.Public().(ed25519.PublicKey), time.Now().UTC()); err != nil {
		t.Fatalf("idempotent generation reload: %v", err)
	}
	if err := registry.ValidateEffectAuthorityConformance(workspace); err != nil {
		t.Fatal(err)
	}
	if current, ok := registry.TrustPolicy(workspace); !ok || current.Signature == "" || current.Revision != "1" {
		t.Fatalf("signed policy was not installed: %+v present=%v", current, ok)
	}
	if current, ok := registry.SchemaCatalog(workspace); !ok || current.Signature == "" || current.Revision != "1" {
		t.Fatalf("signed schema catalog was not installed: %+v present=%v", current, ok)
	}
	older, err := connector.NewTrustPolicy(workspace, "0", definitions)
	if err != nil {
		t.Fatal(err)
	}
	if err := older.Sign("release-1", privateKey, now, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := installSignedAuthority(registry, workspace, older, privateKey.Public().(ed25519.PublicKey), catalog, privateKey.Public().(ed25519.PublicKey), time.Now().UTC()); !errors.Is(err, connector.ErrTrustDowngrade) {
		t.Fatalf("policy downgrade error=%v, want ErrTrustDowngrade", err)
	}
}
