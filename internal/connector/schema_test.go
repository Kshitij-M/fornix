package connector_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/adapters/fakeincident"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

func TestSchemaCatalogIsDeterministicAndSigned(t *testing.T) {
	workspaceID := "workspace-schema-deterministic"
	adapter, err := fakeincident.NewConnector(workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	definitions := make([]contracts.CapabilityDefinition, 0, len(adapter.Capabilities()))
	for _, capability := range adapter.Capabilities() {
		definitions = append(definitions, capability.Definition())
	}
	first, err := connector.NewSchemaCatalog(workspaceID, "1", definitions)
	if err != nil {
		t.Fatal(err)
	}
	for left, right := 0, len(definitions)-1; left < right; left, right = left+1, right-1 {
		definitions[left], definitions[right] = definitions[right], definitions[left]
	}
	second, err := connector.NewSchemaCatalog(workspaceID, "1", definitions)
	if err != nil {
		t.Fatal(err)
	}
	if first.StableHash() != second.StableHash() {
		t.Fatalf("catalog hash depends on definition order: %s != %s", first.StableHash(), second.StableHash())
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := first.Sign("schema-key-1", privateKey, now.Add(-time.Second), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := first.Verify(map[string]ed25519.PublicKey{"schema-key-1": publicKey}, now); err != nil {
		t.Fatalf("verify catalog: %v", err)
	}
	tampered := first
	tampered.Entries = append([]connector.SchemaEntry(nil), first.Entries...)
	tampered.Entries[0].InputSchemaHash = contracts.HashStrings("tampered-input")
	if err := tampered.Verify(map[string]ed25519.PublicKey{"schema-key-1": publicKey}, now); !errors.Is(err, connector.ErrSchemaSignature) {
		t.Fatalf("tampered catalog error=%v, want signature error", err)
	}
	if err := first.Verify(map[string]ed25519.PublicKey{"schema-key-1": publicKey}, now.Add(2*time.Hour)); !errors.Is(err, connector.ErrSchemaExpired) {
		t.Fatalf("expired catalog error=%v, want expired", err)
	}
}

func TestSignedSchemaCatalogTimestampSurvivesPostgresPrecisionRoundTrip(t *testing.T) {
	const workspaceID = "workspace-schema-timestamp"
	adapter, err := fakeincident.NewConnector(workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	definitions := make([]contracts.CapabilityDefinition, 0, len(adapter.Capabilities()))
	for _, capability := range adapter.Capabilities() {
		definitions = append(definitions, capability.Definition())
	}
	catalog, err := connector.NewSchemaCatalog(workspaceID, "1", definitions)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	issuedAt := time.Date(2026, time.January, 2, 3, 4, 5, 123456789, time.UTC)
	expiresAt := issuedAt.Add(time.Hour)
	if err := catalog.Sign("schema-key", privateKey, issuedAt, expiresAt); err != nil {
		t.Fatal(err)
	}
	if catalog.IssuedAt.Nanosecond()%1000 != 0 || catalog.ExpiresAt.Nanosecond()%1000 != 0 {
		t.Fatal("signed schema timestamps were not normalized to database precision")
	}
	loaded := catalog
	loaded.IssuedAt = loaded.IssuedAt.Truncate(time.Microsecond)
	loaded.ExpiresAt = loaded.ExpiresAt.Truncate(time.Microsecond)
	if err := loaded.Verify(map[string]ed25519.PublicKey{"schema-key": publicKey}, issuedAt.Add(time.Minute)); err != nil {
		t.Fatalf("verify schema catalog after Postgres timestamp round trip: %v", err)
	}
}

func TestSignedSchemaCatalogBindsAdmissionToExactSchemas(t *testing.T) {
	workspaceID := "workspace-schema-admission"
	adapter, err := fakeincident.NewConnector(workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	definition := adapter.Capabilities()[0].Definition()
	catalog, err := connector.NewSchemaCatalog(workspaceID, "1", []contracts.CapabilityDefinition{definition})
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := catalog.Sign("schema-key-1", privateKey, now.Add(-time.Second), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := registry.SetTrustSigner("schema-key-1", publicKey); err != nil {
		t.Fatal(err)
	}
	if err := registry.SetSignedSchemaCatalog(catalog, now); err != nil {
		t.Fatal(err)
	}
	registry.RequireSignedSchemaCatalog(true)
	request := trustRequest(definition)
	admission, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{})
	if err != nil {
		t.Fatalf("matching schema admission failed: %v", err)
	}
	if admission.SchemaCatalogHash != catalog.CatalogHash || admission.SchemaCatalogRevision != catalog.Revision {
		t.Fatalf("admission lost schema catalog identity: hash=%q revision=%q", admission.SchemaCatalogHash, admission.SchemaCatalogRevision)
	}
	mismatch := request
	mismatch.InputSchemaHash = contracts.HashStrings("different-input")
	if _, err := registry.Admit(context.Background(), mismatch, connector.AdmissionOptions{}); !errors.Is(err, connector.ErrSchemaMismatch) {
		t.Fatalf("mismatched input schema error=%v, want schema mismatch", err)
	}

	badOutput := catalog
	badOutput.Revision = "2"
	badOutput.CatalogHash = ""
	badOutput.Signature = ""
	badOutput.Entries = append([]connector.SchemaEntry(nil), catalog.Entries...)
	badOutput.Entries[0].OutputSchemaHash = contracts.HashStrings("different-output")
	if err := badOutput.Normalize(); err != nil {
		t.Fatal(err)
	}
	if err := badOutput.Sign("schema-key-1", privateKey, now.Add(-time.Second), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := registry.SetSignedSchemaCatalog(badOutput, now); err != nil {
		t.Fatal(err)
	}
	if _, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{}); !errors.Is(err, connector.ErrSchemaMismatch) {
		t.Fatalf("mismatched output schema error=%v, want schema mismatch", err)
	}
	registry.RevokeTrustSigner("schema-key-1")
	if _, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{}); !errors.Is(err, connector.ErrSchemaSignature) {
		t.Fatalf("revoked schema signer admission error=%v, want signature error", err)
	}
}

func TestSignedSchemaCatalogInstallationIsMonotonicAndDuplicateSafe(t *testing.T) {
	workspaceID := "workspace-schema-monotonic"
	adapter, err := fakeincident.NewConnector(workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	definition := adapter.Capabilities()[0].Definition()
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	newCatalog := func(revision string) connector.SchemaCatalog {
		catalog, catalogErr := connector.NewSchemaCatalog(workspaceID, revision, []contracts.CapabilityDefinition{definition})
		if catalogErr != nil {
			t.Fatal(catalogErr)
		}
		if catalogErr := catalog.Sign("schema-key-1", privateKey, now.Add(-time.Second), now.Add(time.Hour)); catalogErr != nil {
			t.Fatal(catalogErr)
		}
		return catalog
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if err := registry.SetTrustSigner("schema-key-1", publicKey); err != nil {
		t.Fatal(err)
	}
	first := newCatalog("1")
	if err := registry.SetSignedSchemaCatalog(first, now); err != nil {
		t.Fatal(err)
	}
	if err := registry.SetSignedSchemaCatalog(first, now); err != nil {
		t.Fatalf("same signed catalog should be idempotent: %v", err)
	}
	if err := registry.SetSignedSchemaCatalog(newCatalog("0"), now); !errors.Is(err, connector.ErrSchemaDowngrade) {
		t.Fatalf("downgrade error=%v, want downgrade", err)
	}
	unsigned := first
	unsigned.Signature = ""
	if err := registry.SetSchemaCatalog(unsigned); !errors.Is(err, connector.ErrSchemaSignature) {
		t.Fatalf("unsigned replacement error=%v, want signature error", err)
	}
}
