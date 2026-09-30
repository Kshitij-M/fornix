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

func TestTrustPolicyIsOrderIndependentAndPinsDefinitionHashes(t *testing.T) {
	adapter, err := fakeincident.NewConnector("workspace-trust")
	if err != nil {
		t.Fatal(err)
	}
	caps := adapter.Capabilities()
	definitions := make([]contracts.CapabilityDefinition, 0, len(caps))
	for _, capability := range caps {
		definitions = append(definitions, capability.Definition())
	}
	first, err := connector.NewTrustPolicy("workspace-trust", "revision-1", definitions)
	if err != nil {
		t.Fatal(err)
	}
	for left, right := 0, len(definitions)-1; left < right; left, right = left+1, right-1 {
		definitions[left], definitions[right] = definitions[right], definitions[left]
	}
	second, err := connector.NewTrustPolicy("workspace-trust", "revision-1", definitions)
	if err != nil {
		t.Fatal(err)
	}
	if first.StableHash() != second.StableHash() {
		t.Fatalf("trust hash depends on registration order: %s != %s", first.StableHash(), second.StableHash())
	}
	if err := first.Authorize(caps[0].Definition()); err != nil {
		t.Fatal(err)
	}
	altered := caps[0].Definition()
	altered.Ref.DefinitionHash = contracts.HashStrings("altered-definition")
	if first.Authorize(altered) == nil {
		t.Fatal("altered capability definition was trusted")
	}
	foreign := caps[0].Definition()
	foreign.WorkspaceID = "workspace-other"
	foreign.Ref.WorkspaceID = foreign.WorkspaceID
	foreign.Ref.Connector.WorkspaceID = foreign.WorkspaceID
	if first.Authorize(foreign) == nil {
		t.Fatal("cross-workspace capability was trusted")
	}
}

func TestSignedTrustPolicyVerifiesAndRejectsTamperAndDowngrade(t *testing.T) {
	adapter, err := fakeincident.NewConnector("workspace-signed-trust")
	if err != nil {
		t.Fatal(err)
	}
	definitions := make([]contracts.CapabilityDefinition, 0, len(adapter.Capabilities()))
	for _, capability := range adapter.Capabilities() {
		definitions = append(definitions, capability.Definition())
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	policy, err := connector.NewTrustPolicy("workspace-signed-trust", "1", definitions)
	if err != nil {
		t.Fatal(err)
	}
	if err := policy.Sign("release-key-1", privateKey, now.Add(-time.Second), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := policy.Verify(map[string]ed25519.PublicKey{"release-key-1": publicKey}, now); err != nil {
		t.Fatalf("verify signed policy: %v", err)
	}
	if err := policy.Verify(map[string]ed25519.PublicKey{"other-key": publicKey}, now); !errors.Is(err, connector.ErrTrustSignature) {
		t.Fatalf("unknown signer error=%v, want signature error", err)
	}
	if err := policy.Verify(map[string]ed25519.PublicKey{"release-key-1": publicKey}, now.Add(2*time.Hour)); !errors.Is(err, connector.ErrTrustExpired) {
		t.Fatalf("expired policy error=%v, want expired", err)
	}
	tampered := policy
	tampered.Entries = append([]connector.TrustEntry(nil), policy.Entries...)
	tampered.Entries[0].CapabilityHash = contracts.HashStrings("tampered")
	if err := tampered.Verify(map[string]ed25519.PublicKey{"release-key-1": publicKey}, now); !errors.Is(err, connector.ErrTrustSignature) {
		t.Fatalf("tampered policy error=%v, want signature error", err)
	}

	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if err := registry.SetTrustSigner("release-key-1", publicKey); err != nil {
		t.Fatal(err)
	}
	registry.RequireTrustPolicy(true)
	registry.RequireSignedTrustPolicy(true)
	if err := registry.SetSignedTrustPolicy(policy, now); err != nil {
		t.Fatalf("install signed policy: %v", err)
	}
	capability, ok := registry.LookupIdentity("workspace-signed-trust", fakeincident.ConnectorName, fakeincident.ConnectorVersion, "incident.read", "1")
	if !ok {
		t.Fatal("signed capability was not discoverable")
	}
	if _, err := registry.Admit(context.Background(), trustRequest(capability.Definition()), connector.AdmissionOptions{}); err != nil {
		t.Fatalf("signed policy rejected trusted capability: %v", err)
	}
	newPolicy, err := connector.NewTrustPolicy("workspace-signed-trust", "2", definitions)
	if err != nil {
		t.Fatal(err)
	}
	if err := newPolicy.Sign("release-key-1", privateKey, now.Add(-time.Second), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := registry.SetSignedTrustPolicy(newPolicy, now); err != nil {
		t.Fatalf("install newer signed policy: %v", err)
	}
	if err := registry.SetSignedTrustPolicy(policy, now); !errors.Is(err, connector.ErrTrustDowngrade) {
		t.Fatalf("downgrade error=%v, want downgrade", err)
	}
}

func TestSignedTrustPolicyTimestampSurvivesPostgresPrecisionRoundTrip(t *testing.T) {
	adapter, err := fakeincident.NewConnector("workspace-signed-trust-timestamp")
	if err != nil {
		t.Fatal(err)
	}
	definitions := make([]contracts.CapabilityDefinition, 0, len(adapter.Capabilities()))
	for _, capability := range adapter.Capabilities() {
		definitions = append(definitions, capability.Definition())
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := connector.NewTrustPolicy("workspace-signed-trust-timestamp", "1", definitions)
	if err != nil {
		t.Fatal(err)
	}
	issuedAt := time.Date(2026, time.January, 2, 3, 4, 5, 123456789, time.UTC)
	expiresAt := issuedAt.Add(time.Hour)
	if err := policy.Sign("release-key", privateKey, issuedAt, expiresAt); err != nil {
		t.Fatal(err)
	}
	if policy.IssuedAt.Nanosecond()%1000 != 0 || policy.ExpiresAt.Nanosecond()%1000 != 0 {
		t.Fatal("signed trust timestamps were not normalized to database precision")
	}
	loaded := policy
	loaded.IssuedAt = loaded.IssuedAt.Truncate(time.Microsecond)
	loaded.ExpiresAt = loaded.ExpiresAt.Truncate(time.Microsecond)
	if err := loaded.Verify(map[string]ed25519.PublicKey{"release-key": publicKey}, issuedAt.Add(time.Minute)); err != nil {
		t.Fatalf("verify trust policy after Postgres timestamp round trip: %v", err)
	}
}

func TestRegistryTrustPolicyRejectsNewUntrustedRegistration(t *testing.T) {
	adapter, err := fakeincident.NewConnector("workspace-trust-registry")
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if err := registry.TrustWorkspace("workspace-trust-registry", "revision-1"); err != nil {
		t.Fatal(err)
	}
	registry.RequireTrustPolicy(true)
	capability, ok := registry.LookupIdentity("workspace-trust-registry", fakeincident.ConnectorName, fakeincident.ConnectorVersion, "incident.read", "1")
	if !ok {
		t.Fatal("trusted capability was not discoverable")
	}
	request := trustRequest(capability.Definition())
	if _, err := registry.Admit(context.Background(), request, connector.AdmissionOptions{}); err != nil {
		t.Fatalf("trusted capability was rejected: %v", err)
	}
	// A second version can be registered for discovery, but it must not become
	// executable until the trust snapshot is explicitly replaced.
	second, err := fakeincident.NewConnector("workspace-trust-registry-2")
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(second); err != nil {
		t.Fatal(err)
	}
	foreignRequest := trustRequest(second.Capabilities()[0].Definition())
	if _, err := registry.Admit(context.Background(), foreignRequest, connector.AdmissionOptions{}); !errors.Is(err, connector.ErrTrustPolicyMissing) {
		t.Fatalf("untrusted workspace capability error=%v", err)
	}
}

func trustRequest(definition contracts.CapabilityDefinition) contracts.OperationRequest {
	hash := contracts.HashStrings("trust-input")
	return contracts.OperationRequest{
		ID: "trust-operation", RequestID: "trust-request", IdempotencyKey: "trust-key", WorkspaceID: definition.WorkspaceID,
		Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: definition.WorkspaceID}, Capability: definition.Ref,
		Target:    contracts.ResourceRef{WorkspaceID: definition.WorkspaceID, System: contracts.SystemRef{WorkspaceID: definition.WorkspaceID, Type: "monitoring", ID: "monitor", Version: "1"}, Kind: definition.ResourceKinds[0], ID: "resource-1", Version: "1"},
		InputType: "incident.workflow.step", InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: hash, Profile: definition.Profile,
	}
}
