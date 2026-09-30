package contracts

import (
	"encoding/json"
	"strings"
	"testing"
)

func testExternalBoundary() ExternalBoundaryAuthority {
	return ExternalBoundaryAuthority{
		SchemaVersion:         ExternalBoundarySchemaVersion,
		EgressPolicyHash:      HashStrings("egress-policy"),
		DestinationPolicyHash: HashStrings("destination-policy"),
		NetworkBoundary:       NetworkBoundaryControlledTransport,
		NetworkBoundaryHash:   HashStrings("network-boundary"),
	}
}

func TestExternalBoundaryIsDeterministicAndHashOnly(t *testing.T) {
	first := testExternalBoundary()
	second := first
	if err := first.Normalize(); err != nil {
		t.Fatal(err)
	}
	if err := second.Normalize(); err != nil {
		t.Fatal(err)
	}
	if first.StableHash() == "" || first.StableHash() != second.StableHash() {
		t.Fatalf("boundary hash is not stable: %q != %q", first.StableHash(), second.StableHash())
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"api_key", "authorization", "password", "secret", "https://"} {
		if strings.Contains(strings.ToLower(string(encoded)), forbidden) {
			t.Fatalf("boundary serialized secret-bearing value %q: %s", forbidden, encoded)
		}
	}
}

func TestExternalBoundaryRejectsPartialOrUnknownEnvelope(t *testing.T) {
	partial := testExternalBoundary()
	partial.NetworkBoundaryHash = ""
	if err := partial.Normalize(); err == nil {
		t.Fatal("partial boundary was accepted")
	}
	unknown := testExternalBoundary()
	unknown.NetworkBoundary = "host_firewall"
	if err := unknown.Normalize(); err == nil {
		t.Fatal("unknown network boundary was accepted")
	}
}

func TestExternalBoundaryRequirementIsLimitedToNetworkEffects(t *testing.T) {
	if !RequiresExternalBoundary(EffectClassExternalCommunication) {
		t.Fatal("external communication must require a boundary")
	}
	if RequiresExternalBoundary(EffectClassReversibleWrite) || RequiresExternalBoundary(EffectClassReadOnly) {
		t.Fatal("local/read-only effects must not claim a network boundary")
	}
}
