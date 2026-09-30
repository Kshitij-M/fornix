package effectdispatch

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestBuiltinConformanceManifestIsStableAndComplete(t *testing.T) {
	first, err := BuiltinConformanceRegistry()
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuiltinConformanceRegistry()
	if err != nil {
		t.Fatal(err)
	}
	firstHash, err := first.Validate()
	if err != nil {
		t.Fatal(err)
	}
	secondHash, err := second.Validate()
	if err != nil {
		t.Fatal(err)
	}
	if firstHash == "" || firstHash != secondHash || len(first.Entries()) != 6 {
		t.Fatalf("unstable or incomplete manifest: first=%s second=%s entries=%d", firstHash, secondHash, len(first.Entries()))
	}
	encoded, err := json.Marshal(first.Entries())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(string(encoded)), "credential") {
		t.Fatalf("manifest contains secret-bearing metadata: %s", encoded)
	}
}

func TestConformanceRegistryRejectsIncompleteAuthority(t *testing.T) {
	registry := NewConformanceRegistry()
	err := registry.Register(AdapterConformance{ID: "incomplete", DomainKind: "domain", Boundary: "boundary", Effect: "reversible_write"})
	if err == nil || !strings.Contains(err.Error(), "missing an authority requirement") {
		t.Fatalf("Register() error=%v, want fail-closed authority error", err)
	}
}

func TestConformanceRegistryRequiresBoundaryAuthorityForNetworkEffects(t *testing.T) {
	registry := NewConformanceRegistry()
	err := registry.Register(AdapterConformance{
		ID: "network-effect", DomainKind: "http_request", Boundary: "http", Effect: "external_communication",
		UsesOperationAdmission: true, UsesEffectReservation: true, UsesOperationFence: true,
		UsesTaskFenceWhenBound: true, UsesDeploymentAdmission: true, PreservesAtLeastOnce: true,
		SupportsRecovery: true,
	})
	if err == nil || !strings.Contains(err.Error(), "missing an authority requirement") {
		t.Fatalf("Register() error=%v, want missing boundary authority", err)
	}
}
