package contracts

import (
	"encoding/json"
	"strings"
	"testing"
)

func testDomainEffectLink(workspace string) DomainEffectLink {
	return DomainEffectLink{
		WorkspaceID: workspace, OperationID: "operation-1", OperationHash: domainTestHash("operation"),
		StepID: "step-1", AttemptID: "attempt-1", EffectID: "effect-1", EffectReservationHash: domainTestHash("effect"),
		DomainKind: DomainEffectKindModelCall, DomainID: "model-call-1", DomainHash: domainTestHash("domain"),
		RequestHash: domainTestHash("request"), Boundary: "model-provider", EffectClass: EffectClassExternalCommunication,
		DeliveryGuarantee: ExternalDeliveryAtLeastOnce, VerificationStatus: ExternalVerificationNotRequired,
		Actor: ActorRef{ID: "actor-1", Kind: "operator", WorkspaceID: workspace}, RequestID: "request-1", IdempotencyKey: "link-1",
	}
}

func TestDomainEffectLinkNormalizationAndHashAreStable(t *testing.T) {
	first := testDomainEffectLink("workspace-a")
	if err := first.Normalize(); err != nil {
		t.Fatal(err)
	}
	second := first
	second.ID, second.LinkHash, second.CreatedAt = "different-id", "", first.CreatedAt.Add(10)
	if err := second.Normalize(); err != nil {
		t.Fatal(err)
	}
	if first.LinkHash != second.LinkHash {
		t.Fatalf("delivery identity changed link hash: %s != %s", first.LinkHash, second.LinkHash)
	}
	if len(first.LinkHash) != 64 {
		t.Fatalf("unexpected link hash %q", first.LinkHash)
	}
}

func TestDomainEffectLinkRejectsCrossWorkspaceActorAndIncompleteFence(t *testing.T) {
	link := testDomainEffectLink("workspace-a")
	link.Actor.WorkspaceID = "workspace-b"
	if err := link.Normalize(); err == nil {
		t.Fatal("cross-workspace actor was accepted")
	}
	link = testDomainEffectLink("workspace-a")
	link.OperationFence = 4
	if err := link.Normalize(); err == nil {
		t.Fatal("incomplete operation fence was accepted")
	}
}

func TestDomainEffectLinkNeverSerializesCredentialMaterial(t *testing.T) {
	link := testDomainEffectLink("workspace-a")
	link.CredentialLeaseID = "lease-1"
	link.CredentialLeaseFence = 2
	link.CredentialRevocationEpoch = 3
	link.CredentialSourceVersion = "7"
	encoded, err := json.Marshal(link)
	if err != nil {
		t.Fatal(err)
	}
	text := string(encoded)
	for _, forbidden := range []string{"secret", "api_key", "token", "password"} {
		if strings.Contains(strings.ToLower(text), forbidden) {
			t.Fatalf("credential material label leaked into link: %q", forbidden)
		}
	}
	if strings.Contains(text, "lease-1") == false {
		t.Fatal("credential reference should remain auditable")
	}
}

func TestDomainEffectLinkRejectsReadOnlyEffect(t *testing.T) {
	link := testDomainEffectLink("workspace-a")
	link.EffectClass = EffectClassReadOnly
	if err := link.Normalize(); err == nil {
		t.Fatal("read-only effect was accepted")
	}
}

func TestDomainEffectLinkCarriesTheSameExternalBoundaryIdentity(t *testing.T) {
	without := testDomainEffectLink("workspace-a")
	with := without
	boundary := testExternalBoundary()
	with.ExternalBoundary = &boundary
	if err := without.Normalize(); err != nil {
		t.Fatal(err)
	}
	if err := with.Normalize(); err != nil {
		t.Fatal(err)
	}
	if with.LinkHash == without.LinkHash {
		t.Fatal("boundary envelope did not participate in domain-link identity")
	}
	if with.ExternalBoundary.StableHash() == "" {
		t.Fatal("normalized domain-link boundary has no stable identity")
	}
}
