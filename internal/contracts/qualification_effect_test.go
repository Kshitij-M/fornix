package contracts

import (
	"encoding/json"
	"testing"
)

func testEffectAuthorityObservation(success bool) EffectAuthorityObservation {
	value := EffectAuthorityObservation{
		WorkspaceID:              "effect-qualification-workspace",
		OperationID:              "operation-1",
		EffectID:                 "effect-1",
		ReservationHash:          HashStrings("reservation"),
		DomainLinkID:             "link-1",
		DomainLinkHash:           HashStrings("link"),
		ReplayHash:               HashStrings("replay"),
		SuccessReconciled:        success,
		UncertainRecovery:        !success,
		DuplicateSuppressed:      true,
		StaleFenceRejected:       true,
		WorkspaceIsolationProven: true,
		ReplayStable:             true,
		InvocationCount:          1,
	}
	if success {
		value.ResultHash = HashStrings("result")
		value.ReceiptHash = HashStrings("receipt")
		value.ReceiptLinkHash = HashStrings("receipt-link")
		value.ReceiptLinkVerified = true
	}
	return value
}

func TestEffectAuthorityObservationRequiresCompleteProof(t *testing.T) {
	success := testEffectAuthorityObservation(true)
	if err := success.Normalize(); err != nil {
		t.Fatal(err)
	}
	if success.StableHash() == "" {
		t.Fatal("valid success observation has no stable hash")
	}
	uncertain := testEffectAuthorityObservation(false)
	if err := uncertain.Normalize(); err != nil {
		t.Fatal(err)
	}
	if uncertain.ResultHash != "" {
		t.Fatal("uncertain observation claimed a result hash")
	}
	incomplete := success
	incomplete.StaleFenceRejected = false
	if err := incomplete.Normalize(); err == nil {
		t.Fatal("incomplete stale-fence proof was accepted")
	}
	conflicting := success
	conflicting.UncertainRecovery = true
	if err := conflicting.Normalize(); err == nil {
		t.Fatal("conflicting terminal outcomes were accepted")
	}
	missingReceiptLink := success
	missingReceiptLink.ReceiptLinkVerified = false
	if err := missingReceiptLink.Normalize(); err == nil {
		t.Fatal("missing receipt linkage was accepted")
	}
}

func TestEffectAuthorityObservationHashIsStableAndRedacted(t *testing.T) {
	value := testEffectAuthorityObservation(true)
	first := value.StableHash()
	second := value.StableHash()
	if first == "" || first != second {
		t.Fatalf("observation hash is not stable: %q %q", first, second)
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) == "" || string(encoded) != string(mustObservationJSON(value)) {
		t.Fatal("observation encoding is not deterministic")
	}
}

func mustObservationJSON(value any) []byte {
	raw, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return raw
}

func TestEffectAuthorityObservationProducesQualificationCase(t *testing.T) {
	value, err := testEffectAuthorityObservation(true).QualificationCase()
	if err != nil {
		t.Fatal(err)
	}
	if value.Name != "effect-authority-dispatch" || value.Category != QualificationCategoryAuthority || value.Outcome != QualificationOutcomePassed || value.EvidenceHash == "" {
		t.Fatalf("qualification case=%+v", value)
	}
}
