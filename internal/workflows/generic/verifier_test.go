package generic

import (
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func TestUnknownVerificationRemainsRetryable(t *testing.T) {
	state := store.EffectState{State: contracts.ExternalEffectRecoveryRequired, Version: 3, FailureCode: "external_uncertain"}
	link := store.DomainEffectLinkCurrent{Transition: contracts.DomainEffectLinkTransition{ToStatus: contracts.DomainEffectLinkStatusRecoveryRequired}}
	if _, ok, err := durableVerificationOutcome(state, link); err != nil || ok {
		t.Fatalf("unknown recovery result was treated as terminal: ok=%v err=%v", ok, err)
	}
}

func TestVerificationOutcomeReplaysFromAppendOnlyTransition(t *testing.T) {
	transition := store.EffectTransition{
		ToState:           contracts.ExternalEffectRecoveryRequired,
		ProviderRequestID: "provider-1", FailureCode: "external_uncertain",
	}
	first, err := effectVerificationOutcome(transition)
	if err != nil {
		t.Fatal(err)
	}
	second, err := effectVerificationOutcome(transition)
	if err != nil {
		t.Fatal(err)
	}
	if first != second || first.Status != contracts.EffectVerificationStatusUnknown || first.FailureCode != "external_uncertain" {
		t.Fatalf("historical verification outcome was not stable: first=%+v second=%+v", first, second)
	}
}
