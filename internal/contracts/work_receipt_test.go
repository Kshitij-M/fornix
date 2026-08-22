package contracts

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func testWorkReceiptRequest() WorkReceiptFinalizeRequest {
	return WorkReceiptFinalizeRequest{
		ReceiptID: "receipt-delivery-id", RequestID: "request-delivery-id", IdempotencyKey: "receipt-key",
		WorkspaceID: "workspace-a", Actor: ActorRef{ID: "operator", Kind: "user", Name: "Operator", WorkspaceID: "workspace-a"},
		WorkKind: WorkReceiptReferenceAgentRun, WorkID: "run-1",
		Task: &EntityRef{ID: "42", Kind: "task", WorkspaceID: "workspace-a"}, Session: &EntityRef{ID: "worker", Kind: "session", WorkspaceID: "workspace-a"},
		TaskOwnerID: "worker", TaskFence: 7, SourceManifestHash: strings.Repeat("a", 64), ReplayHash: strings.Repeat("b", 64),
		Steps: []WorkReceiptStep{
			{Ordinal: 1, ID: "model", Name: "model", Kind: "model", Status: "succeeded", OutputHash: strings.Repeat("c", 64), Metadata: map[string]string{"provider": "fake"}},
			{Ordinal: 0, ID: "context", Name: "context", Kind: "retrieval", Status: "succeeded", OutputHash: strings.Repeat("d", 64)},
		},
		References: []WorkReceiptReference{{WorkspaceID: "workspace-a", Kind: WorkReceiptReferenceAgentRun, SourceID: "run-1", Hash: strings.Repeat("e", 64)}},
		Cost:       WorkReceiptCost{ModelUSD: 0.02, TotalUSD: 0.02, Measured: true},
	}
}

func TestWorkReceiptHashIgnoresDeliveryIdentityAndWallClock(t *testing.T) {
	one := testWorkReceiptRequest()
	receiptOne, err := one.ToReceipt(time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	two := testWorkReceiptRequest()
	two.ReceiptID, two.RequestID, two.IdempotencyKey = "receipt-other", "request-other", "other-key"
	receiptTwo, err := two.ToReceipt(time.Unix(2, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if receiptOne.CanonicalHash != receiptTwo.CanonicalHash {
		t.Fatalf("delivery metadata changed receipt hash: %s != %s", receiptOne.CanonicalHash, receiptTwo.CanonicalHash)
	}
	if receiptOne.RequestHash != receiptTwo.RequestHash {
		t.Fatalf("delivery metadata changed request hash: %s != %s", receiptOne.RequestHash, receiptTwo.RequestHash)
	}
	if receiptOne.Steps[0].Ordinal != 0 || receiptOne.Steps[1].Ordinal != 1 {
		t.Fatalf("steps were not deterministically ordered: %+v", receiptOne.Steps)
	}
}

func TestWorkReceiptCanonicalJSONIsBoundedAndRedacted(t *testing.T) {
	receipt, err := testWorkReceiptRequest().ToReceipt(time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := receipt.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	if len(canonical) > MaxWorkReceiptPayloadSize || bytes.Contains(canonical, []byte("sk-secret")) {
		t.Fatalf("unsafe canonical payload: %s", canonical)
	}
	if bytes.Contains(canonical, []byte("receipt-delivery-id")) == false {
		t.Fatal("stored payload should retain auditable receipt identity")
	}
}

func TestWorkReceiptRejectsUnsafeMetadataAndCrossWorkspaceLinks(t *testing.T) {
	unsafe := testWorkReceiptRequest()
	unsafe.Steps[0].Metadata = map[string]string{"prompt": "do not persist this"}
	if _, err := unsafe.ToReceipt(time.Unix(1, 0).UTC()); err == nil {
		t.Fatal("unsafe metadata was accepted")
	}
	foreign := testWorkReceiptRequest()
	foreign.References[0].WorkspaceID = "workspace-b"
	if _, err := foreign.ToReceipt(time.Unix(1, 0).UTC()); err == nil {
		t.Fatal("cross-workspace reference was accepted")
	}
}

func TestWorkReceiptDisclosureDefaultsAndBudgetLimits(t *testing.T) {
	request, err := (WorkReceiptDisclosureRequest{WorkspaceID: "workspace-a", ReceiptID: "r", Level: WorkReceiptDisclosureDetail}).Normalize()
	if err != nil {
		t.Fatal(err)
	}
	if request.MaxBytes != DefaultWorkReceiptDisclosureBytes || request.MaxTokens != DefaultWorkReceiptDisclosureTokens || request.MaxItems != DefaultWorkReceiptDisclosureItems {
		t.Fatalf("unexpected disclosure defaults: %+v", request)
	}
	if _, err := (WorkReceiptDisclosureRequest{WorkspaceID: "workspace-a", ReceiptID: "r", MaxBytes: MaxWorkReceiptDisclosureBytes + 1}).Normalize(); err == nil {
		t.Fatal("oversized disclosure budget was accepted")
	}
}
