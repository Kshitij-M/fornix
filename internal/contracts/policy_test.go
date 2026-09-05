package contracts

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestPolicyPackNormalizationIsCanonicalAndSafetyFloorsAreEnabled(t *testing.T) {
	pack := ValidationPolicyPack{
		WorkspaceID: " workspace-a ", PolicyID: "safe", Version: "1",
		Rules: []ValidationPolicyRule{
			{Validator: ValidatorRef{ID: ValidationValidatorTree, Version: "1"}},
			{Validator: ValidatorRef{ID: ValidationValidatorFiles, Version: "1"}},
		},
	}
	if err := pack.Normalize(); err != nil {
		t.Fatal(err)
	}
	if pack.PolicyHash == "" || pack.SafetyFloors != (PolicySafetyFloors{true, true, true, true, true, true}) || !pack.RequireTaskFence {
		t.Fatalf("policy safety defaults missing: %+v", pack)
	}
	if pack.Rules[0].Validator.ID != ValidationValidatorFiles || pack.Rules[1].Validator.ID != ValidationValidatorTree {
		t.Fatalf("rules were not sorted: %+v", pack.Rules)
	}
	first := pack.PolicyHash
	pack.Rules[0], pack.Rules[1] = pack.Rules[1], pack.Rules[0]
	if err := pack.Normalize(); err != nil {
		t.Fatal(err)
	}
	if pack.PolicyHash != first {
		t.Fatalf("equivalent policy changed hash: %s != %s", pack.PolicyHash, first)
	}
}

func TestPolicyRequestAndEventPinRemainWorkspaceScoped(t *testing.T) {
	request := PolicyCreateRequest{WorkspaceID: "workspace-a", IdempotencyKey: "policy-key", Pack: ValidationPolicyPack{PolicyID: "safe", Version: "1"}}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	if request.Pack.WorkspaceID != request.WorkspaceID || request.Pack.PolicyHash == "" {
		t.Fatalf("create request did not pin normalized pack: %+v", request)
	}
	event, err := NewEvent("policy.created", map[string]string{"ok": "true"})
	if err != nil {
		t.Fatal(err)
	}
	event.Scope.WorkspaceID = request.WorkspaceID
	ref := request.Pack.Ref()
	event.Policy = &ref
	if err := event.Normalize(); err != nil {
		t.Fatal(err)
	}
	clone := event.Clone()
	clone.Policy.PolicyHash = strings.Repeat("a", 64)
	if event.Policy.PolicyHash == clone.Policy.PolicyHash {
		t.Fatal("event clone shared mutable policy reference")
	}
	if _, err := json.Marshal(event); err != nil {
		t.Fatal(err)
	}
	event.Policy.WorkspaceID = "workspace-b"
	if err := event.Normalize(); err == nil {
		t.Fatal("cross-workspace event policy unexpectedly accepted")
	}
}

func TestPolicyCreateRejectsForeignWorkspaceAndMismatchedHash(t *testing.T) {
	request := PolicyCreateRequest{
		WorkspaceID:    "workspace-a",
		IdempotencyKey: "policy-create-1",
		Pack: ValidationPolicyPack{
			WorkspaceID: "workspace-b",
			PolicyID:    "policy",
			Version:     "1",
			Rules:       []ValidationPolicyRule{{Validator: ValidatorRef{ID: ValidationValidatorFiles, Version: "1"}}},
		},
	}
	if err := request.Normalize(); err == nil {
		t.Fatal("foreign policy pack workspace was accepted")
	}

	request.Pack.WorkspaceID = "workspace-a"
	request.Pack.PolicyHash = strings.Repeat("a", 64)
	if err := request.Normalize(); err == nil {
		t.Fatal("mismatched policy hash was accepted")
	}
}

func TestPolicyEvaluationRequestCanonicalizesAndBoundsCallerOverrides(t *testing.T) {
	request := PolicyEvaluationRequest{
		WorkspaceID: "workspace-a",
		RequestedValidators: []ValidatorRef{
			{ID: "z", Version: "1"},
			{ID: "a", Version: "1"},
		},
		RequestedBudget: PolicyBudget{Change: ChangeBudgets{MaxOperations: 4}},
		OperationTypes:  []string{"delete_file", "change", "delete_file"},
	}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	if request.RequestedValidators[0].ID != "a" || request.OperationTypes[0] != "change" || len(request.OperationTypes) != 2 {
		t.Fatalf("request was not canonicalized: %+v", request)
	}

	for name, budget := range map[string]PolicyBudget{
		"negative":  {Change: ChangeBudgets{MaxOperations: -1}},
		"too-large": {Validation: ValidationBudget{MaxBytes: MaxValidationBytes + 1}},
	} {
		t.Run(name, func(t *testing.T) {
			invalid := PolicyEvaluationRequest{WorkspaceID: "workspace-a", RequestedBudget: budget}
			if err := invalid.Normalize(); err == nil {
				t.Fatalf("invalid budget was accepted: %+v", budget)
			}
		})
	}
}

func TestPolicyHashCanonicalizesApprovalScopes(t *testing.T) {
	first := ValidationPolicyPack{WorkspaceID: "workspace-a", PolicyID: "safe", Version: "1", Approval: PolicyApprovalConfig{Mode: PolicyApprovalAutomatic, RequireFor: []string{"delete_file", "change"}}}
	second := first
	second.Approval.RequireFor = []string{"change", "delete_file"}
	if first.ComputeHash() != second.ComputeHash() {
		t.Fatal("equivalent approval scopes produced different hashes")
	}
}

func TestPolicyApprovalScopeInputIsBounded(t *testing.T) {
	pack := ValidationPolicyPack{WorkspaceID: "workspace-a", PolicyID: "safe", Version: "1", Approval: PolicyApprovalConfig{RequireFor: make([]string, MaxPolicyApprovalScopes+1)}}
	for i := range pack.Approval.RequireFor {
		pack.Approval.RequireFor[i] = fmt.Sprintf("operation-%d", i)
	}
	if err := pack.Normalize(); err == nil {
		t.Fatal("unbounded approval scope list was accepted")
	}
}
