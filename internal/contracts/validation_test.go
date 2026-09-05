package contracts

import (
	"strings"
	"testing"
	"time"
)

func TestValidationRequestHashExcludesDeliveryIdentityAndCanonicalizesPlan(t *testing.T) {
	request := ValidationRequest{
		ID: "run-a", RequestID: "request-a", IdempotencyKey: "validation-a", WorkspaceID: "workspace-a",
		Actor:               ActorRef{ID: "actor-a", Kind: "operator", WorkspaceID: "workspace-a"},
		ChangeApplicationID: "application-a", ProposalID: "proposal-a", PacketHash: strings.Repeat("a", 64),
		ExpectedTreeHash: strings.Repeat("b", 64), Repository: "repo-a",
		Source:     RepositorySource{Repository: "repo-a", SourceRoot: "/tmp/a", MountRoot: "/tmp"},
		Validators: []ValidatorRef{{ID: ValidationValidatorTree, Version: "1"}, {ID: ValidationValidatorFiles, Version: "1"}},
	}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	first := request.RequestHash()
	request.ID, request.RequestID, request.IdempotencyKey = "run-b", "request-b", "validation-b"
	request.Actor = ActorRef{ID: "different-actor", WorkspaceID: request.WorkspaceID}
	request.DryRun = true
	request.Source.MountRoot = "/another-mounted-parent"
	request.Validators[0], request.Validators[1] = request.Validators[1], request.Validators[0]
	if second := request.RequestHash(); first != second {
		t.Fatalf("delivery-only request changes affected identity: %s != %s", first, second)
	}
	plan, err := request.Plan()
	if err != nil {
		t.Fatal(err)
	}
	if len(plan.Validators) != 2 || plan.Validators[0].ID != ValidationValidatorFiles || plan.Validators[1].ID != ValidationValidatorTree {
		t.Fatalf("validators were not canonically ordered: %+v", plan.Validators)
	}
}

func TestValidationBudgetRejectsOverLimitAndPreservesZeroDefaults(t *testing.T) {
	budget := ValidationBudget{}
	if err := budget.Normalize(); err != nil {
		t.Fatal(err)
	}
	if budget.MaxFiles != DefaultValidationMaxFiles || budget.MaxReportBytes != DefaultValidationMaxReport {
		t.Fatalf("defaults not applied: %+v", budget)
	}
	budget.MaxBytes = MaxValidationBytes + 1
	if err := budget.Normalize(); err == nil {
		t.Fatal("oversized byte budget unexpectedly accepted")
	}
	budget = DefaultValidationBudget()
	budget.MaxWallTimeMS = int64((MaxValidationWallTime / time.Millisecond) + 1)
	if err := budget.Normalize(); err == nil {
		t.Fatal("oversized wall-clock budget unexpectedly accepted")
	}
}

func TestValidationReportHashIgnoresDatabaseIdentityAndTime(t *testing.T) {
	result := ValidationResult{ID: "result-a", RunID: "run-a", WorkspaceID: "workspace-a", Ordinal: 0, Validator: ValidatorRef{ID: ValidationValidatorTree, Version: "1"}, Attempt: 1, Status: ValidationPassed, Outcome: ValidationOutcomePassed, InputHash: strings.Repeat("a", 64), Summary: "tree verified", CreatedAt: time.Now()}
	report := ValidationReport{RunID: "run-a", WorkspaceID: "workspace-a", Status: ValidationPassed, Outcome: ValidationOutcomePassed, PacketHash: strings.Repeat("b", 64), ExpectedTreeHash: strings.Repeat("c", 64), Results: []ValidationResult{result}}
	first := report.StableHash()
	report.Results[0].ID = "result-b"
	report.Results[0].CreatedAt = time.Now().Add(24 * time.Hour)
	if second := report.StableHash(); first != second {
		t.Fatalf("database identity changed report hash: %s != %s", first, second)
	}
}

func TestValidationDisclosureRejectsUnboundedBudget(t *testing.T) {
	request := ValidationDisclosureRequest{WorkspaceID: "workspace-a", ValidationRunID: "run-a", Level: string(DisclosureDetail), MaxBytes: MaxValidationDisclosureBytes + 1, MaxItems: 1}
	if err := request.Normalize(); err == nil {
		t.Fatal("oversized disclosure budget unexpectedly accepted")
	}
}
