package policy

import (
	"strings"
	"sync"
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func testResolver() *Resolver {
	return &Resolver{Lookup: func(ref contracts.ValidatorRef) bool {
		return ref.Version == "1" && strings.HasPrefix(ref.ID, "change.")
	}}
}

func testPack() contracts.ValidationPolicyPack {
	return contracts.ValidationPolicyPack{
		WorkspaceID: "workspace-a", PolicyID: "safe", Version: "1",
		Rules:    []contracts.ValidationPolicyRule{{Validator: contracts.ValidatorRef{ID: contracts.ValidationValidatorFiles, Version: "1"}, Required: true}},
		Budget:   contracts.PolicyBudget{Change: contracts.ChangeBudgets{MaxOperations: 2, MaxFileBytes: 1024, MaxTotalBytes: 2048}, Validation: contracts.ValidationBudget{MaxValidators: 8, MaxFiles: 10, MaxBytes: 4096, MaxOutputBytes: 4096, MaxWallTimeMS: 5000, MaxSQLQueries: 20, MaxRetries: 1, MaxReportBytes: 4096}},
		Approval: contracts.PolicyApprovalConfig{Mode: contracts.PolicyApprovalRequired},
	}
}

func TestResolveInjectsMandatoryValidatorsAndStableHash(t *testing.T) {
	r := testResolver()
	pack, err := r.ValidatePack(testPack())
	if err != nil {
		t.Fatal(err)
	}
	request := contracts.PolicyEvaluationRequest{WorkspaceID: "workspace-a", Policy: &contracts.ValidationPolicyRef{WorkspaceID: "workspace-a", PolicyID: "safe", Version: "1", PolicyHash: pack.PolicyHash}}
	first, err := r.Resolve(&pack, request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := r.Resolve(&pack, request)
	if err != nil {
		t.Fatal(err)
	}
	if first.ResolutionHash == "" || first.ResolutionHash != second.ResolutionHash || len(first.Validators) != 4 {
		t.Fatalf("resolution is not stable or mandatory set is incomplete: first=%+v second=%+v", first, second)
	}
	for _, mandatory := range []string{contracts.ValidationValidatorPreconditions, contracts.ValidationValidatorFiles, contracts.ValidationValidatorSafety, contracts.ValidationValidatorTree} {
		found := false
		for _, ref := range first.Validators {
			if ref.ID == mandatory {
				found = true
			}
		}
		if !found {
			t.Fatalf("mandatory validator %q missing: %+v", mandatory, first.Validators)
		}
	}
}

func TestResolveRejectsBudgetWideningAndCrossWorkspace(t *testing.T) {
	r := testResolver()
	pack, err := r.ValidatePack(testPack())
	if err != nil {
		t.Fatal(err)
	}
	ref := pack.Ref()
	request := contracts.PolicyEvaluationRequest{WorkspaceID: "workspace-a", Policy: &ref}
	request.RequestedBudget.Change.MaxOperations = pack.Budget.Change.MaxOperations + 1
	if _, err := r.Resolve(&pack, request); err == nil {
		t.Fatal("budget widening unexpectedly accepted")
	}
	request.RequestedBudget = contracts.PolicyBudget{}
	request.Policy.WorkspaceID = "workspace-b"
	if _, err := r.Resolve(&pack, request); err == nil {
		t.Fatal("cross-workspace policy unexpectedly accepted")
	}
}

func TestResolveRejectsHashMismatchAndUnknownValidator(t *testing.T) {
	r := testResolver()
	pack := testPack()
	ref := contracts.ValidationPolicyRef{WorkspaceID: "workspace-a", PolicyID: "safe", Version: "1", PolicyHash: strings.Repeat("a", 64)}
	if _, err := r.Resolve(&pack, contracts.PolicyEvaluationRequest{WorkspaceID: "workspace-a", Policy: &ref}); err == nil {
		t.Fatal("hash mismatch unexpectedly accepted")
	}
	pack.Rules = append(pack.Rules, contracts.ValidationPolicyRule{Validator: contracts.ValidatorRef{ID: "custom.unknown", Version: "1"}})
	if _, err := r.ValidatePack(pack); err == nil {
		t.Fatal("unknown validator unexpectedly accepted")
	}
}

func TestResolveCompatibilityPathIsConcurrentAndDeterministic(t *testing.T) {
	r := testResolver()
	request := contracts.PolicyEvaluationRequest{WorkspaceID: "workspace-a"}
	first, err := r.Resolve(nil, request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Selected || first.ResolutionHash == "" {
		t.Fatalf("compatibility path selected a policy: %+v", first)
	}
	const workers = 16
	results := make(chan contracts.PolicyResolution, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, resolveErr := r.Resolve(nil, request)
			if resolveErr != nil {
				errs <- resolveErr
				return
			}
			results <- result
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	for result := range results {
		if result.ResolutionHash != first.ResolutionHash {
			t.Fatalf("concurrent resolution changed hash: %s != %s", result.ResolutionHash, first.ResolutionHash)
		}
	}
}

func TestResolveCompatibilityPathRejectsInvalidOverrides(t *testing.T) {
	r := testResolver()
	for name, request := range map[string]contracts.PolicyEvaluationRequest{
		"budget above global": {WorkspaceID: "workspace-a", RequestedBudget: contracts.PolicyBudget{Validation: contracts.ValidationBudget{MaxBytes: contracts.MaxValidationBytes + 1}}},
		"negative budget":     {WorkspaceID: "workspace-a", RequestedBudget: contracts.PolicyBudget{Change: contracts.ChangeBudgets{MaxOperations: -1}}},
		"unknown approval":    {WorkspaceID: "workspace-a", RequestedApprovalMode: "maybe"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := r.Resolve(nil, request); err == nil {
				t.Fatal("invalid compatibility request was accepted")
			}
		})
	}
}

func TestResolveRequireForApprovalIsConservativeAndScoped(t *testing.T) {
	r := &Resolver{Lookup: func(ref contracts.ValidatorRef) bool { return true }}
	pack := contracts.ValidationPolicyPack{
		WorkspaceID: "workspace-a", PolicyID: "safe", Version: "1",
		Approval: contracts.PolicyApprovalConfig{Mode: contracts.PolicyApprovalAutomatic, RequireFor: []string{"delete_file"}},
	}
	withoutMatch, err := r.Resolve(&pack, contracts.PolicyEvaluationRequest{WorkspaceID: "workspace-a", RequestedApprovalMode: contracts.PolicyApprovalAutomatic, Operation: "change", OperationTypes: []string{"create_file"}})
	if err != nil {
		t.Fatal(err)
	}
	if withoutMatch.ApprovalMode != contracts.PolicyApprovalAutomatic {
		t.Fatalf("unmatched operation unexpectedly required approval: %s", withoutMatch.ApprovalMode)
	}
	withMatch, err := r.Resolve(&pack, contracts.PolicyEvaluationRequest{WorkspaceID: "workspace-a", RequestedApprovalMode: contracts.PolicyApprovalAutomatic, Operation: "change", OperationTypes: []string{"delete_file"}})
	if err != nil {
		t.Fatal(err)
	}
	if withMatch.ApprovalMode != contracts.PolicyApprovalRequired {
		t.Fatalf("matched operation did not require approval: %s", withMatch.ApprovalMode)
	}
	unscoped, err := r.Resolve(&pack, contracts.PolicyEvaluationRequest{WorkspaceID: "workspace-a", RequestedApprovalMode: contracts.PolicyApprovalAutomatic})
	if err != nil {
		t.Fatal(err)
	}
	if unscoped.ApprovalMode != contracts.PolicyApprovalRequired {
		t.Fatalf("unscoped require_for was not fail-closed: %s", unscoped.ApprovalMode)
	}
}

func TestResolveRequiresAnExplicitValidatorRegistry(t *testing.T) {
	if _, err := (&Resolver{}).Resolve(nil, contracts.PolicyEvaluationRequest{WorkspaceID: "workspace-a"}); err == nil {
		t.Fatal("compatibility resolution succeeded without a validator registry")
	}
}
