// Package policy resolves immutable declarative validation policy packs into
// the exact validator, budget, approval, and safety decision used for
// admission. It deliberately has no database, network, shell, or model
// dependency: the store supplies an immutable snapshot and the resolver
// returns a deterministic decision that can be persisted and replayed.
package policy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
)

const (
	mandatoryPreconditions = contracts.ValidationValidatorPreconditions
	mandatoryFiles         = contracts.ValidationValidatorFiles
	mandatorySafety        = contracts.ValidationValidatorSafety
	mandatoryTree          = contracts.ValidationValidatorTree
)

// Resolver is a pure policy admission engine backed by an explicit validator
// capability registry. A policy can name only validators already registered by
// the process; no policy field is executable code.
type Resolver struct {
	// Lookup is supplied by the process' explicit validator registry. Keeping
	// this as a function avoids coupling the pure resolver to the runtime or
	// store packages and prevents an import cycle.
	Lookup func(contracts.ValidatorRef) bool
}

// ValidatePack normalizes a policy and verifies every referenced validator.
// It is safe to call before persistence because it does not mutate the input.
func (r *Resolver) ValidatePack(pack contracts.ValidationPolicyPack) (contracts.ValidationPolicyPack, error) {
	pack.Rules = append([]contracts.ValidationPolicyRule(nil), pack.Rules...)
	pack.Approval.RequireFor = append([]string(nil), pack.Approval.RequireFor...)
	if err := pack.Normalize(); err != nil {
		return contracts.ValidationPolicyPack{}, err
	}
	if r == nil || r.Lookup == nil {
		return contracts.ValidationPolicyPack{}, fmt.Errorf("policy validator registry is not configured")
	}
	for _, rule := range pack.Rules {
		if !r.Lookup(rule.Validator) {
			return contracts.ValidationPolicyPack{}, fmt.Errorf("policy validator %s@%s is not registered", rule.Validator.ID, rule.Validator.Version)
		}
	}
	return pack, nil
}

// Resolve applies one immutable pack to a bounded admission request. A nil
// pack is the explicit compatibility path: it preserves the pre-policy
// validator and budget defaults and intentionally has no policy hash.
func (r *Resolver) Resolve(pack *contracts.ValidationPolicyPack, request contracts.PolicyEvaluationRequest) (contracts.PolicyResolution, error) {
	if err := request.Normalize(); err != nil {
		return contracts.PolicyResolution{}, err
	}
	if pack == nil {
		return r.resolveCompatibility(request)
	}
	normalized, err := r.ValidatePack(*pack)
	if err != nil {
		return contracts.PolicyResolution{}, err
	}
	if request.Policy != nil {
		if request.Policy.PolicyHash != "" && request.Policy.PolicyHash != normalized.PolicyHash {
			return contracts.PolicyResolution{}, fmt.Errorf("policy hash does not match immutable policy body")
		}
		if request.Policy.PolicyID != normalized.PolicyID || request.Policy.Version != normalized.Version {
			return contracts.PolicyResolution{}, fmt.Errorf("policy reference does not match immutable policy body")
		}
	}
	validators := make([]contracts.ValidatorRef, 0, len(normalized.Rules)+len(request.RequestedValidators)+1)
	seen := make(map[string]struct{}, len(normalized.Rules)+len(request.RequestedValidators))
	add := func(ref contracts.ValidatorRef) error {
		ref.ID, ref.Version = strings.TrimSpace(ref.ID), strings.TrimSpace(ref.Version)
		if ref.ID == "" || ref.Version == "" {
			return fmt.Errorf("validator reference is incomplete")
		}
		key := ref.ID + "\x00" + ref.Version
		if _, exists := seen[key]; exists {
			return nil
		}
		if r.Lookup == nil || !r.Lookup(ref) {
			return fmt.Errorf("policy validator %s@%s is not registered", ref.ID, ref.Version)
		}
		seen[key] = struct{}{}
		validators = append(validators, ref)
		return nil
	}
	for _, rule := range normalized.Rules {
		if err := add(rule.Validator); err != nil {
			return contracts.PolicyResolution{}, err
		}
	}
	for _, ref := range []contracts.ValidatorRef{{ID: mandatoryPreconditions, Version: "1"}, {ID: mandatoryFiles, Version: "1"}, {ID: mandatorySafety, Version: "1"}, {ID: mandatoryTree, Version: "1"}} {
		if err := add(ref); err != nil {
			return contracts.PolicyResolution{}, fmt.Errorf("mandatory validator: %w", err)
		}
	}
	for _, ref := range request.RequestedValidators {
		if err := add(ref); err != nil {
			return contracts.PolicyResolution{}, err
		}
	}
	if normalized.RequireReindex {
		if err := add(contracts.ValidatorRef{ID: contracts.ValidationValidatorReindex, Version: "1"}); err != nil {
			return contracts.PolicyResolution{}, fmt.Errorf("re-index validator: %w", err)
		}
	}
	sort.Slice(validators, func(i, j int) bool {
		if validators[i].ID != validators[j].ID {
			return validators[i].ID < validators[j].ID
		}
		return validators[i].Version < validators[j].Version
	})
	if len(validators) > normalized.Budget.Validation.MaxValidators {
		return contracts.PolicyResolution{}, fmt.Errorf("policy validator set exceeds policy budget")
	}
	budget, err := tightenBudget(normalized.Budget, request.RequestedBudget)
	if err != nil {
		return contracts.PolicyResolution{}, err
	}
	approval, err := effectiveApproval(normalized.Approval, request.RequestedApprovalMode, request.Operation, request.OperationTypes)
	if err != nil {
		return contracts.PolicyResolution{}, err
	}
	if approval == contracts.PolicyApprovalDenied {
		return contracts.PolicyResolution{}, fmt.Errorf("policy denies admission")
	}
	ref := normalized.Ref()
	ref.SchemaVersion = contracts.PolicySchemaVersion
	resolution := contracts.PolicyResolution{
		SchemaVersion: contracts.PolicySchemaVersion, WorkspaceID: request.WorkspaceID, Selected: true,
		Ref: &ref, Snapshot: &normalized, Validators: validators, Budget: budget,
		ApprovalMode: approval, RequireReindex: normalized.RequireReindex || request.RequireReindex,
		RequireTaskFence: true,
	}
	resolution.ResolutionHash = resolutionHash(resolution)
	return resolution, nil
}

func (r *Resolver) resolveCompatibility(request contracts.PolicyEvaluationRequest) (contracts.PolicyResolution, error) {
	if r == nil || r.Lookup == nil {
		return contracts.PolicyResolution{}, fmt.Errorf("policy validator registry is not configured")
	}
	validators := append([]contracts.ValidatorRef(nil), request.RequestedValidators...)
	if len(validators) == 0 {
		validators = contracts.DefaultValidatorRefs()
	}
	for _, ref := range validators {
		if !r.Lookup(ref) {
			return contracts.PolicyResolution{}, fmt.Errorf("validator %s@%s is not registered", ref.ID, ref.Version)
		}
	}
	budget := compatibilityBudget(request.RequestedBudget)
	approval, err := effectiveApproval(contracts.PolicyApprovalConfig{Mode: contracts.PolicyApprovalRequired}, request.RequestedApprovalMode, request.Operation, request.OperationTypes)
	if err != nil {
		return contracts.PolicyResolution{}, err
	}
	if approval == contracts.PolicyApprovalDenied {
		return contracts.PolicyResolution{}, fmt.Errorf("admission is denied")
	}
	resolution := contracts.PolicyResolution{SchemaVersion: contracts.PolicySchemaVersion, WorkspaceID: request.WorkspaceID, Validators: validators, Budget: budget, ApprovalMode: approval, RequireReindex: request.RequireReindex, RequireTaskFence: true}
	resolution.ResolutionHash = resolutionHash(resolution)
	return resolution, nil
}

func compatibilityBudget(requested contracts.PolicyBudget) contracts.PolicyBudget {
	budget := contracts.PolicyBudget{Change: contracts.ChangeBudgets{MaxOperations: contracts.DefaultChangeMaxOperations, MaxFileBytes: contracts.DefaultChangeMaxFileBytes, MaxTotalBytes: contracts.DefaultChangeMaxTotalBytes}, Validation: contracts.DefaultValidationBudget()}
	if requested.Change.MaxOperations > 0 {
		budget.Change.MaxOperations = requested.Change.MaxOperations
	}
	if requested.Change.MaxFileBytes > 0 {
		budget.Change.MaxFileBytes = requested.Change.MaxFileBytes
	}
	if requested.Change.MaxTotalBytes > 0 {
		budget.Change.MaxTotalBytes = requested.Change.MaxTotalBytes
	}
	if requested.Validation.MaxValidators > 0 {
		budget.Validation.MaxValidators = requested.Validation.MaxValidators
	}
	if requested.Validation.MaxFiles > 0 {
		budget.Validation.MaxFiles = requested.Validation.MaxFiles
	}
	if requested.Validation.MaxBytes > 0 {
		budget.Validation.MaxBytes = requested.Validation.MaxBytes
	}
	if requested.Validation.MaxOutputBytes > 0 {
		budget.Validation.MaxOutputBytes = requested.Validation.MaxOutputBytes
	}
	if requested.Validation.MaxWallTimeMS > 0 {
		budget.Validation.MaxWallTimeMS = requested.Validation.MaxWallTimeMS
	}
	if requested.Validation.MaxSQLQueries > 0 {
		budget.Validation.MaxSQLQueries = requested.Validation.MaxSQLQueries
	}
	if requested.Validation.MaxRetries > 0 {
		budget.Validation.MaxRetries = requested.Validation.MaxRetries
	}
	if requested.Validation.MaxReportBytes > 0 {
		budget.Validation.MaxReportBytes = requested.Validation.MaxReportBytes
	}
	return budget
}

func tightenBudget(policy, requested contracts.PolicyBudget) (contracts.PolicyBudget, error) {
	effective := policy
	if requested.Change.MaxOperations > policy.Change.MaxOperations || requested.Change.MaxFileBytes > policy.Change.MaxFileBytes || requested.Change.MaxTotalBytes > policy.Change.MaxTotalBytes {
		return contracts.PolicyBudget{}, fmt.Errorf("requested change budget widens policy")
	}
	if requested.Validation.MaxValidators > policy.Validation.MaxValidators || requested.Validation.MaxFiles > policy.Validation.MaxFiles || requested.Validation.MaxBytes > policy.Validation.MaxBytes || requested.Validation.MaxOutputBytes > policy.Validation.MaxOutputBytes || requested.Validation.MaxWallTimeMS > policy.Validation.MaxWallTimeMS || requested.Validation.MaxSQLQueries > policy.Validation.MaxSQLQueries || requested.Validation.MaxRetries > policy.Validation.MaxRetries || requested.Validation.MaxReportBytes > policy.Validation.MaxReportBytes {
		return contracts.PolicyBudget{}, fmt.Errorf("requested validation budget widens policy")
	}
	if requested.Change.MaxOperations > 0 {
		effective.Change.MaxOperations = requested.Change.MaxOperations
	}
	if requested.Change.MaxFileBytes > 0 {
		effective.Change.MaxFileBytes = requested.Change.MaxFileBytes
	}
	if requested.Change.MaxTotalBytes > 0 {
		effective.Change.MaxTotalBytes = requested.Change.MaxTotalBytes
	}
	if requested.Validation.MaxValidators > 0 {
		effective.Validation.MaxValidators = requested.Validation.MaxValidators
	}
	if requested.Validation.MaxFiles > 0 {
		effective.Validation.MaxFiles = requested.Validation.MaxFiles
	}
	if requested.Validation.MaxBytes > 0 {
		effective.Validation.MaxBytes = requested.Validation.MaxBytes
	}
	if requested.Validation.MaxOutputBytes > 0 {
		effective.Validation.MaxOutputBytes = requested.Validation.MaxOutputBytes
	}
	if requested.Validation.MaxWallTimeMS > 0 {
		effective.Validation.MaxWallTimeMS = requested.Validation.MaxWallTimeMS
	}
	if requested.Validation.MaxSQLQueries > 0 {
		effective.Validation.MaxSQLQueries = requested.Validation.MaxSQLQueries
	}
	if requested.Validation.MaxRetries > 0 {
		effective.Validation.MaxRetries = requested.Validation.MaxRetries
	}
	if requested.Validation.MaxReportBytes > 0 {
		effective.Validation.MaxReportBytes = requested.Validation.MaxReportBytes
	}
	return effective, nil
}

func effectiveApproval(config contracts.PolicyApprovalConfig, requested, operation string, operationTypes []string) (string, error) {
	policy := strings.ToLower(strings.TrimSpace(config.Mode))
	requested = strings.ToLower(strings.TrimSpace(requested))
	if policy == "" {
		policy = contracts.PolicyApprovalRequired
	}
	if requested != "" && requested != contracts.PolicyApprovalAutomatic && requested != contracts.PolicyApprovalRequired && requested != contracts.PolicyApprovalDenied {
		return "", fmt.Errorf("unsupported requested approval mode %q", requested)
	}
	if policy == contracts.PolicyApprovalDenied || requested == contracts.PolicyApprovalDenied {
		return contracts.PolicyApprovalDenied, nil
	}
	if policy == contracts.PolicyApprovalAutomatic && approvalRequiredFor(config.RequireFor, operation, operationTypes) {
		policy = contracts.PolicyApprovalRequired
	}
	if requested == "" {
		return policy, nil
	}
	if policy == contracts.PolicyApprovalRequired || requested == contracts.PolicyApprovalRequired {
		return contracts.PolicyApprovalRequired, nil
	}
	return contracts.PolicyApprovalAutomatic, nil
}

func approvalRequiredFor(scopes []string, operation string, operationTypes []string) bool {
	if len(scopes) == 0 {
		return false
	}
	if strings.TrimSpace(operation) == "" && len(operationTypes) == 0 {
		// An automatic policy with an unscoped require_for rule must fail safe.
		return true
	}
	for _, scope := range scopes {
		scope = strings.TrimSpace(scope)
		if scope == strings.TrimSpace(operation) {
			return true
		}
		for _, operationType := range operationTypes {
			if scope == strings.TrimSpace(operationType) {
				return true
			}
		}
	}
	return false
}

func resolutionHash(resolution contracts.PolicyResolution) string {
	clone := resolution
	clone.ResolutionHash = ""
	if clone.Ref != nil {
		ref := *clone.Ref
		clone.Ref = &ref
	}
	if clone.Snapshot != nil {
		pack := *clone.Snapshot
		clone.Snapshot = &pack
	}
	clone.SchemaVersion = contracts.PolicySchemaVersion
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
