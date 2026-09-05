package validation

import (
	"context"
	"fmt"
	"strings"

	"github.com/omaveda/fornix/internal/change"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/ingest"
)

const builtinValidatorVersion = "1"

// NewDefaultRegistry returns the built-in, read-only validator set used by
// the reference workflow.
func NewDefaultRegistry() (*Registry, error) {
	registry := NewRegistry()
	for _, validator := range []Validator{
		builtinValidator{id: contracts.ValidationValidatorPreconditions, check: validatePreconditions},
		builtinValidator{id: contracts.ValidationValidatorFiles, check: validateFiles},
		builtinValidator{id: contracts.ValidationValidatorSafety, check: validateSafety},
		builtinValidator{id: contracts.ValidationValidatorTree, check: validateTree},
		builtinValidator{id: contracts.ValidationValidatorReindex, check: validateReindex},
	} {
		if err := registry.Register(validator); err != nil {
			return nil, err
		}
	}
	return registry, nil
}

type builtinValidator struct {
	id    string
	check func(context.Context, Input) (bool, string, *contracts.ValidationFailure, error)
}

func (v builtinValidator) Definition() contracts.ValidatorRef {
	return contracts.ValidatorRef{ID: v.id, Version: builtinValidatorVersion}
}

func (v builtinValidator) Validate(ctx context.Context, input Input) (contracts.ValidationResult, error) {
	passed, summary, failure, err := v.check(ctx, input)
	if err != nil {
		return contracts.ValidationResult{}, err
	}
	outcome := contracts.ValidationOutcomePassed
	status := contracts.ValidationPassed
	if !passed {
		outcome = contracts.ValidationOutcomeFailed
		status = contracts.ValidationFailed
	}
	result := contracts.ValidationResult{Status: status, Outcome: outcome, Summary: summary, Files: input.Observation.Files, Bytes: input.Observation.Bytes, Evidence: []contracts.ValidationEvidence{{Kind: "change_application", SourceReference: input.Request.ChangeApplicationID, Hash: input.Request.PacketHash, Role: "validated_change"}}}
	if failure != nil {
		result.Failure = failure
	}
	return result, nil
}

func validatePreconditions(ctx context.Context, input Input) (bool, string, *contracts.ValidationFailure, error) {
	if err := ctx.Err(); err != nil {
		return false, "", nil, err
	}
	if input.Observation.Conflict != nil {
		return false, "source precondition conflict", &contracts.ValidationFailure{Code: contracts.ValidationFailureSourceConflict, Message: "source precondition did not hold"}, nil
	}
	return true, "source preconditions verified", nil, nil
}

func validateFiles(ctx context.Context, input Input) (bool, string, *contracts.ValidationFailure, error) {
	if err := ctx.Err(); err != nil {
		return false, "", nil, err
	}
	if input.Observation.Files > input.Plan.Budget.MaxFiles || input.Observation.Bytes > input.Plan.Budget.MaxBytes {
		return false, "filesystem observation exceeds validation budget", &contracts.ValidationFailure{Code: contracts.ValidationFailureBudget, Message: "file or byte validation budget exceeded"}, nil
	}
	return true, fmt.Sprintf("observed %d files within budget", input.Observation.Files), nil, nil
}

func validateSafety(ctx context.Context, input Input) (bool, string, *contracts.ValidationFailure, error) {
	if err := ctx.Err(); err != nil {
		return false, "", nil, err
	}
	if err := ingest.ValidateConfiguredRoot(input.Root, input.MountRoot); err != nil {
		return false, "configured repository root is unsafe", &contracts.ValidationFailure{Code: contracts.ValidationFailureUnsafePath, Message: "repository root is outside the configured mount"}, nil
	}
	for _, operation := range input.Proposal.Operations {
		if _, err := change.NormalizeRelativePath(operation.Path); err != nil {
			return false, "change packet contains an unsafe path", &contracts.ValidationFailure{Code: contracts.ValidationFailureUnsafePath, Message: "change packet path is unsafe"}, nil
		}
		if operation.Destination != "" {
			if _, err := change.NormalizeRelativePath(operation.Destination); err != nil {
				return false, "change packet contains an unsafe destination", &contracts.ValidationFailure{Code: contracts.ValidationFailureUnsafePath, Message: "change packet destination is unsafe"}, nil
			}
		}
	}
	return true, "structured paths remain inside the configured mount", nil, nil
}

func validateTree(ctx context.Context, input Input) (bool, string, *contracts.ValidationFailure, error) {
	if err := ctx.Err(); err != nil {
		return false, "", nil, err
	}
	expected := strings.ToLower(strings.TrimSpace(input.Request.ExpectedTreeHash))
	if input.Application.ResultTreeHash != "" && !strings.EqualFold(input.Application.ResultTreeHash, input.Observation.ResultTreeHash) {
		return false, "observed tree differs from the applied result", &contracts.ValidationFailure{Code: contracts.ValidationFailureRecovery, Message: "applied result tree hash differs from observed state"}, nil
	}
	if expected != input.Observation.ResultTreeHash {
		return false, "observed tree differs from expected tree", &contracts.ValidationFailure{Code: contracts.ValidationFailureRecovery, Message: "observed tree hash differs from expected tree"}, nil
	}
	return true, "result tree hash verified", nil, nil
}

func validateReindex(ctx context.Context, input Input) (bool, string, *contracts.ValidationFailure, error) {
	if err := ctx.Err(); err != nil {
		return false, "", nil, err
	}
	if err := ingest.ValidateConfiguredRoot(input.Root, input.MountRoot); err != nil {
		return false, "re-index root is unavailable", &contracts.ValidationFailure{Code: contracts.ValidationFailureUnsafePath, Message: "re-index root is unavailable"}, nil
	}
	if input.Discovery == nil || strings.TrimSpace(input.Discovery.ManifestHash) == "" {
		return false, "re-index discovery is unavailable", &contracts.ValidationFailure{Code: contracts.ValidationFailureSourceConflict, Message: "re-index discovery did not produce a manifest"}, nil
	}
	if input.Discovery.TotalBytes > input.Plan.Budget.MaxBytes || len(input.Discovery.Files) > input.Plan.Budget.MaxFiles {
		return false, "re-index discovery exceeds validation budget", &contracts.ValidationFailure{Code: contracts.ValidationFailureBudget, Message: "re-index discovery exceeds validation budget"}, nil
	}
	return true, fmt.Sprintf("re-index manifest %s is ready", input.Discovery.ManifestHash), nil, nil
}
