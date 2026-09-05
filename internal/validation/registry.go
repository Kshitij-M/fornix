// Package validation contains the deterministic post-change validation
// runtime. Validators are registered capabilities: they receive bounded,
// hash-only inputs and return bounded results. They cannot execute commands,
// call model providers, or mutate repository files.
package validation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/omaveda/fornix/internal/change"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/ingest"
)

// Input is the bounded input shared by registered validators. Contents are
// never carried in this structure; source and change identity are represented
// by hashes and the filesystem observation is hash-only.
type Input struct {
	Request     contracts.ValidationRequest
	Plan        contracts.ValidationPlan
	Proposal    contracts.ChangeProposal
	Application contracts.ChangeApplication
	Root        string
	MountRoot   string
	Observation change.PacketStateObservation
	Discovery   *ingest.DiscoveryResult
	SourceHash  string
}

// Validator is a deterministic, structured check. Returning a failed result
// is a normal validation outcome; returning an error means the runtime could
// not produce a trustworthy result and must fail closed.
type Validator interface {
	Definition() contracts.ValidatorRef
	Validate(context.Context, Input) (contracts.ValidationResult, error)
}

// Registry is an explicit validator capability registry. Registration and
// lookup are exact ID/version matches so a plan cannot silently select a
// different implementation.
type Registry struct {
	validators map[string]Validator
}

// NewRegistry creates an empty registry.
func NewRegistry() *Registry {
	return &Registry{validators: make(map[string]Validator)}
}

// Register adds one validator and rejects duplicate identities.
func (r *Registry) Register(validator Validator) error {
	if r == nil {
		return fmt.Errorf("validation registry is nil")
	}
	if validator == nil {
		return fmt.Errorf("validator is nil")
	}
	definition := validator.Definition()
	definition.ID = strings.TrimSpace(definition.ID)
	definition.Version = strings.TrimSpace(definition.Version)
	if definition.ID == "" || definition.Version == "" {
		return fmt.Errorf("validator id and version are required")
	}
	if r.validators == nil {
		r.validators = make(map[string]Validator)
	}
	key := validatorKey(definition)
	if _, exists := r.validators[key]; exists {
		return fmt.Errorf("validator %s@%s is already registered", definition.ID, definition.Version)
	}
	r.validators[key] = validator
	return nil
}

// Lookup returns the exact registered validator for a plan reference.
func (r *Registry) Lookup(reference contracts.ValidatorRef) (Validator, bool) {
	if r == nil {
		return nil, false
	}
	validator, ok := r.validators[validatorKey(reference)]
	return validator, ok
}

// ValidatePlan executes a normalized plan in canonical ordinal order. The
// runtime assigns deterministic result identities and input hashes, while the
// validators control only bounded summaries and outcomes.
func (r *Registry) ValidatePlan(ctx context.Context, input Input) ([]contracts.ValidationResult, error) {
	if r == nil {
		return nil, fmt.Errorf("validation registry is nil")
	}
	refs := append([]contracts.ValidatorRef(nil), input.Plan.Validators...)
	sort.Slice(refs, func(i, j int) bool {
		if refs[i].ID != refs[j].ID {
			return refs[i].ID < refs[j].ID
		}
		return refs[i].Version < refs[j].Version
	})
	results := make([]contracts.ValidationResult, 0, len(refs))
	for ordinal, reference := range refs {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		validator, ok := r.Lookup(reference)
		if !ok {
			return nil, fmt.Errorf("validator %s@%s is not registered", reference.ID, reference.Version)
		}
		result, err := validator.Validate(ctx, input)
		if err != nil {
			return nil, fmt.Errorf("validator %s@%s: %w", reference.ID, reference.Version, err)
		}
		result.SchemaVersion = contracts.ValidationSchemaVersion
		result.RunID = input.Request.ID
		result.WorkspaceID = input.Request.WorkspaceID
		result.Ordinal = ordinal
		result.Validator = reference
		result.Attempt = 1
		result.ID = fmt.Sprintf("%s-result-%03d", input.Request.ID, ordinal)
		result.InputHash = validationInputHash(input, reference)
		if result.Status == "" {
			result.Status = result.Outcome
		}
		if result.Outcome == "" {
			result.Outcome = result.Status
		}
		if result.ResultHash == "" {
			result.ResultHash = result.StableHash()
		}
		if result.Summary == "" {
			result.Summary = "validator completed"
		}
		if len(result.Summary) > 8192 {
			return nil, fmt.Errorf("validator %s returned oversized summary", reference.ID)
		}
		if result.Files < 0 || result.Bytes < 0 || result.SQLQueries < 0 || result.DurationMS < 0 {
			return nil, fmt.Errorf("validator %s returned negative measurement", reference.ID)
		}
		if len(result.Evidence) > contracts.MaxValidationEvidence {
			return nil, fmt.Errorf("validator %s returned too many evidence references", reference.ID)
		}
		results = append(results, result)
	}
	return results, nil
}

func validatorKey(reference contracts.ValidatorRef) string {
	return strings.TrimSpace(reference.ID) + "\x00" + strings.TrimSpace(reference.Version)
}

func validationInputHash(input Input, reference contracts.ValidatorRef) string {
	value := struct {
		WorkspaceID    string                 `json:"workspace_id"`
		RequestHash    string                 `json:"request_hash"`
		PacketHash     string                 `json:"packet_hash"`
		ExpectedTree   string                 `json:"expected_tree_hash"`
		ObservedTree   string                 `json:"observed_tree_hash"`
		Validator      contracts.ValidatorRef `json:"validator"`
		SourceManifest string                 `json:"source_manifest_hash,omitempty"`
	}{input.Request.WorkspaceID, input.Plan.RequestHash, input.Request.PacketHash, input.Request.ExpectedTreeHash, input.Observation.ResultTreeHash, reference, input.SourceHash}
	raw, _ := json.Marshal(value)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
