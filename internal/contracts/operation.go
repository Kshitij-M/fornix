package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	MaxOperationSteps        = 64
	MaxOperationDependencies = 16
	MaxOperationFailureText  = 256
)

const (
	OperationStatusCreated          = "created"
	OperationStatusPlanned          = "planned"
	OperationStatusAdmitted         = "admitted"
	OperationStatusRunning          = "running"
	OperationStatusAwaitingApproval = "awaiting_approval"
	OperationStatusAwaitingRetry    = "awaiting_retry"
	OperationStatusAwaitingExternal = "awaiting_external"
	OperationStatusVerifying        = "verifying"
	OperationStatusSucceeded        = "succeeded"
	OperationStatusFailed           = "failed"
	OperationStatusCancelled        = "cancelled"
	OperationStatusRecoveryRequired = "recovery_required"
	OperationStatusDeadLetter       = "dead_letter"
	OperationStatusAbstained        = "abstained"
)

const (
	OperationFailureInvalidRequest    = "invalid_request"
	OperationFailureUnknownCapability = "unknown_capability"
	OperationFailureUnknownResource   = "unknown_resource"
	OperationFailureUnauthorized      = "unauthorized"
	OperationFailureWorkspace         = "workspace_isolation"
	OperationFailureSchema            = "schema_incompatible"
	OperationFailureBudget            = "budget_exceeded"
	OperationFailureTimeout           = "timeout"
	OperationFailureCancelled         = "cancelled"
	OperationFailureConflict          = "conflict"
	OperationFailureExternalUncertain = "external_uncertain"
	OperationFailureAdapter           = "adapter_failure"
)

// OperationReference is a hash-only link from a Work Receipt or another
// authoritative record to a generic operation. It intentionally contains no
// input, output, prompt, credential, or connector payload.
type OperationReference struct {
	SchemaVersion int    `json:"schema_version"`
	WorkspaceID   string `json:"workspace_id"`
	ID            string `json:"id"`
	Hash          string `json:"hash"`
}

// Normalize validates a generic operation link.
func (r *OperationReference) Normalize() error {
	if r == nil {
		return fmt.Errorf("operation reference is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = DomainNeutralSchemaVersion
	}
	if r.SchemaVersion != DomainNeutralSchemaVersion {
		return fmt.Errorf("unsupported operation reference schema_version %d", r.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(r.WorkspaceID)
	if err != nil {
		return err
	}
	id, err := normalizeDomainIdentifier(r.ID, "operation reference id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	hash, err := normalizeDomainHash(r.Hash, "operation reference hash", true)
	if err != nil {
		return err
	}
	r.WorkspaceID, r.ID, r.Hash = workspace, id, hash
	return nil
}

// OperationRequest is the authenticated, typed intent to invoke one
// registered capability against one resource. Raw input is deliberately not a
// field: an adapter must validate and canonicalize input before supplying its
// hash and schema version.
type OperationRequest struct {
	SchemaVersion      int               `json:"schema_version,omitempty"`
	ID                 string            `json:"id,omitempty"`
	RequestID          string            `json:"request_id,omitempty"`
	IdempotencyKey     string            `json:"idempotency_key"`
	CausationID        string            `json:"causation_id,omitempty"`
	CorrelationID      string            `json:"correlation_id,omitempty"`
	WorkspaceID        string            `json:"workspace_id"`
	Actor              ActorRef          `json:"actor"`
	Task               *EntityRef        `json:"task,omitempty"`
	Session            *EntityRef        `json:"session,omitempty"`
	Capability         CapabilityRef     `json:"capability"`
	Target             ResourceRef       `json:"target"`
	InputType          string            `json:"input_type"`
	InputSchemaVersion int               `json:"input_schema_version"`
	InputSchemaHash    string            `json:"input_schema_hash"`
	InputHash          string            `json:"input_hash"`
	Profile            ExecutionProfile  `json:"profile"`
	Metadata           map[string]string `json:"metadata,omitempty"`
}

// Normalize validates and canonicalizes the operation admission boundary.
func (r *OperationRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("operation request is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = DomainNeutralSchemaVersion
	}
	if r.SchemaVersion != DomainNeutralSchemaVersion {
		return fmt.Errorf("unsupported operation schema_version %d", r.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(r.WorkspaceID)
	if err != nil {
		return err
	}
	id, err := normalizeDomainIdentifier(r.ID, "operation id", MaxDomainIDLength, false)
	if err != nil {
		return err
	}
	requestID, err := normalizeDomainIdentifier(r.RequestID, "operation request_id", MaxDomainIDLength, false)
	if err != nil {
		return err
	}
	key, err := normalizeDomainIdentifier(r.IdempotencyKey, "operation idempotency_key", MaxIdempotencyLength, true)
	if err != nil {
		return err
	}
	causation, err := normalizeDomainIdentifier(r.CausationID, "operation causation_id", MaxDomainIDLength, false)
	if err != nil {
		return err
	}
	correlation, err := normalizeDomainIdentifier(r.CorrelationID, "operation correlation_id", MaxDomainIDLength, false)
	if err != nil {
		return err
	}
	if err := normalizeDomainActor(&r.Actor, workspace); err != nil {
		return err
	}
	if err := normalizeDomainEntity(r.Task, "task", workspace); err != nil {
		return err
	}
	if err := normalizeDomainEntity(r.Session, "session", workspace); err != nil {
		return err
	}
	if err := r.Capability.Normalize(); err != nil {
		return fmt.Errorf("operation capability: %w", err)
	}
	if r.Capability.WorkspaceID != workspace {
		return fmt.Errorf("operation capability crosses workspace boundary")
	}
	if err := r.Target.Normalize(); err != nil {
		return fmt.Errorf("operation target: %w", err)
	}
	if r.Target.WorkspaceID != workspace {
		return fmt.Errorf("operation target crosses workspace boundary")
	}
	typ, err := normalizeDomainName(r.InputType, "operation input_type", MaxDomainNameLength, true)
	if err != nil {
		return err
	}
	inputHash, err := normalizeDomainHash(r.InputHash, "operation input_hash", true)
	if err != nil {
		return err
	}
	if r.InputSchemaVersion < 1 || r.InputSchemaVersion > MaxDomainSchemaVersion {
		return fmt.Errorf("operation input_schema_version is required")
	}
	inputSchemaHash, err := normalizeDomainHash(r.InputSchemaHash, "operation input_schema_hash", true)
	if err != nil {
		return err
	}
	if err := r.Profile.Normalize(); err != nil {
		return fmt.Errorf("operation profile: %w", err)
	}
	if err := normalizeDomainMetadata(r.Metadata); err != nil {
		return err
	}
	if id == "" {
		id = NewID("op")
	}
	if requestID == "" {
		requestID = NewID("op-request")
	}
	r.WorkspaceID, r.ID, r.RequestID, r.IdempotencyKey = workspace, id, requestID, key
	r.CausationID, r.CorrelationID, r.InputType, r.InputSchemaHash, r.InputHash = causation, correlation, typ, inputSchemaHash, inputHash
	return nil
}

// CanonicalHash returns the logical operation identity. Delivery identity is
// intentionally excluded so retries with new request IDs compare equal.
func (r OperationRequest) CanonicalHash() (string, error) {
	clone := cloneOperationRequest(r)
	if err := clone.Normalize(); err != nil {
		return "", err
	}
	clone.ID, clone.RequestID, clone.IdempotencyKey, clone.CausationID, clone.CorrelationID = "", "", "", "", ""
	raw, err := json.Marshal(clone)
	if err != nil {
		return "", fmt.Errorf("marshal operation hash: %w", err)
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

// StableHash is the convenience form used by callers that already validated a
// request. It returns an empty string for invalid input rather than hashing an
// unsafe contract.
func (r OperationRequest) StableHash() string {
	hash, _ := r.CanonicalHash()
	return hash
}

func cloneOperationRequest(r OperationRequest) OperationRequest {
	if r.Task != nil {
		task := *r.Task
		r.Task = &task
	}
	if r.Session != nil {
		session := *r.Session
		r.Session = &session
	}
	if r.Metadata != nil {
		r.Metadata = make(map[string]string, len(r.Metadata))
		for key, value := range r.Metadata {
			r.Metadata[key] = value
		}
	}
	return r
}

// OperationStep is one bounded, typed plan node. It contains no executable
// function, shell fragment, or arbitrary parameter payload.
type OperationStep struct {
	ID         string                `json:"id"`
	Ordinal    int                   `json:"ordinal"`
	Kind       string                `json:"kind"`
	Capability CapabilityRef         `json:"capability"`
	Target     ResourceRef           `json:"target"`
	DependsOn  []string              `json:"depends_on,omitempty"`
	Effect     EffectClass           `json:"effect"`
	Profile    ExecutionProfile      `json:"profile"`
	Evidence   []EvidenceRequirement `json:"evidence,omitempty"`
	InputHash  string                `json:"input_hash"`
}

// OperationPlan is the deterministic, bounded DAG selected for an operation.
type OperationPlan struct {
	SchemaVersion int             `json:"schema_version"`
	ID            string          `json:"id"`
	OperationID   string          `json:"operation_id"`
	OperationHash string          `json:"operation_hash"`
	WorkspaceID   string          `json:"workspace_id"`
	Actor         ActorRef        `json:"actor"`
	Steps         []OperationStep `json:"steps"`
}

// Normalize validates step identity, workspace scope, and dependency acyclicity.
func (p *OperationPlan) Normalize() error {
	if p == nil {
		return fmt.Errorf("operation plan is nil")
	}
	if p.SchemaVersion == 0 {
		p.SchemaVersion = DomainNeutralSchemaVersion
	}
	if p.SchemaVersion != DomainNeutralSchemaVersion {
		return fmt.Errorf("unsupported operation plan schema_version %d", p.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(p.WorkspaceID)
	if err != nil {
		return err
	}
	id, err := normalizeDomainIdentifier(p.ID, "operation plan id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	operationID, err := normalizeDomainIdentifier(p.OperationID, "operation plan operation_id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	hash, err := normalizeDomainHash(p.OperationHash, "operation plan operation_hash", true)
	if err != nil {
		return err
	}
	if err := normalizeDomainActor(&p.Actor, workspace); err != nil {
		return err
	}
	if len(p.Steps) == 0 || len(p.Steps) > MaxOperationSteps {
		return fmt.Errorf("operation plan must contain between 1 and %d steps", MaxOperationSteps)
	}
	seen := make(map[string]struct{}, len(p.Steps))
	seenOrdinals := make(map[int]struct{}, len(p.Steps))
	for i := range p.Steps {
		step := &p.Steps[i]
		id, err := normalizeDomainIdentifier(step.ID, "operation step id", MaxDomainIDLength, true)
		if err != nil {
			return fmt.Errorf("steps[%d]: %w", i, err)
		}
		kind, err := normalizeDomainName(step.Kind, "operation step kind", MaxDomainNameLength, true)
		if err != nil {
			return fmt.Errorf("steps[%d]: %w", i, err)
		}
		if step.Ordinal < 0 || step.Ordinal >= MaxOperationSteps {
			return fmt.Errorf("steps[%d]: ordinal is out of bounds", i)
		}
		if _, exists := seenOrdinals[step.Ordinal]; exists {
			return fmt.Errorf("duplicate operation step ordinal %d", step.Ordinal)
		}
		seenOrdinals[step.Ordinal] = struct{}{}
		if _, exists := seen[id]; exists {
			return fmt.Errorf("duplicate operation step id %q", id)
		}
		seen[id] = struct{}{}
		if err := step.Capability.Normalize(); err != nil {
			return fmt.Errorf("steps[%d] capability: %w", i, err)
		}
		if step.Capability.WorkspaceID != workspace {
			return fmt.Errorf("steps[%d] capability crosses workspace boundary", i)
		}
		if err := step.Target.Normalize(); err != nil {
			return fmt.Errorf("steps[%d] target: %w", i, err)
		}
		if step.Target.WorkspaceID != workspace {
			return fmt.Errorf("steps[%d] target crosses workspace boundary", i)
		}
		effect := EffectClass(strings.ToLower(strings.TrimSpace(string(step.Effect))))
		if !effect.valid() {
			return fmt.Errorf("steps[%d] unknown effect %q", i, step.Effect)
		}
		if err := step.Profile.Normalize(); err != nil {
			return fmt.Errorf("steps[%d] profile: %w", i, err)
		}
		inputHash, err := normalizeDomainHash(step.InputHash, "operation step input_hash", true)
		if err != nil {
			return fmt.Errorf("steps[%d]: %w", i, err)
		}
		deps, err := normalizeDomainStrings(step.DependsOn, "operation step depends_on", MaxOperationDependencies)
		if err != nil {
			return fmt.Errorf("steps[%d]: %w", i, err)
		}
		for _, dep := range deps {
			if dep == id {
				return fmt.Errorf("operation step %q depends on itself", id)
			}
		}
		if len(step.Evidence) > MaxDomainReferences {
			return fmt.Errorf("steps[%d] evidence exceeds %d entries", i, MaxDomainReferences)
		}
		for j := range step.Evidence {
			if err := step.Evidence[j].Normalize(); err != nil {
				return fmt.Errorf("steps[%d] evidence[%d]: %w", i, j, err)
			}
			if step.Evidence[j].WorkspaceID != workspace {
				return fmt.Errorf("steps[%d] evidence crosses workspace boundary", i)
			}
		}
		step.ID, step.Kind, step.Effect, step.InputHash, step.DependsOn = id, kind, effect, inputHash, deps
	}
	if err := validateOperationDependencies(p.Steps); err != nil {
		return err
	}
	sort.Slice(p.Steps, func(i, j int) bool {
		if p.Steps[i].Ordinal != p.Steps[j].Ordinal {
			return p.Steps[i].Ordinal < p.Steps[j].Ordinal
		}
		return p.Steps[i].ID < p.Steps[j].ID
	})
	p.WorkspaceID, p.ID, p.OperationID, p.OperationHash = workspace, id, operationID, hash
	return nil
}

func validateOperationDependencies(steps []OperationStep) error {
	ids := make(map[string]struct{}, len(steps))
	deps := make(map[string][]string, len(steps))
	for _, step := range steps {
		ids[step.ID] = struct{}{}
		deps[step.ID] = step.DependsOn
	}
	for id, values := range deps {
		for _, dep := range values {
			if _, ok := ids[dep]; !ok {
				return fmt.Errorf("operation step %q depends on missing step %q", id, dep)
			}
		}
	}
	state := make(map[string]uint8, len(steps))
	var visit func(string) error
	visit = func(id string) error {
		if state[id] == 1 {
			return fmt.Errorf("operation plan contains a dependency cycle at %q", id)
		}
		if state[id] == 2 {
			return nil
		}
		state[id] = 1
		for _, dep := range deps[id] {
			if err := visit(dep); err != nil {
				return err
			}
		}
		state[id] = 2
		return nil
	}
	for _, step := range steps {
		if err := visit(step.ID); err != nil {
			return err
		}
	}
	return nil
}

// StableHash is the logical identity of a normalized plan. Database/delivery
// plan ID and the operation ID are links, not plan content.
func (p OperationPlan) StableHash() string {
	clone := cloneOperationPlan(p)
	if err := clone.Normalize(); err != nil {
		return ""
	}
	clone.ID, clone.OperationID = "", ""
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// OperationEvidenceRef is a workspace-scoped hash-only evidence link.
type OperationEvidenceRef struct {
	WorkspaceID     string `json:"workspace_id"`
	SourceReference string `json:"source_reference"`
	EvidenceHash    string `json:"evidence_hash"`
	Role            string `json:"role,omitempty"`
}

func (r *OperationEvidenceRef) Normalize() error {
	if r == nil {
		return fmt.Errorf("operation evidence reference is nil")
	}
	workspace, err := normalizeDomainWorkspace(r.WorkspaceID)
	if err != nil {
		return err
	}
	source, err := normalizeDomainIdentifier(r.SourceReference, "operation evidence source_reference", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	hash, err := normalizeDomainHash(r.EvidenceHash, "operation evidence evidence_hash", true)
	if err != nil {
		return err
	}
	role, err := normalizeDomainName(r.Role, "operation evidence role", MaxDomainNameLength, false)
	if err != nil {
		return err
	}
	r.WorkspaceID, r.SourceReference, r.EvidenceHash, r.Role = workspace, source, hash, role
	return nil
}

// OperationStepResult is a bounded outcome for one plan node.
type OperationStepResult struct {
	StepID              string                 `json:"step_id"`
	Status              string                 `json:"status"`
	OutputSchemaVersion int                    `json:"output_schema_version,omitempty"`
	OutputSchemaHash    string                 `json:"output_schema_hash,omitempty"`
	OutputHash          string                 `json:"output_hash,omitempty"`
	Evidence            []OperationEvidenceRef `json:"evidence,omitempty"`
	ExternalEffect      *ExternalEffect        `json:"external_effect,omitempty"`
}

// OperationFailure is a stable failure category. Details are represented by a
// hash so driver errors and arbitrary user text do not enter durable contracts.
type OperationFailure struct {
	SchemaVersion        int    `json:"schema_version"`
	WorkspaceID          string `json:"workspace_id"`
	Code                 string `json:"code"`
	Retryable            bool   `json:"retryable"`
	Attempts             int    `json:"attempts,omitempty"`
	DetailHash           string `json:"detail_hash,omitempty"`
	ExternalEffectStatus string `json:"external_effect_status,omitempty"`
}

func (f *OperationFailure) Normalize() error {
	if f == nil {
		return fmt.Errorf("operation failure is nil")
	}
	if f.SchemaVersion == 0 {
		f.SchemaVersion = DomainNeutralSchemaVersion
	}
	if f.SchemaVersion != DomainNeutralSchemaVersion {
		return fmt.Errorf("unsupported operation failure schema_version %d", f.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(f.WorkspaceID)
	if err != nil {
		return err
	}
	code := strings.ToLower(strings.TrimSpace(f.Code))
	if !validOperationFailureCode(code) {
		return fmt.Errorf("unknown operation failure code %q", f.Code)
	}
	if f.Attempts < 0 || f.Attempts > MaxExecutionRetries+1 {
		return fmt.Errorf("operation failure attempts are out of bounds")
	}
	detail, err := normalizeDomainHash(f.DetailHash, "operation failure detail_hash", false)
	if err != nil {
		return err
	}
	status := strings.ToLower(strings.TrimSpace(f.ExternalEffectStatus))
	if status != "" && !validExternalVerification(status) && !validExternalCompensation(status) {
		return fmt.Errorf("unknown operation failure external effect status %q", status)
	}
	f.WorkspaceID, f.Code, f.DetailHash, f.ExternalEffectStatus = workspace, code, detail, status
	return nil
}

func validOperationFailureCode(code string) bool {
	switch code {
	case OperationFailureInvalidRequest, OperationFailureUnknownCapability, OperationFailureUnknownResource,
		OperationFailureUnauthorized, OperationFailureWorkspace, OperationFailureSchema, OperationFailureBudget,
		OperationFailureTimeout, OperationFailureCancelled, OperationFailureConflict, OperationFailureExternalUncertain,
		OperationFailureAdapter:
		return true
	default:
		return false
	}
}

// OperationResult is the durable, hash-only result boundary for one operation.
type OperationResult struct {
	SchemaVersion       int                    `json:"schema_version"`
	ID                  string                 `json:"id"`
	OperationID         string                 `json:"operation_id"`
	OperationHash       string                 `json:"operation_hash"`
	RequestID           string                 `json:"request_id,omitempty"`
	WorkspaceID         string                 `json:"workspace_id"`
	Actor               ActorRef               `json:"actor"`
	Status              string                 `json:"status"`
	OutputSchemaVersion int                    `json:"output_schema_version,omitempty"`
	OutputSchemaHash    string                 `json:"output_schema_hash,omitempty"`
	OutputHash          string                 `json:"output_hash,omitempty"`
	ReportHash          string                 `json:"report_hash,omitempty"`
	Steps               []OperationStepResult  `json:"steps,omitempty"`
	Evidence            []OperationEvidenceRef `json:"evidence,omitempty"`
	ExternalEffects     []ExternalEffect       `json:"external_effects,omitempty"`
	Failure             *OperationFailure      `json:"failure,omitempty"`
	StartedAt           time.Time              `json:"started_at,omitempty"`
	CompletedAt         time.Time              `json:"completed_at,omitempty"`
}

// Normalize validates a result and all nested workspace-scoped evidence.
func (r *OperationResult) Normalize() error {
	if r == nil {
		return fmt.Errorf("operation result is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = DomainNeutralSchemaVersion
	}
	if r.SchemaVersion != DomainNeutralSchemaVersion {
		return fmt.Errorf("unsupported operation result schema_version %d", r.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(r.WorkspaceID)
	if err != nil {
		return err
	}
	id, err := normalizeDomainIdentifier(r.ID, "operation result id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	operationID, err := normalizeDomainIdentifier(r.OperationID, "operation result operation_id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	opHash, err := normalizeDomainHash(r.OperationHash, "operation result operation_hash", true)
	if err != nil {
		return err
	}
	requestID, err := normalizeDomainIdentifier(r.RequestID, "operation result request_id", MaxDomainIDLength, false)
	if err != nil {
		return err
	}
	if err := normalizeDomainActor(&r.Actor, workspace); err != nil {
		return err
	}
	status := strings.ToLower(strings.TrimSpace(r.Status))
	if !validOperationStatus(status) {
		return fmt.Errorf("unknown operation result status %q", r.Status)
	}
	outHash, err := normalizeDomainHash(r.OutputHash, "operation result output_hash", false)
	if err != nil {
		return err
	}
	outSchemaHash, err := normalizeDomainHash(r.OutputSchemaHash, "operation result output_schema_hash", false)
	if err != nil {
		return err
	}
	if r.OutputSchemaVersion < 0 || r.OutputSchemaVersion > MaxDomainSchemaVersion || (r.OutputSchemaVersion == 0) != (outSchemaHash == "") {
		return fmt.Errorf("operation result output schema identity is incomplete")
	}
	reportHash, err := normalizeDomainHash(r.ReportHash, "operation result report_hash", false)
	if err != nil {
		return err
	}
	if len(r.Steps) > MaxOperationSteps || len(r.Evidence) > MaxDomainReferences || len(r.ExternalEffects) > MaxDomainReferences {
		return fmt.Errorf("operation result exceeds bounded references")
	}
	seenSteps := make(map[string]struct{}, len(r.Steps))
	for i := range r.Steps {
		step := &r.Steps[i]
		id, err := normalizeDomainIdentifier(step.StepID, "operation result step_id", MaxDomainIDLength, true)
		if err != nil {
			return fmt.Errorf("steps[%d]: %w", i, err)
		}
		if _, exists := seenSteps[id]; exists {
			return fmt.Errorf("duplicate operation result step_id %q", id)
		}
		seenSteps[id] = struct{}{}
		stepStatus := strings.ToLower(strings.TrimSpace(step.Status))
		if !validOperationStatus(stepStatus) {
			return fmt.Errorf("steps[%d] unknown status %q", i, step.Status)
		}
		output, err := normalizeDomainHash(step.OutputHash, "operation result step output_hash", false)
		if err != nil {
			return fmt.Errorf("steps[%d]: %w", i, err)
		}
		stepOutputSchemaHash, err := normalizeDomainHash(step.OutputSchemaHash, "operation result step output_schema_hash", false)
		if err != nil {
			return fmt.Errorf("steps[%d]: %w", i, err)
		}
		if step.OutputSchemaVersion < 0 || step.OutputSchemaVersion > MaxDomainSchemaVersion || (step.OutputSchemaVersion == 0) != (stepOutputSchemaHash == "") {
			return fmt.Errorf("steps[%d] output schema identity is incomplete", i)
		}
		if len(step.Evidence) > MaxDomainReferences {
			return fmt.Errorf("steps[%d] evidence exceeds bounds", i)
		}
		for j := range step.Evidence {
			if err := step.Evidence[j].Normalize(); err != nil {
				return fmt.Errorf("steps[%d] evidence[%d]: %w", i, j, err)
			}
			if step.Evidence[j].WorkspaceID != workspace {
				return fmt.Errorf("steps[%d] evidence crosses workspace boundary", i)
			}
		}
		if step.ExternalEffect != nil {
			if err := step.ExternalEffect.Normalize(); err != nil {
				return fmt.Errorf("steps[%d] external effect: %w", i, err)
			}
			if step.ExternalEffect.WorkspaceID != workspace {
				return fmt.Errorf("steps[%d] external effect crosses workspace boundary", i)
			}
		}
		step.StepID, step.Status, step.OutputSchemaHash, step.OutputHash = id, stepStatus, stepOutputSchemaHash, output
	}
	sort.Slice(r.Steps, func(i, j int) bool { return r.Steps[i].StepID < r.Steps[j].StepID })
	for i := range r.Evidence {
		if err := r.Evidence[i].Normalize(); err != nil {
			return fmt.Errorf("evidence[%d]: %w", i, err)
		}
		if r.Evidence[i].WorkspaceID != workspace {
			return fmt.Errorf("result evidence crosses workspace boundary")
		}
	}
	for i := range r.ExternalEffects {
		if err := r.ExternalEffects[i].Normalize(); err != nil {
			return fmt.Errorf("external_effects[%d]: %w", i, err)
		}
		if r.ExternalEffects[i].WorkspaceID != workspace {
			return fmt.Errorf("result external effect crosses workspace boundary")
		}
	}
	if r.Failure != nil {
		if err := r.Failure.Normalize(); err != nil {
			return err
		}
		if r.Failure.WorkspaceID != workspace {
			return fmt.Errorf("result failure crosses workspace boundary")
		}
	}
	if status == OperationStatusFailed && r.Failure == nil {
		return fmt.Errorf("failed operation result requires failure")
	}
	if status != OperationStatusFailed && r.Failure != nil {
		return fmt.Errorf("operation failure is only valid for failed results")
	}
	r.WorkspaceID, r.ID, r.OperationID, r.OperationHash, r.RequestID, r.Status, r.OutputSchemaHash, r.OutputHash, r.ReportHash = workspace, id, operationID, opHash, requestID, status, outSchemaHash, outHash, reportHash
	return nil
}

func validOperationStatus(status string) bool {
	switch status {
	case OperationStatusCreated, OperationStatusPlanned, OperationStatusAdmitted,
		OperationStatusRunning, OperationStatusAwaitingApproval,
		OperationStatusAwaitingRetry, OperationStatusAwaitingExternal, OperationStatusSucceeded,
		OperationStatusVerifying, OperationStatusFailed, OperationStatusCancelled,
		OperationStatusRecoveryRequired, OperationStatusDeadLetter, OperationStatusAbstained:
		return true
	default:
		return false
	}
}

// IsTerminalOperationStatus reports whether an operation cannot accept another
// lifecycle transition. Terminality is part of the durable authority rather
// than an adapter convention, so callers must use this predicate before
// attempting to execute or resume work.
func IsTerminalOperationStatus(status string) bool {
	switch status {
	case OperationStatusSucceeded, OperationStatusFailed, OperationStatusCancelled,
		OperationStatusDeadLetter, OperationStatusAbstained:
		return true
	default:
		return false
	}
}

// CanTransitionOperation reports the fail-closed lifecycle graph shared by
// operation stores, workflow runtimes, and replay validators.
func CanTransitionOperation(from, to string) bool {
	if !validOperationStatus(from) || !validOperationStatus(to) || IsTerminalOperationStatus(from) {
		return false
	}
	switch from {
	case OperationStatusCreated:
		return to == OperationStatusPlanned || to == OperationStatusCancelled
	case OperationStatusPlanned:
		return to == OperationStatusAdmitted || to == OperationStatusCancelled || to == OperationStatusFailed
	case OperationStatusAdmitted:
		return to == OperationStatusAwaitingApproval || to == OperationStatusRunning || to == OperationStatusCancelled || to == OperationStatusFailed || to == OperationStatusAbstained
	case OperationStatusAwaitingApproval:
		return to == OperationStatusRunning || to == OperationStatusCancelled || to == OperationStatusFailed
	case OperationStatusRunning:
		return to == OperationStatusAwaitingRetry || to == OperationStatusAwaitingExternal || to == OperationStatusVerifying ||
			to == OperationStatusSucceeded || to == OperationStatusFailed || to == OperationStatusCancelled ||
			to == OperationStatusRecoveryRequired || to == OperationStatusDeadLetter || to == OperationStatusAbstained
	case OperationStatusAwaitingRetry:
		return to == OperationStatusRunning || to == OperationStatusCancelled || to == OperationStatusDeadLetter
	case OperationStatusAwaitingExternal:
		return to == OperationStatusVerifying || to == OperationStatusRecoveryRequired || to == OperationStatusFailed || to == OperationStatusCancelled
	case OperationStatusVerifying:
		return to == OperationStatusSucceeded || to == OperationStatusFailed || to == OperationStatusRecoveryRequired
	case OperationStatusRecoveryRequired:
		return to == OperationStatusRunning || to == OperationStatusVerifying || to == OperationStatusSucceeded || to == OperationStatusFailed || to == OperationStatusDeadLetter
	default:
		return false
	}
}

// StableHash excludes result database identity, request delivery identity, and
// wall-clock timestamps while retaining the verified operation outcome.
func (r OperationResult) StableHash() string {
	clone := cloneOperationResult(r)
	if err := clone.Normalize(); err != nil {
		return ""
	}
	clone.ID, clone.RequestID, clone.StartedAt, clone.CompletedAt = "", "", time.Time{}, time.Time{}
	for i := range clone.Steps {
		if clone.Steps[i].ExternalEffect != nil {
			clone.Steps[i].ExternalEffect.ID = ""
			clone.Steps[i].ExternalEffect.IdempotencyKey = ""
			clone.Steps[i].ExternalEffect.ProviderRequestID = ""
		}
	}
	for i := range clone.ExternalEffects {
		clone.ExternalEffects[i].ID = ""
		clone.ExternalEffects[i].IdempotencyKey = ""
		clone.ExternalEffects[i].ProviderRequestID = ""
	}
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func cloneOperationPlan(p OperationPlan) OperationPlan {
	p.Steps = append([]OperationStep(nil), p.Steps...)
	for i := range p.Steps {
		p.Steps[i].DependsOn = append([]string(nil), p.Steps[i].DependsOn...)
		p.Steps[i].Evidence = append([]EvidenceRequirement(nil), p.Steps[i].Evidence...)
		for j := range p.Steps[i].Evidence {
			p.Steps[i].Evidence[j].RequiredFields = append([]string(nil), p.Steps[i].Evidence[j].RequiredFields...)
		}
	}
	return p
}

func cloneOperationResult(r OperationResult) OperationResult {
	r.Steps = append([]OperationStepResult(nil), r.Steps...)
	for i := range r.Steps {
		r.Steps[i].Evidence = append([]OperationEvidenceRef(nil), r.Steps[i].Evidence...)
		if r.Steps[i].ExternalEffect != nil {
			effect := *r.Steps[i].ExternalEffect
			r.Steps[i].ExternalEffect = &effect
		}
	}
	r.Evidence = append([]OperationEvidenceRef(nil), r.Evidence...)
	r.ExternalEffects = append([]ExternalEffect(nil), r.ExternalEffects...)
	if r.Failure != nil {
		failure := *r.Failure
		r.Failure = &failure
	}
	return r
}
