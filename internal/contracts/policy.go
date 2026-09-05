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

// PolicySchemaVersion is the durable contract version for validation policy
// packs. Policy bodies are immutable after creation; lifecycle state is held
// by the policy authority and its append-only audit history.
const PolicySchemaVersion = 1

const (
	MaxPolicyIDLength       = 128
	MaxPolicyVersionLength  = 64
	MaxPolicyRules          = 64
	MaxPolicyApprovalScopes = 64
	MaxPolicyOperationTypes = 64
	MaxPolicyAuditReason    = 4096
	MaxPolicyDocumentBytes  = 64 << 10
	MaxPolicyPageSize       = 100
)

// PolicyLifecycleStatus is the durable admission state of one immutable
// policy version. Only active versions can admit new work.
type PolicyLifecycleStatus string

const (
	PolicyDraft   PolicyLifecycleStatus = "draft"
	PolicyActive  PolicyLifecycleStatus = "active"
	PolicyRetired PolicyLifecycleStatus = "retired"
)

const (
	PolicyApprovalAutomatic = "automatic"
	PolicyApprovalRequired  = "required"
	PolicyApprovalDenied    = "denied"
)

// ValidationPolicyRef identifies one exact workspace-owned policy version.
// Hash is optional on input and required on a resolved/persisted reference.
type ValidationPolicyRef struct {
	SchemaVersion int    `json:"schema_version,omitempty"`
	WorkspaceID   string `json:"workspace_id"`
	PolicyID      string `json:"policy_id"`
	Version       string `json:"version"`
	PolicyHash    string `json:"policy_hash,omitempty"`
}

// PolicyBudget contains change and validation hard limits. Values are in
// operations/files/bytes/milliseconds/queries/retries/report bytes according
// to the nested contract types; zero values receive safe defaults.
type PolicyBudget struct {
	Change     ChangeBudgets    `json:"change"`
	Validation ValidationBudget `json:"validation"`
}

// PolicyApprovalConfig declares the minimum approval mode for new changes.
// It cannot weaken a caller's stricter request or bypass a mandatory approval.
type PolicyApprovalConfig struct {
	Mode       string   `json:"mode"`
	RequireFor []string `json:"require_for,omitempty"`
}

// PolicySafetyFloors are non-disableable controls. Normalization sets every
// floor to true so a declarative policy cannot weaken the authority boundary;
// the values remain explicit for auditability.
type PolicySafetyFloors struct {
	WorkspaceIsolation bool `json:"workspace_isolation"`
	ActorPropagation   bool `json:"actor_propagation"`
	TaskFencing        bool `json:"task_fencing"`
	EvidenceIntegrity  bool `json:"evidence_integrity"`
	AppendOnlyHistory  bool `json:"append_only_history"`
	ReplaySafety       bool `json:"replay_safety"`
}

// ValidationPolicyRule selects one registered deterministic validator. A rule
// contains references only; it cannot carry shell commands, SQL, prompts,
// credentials, or executable callbacks.
type ValidationPolicyRule struct {
	Validator ValidatorRef `json:"validator"`
	Required  bool         `json:"required"`
}

// ValidationPolicyPack is the normalized declarative policy body. The policy
// store owns its immutable persisted copy; PolicyHash is derived content.
type ValidationPolicyPack struct {
	SchemaVersion    int                    `json:"schema_version"`
	WorkspaceID      string                 `json:"workspace_id"`
	PolicyID         string                 `json:"policy_id"`
	Version          string                 `json:"version"`
	PolicyHash       string                 `json:"policy_hash,omitempty"`
	Rules            []ValidationPolicyRule `json:"rules"`
	Budget           PolicyBudget           `json:"budget"`
	Approval         PolicyApprovalConfig   `json:"approval"`
	RequireReindex   bool                   `json:"require_reindex"`
	RequireTaskFence bool                   `json:"require_task_fence"`
	SafetyFloors     PolicySafetyFloors     `json:"safety_floors"`
}

// ValidationPolicyVersion is an immutable version plus its lifecycle view.
// Status transitions are authoritative audit records, not body mutations.
type ValidationPolicyVersion struct {
	SchemaVersion int                   `json:"schema_version"`
	WorkspaceID   string                `json:"workspace_id"`
	PolicyID      string                `json:"policy_id"`
	Version       string                `json:"version"`
	PolicyHash    string                `json:"policy_hash"`
	Pack          ValidationPolicyPack  `json:"pack"`
	Status        PolicyLifecycleStatus `json:"status"`
	Actor         ActorRef              `json:"actor"`
	CreatedAt     time.Time             `json:"created_at"`
	UpdatedAt     time.Time             `json:"updated_at"`
	ActivatedAt   *time.Time            `json:"activated_at,omitempty"`
	RetiredAt     *time.Time            `json:"retired_at,omitempty"`
}

// PolicyResolution is the immutable decision used by change/validation
// admission. Selected=false is the backwards-compatible implicit policy.
type PolicyResolution struct {
	SchemaVersion    int                   `json:"schema_version"`
	WorkspaceID      string                `json:"workspace_id"`
	Selected         bool                  `json:"selected"`
	Ref              *ValidationPolicyRef  `json:"ref,omitempty"`
	Snapshot         *ValidationPolicyPack `json:"snapshot,omitempty"`
	Validators       []ValidatorRef        `json:"validators"`
	Budget           PolicyBudget          `json:"budget"`
	ApprovalMode     string                `json:"approval_mode"`
	RequireReindex   bool                  `json:"require_reindex"`
	RequireTaskFence bool                  `json:"require_task_fence"`
	ResolutionHash   string                `json:"resolution_hash"`
}

// PolicyEvaluationRequest is the bounded input to pure policy resolution.
// Caller values may tighten a policy but cannot widen its limits.
type PolicyEvaluationRequest struct {
	SchemaVersion         int                  `json:"schema_version,omitempty"`
	WorkspaceID           string               `json:"workspace_id"`
	Policy                *ValidationPolicyRef `json:"policy,omitempty"`
	RequestedValidators   []ValidatorRef       `json:"requested_validators,omitempty"`
	RequestedBudget       PolicyBudget         `json:"requested_budget,omitempty"`
	RequestedApprovalMode string               `json:"requested_approval_mode,omitempty"`
	// Operation and OperationTypes let approval rules apply to a known
	// admission scope. An empty scope is handled conservatively when a policy
	// declares RequireFor values.
	Operation        string   `json:"operation,omitempty"`
	OperationTypes   []string `json:"operation_types,omitempty"`
	RequireReindex   bool     `json:"require_reindex,omitempty"`
	RequireTaskFence bool     `json:"require_task_fence,omitempty"`
	Actor            ActorRef `json:"actor,omitempty"`
	RequestID        string   `json:"request_id,omitempty"`
	IdempotencyKey   string   `json:"idempotency_key,omitempty"`
	CausationID      string   `json:"causation_id,omitempty"`
	CorrelationID    string   `json:"correlation_id,omitempty"`
}

// PolicyAuditRecord is append-only evidence of a lifecycle or resolution
// decision. It contains no policy secrets or arbitrary document text.
type PolicyAuditRecord struct {
	ID             int64                 `json:"id"`
	SchemaVersion  int                   `json:"schema_version"`
	WorkspaceID    string                `json:"workspace_id"`
	PolicyID       string                `json:"policy_id"`
	Version        string                `json:"version"`
	PolicyHash     string                `json:"policy_hash"`
	Operation      string                `json:"operation"`
	FromStatus     PolicyLifecycleStatus `json:"from_status,omitempty"`
	ToStatus       PolicyLifecycleStatus `json:"to_status,omitempty"`
	Actor          AuditActor            `json:"actor"`
	RequestID      string                `json:"request_id"`
	IdempotencyKey string                `json:"idempotency_key,omitempty"`
	Allowed        bool                  `json:"allowed"`
	Reason         string                `json:"reason,omitempty"`
	CreatedAt      time.Time             `json:"created_at"`
}

// PolicyCreateRequest is the authenticated input for one immutable version.
type PolicyCreateRequest struct {
	SchemaVersion  int                  `json:"schema_version,omitempty"`
	RequestID      string               `json:"request_id,omitempty"`
	IdempotencyKey string               `json:"idempotency_key"`
	WorkspaceID    string               `json:"workspace_id"`
	Actor          ActorRef             `json:"actor,omitempty"`
	Pack           ValidationPolicyPack `json:"pack"`
}

// PolicyLifecycleRequest carries one idempotent policy state transition.
type PolicyLifecycleRequest struct {
	SchemaVersion  int                 `json:"schema_version,omitempty"`
	RequestID      string              `json:"request_id,omitempty"`
	IdempotencyKey string              `json:"idempotency_key"`
	WorkspaceID    string              `json:"workspace_id"`
	Policy         ValidationPolicyRef `json:"policy"`
	Actor          ActorRef            `json:"actor,omitempty"`
	Reason         string              `json:"reason,omitempty"`
}

// PolicyCompareRequest selects two exact versions in one workspace.
type PolicyCompareRequest struct {
	WorkspaceID string              `json:"workspace_id"`
	Left        ValidationPolicyRef `json:"left"`
	Right       ValidationPolicyRef `json:"right"`
}

// PolicyComparison is a bounded deterministic difference between versions.
type PolicyComparison struct {
	WorkspaceID string              `json:"workspace_id"`
	Left        ValidationPolicyRef `json:"left"`
	Right       ValidationPolicyRef `json:"right"`
	Changed     []string            `json:"changed"`
	Same        bool                `json:"same"`
	Hash        string              `json:"hash"`
}

func (r *ValidationPolicyRef) Normalize() error {
	if r == nil {
		return fmt.Errorf("policy reference is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = PolicySchemaVersion
	}
	if r.SchemaVersion != PolicySchemaVersion {
		return fmt.Errorf("unsupported policy schema_version %d", r.SchemaVersion)
	}
	r.WorkspaceID, r.PolicyID, r.Version, r.PolicyHash = strings.TrimSpace(r.WorkspaceID), strings.TrimSpace(r.PolicyID), strings.TrimSpace(r.Version), strings.ToLower(strings.TrimSpace(r.PolicyHash))
	if r.WorkspaceID == "" || r.PolicyID == "" || r.Version == "" {
		return fmt.Errorf("policy workspace_id, policy_id, and version are required")
	}
	if len(r.PolicyID) > MaxPolicyIDLength || len(r.Version) > MaxPolicyVersionLength {
		return fmt.Errorf("policy identity is too large")
	}
	if r.PolicyHash != "" && !validSHA256(r.PolicyHash) {
		return fmt.Errorf("policy_hash must be a lowercase SHA-256 hash")
	}
	return nil
}

func (p *PolicyApprovalConfig) Normalize() error {
	if p == nil {
		return fmt.Errorf("policy approval config is nil")
	}
	p.Mode = strings.ToLower(strings.TrimSpace(p.Mode))
	if p.Mode == "" {
		p.Mode = PolicyApprovalRequired
	}
	if p.Mode != PolicyApprovalAutomatic && p.Mode != PolicyApprovalRequired && p.Mode != PolicyApprovalDenied {
		return fmt.Errorf("unsupported policy approval mode %q", p.Mode)
	}
	if len(p.RequireFor) > MaxPolicyApprovalScopes {
		return fmt.Errorf("too many policy approval scopes")
	}
	seen := map[string]struct{}{}
	out := make([]string, 0, len(p.RequireFor))
	for _, value := range p.RequireFor {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if len(value) > MaxPolicyIDLength {
			return fmt.Errorf("policy approval scope is too large")
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	p.RequireFor = out
	return nil
}

func (b *PolicyBudget) Normalize() error {
	if b == nil {
		return fmt.Errorf("policy budget is nil")
	}
	if b.Change.MaxOperations == 0 {
		b.Change.MaxOperations = DefaultChangeMaxOperations
	}
	if b.Change.MaxFileBytes == 0 {
		b.Change.MaxFileBytes = DefaultChangeMaxFileBytes
	}
	if b.Change.MaxTotalBytes == 0 {
		b.Change.MaxTotalBytes = DefaultChangeMaxTotalBytes
	}
	if b.Change.MaxOperations < 1 || b.Change.MaxOperations > MaxChangeOperations || b.Change.MaxFileBytes < 1 || b.Change.MaxFileBytes > MaxChangeFileBytes || b.Change.MaxTotalBytes < 1 || b.Change.MaxTotalBytes > MaxChangeTotalBytes {
		return fmt.Errorf("policy change budget exceeds global limits")
	}
	if err := b.Validation.Normalize(); err != nil {
		return err
	}
	return nil
}

func (p *ValidationPolicyPack) Normalize() error {
	if p == nil {
		return fmt.Errorf("policy pack is nil")
	}
	if p.SchemaVersion == 0 {
		p.SchemaVersion = PolicySchemaVersion
	}
	if p.SchemaVersion != PolicySchemaVersion {
		return fmt.Errorf("unsupported policy schema_version %d", p.SchemaVersion)
	}
	p.WorkspaceID, p.PolicyID, p.Version = strings.TrimSpace(p.WorkspaceID), strings.TrimSpace(p.PolicyID), strings.TrimSpace(p.Version)
	if p.WorkspaceID == "" || p.PolicyID == "" || p.Version == "" {
		return fmt.Errorf("policy workspace_id, policy_id, and version are required")
	}
	if len(p.PolicyID) > MaxPolicyIDLength || len(p.Version) > MaxPolicyVersionLength || len(p.Rules) > MaxPolicyRules {
		return fmt.Errorf("policy identity or rule count exceeds bounds")
	}
	suppliedHash := strings.ToLower(strings.TrimSpace(p.PolicyHash))
	if err := p.Budget.Normalize(); err != nil {
		return err
	}
	if err := p.Approval.Normalize(); err != nil {
		return err
	}
	p.RequireTaskFence = true
	p.SafetyFloors = PolicySafetyFloors{WorkspaceIsolation: true, ActorPropagation: true, TaskFencing: true, EvidenceIntegrity: true, AppendOnlyHistory: true, ReplaySafety: true}
	seen := make(map[string]struct{}, len(p.Rules))
	for i := range p.Rules {
		p.Rules[i].Validator.ID = strings.TrimSpace(p.Rules[i].Validator.ID)
		p.Rules[i].Validator.Version = strings.TrimSpace(p.Rules[i].Validator.Version)
		if p.Rules[i].Validator.ID == "" || p.Rules[i].Validator.Version == "" {
			return fmt.Errorf("policy rule %d has incomplete validator reference", i)
		}
		if len(p.Rules[i].Validator.ID) > MaxPolicyIDLength || len(p.Rules[i].Validator.Version) > MaxPolicyVersionLength {
			return fmt.Errorf("policy rule %d validator reference is too large", i)
		}
		key := p.Rules[i].Validator.ID + "\x00" + p.Rules[i].Validator.Version
		if _, ok := seen[key]; ok {
			return fmt.Errorf("policy contains duplicate validator %q", p.Rules[i].Validator.ID)
		}
		seen[key] = struct{}{}
	}
	sort.Slice(p.Rules, func(i, j int) bool {
		if p.Rules[i].Validator.ID != p.Rules[j].Validator.ID {
			return p.Rules[i].Validator.ID < p.Rules[j].Validator.ID
		}
		return p.Rules[i].Validator.Version < p.Rules[j].Validator.Version
	})
	p.PolicyHash = p.ComputeHash()
	if suppliedHash != "" && suppliedHash != p.PolicyHash {
		return fmt.Errorf("policy_hash does not match normalized policy body")
	}
	document, err := json.Marshal(p)
	if err != nil {
		return fmt.Errorf("marshal policy document: %w", err)
	}
	if len(document) > MaxPolicyDocumentBytes {
		return fmt.Errorf("policy document exceeds %d bytes", MaxPolicyDocumentBytes)
	}
	return nil
}

// ComputeHash returns the canonical content identity, excluding volatile and
// delivery fields. It is safe to call before or after Normalize.
func (p ValidationPolicyPack) ComputeHash() string {
	clone := p
	clone.PolicyHash = ""
	clone.Rules = append([]ValidationPolicyRule(nil), p.Rules...)
	clone.Approval.RequireFor = append([]string(nil), p.Approval.RequireFor...)
	for i := range clone.Rules {
		clone.Rules[i].Validator.ID = strings.TrimSpace(clone.Rules[i].Validator.ID)
		clone.Rules[i].Validator.Version = strings.TrimSpace(clone.Rules[i].Validator.Version)
	}
	for i := range clone.Approval.RequireFor {
		clone.Approval.RequireFor[i] = strings.TrimSpace(clone.Approval.RequireFor[i])
	}
	sort.Slice(clone.Rules, func(i, j int) bool {
		if clone.Rules[i].Validator.ID != clone.Rules[j].Validator.ID {
			return clone.Rules[i].Validator.ID < clone.Rules[j].Validator.ID
		}
		return clone.Rules[i].Validator.Version < clone.Rules[j].Validator.Version
	})
	sort.Strings(clone.Approval.RequireFor)
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// Normalize validates the bounded, authenticated input to policy resolution.
// Zero budget fields mean "use the policy/default"; non-zero values are
// caller-requested upper bounds and are checked against the global envelope.
func (r *PolicyEvaluationRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("policy evaluation request is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = PolicySchemaVersion
	}
	if r.SchemaVersion != PolicySchemaVersion {
		return fmt.Errorf("unsupported policy schema_version %d", r.SchemaVersion)
	}
	r.WorkspaceID, r.RequestID, r.IdempotencyKey = strings.TrimSpace(r.WorkspaceID), strings.TrimSpace(r.RequestID), strings.TrimSpace(r.IdempotencyKey)
	r.Operation = strings.TrimSpace(r.Operation)
	if r.WorkspaceID == "" {
		return fmt.Errorf("workspace_id is required")
	}
	if len(r.RequestID) > MaxEventIDLength || len(r.IdempotencyKey) > MaxIdempotencyLength || len(r.Operation) > MaxPolicyIDLength {
		return fmt.Errorf("policy evaluation identity is too large")
	}
	if r.Actor.WorkspaceID == "" {
		r.Actor.WorkspaceID = r.WorkspaceID
	}
	if r.Actor.WorkspaceID != r.WorkspaceID {
		return fmt.Errorf("actor crosses workspace boundary")
	}
	if r.Policy != nil {
		if err := r.Policy.Normalize(); err != nil {
			return err
		}
		if r.Policy.WorkspaceID != r.WorkspaceID {
			return fmt.Errorf("policy crosses workspace boundary")
		}
	}
	if err := normalizeRequestedValidators(r.RequestedValidators); err != nil {
		return err
	}
	sort.Slice(r.RequestedValidators, func(i, j int) bool {
		if r.RequestedValidators[i].ID != r.RequestedValidators[j].ID {
			return r.RequestedValidators[i].ID < r.RequestedValidators[j].ID
		}
		return r.RequestedValidators[i].Version < r.RequestedValidators[j].Version
	})
	if err := validateRequestedPolicyBudget(r.RequestedBudget); err != nil {
		return err
	}
	r.RequestedApprovalMode = strings.ToLower(strings.TrimSpace(r.RequestedApprovalMode))
	if r.RequestedApprovalMode != "" && r.RequestedApprovalMode != PolicyApprovalAutomatic && r.RequestedApprovalMode != PolicyApprovalRequired && r.RequestedApprovalMode != PolicyApprovalDenied {
		return fmt.Errorf("unsupported requested approval mode %q", r.RequestedApprovalMode)
	}
	if len(r.OperationTypes) > MaxPolicyOperationTypes {
		return fmt.Errorf("too many policy operation types")
	}
	seenOperations := make(map[string]struct{}, len(r.OperationTypes))
	operations := r.OperationTypes[:0]
	for _, operation := range r.OperationTypes {
		operation = strings.TrimSpace(operation)
		if operation == "" {
			continue
		}
		if len(operation) > MaxPolicyIDLength {
			return fmt.Errorf("policy operation type is too large")
		}
		if _, exists := seenOperations[operation]; exists {
			continue
		}
		seenOperations[operation] = struct{}{}
		operations = append(operations, operation)
	}
	sort.Strings(operations)
	r.OperationTypes = operations
	return nil
}

func normalizeRequestedValidators(values []ValidatorRef) error {
	if len(values) > MaxValidationValidators {
		return fmt.Errorf("too many requested validators")
	}
	seen := make(map[string]struct{}, len(values))
	for i := range values {
		values[i].ID = strings.TrimSpace(values[i].ID)
		values[i].Version = strings.TrimSpace(values[i].Version)
		if values[i].ID == "" || values[i].Version == "" {
			return fmt.Errorf("requested validator %d is incomplete", i)
		}
		if len(values[i].ID) > MaxPolicyIDLength || len(values[i].Version) > MaxPolicyVersionLength {
			return fmt.Errorf("requested validator %d is too large", i)
		}
		key := values[i].ID + "\x00" + values[i].Version
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate requested validator %q", values[i].ID)
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateRequestedPolicyBudget(b PolicyBudget) error {
	if b.Change.MaxOperations < 0 || b.Change.MaxFileBytes < 0 || b.Change.MaxTotalBytes < 0 || b.Validation.MaxValidators < 0 || b.Validation.MaxFiles < 0 || b.Validation.MaxBytes < 0 || b.Validation.MaxOutputBytes < 0 || b.Validation.MaxWallTimeMS < 0 || b.Validation.MaxSQLQueries < 0 || b.Validation.MaxRetries < 0 || b.Validation.MaxReportBytes < 0 {
		return fmt.Errorf("requested policy budget contains a negative value")
	}
	if b.Change.MaxOperations > MaxChangeOperations || b.Change.MaxFileBytes > MaxChangeFileBytes || b.Change.MaxTotalBytes > MaxChangeTotalBytes {
		return fmt.Errorf("requested change budget exceeds global limits")
	}
	if b.Validation.MaxValidators > MaxValidationValidators || b.Validation.MaxFiles > MaxValidationFiles || b.Validation.MaxBytes > MaxValidationBytes || b.Validation.MaxOutputBytes > MaxValidationOutputBytes || b.Validation.MaxWallTimeMS > MaxValidationWallTime.Milliseconds() || b.Validation.MaxSQLQueries > MaxValidationSQLQueries || b.Validation.MaxRetries > MaxValidationRetries || b.Validation.MaxReportBytes > MaxValidationReportBytes {
		return fmt.Errorf("requested validation budget exceeds global limits")
	}
	return nil
}

// Ref returns the content-addressed exact reference for this pack.
func (p ValidationPolicyPack) Ref() ValidationPolicyRef {
	return ValidationPolicyRef{SchemaVersion: p.SchemaVersion, WorkspaceID: p.WorkspaceID, PolicyID: p.PolicyID, Version: p.Version, PolicyHash: p.PolicyHash}
}

// Normalize validates the bounded policy creation request and canonicalizes
// its actor/workspace identity before persistence.
func (r *PolicyCreateRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("policy create request is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = PolicySchemaVersion
	}
	if r.SchemaVersion != PolicySchemaVersion {
		return fmt.Errorf("unsupported policy schema_version %d", r.SchemaVersion)
	}
	r.WorkspaceID, r.RequestID, r.IdempotencyKey = strings.TrimSpace(r.WorkspaceID), strings.TrimSpace(r.RequestID), strings.TrimSpace(r.IdempotencyKey)
	if r.WorkspaceID == "" || r.IdempotencyKey == "" {
		return fmt.Errorf("workspace_id and idempotency_key are required")
	}
	if r.RequestID == "" {
		r.RequestID = NewID("policy-request")
	}
	if r.Actor.WorkspaceID == "" {
		r.Actor.WorkspaceID = r.WorkspaceID
	}
	if r.Actor.WorkspaceID != r.WorkspaceID {
		return fmt.Errorf("actor crosses workspace boundary")
	}
	if r.Pack.WorkspaceID != "" && strings.TrimSpace(r.Pack.WorkspaceID) != r.WorkspaceID {
		return fmt.Errorf("policy pack crosses workspace boundary")
	}
	r.Pack.WorkspaceID = r.WorkspaceID
	return r.Pack.Normalize()
}

// Normalize validates a lifecycle request without resolving its current
// version. The store performs that lookup and lifecycle transition under row
// locks so concurrent operators cannot race a policy state change.
func (r *PolicyLifecycleRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("policy lifecycle request is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = PolicySchemaVersion
	}
	if r.SchemaVersion != PolicySchemaVersion {
		return fmt.Errorf("unsupported policy schema_version %d", r.SchemaVersion)
	}
	r.WorkspaceID, r.RequestID, r.IdempotencyKey, r.Reason = strings.TrimSpace(r.WorkspaceID), strings.TrimSpace(r.RequestID), strings.TrimSpace(r.IdempotencyKey), strings.TrimSpace(r.Reason)
	if r.WorkspaceID == "" || r.IdempotencyKey == "" {
		return fmt.Errorf("workspace_id and idempotency_key are required")
	}
	if r.RequestID == "" {
		r.RequestID = NewID("policy-request")
	}
	if r.Actor.WorkspaceID == "" {
		r.Actor.WorkspaceID = r.WorkspaceID
	}
	if r.Actor.WorkspaceID != r.WorkspaceID {
		return fmt.Errorf("actor crosses workspace boundary")
	}
	if err := r.Policy.Normalize(); err != nil {
		return err
	}
	if r.Policy.WorkspaceID != r.WorkspaceID {
		return fmt.Errorf("policy crosses workspace boundary")
	}
	if len(r.Reason) > MaxPolicyAuditReason {
		return fmt.Errorf("policy reason is too large")
	}
	return nil
}

// RequestHash returns the stable identity of a lifecycle request. Request and
// idempotency keys, actor identity, and audit prose are delivery metadata.
func (r PolicyLifecycleRequest) RequestHash() string {
	clone := r
	clone.SchemaVersion = PolicySchemaVersion
	clone.RequestID, clone.IdempotencyKey, clone.Actor, clone.Reason = "", "", ActorRef{}, ""
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// Normalize validates an exact comparison request.
func (r *PolicyCompareRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("policy compare request is nil")
	}
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	if r.WorkspaceID == "" {
		return fmt.Errorf("workspace_id is required")
	}
	if err := r.Left.Normalize(); err != nil {
		return err
	}
	if err := r.Right.Normalize(); err != nil {
		return err
	}
	if r.Left.WorkspaceID != r.WorkspaceID || r.Right.WorkspaceID != r.WorkspaceID {
		return fmt.Errorf("policy comparison crosses workspace boundary")
	}
	return nil
}

// RequestHash returns the stable logical identity of a create request.
func (r PolicyCreateRequest) RequestHash() string {
	clone := r
	clone.RequestID, clone.IdempotencyKey = "", ""
	clone.Actor = ActorRef{}
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// ClonePolicyReference copies an optional policy reference without exposing
// mutable request state to a durable record.
func ClonePolicyReference(ref *ValidationPolicyRef) *ValidationPolicyRef {
	if ref == nil {
		return nil
	}
	copy := *ref
	return &copy
}
