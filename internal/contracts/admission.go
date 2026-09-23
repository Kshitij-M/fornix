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

// AdmissionSchemaVersion is the durable version of generic admission and
// approval records. The operation, capability, and policy contracts remain
// independently versioned; this value versions their decision boundary.
const AdmissionSchemaVersion = 1

const (
	AdmissionAllowed          = "allowed"
	AdmissionAwaitingApproval = "awaiting_approval"
	AdmissionDenied           = "denied"
	AdmissionAbstained        = "abstained"

	ApprovalRequestPending  = "pending"
	ApprovalRequestApproved = "approved"
	ApprovalRequestDenied   = "denied"
	ApprovalRequestExpired  = "expired"

	CredentialStateActive  = "active"
	CredentialStateMissing = "missing"
	CredentialStateRevoked = "revoked"
	CredentialStateExpired = "expired"
	CredentialStateUnknown = "unknown"

	ExternalEffectReserved            = "reserved"
	ExternalEffectDispatched          = "dispatched"
	ExternalEffectAcknowledged        = "acknowledged"
	ExternalEffectVerificationPending = "verification_pending"
	ExternalEffectVerified            = "verified"
	ExternalEffectVerificationFailed  = "verification_failed"
	ExternalEffectCompensationPending = "compensation_pending"
	ExternalEffectCompensated         = "compensated"
	ExternalEffectRecoveryRequired    = "recovery_required"
)

const (
	AdmissionReasonUnknownEffect        = "unknown_effect"
	AdmissionReasonCapabilityDisabled   = "capability_disabled"
	AdmissionReasonConnectorUnavailable = "connector_unavailable"
	AdmissionReasonConnectorDenied      = "connector_not_allowed"
	AdmissionReasonResourceDenied       = "resource_not_allowed"
	AdmissionReasonEvidenceMissing      = "evidence_missing"
	AdmissionReasonCredentialInvalid    = "credential_invalid"
	AdmissionReasonStaleFence           = "stale_fence"
	AdmissionReasonBudgetExceeded       = "budget_exceeded"
	AdmissionReasonQuotaExceeded        = "quota_exceeded"
	AdmissionReasonPolicyDenied         = "policy_denied"
	AdmissionReasonApprovalRequired     = "approval_required"
	AdmissionReasonApproved             = "approved"
)

const (
	MaxAdmissionPolicyRules      = 32
	MaxAdmissionCredentialStates = 128
	MaxAdmissionApprovalTTL      = 24 * time.Hour
	MaxAdmissionQuotaWindow      = 24 * time.Hour
	MaxAdmissionReasonLength     = 128
	MaxAdmissionPolicyJSONBytes  = 128 << 10
)

// AdmissionEffectRule is the policy decision for one capability effect. A
// policy may tighten a default, but it cannot make irreversible, external, or
// unknown effects automatic.
type AdmissionEffectRule struct {
	Effect EffectClass `json:"effect"`
	Mode   string      `json:"mode"`
}

// AdmissionPolicy is an immutable, declarative policy snapshot. It contains
// references, hashes, budgets, and allowlists only; it cannot execute code or
// carry credentials, prompts, headers, or connector payloads.
type AdmissionPolicy struct {
	SchemaVersion          int                   `json:"schema_version"`
	WorkspaceID            string                `json:"workspace_id"`
	PolicyID               string                `json:"policy_id"`
	Version                string                `json:"version"`
	PolicyHash             string                `json:"policy_hash,omitempty"`
	EffectRules            []AdmissionEffectRule `json:"effect_rules"`
	AllowedConnectors      []ConnectorRef        `json:"allowed_connectors"`
	AllowedResourceKinds   []string              `json:"allowed_resource_kinds"`
	AllowedActorIDs        []string              `json:"allowed_actor_ids,omitempty"`
	RequireEvidence        bool                  `json:"require_evidence"`
	RequireTaskFence       bool                  `json:"require_task_fence"`
	MaxCostMicros          int64                 `json:"max_cost_micros"`
	MaxOperationsPerWindow int                   `json:"max_operations_per_window"`
	MaxCostPerWindowMicros int64                 `json:"max_cost_per_window_micros"`
	QuotaWindowSeconds     int                   `json:"quota_window_seconds"`
	ApprovalTTLSeconds     int                   `json:"approval_ttl_seconds"`
}

// Normalize validates and canonicalizes an admission policy. Its hash is the
// immutable identity of the normalized snapshot, not a caller-supplied label.
func (p *AdmissionPolicy) Normalize() error {
	if p == nil {
		return fmt.Errorf("admission policy is nil")
	}
	if p.SchemaVersion == 0 {
		p.SchemaVersion = AdmissionSchemaVersion
	}
	if p.SchemaVersion != AdmissionSchemaVersion {
		return fmt.Errorf("unsupported admission policy schema_version %d", p.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(p.WorkspaceID)
	if err != nil {
		return err
	}
	id, err := normalizeDomainIdentifier(p.PolicyID, "admission policy_id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	version, err := normalizeDomainVersion(p.Version, "admission policy version", true)
	if err != nil {
		return err
	}
	if len(p.EffectRules) > MaxAdmissionPolicyRules {
		return fmt.Errorf("admission policy has too many effect rules")
	}
	seenEffects := map[EffectClass]struct{}{}
	for i := range p.EffectRules {
		rule := &p.EffectRules[i]
		rule.Effect = EffectClass(strings.ToLower(strings.TrimSpace(string(rule.Effect))))
		rule.Mode = strings.ToLower(strings.TrimSpace(rule.Mode))
		if !rule.Effect.valid() || rule.Effect == EffectClassUnknown {
			return fmt.Errorf("admission policy effect rule %d has unknown effect", i)
		}
		if rule.Mode != PolicyApprovalAutomatic && rule.Mode != PolicyApprovalRequired && rule.Mode != PolicyApprovalDenied {
			return fmt.Errorf("admission policy effect rule %d has unsupported mode", i)
		}
		if _, exists := seenEffects[rule.Effect]; exists {
			return fmt.Errorf("duplicate admission policy effect rule %q", rule.Effect)
		}
		seenEffects[rule.Effect] = struct{}{}
		if (rule.Effect == EffectClassIrreversibleWrite || rule.Effect == EffectClassExternalCommunication) && rule.Mode == PolicyApprovalAutomatic {
			return fmt.Errorf("%s cannot be automatic", rule.Effect)
		}
	}
	for i := range p.AllowedConnectors {
		if err := p.AllowedConnectors[i].Normalize(); err != nil {
			return fmt.Errorf("allowed connector %d: %w", i, err)
		}
		if p.AllowedConnectors[i].WorkspaceID != workspace {
			return fmt.Errorf("allowed connector crosses workspace boundary")
		}
	}
	sort.Slice(p.AllowedConnectors, func(i, j int) bool {
		return p.AllowedConnectors[i].StableHash() < p.AllowedConnectors[j].StableHash()
	})
	p.AllowedResourceKinds, err = normalizeDomainStrings(p.AllowedResourceKinds, "allowed resource kinds", MaxDomainReferences)
	if err != nil {
		return err
	}
	p.AllowedActorIDs, err = normalizeDomainStrings(p.AllowedActorIDs, "allowed actor ids", MaxDomainReferences)
	if err != nil {
		return err
	}
	if p.MaxCostMicros < 0 || p.MaxCostPerWindowMicros < 0 || p.MaxOperationsPerWindow < 0 || p.MaxOperationsPerWindow > 1_000_000 {
		return fmt.Errorf("admission policy quota is invalid")
	}
	if p.QuotaWindowSeconds == 0 {
		p.QuotaWindowSeconds = 3600
	}
	if p.QuotaWindowSeconds < 1 || time.Duration(p.QuotaWindowSeconds)*time.Second > MaxAdmissionQuotaWindow {
		return fmt.Errorf("admission policy quota window is out of bounds")
	}
	if p.ApprovalTTLSeconds == 0 {
		p.ApprovalTTLSeconds = 600
	}
	if p.ApprovalTTLSeconds < 1 || time.Duration(p.ApprovalTTLSeconds)*time.Second > MaxAdmissionApprovalTTL {
		return fmt.Errorf("admission policy approval ttl is out of bounds")
	}
	p.WorkspaceID, p.PolicyID, p.Version = workspace, id, version
	supplied := strings.ToLower(strings.TrimSpace(p.PolicyHash))
	p.PolicyHash = ""
	canonical, err := json.Marshal(p)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(canonical)
	hash := hex.EncodeToString(digest[:])
	if supplied != "" && supplied != hash {
		return fmt.Errorf("admission policy_hash does not match normalized policy")
	}
	p.PolicyHash = hash
	return nil
}

func (p AdmissionPolicy) StableHash() string {
	clone := p
	if err := clone.Normalize(); err != nil {
		return ""
	}
	return clone.PolicyHash
}

// CredentialState is the redacted result of resolving one credential
// reference. It never contains secret material.
type CredentialState struct {
	Reference string `json:"reference"`
	Status    string `json:"status"`
}

func (s *CredentialState) Normalize() error {
	if s == nil {
		return fmt.Errorf("credential state is nil")
	}
	s.Reference = strings.TrimSpace(s.Reference)
	s.Status = strings.ToLower(strings.TrimSpace(s.Status))
	if s.Reference == "" || len(s.Reference) > MaxDomainIDLength {
		return fmt.Errorf("credential reference is required and bounded")
	}
	switch s.Status {
	case CredentialStateActive, CredentialStateMissing, CredentialStateRevoked, CredentialStateExpired, CredentialStateUnknown:
	default:
		return fmt.Errorf("unknown credential state %q", s.Status)
	}
	return nil
}

// AdmissionInput is the bounded, secret-free input to deterministic policy
// evaluation. Verified facts such as connector health and credential state
// must come from an authoritative adapter or identity store.
type AdmissionInput struct {
	SchemaVersion       int                  `json:"schema_version"`
	WorkspaceID         string               `json:"workspace_id"`
	OperationID         string               `json:"operation_id"`
	OperationHash       string               `json:"operation_hash"`
	RequestID           string               `json:"request_id"`
	IdempotencyKey      string               `json:"idempotency_key"`
	Actor               ActorRef             `json:"actor"`
	Capability          CapabilityDefinition `json:"capability"`
	Target              ResourceRef          `json:"target"`
	Policy              AdmissionPolicy      `json:"policy"`
	CredentialStates    []CredentialState    `json:"credential_states,omitempty"`
	ConnectorAvailable  bool                 `json:"connector_available"`
	ResourceAllowed     bool                 `json:"resource_allowed"`
	EvidenceSatisfied   bool                 `json:"evidence_satisfied"`
	TaskBound           bool                 `json:"task_bound"`
	TaskOwnerID         string               `json:"task_owner_id,omitempty"`
	TaskFence           uint64               `json:"task_fence,omitempty"`
	TaskFenceValid      bool                 `json:"task_fence_valid"`
	RequestedCostMicros int64                `json:"requested_cost_micros"`
	QuotaOperations     int                  `json:"quota_operations"`
	QuotaCostMicros     int64                `json:"quota_cost_micros"`
}

// Normalize validates all nested references and prevents policy/capability
// facts from crossing the operation workspace.
func (in *AdmissionInput) Normalize() error {
	if in == nil {
		return fmt.Errorf("admission input is nil")
	}
	if in.SchemaVersion == 0 {
		in.SchemaVersion = AdmissionSchemaVersion
	}
	if in.SchemaVersion != AdmissionSchemaVersion {
		return fmt.Errorf("unsupported admission input schema_version %d", in.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(in.WorkspaceID)
	if err != nil {
		return err
	}
	for field, value := range map[string]string{"operation_id": in.OperationID, "request_id": in.RequestID, "idempotency_key": in.IdempotencyKey} {
		if _, err := normalizeDomainIdentifier(value, "admission "+field, MaxIdempotencyLength, true); err != nil {
			return err
		}
	}
	in.TaskOwnerID = strings.TrimSpace(in.TaskOwnerID)
	if len(in.TaskOwnerID) > MaxDomainIDLength {
		return fmt.Errorf("admission task_owner_id is too large")
	}
	if !canonicalSHA256(in.OperationHash) {
		return fmt.Errorf("admission operation_hash must be a SHA-256 hash")
	}
	if err := normalizeDomainActor(&in.Actor, workspace); err != nil {
		return err
	}
	if err := in.Capability.Normalize(); err != nil {
		return fmt.Errorf("admission capability: %w", err)
	}
	if in.Capability.WorkspaceID != workspace {
		return fmt.Errorf("admission capability crosses workspace boundary")
	}
	if err := in.Target.Normalize(); err != nil {
		return fmt.Errorf("admission target: %w", err)
	}
	if in.Target.WorkspaceID != workspace {
		return fmt.Errorf("admission target crosses workspace boundary")
	}
	if err := in.Policy.Normalize(); err != nil {
		return fmt.Errorf("admission policy: %w", err)
	}
	if in.Policy.WorkspaceID != workspace {
		return fmt.Errorf("admission policy crosses workspace boundary")
	}
	if in.RequestedCostMicros < 0 || in.QuotaOperations < 0 || in.QuotaCostMicros < 0 {
		return fmt.Errorf("admission cost and quota values cannot be negative")
	}
	if len(in.CredentialStates) > MaxAdmissionCredentialStates {
		return fmt.Errorf("too many credential states")
	}
	for i := range in.CredentialStates {
		if err := in.CredentialStates[i].Normalize(); err != nil {
			return fmt.Errorf("credential state %d: %w", i, err)
		}
	}
	sort.Slice(in.CredentialStates, func(i, j int) bool { return in.CredentialStates[i].Reference < in.CredentialStates[j].Reference })
	in.WorkspaceID, in.OperationID, in.OperationHash = workspace, strings.TrimSpace(in.OperationID), strings.ToLower(strings.TrimSpace(in.OperationHash))
	in.RequestID, in.IdempotencyKey = strings.TrimSpace(in.RequestID), strings.TrimSpace(in.IdempotencyKey)
	return nil
}

// StableHash identifies the logical, secret-free admission request.
//
// Delivery identifiers and verified runtime facts are intentionally excluded:
// a retry may use a different transport request ID, a takeover may use a new
// task fence, and quota/health/evidence observations may change while the
// same idempotency key is being recovered. Those facts affect the decision at
// the time of the first durable admission, but they must not create a second
// logical admission for the same command.
func (in AdmissionInput) StableHash() string {
	if err := in.Normalize(); err != nil {
		return ""
	}
	in.RequestID = ""
	in.ConnectorAvailable = false
	in.ResourceAllowed = false
	in.EvidenceSatisfied = false
	in.CredentialStates = nil
	in.TaskOwnerID, in.TaskFence, in.TaskFenceValid = "", 0, false
	// QuotaOperations and QuotaCostMicros are transaction-local observations,
	// not caller intent. Excluding them keeps a retried idempotent command tied
	// to the same request even when other admissions changed the window.
	in.QuotaOperations, in.QuotaCostMicros = 0, 0
	raw, _ := json.Marshal(in)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// AdmissionDecision is the immutable, auditable outcome of one evaluation.
type AdmissionDecision struct {
	SchemaVersion  int           `json:"schema_version"`
	ID             string        `json:"id"`
	WorkspaceID    string        `json:"workspace_id"`
	OperationID    string        `json:"operation_id"`
	OperationHash  string        `json:"operation_hash"`
	RequestID      string        `json:"request_id"`
	IdempotencyKey string        `json:"idempotency_key"`
	Actor          ActorRef      `json:"actor"`
	Capability     CapabilityRef `json:"capability"`
	Target         ResourceRef   `json:"target"`
	Effect         EffectClass   `json:"effect"`
	PolicyID       string        `json:"policy_id"`
	PolicyVersion  string        `json:"policy_version"`
	PolicyHash     string        `json:"policy_hash"`
	InputHash      string        `json:"input_hash"`
	DecisionHash   string        `json:"decision_hash"`
	Status         string        `json:"status"`
	ReasonCode     string        `json:"reason_code"`
	ApprovalID     string        `json:"approval_id,omitempty"`
	CostMicros     int64         `json:"cost_micros"`
	CreatedAt      time.Time     `json:"created_at"`
}

// OperationApprovalRequest binds human approval to one exact operation and
// policy decision. It is intentionally hash/reference-only.
type OperationApprovalRequest struct {
	SchemaVersion      int         `json:"schema_version"`
	ID                 string      `json:"id"`
	WorkspaceID        string      `json:"workspace_id"`
	OperationID        string      `json:"operation_id"`
	OperationHash      string      `json:"operation_hash"`
	DecisionHash       string      `json:"decision_hash"`
	CapabilityHash     string      `json:"capability_hash"`
	TargetHash         string      `json:"target_hash"`
	InputHash          string      `json:"input_hash"`
	PolicyHash         string      `json:"policy_hash"`
	Effect             EffectClass `json:"effect"`
	RequestedBy        ActorRef    `json:"requested_by"`
	Status             string      `json:"status"`
	ExpiresAt          time.Time   `json:"expires_at"`
	CreatedAt          time.Time   `json:"created_at"`
	DecidedAt          *time.Time  `json:"decided_at,omitempty"`
	DecidedBy          *ActorRef   `json:"decided_by,omitempty"`
	DecisionReasonHash string      `json:"decision_reason_hash,omitempty"`
}

// OperationApprovalDecision is the idempotent command for a pending approval.
// A reason is carried by hash only so arbitrary human text does not enter the
// control-plane record.
type OperationApprovalDecision struct {
	SchemaVersion  int      `json:"schema_version"`
	RequestID      string   `json:"request_id"`
	IdempotencyKey string   `json:"idempotency_key"`
	WorkspaceID    string   `json:"workspace_id"`
	ApprovalID     string   `json:"approval_id"`
	Decision       string   `json:"decision"`
	Actor          ActorRef `json:"actor"`
	ReasonHash     string   `json:"reason_hash,omitempty"`
}

// Normalize validates an approval command without exposing its reason text.
func (d *OperationApprovalDecision) Normalize() error {
	if d == nil {
		return fmt.Errorf("operation approval decision is nil")
	}
	if d.SchemaVersion == 0 {
		d.SchemaVersion = AdmissionSchemaVersion
	}
	if d.SchemaVersion != AdmissionSchemaVersion {
		return fmt.Errorf("unsupported approval decision schema_version %d", d.SchemaVersion)
	}
	for field, value := range map[string]string{"request_id": d.RequestID, "idempotency_key": d.IdempotencyKey, "workspace_id": d.WorkspaceID, "approval_id": d.ApprovalID} {
		if value == "" || len(value) > MaxIdempotencyLength {
			return fmt.Errorf("approval decision %s is required and bounded", field)
		}
	}
	d.Decision = strings.ToLower(strings.TrimSpace(d.Decision))
	if d.Decision != ApprovalRequestApproved && d.Decision != ApprovalRequestDenied && d.Decision != ApprovalRequestExpired {
		return fmt.Errorf("unsupported approval decision %q", d.Decision)
	}
	if err := normalizeDomainActor(&d.Actor, d.WorkspaceID); err != nil {
		return err
	}
	if d.ReasonHash != "" && !canonicalSHA256(d.ReasonHash) {
		return fmt.Errorf("approval decision reason_hash must be a SHA-256 hash")
	}
	d.WorkspaceID, d.RequestID, d.IdempotencyKey, d.ApprovalID = strings.TrimSpace(d.WorkspaceID), strings.TrimSpace(d.RequestID), strings.TrimSpace(d.IdempotencyKey), strings.TrimSpace(d.ApprovalID)
	return nil
}

// StableHash is the idempotency identity of an approval command.
func (d OperationApprovalDecision) StableHash() string {
	if err := d.Normalize(); err != nil {
		return ""
	}
	payload := struct {
		WorkspaceID, ApprovalID, IdempotencyKey, Decision, ReasonHash string
		Actor                                                         ActorRef
	}{d.WorkspaceID, d.ApprovalID, d.IdempotencyKey, d.Decision, d.ReasonHash, d.Actor}
	raw, _ := json.Marshal(payload)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// ExternalEffectUpdate is the fenced, append-only state transition around an
// immutable operation effect reservation.
type ExternalEffectUpdate struct {
	SchemaVersion     int    `json:"schema_version"`
	WorkspaceID       string `json:"workspace_id"`
	OperationID       string `json:"operation_id"`
	EffectID          string `json:"effect_id"`
	OwnerID           string `json:"owner_id"`
	Fence             uint64 `json:"fence"`
	RequestID         string `json:"request_id"`
	IdempotencyKey    string `json:"idempotency_key"`
	State             string `json:"state"`
	ProviderRequestID string `json:"provider_request_id,omitempty"`
	ResponseHash      string `json:"response_hash,omitempty"`
	VerificationHash  string `json:"verification_hash,omitempty"`
	CompensationHash  string `json:"compensation_hash,omitempty"`
	FailureCode       string `json:"failure_code,omitempty"`
}

func (u *ExternalEffectUpdate) Normalize() error {
	if u == nil {
		return fmt.Errorf("external effect update is nil")
	}
	if u.SchemaVersion == 0 {
		u.SchemaVersion = AdmissionSchemaVersion
	}
	if u.SchemaVersion != AdmissionSchemaVersion {
		return fmt.Errorf("unsupported external effect update schema_version %d", u.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(u.WorkspaceID)
	if err != nil {
		return err
	}
	for field, value := range map[string]string{"operation_id": u.OperationID, "effect_id": u.EffectID, "owner_id": u.OwnerID, "request_id": u.RequestID, "idempotency_key": u.IdempotencyKey} {
		max := MaxDomainIDLength
		if field == "idempotency_key" {
			max = MaxIdempotencyLength
		}
		if _, err := normalizeDomainIdentifier(value, "effect update "+field, max, true); err != nil {
			return err
		}
	}
	if u.Fence == 0 {
		return fmt.Errorf("effect update fence is required")
	}
	switch u.State {
	case ExternalEffectDispatched, ExternalEffectAcknowledged, ExternalEffectVerificationPending, ExternalEffectVerified, ExternalEffectVerificationFailed, ExternalEffectCompensationPending, ExternalEffectCompensated, ExternalEffectRecoveryRequired:
	default:
		return fmt.Errorf("unsupported external effect state %q", u.State)
	}
	for field, value := range map[string]string{"response_hash": u.ResponseHash, "verification_hash": u.VerificationHash, "compensation_hash": u.CompensationHash} {
		if value != "" && !canonicalSHA256(value) {
			return fmt.Errorf("effect update %s must be a SHA-256 hash", field)
		}
	}
	u.WorkspaceID, u.OperationID, u.EffectID, u.OwnerID, u.RequestID, u.IdempotencyKey = workspace, strings.TrimSpace(u.OperationID), strings.TrimSpace(u.EffectID), strings.TrimSpace(u.OwnerID), strings.TrimSpace(u.RequestID), strings.TrimSpace(u.IdempotencyKey)
	u.ProviderRequestID, u.FailureCode = strings.TrimSpace(u.ProviderRequestID), strings.ToLower(strings.TrimSpace(u.FailureCode))
	if len(u.ProviderRequestID) > MaxDomainIDLength || len(u.FailureCode) > MaxAdmissionReasonLength {
		return fmt.Errorf("effect update metadata is too large")
	}
	return nil
}
