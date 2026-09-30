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

// DomainEffectLinkSchemaVersion versions the cross-domain relationship
// between a specialized ledger and the generic operation/effect authority.
const DomainEffectLinkSchemaVersion = DomainNeutralSchemaVersion

const (
	DomainEffectKindModelCall         = "model_call"
	DomainEffectKindToolRun           = "tool_run"
	DomainEffectKindChangeApplication = "change_application"
	DomainEffectKindAgentStep         = "agent_step"
	DomainEffectKindHTTPRequest       = "http_request"
	DomainEffectKindIncidentStep      = "incident_step"
	DomainEffectKindEmbeddingCall     = "embedding_call"
	DomainEffectKindWorkflowStep      = "workflow_step"

	DomainEffectLinkRolePrimary  = "primary"
	DomainEffectLinkRoleEvidence = "evidence"

	DomainEffectLinkStatusLinked           = "linked"
	DomainEffectLinkStatusReconciled       = "reconciled"
	DomainEffectLinkStatusRecoveryRequired = "recovery_required"
)

const MaxDomainEffectLinkMetadata = 32

// DomainEffectLink is a hash-only relationship. It does not become a second
// effect ledger: operation_effects owns dispatch state, while the specialized
// domain row owns domain detail and raw evidence.
type DomainEffectLink struct {
	SchemaVersion int    `json:"schema_version"`
	ID            string `json:"id,omitempty"`
	WorkspaceID   string `json:"workspace_id"`

	OperationID           string `json:"operation_id"`
	OperationHash         string `json:"operation_hash"`
	StepID                string `json:"step_id"`
	AttemptID             string `json:"attempt_id"`
	EffectID              string `json:"effect_id"`
	EffectReservationHash string `json:"effect_reservation_hash"`

	DomainKind  string `json:"domain_kind"`
	DomainID    string `json:"domain_id"`
	DomainHash  string `json:"domain_hash"`
	LinkRole    string `json:"link_role"`
	RequestHash string `json:"request_hash"`
	ResultHash  string `json:"result_hash,omitempty"`

	Boundary            string      `json:"boundary"`
	EffectClass         EffectClass `json:"effect_class"`
	DeliveryGuarantee   string      `json:"delivery_guarantee"`
	ProviderIdempotency bool        `json:"provider_idempotency_supported"`
	ProviderRequestID   string      `json:"provider_request_id,omitempty"`
	VerificationStatus  string      `json:"verification_status"`
	Status              string      `json:"status"`
	// ExternalBoundary is the same hash-only network envelope stored on the
	// generic effect reservation. Keeping it on the immutable domain link
	// prevents a specialized ledger from appearing to prove a different
	// destination or transport policy than the effect authority.
	ExternalBoundary *ExternalBoundaryAuthority `json:"external_boundary,omitempty"`

	OperationOwnerID      string `json:"operation_owner_id,omitempty"`
	OperationFence        uint64 `json:"operation_fence,omitempty"`
	TaskOwnerID           string `json:"task_owner_id,omitempty"`
	TaskFence             uint64 `json:"task_fence,omitempty"`
	SchemaCatalogHash     string `json:"schema_catalog_hash,omitempty"`
	SchemaCatalogRevision string `json:"schema_catalog_revision,omitempty"`

	CredentialLeaseID         string     `json:"credential_lease_id,omitempty"`
	CredentialLeaseFence      uint64     `json:"credential_lease_fence,omitempty"`
	CredentialRevocationEpoch uint64     `json:"credential_revocation_epoch,omitempty"`
	CredentialSourceVersion   string     `json:"credential_source_version,omitempty"`
	CredentialSourceExpiresAt *time.Time `json:"credential_source_expires_at,omitempty"`

	Actor          ActorRef          `json:"actor"`
	RequestID      string            `json:"request_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	CausationID    string            `json:"causation_id,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	Metadata       map[string]string `json:"metadata,omitempty"`
	LinkHash       string            `json:"link_hash"`
	CreatedAt      time.Time         `json:"created_at"`
}

func validDomainEffectKind(value string) bool {
	switch value {
	case DomainEffectKindModelCall, DomainEffectKindToolRun, DomainEffectKindChangeApplication,
		DomainEffectKindAgentStep, DomainEffectKindHTTPRequest, DomainEffectKindIncidentStep,
		DomainEffectKindEmbeddingCall, DomainEffectKindWorkflowStep:
		return true
	default:
		return false
	}
}

func validDomainEffectStatus(value string) bool {
	return value == DomainEffectLinkStatusLinked || value == DomainEffectLinkStatusReconciled || value == DomainEffectLinkStatusRecoveryRequired
}

// Normalize validates the bounded, non-secret relationship and canonicalizes
// metadata. It deliberately does not prove that the referenced rows exist;
// the Postgres store performs that check in the same transaction as binding.
func (l *DomainEffectLink) Normalize() error {
	if l == nil {
		return fmt.Errorf("domain effect link is nil")
	}
	if l.SchemaVersion == 0 {
		l.SchemaVersion = DomainEffectLinkSchemaVersion
	}
	if l.SchemaVersion != DomainEffectLinkSchemaVersion {
		return fmt.Errorf("unsupported domain effect link schema_version %d", l.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(l.WorkspaceID)
	if err != nil {
		return err
	}
	ids := map[string]*string{
		"operation_id": &l.OperationID, "step_id": &l.StepID, "attempt_id": &l.AttemptID,
		"effect_id": &l.EffectID, "domain_id": &l.DomainID, "request_id": &l.RequestID,
		"idempotency_key": &l.IdempotencyKey,
	}
	for field, value := range ids {
		normalized, itemErr := normalizeDomainIdentifier(*value, "domain effect link "+field, MaxIdempotencyLength, true)
		if itemErr != nil {
			return itemErr
		}
		*value = normalized
	}
	if l.ID != "" {
		if l.ID, err = normalizeDomainIdentifier(l.ID, "domain effect link id", MaxDomainIDLength, true); err != nil {
			return err
		}
	}
	for field, value := range map[string]*string{
		"operation_hash": &l.OperationHash, "effect_reservation_hash": &l.EffectReservationHash,
		"domain_hash": &l.DomainHash, "request_hash": &l.RequestHash, "result_hash": &l.ResultHash,
	} {
		required := field != "result_hash"
		normalized, itemErr := normalizeDomainHash(*value, "domain effect link "+field, required)
		if itemErr != nil {
			return itemErr
		}
		*value = normalized
	}
	l.DomainKind = strings.ToLower(strings.TrimSpace(l.DomainKind))
	if !validDomainEffectKind(l.DomainKind) {
		return fmt.Errorf("unsupported domain effect kind %q", l.DomainKind)
	}
	l.LinkRole = strings.ToLower(strings.TrimSpace(l.LinkRole))
	if l.LinkRole == "" {
		l.LinkRole = DomainEffectLinkRolePrimary
	}
	if l.LinkRole != DomainEffectLinkRolePrimary && l.LinkRole != DomainEffectLinkRoleEvidence {
		return fmt.Errorf("unsupported domain effect link role %q", l.LinkRole)
	}
	l.Boundary, err = normalizeDomainName(l.Boundary, "domain effect link boundary", MaxDomainNameLength, true)
	if err != nil {
		return err
	}
	if !l.EffectClass.valid() || l.EffectClass == EffectClassReadOnly || l.EffectClass == EffectClassObservation {
		return fmt.Errorf("domain effect link requires an effectful class")
	}
	if l.ExternalBoundary != nil {
		if err := l.ExternalBoundary.Normalize(); err != nil {
			return err
		}
	}
	l.DeliveryGuarantee = strings.ToLower(strings.TrimSpace(l.DeliveryGuarantee))
	if l.DeliveryGuarantee != ExternalDeliveryAtLeastOnce && l.DeliveryGuarantee != ExternalDeliveryUnknown {
		return fmt.Errorf("invalid delivery guarantee %q", l.DeliveryGuarantee)
	}
	l.VerificationStatus = strings.ToLower(strings.TrimSpace(l.VerificationStatus))
	if l.VerificationStatus == "" {
		l.VerificationStatus = ExternalVerificationPending
	}
	if !validExternalVerification(l.VerificationStatus) {
		return fmt.Errorf("invalid verification status %q", l.VerificationStatus)
	}
	l.Status = strings.ToLower(strings.TrimSpace(l.Status))
	if l.Status == "" {
		l.Status = DomainEffectLinkStatusLinked
	}
	if !validDomainEffectStatus(l.Status) {
		return fmt.Errorf("invalid domain effect link status %q", l.Status)
	}
	if l.ProviderRequestID, err = normalizeDomainIdentifier(l.ProviderRequestID, "provider request id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if l.OperationOwnerID, err = normalizeDomainIdentifier(l.OperationOwnerID, "operation owner id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if l.TaskOwnerID, err = normalizeDomainIdentifier(l.TaskOwnerID, "task owner id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if (l.OperationOwnerID == "") != (l.OperationFence == 0) || (l.TaskOwnerID == "") != (l.TaskFence == 0) {
		return fmt.Errorf("fence and owner must be supplied as pairs")
	}
	if l.CredentialLeaseID == "" && (l.CredentialLeaseFence != 0 || l.CredentialRevocationEpoch != 0 || l.CredentialSourceVersion != "" || l.CredentialSourceExpiresAt != nil) {
		return fmt.Errorf("credential facts require a lease reference")
	}
	l.SchemaCatalogRevision = strings.TrimSpace(l.SchemaCatalogRevision)
	if l.SchemaCatalogHash, err = normalizeDomainHash(l.SchemaCatalogHash, "domain effect link schema_catalog_hash", false); err != nil {
		return err
	}
	if (l.SchemaCatalogHash == "") != (l.SchemaCatalogRevision == "") || len(l.SchemaCatalogRevision) > MaxDomainVersionLength {
		return fmt.Errorf("schema catalog hash and revision must be supplied together")
	}
	if l.CredentialLeaseID != "" && (l.CredentialLeaseFence == 0 || l.CredentialRevocationEpoch == 0 || l.CredentialSourceVersion == "") {
		return fmt.Errorf("credential lease facts are incomplete")
	}
	if l.CredentialSourceExpiresAt != nil {
		value := l.CredentialSourceExpiresAt.UTC()
		l.CredentialSourceExpiresAt = &value
	}
	if err := normalizeDomainActor(&l.Actor, workspace); err != nil {
		return err
	}
	if len(l.Metadata) > MaxDomainEffectLinkMetadata {
		return fmt.Errorf("domain effect link metadata exceeds %d entries", MaxDomainEffectLinkMetadata)
	}
	keys := make([]string, 0, len(l.Metadata))
	for key, value := range l.Metadata {
		if _, itemErr := normalizeDomainName(key, "domain effect metadata key", MaxDomainMetadataKeyLength, true); itemErr != nil {
			return itemErr
		}
		if len(value) > MaxDomainMetadataValueLen || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("domain effect metadata value is invalid")
		}
		keys = append(keys, key)
	}
	sort.Strings(keys)
	l.WorkspaceID = workspace
	if !l.CreatedAt.IsZero() {
		l.CreatedAt = l.CreatedAt.UTC()
	}
	expected := l.StableHash()
	if l.LinkHash != "" && strings.ToLower(strings.TrimSpace(l.LinkHash)) != expected {
		return fmt.Errorf("domain effect link hash does not match normalized link")
	}
	l.LinkHash = expected
	return nil
}

// StableHash excludes database identity, timestamps, and the hash itself.
// Result/provider identities remain included when a link is created after a
// provider result, so a changed outcome cannot masquerade as the same link.
func (l DomainEffectLink) StableHash() string {
	clone := l
	clone.ID, clone.LinkHash, clone.CreatedAt = "", "", time.Time{}
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// DomainEffectLinkTransition is append-only reconciliation history for a
// link. The current link row is immutable; status changes are recorded here.
type DomainEffectLinkTransition struct {
	WorkspaceID       string    `json:"workspace_id"`
	LinkID            string    `json:"link_id"`
	Version           int64     `json:"version"`
	FromStatus        string    `json:"from_status"`
	ToStatus          string    `json:"to_status"`
	ProviderRequestID string    `json:"provider_request_id,omitempty"`
	ResultHash        string    `json:"result_hash,omitempty"`
	FailureCode       string    `json:"failure_code,omitempty"`
	IdempotencyKey    string    `json:"idempotency_key"`
	Actor             ActorRef  `json:"actor"`
	OccurredAt        time.Time `json:"occurred_at"`
}

// DomainEffectLinkTransitionRequest is a hash-only, fenced command for
// advancing the append-only link history. The immutable link row remains the
// identity record; ExpectedVersion prevents two recovery workers from
// authorizing different outcomes from the same observed state.
type DomainEffectLinkTransitionRequest struct {
	WorkspaceID       string   `json:"workspace_id"`
	LinkID            string   `json:"link_id"`
	ExpectedVersion   int64    `json:"expected_version"`
	FromStatus        string   `json:"from_status"`
	ToStatus          string   `json:"to_status"`
	ProviderRequestID string   `json:"provider_request_id,omitempty"`
	ResultHash        string   `json:"result_hash,omitempty"`
	FailureCode       string   `json:"failure_code,omitempty"`
	IdempotencyKey    string   `json:"idempotency_key"`
	Actor             ActorRef `json:"actor"`
}

// Normalize validates a transition without consulting Postgres. The store
// additionally proves the link, current version, and workspace in one
// transaction before appending it.
func (r *DomainEffectLinkTransitionRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("domain effect link transition is nil")
	}
	workspace, err := normalizeDomainWorkspace(r.WorkspaceID)
	if err != nil {
		return err
	}
	r.WorkspaceID = workspace
	if r.LinkID, err = normalizeDomainIdentifier(r.LinkID, "domain effect link transition link_id", MaxDomainIDLength, true); err != nil {
		return err
	}
	if r.ExpectedVersion < 1 {
		return fmt.Errorf("domain effect link transition expected_version must be positive")
	}
	r.FromStatus = strings.ToLower(strings.TrimSpace(r.FromStatus))
	r.ToStatus = strings.ToLower(strings.TrimSpace(r.ToStatus))
	if !validDomainEffectStatus(r.FromStatus) || !validDomainEffectStatus(r.ToStatus) {
		return fmt.Errorf("invalid domain effect link transition status")
	}
	if r.FromStatus == r.ToStatus {
		return fmt.Errorf("domain effect link transition must change status")
	}
	if r.FromStatus == DomainEffectLinkStatusLinked && r.ToStatus != DomainEffectLinkStatusRecoveryRequired && r.ToStatus != DomainEffectLinkStatusReconciled {
		return fmt.Errorf("linked domain effect link may only enter reconciled or recovery_required")
	}
	if r.FromStatus == DomainEffectLinkStatusRecoveryRequired && r.ToStatus != DomainEffectLinkStatusReconciled {
		return fmt.Errorf("recovery_required domain effect link may only become reconciled")
	}
	if r.FromStatus == DomainEffectLinkStatusReconciled {
		return fmt.Errorf("reconciled domain effect link is terminal")
	}
	if r.IdempotencyKey, err = normalizeDomainIdentifier(r.IdempotencyKey, "domain effect link transition idempotency_key", MaxIdempotencyLength, true); err != nil {
		return err
	}
	if r.ProviderRequestID, err = normalizeDomainIdentifier(r.ProviderRequestID, "domain effect link transition provider_request_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.ResultHash, err = normalizeDomainHash(r.ResultHash, "domain effect link transition result_hash", false); err != nil {
		return err
	}
	if r.ToStatus == DomainEffectLinkStatusReconciled && r.ResultHash == "" && strings.TrimSpace(r.FailureCode) == "" {
		return fmt.Errorf("reconciled domain effect link requires a result hash or failure code")
	}
	if r.FailureCode, err = normalizeDomainName(r.FailureCode, "domain effect link transition failure_code", MaxDomainNameLength, false); err != nil {
		return err
	}
	if r.ToStatus == DomainEffectLinkStatusRecoveryRequired && r.ResultHash != "" {
		return fmt.Errorf("recovery_required transition cannot claim a result hash")
	}
	if err := normalizeDomainActor(&r.Actor, workspace); err != nil {
		return err
	}
	return nil
}
