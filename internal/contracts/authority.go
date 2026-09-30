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

// AuthorityLinkSchemaVersion versions the cross-authority join without
// changing the independently versioned operation, admission, or receipt
// contracts.
const AuthorityLinkSchemaVersion = 1

const (
	AuthorityStageAdmission = "admission"
	AuthorityStageResult    = "result"
	AuthorityStageReceipt   = "receipt"

	MaxAuthorityLinkReferences = MaxDomainReferences
	MaxAuthorityLinkEffectIDs  = MaxDomainReferences
	MaxAuthorityLinkJSONBytes  = 128 << 10
)

// EffectAuthority is the non-secret authority envelope passed between
// admission, durable effect reservation, and result reconciliation. It is a
// reference to live authority, never a credential container. The operation
// and task fences identify the worker snapshot; schema and credential fields
// identify the exact compatibility and source snapshots used for the call.
type EffectAuthority struct {
	WorkspaceID      string `json:"workspace_id"`
	OperationID      string `json:"operation_id"`
	OperationOwnerID string `json:"operation_owner_id"`
	OperationFence   uint64 `json:"operation_fence,omitempty"`
	// AttemptID, EffectID, RequestHash, and EffectReservationHash bind the
	// envelope to the exact durable dispatch reservation. They are optional on
	// legacy read-only authority links, but the effect dispatcher always fills
	// and validates them before an external call.
	AttemptID                 string                     `json:"attempt_id,omitempty"`
	EffectID                  string                     `json:"effect_id,omitempty"`
	RequestHash               string                     `json:"request_hash,omitempty"`
	EffectReservationHash     string                     `json:"effect_reservation_hash,omitempty"`
	TaskOwnerID               string                     `json:"task_owner_id,omitempty"`
	TaskFence                 uint64                     `json:"task_fence,omitempty"`
	AgentRunID                string                     `json:"agent_run_id,omitempty"`
	AgentRunOwnerID           string                     `json:"agent_run_owner_id,omitempty"`
	AgentRunFence             uint64                     `json:"agent_run_fence,omitempty"`
	SchemaCatalogHash         string                     `json:"schema_catalog_hash,omitempty"`
	SchemaCatalogRevision     string                     `json:"schema_catalog_revision,omitempty"`
	CredentialLeaseID         string                     `json:"credential_lease_id,omitempty"`
	CredentialLeaseFence      uint64                     `json:"credential_lease_fence,omitempty"`
	CredentialRevocationEpoch uint64                     `json:"credential_revocation_epoch,omitempty"`
	CredentialSourceVersion   string                     `json:"credential_source_version,omitempty"`
	CredentialSourceExpiresAt *time.Time                 `json:"credential_source_expires_at,omitempty"`
	ExternalBoundary          *ExternalBoundaryAuthority `json:"external_boundary,omitempty"`
}

func (a *EffectAuthority) Normalize() error {
	if a == nil {
		return fmt.Errorf("effect authority is nil")
	}
	workspace, err := normalizeDomainWorkspace(a.WorkspaceID)
	if err != nil {
		return err
	}
	operation, err := normalizeDomainIdentifier(a.OperationID, "effect authority operation_id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	if a.OperationFence == 0 {
		return fmt.Errorf("effect authority operation fence is required")
	}
	a.OperationOwnerID, err = normalizeDomainIdentifier(a.OperationOwnerID, "effect authority operation_owner_id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	for field, value := range map[string]*string{
		"effect authority attempt_id": &a.AttemptID,
		"effect authority effect_id":  &a.EffectID,
	} {
		if *value == "" {
			continue
		}
		normalized, normalizeErr := normalizeDomainIdentifier(*value, field, MaxDomainIDLength, true)
		if normalizeErr != nil {
			return normalizeErr
		}
		*value = normalized
	}
	if a.RequestHash, err = normalizeDomainHash(a.RequestHash, "effect authority request_hash", false); err != nil {
		return err
	}
	if a.EffectReservationHash, err = normalizeDomainHash(a.EffectReservationHash, "effect authority effect_reservation_hash", false); err != nil {
		return err
	}
	if a.EffectReservationHash != "" && a.EffectID == "" {
		return fmt.Errorf("effect authority reservation hash requires effect_id")
	}
	if a.TaskOwnerID == "" && a.TaskFence != 0 || a.TaskOwnerID != "" && a.TaskFence == 0 {
		return fmt.Errorf("effect authority task owner and fence must be supplied together")
	}
	if a.AgentRunID == "" && (a.AgentRunOwnerID != "" || a.AgentRunFence != 0) || a.AgentRunID != "" && (a.AgentRunOwnerID == "" || a.AgentRunFence == 0) {
		return fmt.Errorf("effect authority agent-run ID, owner, and fence must be supplied together")
	}
	if a.AgentRunID != "" {
		a.AgentRunID, err = normalizeDomainIdentifier(a.AgentRunID, "effect authority agent_run_id", MaxDomainIDLength, true)
		if err != nil {
			return err
		}
		a.AgentRunOwnerID, err = normalizeDomainIdentifier(a.AgentRunOwnerID, "effect authority agent_run_owner_id", MaxDomainIDLength, true)
		if err != nil {
			return err
		}
		if a.AgentRunFence > uint64(1<<63-1) {
			return fmt.Errorf("effect authority agent-run fence exceeds database range")
		}
	}
	if a.SchemaCatalogHash, err = normalizeDomainHash(a.SchemaCatalogHash, "effect authority schema_catalog_hash", false); err != nil {
		return err
	}
	if a.SchemaCatalogHash == "" && a.SchemaCatalogRevision != "" || a.SchemaCatalogHash != "" && a.SchemaCatalogRevision == "" {
		return fmt.Errorf("effect authority schema catalog hash and revision must be supplied together")
	}
	if a.SchemaCatalogRevision != "" {
		if a.SchemaCatalogRevision, err = normalizeDomainIdentifier(a.SchemaCatalogRevision, "effect authority schema_catalog_revision", MaxDomainVersionLength, true); err != nil {
			return err
		}
	}
	a.CredentialSourceVersion = strings.TrimSpace(a.CredentialSourceVersion)
	if len(a.CredentialSourceVersion) > MaxCredentialSourceVersionLength {
		return fmt.Errorf("effect authority credential source version is too large")
	}
	if a.CredentialSourceVersion == "" && a.CredentialSourceExpiresAt != nil {
		return fmt.Errorf("effect authority source expiry requires source version")
	}
	if a.CredentialSourceExpiresAt != nil {
		expiry := a.CredentialSourceExpiresAt.UTC()
		a.CredentialSourceExpiresAt = &expiry
	}
	if a.CredentialLeaseID == "" {
		if a.CredentialLeaseFence != 0 || a.CredentialRevocationEpoch != 0 || a.CredentialSourceVersion != "" || a.CredentialSourceExpiresAt != nil {
			return fmt.Errorf("effect authority credential facts require credential lease_id")
		}
	} else {
		if a.CredentialLeaseFence == 0 || a.CredentialRevocationEpoch == 0 || a.CredentialSourceVersion == "" {
			return fmt.Errorf("effect authority credential lease is incomplete")
		}
		a.CredentialLeaseID, err = normalizeDomainIdentifier(a.CredentialLeaseID, "effect authority credential lease_id", MaxDomainIDLength, true)
		if err != nil {
			return err
		}
	}
	if a.ExternalBoundary != nil {
		if err := a.ExternalBoundary.Normalize(); err != nil {
			return err
		}
	}
	a.WorkspaceID, a.OperationID = workspace, operation
	return nil
}

func (a EffectAuthority) StableHash() string {
	if err := a.Normalize(); err != nil {
		return ""
	}
	raw, _ := json.Marshal(a)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// AuthorityLinkReference is a bounded, hash-only pointer to an authoritative
// evidence, artifact, output, or effect record. It is never a replacement for
// the source authority.
type AuthorityLinkReference struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
	Hash string `json:"hash,omitempty"`
	Role string `json:"role,omitempty"`
}

func (r *AuthorityLinkReference) normalize(index int) error {
	if r == nil {
		return fmt.Errorf("authority reference %d is nil", index)
	}
	kind, err := normalizeDomainName(r.Kind, "authority reference kind", MaxDomainNameLength, true)
	if err != nil {
		return err
	}
	id, err := normalizeDomainIdentifier(r.ID, "authority reference id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	hash, err := normalizeDomainHash(r.Hash, "authority reference hash", false)
	if err != nil {
		return err
	}
	role, err := normalizeDomainName(r.Role, "authority reference role", MaxDomainNameLength, false)
	if err != nil {
		return err
	}
	r.Kind, r.ID, r.Hash, r.Role = kind, id, hash, role
	return nil
}

func authorityReferenceKey(r AuthorityLinkReference) string {
	return r.Kind + "\x00" + r.ID + "\x00" + r.Hash + "\x00" + r.Role
}

// OperationAuthorityLink is the immutable, reference-only proof that one
// operation boundary used one exact set of trusted and fenced authorities.
// Empty optional fields mean that the caller did not claim that authority;
// they never mean that a missing credential or fence is valid for a path that
// requires one.
type OperationAuthorityLink struct {
	SchemaVersion int    `json:"schema_version"`
	ID            string `json:"id,omitempty"`
	WorkspaceID   string `json:"workspace_id"`
	Stage         string `json:"stage"`
	OperationID   string `json:"operation_id"`
	OperationHash string `json:"operation_hash"`

	AdmissionDecisionID string `json:"admission_decision_id,omitempty"`
	AdmissionInputHash  string `json:"admission_input_hash,omitempty"`

	TrustPolicyHash       string `json:"trust_policy_hash,omitempty"`
	TrustPolicyRevision   string `json:"trust_policy_revision,omitempty"`
	SchemaCatalogHash     string `json:"schema_catalog_hash,omitempty"`
	SchemaCatalogRevision string `json:"schema_catalog_revision,omitempty"`
	ConnectorHash         string `json:"connector_hash,omitempty"`
	CapabilityHash        string `json:"capability_hash,omitempty"`
	PolicyID              string `json:"policy_id,omitempty"`
	PolicyVersion         string `json:"policy_version,omitempty"`
	PolicyHash            string `json:"policy_hash,omitempty"`

	CredentialLeaseID         string                     `json:"credential_lease_id,omitempty"`
	CredentialLeaseFence      uint64                     `json:"credential_lease_fence,omitempty"`
	CredentialRevocationEpoch uint64                     `json:"credential_revocation_epoch,omitempty"`
	CredentialSourceVersion   string                     `json:"credential_source_version,omitempty"`
	CredentialSourceExpiresAt *time.Time                 `json:"credential_source_expires_at,omitempty"`
	ExternalBoundary          *ExternalBoundaryAuthority `json:"external_boundary,omitempty"`
	OperationOwnerID          string                     `json:"operation_owner_id,omitempty"`
	OperationFence            uint64                     `json:"operation_fence,omitempty"`
	TaskOwnerID               string                     `json:"task_owner_id,omitempty"`
	TaskFence                 uint64                     `json:"task_fence,omitempty"`
	EffectReservationHash     string                     `json:"effect_reservation_hash,omitempty"`
	EffectIDs                 []string                   `json:"effect_ids,omitempty"`

	ResultID    string `json:"result_id,omitempty"`
	ResultHash  string `json:"result_hash,omitempty"`
	ReceiptID   string `json:"receipt_id,omitempty"`
	ReceiptHash string `json:"receipt_hash,omitempty"`

	Evidence  []AuthorityLinkReference `json:"evidence,omitempty"`
	Artifacts []AuthorityLinkReference `json:"artifacts,omitempty"`

	Actor          ActorRef  `json:"actor"`
	RequestID      string    `json:"request_id"`
	IdempotencyKey string    `json:"idempotency_key"`
	CausationID    string    `json:"causation_id,omitempty"`
	CorrelationID  string    `json:"correlation_id,omitempty"`
	LinkHash       string    `json:"link_hash"`
	CreatedAt      time.Time `json:"created_at"`
}

// Normalize canonicalizes a link and validates every authority boundary. It
// does not contact Postgres; the store validates references and live fences in
// the same transaction as the source mutation.
func (l *OperationAuthorityLink) Normalize() error {
	if l == nil {
		return fmt.Errorf("authority link is nil")
	}
	if l.SchemaVersion == 0 {
		l.SchemaVersion = AuthorityLinkSchemaVersion
	}
	if l.SchemaVersion != AuthorityLinkSchemaVersion {
		return fmt.Errorf("unsupported authority link schema_version %d", l.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(l.WorkspaceID)
	if err != nil {
		return err
	}
	stage := strings.ToLower(strings.TrimSpace(l.Stage))
	if stage != AuthorityStageAdmission && stage != AuthorityStageResult && stage != AuthorityStageReceipt {
		return fmt.Errorf("unsupported authority link stage %q", l.Stage)
	}
	operationID, err := normalizeDomainIdentifier(l.OperationID, "authority operation_id", MaxDomainIDLength, true)
	if err != nil {
		return err
	}
	operationHash, err := normalizeDomainHash(l.OperationHash, "authority operation_hash", true)
	if err != nil {
		return err
	}
	for field, value := range map[string]string{
		"authority id": l.ID, "admission decision_id": l.AdmissionDecisionID,
		"credential lease_id": l.CredentialLeaseID, "operation owner_id": l.OperationOwnerID,
		"task owner_id": l.TaskOwnerID, "result_id": l.ResultID, "receipt_id": l.ReceiptID,
	} {
		if value == "" {
			continue
		}
		if _, err := normalizeDomainIdentifier(value, field, MaxDomainIDLength, true); err != nil {
			return err
		}
	}
	for field, value := range map[string]*string{
		"admission_input_hash": &l.AdmissionInputHash, "trust_policy_hash": &l.TrustPolicyHash,
		"connector_hash": &l.ConnectorHash, "capability_hash": &l.CapabilityHash,
		"policy_hash": &l.PolicyHash, "effect_reservation_hash": &l.EffectReservationHash,
		"result_hash": &l.ResultHash, "receipt_hash": &l.ReceiptHash,
	} {
		normalized, err := normalizeDomainHash(*value, field, false)
		if err != nil {
			return err
		}
		*value = normalized
	}
	for field, value := range map[string]*string{
		"trust_policy_revision":   &l.TrustPolicyRevision,
		"schema_catalog_revision": &l.SchemaCatalogRevision,
		"policy_version":          &l.PolicyVersion,
	} {
		if *value == "" {
			continue
		}
		normalized, err := normalizeDomainIdentifier(*value, field, MaxDomainVersionLength, true)
		if err != nil {
			return err
		}
		*value = normalized
	}
	if l.PolicyID != "" {
		if l.PolicyID, err = normalizeDomainIdentifier(l.PolicyID, "policy_id", MaxDomainIDLength, true); err != nil {
			return err
		}
	}
	if l.SchemaCatalogHash, err = normalizeDomainHash(l.SchemaCatalogHash, "schema_catalog_hash", false); err != nil {
		return err
	}
	l.CredentialSourceVersion = strings.TrimSpace(l.CredentialSourceVersion)
	if len(l.CredentialSourceVersion) > MaxCredentialSourceVersionLength {
		return fmt.Errorf("credential source version is too large")
	}
	if l.CredentialSourceVersion == "" && l.CredentialSourceExpiresAt != nil {
		return fmt.Errorf("credential source expiry requires source version")
	}
	if l.CredentialSourceExpiresAt != nil {
		expiry := l.CredentialSourceExpiresAt.UTC()
		l.CredentialSourceExpiresAt = &expiry
	}
	for field, value := range map[string]*string{
		"request_id": &l.RequestID, "idempotency_key": &l.IdempotencyKey,
		"causation_id": &l.CausationID, "correlation_id": &l.CorrelationID,
	} {
		required := field == "request_id" || field == "idempotency_key"
		normalized, err := normalizeDomainIdentifier(*value, "authority "+field, MaxIdempotencyLength, required)
		if err != nil {
			return err
		}
		*value = normalized
	}
	if l.CredentialLeaseID == "" && (l.CredentialLeaseFence != 0 || l.CredentialRevocationEpoch != 0) {
		return fmt.Errorf("credential lease fence requires credential lease_id")
	}
	if l.CredentialLeaseID == "" && (l.CredentialSourceVersion != "" || l.CredentialSourceExpiresAt != nil) {
		return fmt.Errorf("credential source facts require credential lease_id")
	}
	if l.ExternalBoundary != nil {
		if err := l.ExternalBoundary.Normalize(); err != nil {
			return err
		}
	}
	if l.SchemaCatalogHash == "" && l.SchemaCatalogRevision != "" || l.SchemaCatalogHash != "" && l.SchemaCatalogRevision == "" {
		return fmt.Errorf("schema catalog hash and revision must be supplied together")
	}
	if l.CredentialLeaseID != "" && (l.CredentialLeaseFence == 0 || l.CredentialRevocationEpoch == 0) {
		return fmt.Errorf("credential lease_id requires positive fence and revocation epoch")
	}
	if l.OperationOwnerID == "" && l.OperationFence != 0 {
		return fmt.Errorf("operation fence requires operation owner_id")
	}
	if l.OperationOwnerID != "" && l.OperationFence == 0 {
		return fmt.Errorf("operation owner_id requires positive operation fence")
	}
	if l.TaskOwnerID == "" && l.TaskFence != 0 {
		return fmt.Errorf("task fence requires task owner_id")
	}
	if l.TaskOwnerID != "" && l.TaskFence == 0 {
		return fmt.Errorf("task owner_id requires positive task fence")
	}
	if l.ResultID == "" && l.ResultHash != "" || l.ResultID != "" && l.ResultHash == "" {
		return fmt.Errorf("result_id and result_hash must be supplied together")
	}
	if l.ReceiptID == "" && l.ReceiptHash != "" || l.ReceiptID != "" && l.ReceiptHash == "" {
		return fmt.Errorf("receipt_id and receipt_hash must be supplied together")
	}
	if len(l.EffectIDs) > MaxAuthorityLinkEffectIDs {
		return fmt.Errorf("authority link has too many effect ids")
	}
	seen := make(map[string]struct{}, len(l.EffectIDs))
	for i, effectID := range l.EffectIDs {
		normalized, err := normalizeDomainIdentifier(effectID, fmt.Sprintf("effect_ids[%d]", i), MaxDomainIDLength, true)
		if err != nil {
			return err
		}
		if _, exists := seen[normalized]; exists {
			return fmt.Errorf("authority link has duplicate effect id %q", normalized)
		}
		seen[normalized] = struct{}{}
		l.EffectIDs[i] = normalized
	}
	sort.Strings(l.EffectIDs)
	if len(l.Evidence) > MaxAuthorityLinkReferences || len(l.Artifacts) > MaxAuthorityLinkReferences {
		return fmt.Errorf("authority link has too many evidence or artifact references")
	}
	for i := range l.Evidence {
		if err := l.Evidence[i].normalize(i); err != nil {
			return fmt.Errorf("evidence: %w", err)
		}
	}
	for i := range l.Artifacts {
		if err := l.Artifacts[i].normalize(i); err != nil {
			return fmt.Errorf("artifacts: %w", err)
		}
	}
	sort.Slice(l.Evidence, func(i, j int) bool {
		return authorityReferenceKey(l.Evidence[i]) < authorityReferenceKey(l.Evidence[j])
	})
	sort.Slice(l.Artifacts, func(i, j int) bool {
		return authorityReferenceKey(l.Artifacts[i]) < authorityReferenceKey(l.Artifacts[j])
	})
	l.Evidence = deduplicateAuthorityReferences(l.Evidence)
	l.Artifacts = deduplicateAuthorityReferences(l.Artifacts)
	if err := normalizeDomainActor(&l.Actor, workspace); err != nil {
		return err
	}
	l.WorkspaceID, l.Stage, l.OperationID, l.OperationHash = workspace, stage, operationID, operationHash
	if l.ID != "" {
		l.ID, _ = normalizeDomainIdentifier(l.ID, "authority id", MaxDomainIDLength, true)
	}
	if !l.CreatedAt.IsZero() {
		l.CreatedAt = l.CreatedAt.UTC()
	}
	expected := l.StableHash()
	if l.LinkHash != "" && strings.ToLower(strings.TrimSpace(l.LinkHash)) != expected {
		return fmt.Errorf("authority link_hash does not match normalized link")
	}
	l.LinkHash = expected
	return nil
}

func deduplicateAuthorityReferences(values []AuthorityLinkReference) []AuthorityLinkReference {
	if len(values) < 2 {
		return values
	}
	result := values[:0]
	previous := ""
	for _, value := range values {
		key := authorityReferenceKey(value)
		if key == previous {
			continue
		}
		result = append(result, value)
		previous = key
	}
	return result
}

// StableHash excludes database identity, delivery timestamps, and the hash
// itself. It is the idempotency/conflict identity of the complete authority
// join.
func (l OperationAuthorityLink) StableHash() string {
	clone := l
	clone.ID, clone.LinkHash, clone.CreatedAt = "", "", time.Time{}
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
