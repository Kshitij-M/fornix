package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// EffectClass is the admission classification for a capability. Unknown is
// intentionally not executable and exists only to represent an invalid or
// incomplete input before it is rejected.
type EffectClass string

const (
	EffectClassReadOnly              EffectClass = "read_only"
	EffectClassObservation           EffectClass = "observation"
	EffectClassReversibleWrite       EffectClass = "reversible_write"
	EffectClassApprovalRequiredWrite EffectClass = "approval_required_write"
	EffectClassIrreversibleWrite     EffectClass = "irreversible_write"
	EffectClassExternalCommunication EffectClass = "external_communication"
	EffectClassUnknown               EffectClass = "unknown"
)

func (e EffectClass) valid() bool {
	switch e {
	case EffectClassReadOnly, EffectClassObservation, EffectClassReversibleWrite,
		EffectClassApprovalRequiredWrite, EffectClassIrreversibleWrite,
		EffectClassExternalCommunication:
		return true
	default:
		return false
	}
}

// ExternalEffect describes the boundary and verification state of an effect
// outside Fornix. It never contains credentials or the external payload.
type ExternalEffect struct {
	SchemaVersion        int         `json:"schema_version"`
	ID                   string      `json:"id,omitempty"`
	WorkspaceID          string      `json:"workspace_id"`
	Boundary             string      `json:"boundary"`
	Class                EffectClass `json:"class"`
	DeliveryGuarantee    string      `json:"delivery_guarantee"`
	IdempotencyKey       string      `json:"idempotency_key,omitempty"`
	ProviderRequestID    string      `json:"provider_request_id,omitempty"`
	ProviderIdempotency  bool        `json:"provider_idempotency_supported"`
	VerificationRequired bool        `json:"verification_required"`
	VerificationStatus   string      `json:"verification_status"`
	CompensationStatus   string      `json:"compensation_status"`
	ExactlyOnceClaimed   bool        `json:"exactly_once_claimed,omitempty"`
}

const (
	ExternalDeliveryAtLeastOnce = "at_least_once"
	ExternalDeliveryUnknown     = "unknown"

	ExternalVerificationNotRequired = "not_required"
	ExternalVerificationPending     = "pending"
	ExternalVerificationVerified    = "verified"
	ExternalVerificationFailed      = "failed"
	ExternalVerificationUnknown     = "unknown"

	ExternalCompensationUnavailable = "unavailable"
	ExternalCompensationAvailable   = "available"
	ExternalCompensationPending     = "pending"
	ExternalCompensationCompleted   = "completed"
	ExternalCompensationFailed      = "failed"
	ExternalCompensationUnknown     = "unknown"
)

// Normalize validates the external-effect record and makes the delivery
// guarantee explicit. Exactly-once is rejected rather than silently weakened.
func (e *ExternalEffect) Normalize() error {
	if e == nil {
		return fmt.Errorf("external effect is nil")
	}
	if e.SchemaVersion == 0 {
		e.SchemaVersion = DomainNeutralSchemaVersion
	}
	if e.SchemaVersion != DomainNeutralSchemaVersion {
		return fmt.Errorf("unsupported external effect schema_version %d", e.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(e.WorkspaceID)
	if err != nil {
		return err
	}
	id, err := normalizeDomainIdentifier(e.ID, "external effect id", MaxDomainIDLength, false)
	if err != nil {
		return err
	}
	boundary, err := normalizeDomainName(e.Boundary, "external effect boundary", MaxDomainNameLength, true)
	if err != nil {
		return err
	}
	class := EffectClass(strings.ToLower(strings.TrimSpace(string(e.Class))))
	if !class.valid() {
		return fmt.Errorf("unknown external effect class %q", e.Class)
	}
	if class == EffectClassReadOnly || class == EffectClassObservation {
		return fmt.Errorf("read-only effects cannot declare an external effect")
	}
	delivery := strings.ToLower(strings.TrimSpace(e.DeliveryGuarantee))
	if delivery != ExternalDeliveryAtLeastOnce && delivery != ExternalDeliveryUnknown {
		return fmt.Errorf("external effect delivery guarantee must be at_least_once or unknown")
	}
	if e.ExactlyOnceClaimed {
		return fmt.Errorf("exactly-once external execution cannot be claimed")
	}
	key, err := normalizeDomainIdentifier(e.IdempotencyKey, "external effect idempotency_key", MaxIdempotencyLength, false)
	if err != nil {
		return err
	}
	providerID, err := normalizeDomainIdentifier(e.ProviderRequestID, "external effect provider_request_id", MaxDomainIDLength, false)
	if err != nil {
		return err
	}
	verification := strings.ToLower(strings.TrimSpace(e.VerificationStatus))
	if verification == "" {
		if e.VerificationRequired {
			verification = ExternalVerificationPending
		} else {
			verification = ExternalVerificationNotRequired
		}
	}
	if !validExternalVerification(verification) {
		return fmt.Errorf("unknown external effect verification status %q", e.VerificationStatus)
	}
	if e.VerificationRequired && verification == ExternalVerificationNotRequired {
		return fmt.Errorf("required external-effect verification cannot be not_required")
	}
	compensation := strings.ToLower(strings.TrimSpace(e.CompensationStatus))
	if compensation == "" {
		compensation = ExternalCompensationUnknown
	}
	if !validExternalCompensation(compensation) {
		return fmt.Errorf("unknown external effect compensation status %q", e.CompensationStatus)
	}
	e.WorkspaceID, e.ID, e.Boundary, e.Class, e.DeliveryGuarantee = workspace, id, boundary, class, delivery
	e.IdempotencyKey, e.ProviderRequestID = key, providerID
	e.VerificationStatus, e.CompensationStatus = verification, compensation
	return nil
}

func validExternalVerification(value string) bool {
	switch value {
	case ExternalVerificationNotRequired, ExternalVerificationPending, ExternalVerificationVerified,
		ExternalVerificationFailed, ExternalVerificationUnknown:
		return true
	default:
		return false
	}
}

func validExternalCompensation(value string) bool {
	switch value {
	case ExternalCompensationUnavailable, ExternalCompensationAvailable, ExternalCompensationPending,
		ExternalCompensationCompleted, ExternalCompensationFailed, ExternalCompensationUnknown:
		return true
	default:
		return false
	}
}

// StableHash excludes delivery identities while retaining boundary semantics.
func (e ExternalEffect) StableHash() string {
	if err := e.Normalize(); err != nil {
		return ""
	}
	e.ID, e.IdempotencyKey, e.ProviderRequestID = "", "", ""
	raw, _ := json.Marshal(e)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}
