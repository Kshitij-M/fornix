package contracts

import (
	"fmt"
	"strings"
)

// EffectVerificationStatus is the only outcome a verifier may return. A
// verifier proves a bounded result; it never returns raw provider payloads.
const (
	EffectVerificationStatusVerified = ExternalVerificationVerified
	EffectVerificationStatusFailed   = ExternalVerificationFailed
	EffectVerificationStatusUnknown  = ExternalVerificationUnknown
)

// EffectVerificationRequest binds an adapter-owned proof attempt to the
// immutable generic operation/effect/link identities. OperationHash identifies
// the complete persisted operation (including its plan), while
// OperationRequestHash binds the request payload supplied to the verifier.
// EffectVersion and LinkVersion are optimistic fences read immediately before
// verification; Postgres proves them again before committing a transition.
type EffectVerificationRequest struct {
	SchemaVersion        int              `json:"schema_version"`
	WorkspaceID          string           `json:"workspace_id"`
	OperationID          string           `json:"operation_id"`
	RunID                string           `json:"run_id"`
	StepID               string           `json:"step_id"`
	EffectID             string           `json:"effect_id"`
	OperationHash        string           `json:"operation_hash"`
	OperationRequestHash string           `json:"operation_request_hash"`
	Operation            OperationRequest `json:"operation"`
	Effect               ExternalEffect   `json:"effect"`
	Link                 DomainEffectLink `json:"link"`
	EffectState          string           `json:"effect_state"`
	EffectVersion        int64            `json:"effect_version"`
	LinkVersion          int64            `json:"link_version"`
	Actor                ActorRef         `json:"actor"`
	IdempotencyKey       string           `json:"idempotency_key"`
}

// Normalize rejects ambiguous or cross-workspace proof requests before an
// adapter is called. The store performs the authoritative version and fence
// checks later in the same transaction as reconciliation.
func (r *EffectVerificationRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("effect verification request is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = DomainNeutralSchemaVersion
	}
	if r.SchemaVersion != DomainNeutralSchemaVersion {
		return fmt.Errorf("unsupported effect verification schema_version %d", r.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(r.WorkspaceID)
	if err != nil {
		return err
	}
	r.WorkspaceID = workspace
	for field, value := range map[string]*string{
		"operation_id": &r.OperationID, "run_id": &r.RunID, "step_id": &r.StepID,
		"effect_id": &r.EffectID, "idempotency_key": &r.IdempotencyKey,
	} {
		max := MaxDomainIDLength
		if field == "idempotency_key" {
			max = MaxIdempotencyLength
		}
		if *value, err = normalizeDomainIdentifier(*value, "effect verification "+field, max, true); err != nil {
			return err
		}
	}
	if r.OperationHash, err = normalizeDomainHash(r.OperationHash, "effect verification operation_hash", true); err != nil {
		return err
	}
	if r.OperationRequestHash, err = normalizeDomainHash(r.OperationRequestHash, "effect verification operation_request_hash", true); err != nil {
		return err
	}
	if err := r.Operation.Normalize(); err != nil {
		return fmt.Errorf("effect verification operation: %w", err)
	}
	if r.Operation.WorkspaceID != workspace || r.Operation.ID != r.OperationID || r.Operation.StableHash() != r.OperationRequestHash {
		return fmt.Errorf("effect verification operation identity is not bound")
	}
	if r.EffectVersion < 1 || r.LinkVersion < 1 {
		return fmt.Errorf("effect and link versions must be positive")
	}
	if err := r.Effect.Normalize(); err != nil {
		return fmt.Errorf("effect verification effect: %w", err)
	}
	if r.Effect.WorkspaceID != workspace || r.Effect.ID != r.EffectID {
		return fmt.Errorf("effect verification effect identity is not bound")
	}
	if err := r.Link.Normalize(); err != nil {
		return fmt.Errorf("effect verification link: %w", err)
	}
	if r.Link.WorkspaceID != workspace || r.Link.OperationID != r.OperationID || r.Link.OperationHash != r.OperationHash || r.Link.StepID != r.StepID || r.Link.EffectID != r.EffectID {
		return fmt.Errorf("effect verification link identity is not bound")
	}
	switch r.EffectState {
	case ExternalEffectAcknowledged, ExternalEffectVerificationPending, ExternalEffectRecoveryRequired:
	default:
		return fmt.Errorf("effect state %q is not verifiable", r.EffectState)
	}
	if err := normalizeDomainActor(&r.Actor, workspace); err != nil {
		return err
	}
	return nil
}

// EffectVerificationResult is a redacted proof classification. A verified
// result must provide both the observed result hash and the verifier's proof
// hash; failed and unknown outcomes must explain why success is not proven.
type EffectVerificationResult struct {
	Status            string `json:"status"`
	ResultHash        string `json:"result_hash,omitempty"`
	VerificationHash  string `json:"verification_hash,omitempty"`
	ProviderRequestID string `json:"provider_request_id,omitempty"`
	FailureCode       string `json:"failure_code,omitempty"`
}

func (r *EffectVerificationResult) Normalize() error {
	if r == nil {
		return fmt.Errorf("effect verification result is nil")
	}
	r.Status = strings.ToLower(strings.TrimSpace(r.Status))
	switch r.Status {
	case EffectVerificationStatusVerified, EffectVerificationStatusFailed, EffectVerificationStatusUnknown:
	default:
		return fmt.Errorf("unsupported effect verification status %q", r.Status)
	}
	var err error
	if r.ResultHash, err = normalizeDomainHash(r.ResultHash, "effect verification result_hash", r.Status == EffectVerificationStatusVerified); err != nil {
		return err
	}
	if r.VerificationHash, err = normalizeDomainHash(r.VerificationHash, "effect verification verification_hash", r.Status == EffectVerificationStatusVerified); err != nil {
		return err
	}
	if r.ProviderRequestID, err = normalizeDomainIdentifier(r.ProviderRequestID, "effect verification provider_request_id", MaxDomainIDLength, false); err != nil {
		return err
	}
	if r.FailureCode, err = normalizeDomainName(r.FailureCode, "effect verification failure_code", MaxDomainNameLength, r.Status != EffectVerificationStatusVerified); err != nil {
		return err
	}
	if r.Status == EffectVerificationStatusVerified && r.FailureCode != "" {
		return fmt.Errorf("verified effect cannot contain a failure code")
	}
	return nil
}
