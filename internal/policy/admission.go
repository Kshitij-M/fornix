// Package policy contains pure, deterministic admission decisions. It does
// not read a database, resolve secrets, invoke connectors, or perform an
// approval side effect; callers supply redacted facts from those authorities
// and persist the returned decision through store.AdmissionStore.
package policy

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

// AdmissionEvaluation is the pure result of one policy evaluation. Approval
// is a durable request, not permission to execute; the caller must persist it
// and wait for an authorized decision before crossing a write boundary.
type AdmissionEvaluation struct {
	Decision contracts.AdmissionDecision
	Approval *contracts.OperationApprovalRequest
}

// Evaluate applies one immutable policy snapshot to one capability and its
// verified, secret-free runtime facts. It is deterministic for the same
// normalized input. Delivery IDs and creation timestamps are excluded from the
// decision hash; a durable rate-limit eligibility time is included because it
// changes the scheduled outcome.
func Evaluate(input contracts.AdmissionInput) (AdmissionEvaluation, error) {
	if err := input.Normalize(); err != nil {
		return AdmissionEvaluation{}, err
	}
	capability := input.Capability
	if !capability.Enabled {
		return deny(input, contracts.AdmissionReasonCapabilityDisabled)
	}
	if !input.ConnectorAvailable {
		return deny(input, contracts.AdmissionReasonConnectorUnavailable)
	}
	if !input.ResourceAllowed || !resourceAllowed(input.Policy, input.Target.Kind) {
		return deny(input, contracts.AdmissionReasonResourceDenied)
	}
	if !connectorAllowed(input.Policy, capability.Ref.Connector) {
		return deny(input, contracts.AdmissionReasonConnectorDenied)
	}
	if !actorAllowed(input.Policy, input.Actor.ID) {
		return deny(input, contracts.AdmissionReasonPolicyDenied)
	}
	if input.Policy.RequireEvidence && !input.EvidenceSatisfied {
		return deny(input, contracts.AdmissionReasonEvidenceMissing)
	}
	if input.TaskBound && !input.TaskFenceValid {
		return deny(input, contracts.AdmissionReasonStaleFence)
	}
	if err := credentialsSatisfied(capability.RequiredCredentialRefs, input.CredentialStates); err != nil {
		return deny(input, contracts.AdmissionReasonCredentialInvalid)
	}
	if input.Policy.MaxCostMicros > 0 && input.RequestedCostMicros > input.Policy.MaxCostMicros {
		return deny(input, contracts.AdmissionReasonBudgetExceeded)
	}
	if input.CapabilityOperationsInWindow >= input.Capability.RateLimitPerMinute {
		return deny(input, contracts.AdmissionReasonRateLimited)
	}
	if input.Policy.MaxOperationsPerWindow > 0 && input.QuotaOperations >= input.Policy.MaxOperationsPerWindow {
		return deny(input, contracts.AdmissionReasonQuotaExceeded)
	}
	if input.Policy.MaxCostPerWindowMicros > 0 && input.QuotaCostMicros+input.RequestedCostMicros > input.Policy.MaxCostPerWindowMicros {
		return deny(input, contracts.AdmissionReasonQuotaExceeded)
	}

	mode := defaultEffectMode(capability.Effect)
	for _, rule := range input.Policy.EffectRules {
		if rule.Effect == capability.Effect {
			mode = rule.Mode
			break
		}
	}
	if capability.Effect == contracts.EffectClassUnknown {
		return deny(input, contracts.AdmissionReasonUnknownEffect)
	}
	if (capability.Effect == contracts.EffectClassIrreversibleWrite || capability.Effect == contracts.EffectClassExternalCommunication) && mode == contracts.PolicyApprovalAutomatic {
		return deny(input, contracts.AdmissionReasonPolicyDenied)
	}
	if mode == contracts.PolicyApprovalDenied {
		return deny(input, contracts.AdmissionReasonPolicyDenied)
	}

	decision := baseDecision(input)
	decision.Status = contracts.AdmissionAllowed
	decision.ReasonCode = contracts.AdmissionReasonApproved
	if mode == contracts.PolicyApprovalRequired || capability.RequiresApproval {
		decision.Status = contracts.AdmissionAwaitingApproval
		decision.ReasonCode = contracts.AdmissionReasonApprovalRequired
		approval := &contracts.OperationApprovalRequest{
			SchemaVersion: contracts.AdmissionSchemaVersion,
			ID:            contracts.NewID("opapproval"), WorkspaceID: input.WorkspaceID,
			OperationID: input.OperationID, OperationHash: input.OperationHash,
			DecisionHash: decisionHash(decision), CapabilityHash: capability.Ref.DefinitionHash,
			TargetHash: input.Target.StableHash(), InputHash: input.StableHash(),
			PolicyHash: input.Policy.PolicyHash, Effect: capability.Effect,
			RequestedBy: input.Actor, Status: contracts.ApprovalRequestPending,
			ExpiresAt: time.Now().UTC().Add(time.Duration(input.Policy.ApprovalTTLSeconds) * time.Second),
			CreatedAt: time.Now().UTC(),
		}
		decision.ApprovalID = approval.ID
		decision.DecisionHash = decisionHash(decision)
		approval.DecisionHash = decision.DecisionHash
		return AdmissionEvaluation{Decision: decision, Approval: approval}, nil
	}
	decision.DecisionHash = decisionHash(decision)
	return AdmissionEvaluation{Decision: decision}, nil
}

func deny(input contracts.AdmissionInput, reason string) (AdmissionEvaluation, error) {
	decision := baseDecision(input)
	decision.Status, decision.ReasonCode = contracts.AdmissionDenied, reason
	if reason == contracts.AdmissionReasonRateLimited && input.CapabilityRetryAt != nil {
		retryAt := input.CapabilityRetryAt.UTC()
		decision.RetryAt = &retryAt
	}
	decision.DecisionHash = decisionHash(decision)
	return AdmissionEvaluation{Decision: decision}, nil
}

func baseDecision(input contracts.AdmissionInput) contracts.AdmissionDecision {
	return contracts.AdmissionDecision{
		SchemaVersion: contracts.AdmissionSchemaVersion,
		ID:            contracts.NewID("admission"), WorkspaceID: input.WorkspaceID,
		OperationID: input.OperationID, OperationHash: input.OperationHash,
		RequestID: input.RequestID, IdempotencyKey: input.IdempotencyKey,
		Actor: input.Actor, Capability: input.Capability.Ref, Target: input.Target,
		Effect: input.Capability.Effect, PolicyID: input.Policy.PolicyID,
		PolicyVersion: input.Policy.Version, PolicyHash: input.Policy.PolicyHash,
		InputHash: input.StableHash(), CostMicros: input.RequestedCostMicros,
		CreatedAt: time.Now().UTC(),
	}
}

func decisionHash(decision contracts.AdmissionDecision) string {
	payload := struct {
		WorkspaceID, OperationID, OperationHash, InputHash              string
		CapabilityHash, TargetHash, PolicyID, PolicyVersion, PolicyHash string
		Effect, Status, ReasonCode                                      string
		ActorID, ActorWorkspace                                         string
		CostMicros                                                      int64
		RetryAt                                                         *time.Time `json:"retry_at,omitempty"`
	}{
		decision.WorkspaceID, decision.OperationID, decision.OperationHash, decision.InputHash,
		decision.Capability.DefinitionHash, decision.Target.StableHash(), decision.PolicyID, decision.PolicyVersion, decision.PolicyHash,
		string(decision.Effect), decision.Status, decision.ReasonCode,
		decision.Actor.ID, decision.Actor.WorkspaceID, decision.CostMicros, decision.RetryAt,
	}
	raw, _ := json.Marshal(payload)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func defaultEffectMode(effect contracts.EffectClass) string {
	switch effect {
	case contracts.EffectClassReadOnly, contracts.EffectClassObservation:
		return contracts.PolicyApprovalAutomatic
	case contracts.EffectClassReversibleWrite, contracts.EffectClassApprovalRequiredWrite,
		contracts.EffectClassIrreversibleWrite, contracts.EffectClassExternalCommunication:
		return contracts.PolicyApprovalRequired
	default:
		return contracts.PolicyApprovalDenied
	}
}

func resourceAllowed(policy contracts.AdmissionPolicy, kind string) bool {
	for _, candidate := range policy.AllowedResourceKinds {
		if candidate == kind {
			return true
		}
	}
	return false
}

func connectorAllowed(policy contracts.AdmissionPolicy, connector contracts.ConnectorRef) bool {
	hash := connector.StableHash()
	for _, candidate := range policy.AllowedConnectors {
		if candidate.StableHash() == hash {
			return true
		}
	}
	return false
}

func actorAllowed(policy contracts.AdmissionPolicy, actorID string) bool {
	if len(policy.AllowedActorIDs) == 0 {
		return false
	}
	for _, candidate := range policy.AllowedActorIDs {
		if candidate == strings.TrimSpace(actorID) {
			return true
		}
	}
	return false
}

func credentialsSatisfied(required []string, states []contracts.CredentialState) error {
	if len(required) == 0 {
		return nil
	}
	byRef := make(map[string]string, len(states))
	for _, state := range states {
		byRef[state.Reference] = state.Status
	}
	for _, reference := range required {
		if byRef[reference] != contracts.CredentialStateActive {
			return fmt.Errorf("credential %q is not active", reference)
		}
	}
	return nil
}
