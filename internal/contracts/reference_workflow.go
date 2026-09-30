package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

// ReferenceWorkflowSchemaVersion is the version of the fake-first reference
// workflow qualification contract. These scenarios exercise the generic
// operation/effect/replay seams; they are not provider-specific authorities.
const ReferenceWorkflowSchemaVersion = 1

const (
	ReferenceWorkflowIncidentResponse          = "incident_response"
	ReferenceWorkflowDataPipeline              = "data_pipeline"
	ReferenceWorkflowCustomerSupport           = "customer_support"
	ReferenceWorkflowInfrastructureMaintenance = "infrastructure_maintenance"
)

const (
	ReferenceWorkflowEffectNotApplicable = "not_applicable"
	ReferenceWorkflowEffectUnresolved    = "unresolved"
	ReferenceWorkflowEffectVerified      = "verified"
)

// ReferenceWorkflowRequest is a bounded, domain-neutral scenario intent. It
// contains identity and hashes only; raw incident text, customer content,
// credentials, and provider payloads belong in their authoritative stores.
type ReferenceWorkflowRequest struct {
	SchemaVersion    int         `json:"schema_version,omitempty"`
	ID               string      `json:"id,omitempty"`
	RequestID        string      `json:"request_id,omitempty"`
	IdempotencyKey   string      `json:"idempotency_key,omitempty"`
	WorkspaceID      string      `json:"workspace_id"`
	Actor            ActorRef    `json:"actor"`
	Kind             string      `json:"kind"`
	Target           ResourceRef `json:"target"`
	IntentHash       string      `json:"intent_hash"`
	Effectful        bool        `json:"effectful,omitempty"`
	RequiresApproval bool        `json:"requires_approval,omitempty"`
}

// Normalize validates the request and derives missing delivery identities
// from logical content so repeated fixture submissions remain deterministic.
func (r *ReferenceWorkflowRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("reference workflow request is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = ReferenceWorkflowSchemaVersion
	}
	if r.SchemaVersion != ReferenceWorkflowSchemaVersion {
		return fmt.Errorf("unsupported reference workflow schema_version %d", r.SchemaVersion)
	}
	workspace, err := normalizeDomainWorkspace(r.WorkspaceID)
	if err != nil {
		return err
	}
	kind, err := normalizeDomainName(r.Kind, "reference workflow kind", MaxDomainNameLength, true)
	if err != nil {
		return err
	}
	if !validReferenceWorkflowKind(kind) {
		return fmt.Errorf("unsupported reference workflow kind %q", kind)
	}
	if err := normalizeDomainActor(&r.Actor, workspace); err != nil {
		return err
	}
	if err := r.Target.Normalize(); err != nil {
		return fmt.Errorf("reference workflow target: %w", err)
	}
	if r.Target.WorkspaceID != workspace {
		return fmt.Errorf("reference workflow target crosses workspace")
	}
	intentHash, err := normalizeDomainHash(r.IntentHash, "reference workflow intent_hash", true)
	if err != nil {
		return err
	}
	if r.Effectful && !r.RequiresApproval {
		return fmt.Errorf("effectful reference workflow requires explicit approval")
	}
	if r.ID != "" {
		r.ID, err = normalizeDomainIdentifier(r.ID, "reference workflow id", MaxDomainIDLength, true)
		if err != nil {
			return err
		}
	}
	if r.RequestID != "" {
		r.RequestID, err = normalizeDomainIdentifier(r.RequestID, "reference workflow request_id", MaxDomainIDLength, true)
		if err != nil {
			return err
		}
	}
	if r.IdempotencyKey != "" {
		r.IdempotencyKey, err = normalizeDomainIdentifier(r.IdempotencyKey, "reference workflow idempotency_key", MaxIdempotencyLength, true)
		if err != nil {
			return err
		}
	}
	r.WorkspaceID, r.Kind, r.IntentHash = workspace, kind, intentHash
	if r.ID == "" || r.RequestID == "" || r.IdempotencyKey == "" {
		identity := HashStrings("fornix-reference-workflow", workspace, kind, r.Target.StableHash(), intentHash)
		if r.ID == "" {
			r.ID = "reference-" + identity[:16]
		}
		if r.RequestID == "" {
			r.RequestID = r.ID + "-request"
		}
		if r.IdempotencyKey == "" {
			r.IdempotencyKey = "reference-" + identity[:32]
		}
	}
	return nil
}

// StableHash identifies logical scenario content and intentionally excludes
// delivery identities, matching OperationRequest.CanonicalHash semantics.
func (r ReferenceWorkflowRequest) StableHash() string {
	clone := r
	if err := clone.Normalize(); err != nil {
		return ""
	}
	clone.ID, clone.RequestID, clone.IdempotencyKey = "", "", ""
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func validReferenceWorkflowKind(kind string) bool {
	switch kind {
	case ReferenceWorkflowIncidentResponse, ReferenceWorkflowDataPipeline, ReferenceWorkflowCustomerSupport, ReferenceWorkflowInfrastructureMaintenance:
		return true
	default:
		return false
	}
}

// ReferenceWorkflowPlan is the deterministic generic operation plan emitted
// by a reference scenario builder. The operation/effect stores remain the
// authorities once a real adapter submits this plan.
type ReferenceWorkflowPlan struct {
	SchemaVersion int                      `json:"schema_version"`
	Request       ReferenceWorkflowRequest `json:"request"`
	Operation     OperationRequest         `json:"operation"`
	Plan          OperationPlan            `json:"plan"`
	Budget        WorkflowBudget           `json:"budget"`
	PlanHash      string                   `json:"plan_hash"`
}

// ReferenceWorkflowTrace is a bounded offline replay result. An unresolved
// effect is deliberately not represented as success: external exactly-once
// execution cannot be inferred from a local fixture.
type ReferenceWorkflowTrace struct {
	SchemaVersion       int      `json:"schema_version"`
	WorkspaceID         string   `json:"workspace_id"`
	WorkflowID          string   `json:"workflow_id"`
	PlanHash            string   `json:"plan_hash"`
	StepOutputHashes    []string `json:"step_output_hashes"`
	TerminalStatus      string   `json:"terminal_status"`
	ExternalEffectState string   `json:"external_effect_state"`
	ReplayHash          string   `json:"replay_hash"`
}

func (t ReferenceWorkflowTrace) StableHash() string {
	clone := t
	clone.ReplayHash = ""
	raw, _ := json.Marshal(clone)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// ReferenceWorkflowStepHash is shared by the offline runner and tests so a
// trace cannot accidentally include raw step output.
func ReferenceWorkflowStepHash(planHash string, step OperationStep) string {
	return HashStrings("fornix-reference-step", planHash, step.ID, step.Kind, string(step.Effect), step.InputHash)
}

func normalizeReferenceWorkflowHash(value, field string) (string, error) {
	value = strings.TrimSpace(value)
	return normalizeDomainHash(value, field, true)
}
