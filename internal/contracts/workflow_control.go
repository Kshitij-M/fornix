package contracts

import (
	"fmt"
	"strings"
)

// WorkflowCreateRequest is the domain-neutral admission envelope for a
// durable workflow. The operation and plan remain the authoritative typed
// inputs; a workflow is a resumable execution projection over them.
//
// The envelope intentionally contains hashes and references only. Connector
// payloads, prompts, credentials, and large output belong in the existing
// evidence and artifact authorities.
type WorkflowCreateRequest struct {
	WorkspaceID string           `json:"workspace_id"`
	Operation   OperationRequest `json:"operation"`
	Plan        OperationPlan    `json:"plan"`
	Budget      WorkflowBudget   `json:"budget"`
	TaskFence   uint64           `json:"task_fence,omitempty"`
	Idempotency string           `json:"idempotency_key,omitempty"`
}

// Normalize binds the workflow request to the authenticated workspace and
// actor. Callers must supply those values from the authentication boundary;
// accepting a different actor or workspace would create an authorization
// confusion primitive.
func (r *WorkflowCreateRequest) Normalize(workspaceID string, actor ActorRef) error {
	if r == nil {
		return fmt.Errorf("workflow create request is nil")
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return fmt.Errorf("workflow workspace_id is required")
	}
	if declared := strings.TrimSpace(r.WorkspaceID); declared != "" && declared != workspaceID {
		return fmt.Errorf("workflow workspace_id conflicts with authenticated workspace")
	}
	if declared := strings.TrimSpace(r.Operation.WorkspaceID); declared != "" && declared != workspaceID {
		return fmt.Errorf("workflow operation crosses workspace boundary")
	}
	r.WorkspaceID = workspaceID
	r.Operation.WorkspaceID = workspaceID
	r.Operation.Actor = actor
	if r.Idempotency != "" {
		if r.Operation.IdempotencyKey != "" && r.Operation.IdempotencyKey != r.Idempotency {
			return fmt.Errorf("workflow idempotency key mismatch")
		}
		r.Operation.IdempotencyKey = r.Idempotency
	}
	if err := r.Operation.Normalize(); err != nil {
		return fmt.Errorf("workflow operation: %w", err)
	}
	r.Plan.WorkspaceID = workspaceID
	r.Plan.Actor = actor
	if r.Plan.OperationID == "" {
		r.Plan.OperationID = r.Operation.ID
	}
	if r.Plan.OperationHash == "" {
		r.Plan.OperationHash = r.Operation.StableHash()
	}
	if err := r.Plan.Normalize(); err != nil {
		return fmt.Errorf("workflow plan: %w", err)
	}
	if r.Plan.OperationID != r.Operation.ID || r.Plan.OperationHash != r.Operation.StableHash() {
		return fmt.Errorf("workflow plan is not bound to operation")
	}
	if err := r.Budget.Normalize(); err != nil {
		return fmt.Errorf("workflow budget: %w", err)
	}
	if r.Operation.Task != nil && r.TaskFence == 0 {
		return fmt.Errorf("task-bound workflow requires task_fence")
	}
	return nil
}

// WorkflowAdvanceRequest is the caller-controlled, bounded continuation
// command. Ownership is derived from the authenticated actor; task fencing is
// the only task fact accepted from the worker.
type WorkflowAdvanceRequest struct {
	TaskFence uint64 `json:"task_fence,omitempty"`
}

// WorkflowResumeRequest contains only a canonical, bounded continuation
// result. It cannot inject actor, owner, workspace, or fence fields.
type WorkflowResumeRequest struct {
	StepID string             `json:"step_id"`
	Result WorkflowStepResult `json:"result"`
}

// WorkflowVerifyRequest requests proof of one already-reserved external
// effect. The effect lease fence is supplied by the recovery worker; the
// workflow operation and task fences remain authenticated server context.
type WorkflowVerifyRequest struct {
	StepID              string `json:"step_id"`
	EffectFence         uint64 `json:"effect_fence"`
	ExpectedEffectVer   int64  `json:"expected_effect_version"`
	ExpectedLinkVersion int64  `json:"expected_link_version"`
	IdempotencyKey      string `json:"idempotency_key"`
	TaskFence           uint64 `json:"task_fence,omitempty"`
}

func (r *WorkflowVerifyRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("workflow verification request is nil")
	}
	r.StepID = strings.TrimSpace(r.StepID)
	r.IdempotencyKey = strings.TrimSpace(r.IdempotencyKey)
	if r.StepID == "" || r.IdempotencyKey == "" || r.EffectFence == 0 || r.ExpectedEffectVer < 1 || r.ExpectedLinkVersion < 1 {
		return fmt.Errorf("workflow verification step, idempotency, fence, and versions are required")
	}
	if len(r.IdempotencyKey) > MaxIdempotencyLength || strings.ContainsAny(r.IdempotencyKey, "\x00\r\n") {
		return fmt.Errorf("workflow verification idempotency key is invalid")
	}
	return nil
}

// WorkflowVerificationResult is the bounded operator response after local
// effect reconciliation and workflow checkpoint advancement.
type WorkflowVerificationResult struct {
	Run           WorkflowRun              `json:"run"`
	Outcome       EffectVerificationResult `json:"outcome"`
	EffectState   string                   `json:"effect_state"`
	EffectVersion int64                    `json:"effect_version"`
	LinkID        string                   `json:"link_id"`
	LinkStatus    string                   `json:"link_status"`
	LinkVersion   int64                    `json:"link_version"`
	Duplicate     bool                     `json:"duplicate"`
}

// WorkflowCancelRequest records a durable cancellation command. The actor is
// always taken from authentication and is therefore not represented here.
type WorkflowCancelRequest struct {
	Reason string `json:"reason,omitempty"`
}

// WorkflowReplayRequest is read-only and bounded. Replay never executes a
// connector, model, tool, or other external system.
type WorkflowReplayRequest struct {
	FromVersion int64 `json:"from_version,omitempty"`
	Limit       int   `json:"limit,omitempty"`
}
