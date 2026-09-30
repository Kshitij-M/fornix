package contracts

import (
	"fmt"
	"strings"
)

// ToolRecoveryFinalizeRequest binds a provider observation to the durable
// attempt and the effect-recovery lease that authorizes local finalization.
// It contains no command or credential material.
type ToolRecoveryFinalizeRequest struct {
	WorkspaceID         string                    `json:"workspace_id"`
	ToolRunID           string                    `json:"tool_run_id"`
	OwnerID             string                    `json:"owner_id"`
	Fence               uint64                    `json:"fence"`
	ExpectedEffectVer   int64                     `json:"expected_effect_version"`
	ExpectedLinkVersion int64                     `json:"expected_link_version"`
	RequestID           string                    `json:"request_id"`
	ToolRequestID       string                    `json:"tool_request_id"`
	IdempotencyKey      string                    `json:"idempotency_key"`
	Actor               ActorRef                  `json:"actor"`
	Identity            SandboxExecutionIdentity  `json:"identity"`
	Observation         SandboxAttemptObservation `json:"observation"`
}

// Normalize validates bounded identities and accepts only a completed,
// hash-bound sandbox result. Non-completed observations remain recoverable and
// cannot be used to finalize a tool run.
func (r *ToolRecoveryFinalizeRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("tool recovery request is nil")
	}
	workspace, err := normalizeDomainWorkspace(r.WorkspaceID)
	if err != nil {
		return err
	}
	r.WorkspaceID = workspace
	for field, value := range map[string]*string{
		"tool_run_id": &r.ToolRunID, "owner_id": &r.OwnerID,
		"request_id": &r.RequestID, "tool_request_id": &r.ToolRequestID, "idempotency_key": &r.IdempotencyKey,
	} {
		limit := MaxDomainIDLength
		if field == "idempotency_key" {
			limit = MaxIdempotencyLength
		}
		*value, err = normalizeDomainIdentifier(*value, "tool recovery "+field, limit, true)
		if err != nil {
			return err
		}
	}
	if r.Fence == 0 || r.Fence > uint64(1<<63-1) || r.ExpectedEffectVer < 1 || r.ExpectedLinkVersion < 1 {
		return fmt.Errorf("tool recovery fence and expected versions must be positive")
	}
	if err := r.Identity.Normalize(); err != nil {
		return fmt.Errorf("tool recovery identity: %w", err)
	}
	if r.Identity.WorkspaceID != workspace || r.Identity.ToolRunID != r.ToolRunID {
		return fmt.Errorf("tool recovery identity crosses workspace or run boundary")
	}
	if r.Observation.State != SandboxAttemptCompleted || r.Observation.Result == nil {
		return fmt.Errorf("tool recovery requires a completed attempt")
	}
	result := r.Observation.Result
	result.Status = strings.ToLower(strings.TrimSpace(result.Status))
	if err := r.Observation.Normalize(r.Identity); err != nil {
		return fmt.Errorf("tool recovery observation: %w", err)
	}
	if result.Status != ToolRunSucceeded && result.Status != ToolRunFailed {
		return fmt.Errorf("completed sandbox result must be succeeded or failed")
	}
	if result.Failure != nil && result.Status != ToolRunFailed {
		return fmt.Errorf("successful sandbox result cannot contain a failure")
	}
	if err := normalizeDomainActor(&r.Actor, workspace); err != nil {
		return err
	}
	return nil
}
