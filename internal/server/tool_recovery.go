package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

type toolRecoveryHTTPRequest struct {
	WorkspaceID         string `json:"workspace_id,omitempty"`
	ToolRunID           string `json:"tool_run_id"`
	ExpectedEffectVer   int64  `json:"expected_effect_version,omitempty"`
	ExpectedLinkVersion int64  `json:"expected_link_version,omitempty"`
	IdempotencyKey      string `json:"idempotency_key,omitempty"`
	TTLMS               int    `json:"ttl_ms,omitempty"`
}

// handleToolRecovery inspects and finalizes an already-started sandbox attempt.
// It never dispatches a tool, and the security middleware requires the
// workspace-scoped operation:execute permission before this handler is called.
func (s *server) handleToolRecovery(w http.ResponseWriter, r *http.Request) {
	if s.toolRecovery == nil || s.toolRuns == nil || s.domainLinks == nil || s.admission == nil || s.toolExecutor == nil {
		writeErr(w, http.StatusServiceUnavailable, "tool recovery unavailable")
		return
	}
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	principal, ok := principalFromRequest(r)
	if !ok || !principal.Authenticated {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var input toolRecoveryHTTPRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&input); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid tool recovery request")
		return
	}
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	input.ToolRunID = strings.TrimSpace(input.ToolRunID)
	input.IdempotencyKey = strings.TrimSpace(input.IdempotencyKey)
	if input.ToolRunID == "" {
		writeErr(w, http.StatusBadRequest, "tool_run_id is required")
		return
	}
	workspaceID := requestWorkspace(r, input.WorkspaceID)
	if workspaceID != principal.WorkspaceID || (input.WorkspaceID != "" && input.WorkspaceID != workspaceID) {
		writeErr(w, http.StatusForbidden, "workspace isolation violation")
		return
	}
	ctx, cancel := contextWithToolRecoveryTimeout(r)
	defer cancel()
	run, err := s.toolRuns.GetByID(ctx, workspaceID, input.ToolRunID)
	if err != nil {
		writeToolRecoveryErr(w, err)
		return
	}
	currentLink, err := s.domainLinks.CurrentByDomain(ctx, workspaceID, contracts.DomainEffectKindToolRun, run.ID, contracts.DomainEffectLinkRolePrimary)
	if err != nil {
		writeToolRecoveryErr(w, err)
		return
	}
	effect, err := s.admission.GetEffectState(ctx, workspaceID, currentLink.Link.EffectID)
	if err != nil {
		writeToolRecoveryErr(w, err)
		return
	}
	if contracts.IsToolTerminal(run.Status) {
		if run.Result == nil || currentLink.Transition.ToStatus != contracts.DomainEffectLinkStatusReconciled || effect.State != contracts.ExternalEffectVerified || effect.ResponseHash == "" || run.Result.ContentHash != effect.ResponseHash || currentLink.Transition.ResultHash != effect.ResponseHash {
			writeErr(w, http.StatusConflict, "tool recovery record is inconsistent")
			return
		}
		writeToolRecoveryResult(w, run, effect, currentLink, true, "", 0)
		return
	}
	if (run.Status != contracts.ToolRunRunning && run.Status != contracts.ToolRunRecoveryRequired) ||
		(effect.State != contracts.ExternalEffectDispatching && effect.State != contracts.ExternalEffectDispatched && effect.State != contracts.ExternalEffectAcknowledged && effect.State != contracts.ExternalEffectRecoveryRequired) ||
		(currentLink.Transition.ToStatus != contracts.DomainEffectLinkStatusLinked && currentLink.Transition.ToStatus != contracts.DomainEffectLinkStatusRecoveryRequired) {
		writeErr(w, http.StatusConflict, "tool run is not awaiting recovery")
		return
	}
	if input.ExpectedEffectVer < 1 || input.ExpectedLinkVersion < 1 {
		writeErr(w, http.StatusBadRequest, "expected effect and link versions are required")
		return
	}
	if effect.Version != input.ExpectedEffectVer || currentLink.Transition.Version != input.ExpectedLinkVersion {
		writeErr(w, http.StatusConflict, "recovery versions are stale")
		return
	}
	if input.IdempotencyKey == "" {
		input.IdempotencyKey = "tool-recovery-" + contracts.HashStrings(workspaceID, run.ID, run.RequestHash)[:48]
	}
	ownerID := contracts.NewID("tool-recovery-" + contracts.HashStrings(principal.ID, workspaceID, run.ID, input.IdempotencyKey)[:12])
	lease, err := s.admission.AcquireEffectLease(ctx, workspaceID, currentLink.Link.OperationID, currentLink.Link.EffectID, ownerID, boundedEffectLeaseTTL(int64(input.TTLMS)))
	if err != nil {
		writeToolRecoveryErr(w, err)
		return
	}
	defer func() { _ = s.admission.ReleaseEffectLease(ctx, lease.Lease) }()
	// Re-read the versions after acquiring fenced ownership. Provider lookup is
	// read-only; finalization below checks the same expected versions again in
	// its transaction.
	currentLink, err = s.domainLinks.CurrentByDomain(ctx, workspaceID, contracts.DomainEffectKindToolRun, run.ID, contracts.DomainEffectLinkRolePrimary)
	if err != nil {
		writeToolRecoveryErr(w, err)
		return
	}
	effect, err = s.admission.GetEffectState(ctx, workspaceID, currentLink.Link.EffectID)
	if err != nil {
		writeToolRecoveryErr(w, err)
		return
	}
	if effect.Version != input.ExpectedEffectVer || currentLink.Transition.Version != input.ExpectedLinkVersion {
		writeErr(w, http.StatusConflict, "recovery state changed while acquiring ownership")
		return
	}
	currentEffect := effect
	prepared, err := s.toolRecovery.Prepare(ctx, workspaceID, run.ID, requestIDFromRequest(r), lease.Lease.OwnerID, lease.Lease.Fence, input.ExpectedEffectVer, input.ExpectedLinkVersion, principal.Actor())
	if err != nil {
		writeToolRecoveryErr(w, err)
		return
	}
	run, currentEffect, currentLink = prepared.Run, prepared.Effect, prepared.Link
	identity, observation, err := s.toolExecutor.ReconcileSandboxAttempt(ctx, run, currentLink.Link)
	if err != nil {
		writeToolRecoveryErr(w, err)
		return
	}
	if observation.State != contracts.SandboxAttemptCompleted {
		writeJSON(w, http.StatusAccepted, map[string]any{
			"workspace_id": workspaceID, "tool_run_id": run.ID, "status": run.Status,
			"observation_state": observation.State, "identity_hash": identity.StableHash(),
			"effect_version": currentEffect.Version, "link_version": currentLink.Transition.Version,
			"message": "the exact sandbox attempt has no completed result; it was not re-executed",
		})
		return
	}
	command := contracts.ToolRecoveryFinalizeRequest{
		WorkspaceID: workspaceID, ToolRunID: run.ID, RequestID: requestIDFromRequest(r), ToolRequestID: run.RequestID,
		OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence,
		ExpectedEffectVer: currentEffect.Version, ExpectedLinkVersion: currentLink.Transition.Version,
		IdempotencyKey: input.IdempotencyKey, Actor: principal.Actor(), Identity: identity, Observation: observation,
	}
	finalized, err := s.toolRecovery.Finalize(ctx, command)
	if err != nil {
		writeToolRecoveryErr(w, err)
		return
	}
	finalLink, linkErr := s.domainLinks.CurrentByDomain(ctx, workspaceID, contracts.DomainEffectKindToolRun, run.ID, contracts.DomainEffectLinkRolePrimary)
	if linkErr != nil {
		writeToolRecoveryErr(w, linkErr)
		return
	}
	writeToolRecoveryResult(w, finalized.Run, finalized.Effect, finalLink, finalized.Duplicate, identity.StableHash(), lease.Lease.Fence)
}

func contextWithToolRecoveryTimeout(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), 30*time.Second)
}

func writeToolRecoveryResult(w http.ResponseWriter, run contracts.ToolRun, effect store.EffectState, link store.DomainEffectLinkCurrent, duplicate bool, identityHash string, fence uint64) {
	resultHash := ""
	if run.Result != nil {
		resultHash = run.Result.ContentHash
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"workspace_id": run.WorkspaceID, "tool_run_id": run.ID, "status": run.Status,
		"effect_state": effect.State, "effect_version": effect.Version,
		"link_status": link.Transition.ToStatus, "link_version": link.Transition.Version,
		"result_hash": resultHash, "identity_hash": identityHash,
		"duplicate": duplicate, "recovery_fence": fence,
	})
}

func writeToolRecoveryErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrToolRunMissing), errors.Is(err, store.ErrDomainEffectLinkNotFound):
		writeErr(w, http.StatusNotFound, "tool recovery record not found")
	case errors.Is(err, store.ErrToolRecoveryConflict), errors.Is(err, store.ErrToolRecoveryPending), errors.Is(err, store.ErrEffectLeaseHeld), errors.Is(err, store.ErrEffectLeaseFenced), errors.Is(err, store.ErrEffectLeaseExpired), errors.Is(err, store.ErrDomainEffectLinkFenced):
		writeErr(w, http.StatusConflict, "tool recovery authority or evidence is stale")
	case errors.Is(err, store.ErrOperationWorkspace), errors.Is(err, store.ErrDomainEffectLinkWorkspace):
		writeErr(w, http.StatusForbidden, "workspace isolation violation")
	default:
		writeErr(w, http.StatusConflict, "tool recovery could not be completed")
	}
}
