package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

// embeddingRecoveryHTTPRequest is deliberately hash-and-version only. The
// provider proof is obtained by the configured provider adapter; callers
// cannot submit source text, credentials, or an arbitrary vector.
type embeddingRecoveryHTTPRequest struct {
	WorkspaceID           string `json:"workspace_id,omitempty"`
	RequestID             string `json:"request_id"`
	ExpectedEffectVersion int64  `json:"expected_effect_version"`
	ExpectedLinkVersion   int64  `json:"expected_link_version"`
	IdempotencyKey        string `json:"idempotency_key,omitempty"`
	TTLMS                 int    `json:"ttl_ms,omitempty"`
}

func (s *server) handleEmbeddingRecovery(w http.ResponseWriter, r *http.Request) {
	if s.embeddingGateway == nil || s.embeddingCalls == nil || s.admission == nil || s.domainLinks == nil {
		writeErr(w, http.StatusServiceUnavailable, "embedding recovery unavailable")
		return
	}
	principal, ok := principalFromRequest(r)
	if !ok || !principal.Authenticated {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var input embeddingRecoveryHTTPRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&input); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid embedding recovery request")
		return
	}
	input.RequestID = strings.TrimSpace(input.RequestID)
	input.WorkspaceID = strings.TrimSpace(input.WorkspaceID)
	if input.RequestID == "" {
		writeErr(w, http.StatusBadRequest, "request_id is required")
		return
	}
	workspaceID := requestWorkspace(r, input.WorkspaceID)
	if workspaceID != principal.WorkspaceID || (input.WorkspaceID != "" && input.WorkspaceID != workspaceID) {
		writeErr(w, http.StatusForbidden, "workspace isolation violation")
		return
	}
	if input.ExpectedEffectVersion < 1 || input.ExpectedLinkVersion < 1 {
		writeErr(w, http.StatusBadRequest, "expected effect and link versions are required")
		return
	}
	ctx := r.Context()
	record, err := s.embeddingCalls.Get(ctx, workspaceID, input.RequestID)
	if err != nil {
		writeEmbeddingRecoveryErr(w, err)
		return
	}
	currentLink, err := s.domainLinks.CurrentByDomain(ctx, workspaceID, contracts.DomainEffectKindEmbeddingCall, record.IdempotencyKey, contracts.DomainEffectLinkRolePrimary)
	if err != nil {
		writeEmbeddingRecoveryErr(w, err)
		return
	}
	effect, err := s.admission.GetEffectState(ctx, workspaceID, currentLink.Link.EffectID)
	if err != nil {
		writeEmbeddingRecoveryErr(w, err)
		return
	}
	if record.Status != contracts.EmbeddingCallRecoveryRequired {
		if input.IdempotencyKey != "" && currentLink.Transition.ToStatus == contracts.DomainEffectLinkStatusReconciled && currentLink.Transition.IdempotencyKey == input.IdempotencyKey {
			writeJSON(w, http.StatusOK, map[string]any{
				"workspace_id": workspaceID, "request_id": record.RequestID, "status": record.Status,
				"source_hash": record.SourceHash, "vector_hash": record.VectorHash, "provider_request_id": record.ProviderRequestID,
				"effect": publicEffectState(effect), "link_status": currentLink.Transition.ToStatus, "link_version": currentLink.Transition.Version,
				"duplicate": true, "idempotency_key": input.IdempotencyKey, "provider": record.Provider.Provider, "model": record.Provider.Model,
				"duration_ms": record.DurationMS, "retention_class": record.RetentionClass, "request_hash": record.RequestHash,
			})
			return
		}
		writeErr(w, http.StatusConflict, "embedding call is not awaiting recovery")
		return
	}
	if effect.Version != input.ExpectedEffectVersion || currentLink.Transition.Version != input.ExpectedLinkVersion {
		writeErr(w, http.StatusConflict, "recovery versions are stale")
		return
	}
	if effect.State != contracts.ExternalEffectRecoveryRequired {
		writeErr(w, http.StatusConflict, "generic effect is not awaiting recovery")
		return
	}
	key := strings.TrimSpace(input.IdempotencyKey)
	if key == "" {
		key = "embedding-recovery-" + contracts.HashStrings(workspaceID, input.RequestID, record.ProviderRequestID)[:48]
	}
	// A principal is an actor, not a worker instance. Every HTTP delivery gets
	// a unique lease owner so two concurrent requests from the same user cannot
	// reuse one fence and contact the provider at the same time.
	ownerID := contracts.NewID("embedding-recovery-" + contracts.HashStrings(principal.ID, workspaceID, input.RequestID, key)[:12])
	lease, err := s.admission.AcquireEffectLease(ctx, workspaceID, currentLink.Link.OperationID, currentLink.Link.EffectID, ownerID, boundedEffectLeaseTTL(int64(input.TTLMS)))
	if err != nil {
		writeEmbeddingRecoveryErr(w, err)
		return
	}
	command := contracts.EmbeddingRecoveryFinalizeRequest{
		WorkspaceID: workspaceID, RequestID: record.RequestID, OwnerID: ownerID,
		Fence: lease.Lease.Fence, ExpectedEffectVersion: input.ExpectedEffectVersion,
		ExpectedLinkVersion: input.ExpectedLinkVersion, IdempotencyKey: key, Actor: principal.Actor(),
	}
	resolved, err := s.embeddingGateway.ReconcileWithLease(ctx, command)
	if err != nil {
		writeEmbeddingRecoveryErr(w, err)
		return
	}
	finalEffect, effectErr := s.admission.GetEffectState(ctx, workspaceID, currentLink.Link.EffectID)
	if effectErr != nil {
		writeEmbeddingRecoveryErr(w, effectErr)
		return
	}
	finalLink, linkErr := s.domainLinks.CurrentByDomain(ctx, workspaceID, contracts.DomainEffectKindEmbeddingCall, record.IdempotencyKey, contracts.DomainEffectLinkRolePrimary)
	if linkErr != nil {
		writeEmbeddingRecoveryErr(w, linkErr)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"workspace_id": workspaceID, "request_id": resolved.RequestID, "status": resolved.Status,
		"source_hash": resolved.SourceHash, "vector_hash": resolved.VectorHash,
		"provider_request_id": resolved.ProviderRequestID, "effect": publicEffectState(finalEffect),
		"link_status": finalLink.Transition.ToStatus, "link_version": finalLink.Transition.Version,
		"duplicate": false, "idempotency_key": key, "provider": resolved.Provider.Provider,
		"model": resolved.Provider.Model, "duration_ms": resolved.DurationMS,
		"retention_class": resolved.RetentionClass,
		"request_hash":    resolved.RequestHash, "actor_id": principal.ID,
		"fence": lease.Lease.Fence, "expected_effect_version": input.ExpectedEffectVersion,
		"expected_link_version": input.ExpectedLinkVersion,
	})
}

func writeEmbeddingRecoveryErr(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrEffectLeaseHeld) || errors.Is(err, store.ErrEffectLeaseFenced) || errors.Is(err, store.ErrEffectLeaseExpired) || errors.Is(err, store.ErrDomainEffectLinkFenced) {
		writeErr(w, http.StatusConflict, "embedding recovery authority is stale or held")
		return
	}
	if errors.Is(err, store.ErrDomainEffectLinkNotFound) || errors.Is(err, store.ErrEmbeddingCallMissing) {
		writeErr(w, http.StatusNotFound, "embedding recovery record not found")
		return
	}
	if errors.Is(err, store.ErrDomainEffectLinkWorkspace) || errors.Is(err, store.ErrOperationWorkspace) {
		writeErr(w, http.StatusForbidden, "workspace isolation violation")
		return
	}
	writeErr(w, http.StatusConflict, "embedding recovery could not be completed")
}
