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

func (s *server) handleArtifactPut(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var request contracts.ArtifactCreateRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	if request.IdempotencyKey == "" {
		request.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	result, err := s.artifacts.Put(ctx, store.ArtifactPutInput{
		WorkspaceID: requestWorkspace(r, request.WorkspaceID), Kind: request.Kind,
		MediaType: request.MediaType, Raw: request.Raw, Manifest: request.Manifest,
		Retention: request.Retention, SourceKind: request.SourceKind, SourceID: request.SourceID,
		Role: request.Role, IdempotencyKey: request.IdempotencyKey, Actor: requestActor(r),
	})
	if err != nil {
		writeArtifactErr(w, err)
		return
	}
	if s.observability != nil {
		if metrics, metricsErr := s.artifacts.Metrics(ctx, requestWorkspace(r, request.WorkspaceID)); metricsErr == nil {
			_ = s.observability.ObserveArtifact(ctx, metrics, "put")
		}
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleArtifactDisclosure(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var request contracts.ArtifactDisclosureRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	result, err := s.artifacts.Disclose(ctx, request)
	if err != nil {
		writeArtifactErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleArtifactProvenance(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var request struct {
		WorkspaceID  string            `json:"workspace_id,omitempty"`
		FromArtifact int64             `json:"from_artifact_id"`
		ToArtifact   int64             `json:"to_artifact_id"`
		Relation     string            `json:"relation"`
		Metadata     map[string]string `json:"metadata,omitempty"`
	}
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	link, created, err := s.artifacts.AddProvenance(ctx, store.ArtifactProvenanceInput{
		WorkspaceID: requestWorkspace(r, request.WorkspaceID), FromArtifact: request.FromArtifact,
		ToArtifact: request.ToArtifact, Relation: request.Relation, Metadata: request.Metadata,
	})
	if err != nil {
		writeArtifactErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"link": link, "created": created})
}

func (s *server) handleArtifactBackfill(w http.ResponseWriter, r *http.Request) {
	var request contracts.ArtifactBackfillRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	request.Actor = requestActor(r)
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	result, err := s.artifacts.Backfill(ctx, request)
	if err != nil {
		writeArtifactErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleArtifactRetention(w http.ResponseWriter, r *http.Request) {
	var request contracts.ArtifactRetentionSweepRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	request.Actor = requestActor(r)
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	result, err := s.artifacts.RetentionSweep(ctx, request)
	if err != nil {
		writeArtifactErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleArtifactIntegrity(w http.ResponseWriter, r *http.Request) {
	var request contracts.ArtifactIntegrityRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	result, err := s.artifacts.VerifyBatch(ctx, request)
	if err != nil {
		writeArtifactErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleArtifactMetrics(w http.ResponseWriter, r *http.Request) {
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	result, err := s.artifacts.Metrics(ctx, workspaceID)
	if err != nil {
		writeArtifactErr(w, err)
		return
	}
	if s.observability != nil {
		_ = s.observability.ObserveArtifact(ctx, result, "metrics")
	}
	writeJSON(w, http.StatusOK, result)
}

func writeArtifactErr(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, store.ErrArtifactNotFound):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrArtifactDeleted):
		status = http.StatusGone
	case errors.Is(err, store.ErrArtifactConflict), errors.Is(err, store.ErrArtifactRefConflict), errors.Is(err, store.ErrArtifactReferenced):
		status = http.StatusConflict
	case errors.Is(err, store.ErrArtifactIntegrity):
		status = http.StatusUnprocessableEntity
	}
	writeErr(w, status, err.Error())
}
