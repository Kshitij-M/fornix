package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
	incidentworkflow "github.com/omaveda/fornix/internal/workflows/incident"
)

func (s *server) handleIncidentWorkflowStart(w http.ResponseWriter, r *http.Request) {
	var request contracts.IncidentWorkflowRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 256<<10))
	if err := decoder.Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid incident workflow request")
		return
	}
	request.Event.WorkspaceID = requestWorkspace(r, request.Event.WorkspaceID)
	request.Event.Actor = requestActor(r)
	request.Event.DeliveryMode = strings.TrimSpace(request.Event.DeliveryMode)
	if request.Event.DeliveryMode == "" {
		request.Event.DeliveryMode = contracts.IncidentDeliveryFake
	}
	request.Event.RequestID = requestIDFromRequest(r)
	if request.Event.IdempotencyKey == "" {
		request.Event.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	if err := registerIncidentConnector(s.connectorRegistry, request.Event.WorkspaceID); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "incident connector unavailable")
		return
	}
	if err := s.connectorRegistry.TrustWorkspace(request.Event.WorkspaceID, "workspace-builtins-v1"); err != nil {
		writeErr(w, http.StatusServiceUnavailable, "incident connector trust unavailable")
		return
	}
	result, err := s.incidentWorkflows.Start(r.Context(), request)
	if err != nil {
		writeIncidentError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleIncidentWorkflowGet(w http.ResponseWriter, r *http.Request, runID string) {
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	result, err := s.incidentWorkflows.Get(r.Context(), workspaceID, runID)
	if err != nil {
		writeIncidentError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleIncidentWorkflowApprove(w http.ResponseWriter, r *http.Request, runID string) {
	var input struct {
		WorkspaceID    string `json:"workspace_id,omitempty"`
		Decision       string `json:"decision"`
		IdempotencyKey string `json:"idempotency_key,omitempty"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10)).Decode(&input); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid incident approval request")
		return
	}
	workspaceID := requestWorkspace(r, input.WorkspaceID)
	if input.IdempotencyKey == "" {
		input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	result, err := s.incidentWorkflows.Approve(r.Context(), workspaceID, runID, input.Decision, input.IdempotencyKey, requestActor(r))
	if err != nil {
		writeIncidentError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleIncidentWorkflowReplay(w http.ResponseWriter, r *http.Request, runID string) {
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	fromVersion := int64(0)
	if raw := strings.TrimSpace(r.URL.Query().Get("from_version")); raw != "" {
		parsed, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || parsed < 0 {
			writeErr(w, http.StatusBadRequest, "invalid from_version")
			return
		}
		fromVersion = parsed
	}
	result, err := s.incidentWorkflows.Replay(r.Context(), workspaceID, runID, fromVersion)
	if err != nil {
		writeIncidentError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func writeIncidentError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, store.ErrIncidentNotFound), errors.Is(err, store.ErrWorkflowNotFound):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrIncidentConflict), errors.Is(err, store.ErrIncidentIdempotency), errors.Is(err, store.ErrIncidentApprovalConflict), errors.Is(err, store.ErrWorkflowIdempotency), errors.Is(err, store.ErrWorkflowReplay), errors.Is(err, incidentworkflow.ErrIncidentApprovalRequired), errors.Is(err, incidentworkflow.ErrIncidentApprovalConflict):
		status = http.StatusConflict
	case errors.Is(err, store.ErrIncidentWorkspace), errors.Is(err, store.ErrWorkflowWorkspace), errors.Is(err, store.ErrOperationLeaseFenced), errors.Is(err, store.ErrOperationLeaseExpired), errors.Is(err, store.ErrOperationLeaseReleased):
		status = http.StatusForbidden
	case errors.Is(err, store.ErrWorkflowLease), errors.Is(err, store.ErrOperationLeaseHeld):
		status = http.StatusConflict
	}
	writeErr(w, status, err.Error())
}
