package server

import (
	"net/http"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
)

func (s *server) handleReadinessPolicies(w http.ResponseWriter, r *http.Request) {
	if s.readiness == nil {
		writeErr(w, http.StatusServiceUnavailable, "readiness store is unavailable")
		return
	}
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/qualification/readiness/policies"), "/")
	if path == "" {
		switch r.Method {
		case http.MethodGet:
			limit, err := qualificationPageLimit(r.URL.Query().Get("limit"))
			if err != nil {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
			page, err := s.readiness.ListFreshnessPolicies(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), r.URL.Query().Get("deployment_id"), limit, r.URL.Query().Get("cursor"))
			if err != nil {
				writeQualificationError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, page)
			return
		case http.MethodPost:
			var input contracts.ReadinessFreshnessPolicyRequest
			if err := decodeQualificationJSON(r, &input); err != nil {
				writeErr(w, http.StatusBadRequest, "invalid readiness freshness policy request")
				return
			}
			input.WorkspaceID = requestWorkspace(r, input.WorkspaceID)
			input.Actor = qualificationAuditActor(r)
			if input.IdempotencyKey == "" {
				input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
			}
			if input.IdempotencyKey == "" {
				input.IdempotencyKey = requestIDFromRequest(r)
			}
			policy, created, err := s.readiness.PublishFreshnessPolicy(r.Context(), input, nowUTC())
			if err != nil {
				writeQualificationError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"policy": policy, "created": created})
			return
		default:
			writeErr(w, http.StatusMethodNotAllowed, "GET or POST only")
			return
		}
	}
	if path == "current" && r.Method == http.MethodGet {
		policy, err := s.readiness.CurrentFreshnessPolicy(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), r.URL.Query().Get("deployment_id"))
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, policy)
		return
	}
	writeErr(w, http.StatusNotFound, "readiness freshness policy path not found")
}

func (s *server) handleReadinessReview(w http.ResponseWriter, r *http.Request) {
	if s.readiness == nil {
		writeErr(w, http.StatusServiceUnavailable, "readiness store is unavailable")
		return
	}
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	var input contracts.ReadinessReviewRequest
	if err := decodeQualificationJSON(r, &input); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid readiness review request")
		return
	}
	input.WorkspaceID = requestWorkspace(r, input.WorkspaceID)
	input.Actor = qualificationAuditActor(r)
	review, err := s.readiness.Review(r.Context(), input, nowUTC())
	if err != nil {
		writeQualificationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, review)
}

// handleReadinessSnapshots exposes advisory readiness observations and structured
// incident annotations. It never replaces the release admission decision.
func (s *server) handleReadinessSnapshots(w http.ResponseWriter, r *http.Request) {
	if s.readiness == nil {
		writeErr(w, http.StatusServiceUnavailable, "readiness store is unavailable")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/qualification/readiness/snapshots")
	path = strings.Trim(path, "/")
	if path == "" {
		switch r.Method {
		case http.MethodGet:
			limit, err := qualificationPageLimit(r.URL.Query().Get("limit"))
			if err != nil {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
			page, err := s.readiness.List(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), r.URL.Query().Get("deployment_id"), r.URL.Query().Get("release_id"), limit, r.URL.Query().Get("cursor"))
			if err != nil {
				writeQualificationError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, page)
			return
		case http.MethodPost:
			var input contracts.ReadinessSnapshotRequest
			if err := decodeQualificationJSON(r, &input); err != nil {
				writeErr(w, http.StatusBadRequest, "invalid readiness snapshot request")
				return
			}
			input.WorkspaceID = requestWorkspace(r, input.WorkspaceID)
			input.Actor = qualificationAuditActor(r)
			if input.IdempotencyKey == "" {
				input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
			}
			if input.IdempotencyKey == "" {
				input.IdempotencyKey = requestIDFromRequest(r)
			}
			snapshot, created, err := s.readiness.Capture(r.Context(), input, nowUTC())
			if err != nil {
				writeQualificationError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"snapshot": snapshot, "created": created})
			return
		default:
			writeErr(w, http.StatusMethodNotAllowed, "GET or POST only")
			return
		}
	}
	parts := strings.Split(path, "/")
	if len(parts) == 1 && r.Method == http.MethodGet {
		snapshot, err := s.readiness.Get(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), r.URL.Query().Get("deployment_id"), parts[0])
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, snapshot)
		return
	}
	if len(parts) == 2 && parts[1] == "annotations" {
		workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
		deploymentID := r.URL.Query().Get("deployment_id")
		releaseID := r.URL.Query().Get("release_id")
		if r.Method == http.MethodGet {
			limit, err := qualificationPageLimit(r.URL.Query().Get("limit"))
			if err != nil {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
			page, err := s.readiness.ListAnnotations(r.Context(), workspaceID, deploymentID, releaseID, parts[0], limit, r.URL.Query().Get("cursor"))
			if err != nil {
				writeQualificationError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, page)
			return
		}
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "GET or POST only")
			return
		}
		var input contracts.IncidentAnnotationRequest
		if err := decodeQualificationJSON(r, &input); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid incident annotation request")
			return
		}
		input.WorkspaceID, input.DeploymentID, input.ReleaseID, input.SnapshotID = workspaceID, deploymentID, releaseID, parts[0]
		input.Actor = qualificationAuditActor(r)
		if input.IdempotencyKey == "" {
			input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		}
		if input.IdempotencyKey == "" {
			input.IdempotencyKey = requestIDFromRequest(r)
		}
		annotation, created, err := s.readiness.Annotate(r.Context(), input, nowUTC())
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"annotation": annotation, "created": created})
		return
	}
	writeErr(w, http.StatusNotFound, "readiness snapshot or annotations path not found")
}
