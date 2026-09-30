package server

import (
	"net/http"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
)

// handleQualificationRetention exposes policy, metadata-sync, planning, and
// recovery operations. Planning and recovery are read-only even though their
// request shape is POST so callers can supply an explicit as_of timestamp.
func (s *server) handleQualificationRetention(w http.ResponseWriter, r *http.Request) {
	if s.readiness == nil {
		writeErr(w, http.StatusServiceUnavailable, "readiness store is unavailable")
		return
	}
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/qualification/readiness/retention"), "/")
	switch path {
	case "policies", "policies/":
		s.handleQualificationRetentionPolicies(w, r)
	case "policies/current":
		s.handleQualificationRetentionCurrentPolicy(w, r)
	case "metadata/sync":
		s.handleQualificationRetentionMetadataSync(w, r)
	case "plan":
		s.handleQualificationRetentionPlan(w, r)
	case "recovery":
		s.handleQualificationRetentionRecovery(w, r)
	default:
		writeErr(w, http.StatusNotFound, "qualification retention path not found")
	}
}

func (s *server) handleQualificationRetentionPolicies(w http.ResponseWriter, r *http.Request) {
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	deploymentID := r.URL.Query().Get("deployment_id")
	switch r.Method {
	case http.MethodGet:
		limit, err := qualificationPageLimit(r.URL.Query().Get("limit"))
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		page, err := s.readiness.ListQualificationRetentionPolicies(r.Context(), workspaceID, deploymentID, limit, r.URL.Query().Get("cursor"))
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, page)
	case http.MethodPost:
		var input contracts.QualificationRetentionPolicyRequest
		if err := decodeQualificationJSON(r, &input); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid qualification retention policy request")
			return
		}
		input.WorkspaceID, input.Actor = workspaceID, qualificationAuditActor(r)
		if input.IdempotencyKey == "" {
			input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		}
		if input.IdempotencyKey == "" {
			input.IdempotencyKey = requestIDFromRequest(r)
		}
		policy, created, err := s.readiness.PublishQualificationRetentionPolicy(r.Context(), input, nowUTC())
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"policy": policy, "created": created})
	default:
		writeErr(w, http.StatusMethodNotAllowed, "GET or POST only")
	}
}

func (s *server) handleQualificationRetentionCurrentPolicy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	policy, err := s.readiness.CurrentQualificationRetentionPolicy(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), r.URL.Query().Get("deployment_id"))
	if err != nil {
		writeQualificationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, policy)
}

func (s *server) handleQualificationRetentionMetadataSync(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	var input contracts.QualificationRetentionSyncRequest
	if err := decodeQualificationJSON(r, &input); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid qualification retention metadata sync request")
		return
	}
	input.WorkspaceID, input.Actor = requestWorkspace(r, input.WorkspaceID), qualificationAuditActor(r)
	if input.IdempotencyKey == "" && !input.DryRun {
		input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		if input.IdempotencyKey == "" {
			input.IdempotencyKey = requestIDFromRequest(r)
		}
	}
	result, err := s.readiness.SyncQualificationRetentionMetadata(r.Context(), input, nowUTC())
	if err != nil {
		writeQualificationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleQualificationRetentionPlan(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	var input contracts.QualificationRetentionPlanRequest
	if err := decodeQualificationJSON(r, &input); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid qualification retention plan request")
		return
	}
	input.WorkspaceID, input.Actor = requestWorkspace(r, input.WorkspaceID), qualificationAuditActor(r)
	plan, err := s.readiness.PlanQualificationRetention(r.Context(), input, nowUTC())
	if err != nil {
		writeQualificationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, plan)
}

func (s *server) handleQualificationRetentionRecovery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	var input contracts.QualificationRecoveryReportRequest
	if err := decodeQualificationJSON(r, &input); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid qualification recovery report request")
		return
	}
	input.WorkspaceID, input.Actor = requestWorkspace(r, input.WorkspaceID), qualificationAuditActor(r)
	report, err := s.readiness.QualificationRecoveryReport(r.Context(), input, nowUTC())
	if err != nil {
		writeQualificationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, report)
}
