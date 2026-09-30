package server

import (
	"net/http"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

// handleDeploymentEvidence exposes only the release/evidence index and its
// read-only gate. It does not execute deployments, migrations, backups,
// provider calls, or external effects.
func (s *server) handleDeploymentEvidence(w http.ResponseWriter, r *http.Request) {
	if s.deploymentEvidence == nil {
		writeErr(w, http.StatusServiceUnavailable, "deployment evidence is unavailable")
		return
	}
	path := strings.TrimPrefix(r.URL.Path, "/v1/qualification/releases")
	path = strings.Trim(path, "/")
	if path == "" {
		if r.Method == http.MethodGet {
			limit, err := qualificationPageLimit(r.URL.Query().Get("limit"))
			if err != nil {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
			page, err := s.deploymentEvidence.ListReleases(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), r.URL.Query().Get("deployment_id"), limit, r.URL.Query().Get("cursor"))
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
		var input contracts.DeploymentReleaseRequest
		if err := decodeQualificationJSON(r, &input); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid deployment release request")
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
		release, created, err := s.deploymentEvidence.RegisterRelease(r.Context(), input, nowUTC())
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"release": release, "created": created})
		return
	}

	parts := strings.Split(path, "/")
	if (len(parts) == 2 && parts[1] == "refresh" || len(parts) == 3 && parts[1] == "refresh" && parts[2] == "plan") && r.Method == http.MethodPost {
		var input contracts.QualificationRefreshRequest
		if err := decodeQualificationJSON(r, &input); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid qualification refresh request")
			return
		}
		input.WorkspaceID = requestWorkspace(r, input.WorkspaceID)
		input.DeploymentID = strings.TrimSpace(input.DeploymentID)
		input.ReleaseID = parts[0]
		input.Actor = qualificationAuditActor(r)
		if input.IdempotencyKey == "" {
			input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		}
		if input.IdempotencyKey == "" {
			input.IdempotencyKey = requestIDFromRequest(r)
		}
		if len(parts) == 3 {
			input.DryRun = true
		}
		result, err := s.deploymentEvidence.RefreshQualificationEvidence(r.Context(), input, nowUTC())
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
		return
	}
	if len(parts) == 2 && parts[1] == "refreshes" && r.Method == http.MethodGet {
		limit, err := qualificationPageLimit(r.URL.Query().Get("limit"))
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		page, err := s.deploymentEvidence.ListQualificationRefreshes(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), r.URL.Query().Get("deployment_id"), parts[0], limit, r.URL.Query().Get("cursor"))
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, page)
		return
	}
	if len(parts) == 3 && parts[1] == "refreshes" && r.Method == http.MethodGet {
		report, err := s.deploymentEvidence.GetQualificationRefresh(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), r.URL.Query().Get("deployment_id"), parts[2])
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, report)
		return
	}
	if len(parts) == 1 && r.Method == http.MethodGet {
		release, err := s.deploymentEvidence.GetRelease(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), r.URL.Query().Get("deployment_id"), parts[0])
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, release)
		return
	}
	if len(parts) == 2 && parts[1] == "evidence" {
		workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
		deploymentID := r.URL.Query().Get("deployment_id")
		if r.Method == http.MethodGet {
			limit, err := qualificationPageLimit(r.URL.Query().Get("limit"))
			if err != nil {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
			page, err := s.deploymentEvidence.ListEvidence(r.Context(), workspaceID, deploymentID, parts[0], limit, r.URL.Query().Get("cursor"))
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
		var input contracts.DeploymentEvidenceLinkRequest
		if err := decodeQualificationJSON(r, &input); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid deployment evidence link request")
			return
		}
		input.WorkspaceID = workspaceID
		input.DeploymentID = deploymentID
		input.ReleaseID = parts[0]
		input.Actor = qualificationAuditActor(r)
		if input.IdempotencyKey == "" {
			input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		}
		if input.IdempotencyKey == "" {
			input.IdempotencyKey = requestIDFromRequest(r)
		}
		link, created, err := s.deploymentEvidence.LinkEvidence(r.Context(), input, nowUTC())
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"evidence": link, "created": created})
		return
	}
	if len(parts) == 4 && parts[1] == "evidence" && parts[3] == "revoke" && r.Method == http.MethodPost {
		workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
		var input contracts.DeploymentEvidenceRevocationRequest
		if err := decodeQualificationJSON(r, &input); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid deployment evidence revocation request")
			return
		}
		input.WorkspaceID, input.DeploymentID, input.ReleaseID, input.LinkID = workspaceID, r.URL.Query().Get("deployment_id"), parts[0], parts[2]
		input.Kind = r.URL.Query().Get("kind")
		input.Actor = qualificationAuditActor(r)
		if input.IdempotencyKey == "" {
			input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		}
		if input.IdempotencyKey == "" {
			input.IdempotencyKey = requestIDFromRequest(r)
		}
		link, changed, err := s.deploymentEvidence.RevokeEvidence(r.Context(), input, nowUTC())
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"evidence": link, "revoked": changed})
		return
	}
	if len(parts) == 2 && parts[1] == "gate" && r.Method == http.MethodGet {
		gate, err := s.deploymentEvidence.EvaluateGate(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), r.URL.Query().Get("deployment_id"), parts[0], deploymentEvidenceKindsQuery(r.URL.Query().Get("required_kinds")), nowUTC())
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, gate)
		return
	}
	if len(parts) == 2 && parts[1] == "verification" {
		workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
		deploymentID := r.URL.Query().Get("deployment_id")
		artifactKind := r.URL.Query().Get("artifact_kind")
		if r.Method == http.MethodGet {
			verification, err := s.deploymentEvidence.GetVerification(r.Context(), workspaceID, deploymentID, parts[0], artifactKind)
			if err != nil {
				writeQualificationError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, verification)
			return
		}
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "GET or POST only")
			return
		}
		var input contracts.DeploymentReleaseVerificationRequest
		if err := decodeQualificationJSON(r, &input); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid release verification request")
			return
		}
		input.WorkspaceID, input.DeploymentID, input.ReleaseID = workspaceID, deploymentID, parts[0]
		input.Actor = qualificationAuditActor(r)
		if input.IdempotencyKey == "" {
			input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		}
		if input.IdempotencyKey == "" {
			input.IdempotencyKey = requestIDFromRequest(r)
		}
		verification, created, err := s.deploymentEvidence.RegisterVerification(r.Context(), input, nowUTC())
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"verification": verification, "created": created})
		return
	}
	if len(parts) == 3 && parts[1] == "verification" && parts[2] == "revoke" && r.Method == http.MethodPost {
		workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
		if err := s.deploymentEvidence.RevokeVerification(r.Context(), workspaceID, r.URL.Query().Get("deployment_id"), parts[0], r.URL.Query().Get("artifact_kind"), qualificationAuditActor(r), nowUTC()); err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"release_id": parts[0], "revoked": true})
		return
	}
	if len(parts) == 2 && parts[1] == "admission" && r.Method == http.MethodGet {
		decision, err := s.deploymentEvidence.EvaluateAdmission(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), r.URL.Query().Get("deployment_id"), parts[0], r.URL.Query().Get("artifact_kind"), r.URL.Query().Get("artifact_hash"), nowUTC())
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, decision)
		return
	}
	writeErr(w, http.StatusNotFound, "unknown deployment qualification operation")
}

func deploymentEvidenceKindsQuery(raw string) []string {
	if strings.TrimSpace(raw) == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			result = append(result, value)
		}
	}
	return result
}

func nowUTC() (now time.Time) {
	return time.Now().UTC()
}
