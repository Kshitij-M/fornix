package server

import (
	"net/http"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

type qualificationScheduleClaimRequest struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	OwnerID     string `json:"owner_id"`
	LeaseTTLMS  int64  `json:"lease_ttl_ms,omitempty"`
}

type qualificationScheduleLeaseRequest struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	OwnerID     string `json:"owner_id"`
	Fence       uint64 `json:"fence"`
	LeaseTTLMS  int64  `json:"lease_ttl_ms,omitempty"`
}

type qualificationScheduleStateRequest struct {
	WorkspaceID string `json:"workspace_id,omitempty"`
	OwnerID     string `json:"owner_id,omitempty"`
	Fence       uint64 `json:"fence,omitempty"`
	Reason      string `json:"reason,omitempty"`
}

func (s *server) handleQualificationRefreshSchedules(w http.ResponseWriter, r *http.Request) {
	if s.deploymentEvidence == nil {
		writeErr(w, http.StatusServiceUnavailable, "deployment qualification is unavailable")
		return
	}
	path := strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/qualification/refresh-schedules"), "/")
	parts := []string{}
	if path != "" {
		parts = strings.Split(path, "/")
	}
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	if len(parts) == 0 {
		switch r.Method {
		case http.MethodGet:
			limit, err := qualificationPageLimit(r.URL.Query().Get("limit"))
			if err != nil {
				writeErr(w, http.StatusBadRequest, err.Error())
				return
			}
			page, err := s.deploymentEvidence.ListQualificationRefreshSchedules(r.Context(), workspaceID, r.URL.Query().Get("deployment_id"), r.URL.Query().Get("release_id"), limit, r.URL.Query().Get("cursor"))
			if err != nil {
				writeQualificationError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, page)
		case http.MethodPost:
			var input contracts.QualificationRefreshScheduleRequest
			if err := decodeQualificationJSON(r, &input); err != nil {
				writeErr(w, http.StatusBadRequest, "invalid qualification refresh schedule request")
				return
			}
			input.WorkspaceID, input.Actor = workspaceID, qualificationAuditActor(r)
			if input.IdempotencyKey == "" {
				input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
			}
			if input.IdempotencyKey == "" {
				input.IdempotencyKey = requestIDFromRequest(r)
			}
			schedule, created, err := s.deploymentEvidence.RegisterQualificationRefreshSchedule(r.Context(), input, nowUTC())
			if err != nil {
				writeQualificationError(w, err)
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"schedule": schedule, "created": created})
		default:
			writeErr(w, http.StatusMethodNotAllowed, "GET or POST only")
		}
		return
	}
	if len(parts) == 1 && parts[0] == "claim" && r.Method == http.MethodPost {
		var input qualificationScheduleClaimRequest
		if err := decodeQualificationJSON(r, &input); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid qualification schedule claim request")
			return
		}
		input.WorkspaceID = workspaceID
		claim, found, err := s.deploymentEvidence.ClaimQualificationRefreshSchedule(r.Context(), input.WorkspaceID, input.OwnerID, time.Duration(input.LeaseTTLMS)*time.Millisecond, nowUTC())
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"found": found, "claim": claim})
		return
	}
	if len(parts) == 1 && parts[0] == "plan" && r.Method == http.MethodGet {
		limit, err := qualificationPageLimit(r.URL.Query().Get("limit"))
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		page, err := s.deploymentEvidence.ListQualificationRefreshSchedulePlan(r.Context(), workspaceID, r.URL.Query().Get("deployment_id"), r.URL.Query().Get("release_id"), limit, r.URL.Query().Get("cursor"), nowUTC())
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, page)
		return
	}
	if len(parts) == 1 || (len(parts) == 2 && parts[1] == "") {
		if r.Method != http.MethodGet {
			writeErr(w, http.StatusMethodNotAllowed, "GET only")
			return
		}
		schedule, err := s.deploymentEvidence.GetQualificationRefreshSchedule(r.Context(), workspaceID, parts[0])
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, schedule)
		return
	}
	if len(parts) == 2 && parts[1] == "attempts" && r.Method == http.MethodGet {
		limit, err := qualificationPageLimit(r.URL.Query().Get("limit"))
		if err != nil {
			writeErr(w, http.StatusBadRequest, err.Error())
			return
		}
		page, err := s.deploymentEvidence.ListQualificationRefreshScheduleAttempts(r.Context(), workspaceID, parts[0], limit, r.URL.Query().Get("cursor"))
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, page)
		return
	}
	if len(parts) != 2 || r.Method != http.MethodPost {
		writeErr(w, http.StatusNotFound, "unknown qualification refresh schedule operation")
		return
	}
	scheduleID := parts[0]
	switch parts[1] {
	case "renew":
		var input qualificationScheduleLeaseRequest
		if err := decodeQualificationJSON(r, &input); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid qualification schedule renewal request")
			return
		}
		schedule, err := s.deploymentEvidence.RenewQualificationRefreshSchedule(r.Context(), workspaceID, scheduleID, input.OwnerID, input.Fence, time.Duration(input.LeaseTTLMS)*time.Millisecond, nowUTC())
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, schedule)
	case "release":
		var input qualificationScheduleLeaseRequest
		if err := decodeQualificationJSON(r, &input); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid qualification schedule release request")
			return
		}
		if err := s.deploymentEvidence.ReleaseQualificationRefreshSchedule(r.Context(), workspaceID, scheduleID, input.OwnerID, input.Fence, nowUTC()); err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"released": true, "schedule_id": scheduleID})
	case "complete":
		var input contracts.QualificationRefreshScheduleCompletionRequest
		if err := decodeQualificationJSON(r, &input); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid qualification schedule completion request")
			return
		}
		input.WorkspaceID, input.ScheduleID, input.Actor = workspaceID, scheduleID, qualificationAuditActor(r)
		if input.IdempotencyKey == "" {
			input.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
		}
		if input.IdempotencyKey == "" {
			input.IdempotencyKey = requestIDFromRequest(r)
		}
		schedule, attempt, deduplicated, err := s.deploymentEvidence.CompleteQualificationRefreshSchedule(r.Context(), input, nowUTC())
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"schedule": schedule, "attempt": attempt, "deduplicated": deduplicated})
	case "pause", "resume", "cancel":
		var input qualificationScheduleStateRequest
		if err := decodeQualificationJSON(r, &input); err != nil {
			writeErr(w, http.StatusBadRequest, "invalid qualification schedule state request")
			return
		}
		schedule, err := s.deploymentEvidence.SetQualificationRefreshScheduleState(r.Context(), workspaceID, scheduleID, parts1Map(parts[1]), input.OwnerID, input.Fence, qualificationAuditActor(r), input.Reason, nowUTC())
		if err != nil {
			writeQualificationError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, schedule)
	default:
		writeErr(w, http.StatusNotFound, "unknown qualification refresh schedule operation")
	}
}

func parts1Map(value string) string {
	switch value {
	case "pause":
		return contracts.QualificationRefreshSchedulePaused
	case "resume":
		return contracts.QualificationRefreshScheduleActive
	default:
		return contracts.QualificationRefreshScheduleCancelled
	}
}
