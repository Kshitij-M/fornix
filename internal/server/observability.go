package server

import (
	"context"
	"net/http"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/tool"
)

func (s *server) observeModelRequest(ctx context.Context, request contracts.ModelRequest) {
	if s == nil || s.observability == nil || s.modelCalls == nil || strings.TrimSpace(request.IdempotencyKey) == "" {
		return
	}
	call, err := s.modelCalls.Get(ctx, request.WorkspaceID, request.IdempotencyKey)
	if err == nil {
		_ = s.observability.ObserveModelCall(ctx, call)
	}
}

func (s *server) observeToolOutcome(ctx context.Context, outcome tool.Outcome) {
	if s == nil || s.observability == nil || outcome.Run.ID == "" {
		return
	}
	_ = s.observability.ObserveToolRun(ctx, outcome.Run)
}

func (s *server) observeAgentOutcome(ctx context.Context, run contracts.AgentRun) {
	if s == nil || s.observability == nil || run.ID == "" {
		return
	}
	_ = s.observability.ObserveAgentRun(ctx, run)
}

func (s *server) handleObservabilityMetrics(w http.ResponseWriter, r *http.Request) {
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	until := time.Now().UTC()
	if raw := strings.TrimSpace(r.URL.Query().Get("until")); raw != "" {
		if parsed, err := time.Parse(time.RFC3339Nano, raw); err == nil {
			until = parsed.UTC()
		} else {
			writeErr(w, http.StatusBadRequest, "invalid until")
			return
		}
	}
	since := until.Add(-24 * time.Hour)
	if raw := strings.TrimSpace(r.URL.Query().Get("since")); raw != "" {
		parsed, err := time.Parse(time.RFC3339Nano, raw)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid since")
			return
		}
		since = parsed.UTC()
	}
	result, err := s.observability.Snapshot(r.Context(), workspaceID, since, until)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}
