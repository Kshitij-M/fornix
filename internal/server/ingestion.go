package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/ingest"
)

func (s *server) ingestRequest(r *http.Request) (contracts.IngestJobRequest, contracts.Workspace, error) {
	var request contracts.IngestJobRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		return request, contracts.Workspace{}, errors.New("invalid ingest request")
	}
	workspaceID := requestWorkspace(r, request.WorkspaceID)
	workspace, err := s.operator.GetWorkspace(r.Context(), workspaceID)
	if err != nil {
		return request, contracts.Workspace{}, err
	}
	if strings.TrimSpace(workspace.ToolRoot) == "" {
		return request, contracts.Workspace{}, errors.New("workspace has no configured tool_root mount")
	}
	request.WorkspaceID = workspaceID
	request.Source.MountRoot = workspace.ToolRoot
	request.Actor = requestActor(r)
	if request.IdempotencyKey == "" {
		request.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	if request.RequestID == "" {
		request.RequestID = requestIDFromRequest(r)
	}
	return request, workspace, nil
}

func (s *server) handleIngestDryRun(w http.ResponseWriter, r *http.Request) {
	request, _, err := s.ingestRequest(r)
	if err != nil {
		writeOperatorError(w, err)
		return
	}
	normalized, err := request.Normalize()
	if err != nil {
		writeOperatorError(w, err)
		return
	}
	result, err := ingest.Discover(r.Context(), normalized.Source)
	if err != nil {
		writeOperatorError(w, err)
		return
	}
	files := make([]contracts.IngestFile, len(result.Files))
	for i := range result.Files {
		files[i] = result.Files[i].File
	}
	report := contracts.IngestReport{SchemaVersion: contracts.IngestSchemaVersion, WorkspaceID: normalized.WorkspaceID, Repository: normalized.Source.Repository, SourceRoot: normalized.Source.SourceRoot, ManifestHash: result.ManifestHash, Status: contracts.IngestQueued, DryRun: true, FileCount: len(result.Files), SkippedFiles: len(result.Skipped), SourceBytes: result.TotalBytes, DiscoveryMillis: result.Duration.Milliseconds()}
	report.ReportHash = report.StableHash()
	writeJSON(w, http.StatusOK, map[string]any{"report": report, "skipped": result.Skipped, "files": files})
}

func (s *server) handleIngestJobs(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/v1/operator/ingest/jobs" {
		writeErr(w, http.StatusNotFound, "unknown ingest path")
		return
	}
	if r.Method == http.MethodGet {
		page, err := s.ingests.List(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), r.URL.Query().Get("cursor"), atoi(r.URL.Query().Get("limit")))
		if err != nil {
			writeOperatorError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, page)
		return
	}
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "GET or POST only")
		return
	}
	request, _, err := s.ingestRequest(r)
	if err != nil {
		writeOperatorError(w, err)
		return
	}
	normalized, err := request.Normalize()
	if err != nil {
		writeOperatorError(w, err)
		return
	}
	discovered, err := ingest.Discover(r.Context(), normalized.Source)
	if err != nil {
		writeOperatorError(w, err)
		return
	}
	job, created, err := s.ingests.Submit(r.Context(), normalized, discovered)
	if err != nil {
		writeOperatorError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"job": job, "created": created, "deduped": !created})
}

func (s *server) handleIngestJob(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/operator/ingest/jobs/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeErr(w, http.StatusNotFound, "ingest job id required")
		return
	}
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	jobID := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		job, _, err := s.ingests.Get(r.Context(), workspaceID, jobID)
		if err != nil {
			writeOperatorError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, job)
		return
	}
	if len(parts) != 2 || r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "GET or POST /resume or /cancel only")
		return
	}
	switch parts[1] {
	case "resume":
		var request contracts.IngestBatchRequest
		if r.Body != nil && r.ContentLength != 0 {
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil && !errors.Is(err, http.ErrBodyReadAfterClose) {
				writeErr(w, http.StatusBadRequest, "invalid resume request")
				return
			}
		}
		request.WorkspaceID, request.JobID, request.Actor = workspaceID, jobID, requestActor(r)
		result, err := s.ingests.ProcessBatch(r.Context(), request)
		if err != nil {
			writeOperatorError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	case "cancel":
		job, err := s.ingests.Cancel(r.Context(), workspaceID, jobID, requestActor(r))
		if err != nil {
			writeOperatorError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"job": job})
	default:
		writeErr(w, http.StatusNotFound, "unknown ingest job operation")
	}
}

func atoi(value string) int { result, _ := strconv.Atoi(strings.TrimSpace(value)); return result }
