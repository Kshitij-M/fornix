package server

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

// handleValidationCreate admits and executes one bounded validation request.
// The live checks are read-only; all durable state is written by the
// validation service and its Postgres transaction boundary.
func (s *server) handleValidationCreate(w http.ResponseWriter, r *http.Request) {
	var request contracts.ValidationRequest
	if err := decodeValidationJSON(w, r, &request); err != nil {
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	request.Actor = requestActor(r)
	if request.IdempotencyKey == "" {
		request.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	if request.IdempotencyKey == "" {
		request.IdempotencyKey = requestIDFromRequest(r)
	}
	if request.RequestID == "" {
		request.RequestID = requestIDFromRequest(r)
	}
	root, err := s.changeRoot(r.Context(), request.WorkspaceID, request.Source.SourceRoot)
	if err != nil {
		writeValidationError(w, err)
		return
	}
	workspace, err := s.operator.GetWorkspace(r.Context(), request.WorkspaceID)
	if err != nil {
		writeValidationError(w, err)
		return
	}
	result, err := s.validation.Validate(r.Context(), request, root, workspace.ToolRoot)
	if err != nil {
		writeValidationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleValidationGet(w http.ResponseWriter, r *http.Request, runID string) {
	run, err := s.validations.Get(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), runID)
	if err != nil {
		writeValidationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *server) handleValidationResults(w http.ResponseWriter, r *http.Request, runID string) {
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	limit, offset := atoi(r.URL.Query().Get("limit")), atoi(r.URL.Query().Get("offset"))
	results, err := s.validations.ListResults(r.Context(), workspaceID, runID, limit, offset)
	if err != nil {
		writeValidationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results, "limit": limit, "offset": offset})
}

func (s *server) handleValidationDisclosure(w http.ResponseWriter, r *http.Request) {
	var request contracts.ValidationDisclosureRequest
	if err := decodeValidationJSON(w, r, &request); err != nil {
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	result, err := s.validations.Disclose(r.Context(), request)
	if err != nil {
		writeValidationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleValidationReplay(w http.ResponseWriter, r *http.Request, runID string) {
	request := contracts.ValidationReplayRequest{WorkspaceID: requestWorkspace(r, r.URL.Query().Get("workspace_id")), ValidationRunID: runID, FromSequence: uint64(atoi(r.URL.Query().Get("from_sequence"))), Limit: atoi(r.URL.Query().Get("limit"))}
	result, err := s.validations.Replay(r.Context(), request)
	if err != nil {
		writeValidationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleValidationResume(w http.ResponseWriter, r *http.Request, runID string) {
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	run, err := s.validations.Get(r.Context(), workspaceID, runID)
	if err != nil {
		writeValidationError(w, err)
		return
	}
	workspace, err := s.operator.GetWorkspace(r.Context(), workspaceID)
	if err != nil {
		writeValidationError(w, err)
		return
	}
	root, err := s.changeRoot(r.Context(), workspaceID, run.SourceRoot)
	if err != nil {
		writeValidationError(w, err)
		return
	}
	result, err := s.validation.Resume(r.Context(), workspaceID, runID, root, workspace.ToolRoot)
	if err != nil {
		writeValidationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleValidationCancel(w http.ResponseWriter, r *http.Request, runID string) {
	run, err := s.validations.Cancel(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), runID, requestActor(r))
	if err != nil {
		writeValidationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run": run})
}

func (s *server) handleHandoffGet(w http.ResponseWriter, r *http.Request, handoffID string) {
	handoff, err := s.validations.GetHandoff(r.Context(), requestWorkspace(r, r.URL.Query().Get("workspace_id")), handoffID)
	if err != nil {
		writeValidationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, handoff)
}

func (s *server) handleHandoffSubmit(w http.ResponseWriter, r *http.Request, handoffID string) {
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	handoff, err := s.validations.GetHandoff(r.Context(), workspaceID, handoffID)
	if err != nil {
		writeValidationError(w, err)
		return
	}
	if handoff.Status == contracts.ReindexHandoffSubmitted {
		writeJSON(w, http.StatusOK, handoff)
		return
	}
	if s.validation == nil || s.validation.SubmitHandoff == nil {
		writeValidationError(w, errors.New("re-index handoff submission is not configured"))
		return
	}
	job, submitErr := s.validation.SubmitHandoff(r.Context(), handoff)
	if submitErr != nil {
		failed, failErr := s.validations.MarkHandoffFailed(r.Context(), workspaceID, handoffID, contracts.ValidationFailure{Code: contracts.ValidationFailureRecovery, Message: "re-index handoff submission failed"})
		if failErr != nil {
			writeValidationError(w, failErr)
			return
		}
		// The durable handoff retains only the bounded failure class. The
		// underlying submission error may contain database or path details and
		// must not cross the authenticated API boundary.
		writeJSON(w, http.StatusConflict, map[string]any{"handoff": failed, "error": "re-index handoff submission failed"})
		return
	}
	submitted, err := s.validations.MarkHandoffSubmitted(r.Context(), handoffID, workspaceID, job)
	if err != nil {
		writeValidationError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, submitted)
}

func decodeValidationJSON(w http.ResponseWriter, r *http.Request, target any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, contracts.MaxValidationReportBytes))
	if err := decoder.Decode(target); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid validation request: "+err.Error())
		return err
	}
	return nil
}

func writeValidationError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, store.ErrValidationNotFound), errors.Is(err, store.ErrHandoffNotFound):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrValidationConflict), errors.Is(err, store.ErrValidationStaleFence), errors.Is(err, store.ErrValidationTerminal), errors.Is(err, store.ErrValidationResultConflict), errors.Is(err, store.ErrHandoffConflict), errors.Is(err, store.ErrIdempotencyConflict):
		status = http.StatusConflict
	case errors.Is(err, store.ErrValidationAuthority), errors.Is(err, store.ErrValidationEvidence), errors.Is(err, store.ErrWorkspaceViolation):
		status = http.StatusForbidden
	case errors.Is(err, store.ErrChangeNotFound):
		status = http.StatusNotFound
	}
	writeErr(w, status, err.Error())
}
