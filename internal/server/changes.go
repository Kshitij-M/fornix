package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/omaveda/fornix/internal/change"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func (s *server) handleChangeDryRun(w http.ResponseWriter, r *http.Request) {
	var request contracts.ChangeProposalRequest
	if err := decodeChangeJSON(w, r, &request); err != nil {
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	request.Actor = requestActor(r)
	root, err := s.changeRoot(r.Context(), request.WorkspaceID, request.Source.SourceRoot)
	if err != nil {
		writeChangeError(w, err)
		return
	}
	request.Source.SourceRoot = root
	planned, err := s.changes.DryRun(r.Context(), change.PlanInput{Request: request, Root: root})
	if err != nil {
		writeChangeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"packet": planned.Packet, "packet_hash": planned.Packet.StableHash(), "expected_tree_hash": planned.ExpectedTreeHash, "diff": json.RawMessage(planned.Diff), "dry_run": true})
}

func (s *server) handleChangePropose(w http.ResponseWriter, r *http.Request) {
	var request contracts.ChangeProposalRequest
	if err := decodeChangeJSON(w, r, &request); err != nil {
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	request.Actor = requestActor(r)
	if request.IdempotencyKey == "" {
		request.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	if request.RequestID == "" {
		request.RequestID = requestIDFromRequest(r)
	}
	root, err := s.changeRoot(r.Context(), request.WorkspaceID, request.Source.SourceRoot)
	if err != nil {
		writeChangeError(w, err)
		return
	}
	request.Source.SourceRoot = root
	proposal, duplicate, planned, err := s.changes.Propose(r.Context(), change.PlanInput{Request: request, Root: root})
	if err != nil {
		writeChangeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"proposal": proposal, "packet_hash": planned.Packet.StableHash(), "expected_tree_hash": planned.ExpectedTreeHash, "duplicate": duplicate})
}

func (s *server) handleChangeGet(w http.ResponseWriter, r *http.Request, proposalID string) {
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	proposal, err := s.changes.Get(r.Context(), workspaceID, proposalID)
	if err != nil {
		writeChangeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"proposal": proposal})
}

func (s *server) handleChangeApprove(w http.ResponseWriter, r *http.Request, proposalID string) {
	var request contracts.ChangeApprovalRequest
	if err := decodeChangeJSON(w, r, &request); err != nil {
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	request.ProposalID = proposalID
	request.Actor = requestActor(r)
	if request.IdempotencyKey == "" {
		request.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	if request.ID == "" {
		request.ID = requestIDFromRequest(r)
	}
	approval, proposal, duplicate, err := s.changes.Approve(r.Context(), request)
	if err != nil {
		writeChangeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"approval": approval, "proposal": proposal, "duplicate": duplicate})
}

func (s *server) handleChangeApply(w http.ResponseWriter, r *http.Request, proposalID string) {
	var request contracts.ChangeApplicationRequest
	if err := decodeChangeJSON(w, r, &request); err != nil {
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	request.ProposalID = proposalID
	request.Actor = requestActor(r)
	if request.IdempotencyKey == "" {
		request.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	if request.ID == "" {
		request.ID = requestIDFromRequest(r)
	}
	proposal, err := s.changes.Get(r.Context(), request.WorkspaceID, proposalID)
	if err != nil {
		writeChangeError(w, err)
		return
	}
	root, err := s.changeRoot(r.Context(), request.WorkspaceID, proposal.Source.SourceRoot)
	if err != nil {
		writeChangeError(w, err)
		return
	}
	application, resultProposal, err := s.changes.Apply(r.Context(), request, root)
	if err != nil {
		if application.ID != "" {
			writeJSON(w, http.StatusConflict, map[string]any{"application": application, "proposal": resultProposal, "error": err.Error()})
			return
		}
		writeChangeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"application": application, "proposal": resultProposal})
}

func (s *server) handleChangeDisclosure(w http.ResponseWriter, r *http.Request) {
	var request contracts.ChangeDisclosureRequest
	if err := decodeChangeJSON(w, r, &request); err != nil {
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	result, err := s.changes.Disclose(r.Context(), request)
	if err != nil {
		writeChangeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) changeRoot(ctx context.Context, workspaceID, requested string) (string, error) {
	workspace, err := s.operator.GetWorkspace(ctx, workspaceID)
	if err != nil {
		return "", err
	}
	configured := filepath.Clean(strings.TrimSpace(workspace.ToolRoot))
	if configured == "." || configured == "" || !filepath.IsAbs(configured) {
		return "", fmt.Errorf("workspace has no configured repository mount")
	}
	root := configured
	if strings.TrimSpace(requested) != "" {
		root = filepath.Clean(strings.TrimSpace(requested))
	}
	relative, err := filepath.Rel(configured, root)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return "", fmt.Errorf("repository root is outside workspace mount")
	}
	return root, nil
}

func decodeChangeJSON(w http.ResponseWriter, r *http.Request, target any) error {
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, contracts.MaxChangeReportBytes))
	if err := decoder.Decode(target); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid change request: "+err.Error())
		return err
	}
	return nil
}

func writeChangeError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, store.ErrChangeNotFound):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrChangeStaleFence), errors.Is(err, store.ErrChangeConflict), errors.Is(err, store.ErrChangeInProgress), errors.Is(err, store.ErrChangePacketMismatch), errors.Is(err, store.ErrIdempotencyConflict):
		status = http.StatusConflict
	case errors.Is(err, change.ErrUnsafePath):
		status = http.StatusForbidden
	case errors.Is(err, store.ErrWorkspaceViolation):
		status = http.StatusForbidden
	}
	writeErr(w, status, err.Error())
}
