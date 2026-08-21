package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func (s *server) handleEvidencePut(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var input contracts.SourceRecordInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	input.WorkspaceID = requestWorkspace(r, input.WorkspaceID)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	result, err := s.evidence.Put(ctx, store.EvidencePutInput{
		WorkspaceID: input.WorkspaceID, SourceReference: input.SourceReference,
		DeduplicationKey: input.DeduplicationKey, Kind: input.Kind,
		MediaType: input.MediaType, Gist: input.Gist, Detail: input.Detail,
		RawPayload: append([]byte(nil), input.RawPayload...), SupersedesID: input.SupersedesID,
		Contradicts: input.Contradicts, Actor: requestActor(r), CausationID: requestIDFromRequest(r), CorrelationID: requestIDFromRequest(r),
	})
	if err != nil {
		writeEvidenceErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleEvidenceEdge(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var input contracts.ProvenanceEdgeInput
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	input.WorkspaceID = requestWorkspace(r, input.WorkspaceID)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	result, err := s.evidence.AddEdge(ctx, input)
	if err != nil {
		writeEvidenceErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleEvidenceDisclosure(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var request contracts.DisclosureRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	result, err := s.evidence.Disclose(ctx, request)
	if err != nil {
		writeEvidenceErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleEvidenceProvenance(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var request contracts.ProvenanceTraversalRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	edges, err := s.evidence.Traverse(ctx, request)
	if err != nil {
		writeEvidenceErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"edges": edges, "count": len(edges)})
}

func writeEvidenceErr(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	if errors.Is(err, store.ErrEvidenceNotFound) {
		status = http.StatusNotFound
	} else if errors.Is(err, store.ErrEvidenceConflict) || errors.Is(err, store.ErrEvidenceCycle) {
		status = http.StatusConflict
	} else if errors.Is(err, store.ErrEvidenceIntegrity) {
		status = http.StatusUnprocessableEntity
	}
	writeErr(w, status, err.Error())
}
