package server

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func (s *server) handleRetrieve(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var request contracts.RetrievalRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	result, err := s.retrieval.Retrieve(ctx, request)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, result)
}
