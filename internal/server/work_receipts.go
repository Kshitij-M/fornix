package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func (s *server) handleWorkReceiptFinalize(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var request contracts.WorkReceiptFinalizeRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, contracts.MaxWorkReceiptPayloadSize))
	if err := decoder.Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	request.Actor = requestActor(r)
	if request.IdempotencyKey == "" {
		request.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	receipt, created, err := s.workReceipts.Finalize(ctx, request)
	if err != nil {
		writeWorkReceiptErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"receipt": receipt, "created": created})
}

func (s *server) handleWorkReceiptGet(w http.ResponseWriter, r *http.Request, id string) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	receipt, err := s.workReceipts.Get(ctx, requestWorkspace(r, r.URL.Query().Get("workspace_id")), id)
	if err != nil {
		writeWorkReceiptErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"receipt": receipt})
}

func (s *server) handleWorkReceiptDisclosure(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var request contracts.WorkReceiptDisclosureRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json: "+err.Error())
		return
	}
	request.WorkspaceID = requestWorkspace(r, request.WorkspaceID)
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	result, err := s.workReceipts.Disclose(ctx, request)
	if err != nil {
		writeWorkReceiptErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func writeWorkReceiptErr(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, store.ErrWorkReceiptNotFound):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrWorkReceiptConflict):
		status = http.StatusConflict
	case errors.Is(err, store.ErrWorkReceiptStale):
		status = http.StatusConflict
	case errors.Is(err, store.ErrWorkReceiptWorkspace):
		status = http.StatusForbidden
	case errors.Is(err, store.ErrWorkReceiptIntegrity):
		status = http.StatusUnprocessableEntity
	}
	writeErr(w, status, err.Error())
}
