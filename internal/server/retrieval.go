package server

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/retrieval"
	"github.com/omaveda/fornix/internal/store"
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
	request.RequestID = requestIDFromRequest(r)
	if request.IdempotencyKey == "" {
		request.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	request.Actor = requestActor(r)
	request.CausationID = requestIDFromRequest(r)
	request.CorrelationID = requestIDFromRequest(r)
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	result, err := s.retrieval.Retrieve(ctx, request)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err.Error())
		return
	}
	if s.observability != nil {
		_ = s.observability.ObserveRetrieval(ctx, request, result.Trace)
	}
	writeJSON(w, http.StatusOK, result)
}

func captureRetrievalSurface(ctx context.Context, surfaces *store.RetrievalSurfaceStore, request contracts.RetrievalRequest, result retrieval.Result, duration time.Duration) error {
	if surfaces == nil {
		return nil
	}
	requestHash := result.Plan.RequestHash
	requestID := strings.TrimSpace(request.RequestID)
	idempotencyKey := strings.TrimSpace(request.IdempotencyKey)
	if idempotencyKey == "" && requestID != "" {
		idempotencyKey = "retrieval-request:" + requestID
	}
	if idempotencyKey == "" {
		// Internal callers do not always have a transport request ID. Give each
		// capture an opaque identity so corpus changes cannot conflict with an
		// unrelated later request that has the same logical request hash.
		idempotencyKey = "retrieval-surface:" + contracts.NewID("capture")
	}
	if requestID == "" {
		requestID = idempotencyKey
	}
	refs := make([]contracts.RetrievalSurfaceReference, 0, len(result.Pack.Items))
	for _, item := range result.Pack.Items {
		refs = append(refs, contracts.RetrievalSurfaceReference{
			SourceReference: item.SourceReference, Kind: item.Kind, EvidenceHash: item.EvidenceHash,
			Score: item.Score, Stage: item.Stage, Representation: item.Representation, Truncated: item.Truncated,
		})
	}
	durationMS := duration.Milliseconds()
	if duration > 0 && durationMS == 0 {
		durationMS = 1
	}
	surface := contracts.RetrievalSurface{
		WorkspaceID: request.WorkspaceID, RequestID: requestID, IdempotencyKey: idempotencyKey,
		RequestHash: requestHash, PlanHash: result.Trace.PlanHash, ContextHash: result.Pack.ContentHash,
		Budget: result.Plan.Budget, Trace: result.Trace, References: refs, DurationMS: durationMS,
		SQLQueries: retrievalTraceQueries(result.Trace), CostEstimated: true, Actor: request.Actor,
		CausationID: request.CausationID, CorrelationID: request.CorrelationID,
	}
	_, _, err := surfaces.Capture(ctx, surface)
	return err
}

func retrievalTraceQueries(trace contracts.RetrievalTrace) int {
	queries := 0
	for _, stage := range trace.Stages {
		queries += stage.Queries
	}
	return queries
}
