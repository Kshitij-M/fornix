package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/eval"
	"github.com/omaveda/fornix/internal/store"
)

type retrievalEvalRunRequest struct {
	WorkspaceID     string                  `json:"workspace_id,omitempty"`
	DatasetID       string                  `json:"dataset_id,omitempty"`
	DatasetName     string                  `json:"dataset_name,omitempty"`
	DatasetVersion  int                     `json:"dataset_version,omitempty"`
	IdempotencyKey  string                  `json:"idempotency_key,omitempty"`
	BaselineEvalRun string                  `json:"baseline_eval_run_id,omitempty"`
	DryRun          bool                    `json:"dry_run,omitempty"`
	BatchLimit      int                     `json:"batch_limit,omitempty"`
	Gates           []contracts.QualityGate `json:"gates,omitempty"`
}

func (s *server) handleEvaluationDatasetCreate(w http.ResponseWriter, r *http.Request) {
	var dataset contracts.EvalDataset
	if err := json.NewDecoder(r.Body).Decode(&dataset); err != nil {
		writeEvalErr(w, http.StatusBadRequest, err)
		return
	}
	dataset.WorkspaceID = requestWorkspace(r, dataset.WorkspaceID)
	if strings.TrimSpace(dataset.Name) == "" || dataset.Version < 1 {
		writeEvalErr(w, http.StatusBadRequest, fmt.Errorf("dataset name and positive version are required"))
		return
	}
	if dataset.ID == "" {
		dataset.ID = deterministicDatasetID(dataset.WorkspaceID, dataset.Name, dataset.Version)
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	stored, created, err := s.evaluations.CreateDataset(ctx, dataset)
	if err != nil {
		writeEvalErr(w, http.StatusConflict, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"dataset": stored, "created": created})
}

func (s *server) handleRetrievalSurfaceRegister(w http.ResponseWriter, r *http.Request) {
	var surface contracts.RetrievalSurface
	if err := json.NewDecoder(r.Body).Decode(&surface); err != nil {
		writeEvalErr(w, http.StatusBadRequest, err)
		return
	}
	surface.WorkspaceID = requestWorkspace(r, surface.WorkspaceID)
	surface.Actor = requestActor(r)
	if surface.RequestID == "" {
		surface.RequestID = requestIDFromRequest(r)
	}
	if surface.IdempotencyKey == "" {
		surface.IdempotencyKey = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	if surface.IdempotencyKey == "" {
		surface.IdempotencyKey = surface.RequestID
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	stored, created, err := s.retrievalSurfaces.Capture(ctx, surface)
	if err != nil {
		if errors.Is(err, store.ErrRetrievalSurfaceConflict) {
			writeEvalErr(w, http.StatusConflict, err)
			return
		}
		writeEvalErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"surface": stored, "created": created})
}

func (s *server) handleRetrievalSurfaceList(w http.ResponseWriter, r *http.Request) {
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	limit := 0
	if raw := strings.TrimSpace(r.URL.Query().Get("limit")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			writeEvalErr(w, http.StatusBadRequest, fmt.Errorf("invalid limit"))
			return
		}
		limit = parsed
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	page, err := s.retrievalSurfaces.List(ctx, workspaceID, limit, r.URL.Query().Get("cursor"))
	if err != nil {
		writeEvalErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, page)
}

func (s *server) handleRetrievalEvaluationRun(w http.ResponseWriter, r *http.Request) {
	var input retrievalEvalRunRequest
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		writeEvalErr(w, http.StatusBadRequest, err)
		return
	}
	workspaceID := requestWorkspace(r, input.WorkspaceID)
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	dataset, err := s.evaluationDataset(ctx, workspaceID, input)
	if err != nil {
		writeEvalErr(w, http.StatusNotFound, err)
		return
	}
	surfaces, err := s.retrievalEvaluationInputs(ctx, dataset)
	if err != nil {
		writeEvalErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	key := strings.TrimSpace(input.IdempotencyKey)
	if key == "" {
		key = strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	}
	if key == "" {
		key = "evaluation-request:" + requestIDFromRequest(r)
	}
	input.Gates = canonicalEvaluationGates(input.Gates)
	requestHash := hashEvaluationRequest(workspaceID, dataset.DatasetHash, input, key)
	run := contracts.EvalRun{
		ID: contracts.NewID("eval"), WorkspaceID: workspaceID, DatasetID: dataset.ID,
		DatasetHash: dataset.DatasetHash, IdempotencyKey: key, RequestHash: requestHash,
		Status: contracts.EvalRunPending, DryRun: input.DryRun, BatchLimit: input.BatchLimit,
		BaselineEvalRunID: strings.TrimSpace(input.BaselineEvalRun), Gates: input.Gates,
	}
	// A stable run ID keeps dry-run reports deterministic. Durable retries still
	// use the idempotency key as the authority boundary in EvaluationStore.
	run.ID = "eval_" + requestHash[:32]
	runner := eval.Runner{Evaluations: s.evaluations, Evidence: s.evidence}
	finished, results, err := runner.EvaluateRetrievalDataset(ctx, dataset, run, surfaces, eval.DefaultRegressionPolicy())
	if err != nil {
		writeEvalErr(w, http.StatusUnprocessableEntity, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run": finished, "results": results, "dry_run": input.DryRun})
}

func canonicalEvaluationGates(gates []contracts.QualityGate) []contracts.QualityGate {
	result := append([]contracts.QualityGate(nil), gates...)
	for i := range result {
		result[i].Name = strings.TrimSpace(result[i].Name)
		result[i].Metric = strings.TrimSpace(result[i].Metric)
		result[i].Operator = strings.TrimSpace(result[i].Operator)
		result[i].Reason = ""
		result[i].Actual = 0
		result[i].Passed = false
	}
	sort.SliceStable(result, func(i, j int) bool {
		if result[i].Name != result[j].Name {
			return result[i].Name < result[j].Name
		}
		if result[i].Metric != result[j].Metric {
			return result[i].Metric < result[j].Metric
		}
		if result[i].Operator != result[j].Operator {
			return result[i].Operator < result[j].Operator
		}
		return result[i].Threshold < result[j].Threshold
	})
	return result
}

func (s *server) handleEvaluationRunGet(w http.ResponseWriter, r *http.Request, id string) {
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	run, err := s.evaluations.GetRun(ctx, workspaceID, id)
	if err != nil {
		if errors.Is(err, store.ErrEvalNotFound) {
			writeEvalErr(w, http.StatusNotFound, err)
			return
		}
		writeEvalErr(w, http.StatusBadRequest, err)
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *server) evaluationDataset(ctx context.Context, workspaceID string, input retrievalEvalRunRequest) (contracts.EvalDataset, error) {
	if strings.TrimSpace(input.DatasetID) != "" {
		return s.evaluations.GetDatasetByID(ctx, workspaceID, input.DatasetID)
	}
	if strings.TrimSpace(input.DatasetName) == "" || input.DatasetVersion < 1 {
		return contracts.EvalDataset{}, fmt.Errorf("dataset_id or dataset_name and dataset_version are required")
	}
	return s.evaluations.GetDataset(ctx, workspaceID, input.DatasetName, input.DatasetVersion)
}

func (s *server) retrievalEvaluationInputs(ctx context.Context, dataset contracts.EvalDataset) (map[string]eval.RetrievalScoreInput, error) {
	ids := make([]string, 0, len(dataset.Cases))
	for _, item := range dataset.Cases {
		id := item.RetrievalSurfaceID
		if id == "" {
			id = item.ReplayRunID
		}
		if id == "" {
			return nil, fmt.Errorf("case %s has no retrieval_surface_id", item.ID)
		}
		ids = append(ids, id)
	}
	stored, err := s.retrievalSurfaces.GetMany(ctx, dataset.WorkspaceID, ids)
	if err != nil {
		return nil, fmt.Errorf("resolve recorded retrieval surfaces: %w", err)
	}
	result := make(map[string]eval.RetrievalScoreInput, len(dataset.Cases))
	for _, item := range dataset.Cases {
		id := item.RetrievalSurfaceID
		if id == "" {
			id = item.ReplayRunID
		}
		surface, ok := stored[id]
		if !ok {
			return nil, fmt.Errorf("recorded retrieval surface %s is missing", id)
		}
		result[item.ID] = eval.RetrievalScoreInput{
			WorkspaceID: dataset.WorkspaceID, Case: item, Pack: surface.ContextPack(), Trace: surface.Trace,
			Measurement: eval.RetrievalMeasurement{LatencyMS: surface.DurationMS, SQLQueries: surface.SQLQueries, CostUSD: surface.CostUSD, CostKnown: surface.CostKnown, CostEstimated: surface.CostEstimated},
		}
	}
	return result, nil
}

func deterministicDatasetID(workspaceID, name string, version int) string {
	digest := sha256.Sum256([]byte("dataset\x00" + strings.TrimSpace(workspaceID) + "\x00" + strings.TrimSpace(name) + "\x00" + strconv.Itoa(version)))
	return "dataset_" + hex.EncodeToString(digest[:16])
}

func hashEvaluationRequest(workspaceID, datasetHash string, input retrievalEvalRunRequest, key string) string {
	payload := struct {
		WorkspaceID     string                  `json:"workspace_id"`
		DatasetHash     string                  `json:"dataset_hash"`
		DatasetID       string                  `json:"dataset_id"`
		DatasetName     string                  `json:"dataset_name"`
		DatasetVersion  int                     `json:"dataset_version"`
		IdempotencyKey  string                  `json:"idempotency_key"`
		BaselineEvalRun string                  `json:"baseline_eval_run_id"`
		DryRun          bool                    `json:"dry_run"`
		BatchLimit      int                     `json:"batch_limit"`
		Gates           []contracts.QualityGate `json:"gates,omitempty"`
	}{workspaceID, datasetHash, input.DatasetID, input.DatasetName, input.DatasetVersion, key, strings.TrimSpace(input.BaselineEvalRun), input.DryRun, input.BatchLimit, input.Gates}
	raw, _ := json.Marshal(payload)
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func writeEvalErr(w http.ResponseWriter, status int, err error) {
	if err == nil {
		writeErr(w, status, "evaluation failed")
		return
	}
	writeErr(w, status, err.Error())
}
