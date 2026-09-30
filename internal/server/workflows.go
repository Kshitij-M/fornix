package server

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/effectdispatch"
	"github.com/omaveda/fornix/internal/store"
	genericworkflow "github.com/omaveda/fornix/internal/workflows/generic"
)

func (s *server) handleGenericWorkflowCreate(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var input contracts.WorkflowCreateRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := decoder.Decode(&input); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid workflow create request")
		return
	}
	workspaceID := requestWorkspace(r, input.WorkspaceID)
	input.WorkspaceID = workspaceID
	input.Operation.WorkspaceID = workspaceID
	input.Operation.Actor = requestActor(r)
	if key := strings.TrimSpace(r.Header.Get("Idempotency-Key")); key != "" {
		if input.Idempotency != "" && input.Idempotency != key {
			writeErr(w, http.StatusBadRequest, "idempotency key mismatch")
			return
		}
		input.Idempotency = key
	}
	result, err := s.genericWorkflows.Create(r.Context(), input)
	if err != nil {
		writeGenericWorkflowError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run": result.Run, "operation": publicOperation(result.Operation), "event": result.Event, "duplicate": result.Duplicate})
}

func (s *server) handleGenericWorkflow(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/v1/workflows/")
	parts := strings.Split(strings.Trim(rest, "/"), "/")
	if len(parts) == 0 || strings.TrimSpace(parts[0]) == "" {
		writeErr(w, http.StatusNotFound, "workflow id required")
		return
	}
	runID := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		s.handleGenericWorkflowGet(w, r, runID)
		return
	}
	if len(parts) != 2 {
		writeErr(w, http.StatusNotFound, "unknown workflow operation")
		return
	}
	switch parts[1] {
	case "lease":
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleGenericWorkflowLease(w, r, runID)
	case "renew":
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleGenericWorkflowRenew(w, r, runID)
	case "release":
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleGenericWorkflowRelease(w, r, runID)
	case "advance":
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleGenericWorkflowAdvance(w, r, runID)
	case "approve":
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleGenericWorkflowApprove(w, r, runID)
	case "verify":
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleGenericWorkflowVerify(w, r, runID)
	case "cancel":
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleGenericWorkflowCancel(w, r, runID)
	case "replay":
		if r.Method != http.MethodPost {
			writeErr(w, http.StatusMethodNotAllowed, "POST only")
			return
		}
		s.handleGenericWorkflowReplay(w, r, runID)
	case "receipt":
		s.handleGenericWorkflowReceipt(w, r, runID)
	default:
		writeErr(w, http.StatusNotFound, "unknown workflow operation")
	}
}

func (s *server) handleGenericWorkflowGet(w http.ResponseWriter, r *http.Request, runID string) {
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	run, err := s.genericWorkflows.Get(r.Context(), workspaceID, runID)
	if err != nil {
		writeGenericWorkflowError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run": run})
}

func (s *server) handleGenericWorkflowAdvance(w http.ResponseWriter, r *http.Request, runID string) {
	var input contracts.WorkflowAdvanceRequest
	if err := decodeOptionalJSON(r, &input, 32<<10); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid workflow advance request")
		return
	}
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	fence, err := workflowFence(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "valid X-Operation-Fence is required")
		return
	}
	taskFence, err := workflowTaskFence(r, input.TaskFence)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid task fence")
		return
	}
	result, err := s.genericWorkflows.Advance(r.Context(), workspaceID, runID, requestActor(r), fence, taskFence)
	if err != nil {
		writeGenericWorkflowError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run": result})
}

func (s *server) handleGenericWorkflowApprove(w http.ResponseWriter, r *http.Request, runID string) {
	var input contracts.WorkflowResumeRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10)).Decode(&input); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid workflow approval request")
		return
	}
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	fence, err := workflowFence(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "valid X-Operation-Fence is required")
		return
	}
	taskFence, err := workflowTaskFence(r, 0)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid task fence")
		return
	}
	run, err := s.genericWorkflows.Approve(r.Context(), workspaceID, runID, requestActor(r), fence, taskFence, input.StepID)
	if err != nil {
		writeGenericWorkflowError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run": run})
}

func (s *server) handleGenericWorkflowVerify(w http.ResponseWriter, r *http.Request, runID string) {
	var input contracts.WorkflowVerifyRequest
	if err := decodeOptionalJSON(r, &input, 32<<10); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid workflow verification request")
		return
	}
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	workflowFenceValue, err := workflowFence(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "valid X-Operation-Fence is required")
		return
	}
	effectFenceValue := effectFenceHeader(r)
	if input.EffectFence != 0 && effectFenceValue != 0 && input.EffectFence != effectFenceValue {
		writeErr(w, http.StatusBadRequest, "effect fence mismatch")
		return
	}
	if effectFenceValue == 0 {
		effectFenceValue = input.EffectFence
	}
	if effectFenceValue == 0 {
		writeErr(w, http.StatusBadRequest, "X-Effect-Fence is required")
		return
	}
	input.EffectFence = effectFenceValue
	taskFenceValue, err := workflowTaskFence(r, input.TaskFence)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid task fence")
		return
	}
	input.TaskFence = taskFenceValue
	result, err := s.genericWorkflows.VerifyEffect(r.Context(), workspaceID, runID, requestActor(r), workflowFenceValue, taskFenceValue, input)
	if err != nil {
		writeGenericWorkflowError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleGenericWorkflowCancel(w http.ResponseWriter, r *http.Request, runID string) {
	var input contracts.WorkflowCancelRequest
	if err := decodeOptionalJSON(r, &input, 16<<10); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid workflow cancellation request")
		return
	}
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	fence, err := workflowFence(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "valid X-Operation-Fence is required")
		return
	}
	taskFence, err := workflowTaskFence(r, 0)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid task fence")
		return
	}
	run, err := s.genericWorkflows.Cancel(r.Context(), workspaceID, runID, requestActor(r), fence, taskFence)
	if err != nil {
		writeGenericWorkflowError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run": run, "reason": input.Reason})
}

func (s *server) handleGenericWorkflowReplay(w http.ResponseWriter, r *http.Request, runID string) {
	var input contracts.WorkflowReplayRequest
	if err := decodeOptionalJSON(r, &input, 16<<10); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid workflow replay request")
		return
	}
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	result, err := s.genericWorkflows.Replay(r.Context(), workspaceID, runID, input)
	if err != nil {
		writeGenericWorkflowError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) handleGenericWorkflowReceipt(w http.ResponseWriter, r *http.Request, runID string) {
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	receiptID := "receipt-" + runID
	if r.Method == http.MethodPost {
		receipt, duplicate, err := s.genericWorkflows.FinalizeReceipt(r.Context(), workspaceID, runID, requestActor(r))
		if err != nil {
			writeGenericWorkflowError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"receipt": receipt, "duplicate": duplicate})
		return
	}
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "GET or POST only")
		return
	}
	level := strings.TrimSpace(r.URL.Query().Get("level"))
	if level == "" {
		level = contracts.WorkReceiptDisclosureDetail
	}
	request := contracts.WorkReceiptDisclosureRequest{WorkspaceID: workspaceID, ReceiptID: receiptID, Level: level}
	if raw := strings.TrimSpace(r.URL.Query().Get("max_bytes")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil {
			writeErr(w, http.StatusBadRequest, "invalid max_bytes")
			return
		}
		request.MaxBytes = value
	}
	result, err := s.workReceipts.Disclose(r.Context(), request)
	if err != nil {
		writeGenericWorkflowError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func decodeOptionalJSON(r *http.Request, destination any, maxBytes int64) error {
	payload, err := io.ReadAll(io.LimitReader(r.Body, maxBytes+1))
	if err != nil {
		return err
	}
	if int64(len(payload)) > maxBytes {
		return errors.New("request body exceeds limit")
	}
	if len(strings.TrimSpace(string(payload))) == 0 {
		return nil
	}
	return json.Unmarshal(payload, destination)
}

type workflowLeaseRequest struct {
	TTLMS int64 `json:"ttl_ms,omitempty"`
}

func (s *server) handleGenericWorkflowLease(w http.ResponseWriter, r *http.Request, runID string) {
	var input workflowLeaseRequest
	if err := decodeOptionalJSON(r, &input, 16<<10); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid workflow lease request")
		return
	}
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	ttl := genericworkflow.DefaultLeaseTTL
	if input.TTLMS > 0 {
		ttl = time.Duration(input.TTLMS) * time.Millisecond
	}
	result, err := s.genericWorkflows.AcquireLease(r.Context(), workspaceID, runID, requestActor(r).ID, ttl)
	if err != nil {
		writeGenericWorkflowError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"lease": result.Lease, "acquired": result.Acquired, "reused": result.Reused, "takeover": result.Takeover})
}

func (s *server) handleGenericWorkflowRenew(w http.ResponseWriter, r *http.Request, runID string) {
	fence, err := workflowFence(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "valid X-Operation-Fence is required")
		return
	}
	var input workflowLeaseRequest
	if err := decodeOptionalJSON(r, &input, 16<<10); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid workflow renewal request")
		return
	}
	ttl := genericworkflow.DefaultLeaseTTL
	if input.TTLMS > 0 {
		ttl = time.Duration(input.TTLMS) * time.Millisecond
	}
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	lease, err := s.genericWorkflows.RenewLease(r.Context(), store.WorkflowLease{WorkspaceID: workspaceID, OperationID: runID, OwnerID: requestActor(r).ID, Fence: fence}, ttl)
	if err != nil {
		writeGenericWorkflowError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"lease": lease})
}

func (s *server) handleGenericWorkflowRelease(w http.ResponseWriter, r *http.Request, runID string) {
	fence, err := workflowFence(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "valid X-Operation-Fence is required")
		return
	}
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	err = s.genericWorkflows.ReleaseLease(r.Context(), store.WorkflowLease{WorkspaceID: workspaceID, OperationID: runID, OwnerID: requestActor(r).ID, Fence: fence})
	if err != nil {
		writeGenericWorkflowError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"released": true, "run_id": runID})
}

func workflowFence(r *http.Request) (uint64, error) {
	value := strings.TrimSpace(r.Header.Get("X-Operation-Fence"))
	if value == "" {
		return 0, store.ErrWorkflowLease
	}
	return strconv.ParseUint(value, 10, 64)
}

func workflowTaskFence(r *http.Request, bodyValue uint64) (uint64, error) {
	value := strings.TrimSpace(r.Header.Get("X-Task-Fence"))
	if value == "" {
		return bodyValue, nil
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0, err
	}
	if bodyValue != 0 && bodyValue != parsed {
		return 0, errors.New("task fence mismatch")
	}
	return parsed, nil
}

func writeGenericWorkflowError(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, store.ErrWorkflowNotFound), errors.Is(err, store.ErrWorkReceiptNotFound):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrWorkflowLease), errors.Is(err, store.ErrOperationLeaseHeld), errors.Is(err, store.ErrWorkflowIdempotency), errors.Is(err, store.ErrWorkflowReplay), errors.Is(err, genericworkflow.ErrEffectAuthorityNeeded), errors.Is(err, genericworkflow.ErrApprovalRequired), errors.Is(err, genericworkflow.ErrVerifierNotConfigured), errors.Is(err, genericworkflow.ErrVerifierUnavailable), errors.Is(err, genericworkflow.ErrVerifierStep), errors.Is(err, effectdispatch.ErrVerificationFence), errors.Is(err, effectdispatch.ErrVerificationState), errors.Is(err, effectdispatch.ErrVerificationConflict), errors.Is(err, effectdispatch.ErrVerificationProofNeeded):
		status = http.StatusConflict
	case errors.Is(err, store.ErrWorkflowWorkspace), errors.Is(err, store.ErrOperationLeaseFenced), errors.Is(err, store.ErrOperationLeaseExpired), errors.Is(err, store.ErrOperationLeaseReleased), errors.Is(err, store.ErrEffectLeaseMissing), errors.Is(err, store.ErrEffectLeaseOwned), errors.Is(err, store.ErrEffectLeaseFenced), errors.Is(err, store.ErrEffectLeaseExpired), errors.Is(err, store.ErrEffectLeaseReleased), errors.Is(err, store.ErrWorkReceiptWorkspace), errors.Is(err, store.ErrWorkReceiptIntegrity):
		status = http.StatusForbidden
	}
	writeErr(w, status, err.Error())
}
