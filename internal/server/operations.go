package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

// operationCreateRequest is the authenticated HTTP envelope for one generic
// operation. The request is typed and hashable; it never carries executable
// code, raw prompts, credentials, or connector payload bytes.
type operationCreateRequest struct {
	Request     contracts.OperationRequest `json:"request"`
	Plan        *contracts.OperationPlan   `json:"plan,omitempty"`
	Resources   []store.OperationResource  `json:"resources,omitempty"`
	Links       []store.OperationLink      `json:"links,omitempty"`
	TaskFence   uint64                     `json:"task_fence,omitempty"`
	Idempotency string                     `json:"idempotency_key,omitempty"`
}

type operationLeaseRequest struct {
	TTLMS int64 `json:"ttl_ms,omitempty"`
}

// operationTransitionRequest deliberately omits actor and owner fields from
// the public contract. They come from the authenticated principal and the
// current operation lease; accepting caller-supplied identity here would turn
// the HTTP adapter into a fence bypass.
type operationTransitionRequest struct {
	RequestID      string                      `json:"request_id,omitempty"`
	IdempotencyKey string                      `json:"idempotency_key"`
	CausationID    string                      `json:"causation_id,omitempty"`
	CorrelationID  string                      `json:"correlation_id,omitempty"`
	ToStatus       string                      `json:"to_status"`
	ReasonCode     string                      `json:"reason_code,omitempty"`
	ResultHash     string                      `json:"result_hash,omitempty"`
	ReportHash     string                      `json:"report_hash,omitempty"`
	Failure        *contracts.OperationFailure `json:"failure,omitempty"`
	NextRetryAt    *time.Time                  `json:"next_retry_at,omitempty"`
}

type operationReplayRequest struct {
	FromVersion int64 `json:"from_version,omitempty"`
	Limit       int   `json:"limit,omitempty"`
}

func (s *server) handleOperationCreate(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	var input operationCreateRequest
	decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := decoder.Decode(&input); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid operation request")
		return
	}
	workspaceID := requestWorkspace(r, input.Request.WorkspaceID)
	input.Request.WorkspaceID = workspaceID
	input.Request.Actor = requestActor(r)
	headerIdempotency := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if input.Idempotency != "" && headerIdempotency != "" && input.Idempotency != headerIdempotency {
		writeErr(w, http.StatusBadRequest, "idempotency key mismatch")
		return
	}
	if input.Idempotency == "" {
		input.Idempotency = headerIdempotency
	}
	if input.Request.IdempotencyKey != "" && input.Idempotency != "" && input.Request.IdempotencyKey != input.Idempotency {
		writeErr(w, http.StatusBadRequest, "idempotency key mismatch")
		return
	}
	if input.Request.IdempotencyKey == "" {
		input.Request.IdempotencyKey = input.Idempotency
	}
	// A task-bound operation is owned by the authenticated worker. The fence
	// itself remains caller-supplied because it is obtained by claiming the
	// task, then validated against the live Postgres lease in OperationStore.
	taskOwner := ""
	if input.Request.Task != nil {
		taskOwner = requestActor(r).ID
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	result, err := s.operations.Create(ctx, store.OperationCreateInput{
		Request: input.Request, Plan: input.Plan, Resources: input.Resources,
		Links: input.Links, TaskOwnerID: taskOwner, TaskFence: input.TaskFence,
	})
	if err != nil {
		writeOperationErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"operation": publicOperation(result.Operation),
		"event":     result.Event,
		"duplicate": result.Duplicate,
	})
}

func (s *server) handleOperation(w http.ResponseWriter, r *http.Request) {
	if !s.requireAuth(r) {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	parts := strings.Split(strings.Trim(strings.TrimPrefix(r.URL.Path, "/v1/operations/"), "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		writeErr(w, http.StatusNotFound, "operation id required")
		return
	}
	operationID := parts[0]
	if len(parts) == 1 && r.Method == http.MethodGet {
		s.handleOperationGet(w, r, operationID)
		return
	}
	if len(parts) != 2 || r.Method != http.MethodPost {
		writeErr(w, http.StatusNotFound, "unknown operation command")
		return
	}
	switch parts[1] {
	case "lease":
		s.handleOperationLease(w, r, operationID)
	case "renew":
		s.handleOperationLeaseRenew(w, r, operationID)
	case "release":
		s.handleOperationLeaseRelease(w, r, operationID)
	case "transition":
		s.handleOperationTransition(w, r, operationID)
	case "replay":
		s.handleOperationReplay(w, r, operationID)
	default:
		writeErr(w, http.StatusNotFound, "unknown operation command")
	}
}

func (s *server) handleOperationLeaseRenew(w http.ResponseWriter, r *http.Request, operationID string) {
	input, err := decodeOperationLeaseRequest(w, r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid lease renewal request")
		return
	}
	principal, _ := principalFromRequest(r)
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	lease, err := s.operations.RenewLease(ctx, store.OperationLease{
		WorkspaceID: requestWorkspace(r, r.URL.Query().Get("workspace_id")), OperationID: operationID,
		OwnerID: principal.ID, Fence: operationLeaseFence(r),
	}, leaseTTL(input.TTLMS))
	if err != nil {
		writeOperationErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"lease": publicOperationLease(lease)})
}

func (s *server) handleOperationLeaseRelease(w http.ResponseWriter, r *http.Request, operationID string) {
	principal, _ := principalFromRequest(r)
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	err := s.operations.ReleaseLease(ctx, store.OperationLease{
		WorkspaceID: requestWorkspace(r, r.URL.Query().Get("workspace_id")), OperationID: operationID,
		OwnerID: principal.ID, Fence: operationLeaseFence(r),
	})
	if err != nil {
		writeOperationErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"released": true, "operation_id": operationID})
}

func decodeOperationLeaseRequest(w http.ResponseWriter, r *http.Request) (operationLeaseRequest, error) {
	var input operationLeaseRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
		return operationLeaseRequest{}, err
	}
	return input, nil
}

func leaseTTL(ttlMS int64) time.Duration {
	if ttlMS == 0 {
		return 30 * time.Second
	}
	return time.Duration(ttlMS) * time.Millisecond
}

func (s *server) handleOperationGet(w http.ResponseWriter, r *http.Request, operationID string) {
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	operation, err := s.operations.Get(ctx, requestWorkspace(r, r.URL.Query().Get("workspace_id")), operationID)
	if err != nil {
		writeOperationErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"operation": publicOperation(operation)})
}

func (s *server) handleOperationLease(w http.ResponseWriter, r *http.Request, operationID string) {
	input, err := decodeOperationLeaseRequest(w, r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid lease request")
		return
	}
	ttl := leaseTTL(input.TTLMS)
	principal, _ := principalFromRequest(r)
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	result, err := s.operations.AcquireLease(ctx, requestWorkspace(r, r.URL.Query().Get("workspace_id")), operationID, principal.ID, ttl)
	if err != nil {
		writeOperationErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"lease": publicOperationLease(result.Lease), "acquired": result.Acquired, "reused": result.Reused, "takeover": result.Takeover})
}

func (s *server) handleOperationTransition(w http.ResponseWriter, r *http.Request, operationID string) {
	var input operationTransitionRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 128<<10)).Decode(&input); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid transition request")
		return
	}
	headerIdempotency := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if input.IdempotencyKey != "" && headerIdempotency != "" && input.IdempotencyKey != headerIdempotency {
		writeErr(w, http.StatusBadRequest, "idempotency key mismatch")
		return
	}
	if input.IdempotencyKey == "" {
		input.IdempotencyKey = headerIdempotency
	}
	workspaceID := requestWorkspace(r, "")
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	operation, err := s.operations.Get(ctx, workspaceID, operationID)
	if err != nil {
		writeOperationErr(w, err)
		return
	}
	principal, _ := principalFromRequest(r)
	result, err := s.operations.Transition(ctx, store.OperationTransitionInput{
		WorkspaceID: workspaceID, OperationID: operationID, OwnerID: principal.ID,
		Actor: requestActor(r), RequestID: input.RequestID, IdempotencyKey: input.IdempotencyKey,
		CausationID: input.CausationID, CorrelationID: input.CorrelationID,
		ToStatus: input.ToStatus, ReasonCode: input.ReasonCode, ResultHash: input.ResultHash,
		ReportHash: input.ReportHash, Failure: input.Failure, NextRetryAt: input.NextRetryAt,
		TaskOwnerID: operation.TaskOwnerID, TaskFence: operation.TaskFence,
		// The current operation lease is looked up and validated by the store.
		Fence: operationLeaseFence(r),
	})
	if err != nil {
		writeOperationErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"operation": publicOperation(result.Operation), "transition": result.Transition, "event": result.Event, "duplicate": result.Duplicate})
}

// operationLeaseFence reads only an explicit worker header. The owner is
// always the authenticated principal; a missing or malformed fence fails in
// the store rather than silently selecting a current lease.
func operationLeaseFence(r *http.Request) uint64 {
	value, _ := strconv.ParseUint(strings.TrimSpace(r.Header.Get("X-Operation-Fence")), 10, 64)
	return value
}

func (s *server) handleOperationReplay(w http.ResponseWriter, r *http.Request, operationID string) {
	var input operationReplayRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, "invalid replay request")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	result, err := s.operations.Replay(ctx, requestWorkspace(r, r.URL.Query().Get("workspace_id")), operationID, input.FromVersion, input.Limit)
	if err != nil {
		writeOperationErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"operation": publicOperation(result.Operation), "state_version": result.StateVersion, "state_hash": result.StateHash, "replay_hash": result.ReplayHash, "transition_count": result.TransitionCount, "verified": result.Verified})
}

func publicOperation(operation store.Operation) map[string]any {
	return map[string]any{
		"workspace_id": operation.WorkspaceID, "id": operation.ID, "schema_version": operation.SchemaVersion,
		"request_id": operation.RequestID, "idempotency_key": operation.IdempotencyKey,
		"request_hash": operation.RequestHash, "operation_hash": operation.OperationHash,
		"status": operation.Status, "request": operation.Request, "plan": operation.Plan,
		"plan_hash": operation.PlanHash, "result_hash": operation.ResultHash, "report_hash": operation.ReportHash,
		"failure": operation.Failure, "next_retry_at": operation.NextRetryAt, "state_version": operation.StateVersion,
		"state_hash": operation.StateHash, "task_owner_id": operation.TaskOwnerID, "task_fence": operation.TaskFence,
		"created_at": operation.CreatedAt, "updated_at": operation.UpdatedAt, "started_at": operation.StartedAt,
		"completed_at": operation.CompletedAt,
	}
}

func publicOperationLease(lease store.OperationLease) map[string]any {
	return map[string]any{"workspace_id": lease.WorkspaceID, "operation_id": lease.OperationID, "owner_id": lease.OwnerID, "fence": lease.Fence, "lease_until": lease.LeaseUntil, "acquired_at": lease.AcquiredAt, "renewed_at": lease.RenewedAt, "released_at": lease.ReleasedAt}
}

func writeOperationErr(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, store.ErrOperationNotFound):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrOperationWorkspace), errors.Is(err, store.ErrOperationTaskFence):
		status = http.StatusForbidden
	case errors.Is(err, store.ErrOperationIdempotency), errors.Is(err, store.ErrOperationTransition), errors.Is(err, store.ErrOperationLeaseMissing), errors.Is(err, store.ErrOperationLeaseHeld), errors.Is(err, store.ErrOperationLeaseOwned), errors.Is(err, store.ErrOperationLeaseFenced), errors.Is(err, store.ErrOperationLeaseExpired), errors.Is(err, store.ErrOperationLeaseReleased):
		status = http.StatusConflict
	case errors.Is(err, store.ErrOperationReplay):
		status = http.StatusUnprocessableEntity
	}
	writeErr(w, status, err.Error())
}
