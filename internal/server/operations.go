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

	"github.com/jackc/pgx/v5"

	connectorruntime "github.com/omaveda/fornix/internal/connector"
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

// operationEffectReserveRequest is deliberately reference-only. The effect
// payload is owned by the connector and must be represented by hashes and
// provider identifiers, never by credentials or arbitrary request bytes.
type operationEffectReserveRequest struct {
	StepID      string                   `json:"step_id"`
	AttemptID   string                   `json:"attempt_id"`
	RequestHash string                   `json:"request_hash"`
	Effect      contracts.ExternalEffect `json:"effect"`
}

// operationEffectStateRequest is the public reconciliation command. Identity,
// workspace, operation, owner, and fence are supplied by the authenticated
// route and operation lease; accepting them from JSON would create a fence or
// workspace-confusion primitive.
type operationEffectStateRequest struct {
	RequestID         string `json:"request_id,omitempty"`
	IdempotencyKey    string `json:"idempotency_key"`
	State             string `json:"state"`
	ProviderRequestID string `json:"provider_request_id,omitempty"`
	ResponseHash      string `json:"response_hash,omitempty"`
	VerificationHash  string `json:"verification_hash,omitempty"`
	CompensationHash  string `json:"compensation_hash,omitempty"`
	FailureCode       string `json:"failure_code,omitempty"`
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

type operationExecuteRequest struct {
	IdempotencyKey  string `json:"idempotency_key,omitempty"`
	TaskFence       uint64 `json:"task_fence,omitempty"`
	ApprovalGranted bool   `json:"approval_granted,omitempty"`
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
	if len(parts) == 3 && parts[1] == "effects" {
		effectID := parts[2]
		if r.Method == http.MethodGet {
			s.handleOperationEffectGet(w, r, operationID, effectID)
			return
		}
		if r.Method == http.MethodPost {
			s.handleOperationEffectReserve(w, r, operationID, effectID)
			return
		}
	}
	if len(parts) == 4 && parts[1] == "effects" && parts[3] == "state" && r.Method == http.MethodPost {
		s.handleOperationEffectState(w, r, operationID, parts[2])
		return
	}
	if len(parts) != 2 || r.Method != http.MethodPost {
		writeErr(w, http.StatusNotFound, "unknown operation command")
		return
	}
	switch parts[1] {
	case "execute":
		s.handleOperationExecute(w, r, operationID)
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

// handleOperationEffectReserve makes an external effect visible before a
// connector is dispatched. The operation store owns reservation idempotency,
// operation leases, task fences, and the initial append-only state.
func (s *server) handleOperationEffectReserve(w http.ResponseWriter, r *http.Request, operationID, effectID string) {
	if s.admission == nil {
		writeErr(w, http.StatusServiceUnavailable, "effect admission unavailable")
		return
	}
	var input operationEffectReserveRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10)).Decode(&input); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid effect reservation request")
		return
	}
	principal, ok := principalFromRequest(r)
	if !ok || !principal.Authenticated {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	operation, err := s.operations.Get(ctx, workspaceID, operationID)
	if err != nil {
		writeOperationErr(w, err)
		return
	}
	if operation.Request.Actor.ID != principal.ID || operation.Request.Actor.WorkspaceID != principal.WorkspaceID {
		writeOperationErr(w, store.ErrOperationWorkspace)
		return
	}
	pathEffectID := strings.TrimSpace(effectID)
	if pathEffectID == "reserve" {
		pathEffectID = ""
	}
	if pathEffectID != "" && strings.TrimSpace(input.Effect.ID) != "" && strings.TrimSpace(input.Effect.ID) != pathEffectID {
		writeErr(w, http.StatusBadRequest, "effect id mismatch")
		return
	}
	// `reserve` is the create form of this endpoint; an explicit effect ID is
	// still accepted in the body, while the path form remains authoritative
	// when a caller selects an identity.
	if pathEffectID != "" {
		input.Effect.ID = pathEffectID
	}
	input.Effect.WorkspaceID = workspaceID
	result, inserted, err := s.operations.ReserveEffect(ctx, store.OperationEffectInput{
		WorkspaceID: workspaceID, OperationID: operationID, StepID: input.StepID, AttemptID: input.AttemptID,
		OwnerID: principal.ID, Fence: operationLeaseFence(r), Effect: input.Effect, RequestHash: input.RequestHash,
	})
	if err != nil {
		writeOperationErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"effect": publicOperationEffect(result), "reserved": inserted})
}

func (s *server) handleOperationEffectGet(w http.ResponseWriter, r *http.Request, operationID, effectID string) {
	if s.admission == nil {
		writeErr(w, http.StatusServiceUnavailable, "effect admission unavailable")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
	defer cancel()
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	state, err := s.admission.GetEffectState(ctx, workspaceID, effectID)
	if err != nil {
		writeAdmissionErr(w, err)
		return
	}
	if state.OperationID != operationID {
		writeErr(w, http.StatusNotFound, "effect not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"effect": publicEffectState(state)})
}

func (s *server) handleOperationEffectState(w http.ResponseWriter, r *http.Request, operationID, effectID string) {
	if s.admission == nil {
		writeErr(w, http.StatusServiceUnavailable, "effect admission unavailable")
		return
	}
	var input operationEffectStateRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<10)).Decode(&input); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid effect state request")
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
	if input.IdempotencyKey == "" {
		writeErr(w, http.StatusBadRequest, "idempotency key is required")
		return
	}
	principal, ok := principalFromRequest(r)
	if !ok || !principal.Authenticated {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	operation, err := s.operations.Get(ctx, workspaceID, operationID)
	if err != nil {
		writeOperationErr(w, err)
		return
	}
	if operation.Request.Actor.ID != principal.ID || operation.Request.Actor.WorkspaceID != principal.WorkspaceID {
		writeOperationErr(w, store.ErrOperationWorkspace)
		return
	}
	requestID := strings.TrimSpace(input.RequestID)
	if requestID == "" {
		requestID = requestIDFromRequest(r)
	}
	if requestID == "" {
		requestID = "effect-state:" + input.IdempotencyKey
	}
	result, err := s.admission.UpdateEffect(ctx, contracts.ExternalEffectUpdate{
		WorkspaceID: workspaceID, OperationID: operationID, EffectID: effectID, OwnerID: principal.ID,
		Fence: operationLeaseFence(r), RequestID: requestID, IdempotencyKey: input.IdempotencyKey,
		State: input.State, ProviderRequestID: input.ProviderRequestID, ResponseHash: input.ResponseHash,
		VerificationHash: input.VerificationHash, CompensationHash: input.CompensationHash, FailureCode: input.FailureCode,
	})
	if err != nil {
		writeAdmissionErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"effect": publicEffectState(result.State), "duplicate": result.Duplicate})
}

// handleOperationExecute connects the durable operation authority to the
// registered connector catalog. The first public executor is intentionally
// limited to read-only and observation capabilities; effectful connectors
// must use the durable admission/effect reservation path before they can be
// exposed here.
func (s *server) handleOperationExecute(w http.ResponseWriter, r *http.Request, operationID string) {
	if s.connectorExecutor == nil || s.connectorRegistry == nil {
		writeErr(w, http.StatusServiceUnavailable, "connector executor unavailable")
		return
	}
	var input operationExecuteRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&input); err != nil && !errors.Is(err, io.EOF) {
		writeErr(w, http.StatusBadRequest, "invalid operation execution request")
		return
	}
	principal, ok := principalFromRequest(r)
	if !ok || !principal.Authenticated {
		writeErr(w, http.StatusUnauthorized, "unauthorised")
		return
	}
	workspaceID := requestWorkspace(r, r.URL.Query().Get("workspace_id"))
	ctx, cancel := context.WithTimeout(r.Context(), 90*time.Second)
	defer cancel()
	operation, err := s.operations.Get(ctx, workspaceID, operationID)
	if err != nil {
		writeOperationErr(w, err)
		return
	}
	if operation.Request.Actor.ID != principal.ID || operation.Request.Actor.WorkspaceID != principal.WorkspaceID {
		writeOperationErr(w, store.ErrOperationWorkspace)
		return
	}
	if stored, resultErr := s.operations.GetResult(ctx, workspaceID, operationID); resultErr == nil {
		writeJSON(w, http.StatusOK, map[string]any{"operation": publicOperation(operation), "result": publicOperationResult(stored), "duplicate": true})
		return
	} else if !errors.Is(resultErr, store.ErrOperationResultNotFound) {
		writeOperationErr(w, resultErr)
		return
	}
	capability, found := s.connectorRegistry.Lookup(operation.Request.Capability)
	if !found {
		writeErr(w, http.StatusConflict, "capability is not available")
		return
	}
	definition := capability.Definition()
	if definition.Effect != contracts.EffectClassReadOnly && definition.Effect != contracts.EffectClassObservation {
		writeErr(w, http.StatusConflict, "effectful capability requires durable effect admission")
		return
	}
	if len(operation.Request.Metadata) > 0 && strings.EqualFold(operation.Request.Metadata["external_effect"], "true") {
		writeErr(w, http.StatusConflict, "external effect execution requires durable effect admission")
		return
	}
	options := connectorruntime.AdmissionOptions{
		Principal:       &principal,
		ApprovalGranted: input.ApprovalGranted,
		Authorize: func(_ context.Context, value contracts.Principal, request contracts.OperationRequest, _ contracts.CapabilityDefinition) (bool, error) {
			return value.WorkspaceID == request.WorkspaceID && value.ID == request.Actor.ID && value.Has(contracts.PermissionOperationExecute), nil
		},
	}
	admission, err := s.connectorRegistry.Admit(ctx, operation.Request, options)
	if err != nil {
		writeErr(w, http.StatusConflict, "operation admission failed")
		return
	}
	plan, err := admission.Capability.Plan(operation.Request)
	if err != nil {
		writeErr(w, http.StatusUnprocessableEntity, "connector plan failed")
		return
	}
	ownerID := principal.ID
	taskOwnerID, taskFence := operation.TaskOwnerID, operation.TaskFence
	lease, err := operationLeaseForRequest(ctx, s.operations, workspaceID, operationID, ownerID, operationFenceHeader(r), operation.Request.Task != nil)
	if err != nil {
		writeOperationErr(w, err)
		return
	}
	if input.TaskFence != 0 {
		taskFence = input.TaskFence
	}
	executionKey := strings.TrimSpace(input.IdempotencyKey)
	if executionKey == "" {
		executionKey = contracts.HashStrings("operation-execute", operationID, operation.OperationHash)
	}
	if operation.Status == contracts.OperationStatusCreated {
		planned, planErr := s.operations.AttachPlan(ctx, store.OperationPlanInput{
			WorkspaceID: workspaceID, OperationID: operationID, OwnerID: ownerID, Fence: lease.Fence,
			TaskOwnerID: taskOwnerID, TaskFence: taskFence, Actor: principal.Actor(), RequestID: requestIDFromRequest(r),
			IdempotencyKey: executionKey + ":plan", CausationID: operation.Request.CausationID, CorrelationID: operation.Request.CorrelationID, Plan: plan,
		})
		if planErr != nil {
			writeOperationErr(w, planErr)
			return
		}
		operation = planned.Operation
	}
	for _, target := range []string{contracts.OperationStatusAdmitted, contracts.OperationStatusRunning} {
		if operation.Status == target || operation.Status == contracts.OperationStatusRunning {
			if operation.Status == contracts.OperationStatusRunning {
				break
			}
			continue
		}
		if !contracts.CanTransitionOperation(operation.Status, target) {
			break
		}
		transition, transitionErr := s.operations.Transition(ctx, store.OperationTransitionInput{
			WorkspaceID: workspaceID, OperationID: operationID, OwnerID: ownerID, Fence: lease.Fence,
			TaskOwnerID: taskOwnerID, TaskFence: taskFence, Actor: principal.Actor(), RequestID: requestIDFromRequest(r),
			IdempotencyKey: executionKey + ":" + target, CausationID: operation.Request.CausationID, CorrelationID: operation.Request.CorrelationID,
			ToStatus: target, ReasonCode: "connector_execute",
		})
		if transitionErr != nil {
			writeOperationErr(w, transitionErr)
			return
		}
		operation = transition.Operation
	}
	outcome, executeErr := s.connectorExecutor.Execute(ctx, operation.Request, options)
	if executeErr != nil {
		failure := &contracts.OperationFailure{SchemaVersion: contracts.DomainNeutralSchemaVersion, WorkspaceID: workspaceID, Code: contracts.OperationFailureAdapter, Retryable: false, DetailHash: contracts.HashStrings("connector-execution-failure", safeConnectorFailure(executeErr))}
		failed := contracts.OperationResult{ID: operationID + "-result", OperationID: operationID, OperationHash: operation.OperationHash, RequestID: operation.RequestID, WorkspaceID: workspaceID, Actor: principal.Actor(), Status: contracts.OperationStatusFailed, Failure: failure}
		written, writeErr := s.operations.RecordResult(ctx, store.OperationResultInput{WorkspaceID: workspaceID, OperationID: operationID, OwnerID: ownerID, Fence: lease.Fence, TaskOwnerID: taskOwnerID, TaskFence: taskFence, Actor: principal.Actor(), RequestID: requestIDFromRequest(r), IdempotencyKey: executionKey + ":result", CausationID: operation.Request.CausationID, CorrelationID: operation.Request.CorrelationID, Result: failed})
		if writeErr != nil {
			writeOperationErr(w, writeErr)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"operation": publicOperation(written.Operation), "result": publicOperationResult(written.Record), "duplicate": written.Duplicate})
		return
	}
	if outcome.Admission.Definition.Effect != contracts.EffectClassReadOnly && outcome.Admission.Definition.Effect != contracts.EffectClassObservation || len(outcome.Result.ExternalEffects) != 0 {
		writeErr(w, http.StatusConflict, "connector returned an unreserved external effect")
		return
	}
	if operation.PlanHash != "" && operation.PlanHash != outcome.Plan.StableHash() {
		writeErr(w, http.StatusConflict, "connector plan changed during execution")
		return
	}
	written, err := s.operations.RecordResult(ctx, store.OperationResultInput{WorkspaceID: workspaceID, OperationID: operationID, OwnerID: ownerID, Fence: lease.Fence, TaskOwnerID: taskOwnerID, TaskFence: taskFence, Actor: principal.Actor(), RequestID: requestIDFromRequest(r), IdempotencyKey: executionKey + ":result", CausationID: operation.Request.CausationID, CorrelationID: operation.Request.CorrelationID, Result: outcome.Result})
	if err != nil {
		writeOperationErr(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"operation": publicOperation(written.Operation), "result": publicOperationResult(written.Record), "attempts": outcome.Attempts, "duplicate": written.Duplicate})
}

func operationLeaseForRequest(ctx context.Context, operations *store.OperationStore, workspaceID, operationID, ownerID string, fence uint64, taskBound bool) (store.OperationLease, error) {
	if fence != 0 {
		return store.OperationLease{WorkspaceID: workspaceID, OperationID: operationID, OwnerID: ownerID, Fence: fence}, nil
	}
	result, err := operations.AcquireLease(ctx, workspaceID, operationID, ownerID, 90*time.Second)
	if err != nil && taskBound {
		return store.OperationLease{}, err
	}
	if err != nil {
		return store.OperationLease{}, err
	}
	return result.Lease, nil
}

func operationFenceHeader(r *http.Request) uint64 {
	value := strings.TrimSpace(r.Header.Get("X-Operation-Fence"))
	if value == "" {
		return 0
	}
	fence, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0
	}
	return fence
}

func safeConnectorFailure(err error) string {
	var failure *connectorruntime.FailureError
	if errors.As(err, &failure) && failure != nil && strings.TrimSpace(failure.Code) != "" {
		return strings.ToLower(strings.TrimSpace(failure.Code))
	}
	return "adapter_failure"
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

func publicOperationResult(record store.OperationResultRecord) map[string]any {
	return map[string]any{
		"workspace_id": record.WorkspaceID,
		"operation_id": record.OperationID,
		"result_id":    record.ResultID,
		"result_hash":  record.ResultHash,
		"result":       record.Result,
		"created_at":   record.CreatedAt,
	}
}

func publicOperationEffect(effect store.OperationEffect) map[string]any {
	return map[string]any{
		"workspace_id": effect.WorkspaceID, "operation_id": effect.OperationID, "step_id": effect.StepID,
		"attempt_id": effect.AttemptID, "effect_id": effect.EffectID, "effect_class": effect.EffectClass,
		"boundary": effect.Boundary, "idempotency_key": effect.IdempotencyKey,
		"provider_request_id": effect.ProviderRequestID, "provider_idempotency_supported": effect.ProviderIdempotency,
		"delivery_semantics": effect.DeliverySemantics, "verification_status": effect.VerificationStatus,
		"compensation_status": effect.CompensationStatus, "request_hash": effect.RequestHash,
		"response_hash": effect.ResponseHash, "created_at": effect.CreatedAt, "verified_at": effect.VerifiedAt,
	}
}

func publicEffectState(state store.EffectState) map[string]any {
	return map[string]any{
		"workspace_id": state.WorkspaceID, "effect_id": state.EffectID, "operation_id": state.OperationID,
		"state": state.State, "version": state.Version, "provider_request_id": state.ProviderRequestID,
		"response_hash": state.ResponseHash, "verification_hash": state.VerificationHash,
		"compensation_hash": state.CompensationHash, "failure_code": state.FailureCode, "updated_at": state.UpdatedAt,
	}
}

func writeOperationErr(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, store.ErrOperationNotFound):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrOperationWorkspace), errors.Is(err, store.ErrOperationTaskFence):
		status = http.StatusForbidden
	case errors.Is(err, store.ErrOperationIdempotency), errors.Is(err, store.ErrOperationPlanConflict), errors.Is(err, store.ErrOperationResultConflict), errors.Is(err, store.ErrOperationTransition), errors.Is(err, store.ErrOperationLeaseMissing), errors.Is(err, store.ErrOperationLeaseHeld), errors.Is(err, store.ErrOperationLeaseOwned), errors.Is(err, store.ErrOperationLeaseFenced), errors.Is(err, store.ErrOperationLeaseExpired), errors.Is(err, store.ErrOperationLeaseReleased):
		status = http.StatusConflict
	case errors.Is(err, store.ErrOperationReplay):
		status = http.StatusUnprocessableEntity
	}
	writeErr(w, status, err.Error())
}

func writeAdmissionErr(w http.ResponseWriter, err error) {
	status := http.StatusBadRequest
	switch {
	case errors.Is(err, store.ErrAdmissionNotFound), errors.Is(err, pgx.ErrNoRows):
		status = http.StatusNotFound
	case errors.Is(err, store.ErrOperationWorkspace), errors.Is(err, store.ErrOperationTaskFence):
		status = http.StatusForbidden
	case errors.Is(err, store.ErrAdmissionConflict), errors.Is(err, store.ErrAdmissionEffect), errors.Is(err, store.ErrAdmissionEffectTerminal),
		errors.Is(err, store.ErrOperationLeaseMissing), errors.Is(err, store.ErrOperationLeaseHeld), errors.Is(err, store.ErrOperationLeaseOwned),
		errors.Is(err, store.ErrOperationLeaseFenced), errors.Is(err, store.ErrOperationLeaseExpired), errors.Is(err, store.ErrOperationLeaseReleased),
		errors.Is(err, store.ErrOperationIdempotency):
		status = http.StatusConflict
	}
	writeErr(w, status, err.Error())
}
