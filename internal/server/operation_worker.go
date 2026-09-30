package server

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"time"

	connectorruntime "github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/operationsupervisor"
	"github.com/omaveda/fornix/internal/operationworker"
	"github.com/omaveda/fornix/internal/store"
)

// serverOperationHandler is deliberately limited to read-only and observation
// plans. Effectful operations remain with the durable effect dispatcher and
// connector-owned worker because a generic server worker cannot infer provider
// verification or compensation semantics.
type serverOperationHandler struct {
	server *server
}

func (h *serverOperationHandler) Handle(ctx context.Context, claim store.OperationClaim) error {
	if h == nil || h.server == nil || h.server.operations == nil || h.server.admission == nil || h.server.connectorRegistry == nil || h.server.connectorExecutor == nil {
		return errors.New("server operation worker is not configured")
	}
	if ctx == nil {
		return errors.New("server operation worker context is nil")
	}
	operation := claim.Operation
	if operation.Plan == nil || !readOnlyOperationPlan(*operation.Plan) || operation.PlanHash == "" || operation.Plan.StableHash() != operation.PlanHash {
		return fmt.Errorf("operation worker requires a valid read-only plan")
	}
	principal, err := h.server.backgroundOperationPrincipal(ctx, operation.Request)
	if err != nil {
		return err
	}
	_, durableAdmission, admissionErr := h.server.admitPersistedOperation(ctx, operation, principal, operation.RequestID)
	if admissionErr != nil {
		if err := h.server.advanceOperationToRunning(ctx, claim, principal.Actor()); err != nil {
			return err
		}
		return h.server.recordBackgroundOperationFailure(ctx, claim, principal.Actor(), admissionErr)
	}
	if durableAdmission.Decision.Status != contracts.AdmissionAllowed {
		if err := h.server.advanceOperationToRunning(ctx, claim, principal.Actor()); err != nil {
			return err
		}
		return h.server.recordBackgroundOperationFailure(ctx, claim, principal.Actor(), fmt.Errorf("operation admission denied: %s", durableAdmission.Decision.ReasonCode))
	}
	if err := h.server.advanceOperationToRunning(ctx, claim, principal.Actor()); err != nil {
		return err
	}
	options := connectorruntime.AdmissionOptions{Principal: &principal, Authorize: func(_ context.Context, value contracts.Principal, request contracts.OperationRequest, _ contracts.CapabilityDefinition) (bool, error) {
		return value.WorkspaceID == request.WorkspaceID && value.ID == request.Actor.ID && value.Has(contracts.PermissionOperationExecute), nil
	}}
	outcome, executeErr := h.server.connectorExecutor.Execute(ctx, operation.Request, options)
	if executeErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return h.server.recordBackgroundOperationFailure(ctx, claim, principal.Actor(), executeErr)
	}
	if outcome.Admission.Definition.Effect != contracts.EffectClassReadOnly && outcome.Admission.Definition.Effect != contracts.EffectClassObservation {
		return h.server.recordBackgroundOperationFailure(ctx, claim, principal.Actor(), errors.New("unexpected effect class"))
	}
	if len(outcome.Result.ExternalEffects) != 0 || outcome.Plan.StableHash() != operation.PlanHash {
		return h.server.recordBackgroundOperationFailure(ctx, claim, principal.Actor(), errors.New("unreserved or changed operation result"))
	}
	outcome.Result.OperationHash = operation.OperationHash
	if _, err := h.server.operations.RecordResult(ctx, store.OperationResultInput{
		WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, OwnerID: claim.Lease.OwnerID,
		Fence: claim.Lease.Fence, TaskOwnerID: operation.TaskOwnerID, TaskFence: operation.TaskFence,
		Actor: principal.Actor(), RequestID: operation.RequestID,
		IdempotencyKey: "operation-worker:" + operation.ID + ":result",
		CausationID:    operation.Request.CausationID, CorrelationID: operation.Request.CorrelationID,
		SchemaCatalogHash: outcome.Admission.SchemaCatalogHash, SchemaCatalogRevision: outcome.Admission.SchemaCatalogRevision,
		Result: outcome.Result,
	}); err != nil {
		return err
	}
	return nil
}

func readOnlyOperationPlan(plan contracts.OperationPlan) bool {
	if len(plan.Steps) == 0 {
		return false
	}
	for _, step := range plan.Steps {
		if step.Effect != contracts.EffectClassReadOnly && step.Effect != contracts.EffectClassObservation {
			return false
		}
	}
	return true
}

func (s *server) backgroundOperationPrincipal(ctx context.Context, request contracts.OperationRequest) (contracts.Principal, error) {
	if s == nil {
		return contracts.Principal{}, errors.New("server is not configured")
	}
	if request.Actor.WorkspaceID != request.WorkspaceID || strings.TrimSpace(request.Actor.ID) == "" {
		return contracts.Principal{}, store.ErrWorkspaceViolation
	}
	if s.authMode == "development" && request.Actor.ID == "development" {
		return contracts.Principal{ID: "development", WorkspaceID: request.WorkspaceID, Kind: request.Actor.Kind, DisplayName: request.Actor.Name, Permissions: []contracts.Permission{contracts.AdminWildcard}, Authenticated: true, Development: true}, nil
	}
	if s.auth == nil {
		return contracts.Principal{}, store.ErrUnauthenticated
	}
	principal, err := s.auth.PrincipalForIdentity(ctx, request.WorkspaceID, request.Actor.ID)
	if err != nil {
		return contracts.Principal{}, err
	}
	if principal.Actor() != request.Actor {
		return contracts.Principal{}, store.ErrWorkspaceViolation
	}
	return principal, nil
}

func (s *server) advanceOperationToRunning(ctx context.Context, claim store.OperationClaim, actor contracts.ActorRef) error {
	operation := claim.Operation
	for operation.Status != contracts.OperationStatusRunning {
		target := ""
		switch operation.Status {
		case contracts.OperationStatusCreated:
			if operation.Plan == nil {
				return fmt.Errorf("operation worker cannot plan an operation without a persisted plan")
			}
			planned, err := s.operations.AttachPlan(ctx, store.OperationPlanInput{
				WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, OwnerID: claim.Lease.OwnerID, Fence: claim.Lease.Fence,
				TaskOwnerID: operation.TaskOwnerID, TaskFence: operation.TaskFence, Actor: actor, RequestID: operation.RequestID,
				IdempotencyKey: "operation-worker:" + operation.ID + ":plan",
				CausationID:    operation.Request.CausationID, CorrelationID: operation.Request.CorrelationID, Plan: *operation.Plan,
			})
			if err != nil {
				return err
			}
			operation = planned.Operation
			continue
		case contracts.OperationStatusPlanned:
			target = contracts.OperationStatusAdmitted
		case contracts.OperationStatusAdmitted, contracts.OperationStatusAwaitingRetry, contracts.OperationStatusRecoveryRequired:
			target = contracts.OperationStatusRunning
		default:
			return fmt.Errorf("operation worker cannot run operation in status %q", operation.Status)
		}
		transition, err := s.operations.Transition(ctx, store.OperationTransitionInput{
			WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, OwnerID: claim.Lease.OwnerID, Fence: claim.Lease.Fence,
			TaskOwnerID: operation.TaskOwnerID, TaskFence: operation.TaskFence, Actor: actor, RequestID: operation.RequestID,
			IdempotencyKey: "operation-worker:" + operation.ID + ":transition:" + target,
			CausationID:    operation.Request.CausationID, CorrelationID: operation.Request.CorrelationID,
			ToStatus: target, ReasonCode: "server_read_only_worker",
		})
		if err != nil {
			return err
		}
		operation = transition.Operation
	}
	return nil
}

func (s *server) recordBackgroundOperationFailure(ctx context.Context, claim store.OperationClaim, actor contracts.ActorRef, cause error) error {
	operation := claim.Operation
	code := contracts.OperationFailureAdapter
	if errors.Is(cause, context.DeadlineExceeded) {
		code = contracts.OperationFailureTimeout
	}
	result := contracts.OperationResult{
		ID: operation.ID + "-worker-result", OperationID: operation.ID, OperationHash: operation.OperationHash,
		RequestID: operation.RequestID, WorkspaceID: operation.WorkspaceID, Actor: actor,
		Status:  contracts.OperationStatusFailed,
		Failure: &contracts.OperationFailure{WorkspaceID: operation.WorkspaceID, Code: code, DetailHash: contracts.HashStrings("server-operation-worker", workerFailureCode(cause))},
	}
	_, err := s.operations.RecordResult(ctx, store.OperationResultInput{
		WorkspaceID: operation.WorkspaceID, OperationID: operation.ID, OwnerID: claim.Lease.OwnerID,
		Fence: claim.Lease.Fence, TaskOwnerID: operation.TaskOwnerID, TaskFence: operation.TaskFence,
		Actor: actor, RequestID: operation.RequestID, IdempotencyKey: "operation-worker:" + operation.ID + ":failure",
		CausationID: operation.Request.CausationID, CorrelationID: operation.Request.CorrelationID, Result: result,
	})
	return err
}

func workerFailureCode(err error) string {
	var failure *connectorruntime.FailureError
	if errors.As(err, &failure) && failure != nil && strings.TrimSpace(failure.Code) != "" {
		return strings.ToLower(strings.TrimSpace(failure.Code))
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return contracts.OperationFailureTimeout
	}
	if errors.Is(err, context.Canceled) {
		return contracts.OperationFailureCancelled
	}
	return contracts.OperationFailureAdapter
}

func (s *server) newOperationWorker(_ string) (operationsupervisor.Runner, error) {
	if s == nil || s.operations == nil {
		return nil, errors.New("operation worker store is not configured")
	}
	worker := operationworker.New(s.operations, &serverOperationHandler{server: s}, contracts.NewID("server-operation-worker"))
	worker.Limit = s.operationWorkerClaimBatch
	worker.MaxActive = s.operationWorkerMaxActive
	worker.ReadOnlyOnly = true
	return worker, nil
}

func (s *server) activeOperationWorkspaces(ctx context.Context) ([]string, error) {
	if s == nil || s.operator == nil {
		return nil, errors.New("operation worker workspace authority is not configured")
	}
	const pageSize = 256
	const maxWorkspaces = 4096
	workspaceIDs := make([]string, 0, pageSize)
	cursor := ""
	for len(workspaceIDs) < maxWorkspaces {
		page, err := s.operator.ListWorkspaces(ctx, pageSize, cursor)
		if err != nil {
			return nil, err
		}
		for _, workspace := range page.Items {
			if workspace.Status == "active" {
				workspaceIDs = append(workspaceIDs, workspace.ID)
			}
		}
		if page.NextCursor == "" {
			return workspaceIDs, nil
		}
		cursor = page.NextCursor
	}
	return nil, errors.New("operation worker workspace inventory exceeds 4096 entries")
}

func (s *server) runOperationWorker(ctx context.Context) error {
	if s == nil || s.operationWorkerPollInterval <= 0 {
		return errors.New("operation worker is not configured")
	}
	supervisor, err := operationsupervisor.New([]string{contracts.DefaultWorkspaceID}, s.newOperationWorker)
	if err != nil {
		return err
	}
	supervisor.MaxConcurrent = s.operationWorkerMaxConcurrent
	supervisor.PollInterval = s.operationWorkerPollInterval
	supervisor.OnWorkerError = func(result operationsupervisor.WorkspaceResult) {
		if result.Err != nil {
			log.Printf("generic operation worker delivery failed workspace=%s code=%s", result.WorkspaceID, workerFailureCode(result.Err))
		}
	}
	for {
		workspaceIDs, inventoryErr := s.activeOperationWorkspaces(ctx)
		if inventoryErr != nil {
			_ = supervisor.ReplaceWorkspaces(nil)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("generic operation worker inventory unavailable code=%s", workerFailureCode(inventoryErr))
		} else {
			if err := supervisor.ReplaceWorkspaces(workspaceIDs); err != nil {
				return err
			}
			if len(workspaceIDs) > 0 {
				if _, stepErr := supervisor.Step(ctx); stepErr != nil && ctx.Err() != nil {
					return ctx.Err()
				}
			}
		}
		timer := time.NewTimer(s.operationWorkerPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

type sandboxCleanupSupervisorRunner struct {
	worker *operationworker.SandboxCleanupWorker
}

func (r sandboxCleanupSupervisorRunner) RunOnce(ctx context.Context, workspaceID string) (operationworker.BatchResult, error) {
	if r.worker == nil {
		return operationworker.BatchResult{}, operationworker.ErrSandboxCleanupWorkerNotConfigured
	}
	result, err := r.worker.RunOnce(ctx, workspaceID)
	return operationworker.BatchResult{
		Claims: result.Claims, Handled: result.Completed, Failed: result.Failed + result.Retried,
	}, err
}

// runSandboxCleanupWorker consumes only the durable cleanup queue. It is
// enabled only when a deployment injects the authenticated host-runner
// transport; no Engine endpoint or local process is inferred here.
func (s *server) runSandboxCleanupWorker(ctx context.Context) error {
	if s == nil || s.sandboxCleanupWorker == nil || s.operationWorkerPollInterval <= 0 {
		return operationworker.ErrSandboxCleanupWorkerNotConfigured
	}
	supervisor, err := operationsupervisor.New([]string{contracts.DefaultWorkspaceID}, func(string) (operationsupervisor.Runner, error) {
		return sandboxCleanupSupervisorRunner{worker: s.sandboxCleanupWorker}, nil
	})
	if err != nil {
		return err
	}
	supervisor.MaxConcurrent = s.operationWorkerMaxConcurrent
	supervisor.PollInterval = s.operationWorkerPollInterval
	supervisor.OnWorkerError = func(result operationsupervisor.WorkspaceResult) {
		if result.Err != nil {
			log.Printf("sandbox cleanup delivery failed workspace=%s code=%s", result.WorkspaceID, workerFailureCode(result.Err))
		}
	}
	for {
		workspaceIDs, inventoryErr := s.activeOperationWorkspaces(ctx)
		if inventoryErr != nil {
			_ = supervisor.ReplaceWorkspaces(nil)
			if ctx.Err() != nil {
				return ctx.Err()
			}
			log.Printf("sandbox cleanup workspace inventory unavailable code=%s", workerFailureCode(inventoryErr))
		} else {
			if err := supervisor.ReplaceWorkspaces(workspaceIDs); err != nil {
				return err
			}
			if len(workspaceIDs) > 0 {
				if _, stepErr := supervisor.Step(ctx); stepErr != nil && ctx.Err() != nil {
					return ctx.Err()
				}
			}
		}
		timer := time.NewTimer(s.operationWorkerPollInterval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
