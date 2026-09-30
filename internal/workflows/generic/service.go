// Package generic exposes the domain-neutral workflow control surface. It
// coordinates durable workflow state, leases, and replay; domain adapters are
// injected through the existing workflow.StepExecutor seam.
package generic

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/effectdispatch"
	"github.com/omaveda/fornix/internal/store"
	workflowruntime "github.com/omaveda/fornix/internal/workflow"
)

const (
	DefaultLeaseTTL = 30 * time.Second
	MaxReplayLimit  = 4096
)

var (
	ErrNotConfigured          = errors.New("generic workflow service is not configured")
	ErrActorRequired          = errors.New("authenticated workflow actor is required")
	ErrEffectAuthorityNeeded  = errors.New("effectful workflow step requires an authority-aware adapter")
	ErrEffectExecutionDenied  = errors.New("generic workflow effect execution is denied")
	ErrApprovalRequired       = errors.New("workflow step is not awaiting approval")
	ErrInvalidReplayArguments = errors.New("invalid workflow replay arguments")
)

// Service is the domain-neutral workflow application service. It deliberately
// owns no domain tables and no provider clients. Postgres-backed stores remain
// the authority; the executor is called only after a committed fenced step
// reservation.
type Service struct {
	Store    *store.WorkflowStore
	Receipts *store.WorkReceiptStore
	Executor workflowruntime.StepExecutor
	Verifier EffectVerifier
	Effects  *effectdispatch.Dispatcher
	LeaseTTL time.Duration
	Now      func() time.Time
}

// NewService creates a generic service with the deterministic offline
// executor. Production deployments may inject a domain adapter executor, but
// effectful work must use an explicit fenced implementation.
func NewService(workflows *store.WorkflowStore, receipts ...*store.WorkReceiptStore) *Service {
	service := &Service{Store: workflows, Executor: DeterministicExecutor{}, LeaseTTL: DefaultLeaseTTL, Now: time.Now}
	if len(receipts) > 0 {
		service.Receipts = receipts[0]
	}
	return service
}

func (s *Service) validate() error {
	if s == nil || s.Store == nil || s.Executor == nil {
		return ErrNotConfigured
	}
	return nil
}

func (s *Service) leaseTTL() time.Duration {
	if s.LeaseTTL <= 0 {
		return DefaultLeaseTTL
	}
	if s.LeaseTTL > 5*time.Minute {
		return 5 * time.Minute
	}
	return s.LeaseTTL
}

// Create persists a workflow and its operation/plan atomically. It is safe to
// retry with the same idempotency key because WorkflowStore owns duplicate
// detection and plan-hash conflict handling.
func (s *Service) Create(ctx context.Context, request contracts.WorkflowCreateRequest) (store.WorkflowCreateResult, error) {
	if err := s.validate(); err != nil {
		return store.WorkflowCreateResult{}, err
	}
	actor := request.Operation.Actor
	if strings.TrimSpace(actor.ID) == "" {
		return store.WorkflowCreateResult{}, ErrActorRequired
	}
	if err := request.Normalize(request.WorkspaceID, actor); err != nil {
		return store.WorkflowCreateResult{}, err
	}
	taskOwner := ""
	if request.Operation.Task != nil {
		taskOwner = actor.ID
	}
	return s.Store.Create(ctx, store.WorkflowCreateInput{
		Operation: store.OperationCreateInput{Request: request.Operation, Plan: &request.Plan, TaskOwnerID: taskOwner, TaskFence: request.TaskFence},
		Budget:    request.Budget,
	})
}

// Get returns the workspace-scoped durable projection.
func (s *Service) Get(ctx context.Context, workspaceID, runID string) (contracts.WorkflowRun, error) {
	if err := s.validate(); err != nil {
		return contracts.WorkflowRun{}, err
	}
	return s.Store.Get(ctx, strings.TrimSpace(workspaceID), strings.TrimSpace(runID))
}

// AcquireLease claims explicit workflow ownership using the existing
// operation lease authority and monotonically increasing fence.
func (s *Service) AcquireLease(ctx context.Context, workspaceID, runID, ownerID string, ttl time.Duration) (store.WorkflowLeaseResult, error) {
	if err := s.validate(); err != nil {
		return store.WorkflowLeaseResult{}, err
	}
	if strings.TrimSpace(ownerID) == "" {
		return store.WorkflowLeaseResult{}, ErrActorRequired
	}
	return s.Store.AcquireLease(ctx, workspaceID, runID, ownerID, ttl)
}

// RenewLease extends a live lease without changing its fence.
func (s *Service) RenewLease(ctx context.Context, lease store.WorkflowLease, ttl time.Duration) (store.WorkflowLease, error) {
	if err := s.validate(); err != nil {
		return store.WorkflowLease{}, err
	}
	return s.Store.RenewLease(ctx, lease, s.boundTTL(ttl))
}

// ReleaseLease relinquishes explicit workflow ownership. It never transfers
// ownership and cannot be used with a stale fence.
func (s *Service) ReleaseLease(ctx context.Context, lease store.WorkflowLease) error {
	if err := s.validate(); err != nil {
		return err
	}
	return s.Store.ReleaseLease(ctx, lease)
}

func (s *Service) boundTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return s.leaseTTL()
	}
	if ttl > 5*time.Minute {
		return 5 * time.Minute
	}
	return ttl
}

func workflowLease(workspaceID, runID, ownerID string, fence uint64) (store.WorkflowLease, error) {
	if strings.TrimSpace(workspaceID) == "" || strings.TrimSpace(runID) == "" || strings.TrimSpace(ownerID) == "" || fence == 0 {
		return store.WorkflowLease{}, store.ErrWorkflowLease
	}
	return store.WorkflowLease{WorkspaceID: workspaceID, OperationID: runID, OwnerID: ownerID, Fence: fence}, nil
}

// Advance runs bounded deterministic transitions under an explicitly acquired
// operation lease and leaves lease ownership with the caller. A task-bound run
// also requires the current task fence; the store validates it transactionally.
func (s *Service) Advance(ctx context.Context, workspaceID, runID string, actor contracts.ActorRef, fence, taskFence uint64) (contracts.WorkflowRun, error) {
	if err := s.validate(); err != nil {
		return contracts.WorkflowRun{}, err
	}
	run, err := s.Store.Get(ctx, workspaceID, runID)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	if actor.ID == "" || actor.WorkspaceID != run.WorkspaceID {
		return contracts.WorkflowRun{}, ErrActorRequired
	}
	lease, err := workflowLease(workspaceID, runID, actor.ID, fence)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	runtime := &workflowruntime.Runtime{Store: s.Store, Executor: s.Executor, Now: s.Now}
	taskOwner := ""
	if run.Task != nil {
		taskOwner = actor.ID
	}
	return runtime.Run(ctx, workflowruntime.RunInput{WorkspaceID: workspaceID, RunID: runID, Lease: lease, TaskOwnerID: taskOwner, TaskFence: taskFence, Actor: actor})
}

// Resume records a canonical approval, callback, human, or external result.
// It performs no external work and remains protected by the workflow lease.
func (s *Service) Resume(ctx context.Context, workspaceID, runID string, actor contracts.ActorRef, fence, taskFence uint64, request contracts.WorkflowResumeRequest) (contracts.WorkflowRun, error) {
	return s.resume(ctx, workspaceID, runID, actor, fence, taskFence, request, false)
}

func (s *Service) resume(ctx context.Context, workspaceID, runID string, actor contracts.ActorRef, fence, taskFence uint64, request contracts.WorkflowResumeRequest, allowVerifiedEffect bool) (contracts.WorkflowRun, error) {
	if err := s.validate(); err != nil {
		return contracts.WorkflowRun{}, err
	}
	if strings.TrimSpace(request.StepID) == "" {
		return contracts.WorkflowRun{}, fmt.Errorf("workflow resume step_id is required")
	}
	run, err := s.Store.Get(ctx, workspaceID, runID)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	if actor.ID == "" || actor.WorkspaceID != run.WorkspaceID {
		return contracts.WorkflowRun{}, ErrActorRequired
	}
	for _, step := range run.Steps {
		if step.StepID != request.StepID {
			continue
		}
		if (step.Status == contracts.WorkflowStepAwaitingExternal || step.Status == contracts.WorkflowStepRecoveryRequired) && !allowVerifiedEffect {
			return contracts.WorkflowRun{}, ErrEffectAuthorityNeeded
		}
		break
	}
	if err := request.Result.Normalize(workspaceID); err != nil {
		return contracts.WorkflowRun{}, err
	}
	lease, err := workflowLease(workspaceID, runID, actor.ID, fence)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	taskOwner := ""
	if run.Task != nil {
		taskOwner = actor.ID
	}
	runtime := &workflowruntime.Runtime{Store: s.Store, Executor: s.Executor, Now: s.Now}
	return runtime.ResumeWait(ctx, workflowruntime.RunInput{WorkspaceID: workspaceID, RunID: runID, Lease: lease, TaskOwnerID: taskOwner, TaskFence: taskFence, Actor: actor}, request.StepID, request.Result)
}

// Approve is narrower than Resume by design. Only an explicit approval wait
// can be resolved here; provider/external waits require their verifier and
// durable effect-state authority.
func (s *Service) Approve(ctx context.Context, workspaceID, runID string, actor contracts.ActorRef, fence, taskFence uint64, stepID string) (contracts.WorkflowRun, error) {
	run, err := s.Get(ctx, workspaceID, runID)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	for _, step := range run.Steps {
		if step.StepID == stepID && step.Status == contracts.WorkflowStepAwaitingApproval {
			return s.Resume(ctx, workspaceID, runID, actor, fence, taskFence, contracts.WorkflowResumeRequest{StepID: stepID, Result: contracts.WorkflowStepResult{Status: contracts.WorkflowStepSucceeded}})
		}
	}
	return contracts.WorkflowRun{}, ErrApprovalRequired
}

// Cancel writes a durable cancellation transition. Cancellation does not
// attempt to interrupt an already-started external call; recovery remains
// explicit and at-least-once at that boundary.
func (s *Service) Cancel(ctx context.Context, workspaceID, runID string, actor contracts.ActorRef, fence, taskFence uint64) (contracts.WorkflowRun, error) {
	if err := s.validate(); err != nil {
		return contracts.WorkflowRun{}, err
	}
	run, err := s.Store.Get(ctx, workspaceID, runID)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	if actor.ID == "" || actor.WorkspaceID != run.WorkspaceID {
		return contracts.WorkflowRun{}, ErrActorRequired
	}
	lease, err := workflowLease(workspaceID, runID, actor.ID, fence)
	if err != nil {
		return contracts.WorkflowRun{}, err
	}
	taskOwner := ""
	if run.Task != nil {
		taskOwner = actor.ID
	}
	runtime := &workflowruntime.Runtime{Store: s.Store, Executor: s.Executor, Now: s.Now}
	return runtime.Cancel(ctx, workflowruntime.RunInput{WorkspaceID: workspaceID, RunID: runID, Lease: lease, TaskOwnerID: taskOwner, TaskFence: taskFence, Actor: actor})
}

// Replay reads the append-only workflow transitions and verifies the stored
// projection hash. It never acquires a lease and never invokes an executor.
func (s *Service) Replay(ctx context.Context, workspaceID, runID string, request contracts.WorkflowReplayRequest) (store.WorkflowReplayResult, error) {
	if err := s.validate(); err != nil {
		return store.WorkflowReplayResult{}, err
	}
	if request.FromVersion < 0 || request.Limit < 0 || request.Limit > MaxReplayLimit {
		return store.WorkflowReplayResult{}, ErrInvalidReplayArguments
	}
	return s.Store.Replay(ctx, workspaceID, runID, request.FromVersion, request.Limit)
}

// FinalizeReceipt creates the generic operation-backed Work Receipt only for
// a terminal successful run whose append-only replay verifies. It intentionally
// does not manufacture evidence or artifact claims; domain adapters must add
// those authoritative references before finalization when they exist.
func (s *Service) FinalizeReceipt(ctx context.Context, workspaceID, runID string, actor contracts.ActorRef) (contracts.WorkReceipt, bool, error) {
	if err := s.validate(); err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	if s.Receipts == nil {
		return contracts.WorkReceipt{}, false, ErrNotConfigured
	}
	run, err := s.Store.Get(ctx, workspaceID, runID)
	if err != nil {
		return contracts.WorkReceipt{}, false, err
	}
	if run.Status != contracts.WorkflowStatusSucceeded {
		return contracts.WorkReceipt{}, false, fmt.Errorf("workflow receipt requires succeeded workflow")
	}
	replay, err := s.Store.Replay(ctx, workspaceID, runID, 0, MaxReplayLimit)
	if err != nil || !replay.Verified {
		if err == nil {
			err = store.ErrWorkflowReplay
		}
		return contracts.WorkReceipt{}, false, fmt.Errorf("workflow receipt replay is not verified: %w", err)
	}
	steps := make([]contracts.WorkReceiptStep, 0, len(run.Steps))
	for _, state := range run.Steps {
		step := contracts.WorkReceiptStep{
			Ordinal: state.Ordinal, ID: state.StepID, Name: state.Kind, Kind: state.Kind,
			Status: state.Status, SourceKind: "workflow", SourceID: run.ID,
			InputHash: contracts.HashStrings(run.ID, state.StepID), OutputHash: state.OutputHash,
			Attempts: state.Attempt,
		}
		steps = append(steps, step)
	}
	return s.Receipts.Finalize(ctx, contracts.WorkReceiptFinalizeRequest{
		ReceiptID: "receipt-" + run.ID, RequestID: "receipt-request-" + run.ID,
		IdempotencyKey: "generic-workflow-receipt-" + run.ID, WorkspaceID: workspaceID,
		Actor: actor, WorkKind: contracts.WorkReceiptReferenceOperation, WorkID: run.ID,
		Operation: &run.Operation, ReplayHash: replay.ReplayHash, Steps: steps,
		Cost: contracts.WorkReceiptCost{Measured: false, Estimated: false, UnknownProviderUsage: true},
	})
}
