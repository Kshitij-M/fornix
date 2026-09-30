package effectdispatch

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

// ChildRequest describes one dynamic domain effect. It is deliberately
// reference-only: domain adapters supply hashes and a bounded invoker, while
// Runtime owns child-operation identity, lease acquisition, admission, and
// dispatch ordering.
type ChildRequest struct {
	WorkspaceID     string
	ParentID        string
	DomainKind      string
	DomainID        string
	DomainHash      string
	LinkRole        string
	RequestID       string
	IdempotencyKey  string
	CausationID     string
	CorrelationID   string
	Actor           contracts.ActorRef
	Task            *contracts.EntityRef
	Session         *contracts.EntityRef
	TaskOwnerID     string
	TaskFence       uint64
	AgentRunID      string
	AgentRunOwnerID string
	AgentRunFence   uint64
	OwnerID         string
	LeaseTTL        time.Duration
	Capability      contracts.CapabilityDefinition
	Target          contracts.ResourceRef
	InputType       string
	InputHash       string
	Policy          contracts.AdmissionPolicy
	Effect          contracts.ExternalEffect
	Authority       contracts.EffectAuthority
	// ExternalBoundary is the exact redacted egress envelope selected by the
	// provider or connector. Local/fake paths may leave it nil.
	ExternalBoundary *contracts.ExternalBoundaryAuthority
	// DeploymentAdmission is a hash-only reference to the currently qualified
	// deployment. Strict production reservations revalidate it in Postgres;
	// the reference is never a bearer token or a substitute for admission.
	DeploymentAdmission *contracts.DeploymentAdmissionReference
	Invoker             Invoker
	FinalizeTx          TransactionFinalizer
	Metadata            map[string]string
}

// Runtime composes child operation lifecycle with the common effect
// dispatcher. It is the production composition seam for model calls, tools,
// repository mutations, and dynamic agent steps. It does not execute a
// provider or process itself.
type Runtime struct {
	Operations *store.OperationStore
	Admission  *store.AdmissionStore
	Links      *store.DomainEffectLinkStore
	LeaseTTL   time.Duration
}

// Run creates or resumes one deterministic child operation and dispatches its
// external effect. A duplicate child operation reuses its durable lease and
// effect identity; it never silently creates a second local reservation.
func (r *Runtime) Run(ctx context.Context, request ChildRequest) (Result, error) {
	if r == nil || r.Operations == nil || r.Admission == nil || r.Links == nil {
		return Result{}, ErrNotConfigured
	}
	if request.Invoker == nil {
		return Result{}, fmt.Errorf("child effect invoker is required")
	}
	if err := normalizeChildRequest(&request); err != nil {
		return Result{}, err
	}
	operationRequest, definition, plan, err := childOperation(request)
	if err != nil {
		return Result{}, err
	}
	created, err := r.Operations.Create(ctx, store.OperationCreateInput{
		Request: operationRequest, Plan: &plan,
		TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence,
	})
	if err != nil {
		return Result{}, fmt.Errorf("create child operation: %w", err)
	}
	operation := created.Operation
	if contracts.IsTerminalOperationStatus(operation.Status) {
		// The owning specialized ledger is responsible for replaying its
		// terminal result. The generic child identity is already terminal, so
		// never attempt to reacquire authority or invoke an external boundary.
		return Result{Duplicate: true}, nil
	}
	if operation.Status == contracts.OperationStatusRecoveryRequired {
		// Recovery is an explicit operator/domain decision. Re-opening the
		// operation here would turn an unknown external outcome into a blind
		// duplicate. A future reconciler must first resolve the authoritative
		// effect state and then issue a separate fenced transition.
		return Result{Duplicate: true}, ErrUncertainOutcome
	}
	ttl := request.LeaseTTL
	if ttl <= 0 {
		ttl = r.LeaseTTL
	}
	if ttl <= 0 {
		ttl = 2 * time.Minute
	}
	lease, err := r.Operations.AcquireLease(ctx, request.WorkspaceID, operation.ID, request.OwnerID, ttl)
	if err != nil {
		return Result{}, fmt.Errorf("acquire child operation lease: %w", err)
	}
	if operation.Status == contracts.OperationStatusCreated {
		planned, planErr := r.Operations.AttachPlan(ctx, store.OperationPlanInput{
			WorkspaceID: request.WorkspaceID, OperationID: operation.ID,
			OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence,
			TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence,
			Actor: request.Actor, RequestID: request.RequestID,
			IdempotencyKey: operationRequest.IdempotencyKey + ":plan",
			CausationID:    request.CausationID, CorrelationID: request.CorrelationID,
			Plan: plan,
		})
		if planErr != nil {
			return Result{}, fmt.Errorf("attach child plan: %w", planErr)
		}
		operation = planned.Operation
	}
	for _, status := range []string{contracts.OperationStatusAdmitted, contracts.OperationStatusRunning} {
		if operation.Status != contracts.OperationStatusPlanned && status == contracts.OperationStatusAdmitted {
			continue
		}
		if operation.Status != status {
			transition, transitionErr := r.Operations.Transition(ctx, store.OperationTransitionInput{
				WorkspaceID: request.WorkspaceID, OperationID: operation.ID,
				OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence,
				TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence,
				Actor: request.Actor, RequestID: request.RequestID,
				IdempotencyKey: operationRequest.IdempotencyKey + ":" + status,
				CausationID:    request.CausationID, CorrelationID: request.CorrelationID,
				ToStatus: status, ReasonCode: "child_effect_prepare",
			})
			if transitionErr != nil {
				return Result{}, fmt.Errorf("advance child operation to %s: %w", status, transitionErr)
			}
			operation = transition.Operation
		}
	}
	dispatcher := &Dispatcher{Operations: r.Operations, Admission: r.Admission, Links: r.Links}
	effect := request.Effect
	effect.ID = ""
	effect.IdempotencyKey = request.IdempotencyKey + ":effect"
	effect.WorkspaceID = request.WorkspaceID
	effect.Class = definition.Effect
	effect.Boundary = strings.TrimSpace(effect.Boundary)
	effect.DeliveryGuarantee = contracts.ExternalDeliveryAtLeastOnce
	domainLink := &contracts.DomainEffectLink{
		WorkspaceID: request.WorkspaceID, DomainKind: request.DomainKind,
		DomainID: request.DomainID, DomainHash: request.DomainHash,
		LinkRole: request.LinkRole, IdempotencyKey: request.IdempotencyKey + ":link",
		Actor: request.Actor, RequestID: request.RequestID,
		CausationID: request.CausationID, CorrelationID: request.CorrelationID,
		Metadata: request.Metadata,
	}
	admission := contracts.AdmissionInput{
		WorkspaceID: request.WorkspaceID, OperationID: operation.ID,
		OperationHash: operation.OperationHash, RequestID: request.RequestID,
		IdempotencyKey: request.IdempotencyKey + ":admission", Actor: request.Actor,
		Capability: definition, Target: request.Target, Policy: request.Policy,
		ConnectorAvailable: true, ResourceAllowed: true, EvidenceSatisfied: true,
		TaskBound: request.Task != nil, TaskOwnerID: request.TaskOwnerID,
		TaskFence: request.TaskFence, TaskFenceValid: request.Task == nil,
		ExternalBoundary:    contracts.CloneExternalBoundary(request.ExternalBoundary),
		RequestedCostMicros: 1,
	}
	authority := request.Authority
	authority.AgentRunID = request.AgentRunID
	authority.AgentRunOwnerID = request.AgentRunOwnerID
	authority.AgentRunFence = request.AgentRunFence
	if authority.ExternalBoundary == nil {
		authority.ExternalBoundary = contracts.CloneExternalBoundary(request.ExternalBoundary)
	}
	return dispatcher.Dispatch(ctx, Request{
		Admission: admission, Operation: operationRequest,
		Definition: definition, OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence,
		TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence,
		StepID: plan.Steps[0].ID, Attempt: 1,
		AttemptKey: request.IdempotencyKey + ":attempt", Effect: effect, Authority: authority,
		DomainLink: domainLink, Invoker: request.Invoker, RecordResult: true, FinalizeTx: request.FinalizeTx,
	})
}

func normalizeChildRequest(request *ChildRequest) error {
	request.WorkspaceID = strings.TrimSpace(request.WorkspaceID)
	request.ParentID = strings.TrimSpace(request.ParentID)
	request.DomainKind = strings.TrimSpace(request.DomainKind)
	request.DomainID = strings.TrimSpace(request.DomainID)
	request.RequestID = strings.TrimSpace(request.RequestID)
	request.IdempotencyKey = strings.TrimSpace(request.IdempotencyKey)
	request.InputType = strings.TrimSpace(request.InputType)
	request.InputHash = strings.ToLower(strings.TrimSpace(request.InputHash))
	if request.WorkspaceID == "" || request.DomainKind == "" || request.DomainID == "" || request.IdempotencyKey == "" || request.InputHash == "" {
		return fmt.Errorf("child effect workspace, domain, idempotency, and input hash are required")
	}
	if request.RequestID == "" {
		request.RequestID = "child-request-" + contracts.HashStrings(request.WorkspaceID, request.DomainKind, request.DomainID, request.IdempotencyKey)[:40]
	}
	if request.InputType == "" {
		request.InputType = "fornix." + request.DomainKind
	}
	if request.Actor.WorkspaceID == "" {
		request.Actor.WorkspaceID = request.WorkspaceID
	}
	if request.DeploymentAdmission != nil {
		if err := request.DeploymentAdmission.Normalize(); err != nil {
			return fmt.Errorf("normalize child deployment admission: %w", err)
		}
		if request.DeploymentAdmission.WorkspaceID != request.WorkspaceID {
			return fmt.Errorf("child deployment admission crosses workspace boundary")
		}
	}
	request.ExternalBoundary = contracts.CloneExternalBoundary(request.ExternalBoundary)
	if request.ExternalBoundary != nil {
		if err := request.ExternalBoundary.Normalize(); err != nil {
			return fmt.Errorf("normalize child external boundary: %w", err)
		}
	}
	if request.Task != nil && (request.TaskOwnerID == "" || request.TaskFence == 0) {
		return store.ErrOperationTaskFence
	}
	if request.Task == nil && (request.TaskOwnerID != "" || request.TaskFence != 0) {
		return store.ErrOperationTaskFence
	}
	request.AgentRunID = strings.TrimSpace(request.AgentRunID)
	request.AgentRunOwnerID = strings.TrimSpace(request.AgentRunOwnerID)
	if request.AgentRunID == "" && (request.AgentRunOwnerID != "" || request.AgentRunFence != 0) || request.AgentRunID != "" && (request.AgentRunOwnerID == "" || request.AgentRunFence == 0) {
		return fmt.Errorf("child effect agent-run ID, owner, and fence must be supplied together")
	}
	if request.AgentRunID != "" && request.AgentRunFence > uint64(1<<63-1) {
		return fmt.Errorf("child effect agent-run fence exceeds database range")
	}
	if request.Authority.AgentRunID != "" || request.Authority.AgentRunOwnerID != "" || request.Authority.AgentRunFence != 0 {
		if request.Authority.AgentRunID != request.AgentRunID || request.Authority.AgentRunOwnerID != request.AgentRunOwnerID || request.Authority.AgentRunFence != request.AgentRunFence {
			return fmt.Errorf("child effect agent-run authority conflicts with request")
		}
	}
	if request.AgentRunID != "" {
		candidate := contracts.EffectAuthority{
			WorkspaceID: request.WorkspaceID, OperationID: "child-validation",
			OperationOwnerID: "child-validation", OperationFence: 1,
			AgentRunID: request.AgentRunID, AgentRunOwnerID: request.AgentRunOwnerID, AgentRunFence: request.AgentRunFence,
		}
		if err := candidate.Normalize(); err != nil {
			return fmt.Errorf("normalize child agent-run authority: %w", err)
		}
		request.AgentRunID, request.AgentRunOwnerID = candidate.AgentRunID, candidate.AgentRunOwnerID
	}
	if request.OwnerID == "" {
		request.OwnerID = "child-owner-" + contracts.HashStrings(request.WorkspaceID, request.IdempotencyKey)[:40]
	}
	if request.Task != nil {
		request.OwnerID = request.TaskOwnerID
	}
	if request.LinkRole == "" {
		request.LinkRole = contracts.DomainEffectLinkRolePrimary
	}
	if err := request.Capability.Normalize(); err != nil {
		return fmt.Errorf("normalize child capability: %w", err)
	}
	if request.Capability.WorkspaceID != request.WorkspaceID {
		return fmt.Errorf("child capability crosses workspace boundary")
	}
	if err := request.Target.Normalize(); err != nil {
		return fmt.Errorf("normalize child target: %w", err)
	}
	if request.Target.WorkspaceID != request.WorkspaceID {
		return fmt.Errorf("child target crosses workspace boundary")
	}
	request.Actor.ID = strings.TrimSpace(request.Actor.ID)
	request.Actor.Kind = strings.TrimSpace(request.Actor.Kind)
	request.Actor.Name = strings.TrimSpace(request.Actor.Name)
	if request.Actor.ID == "" || request.Actor.WorkspaceID != request.WorkspaceID {
		return fmt.Errorf("child actor is missing or crosses workspace boundary")
	}
	if request.Actor.WorkspaceID != request.WorkspaceID {
		return fmt.Errorf("child actor crosses workspace boundary")
	}
	if err := request.Policy.Normalize(); err != nil {
		return fmt.Errorf("normalize child policy: %w", err)
	}
	return nil
}

func childOperation(request ChildRequest) (contracts.OperationRequest, contracts.CapabilityDefinition, contracts.OperationPlan, error) {
	definition := request.Capability
	inputHash := request.InputHash
	operationKey := "child-operation:" + contracts.HashStrings(request.WorkspaceID, request.DomainKind, request.DomainID, request.IdempotencyKey)[:48]
	operationID := "child-op-" + contracts.HashStrings(request.WorkspaceID, request.DomainKind, request.DomainID, request.IdempotencyKey)[:40]
	requestID := request.RequestID
	operation := contracts.OperationRequest{
		SchemaVersion: contracts.DomainNeutralSchemaVersion, ID: operationID, RequestID: requestID,
		IdempotencyKey: operationKey, CausationID: request.CausationID, CorrelationID: request.CorrelationID,
		WorkspaceID: request.WorkspaceID, Actor: request.Actor, Task: request.Task, Session: request.Session,
		Capability: definition.Ref, Target: request.Target, InputType: request.InputType,
		InputSchemaVersion: contracts.DomainNeutralSchemaVersion, InputSchemaHash: contracts.HashStrings("fornix.child.input", request.DomainKind), InputHash: inputHash,
		Profile: definition.Profile, Metadata: request.Metadata, DeploymentAdmission: request.DeploymentAdmission,
	}
	if err := operation.Normalize(); err != nil {
		return contracts.OperationRequest{}, contracts.CapabilityDefinition{}, contracts.OperationPlan{}, fmt.Errorf("normalize child operation: %w", err)
	}
	step := contracts.OperationStep{ID: "child-step-" + contracts.HashStrings(operation.ID)[:32], Ordinal: 0, Kind: request.DomainKind, Capability: definition.Ref, Target: request.Target, Effect: definition.Effect, Profile: definition.Profile, InputHash: inputHash}
	plan := contracts.OperationPlan{SchemaVersion: contracts.DomainNeutralSchemaVersion, ID: operation.ID + "-plan", OperationID: operation.ID, OperationHash: operation.StableHash(), WorkspaceID: request.WorkspaceID, Actor: request.Actor, Steps: []contracts.OperationStep{step}}
	if err := plan.Normalize(); err != nil {
		return contracts.OperationRequest{}, contracts.CapabilityDefinition{}, contracts.OperationPlan{}, fmt.Errorf("normalize child plan: %w", err)
	}
	return operation, definition, plan, nil
}
