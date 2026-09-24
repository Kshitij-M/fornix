// Package incident demonstrates that the Fornix control plane is useful
// outside repository maintenance. It is a bounded fake-first incident
// investigation and approval-gated remediation workflow.
package incident

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/adapters/fakeincident"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/model"
	"github.com/omaveda/fornix/internal/store"
	workflowruntime "github.com/omaveda/fornix/internal/workflow"
)

const (
	workerLeaseTTL = 30 * time.Second
	workerPrefix   = "incident-worker-"
	approvalStep   = "approval"
	reportStep     = "report"
)

var (
	ErrIncidentApprovalRequired = errors.New("incident remediation approval is required")
	ErrIncidentAlreadyTerminal  = errors.New("incident workflow is terminal")
	ErrIncidentApprovalConflict = errors.New("incident approval does not match the waiting workflow")
)

// Service composes existing durable authorities. It intentionally owns no
// second scheduler or lease table; operation-linked workflow leases remain
// the only worker ownership mechanism.
type Service struct {
	Incidents *store.IncidentStore
	Workflows *store.WorkflowStore
	Evidence  *store.EvidenceStore
	Artifacts *store.ArtifactStore
	Receipts  *store.WorkReceiptStore
	Registry  *connector.Registry
	Model     *model.Gateway
}

// NewService creates the fake-first universal incident workflow service.
func NewService(incidents *store.IncidentStore, workflows *store.WorkflowStore, evidence *store.EvidenceStore, artifacts *store.ArtifactStore, receipts *store.WorkReceiptStore, registry *connector.Registry, gateway *model.Gateway) *Service {
	return &Service{Incidents: incidents, Workflows: workflows, Evidence: evidence, Artifacts: artifacts, Receipts: receipts, Registry: registry, Model: gateway}
}

// Start creates or resumes the deterministic incident workflow. The initial
// call runs only until approval or terminal completion.
func (s *Service) Start(ctx context.Context, request contracts.IncidentWorkflowRequest) (contracts.IncidentWorkflowResult, error) {
	if err := s.validate(); err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	if request.Event.Actor.ID == "" {
		request.Event.Actor = request.Actor
	}
	if request.Event.WorkspaceID == "" {
		request.Event.WorkspaceID = request.Actor.WorkspaceID
	}
	if request.Event.RequestID == "" {
		request.Event.RequestID = request.RequestID
	}
	if request.Event.IdempotencyKey == "" {
		request.Event.IdempotencyKey = request.IdempotencyKey
	}
	if err := request.Event.Normalize(); err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	if request.Actor.ID == "" {
		request.Actor = request.Event.Actor
	}
	if request.Actor.WorkspaceID == "" {
		request.Actor.WorkspaceID = request.Event.WorkspaceID
	}
	if request.Actor.WorkspaceID != request.Event.WorkspaceID {
		return contracts.IncidentWorkflowResult{}, store.ErrIncidentWorkspace
	}

	ingested, err := s.Incidents.Ingest(ctx, request.Event)
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	runID := strings.TrimSpace(request.RunID)
	if runID == "" {
		runID = "incident-run-" + ingested.Incident.ID
	}
	plan, operation, budget, err := s.plan(ingested.Incident, runID, request.Actor)
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	created, err := s.Workflows.Create(ctx, store.WorkflowCreateInput{Operation: store.OperationCreateInput{Request: operation, Plan: &plan}, Budget: budget})
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	if err := s.Incidents.LinkWorkflow(ctx, ingested.Incident.WorkspaceID, ingested.Incident.ID, created.Run.ID, workflowIncidentStatus(created.Run.Status)); err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	result, err := s.advance(ctx, ingested.Incident, created.Run, request.Actor)
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	result.Duplicate = ingested.Duplicate || created.Duplicate
	return result, nil
}

// Approve records a durable approval/rejection bound to the exact waiting
// plan, then resumes the workflow. A rejection is terminal and cannot unlock
// remediation.
func (s *Service) Approve(ctx context.Context, workspaceID, runID, decision, idempotencyKey string, actor contracts.ActorRef) (contracts.IncidentWorkflowResult, error) {
	if err := s.validate(); err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	run, err := s.Workflows.Get(ctx, workspaceID, runID)
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	if strings.TrimSpace(idempotencyKey) == "" {
		idempotencyKey = contracts.HashStrings("incident-approval", run.ID, decision)[:48]
	}
	incident, err := s.Incidents.Get(ctx, workspaceID, incidentIDForRun(run))
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	waiting, ok := workflowApprovalStep(run)
	if !ok || run.Status != contracts.WorkflowStatusAwaitingApproval {
		if existing, found, findErr := s.Incidents.FindApproval(ctx, workspaceID, run.ID, idempotencyKey); findErr != nil {
			return contracts.IncidentWorkflowResult{}, findErr
		} else if found && strings.EqualFold(existing.Decision, decision) {
			result, getErr := s.Get(ctx, workspaceID, run.ID)
			if getErr != nil {
				return contracts.IncidentWorkflowResult{}, getErr
			}
			result.Approval = &existing
			result.Duplicate = true
			return result, nil
		}
		return contracts.IncidentWorkflowResult{}, ErrIncidentApprovalRequired
	}
	if actor.WorkspaceID == "" {
		actor.WorkspaceID = workspaceID
	}
	if actor.WorkspaceID != workspaceID {
		return contracts.IncidentWorkflowResult{}, store.ErrIncidentWorkspace
	}
	approval := contracts.IncidentApproval{WorkspaceID: workspaceID, RunID: run.ID, OperationHash: run.Operation.Hash, PlanHash: run.PlanHash, StepID: waiting.StepID, Decision: decision, Actor: actor, IdempotencyKey: idempotencyKey, DecidedAt: time.Now().UTC()}
	if err := approval.Normalize(); err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	owner := workerPrefix + run.ID
	lease, err := s.Workflows.AcquireLease(ctx, workspaceID, run.ID, owner, workerLeaseTTL)
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	defer func() { _ = s.Workflows.ReleaseLease(context.Background(), lease.Lease) }()
	approvalRaw, _ := json.Marshal(map[string]any{"decision": approval.Decision, "decision_hash": approval.DecisionHash, "run_id": approval.RunID, "step_id": approval.StepID})
	approvalReference := incidentStepReference(run.ID, waiting.StepID) + ":approval"
	evidence, err := s.Evidence.Put(ctx, store.EvidencePutInput{WorkspaceID: workspaceID, SourceReference: approvalReference, DeduplicationKey: "approval_" + approval.DecisionHash, Kind: "incident-approval", MediaType: "application/json", Gist: "incident remediation approval decision", Detail: "hash-only approval decision", RawPayload: approvalRaw, Actor: actor, CausationID: run.ID, CorrelationID: run.ID})
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	recorded, err := s.Incidents.RecordApproval(ctx, approval, evidence.Record.ID, evidence.Record.EvidenceHash)
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	approval = recorded.Approval
	stepResult := contracts.WorkflowStepResult{Status: contracts.WorkflowStepSucceeded, OutputHash: approval.DecisionHash, Evidence: []contracts.OperationEvidenceRef{{WorkspaceID: workspaceID, SourceReference: approvalReference, EvidenceHash: recorded.EvidenceHash, Role: "approval"}}, OutputBytes: int64(len(approvalRaw))}
	if approval.Decision == "reject" {
		stepResult = contracts.WorkflowStepResult{Status: contracts.WorkflowStepFailed, OutputHash: approval.DecisionHash, Evidence: stepResult.Evidence, Failure: &contracts.WorkflowFailure{Code: "approval_rejected", Message: "approval was rejected", Retryable: false}}
	}
	runtime := s.runtime()
	resumed, err := runtime.ResumeWait(ctx, workflowruntime.RunInput{WorkspaceID: workspaceID, RunID: run.ID, Lease: lease.Lease, Actor: actor}, waiting.StepID, stepResult)
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	if err := s.Incidents.LinkWorkflow(ctx, workspaceID, incident.ID, run.ID, workflowIncidentStatus(resumed.Status)); err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	result, err := s.advanceWithLease(ctx, incident, resumed, actor, lease.Lease)
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	result.Approval = &approval
	return result, nil
}

// Get returns the bounded current workflow surface and incident projection.
func (s *Service) Get(ctx context.Context, workspaceID, runID string) (contracts.IncidentWorkflowResult, error) {
	if err := s.validate(); err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	run, err := s.Workflows.Get(ctx, workspaceID, runID)
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	incident, err := s.Incidents.Get(ctx, workspaceID, incidentIDForRun(run))
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	return contracts.IncidentWorkflowResult{Incident: incident, Workflow: run}, nil
}

// Replay verifies the append-only workflow transition chain. It never calls
// the model, connector, artifact, evidence, or approval runtime.
func (s *Service) Replay(ctx context.Context, workspaceID, runID string, fromVersion int64) (contracts.IncidentWorkflowResult, error) {
	if err := s.validate(); err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	replay, err := s.Workflows.Replay(ctx, workspaceID, runID, fromVersion, 4096)
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	result, err := s.Get(ctx, workspaceID, runID)
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	result.ReplayHash, result.ReplayVerify = replay.ReplayHash, replay.Verified
	return result, nil
}

func (s *Service) validate() error {
	if s == nil || s.Incidents == nil || s.Workflows == nil || s.Evidence == nil || s.Artifacts == nil || s.Receipts == nil || s.Registry == nil {
		return fmt.Errorf("incident workflow service is not configured")
	}
	return nil
}

func (s *Service) advance(ctx context.Context, incident contracts.Incident, run contracts.WorkflowRun, actor contracts.ActorRef) (contracts.IncidentWorkflowResult, error) {
	owner := workerPrefix + run.ID
	lease, err := s.Workflows.AcquireLease(ctx, run.WorkspaceID, run.ID, owner, workerLeaseTTL)
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	defer func() { _ = s.Workflows.ReleaseLease(context.Background(), lease.Lease) }()
	return s.advanceWithLease(ctx, incident, run, actor, lease.Lease)
}

func (s *Service) advanceWithRun(ctx context.Context, incident contracts.Incident, run contracts.WorkflowRun, actor contracts.ActorRef) (contracts.IncidentWorkflowResult, error) {
	owner := workerPrefix + run.ID
	lease, err := s.Workflows.AcquireLease(ctx, run.WorkspaceID, run.ID, owner, workerLeaseTTL)
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	defer func() { _ = s.Workflows.ReleaseLease(context.Background(), lease.Lease) }()
	return s.advanceWithLease(ctx, incident, run, actor, lease.Lease)
}

func (s *Service) advanceWithLease(ctx context.Context, incident contracts.Incident, run contracts.WorkflowRun, actor contracts.ActorRef, lease store.WorkflowLease) (contracts.IncidentWorkflowResult, error) {
	var err error
	run, err = s.retryRecoverable(ctx, run, lease, actor)
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	run, err = s.runtime().Run(ctx, workflowruntime.RunInput{WorkspaceID: run.WorkspaceID, RunID: run.ID, Lease: lease, Actor: actor})
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	if run.Status == contracts.WorkflowStatusRecoveryRequired {
		run, err = s.retryRecoverable(ctx, run, lease, actor)
		if err != nil {
			return contracts.IncidentWorkflowResult{}, err
		}
		if run.Status == contracts.WorkflowStatusRunning {
			run, err = s.runtime().Run(ctx, workflowruntime.RunInput{WorkspaceID: run.WorkspaceID, RunID: run.ID, Lease: lease, Actor: actor})
			if err != nil {
				return contracts.IncidentWorkflowResult{}, err
			}
		}
	}
	if err := s.Incidents.LinkWorkflow(ctx, incident.WorkspaceID, incident.ID, run.ID, workflowIncidentStatus(run.Status)); err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	currentIncident, err := s.Incidents.Get(ctx, incident.WorkspaceID, incident.ID)
	if err != nil {
		return contracts.IncidentWorkflowResult{}, err
	}
	result := contracts.IncidentWorkflowResult{Incident: currentIncident, Workflow: run}
	if run.Status == contracts.WorkflowStatusSucceeded {
		replay, replayErr := s.Workflows.Replay(ctx, run.WorkspaceID, run.ID, 0, 4096)
		if replayErr != nil {
			return contracts.IncidentWorkflowResult{}, replayErr
		}
		result.ReplayHash, result.ReplayVerify = replay.ReplayHash, replay.Verified
		receipt, receiptErr := s.finalizeReceipt(ctx, run, replay.ReplayHash, actor)
		if receiptErr != nil {
			return contracts.IncidentWorkflowResult{}, receiptErr
		}
		result.Receipt = &receipt
		if err := s.Incidents.LinkReceipt(ctx, incident.WorkspaceID, incident.ID, receipt.ID, contracts.IncidentStatusResolved); err != nil {
			return contracts.IncidentWorkflowResult{}, err
		}
		result.Incident, err = s.Incidents.Get(ctx, incident.WorkspaceID, incident.ID)
		if err != nil {
			return contracts.IncidentWorkflowResult{}, err
		}
	}
	return result, nil
}

func (s *Service) retryRecoverable(ctx context.Context, run contracts.WorkflowRun, lease store.WorkflowLease, actor contracts.ActorRef) (contracts.WorkflowRun, error) {
	if run.Status != contracts.WorkflowStatusRecoveryRequired {
		return run, nil
	}
	for _, state := range run.Steps {
		if state.Status != contracts.WorkflowStepRecoveryRequired {
			continue
		}
		var planStep contracts.OperationStep
		for _, candidate := range run.Plan.Steps {
			if candidate.ID == state.StepID {
				planStep = candidate
				break
			}
		}
		if planStep.Effect != contracts.EffectClassReadOnly && planStep.Effect != contracts.EffectClassObservation {
			return run, nil
		}
		key := "incident-recovery-" + contracts.HashStrings(run.ID, state.StepID, fmt.Sprint(run.StateVersion))[:48]
		retried, err := s.Workflows.RetryRecovery(ctx, store.WorkflowStepStartInput{WorkspaceID: run.WorkspaceID, RunID: run.ID, StepID: state.StepID, OwnerID: lease.OwnerID, Fence: lease.Fence, Actor: actor, RequestID: key, IdempotencyKey: key, CausationID: run.Operation.ID, CorrelationID: run.ID})
		if err != nil {
			return contracts.WorkflowRun{}, err
		}
		return retried, nil
	}
	return run, nil
}

func (s *Service) runtime() *workflowruntime.Runtime {
	return &workflowruntime.Runtime{Store: s.Workflows, Executor: &executor{service: s}, Now: time.Now}
}

func (s *Service) plan(incident contracts.Incident, runID string, actor contracts.ActorRef) (contracts.OperationPlan, contracts.OperationRequest, contracts.WorkflowBudget, error) {
	lookup := func(name string) (contracts.CapabilityDefinition, error) {
		capability, ok := s.Registry.LookupIdentity(incident.WorkspaceID, fakeincident.ConnectorName, fakeincident.ConnectorVersion, name, "1")
		if !ok {
			return contracts.CapabilityDefinition{}, fmt.Errorf("incident capability %s is not registered", name)
		}
		return capability.Definition(), nil
	}
	readDef, err := lookup("incident.read")
	if err != nil {
		return contracts.OperationPlan{}, contracts.OperationRequest{}, contracts.WorkflowBudget{}, err
	}
	runbookDef, err := lookup("runbook.read")
	if err != nil {
		return contracts.OperationPlan{}, contracts.OperationRequest{}, contracts.WorkflowBudget{}, err
	}
	verifyDef, err := lookup("incident.verify")
	if err != nil {
		return contracts.OperationPlan{}, contracts.OperationRequest{}, contracts.WorkflowBudget{}, err
	}
	remediateDef, err := lookup("incident.remediate")
	if err != nil {
		return contracts.OperationPlan{}, contracts.OperationRequest{}, contracts.WorkflowBudget{}, err
	}
	profile := contracts.DefaultExecutionProfile()
	profile.MaxSteps, profile.MaxConcurrency, profile.MaxOutputBytes, profile.MaxOutputTokens, profile.MaxRetries = 10, 2, 16<<20, 100000, 2
	profile.MaxCostUSD = 0
	profile.AllowExternalEffects = true
	inputHash := incident.PayloadHash
	incidentTarget := contracts.ResourceRef{WorkspaceID: incident.WorkspaceID, System: contracts.SystemRef{WorkspaceID: incident.WorkspaceID, Type: "monitoring", ID: incident.SourceSystem, Version: "1"}, Kind: fakeincident.ResourceKind, ID: incident.ID, Version: "1", ContentHash: incident.PayloadHash}
	runbookTarget := contracts.ResourceRef{WorkspaceID: incident.WorkspaceID, System: contracts.SystemRef{WorkspaceID: incident.WorkspaceID, Type: "repository", ID: "runbook", Version: "1"}, Kind: fakeincident.RunbookKind, ID: "runbook-" + incident.SourceSystem, Version: "1"}
	request := contracts.OperationRequest{SchemaVersion: contracts.DomainNeutralSchemaVersion, ID: runID, RequestID: "incident-request-" + incident.ID, IdempotencyKey: "incident-workflow-" + incident.ID, CausationID: incident.EventHash, CorrelationID: runID, WorkspaceID: incident.WorkspaceID, Actor: actor, Capability: readDef.Ref, Target: incidentTarget, InputType: fakeincident.InputType, InputSchemaVersion: readDef.InputSchemaVersion, InputSchemaHash: readDef.InputSchemaHash, InputHash: inputHash, Profile: readDef.Profile, Metadata: map[string]string{"fornix.workflow": "incident-investigation"}}
	if err := request.Normalize(); err != nil {
		return contracts.OperationPlan{}, contracts.OperationRequest{}, contracts.WorkflowBudget{}, err
	}
	step := func(id, kind string, ordinal int, cap contracts.CapabilityDefinition, target contracts.ResourceRef, effect contracts.EffectClass, depends []string) contracts.OperationStep {
		return contracts.OperationStep{ID: id, Ordinal: ordinal, Kind: kind, Capability: cap.Ref, Target: target, DependsOn: depends, Effect: effect, Profile: cap.Profile, Evidence: cap.Evidence, InputHash: contracts.HashStrings(inputHash, id)}
	}
	steps := []contracts.OperationStep{
		step(runID+"-incident-read", contracts.WorkflowStepConnector, 0, readDef, incidentTarget, contracts.EffectClassReadOnly, nil),
		step(runID+"-runbook-read", contracts.WorkflowStepConnector, 1, runbookDef, runbookTarget, contracts.EffectClassReadOnly, []string{runID + "-incident-read"}),
		step(runID+"-investigate", contracts.WorkflowStepModel, 2, readDef, incidentTarget, contracts.EffectClassObservation, []string{runID + "-runbook-read"}),
		step(runID+"-diagnostics", contracts.WorkflowStepTool, 3, verifyDef, incidentTarget, contracts.EffectClassObservation, []string{runID + "-investigate"}),
		step(runID+"-report", contracts.WorkflowStepValidation, 4, verifyDef, incidentTarget, contracts.EffectClassObservation, []string{runID + "-diagnostics"}),
		step(runID+"-"+approvalStep, contracts.WorkflowStepApproval, 5, remediateDef, incidentTarget, contracts.EffectClassApprovalRequiredWrite, []string{runID + "-report"}),
		step(runID+"-remediate", contracts.WorkflowStepConnector, 6, remediateDef, incidentTarget, contracts.EffectClassApprovalRequiredWrite, []string{runID + "-" + approvalStep}),
		step(runID+"-verify", contracts.WorkflowStepConnector, 7, verifyDef, incidentTarget, contracts.EffectClassObservation, []string{runID + "-remediate"}),
	}
	plan := contracts.OperationPlan{SchemaVersion: contracts.DomainNeutralSchemaVersion, ID: runID + "-plan", OperationID: runID, WorkspaceID: incident.WorkspaceID, Actor: actor, Steps: steps}
	plan.OperationHash = request.StableHash()
	if err := plan.Normalize(); err != nil {
		return contracts.OperationPlan{}, contracts.OperationRequest{}, contracts.WorkflowBudget{}, err
	}
	budget := contracts.WorkflowBudget{MaxSteps: 10, MaxParallelRead: 2, MaxRetries: 2, MaxOutputBytes: 16 << 20, MaxTokens: 100000, MaxWallTimeMS: int64((15 * time.Minute) / time.Millisecond), MaxCostMicros: 0}
	return plan, request, budget, nil
}

func (s *Service) finalizeReceipt(ctx context.Context, run contracts.WorkflowRun, replayHash string, actor contracts.ActorRef) (contracts.WorkReceipt, error) {
	hashes := make([]string, 0)
	artifacts := make([]contracts.WorkReceiptArtifact, 0)
	steps := make([]contracts.WorkReceiptStep, 0, len(run.Steps))
	for _, state := range run.Steps {
		roles := make([]string, 0, len(state.Evidence)+len(state.Artifacts))
		for _, evidence := range state.Evidence {
			hashes = append(hashes, evidence.EvidenceHash)
			roles = append(roles, evidence.Role)
		}
		for _, ref := range state.Artifacts {
			roles = append(roles, ref.Role)
			artifacts = append(artifacts, contracts.WorkReceiptArtifact{ID: ref.ID, ArtifactID: ref.ArtifactID, WorkspaceID: ref.WorkspaceID, ContentHash: ref.ContentHash, SourceKind: ref.SourceKind, SourceID: ref.SourceID, Role: ref.Role})
		}
		steps = append(steps, contracts.WorkReceiptStep{Ordinal: state.Ordinal, ID: state.StepID, Name: state.Kind, Kind: state.Kind, Status: state.Status, SourceKind: "incident_workflow", SourceID: run.ID, InputHash: contracts.HashStrings(run.ID, state.StepID), OutputHash: state.OutputHash, ReferenceRoles: roles, Attempts: state.Attempt, ExternalEffect: state.Effect != nil, ExternalBoundary: externalBoundary(state.Effect)})
	}
	resolved, err := s.Evidence.ResolveEvidenceHashes(ctx, run.WorkspaceID, hashes)
	if err != nil {
		return contracts.WorkReceipt{}, err
	}
	evidenceLinks := make([]contracts.WorkReceiptEvidence, 0, len(resolved))
	seen := make(map[string]struct{})
	for _, value := range resolved {
		if _, ok := seen[value.EvidenceHash]; ok {
			continue
		}
		seen[value.EvidenceHash] = struct{}{}
		evidenceLinks = append(evidenceLinks, contracts.WorkReceiptEvidence{ID: value.ID, WorkspaceID: value.WorkspaceID, EvidenceHash: value.EvidenceHash, SourceReference: value.SourceReference, Role: "workflow"})
	}
	receipt, _, err := s.Receipts.Finalize(ctx, contracts.WorkReceiptFinalizeRequest{ReceiptID: "receipt-" + run.ID, RequestID: "receipt-request-" + run.ID, IdempotencyKey: "incident-receipt-" + run.ID, WorkspaceID: run.WorkspaceID, Actor: actor, WorkKind: contracts.WorkReceiptReferenceOperation, WorkID: run.ID, Operation: &run.Operation, ReplayHash: replayHash, Steps: steps, Evidence: evidenceLinks, Artifacts: artifacts, Cost: contracts.WorkReceiptCost{Measured: true}})
	return receipt, err
}

type executor struct{ service *Service }

func (e *executor) Execute(ctx context.Context, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep) (contracts.WorkflowStepResult, error) {
	if e == nil || e.service == nil {
		return contracts.WorkflowStepResult{}, fmt.Errorf("incident executor is not configured")
	}
	switch step.Kind {
	case contracts.WorkflowStepApproval:
		return contracts.WorkflowStepResult{Status: contracts.WorkflowStepAwaitingApproval, Wait: &contracts.WorkflowWait{Kind: contracts.WorkflowWaitApproval, Token: contracts.HashStrings("approval", run.ID, step.ID, fmt.Sprint(state.Attempt+1)), Reason: "explicit approval is required before remediation"}}, nil
	case contracts.WorkflowStepModel:
		return e.model(ctx, run, state, step)
	case contracts.WorkflowStepTool:
		return e.diagnostic(ctx, run, state, step)
	case contracts.WorkflowStepValidation:
		return e.report(ctx, run, state, step)
	case contracts.WorkflowStepConnector:
		return e.connector(ctx, run, state, step)
	default:
		return contracts.WorkflowStepResult{}, fmt.Errorf("unsupported incident workflow step kind %q", step.Kind)
	}
}

func (e *executor) connector(ctx context.Context, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep) (contracts.WorkflowStepResult, error) {
	capability, ok := e.service.Registry.Lookup(step.Capability)
	if !ok {
		return contracts.WorkflowStepResult{}, connector.ErrCapabilityNotFound
	}
	definition := capability.Definition()
	identity := contracts.HashStrings("incident-connector", run.ID, step.ID, fmt.Sprint(state.Attempt))[:48]
	request := contracts.OperationRequest{ID: "incident-op-" + identity, RequestID: "incident-request-" + identity, IdempotencyKey: "incident-idempotency-" + identity, CausationID: run.Operation.ID, CorrelationID: run.ID, WorkspaceID: run.WorkspaceID, Actor: run.Actor, Capability: step.Capability, Target: step.Target, InputType: fakeincident.InputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: step.InputHash, Profile: step.Profile}
	principal := contracts.Principal{ID: run.Actor.ID, WorkspaceID: run.WorkspaceID, Subject: run.Actor.ID, Kind: run.Actor.Kind, Authenticated: true}
	approved := false
	for _, candidate := range run.Steps {
		if strings.HasSuffix(candidate.StepID, "-"+approvalStep) && candidate.Status == contracts.WorkflowStepSucceeded {
			approved = true
		}
	}
	admission, err := e.service.Registry.Admit(ctx, request, connector.AdmissionOptions{Principal: &principal, ApprovalGranted: approved})
	if err != nil {
		return contracts.WorkflowStepResult{}, err
	}
	operationPlan, err := admission.Capability.Plan(request)
	if err != nil {
		return contracts.WorkflowStepResult{}, err
	}
	operationResult, err := admission.Capability.Execute(ctx, request, operationPlan)
	if err != nil {
		return contracts.WorkflowStepResult{}, err
	}
	payload, _ := json.Marshal(map[string]any{"capability": step.Capability.Name, "target_hash": step.Target.StableHash(), "output_hash": operationResult.OutputHash, "external": len(operationResult.ExternalEffects) > 0})
	evidence, err := e.writeEvidence(ctx, run, step, payload, "connector")
	if err != nil {
		return contracts.WorkflowStepResult{}, err
	}
	result := contracts.WorkflowStepResult{Status: contracts.WorkflowStepSucceeded, OutputHash: operationResult.OutputHash, Evidence: []contracts.OperationEvidenceRef{evidence}, OutputBytes: int64(len(payload)), ExternalEffectStarted: len(operationResult.ExternalEffects) > 0}
	if len(operationResult.ExternalEffects) > 0 {
		result.Effect = &operationResult.ExternalEffects[0]
	}
	return result, nil
}

func (e *executor) model(ctx context.Context, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep) (contracts.WorkflowStepResult, error) {
	var response contracts.ModelResponse
	var err error
	if e.service.Model != nil {
		request := contracts.NewModelRequest(run.WorkspaceID, "fake", "incident-fake", "Produce a bounded incident investigation plan from the recorded incident and runbook evidence.")
		request.RequestID, request.IdempotencyKey = "incident-model-"+run.ID, "incident-model-"+run.ID
		request.CausationID, request.CorrelationID, request.Actor = run.Operation.ID, run.ID, run.Actor
		request.Metadata = map[string]string{"fornix.workflow": "incident", "fornix.replay": "recorded-only"}
		request.Budget = contracts.ModelBudget{MaxInputTokens: 4096, MaxOutputTokens: 2048, MaxTotalTokens: 6144, TimeoutMS: 30000, MaxOutputBytes: 16 << 10}
		response, err = e.service.Model.Complete(ctx, request)
	} else {
		content := "deterministic incident investigation " + contracts.HashStrings(run.ID, step.InputHash)[:16]
		response = contracts.ModelResponse{RequestID: "incident-model-" + run.ID, Provider: contracts.ProviderRef{Provider: "fake", Model: "incident-fake"}, Content: content, FinishReason: "stop", Usage: contracts.ModelUsage{InputTokens: 16, OutputTokens: contracts.EstimateModelTokens(content), Source: "fake"}, Cost: contracts.ModelCost{Currency: "USD", Source: "fake"}}
		err = response.Normalize()
	}
	if err != nil {
		return contracts.WorkflowStepResult{}, err
	}
	artifact, err := e.service.Artifacts.Put(ctx, store.ArtifactPutInput{WorkspaceID: run.WorkspaceID, Kind: "incident-investigation", MediaType: "text/plain", Raw: []byte(response.Content), Manifest: contracts.ArtifactManifest{Gist: "bounded fake incident investigation", Detail: "model output preserved as immutable artifact"}, SourceKind: "workflow", SourceID: "incident-step-" + contracts.HashStrings(run.ID, step.ID)[:48], Role: "investigation", IdempotencyKey: "incident-artifact-" + run.ID + "-investigation", Actor: run.Actor, CausationID: run.Operation.ID, CorrelationID: run.ID})
	if err != nil {
		return contracts.WorkflowStepResult{}, err
	}
	payload, _ := json.Marshal(map[string]any{"provider": response.Provider.Provider, "content_hash": response.ContentHash, "artifact_hash": artifact.Artifact.ContentHash, "input_tokens": response.Usage.InputTokens, "output_tokens": response.Usage.OutputTokens})
	evidence, err := e.writeEvidence(ctx, run, step, payload, "investigation")
	if err != nil {
		return contracts.WorkflowStepResult{}, err
	}
	return contracts.WorkflowStepResult{Status: contracts.WorkflowStepSucceeded, OutputHash: response.ContentHash, Evidence: []contracts.OperationEvidenceRef{evidence}, Artifacts: []contracts.ArtifactRef{artifact.Reference}, OutputBytes: int64(len(response.Content)), Tokens: int64(response.Usage.InputTokens + response.Usage.OutputTokens), CostMicros: int64(response.Cost.TotalCostUSD * 1_000_000)}, nil
}

func (e *executor) diagnostic(ctx context.Context, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep) (contracts.WorkflowStepResult, error) {
	_ = ctx
	outputHash := contracts.HashStrings("read-only-diagnostic", run.ID, step.InputHash)
	payload, _ := json.Marshal(map[string]any{"diagnostic": "bounded_fake_read_only", "output_hash": outputHash, "attempt": state.Attempt})
	evidence, err := e.writeEvidence(ctx, run, step, payload, "diagnostic")
	if err != nil {
		return contracts.WorkflowStepResult{}, err
	}
	return contracts.WorkflowStepResult{Status: contracts.WorkflowStepSucceeded, OutputHash: outputHash, Evidence: []contracts.OperationEvidenceRef{evidence}, OutputBytes: int64(len(payload))}, nil
}

func (e *executor) report(ctx context.Context, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep) (contracts.WorkflowStepResult, error) {
	payload, _ := json.Marshal(map[string]any{"workflow": run.ID, "incident": run.Operation.ID, "status": "investigation_complete", "step": step.ID, "state_hash": run.StateHash})
	artifact, err := e.service.Artifacts.Put(ctx, store.ArtifactPutInput{WorkspaceID: run.WorkspaceID, Kind: "incident-report", MediaType: "application/json", Raw: payload, Manifest: contracts.ArtifactManifest{Gist: "bounded incident investigation report", Detail: "report links incident evidence and awaits remediation approval"}, SourceKind: "workflow", SourceID: "incident-step-" + contracts.HashStrings(run.ID, step.ID)[:48], Role: "report", IdempotencyKey: "incident-artifact-" + run.ID + "-report", Actor: run.Actor, CausationID: run.Operation.ID, CorrelationID: run.ID})
	if err != nil {
		return contracts.WorkflowStepResult{}, err
	}
	evidence, err := e.writeEvidence(ctx, run, step, payload, "report")
	if err != nil {
		return contracts.WorkflowStepResult{}, err
	}
	return contracts.WorkflowStepResult{Status: contracts.WorkflowStepSucceeded, OutputHash: contracts.ArtifactContentHash(payload), Evidence: []contracts.OperationEvidenceRef{evidence}, Artifacts: []contracts.ArtifactRef{artifact.Reference}, OutputBytes: int64(len(payload))}, nil
}

func (e *executor) writeEvidence(ctx context.Context, run contracts.WorkflowRun, step contracts.OperationStep, payload []byte, role string) (contracts.OperationEvidenceRef, error) {
	reference := incidentStepReference(run.ID, step.ID) + ":" + role
	result, err := e.service.Evidence.Put(ctx, store.EvidencePutInput{WorkspaceID: run.WorkspaceID, SourceReference: reference, DeduplicationKey: "output_" + contracts.ArtifactContentHash(payload), Kind: "incident-workflow-" + role, MediaType: "application/json", Gist: "bounded incident workflow evidence", Detail: "hash-stable workflow evidence; disclose through the evidence API", RawPayload: payload, Actor: run.Actor, CausationID: run.Operation.ID, CorrelationID: run.ID})
	if err != nil {
		return contracts.OperationEvidenceRef{}, err
	}
	return contracts.OperationEvidenceRef{WorkspaceID: run.WorkspaceID, SourceReference: reference, EvidenceHash: result.Record.EvidenceHash, Role: role}, nil
}

func incidentStepReference(runID, stepID string) string {
	return "incident-workflow:" + contracts.HashStrings(runID, stepID)[:48]
}

func incidentIDForRun(run contracts.WorkflowRun) string {
	// The deterministic operation identity embeds the incident identity.
	return strings.TrimPrefix(run.ID, "incident-run-")
}

func workflowApprovalStep(run contracts.WorkflowRun) (contracts.WorkflowStepState, bool) {
	for _, step := range run.Steps {
		if strings.HasSuffix(step.StepID, "-"+approvalStep) {
			return step, true
		}
	}
	return contracts.WorkflowStepState{}, false
}

func workflowIncidentStatus(status string) string {
	switch status {
	case contracts.WorkflowStatusAwaitingApproval:
		return contracts.IncidentStatusAwaitingApproval
	case contracts.WorkflowStatusSucceeded:
		return contracts.IncidentStatusResolved
	case contracts.WorkflowStatusCancelled:
		return contracts.IncidentStatusCancelled
	case contracts.WorkflowStatusFailed, contracts.WorkflowStatusDeadLetter:
		return contracts.IncidentStatusFailed
	case contracts.WorkflowStatusRecoveryRequired:
		return contracts.IncidentStatusFailed
	default:
		return contracts.IncidentStatusInvestigating
	}
}

func externalBoundary(effect *contracts.ExternalEffect) string {
	if effect == nil {
		return ""
	}
	return effect.Boundary
}
