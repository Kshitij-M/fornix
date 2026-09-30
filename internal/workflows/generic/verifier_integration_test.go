package generic

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/omaveda/fornix/internal/adapters/fakedomains"
	connectorruntime "github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/effectdispatch"
	"github.com/omaveda/fornix/internal/store"
)

func TestGenericWorkflowUnknownVerificationResumesWithoutRepeatingVerifier(t *testing.T) {
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set; use an explicitly disposable Postgres database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}

	workspaceID := fmt.Sprintf("test-generic-verify-%d", time.Now().UnixNano())
	actor := contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspaceID}
	events := store.NewEventStore(pool)
	operations := store.NewOperationStore(pool, events)
	admission := store.NewAdmissionStore(pool, events)
	links := store.NewDomainEffectLinkStore(pool)
	dispatcher := &effectdispatch.Dispatcher{Operations: operations, Admission: admission, Links: links}
	workflowStore := store.NewWorkflowStore(pool, events, operations)
	receipts := store.NewWorkReceiptStore(pool)

	domainConnector, err := fakedomains.NewConnector(workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	registry := connectorruntime.NewRegistry()
	if err := registry.Register(domainConnector); err != nil {
		t.Fatal(err)
	}
	registry.RequireTrustPolicy(true)
	registry.RequireEffectAuthority(true)
	if err := registry.TrustWorkspace(workspaceID, "integration-test"); err != nil {
		t.Fatal(err)
	}
	var publish connectorruntime.Capability
	for _, capability := range domainConnector.Capabilities() {
		if capability.Definition().Ref.Name == "data_pipeline.publish" {
			publish = capability
			break
		}
	}
	if publish == nil {
		t.Fatal("fake data-pipeline publish capability is not registered")
	}
	definition := publish.Definition()
	target := contracts.ResourceRef{
		WorkspaceID: workspaceID,
		System:      contracts.SystemRef{WorkspaceID: workspaceID, Type: "pipeline", ID: "warehouse", Version: "1"},
		Kind:        fakedomains.PipelineKind, ID: "pipeline-1", Version: "1", ContentHash: contracts.HashStrings("pipeline-target"),
	}
	operationID := contracts.NewID("generic-verify")
	operation := contracts.OperationRequest{
		ID: operationID, RequestID: operationID + "-request", IdempotencyKey: operationID + "-idempotency",
		WorkspaceID: workspaceID, Actor: actor, Capability: definition.Ref, Target: target,
		InputType: fakedomains.PipelineInput, InputSchemaVersion: definition.InputSchemaVersion,
		InputSchemaHash: definition.InputSchemaHash, InputHash: contracts.HashStrings("publish-pipeline"), Profile: definition.Profile,
	}
	if err := operation.Normalize(); err != nil {
		t.Fatal(err)
	}
	plan := contracts.OperationPlan{
		ID: operationID + "-plan", OperationID: operationID, OperationHash: operation.StableHash(),
		WorkspaceID: workspaceID, Actor: actor,
		Steps: []contracts.OperationStep{
			{ID: "approval", Ordinal: 0, Kind: contracts.WorkflowStepApproval, Capability: definition.Ref, Target: target, Effect: contracts.EffectClassObservation, Profile: contracts.DefaultExecutionProfile(), InputHash: contracts.HashStrings("approval")},
			{ID: "publish", Ordinal: 1, Kind: contracts.WorkflowStepConnector, Capability: definition.Ref, Target: target, DependsOn: []string{"approval"}, Effect: definition.Effect, Profile: definition.Profile, InputHash: operation.InputHash},
		},
	}
	service := NewService(workflowStore, receipts)
	connectorExecutor := NewConnectorStepExecutor(registry, &connectorruntime.Executor{Registry: registry}, operations)
	connectorExecutor.SetEffectBoundary(admission, dispatcher)
	service.Executor = approvalThenConnectorExecutor{connector: connectorExecutor}
	service.Effects = dispatcher
	verifier := &retrySequenceVerifier{}
	service.Verifier = verifier
	created, err := service.Create(ctx, contracts.WorkflowCreateRequest{
		WorkspaceID: workspaceID, Operation: operation, Plan: plan,
		Budget: contracts.DefaultWorkflowBudget(), Idempotency: operation.IdempotencyKey,
	})
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	leaseResult, err := service.AcquireLease(ctx, workspaceID, created.Run.ID, actor.ID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	fence := leaseResult.Lease.Fence
	waiting, err := service.Advance(ctx, workspaceID, created.Run.ID, actor, fence, 0)
	if err != nil {
		t.Fatalf("reach approval: %v", err)
	}
	if waiting.Status != contracts.WorkflowStatusAwaitingApproval {
		t.Fatalf("first checkpoint=%s, want approval", waiting.Status)
	}
	if _, err := service.Resume(ctx, workspaceID, created.Run.ID, actor, fence, 0, contracts.WorkflowResumeRequest{
		StepID: "approval", Result: contracts.WorkflowStepResult{Status: contracts.WorkflowStepSucceeded},
	}); !errors.Is(err, ErrApprovalRequired) {
		t.Fatalf("generic resume bypassed the approval command: %v", err)
	}
	if _, err := service.Approve(ctx, workspaceID, created.Run.ID, actor, fence, 0, "approval"); !errors.Is(err, ErrSelfApproval) {
		t.Fatalf("workflow requester self-approved an external effect: %v", err)
	}
	if err := service.ReleaseLease(ctx, leaseResult.Lease); err != nil {
		t.Fatal(err)
	}
	approver := contracts.ActorRef{ID: "approver", Kind: "human", WorkspaceID: workspaceID}
	leaseResult, err = service.AcquireLease(ctx, workspaceID, created.Run.ID, approver.ID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	fence = leaseResult.Lease.Fence
	approved, err := service.Approve(ctx, workspaceID, created.Run.ID, approver, fence, 0, "approval")
	if err != nil {
		t.Fatalf("approve workflow: %v", err)
	}
	if approved.Steps[0].Status != contracts.WorkflowStepSucceeded {
		t.Fatalf("approval step=%s, want succeeded", approved.Steps[0].Status)
	}
	waiting, err = service.Advance(ctx, workspaceID, created.Run.ID, approver, fence, 0)
	if err != nil {
		t.Fatalf("dispatch effect: %v", err)
	}
	var effect *contracts.ExternalEffect
	for _, step := range waiting.Steps {
		if step.StepID == "publish" {
			effect = step.Effect
		}
	}
	if waiting.Status != contracts.WorkflowStatusAwaitingExternal || effect == nil || effect.ID == "" {
		var stepFailure *contracts.WorkflowFailure
		for _, step := range waiting.Steps {
			if step.StepID == "publish" {
				stepFailure = step.Failure
			}
		}
		t.Fatalf("publish did not pause on its durable effect: status=%s terminal_reason=%s failure=%+v step_failure=%+v", waiting.Status, waiting.TerminalReason, waiting.Failure, stepFailure)
	}
	effectLease, err := admission.AcquireEffectLease(ctx, workspaceID, created.Run.ID, effect.ID, approver.ID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	state, err := admission.GetEffectState(ctx, workspaceID, effect.ID)
	if err != nil {
		t.Fatal(err)
	}
	link, err := links.CurrentByDomain(ctx, workspaceID, contracts.DomainEffectKindWorkflowStep, created.Run.ID+":publish", contracts.DomainEffectLinkRolePrimary)
	if err != nil {
		t.Fatal(err)
	}
	verify := contracts.WorkflowVerifyRequest{
		StepID: "publish", EffectFence: effectLease.Lease.Fence, ExpectedEffectVer: state.Version,
		ExpectedLinkVersion: link.Transition.Version, IdempotencyKey: "verify-unknown-attempt",
	}
	workflowStore.SetFailureHook(func(point string) error {
		if point == "workflow_step_completed" {
			return fmt.Errorf("injected crash after effect proof commit")
		}
		return nil
	})
	if _, err := service.VerifyEffect(ctx, workspaceID, created.Run.ID, approver, fence, 0, verify); err == nil {
		t.Fatal("expected workflow-checkpoint crash after effect proof commit")
	} else if !strings.Contains(err.Error(), "injected crash after effect proof commit") {
		t.Fatalf("expected injected crash after verifier/reconciliation, got earlier failure: %v", err)
	}
	workflowStore.SetFailureHook(nil)
	if verifier.Calls() != 1 {
		t.Fatalf("verifier calls after injected crash=%d, want 1", verifier.Calls())
	}
	committedState, err := admission.GetEffectState(ctx, workspaceID, effect.ID)
	if err != nil {
		t.Fatal(err)
	}
	if committedState.State != contracts.ExternalEffectRecoveryRequired {
		t.Fatalf("effect proof did not commit before workflow crash: %+v", committedState)
	}
	unchangedRun, err := service.Get(ctx, workspaceID, created.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unchangedRun.Steps[1].Status != contracts.WorkflowStepAwaitingExternal {
		t.Fatalf("workflow checkpoint partially committed: %+v", unchangedRun.Steps[1])
	}

	unknown, err := service.VerifyEffect(ctx, workspaceID, created.Run.ID, approver, fence, 0, verify)
	if err != nil {
		t.Fatalf("resume from committed proof without repeating verifier: %v", err)
	}
	if unknown.Outcome.Status != contracts.EffectVerificationStatusUnknown || unknown.Run.Steps[1].Status != contracts.WorkflowStepRecoveryRequired || !unknown.Duplicate {
		t.Fatalf("unknown proof replay=%+v", unknown)
	}
	if verifier.Calls() != 1 {
		t.Fatalf("same idempotency key called verifier again: calls=%d", verifier.Calls())
	}
	if _, err := service.VerifyEffect(ctx, workspaceID, created.Run.ID, approver, fence, 0, verify); err != nil {
		t.Fatalf("repeat same verification request: %v", err)
	}
	if verifier.Calls() != 1 {
		t.Fatalf("duplicate verification performed external proof work: calls=%d", verifier.Calls())
	}

	state, err = admission.GetEffectState(ctx, workspaceID, effect.ID)
	if err != nil {
		t.Fatal(err)
	}
	link, err = links.CurrentByDomain(ctx, workspaceID, contracts.DomainEffectKindWorkflowStep, created.Run.ID+":publish", contracts.DomainEffectLinkRolePrimary)
	if err != nil {
		t.Fatal(err)
	}
	retry := verify
	retry.ExpectedEffectVer = state.Version
	retry.ExpectedLinkVersion = link.Transition.Version
	retry.IdempotencyKey = "verify-success-attempt"
	verified, err := service.VerifyEffect(ctx, workspaceID, created.Run.ID, approver, fence, 0, retry)
	if err != nil {
		t.Fatalf("fresh verification attempt could not resolve recovery-required effect: %v", err)
	}
	if verified.Outcome.Status != contracts.EffectVerificationStatusVerified || verified.Run.Status != contracts.WorkflowStatusSucceeded || verified.Run.Steps[1].Status != contracts.WorkflowStepSucceeded {
		t.Fatalf("verified workflow did not complete: %+v", verified)
	}
	if verifier.Calls() != 2 {
		t.Fatalf("fresh idempotency key verifier calls=%d, want 2", verifier.Calls())
	}
	replay, err := service.Replay(ctx, workspaceID, created.Run.ID, contracts.WorkflowReplayRequest{Limit: MaxReplayLimit})
	if err != nil || !replay.Verified {
		t.Fatalf("workflow replay verified=%v err=%v", replay.Verified, err)
	}
	receipt, createdReceipt, err := service.FinalizeReceipt(ctx, workspaceID, created.Run.ID, approver)
	if err != nil {
		t.Fatalf("finalize work receipt: %v", err)
	}
	if !createdReceipt || receipt.WorkID != created.Run.ID || receipt.ReplayHash != replay.ReplayHash {
		t.Fatalf("receipt did not bind verified workflow replay: created=%v receipt=%+v replay=%+v", createdReceipt, receipt, replay)
	}
	duplicateReceipt, createdDuplicate, err := service.FinalizeReceipt(ctx, workspaceID, created.Run.ID, approver)
	if err != nil {
		t.Fatalf("repeat work receipt finalization: %v", err)
	}
	if createdDuplicate || duplicateReceipt.CanonicalHash != receipt.CanonicalHash {
		t.Fatalf("repeated finalization was not idempotent: created=%v receipt_hash=%s duplicate_hash=%s", createdDuplicate, receipt.CanonicalHash, duplicateReceipt.CanonicalHash)
	}
}

type approvalThenConnectorExecutor struct {
	connector *ConnectorStepExecutor
}

func (e approvalThenConnectorExecutor) Execute(ctx context.Context, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep) (contracts.WorkflowStepResult, error) {
	if step.Kind == contracts.WorkflowStepApproval {
		return contracts.WorkflowStepResult{Status: contracts.WorkflowStepAwaitingApproval, Wait: &contracts.WorkflowWait{
			Kind: contracts.WorkflowWaitApproval, Token: contracts.HashStrings("approval", run.ID, step.ID, fmt.Sprint(state.Attempt+1)),
			Reason: "explicit approval is required",
		}}, nil
	}
	return e.connector.Execute(ctx, run, state, step)
}

func (e approvalThenConnectorExecutor) ExecuteWithLease(ctx context.Context, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep, lease store.WorkflowLease, taskOwner string, taskFence uint64) (contracts.WorkflowStepResult, error) {
	if step.Kind == contracts.WorkflowStepApproval {
		return e.Execute(ctx, run, state, step)
	}
	return e.connector.ExecuteWithLease(ctx, run, state, step, lease, taskOwner, taskFence)
}

type retrySequenceVerifier struct {
	calls atomic.Int32
}

func (v *retrySequenceVerifier) Calls() int32 { return v.calls.Load() }

func (v *retrySequenceVerifier) VerifyEffect(_ context.Context, request contracts.EffectVerificationRequest) (contracts.EffectVerificationResult, error) {
	if v.calls.Add(1) == 1 {
		return contracts.EffectVerificationResult{
			Status:            contracts.EffectVerificationStatusUnknown,
			ProviderRequestID: request.Effect.ProviderRequestID, FailureCode: "verification_inconclusive",
		}, nil
	}
	resultHash := contracts.HashStrings("verified-output", request.EffectID)
	return contracts.EffectVerificationResult{
		Status: contracts.EffectVerificationStatusVerified, ProviderRequestID: request.Effect.ProviderRequestID,
		ResultHash: resultHash, VerificationHash: contracts.HashStrings("verified-proof", request.Link.DomainHash, resultHash),
	}, nil
}
