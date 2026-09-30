package generic

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	connectorruntime "github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
	workflowruntime "github.com/omaveda/fornix/internal/workflow"
)

func TestGenericWorkflowReadStepsUseDurableCapabilityRateAdmission(t *testing.T) {
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set; use an explicitly disposable Postgres database")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	workspaceID := fmt.Sprintf("test-generic-rate-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.workflow_transitions WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.workflow_idempotency WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.workflow_step_states WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.workflow_runs WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_effect_transitions WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_effect_state WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_approval_transitions WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_approvals WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_admission_decisions WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_results WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_authority_links WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_attempts WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_callbacks WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_transitions WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_links WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_resources WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_steps WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_leases WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_idempotency WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operations WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.idempotency_records WHERE workspace_id=$1`, workspaceID)
		pool.Close()
	})
	if err := store.ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}

	actor := contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: workspaceID}
	events := store.NewEventStore(pool)
	operations := store.NewOperationStore(pool, events)
	admission := store.NewAdmissionStore(pool, events)
	workflowStore := store.NewWorkflowStore(pool, events, operations)
	connector, err := newRateLimitedReadConnector(workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	registry := connectorruntime.NewRegistry()
	if err := registry.Register(connector); err != nil {
		t.Fatal(err)
	}
	registry.TrustWorkspace(workspaceID, "rate-limit-integration")
	executor := &ConnectorStepExecutor{Registry: registry, Executor: &connectorruntime.Executor{Registry: registry}, Operations: operations, Admission: admission}
	service := NewService(workflowStore)
	service.Executor = executor

	definition := connector.cap.Definition()
	target := contracts.ResourceRef{
		WorkspaceID: workspaceID,
		System:      contracts.SystemRef{WorkspaceID: workspaceID, Type: "service", ID: "fixture", Version: "1"},
		Kind:        "record", ID: "record-1", Version: "1", ContentHash: contracts.HashStrings("rate-target", workspaceID),
	}
	operationID := contracts.NewID("rate-operation")
	request := contracts.OperationRequest{
		ID: operationID, RequestID: operationID + "-request", IdempotencyKey: operationID + "-key",
		WorkspaceID: workspaceID, Actor: actor, Capability: definition.Ref, Target: target,
		InputType: "fornix.rate_test.v1", InputSchemaVersion: definition.InputSchemaVersion,
		InputSchemaHash: definition.InputSchemaHash, InputHash: contracts.HashStrings("rate-input"), Profile: definition.Profile,
	}
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	plan := contracts.OperationPlan{
		ID: operationID + "-plan", OperationID: operationID, OperationHash: request.StableHash(), WorkspaceID: workspaceID, Actor: actor,
		Steps: []contracts.OperationStep{
			{ID: "read-1", Ordinal: 0, Kind: contracts.WorkflowStepConnector, Capability: definition.Ref, Target: target, Effect: definition.Effect, Profile: definition.Profile, InputHash: contracts.HashStrings("step-input", "1")},
			{ID: "read-2", Ordinal: 1, Kind: contracts.WorkflowStepConnector, Capability: definition.Ref, Target: target, DependsOn: []string{"read-1"}, Effect: definition.Effect, Profile: definition.Profile, InputHash: contracts.HashStrings("step-input", "2")},
		},
	}
	if err := plan.Normalize(); err != nil {
		t.Fatal(err)
	}
	created, err := service.Create(ctx, contracts.WorkflowCreateRequest{WorkspaceID: workspaceID, Operation: request, Plan: plan, Budget: contracts.DefaultWorkflowBudget(), Idempotency: request.IdempotencyKey})
	if err != nil {
		t.Fatalf("create workflow: %v", err)
	}
	lease, err := service.AcquireLease(ctx, workspaceID, created.Run.ID, actor.ID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	first, err := workflowStore.StartStep(ctx, store.WorkflowStepStartInput{
		WorkspaceID: workspaceID, RunID: created.Run.ID, StepID: "read-1", OwnerID: lease.Lease.OwnerID,
		Fence: lease.Lease.Fence, Actor: actor, IdempotencyKey: "rate-limit-read-1-start",
	})
	if err != nil {
		t.Fatalf("start first read: %v", err)
	}
	firstResult, err := executor.ExecuteWithLease(ctx, first.Run, first.Step, plan.Steps[0], lease.Lease, "", 0)
	if err != nil || firstResult.Status != contracts.WorkflowStepSucceeded {
		t.Fatalf("execute first read result=%+v err=%v", firstResult, err)
	}
	_, err = workflowStore.CompleteStep(ctx, store.WorkflowStepCompleteInput{
		WorkspaceID: workspaceID, RunID: created.Run.ID, StepID: "read-1", Attempt: first.Step.Attempt,
		OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: actor,
		IdempotencyKey: "rate-limit-read-1-complete", Result: firstResult,
	})
	if err != nil {
		t.Fatalf("complete first read: %v", err)
	}
	second, err := workflowStore.StartStep(ctx, store.WorkflowStepStartInput{
		WorkspaceID: workspaceID, RunID: created.Run.ID, StepID: "read-2", OwnerID: lease.Lease.OwnerID,
		Fence: lease.Lease.Fence, Actor: actor, IdempotencyKey: "rate-limit-read-2-start",
	})
	if err != nil {
		t.Fatalf("start second read: %v", err)
	}
	deniedResult, err := executor.ExecuteWithLease(ctx, second.Run, second.Step, plan.Steps[1], lease.Lease, "", 0)
	if err != nil || deniedResult.Status != contracts.WorkflowStepAwaitingRetry || deniedResult.Wait == nil || deniedResult.Wait.ExpiresAt == nil {
		t.Fatalf("rate-limited admission result=%+v err=%v", deniedResult, err)
	}
	// Simulate process death after the admission transaction committed but
	// before CompleteStep stored its workflow checkpoint.
	runtime := &workflowruntime.Runtime{Store: workflowStore, Executor: executor}
	recovered, err := runtime.AdvanceBatch(ctx, workflowruntime.RunInput{
		WorkspaceID: workspaceID, RunID: created.Run.ID, Lease: lease.Lease, Actor: actor,
	})
	if err != nil {
		t.Fatalf("recover committed rate denial: %v", err)
	}
	if !recovered.Recovery || !recovered.Waiting || recovered.Run.Status != contracts.WorkflowStatusAwaitingRetry || !workflowStepHasStatus(recovered.Run, "read-2", contracts.WorkflowStepAwaitingRetry) {
		t.Fatalf("crash recovery did not commit the durable retry wait: %+v", recovered)
	}
	decision, err := admission.GetDecisionByIdempotencyKey(ctx, workspaceID, workflowStepAdmissionKey(created.Run.ID, "read-2", second.Step.Attempt))
	if err != nil || decision.RetryAt == nil {
		t.Fatalf("persisted rate decision=%+v err=%v", decision, err)
	}
	var retryDeadline *time.Time
	for _, state := range recovered.Run.Steps {
		if state.StepID == "read-2" {
			retryDeadline = state.NextRetryAt
			if state.Failure == nil || !state.Failure.Retryable {
				t.Fatalf("rate denial did not preserve retry classification: %+v", state.Failure)
			}
		}
	}
	if retryDeadline == nil || !retryDeadline.Equal(*decision.RetryAt) {
		t.Fatalf("workflow retry deadline=%v differs from admission retry=%v", retryDeadline, decision.RetryAt)
	}
	operation, err := operations.Get(ctx, workspaceID, created.Run.ID)
	if err != nil || operation.NextRetryAt == nil || !operation.NextRetryAt.Equal(*decision.RetryAt) || operation.Status != contracts.OperationStatusAwaitingRetry {
		t.Fatalf("operation retry queue lost admission deadline: operation=%+v err=%v", operation, err)
	}
	if connector.cap.calls.Load() != 1 {
		t.Fatalf("connector executions=%d, want exactly one; denial and recovery must not call connector", connector.cap.calls.Load())
	}
}

func workflowStepHasStatus(run contracts.WorkflowRun, stepID, status string) bool {
	for _, step := range run.Steps {
		if step.StepID == stepID {
			if step.Status != status {
				return false
			}
			return true
		}
	}
	return false
}

type rateLimitedReadConnector struct {
	ref contracts.ConnectorRef
	cap *rateLimitedReadCapability
}

func newRateLimitedReadConnector(workspaceID string) (*rateLimitedReadConnector, error) {
	ref := contracts.ConnectorRef{WorkspaceID: workspaceID, Name: "rate-test", Version: "1"}
	if err := ref.Normalize(); err != nil {
		return nil, err
	}
	definition := contracts.CapabilityDefinition{
		WorkspaceID:        workspaceID,
		Ref:                contracts.CapabilityRef{WorkspaceID: workspaceID, Connector: ref, Name: "record.read", Version: "1"},
		Description:        "integration fixture for durable capability rate admission",
		InputSchemaVersion: 1, InputSchemaHash: contracts.HashStrings("rate-test-input"),
		OutputSchemaVersion: 1, OutputSchemaHash: contracts.HashStrings("rate-test-output"),
		Effect: contracts.EffectClassReadOnly, Profile: contracts.DefaultExecutionProfile(),
		ResourceKinds: []string{"record"}, RetryPolicy: contracts.CapabilityRetryPolicy{MaxAttempts: 1, BackoffMS: 1, MaxBackoffMS: 1, Jitter: "none"},
		MaxRows: 10, RateLimitPerMinute: 1, Enabled: true, SupportsIdempotency: true,
	}
	if err := definition.Normalize(); err != nil {
		return nil, err
	}
	connector := &rateLimitedReadConnector{ref: ref}
	connector.cap = &rateLimitedReadCapability{parent: connector, definition: definition}
	return connector, nil
}

func (c *rateLimitedReadConnector) Definition() contracts.ConnectorRef { return c.ref }
func (c *rateLimitedReadConnector) Capabilities() []connectorruntime.Capability {
	return []connectorruntime.Capability{c.cap}
}
func (c *rateLimitedReadConnector) Health(context.Context) connectorruntime.HealthStatus {
	return connectorruntime.HealthStatus{Status: connectorruntime.HealthReady}
}

type rateLimitedReadCapability struct {
	parent     *rateLimitedReadConnector
	definition contracts.CapabilityDefinition
	calls      atomic.Int32
}

func (c *rateLimitedReadCapability) Definition() contracts.CapabilityDefinition { return c.definition }
func (c *rateLimitedReadCapability) Validate(request contracts.OperationRequest) error {
	return contracts.ValidateOperationRequest(request, c.definition)
}
func (c *rateLimitedReadCapability) Plan(request contracts.OperationRequest) (contracts.OperationPlan, error) {
	if err := c.Validate(request); err != nil {
		return contracts.OperationPlan{}, err
	}
	plan := contracts.OperationPlan{
		ID: request.ID + "-adapter-plan", OperationID: request.ID, OperationHash: request.StableHash(), WorkspaceID: request.WorkspaceID, Actor: request.Actor,
		Steps: []contracts.OperationStep{{ID: request.ID + "-read", Ordinal: 0, Kind: request.Capability.Name, Capability: request.Capability, Target: request.Target, Effect: c.definition.Effect, Profile: request.Profile, InputHash: request.InputHash}},
	}
	return plan, plan.Normalize()
}
func (c *rateLimitedReadCapability) Execute(ctx context.Context, request contracts.OperationRequest, plan contracts.OperationPlan) (contracts.OperationResult, error) {
	if err := c.Validate(request); err != nil {
		return contracts.OperationResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return contracts.OperationResult{}, err
	}
	if err := plan.Normalize(); err != nil {
		return contracts.OperationResult{}, err
	}
	c.calls.Add(1)
	outputHash := contracts.HashStrings("rate-read-result", request.ID, request.InputHash)
	result := contracts.OperationResult{
		ID: request.ID + "-result", OperationID: request.ID, OperationHash: request.StableHash(), RequestID: request.RequestID,
		WorkspaceID: request.WorkspaceID, Actor: request.Actor, Status: contracts.OperationStatusSucceeded,
		OutputSchemaVersion: c.definition.OutputSchemaVersion, OutputSchemaHash: c.definition.OutputSchemaHash, OutputHash: outputHash,
		Steps: []contracts.OperationStepResult{{StepID: plan.Steps[0].ID, Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: c.definition.OutputSchemaVersion, OutputSchemaHash: c.definition.OutputSchemaHash, OutputHash: outputHash}},
	}
	return result, result.Normalize()
}
