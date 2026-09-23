package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func TestRuntimeFakeExecutorCompletesDeterministically(t *testing.T) {
	workflows, _, workspace, run := newRuntimeFixture(t, "runtime")
	lease, err := workflows.AcquireLease(context.Background(), workspace, run.ID, "runtime-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	executor := ExecutorFunc(func(_ context.Context, _ contracts.WorkflowRun, _ contracts.WorkflowStepState, _ contracts.OperationStep) (contracts.WorkflowStepResult, error) {
		calls.Add(1)
		return contracts.WorkflowStepResult{Status: contracts.WorkflowStepSucceeded, OutputHash: testWorkflowHash("runtime-output")}, nil
	})
	runtime := &Runtime{Store: workflows, Executor: executor}
	completed, err := runtime.Run(context.Background(), RunInput{WorkspaceID: workspace, RunID: run.ID, Lease: lease.Lease, Actor: run.Actor})
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != contracts.WorkflowStatusSucceeded || calls.Load() != 1 {
		t.Fatalf("completed=%+v calls=%d", completed, calls.Load())
	}
	replay, err := workflows.Replay(context.Background(), workspace, run.ID, 0, 100)
	if err != nil || !replay.Verified {
		t.Fatalf("replay=%+v err=%v", replay, err)
	}
}

func TestRuntimeConvertsCrashedRunningStepToRecovery(t *testing.T) {
	workflows, _, workspace, run := newRuntimeFixture(t, "recovery")
	lease, err := workflows.AcquireLease(context.Background(), workspace, run.ID, "crashed-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	started, err := workflows.StartStep(context.Background(), store.WorkflowStepStartInput{WorkspaceID: workspace, RunID: run.ID, StepID: "step-1", OwnerID: lease.Lease.OwnerID, Fence: lease.Lease.Fence, Actor: run.Actor, IdempotencyKey: "runtime-crash-start"})
	if err != nil {
		t.Fatal(err)
	}
	if started.Step.Status != contracts.WorkflowStepRunning {
		t.Fatalf("started=%+v", started)
	}
	runtime := &Runtime{Store: workflows, Executor: ExecutorFunc(func(context.Context, contracts.WorkflowRun, contracts.WorkflowStepState, contracts.OperationStep) (contracts.WorkflowStepResult, error) {
		t.Fatal("executor must not run before recovery is acknowledged")
		return contracts.WorkflowStepResult{}, nil
	})}
	result, err := runtime.Advance(context.Background(), RunInput{WorkspaceID: workspace, RunID: run.ID, Lease: lease.Lease, Actor: run.Actor})
	if err != nil {
		t.Fatal(err)
	}
	if !result.Recovery || result.Run.Status != contracts.WorkflowStatusRecoveryRequired {
		t.Fatalf("recovery result=%+v", result)
	}
}

func TestRuntimeParallelReadOnlyFanoutCommitsInPlanOrder(t *testing.T) {
	workflows, _, workspace, run := newRuntimeFixture(t, "parallel")
	lease, err := workflows.AcquireLease(context.Background(), workspace, run.ID, "parallel-worker", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var calls, active, maximum atomic.Int32
	executor := ExecutorFunc(func(_ context.Context, _ contracts.WorkflowRun, _ contracts.WorkflowStepState, step contracts.OperationStep) (contracts.WorkflowStepResult, error) {
		calls.Add(1)
		current := active.Add(1)
		for {
			previous := maximum.Load()
			if current <= previous || maximum.CompareAndSwap(previous, current) {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
		active.Add(-1)
		return contracts.WorkflowStepResult{Status: contracts.WorkflowStepSucceeded, OutputHash: testWorkflowHash(step.ID)}, nil
	})
	runtime := &Runtime{Store: workflows, Executor: executor}
	completed, err := runtime.Run(context.Background(), RunInput{WorkspaceID: workspace, RunID: run.ID, Lease: lease.Lease, Actor: run.Actor})
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != contracts.WorkflowStatusSucceeded || calls.Load() != 2 || maximum.Load() < 2 {
		t.Fatalf("parallel run=%+v calls=%d maximum_concurrency=%d", completed, calls.Load(), maximum.Load())
	}
}

func TestRuntimeIdentityHelpersAreStable(t *testing.T) {
	first := deterministicKey("run", "step", "1")
	second := deterministicKey("run", "step", "1")
	if first != second || deterministicKey("run", "step", "2") == first {
		t.Fatalf("deterministic keys are unstable: %q %q", first, second)
	}
	for _, status := range []string{contracts.WorkflowStatusAwaitingApproval, contracts.WorkflowStatusAwaitingHuman, contracts.WorkflowStatusAwaitingCallback, contracts.WorkflowStatusAwaitingRetry, contracts.WorkflowStatusAwaitingExternal, contracts.WorkflowStatusRecoveryRequired} {
		if !isWaiting(status) {
			t.Fatalf("status %q was not classified as waiting", status)
		}
	}
	if !contracts.IsTerminalWorkflowStatus(contracts.WorkflowStatusSucceeded) || isWaiting(contracts.WorkflowStatusSucceeded) {
		t.Fatal("terminal status classification is incorrect")
	}
}

func newRuntimeFixture(t *testing.T, suffix string) (*store.WorkflowStore, *pgxpool.Pool, string, contracts.WorkflowRun) {
	t.Helper()
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if err := store.ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	workspace := fmt.Sprintf("test-runtime-%s-%d", suffix, time.Now().UnixNano())
	events := store.NewEventStore(pool)
	operations := store.NewOperationStore(pool, events)
	workflows := store.NewWorkflowStore(pool, events, operations)
	request := runtimeRequest(workspace, suffix)
	steps := []contracts.OperationStep{{ID: "step-1", Ordinal: 0, Kind: contracts.WorkflowStepModel, Capability: request.Capability, Target: request.Target, Effect: contracts.EffectClassReadOnly, Profile: contracts.DefaultExecutionProfile(), InputHash: testWorkflowHash("step-input")}}
	if suffix == "parallel" {
		steps = append(steps, contracts.OperationStep{ID: "step-2", Ordinal: 1, Kind: contracts.WorkflowStepTool, Capability: request.Capability, Target: request.Target, Effect: contracts.EffectClassObservation, Profile: contracts.DefaultExecutionProfile(), InputHash: testWorkflowHash("step-input-2")})
	}
	plan := contracts.OperationPlan{ID: "plan-" + suffix, WorkspaceID: workspace, Actor: request.Actor, Steps: steps}
	created, err := workflows.Create(ctx, store.WorkflowCreateInput{Operation: store.OperationCreateInput{Request: request, Plan: &plan}, Budget: contracts.DefaultWorkflowBudget()})
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		for _, query := range []string{
			`DELETE FROM fornix.workflow_transitions WHERE workspace_id=$1`,
			`DELETE FROM fornix.workflow_idempotency WHERE workspace_id=$1`,
			`DELETE FROM fornix.workflow_step_states WHERE workspace_id=$1`,
			`DELETE FROM fornix.workflow_runs WHERE workspace_id=$1`,
			`DELETE FROM fornix.operation_transitions WHERE workspace_id=$1`,
			`DELETE FROM fornix.operation_steps WHERE workspace_id=$1`,
			`DELETE FROM fornix.operation_leases WHERE workspace_id=$1`,
			`DELETE FROM fornix.operation_idempotency WHERE workspace_id=$1`,
			`DELETE FROM fornix.operations WHERE workspace_id=$1`,
			`DELETE FROM fornix.control_events WHERE workspace_id=$1`,
			`DELETE FROM fornix.idempotency_records WHERE workspace_id=$1`,
		} {
			_, _ = pool.Exec(cleanupCtx, query, workspace)
		}
		pool.Close()
	})
	return workflows, pool, workspace, created.Run
}

func runtimeRequest(workspace, suffix string) contracts.OperationRequest {
	hash := testWorkflowHash("runtime-" + suffix)
	return contracts.OperationRequest{ID: "operation-" + suffix, RequestID: "request-" + suffix, WorkspaceID: workspace, IdempotencyKey: "runtime-" + suffix, Actor: contracts.ActorRef{ID: "runtime-actor", Kind: "human", WorkspaceID: workspace}, Capability: contracts.CapabilityRef{WorkspaceID: workspace, Connector: contracts.ConnectorRef{WorkspaceID: workspace, Name: "fixture", Version: "1"}, Name: "read", Version: "1", DefinitionHash: hash}, Target: contracts.ResourceRef{WorkspaceID: workspace, System: contracts.SystemRef{WorkspaceID: workspace, Type: "fixture", ID: "runtime", Version: "1"}, Kind: "record", ID: "runtime", Version: "1"}, InputType: "runtime.input", InputSchemaVersion: 1, InputSchemaHash: hash, InputHash: hash, Profile: contracts.DefaultExecutionProfile()}
}

func testWorkflowHash(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}
