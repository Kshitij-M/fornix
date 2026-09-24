package incident

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/adapters/fakeincident"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/model"
	"github.com/omaveda/fornix/internal/store"
)

type incidentTestHarness struct {
	service   *Service
	pool      *pgxpool.Pool
	workspace string
}

func newIncidentTestHarness(t *testing.T) *incidentTestHarness {
	t.Helper()
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	workspace := fmt.Sprintf("test-incident-%d", time.Now().UnixNano())
	events := store.NewEventStore(pool)
	operations := store.NewOperationStore(pool, events)
	workflows := store.NewWorkflowStore(pool, events, operations)
	evidence := store.NewEvidenceStore(pool)
	artifacts := store.NewArtifactStore(pool)
	receipts := store.NewWorkReceiptStore(pool)
	registry := connector.NewRegistry()
	adapter, err := fakeincident.NewConnector(workspace)
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if err := registry.Register(adapter); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	providers := model.NewRegistry()
	fake := model.NewFakeProvider(model.FakeConfig{Response: "stable incident investigation"})
	if err := providers.Register(fake); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	gateway := model.NewGateway(providers, store.NewModelCallStore(pool))
	harness := &incidentTestHarness{service: NewService(store.NewIncidentStore(pool, events, evidence), workflows, evidence, artifacts, receipts, registry, gateway), pool: pool, workspace: workspace}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		// The workspace is unique per test. Delete only this workspace and honor
		// the same dependency order used by the production foreign keys.
		statements := []string{
			`DELETE FROM fornix.work_receipt_references WHERE workspace_id=$1`,
			`DELETE FROM fornix.work_receipt_steps WHERE workspace_id=$1`,
			`DELETE FROM fornix.work_receipts WHERE workspace_id=$1`,
			`DELETE FROM fornix.incident_approvals WHERE workspace_id=$1`,
			`DELETE FROM fornix.incident_event_deliveries WHERE workspace_id=$1`,
			`DELETE FROM fornix.incident_idempotency WHERE workspace_id=$1`,
			`DELETE FROM fornix.incident_records WHERE workspace_id=$1`,
			`DELETE FROM fornix.artifact_refs WHERE workspace_id=$1`,
			`DELETE FROM fornix.artifact_chunks WHERE workspace_id=$1`,
			`DELETE FROM fornix.artifacts WHERE workspace_id=$1`,
			`DELETE FROM fornix.model_calls WHERE workspace_id=$1`,
			`DELETE FROM fornix.workflow_transitions WHERE workspace_id=$1`,
			`DELETE FROM fornix.workflow_idempotency WHERE workspace_id=$1`,
			`DELETE FROM fornix.workflow_step_states WHERE workspace_id=$1`,
			`DELETE FROM fornix.workflow_runs WHERE workspace_id=$1`,
			`DELETE FROM fornix.operation_effects WHERE workspace_id=$1`,
			`DELETE FROM fornix.operation_attempts WHERE workspace_id=$1`,
			`DELETE FROM fornix.operation_callbacks WHERE workspace_id=$1`,
			`DELETE FROM fornix.operation_transitions WHERE workspace_id=$1`,
			`DELETE FROM fornix.operation_links WHERE workspace_id=$1`,
			`DELETE FROM fornix.operation_resources WHERE workspace_id=$1`,
			`DELETE FROM fornix.operation_steps WHERE workspace_id=$1`,
			`DELETE FROM fornix.operation_leases WHERE workspace_id=$1`,
			`DELETE FROM fornix.operation_idempotency WHERE workspace_id=$1`,
			`DELETE FROM fornix.operations WHERE workspace_id=$1`,
			`DELETE FROM fornix.provenance_edges WHERE workspace_id=$1`,
			`DELETE FROM fornix.evidence_records WHERE workspace_id=$1`,
			`DELETE FROM fornix.idempotency_records WHERE workspace_id=$1`,
			`DELETE FROM fornix.control_events WHERE workspace_id=$1`,
		}
		for _, statement := range statements {
			_, _ = pool.Exec(cleanupCtx, statement, workspace)
		}
		pool.Close()
	})
	return harness
}

func (h *incidentTestHarness) request(externalID, payload string) contracts.IncidentWorkflowRequest {
	actor := contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: h.workspace}
	return contracts.IncidentWorkflowRequest{
		Event: contracts.IncidentEvent{
			WorkspaceID: h.workspace, SourceSystem: "monitor", ExternalID: externalID,
			Severity: contracts.IncidentSeverityWarning, Payload: []byte(payload),
			DeliveryMode: contracts.IncidentDeliveryFake, IdempotencyKey: "incident:" + externalID,
			Actor: actor,
		},
		Actor: actor, IdempotencyKey: "incident:" + externalID, RequestID: "request:" + externalID,
	}
}

func TestIncidentWorkflowEndToEndDuplicateApprovalConflictIsolationAndReplay(t *testing.T) {
	h := newIncidentTestHarness(t)
	ctx := context.Background()
	request := h.request("external-1", `{"service":"payments","status":"degraded"}`)
	first, err := h.service.Start(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Workflow.Status != contracts.WorkflowStatusAwaitingApproval || first.Incident.Status != contracts.IncidentStatusAwaitingApproval {
		t.Fatalf("workflow did not pause for approval: status=%s incident=%s failure=%+v", first.Workflow.Status, first.Incident.Status, first.Workflow.Failure)
	}
	duplicate, err := h.service.Start(ctx, request)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Duplicate || duplicate.Workflow.ID != first.Workflow.ID {
		t.Fatalf("duplicate event/workflow was not idempotent: %+v", duplicate)
	}
	approved, err := h.service.Approve(ctx, h.workspace, first.Workflow.ID, "approve", "approval:external-1", request.Actor)
	if err != nil {
		t.Fatal(err)
	}
	if approved.Workflow.Status != contracts.WorkflowStatusSucceeded || approved.Incident.Status != contracts.IncidentStatusResolved || approved.Receipt == nil || !approved.ReplayVerify {
		t.Fatalf("approved workflow did not complete with receipt/replay: %+v", approved)
	}
	approvedAgain, err := h.service.Approve(ctx, h.workspace, first.Workflow.ID, "approve", "approval:external-1", request.Actor)
	if err != nil {
		t.Fatal(err)
	}
	if !approvedAgain.Duplicate || approvedAgain.Approval == nil {
		t.Fatalf("duplicate approval was not a read-only replay: %+v", approvedAgain)
	}
	replayed, err := h.service.Replay(ctx, h.workspace, first.Workflow.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	if !replayed.ReplayVerify || replayed.ReplayHash != approved.ReplayHash {
		t.Fatalf("replay hash changed: original=%s replay=%s", approved.ReplayHash, replayed.ReplayHash)
	}
	conflict := h.request("external-1", `{"service":"payments","status":"resolved"}`)
	conflict.Event.IdempotencyKey = "incident:conflict"
	if _, err := h.service.Start(ctx, conflict); !errors.Is(err, store.ErrIncidentConflict) {
		t.Fatalf("conflicting natural incident identity error=%v", err)
	}
	if _, err := h.service.Get(ctx, "other-workspace", first.Workflow.ID); !errors.Is(err, store.ErrWorkflowNotFound) {
		t.Fatalf("cross-workspace workflow read error=%v", err)
	}
}

func TestIncidentWorkflowCrashBeforeCheckpointIsResumable(t *testing.T) {
	h := newIncidentTestHarness(t)
	var failed atomic.Bool
	h.service.Workflows.SetFailureHook(func(point string) error {
		if point == "workflow_step_completed" && failed.CompareAndSwap(false, true) {
			return errors.New("simulated worker crash before checkpoint commit")
		}
		return nil
	})
	request := h.request("external-crash", `{"service":"search","status":"degraded"}`)
	if _, err := h.service.Start(context.Background(), request); err == nil {
		t.Fatal("simulated crash did not surface")
	}
	h.service.Workflows.SetFailureHook(nil)
	resumed, err := h.service.Start(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !resumed.Duplicate || resumed.Workflow.StateVersion == 0 || resumed.Workflow.Status != contracts.WorkflowStatusAwaitingApproval {
		t.Fatalf("crash recovery did not preserve/resume checkpoint: %+v", resumed)
	}
}

func TestIncidentWorkflowRejectedApprovalFailsClosed(t *testing.T) {
	h := newIncidentTestHarness(t)
	request := h.request("external-reject", `{"service":"billing","status":"degraded"}`)
	started, err := h.service.Start(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	rejected, err := h.service.Approve(context.Background(), h.workspace, started.Workflow.ID, "reject", "approval:external-reject", request.Actor)
	if err != nil {
		t.Fatal(err)
	}
	if rejected.Workflow.Status != contracts.WorkflowStatusFailed || rejected.Incident.Status != contracts.IncidentStatusFailed {
		t.Fatalf("rejected approval did not fail closed: workflow=%s incident=%s", rejected.Workflow.Status, rejected.Incident.Status)
	}
	if rejected.Receipt != nil {
		t.Fatal("rejected approval unexpectedly produced a completion receipt")
	}
	duplicate, err := h.service.Approve(context.Background(), h.workspace, started.Workflow.ID, "reject", "approval:external-reject", request.Actor)
	if err != nil {
		t.Fatal(err)
	}
	if !duplicate.Duplicate || duplicate.Receipt != nil {
		t.Fatalf("duplicate rejection was not a read-only durable result: %+v", duplicate)
	}
	if _, err := h.service.Approve(context.Background(), h.workspace, started.Workflow.ID, "approve", "approval:external-reject-2", request.Actor); !errors.Is(err, ErrIncidentApprovalRequired) {
		t.Fatalf("terminal workflow accepted a new approval: %v", err)
	}
}

func TestIncidentWorkflowApprovalIdempotencyCannotCrossRuns(t *testing.T) {
	h := newIncidentTestHarness(t)
	ctx := context.Background()
	firstRequest := h.request("external-approval-key-1", `{"service":"payments","status":"degraded"}`)
	first, err := h.service.Start(ctx, firstRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.service.Approve(ctx, h.workspace, first.Workflow.ID, "approve", "shared-approval-key", firstRequest.Actor); err != nil {
		t.Fatal(err)
	}
	secondRequest := h.request("external-approval-key-2", `{"service":"search","status":"degraded"}`)
	second, err := h.service.Start(ctx, secondRequest)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := h.service.Approve(ctx, h.workspace, second.Workflow.ID, "approve", "shared-approval-key", secondRequest.Actor); !errors.Is(err, store.ErrIncidentApprovalConflict) {
		t.Fatalf("approval idempotency key crossed runs: %v", err)
	}
}
