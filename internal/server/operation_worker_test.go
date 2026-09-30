package server

import (
	"context"
	"testing"

	"github.com/omaveda/fornix/internal/adapters/fakeincident"
	connectorruntime "github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func TestServerOperationWorkerExecutesReadOnlyOperationAndDeduplicates(t *testing.T) {
	srv, pool, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionOperationExecute})
	registry := connectorruntime.NewRegistry()
	adapter, err := fakeincident.NewConnector(workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if err := registry.TrustWorkspace(workspaceID, "server-worker-test"); err != nil {
		t.Fatal(err)
	}
	registry.RequireTrustPolicy(true)
	srv.connectorRegistry = registry
	srv.connectorExecutor = &connectorruntime.Executor{Registry: registry}
	principal, err := srv.auth.Authenticate(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	capability, ok := registry.LookupIdentity(workspaceID, fakeincident.ConnectorName, fakeincident.ConnectorVersion, "incident.read", "1")
	if !ok {
		t.Fatal("incident.read capability is not registered")
	}
	definition := capability.Definition()
	request := contracts.OperationRequest{
		ID: "server-worker-operation", RequestID: "server-worker-request", IdempotencyKey: "server-worker-create",
		WorkspaceID: workspaceID, Actor: principal.Actor(), Capability: definition.Ref,
		Target:    contracts.ResourceRef{WorkspaceID: workspaceID, System: contracts.SystemRef{WorkspaceID: workspaceID, Type: "incident", ID: "monitor", Version: "1"}, Kind: fakeincident.ResourceKind, ID: "incident-1", Version: "1"},
		InputType: fakeincident.InputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash,
		InputHash: contracts.HashStrings("server-worker-input"), Profile: definition.Profile,
	}
	plan, err := capability.Plan(request)
	if err != nil {
		t.Fatal(err)
	}
	created, err := srv.operations.Create(context.Background(), store.OperationCreateInput{Request: request, Plan: &plan})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := srv.newOperationWorker(workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.RunOnce(context.Background(), workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Claims != 1 || result.Handled != 1 || result.Released != 1 {
		t.Fatalf("worker result=%+v", result)
	}
	stored, err := srv.operations.Get(context.Background(), workspaceID, created.Operation.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != contracts.OperationStatusSucceeded {
		t.Fatalf("operation status=%q, want succeeded", stored.Status)
	}
	var admissions int
	if err := pool.QueryRow(context.Background(), `SELECT count(*)::int FROM fornix.operation_admission_decisions WHERE workspace_id=$1 AND operation_id=$2`, workspaceID, created.Operation.ID).Scan(&admissions); err != nil {
		t.Fatal(err)
	}
	if admissions != 1 {
		t.Fatalf("background worker durable admissions=%d, want exactly one", admissions)
	}
	second, err := runner.RunOnce(context.Background(), workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	if second.Claims != 0 {
		t.Fatalf("completed operation was delivered again: %+v", second)
	}
}

func TestServerOperationWorkerDoesNotClaimEffectfulOperation(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionOperationExecute})
	registry := connectorruntime.NewRegistry()
	adapter, err := fakeincident.NewConnector(workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if err := registry.TrustWorkspace(workspaceID, "server-worker-effect-test"); err != nil {
		t.Fatal(err)
	}
	registry.RequireTrustPolicy(true)
	srv.connectorRegistry = registry
	srv.connectorExecutor = &connectorruntime.Executor{Registry: registry}
	principal, err := srv.auth.Authenticate(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	capability, ok := registry.LookupIdentity(workspaceID, fakeincident.ConnectorName, fakeincident.ConnectorVersion, "incident.remediate", "1")
	if !ok {
		t.Fatal("incident.remediate capability is not registered")
	}
	definition := capability.Definition()
	request := contracts.OperationRequest{
		ID: "server-worker-effect", RequestID: "server-worker-effect-request", IdempotencyKey: "server-worker-effect-create",
		WorkspaceID: workspaceID, Actor: principal.Actor(), Capability: definition.Ref,
		Target:    contracts.ResourceRef{WorkspaceID: workspaceID, System: contracts.SystemRef{WorkspaceID: workspaceID, Type: "incident", ID: "monitor", Version: "1"}, Kind: fakeincident.ResourceKind, ID: "incident-1", Version: "1"},
		InputType: fakeincident.InputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash,
		InputHash: contracts.HashStrings("server-worker-effect-input"), Profile: definition.Profile,
	}
	plan, err := capability.Plan(request)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := srv.operations.Create(context.Background(), store.OperationCreateInput{Request: request, Plan: &plan}); err != nil {
		t.Fatal(err)
	}
	runner, err := srv.newOperationWorker(workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runner.RunOnce(context.Background(), workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	if result.Claims != 0 || result.Handled != 0 {
		t.Fatalf("effectful operation reached read-only worker: %+v", result)
	}
}
