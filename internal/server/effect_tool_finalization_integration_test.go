package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/effectdispatch"
	"github.com/omaveda/fornix/internal/store"
)

func TestToolAdapterCommitsSpecializedAndGenericResultsTogether(t *testing.T) {
	srv, pool, workspace, _ := newServerAuthTest(t, []contracts.Permission{contracts.PermissionOperationCreate, contracts.PermissionOperationExecute})
	ctx := context.Background()
	toolRuns := store.NewToolRunStore(pool, srv.events)
	domainLinks := store.NewDomainEffectLinkStore(pool)
	srv.toolRuns, srv.domainLinks = toolRuns, domainLinks
	runtime := &effectdispatch.Runtime{Operations: srv.operations, Admission: srv.admission, Links: domainLinks}
	effects := &domainEffectAdapters{runtime: runtime, toolRuns: toolRuns}
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for _, table := range []string{
			"domain_effect_link_transitions", "domain_effect_links", "operation_effect_transitions",
			"operation_effect_state", "operation_results", "operation_effects", "operation_attempts",
			"tool_runs", "operation_transitions", "operation_leases", "operation_resources",
			"operation_idempotency", "operations", "control_events", "idempotency_records",
		} {
			if _, err := pool.Exec(cleanupCtx, "DELETE FROM fornix."+table+" WHERE workspace_id=$1", workspace); err != nil {
				t.Errorf("cleanup %s: %v", table, err)
			}
		}
	})

	root := t.TempDir()
	definition := contracts.ToolDefinition{
		ID: "fixture.finalizer", Version: "1", Name: "finalizer fixture", Capability: "fixture.finalize",
		Executable: "/usr/bin/true", WorkdirRoot: root, Sandbox: contracts.DefaultSandboxProfile(), Enabled: true,
	}
	if err := definition.Normalize(); err != nil {
		t.Fatal(err)
	}
	request := contracts.ToolRequest{
		WorkspaceID: workspace, RequestID: "tool-finalizer-request", IdempotencyKey: "tool-finalizer-key",
		Actor:  contracts.ActorRef{ID: "test-worker", Kind: "service", WorkspaceID: workspace},
		ToolID: definition.ID, Capability: definition.Capability, Argv: []string{definition.Executable},
		Budget: definition.Sandbox, Workdir: root,
	}
	definitionHash, err := definition.Hash()
	if err != nil {
		t.Fatal(err)
	}
	profileHash, err := definition.Sandbox.Hash()
	if err != nil {
		t.Fatal(err)
	}
	request.ToolDefinitionHash, request.SandboxProfileHash = definitionHash, profileHash
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	run, duplicate, err := toolRuns.Reserve(ctx, request, contracts.ToolModeAutomatic)
	if err != nil || duplicate {
		t.Fatalf("reserve run=%+v duplicate=%t err=%v", run, duplicate, err)
	}
	run, err = toolRuns.MarkStarted(ctx, run)
	if err != nil || run.Status != contracts.ToolRunRunning {
		t.Fatalf("start run=%+v err=%v", run, err)
	}
	result := contracts.ToolResult{Status: contracts.ToolRunSucceeded, ExitCode: 0, Stdout: "atomic tool output"}
	var calls atomic.Int32
	invoke := func(context.Context, contracts.EffectAuthority) (contracts.ToolResult, error) {
		calls.Add(1)
		return result, nil
	}
	first, err := effects.Run(ctx, request, definition, run, invoke)
	if err != nil {
		t.Fatalf("first tool effect: %v", err)
	}
	if first.Hash() != result.Hash() || calls.Load() != 1 {
		t.Fatalf("first result=%+v calls=%d", first, calls.Load())
	}
	finalRun, err := toolRuns.GetByID(ctx, workspace, run.ID)
	if err != nil || finalRun.Status != contracts.ToolRunSucceeded || finalRun.Result == nil {
		t.Fatalf("durable tool result=%+v err=%v", finalRun, err)
	}
	link, err := domainLinks.CurrentByDomain(ctx, workspace, contracts.DomainEffectKindToolRun, run.ID, contracts.DomainEffectLinkRolePrimary)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := srv.admission.GetEffectState(ctx, workspace, link.Link.EffectID)
	if err != nil {
		t.Fatal(err)
	}
	operationResult, err := srv.operations.GetResult(ctx, workspace, link.Link.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if effect.State != contracts.ExternalEffectVerified || link.Transition.ToStatus != contracts.DomainEffectLinkStatusReconciled ||
		finalRun.Result.ContentHash != result.Hash() || effect.ResponseHash != result.Hash() || link.Transition.ResultHash != result.Hash() ||
		operationResult.ResultHash == "" || operationResult.Result.OutputHash != result.Hash() || operationResult.Result.Status != contracts.OperationStatusSucceeded {
		t.Fatalf("generic and specialized records disagree: run=%+v effect=%+v link=%+v operation=%+v", finalRun, effect, link, operationResult)
	}
	second, err := effects.Run(ctx, request, definition, run, invoke)
	if err != nil {
		t.Fatalf("duplicate tool effect replay: %v", err)
	}
	if second.Hash() != finalRun.Result.Hash() || calls.Load() != 1 {
		t.Fatalf("duplicate returned=%+v canonical=%+v invocations=%d", second, finalRun.Result, calls.Load())
	}
	var successEvents int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1 AND event_type=$2`, workspace, contracts.ToolEventSucceeded).Scan(&successEvents); err != nil {
		t.Fatal(err)
	}
	if successEvents != 1 {
		t.Fatalf("tool success events=%d want 1", successEvents)
	}
}
