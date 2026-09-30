package server

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/adapters/fakedomains"
	connectorruntime "github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/effectdispatch"
	"github.com/omaveda/fornix/internal/store"
	"github.com/omaveda/fornix/internal/testutil"
	genericworkflow "github.com/omaveda/fornix/internal/workflows/generic"
)

func TestAuthenticatedWorkflowCLIOverHTTPResumesUnknownEffectWithoutDuplicateVerifierCall(t *testing.T) {
	testutil.RequireLocalHTTP(t)
	permissions := []contracts.Permission{
		contracts.PermissionOperationCreate,
		contracts.PermissionOperationExecute,
		contracts.PermissionOperationRead,
		contracts.PermissionToolApprove,
		contracts.PermissionReceiptWrite,
	}
	srv, pool, workspaceID, token := newServerAuthTest(t, permissions)
	ctx := context.Background()
	principal, err := srv.auth.Authenticate(ctx, token)
	if err != nil {
		t.Fatal(err)
	}
	actor := principal.Actor()
	reviewer, err := srv.auth.CreateIdentity(ctx, contracts.IdentityInput{
		WorkspaceID: workspaceID, Subject: "workflow-reviewer", Kind: "user",
		Permissions: []contracts.Permission{contracts.PermissionToolApprove},
	})
	if err != nil {
		t.Fatalf("create distinct workflow reviewer: %v", err)
	}
	_, reviewerToken, err := srv.auth.CreateAPIKey(ctx, contracts.APIKeyInput{WorkspaceID: workspaceID, IdentityID: reviewer.ID})
	if err != nil {
		t.Fatalf("create reviewer API key: %v", err)
	}

	links := store.NewDomainEffectLinkStore(pool)
	dispatcher := &effectdispatch.Dispatcher{Operations: srv.operations, Admission: srv.admission, Links: links}
	workflowStore := store.NewWorkflowStore(pool, srv.events, srv.operations)
	srv.workReceipts = store.NewWorkReceiptStore(pool)
	domainConnector, err := fakedomains.NewConnector(workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	registry := connectorruntime.NewRegistry()
	if err := registry.Register(domainConnector); err != nil {
		t.Fatal(err)
	}
	var sendReply connectorruntime.Capability
	for _, capability := range domainConnector.Capabilities() {
		if capability.Definition().Ref.Name == "customer_support.reply_send" {
			sendReply = capability
			break
		}
	}
	if sendReply == nil {
		t.Fatal("fake customer-support reply capability is not registered")
	}
	definition := sendReply.Definition()
	target := contracts.ResourceRef{
		WorkspaceID: workspaceID,
		System:      contracts.SystemRef{WorkspaceID: workspaceID, Type: "support", ID: "helpdesk", Version: "1"},
		Kind:        fakedomains.SupportKind, ID: "case-1", Version: "1", ContentHash: contracts.HashStrings("support-case"),
	}
	operationID := contracts.NewID("workflow-http")
	operation := contracts.OperationRequest{
		ID: operationID, RequestID: operationID + "-request", IdempotencyKey: operationID + "-idempotency",
		WorkspaceID: workspaceID, Actor: actor, Capability: definition.Ref, Target: target,
		InputType: fakedomains.SupportInput, InputSchemaVersion: definition.InputSchemaVersion,
		InputSchemaHash: definition.InputSchemaHash, InputHash: contracts.HashStrings("support-reply"), Profile: definition.Profile,
	}
	if err := operation.Normalize(); err != nil {
		t.Fatal(err)
	}
	plan := contracts.OperationPlan{
		ID: operationID + "-plan", OperationID: operationID, OperationHash: operation.StableHash(),
		WorkspaceID: workspaceID, Actor: actor,
		Steps: []contracts.OperationStep{
			{ID: "approval", Ordinal: 0, Kind: contracts.WorkflowStepApproval, Capability: definition.Ref, Target: target, Effect: contracts.EffectClassObservation, Profile: contracts.DefaultExecutionProfile(), InputHash: contracts.HashStrings("reply-approval")},
			{ID: "reply", Ordinal: 1, Kind: contracts.WorkflowStepConnector, Capability: definition.Ref, Target: target, DependsOn: []string{"approval"}, Effect: definition.Effect, Profile: definition.Profile, InputHash: operation.InputHash},
		},
	}
	service := genericworkflow.NewService(workflowStore, srv.workReceipts)
	connectorExecutor := genericworkflow.NewConnectorStepExecutor(registry, &connectorruntime.Executor{Registry: registry}, srv.operations)
	connectorExecutor.SetEffectBoundary(srv.admission, dispatcher)
	workflowExecutor := &approvalThenConnectorExecutorForServer{connector: connectorExecutor}
	service.Executor = workflowExecutor
	service.Effects = dispatcher
	verifier := &httpRetrySequenceVerifier{}
	service.Verifier = verifier
	srv.genericWorkflows = service

	handler := withRequestMiddleware(srv.securityMiddleware(srv.routes()), 1<<20)
	cliBinary := buildFornixCLIForIntegrationTest(t)
	api := httptest.NewServer(handler)
	defer api.Close()
	create := contracts.WorkflowCreateRequest{WorkspaceID: workspaceID, Operation: operation, Plan: plan, Budget: contracts.DefaultWorkflowBudget(), Idempotency: operation.IdempotencyKey}
	createPayload, err := json.Marshal(create)
	if err != nil {
		t.Fatal(err)
	}
	requestPath := filepath.Join(t.TempDir(), "workflow-request.json")
	if err := os.WriteFile(requestPath, createPayload, 0o600); err != nil {
		t.Fatal(err)
	}
	created := runFornixCLIJSON[struct {
		Run contracts.WorkflowRun `json:"run"`
	}](t, cliBinary, api.URL, workspaceID, token, "workflow", "create", "--request-file", requestPath, "--idempotency", operation.IdempotencyKey)
	runID := created.Run.ID
	if runID == "" {
		t.Fatal("CLI create response omitted workflow run ID")
	}
	leaseEnvelope := runFornixCLIJSON[struct {
		Lease store.WorkflowLease `json:"lease"`
	}](t, cliBinary, api.URL, workspaceID, token, "workflow", "lease", "--id", runID, "--ttl-ms", "120000")
	fence := leaseEnvelope.Lease.Fence
	if fence == 0 {
		t.Fatal("CLI workflow lease response omitted its fence")
	}
	waiting := runFornixCLIJSON[struct {
		Run contracts.WorkflowRun `json:"run"`
	}](t, cliBinary, api.URL, workspaceID, token, "workflow", "advance", "--id", runID, "--fence", strconv.FormatUint(fence, 10))
	if waiting.Run.Status != contracts.WorkflowStatusAwaitingApproval {
		t.Fatalf("first CLI advance status=%s, want awaiting approval", waiting.Run.Status)
	}
	approved := runFornixCLIJSON[struct {
		Run contracts.WorkflowRun `json:"run"`
	}](t, cliBinary, api.URL, workspaceID, reviewerToken, "workflow", "approve", "--id", runID, "--fence", strconv.FormatUint(fence, 10), "--step-id", "approval")
	if approved.Run.Steps[0].Status != contracts.WorkflowStepSucceeded {
		t.Fatalf("distinct reviewer approval status=%s, want succeeded", approved.Run.Steps[0].Status)
	}
	dispatchedEnvelope := runFornixCLIJSON[struct {
		Run contracts.WorkflowRun `json:"run"`
	}](t, cliBinary, api.URL, workspaceID, token, "workflow", "advance", "--id", runID, "--fence", strconv.FormatUint(fence, 10))
	if dispatchedEnvelope.Run.Status != contracts.WorkflowStatusAwaitingExternal {
		t.Fatalf("second CLI advance status=%s, want awaiting external", dispatchedEnvelope.Run.Status)
	}
	var effect *contracts.ExternalEffect
	for _, step := range dispatchedEnvelope.Run.Steps {
		if step.StepID == "reply" {
			effect = step.Effect
		}
	}
	if effect == nil || effect.ID == "" || dispatchedEnvelope.Run.Status != contracts.WorkflowStatusAwaitingExternal {
		t.Fatalf("reply step did not durably wait on its effect: %+v", dispatchedEnvelope.Run)
	}
	if workflowExecutor.ConnectorCalls() != 1 {
		t.Fatalf("connector dispatch calls=%d, want exactly one", workflowExecutor.ConnectorCalls())
	}
	effectLeaseEnvelope := runFornixCLIJSON[struct {
		Lease struct {
			Fence uint64 `json:"fence"`
		} `json:"lease"`
	}](t, cliBinary, api.URL, workspaceID, token, "operation", "effect-lease", "--id", runID, "--effect-id", effect.ID, "--ttl-ms", "120000")
	effectFence := effectLeaseEnvelope.Lease.Fence
	if effectFence == 0 {
		t.Fatal("CLI effect recovery lease response omitted its fence")
	}
	state, err := srv.admission.GetEffectState(ctx, workspaceID, effect.ID)
	if err != nil {
		t.Fatal(err)
	}
	link, err := links.CurrentByDomain(ctx, workspaceID, contracts.DomainEffectKindWorkflowStep, runID+":reply", contracts.DomainEffectLinkRolePrimary)
	if err != nil {
		t.Fatal(err)
	}
	verifyArgs := []string{"workflow", "verify", "--id", runID, "--fence", strconv.FormatUint(fence, 10), "--effect-fence", strconv.FormatUint(effectFence, 10), "--step-id", "reply", "--expected-effect-version", strconv.FormatInt(state.Version, 10), "--expected-link-version", strconv.FormatInt(link.Transition.Version, 10), "--idempotency", "http-verify-unknown"}
	workflowStore.SetFailureHook(func(point string) error {
		if point == "workflow_step_completed" {
			return fmt.Errorf("injected workflow checkpoint crash")
		}
		return nil
	})
	crashed, crashErr := runFornixCLIProcess(t, cliBinary, api.URL, workspaceID, token, verifyArgs...)
	workflowStore.SetFailureHook(nil)
	if crashErr == nil {
		t.Fatalf("verification was expected to fail after the proof transaction, output=%s", crashed)
	}
	if strings.Contains(string(crashed), token) {
		t.Fatal("API key appeared in failed CLI output")
	}
	unknown := runFornixCLIJSON[contracts.WorkflowVerificationResult](t, cliBinary, api.URL, workspaceID, token, verifyArgs...)
	if verifier.Calls() != 1 {
		t.Fatalf("same-key proof resume invoked verifier %d times, want 1", verifier.Calls())
	}
	if unknown.Outcome.Status != contracts.EffectVerificationStatusUnknown || unknown.Run.Steps[1].Status != contracts.WorkflowStepRecoveryRequired {
		t.Fatalf("resumed unknown result=%+v", unknown)
	}
	_ = runFornixCLIJSON[contracts.WorkflowVerificationResult](t, cliBinary, api.URL, workspaceID, token, verifyArgs...)
	if verifier.Calls() != 1 || workflowExecutor.ConnectorCalls() != 1 {
		t.Fatalf("duplicate verification calls: verifier=%d connector=%d, want 1 each", verifier.Calls(), workflowExecutor.ConnectorCalls())
	}

	state, err = srv.admission.GetEffectState(ctx, workspaceID, effect.ID)
	if err != nil {
		t.Fatal(err)
	}
	link, err = links.CurrentByDomain(ctx, workspaceID, contracts.DomainEffectKindWorkflowStep, runID+":reply", contracts.DomainEffectLinkRolePrimary)
	if err != nil {
		t.Fatal(err)
	}
	retryArgs := []string{"workflow", "verify", "--id", runID, "--fence", strconv.FormatUint(fence, 10), "--effect-fence", strconv.FormatUint(effectFence, 10), "--step-id", "reply", "--expected-effect-version", strconv.FormatInt(state.Version, 10), "--expected-link-version", strconv.FormatInt(link.Transition.Version, 10)}
	verified := runFornixCLIJSON[contracts.WorkflowVerificationResult](t, cliBinary, api.URL, workspaceID, token, retryArgs...)
	if verifier.Calls() != 2 || workflowExecutor.ConnectorCalls() != 1 {
		t.Fatalf("fresh proof calls: verifier=%d connector=%d, want 2 and 1", verifier.Calls(), workflowExecutor.ConnectorCalls())
	}
	if verified.Outcome.Status != contracts.EffectVerificationStatusVerified || verified.Run.Status != contracts.WorkflowStatusSucceeded {
		t.Fatalf("fresh proof did not complete workflow: %+v", verified)
	}
	replay := runFornixCLIJSON[struct {
		Verified bool `json:"verified"`
	}](t, cliBinary, api.URL, workspaceID, token, "workflow", "replay", "--id", runID, "--limit", strconv.Itoa(genericworkflow.MaxReplayLimit))
	if !replay.Verified {
		t.Fatal("CLI replay did not verify the workflow history")
	}
	receipt := runFornixCLIJSON[struct {
		Receipt json.RawMessage `json:"receipt"`
	}](t, cliBinary, api.URL, workspaceID, token, "workflow", "receipt", "--id", runID, "--finalize", "true")
	if len(receipt.Receipt) == 0 {
		t.Fatal("CLI receipt finalization returned no receipt")
	}
}

func buildFornixCLIForIntegrationTest(t *testing.T) string {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate integration test source for CLI build")
	}
	moduleRoot := filepath.Clean(filepath.Join(filepath.Dir(sourceFile), "../.."))
	binary := filepath.Join(t.TempDir(), "fornix")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	command := exec.CommandContext(ctx, "go", "build", "-o", binary, "./cmd/fornix")
	command.Dir = moduleRoot
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("build Fornix CLI for authenticated integration test: %v\n%s", err, output)
	}
	return binary
}

func runFornixCLIProcess(t *testing.T, binary, baseURL, workspaceID, token string, args ...string) ([]byte, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	commandArgs := []string{"--url", baseURL, "--workspace", workspaceID}
	commandArgs = append(commandArgs, args...)
	command := exec.CommandContext(ctx, binary, commandArgs...)
	env := make([]string, 0, len(os.Environ())+1)
	for _, entry := range os.Environ() {
		name, _, _ := strings.Cut(entry, "=")
		if name == "FORNIX_KEY" || name == "FORNIX_URL" || name == "FORNIX_WORKSPACE_ID" {
			continue
		}
		env = append(env, entry)
	}
	command.Env = append(env, "FORNIX_KEY="+token)
	return command.CombinedOutput()
}

func runFornixCLIJSON[T any](t *testing.T, binary, baseURL, workspaceID, token string, args ...string) T {
	t.Helper()
	output, err := runFornixCLIProcess(t, binary, baseURL, workspaceID, token, args...)
	if err != nil {
		t.Fatalf("fornix %s failed: %v\n%s", strings.Join(args[:min(len(args), 2)], " "), err, output)
	}
	var result T
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode fornix %s response: %v\n%s", strings.Join(args[:min(len(args), 2)], " "), err, output)
	}
	return result
}

type approvalThenConnectorExecutorForServer struct {
	connector *genericworkflow.ConnectorStepExecutor
	calls     atomic.Int32
}

func (e *approvalThenConnectorExecutorForServer) ConnectorCalls() int32 { return e.calls.Load() }

func (e *approvalThenConnectorExecutorForServer) Execute(ctx context.Context, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep) (contracts.WorkflowStepResult, error) {
	if step.Kind == contracts.WorkflowStepApproval {
		return contracts.WorkflowStepResult{Status: contracts.WorkflowStepAwaitingApproval, Wait: &contracts.WorkflowWait{
			Kind: contracts.WorkflowWaitApproval, Token: contracts.HashStrings("http-approval", run.ID, step.ID), Reason: "explicit approval is required",
		}}, nil
	}
	e.calls.Add(1)
	return e.connector.Execute(ctx, run, state, step)
}

func (e *approvalThenConnectorExecutorForServer) ExecuteWithLease(ctx context.Context, run contracts.WorkflowRun, state contracts.WorkflowStepState, step contracts.OperationStep, lease store.WorkflowLease, taskOwner string, taskFence uint64) (contracts.WorkflowStepResult, error) {
	if step.Kind == contracts.WorkflowStepApproval {
		return e.Execute(ctx, run, state, step)
	}
	e.calls.Add(1)
	return e.connector.ExecuteWithLease(ctx, run, state, step, lease, taskOwner, taskFence)
}

type httpRetrySequenceVerifier struct {
	calls atomic.Int32
}

func (v *httpRetrySequenceVerifier) Calls() int32 { return v.calls.Load() }

func (v *httpRetrySequenceVerifier) VerifyEffect(_ context.Context, request contracts.EffectVerificationRequest) (contracts.EffectVerificationResult, error) {
	if v.calls.Add(1) == 1 {
		return contracts.EffectVerificationResult{Status: contracts.EffectVerificationStatusUnknown, ProviderRequestID: request.Effect.ProviderRequestID, FailureCode: "verification_inconclusive"}, nil
	}
	resultHash := contracts.HashStrings("http-verified-result", request.EffectID)
	return contracts.EffectVerificationResult{Status: contracts.EffectVerificationStatusVerified, ProviderRequestID: request.Effect.ProviderRequestID, ResultHash: resultHash, VerificationHash: contracts.HashStrings("http-verification-proof", request.Link.DomainHash, resultHash)}, nil
}
