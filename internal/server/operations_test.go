package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/adapters/fakeincident"
	connectorruntime "github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func TestOperationAdmissionHTTPStatusMapsCapabilityRateLimit(t *testing.T) {
	if got := operationAdmissionHTTPStatus(contracts.AdmissionDecision{ReasonCode: contracts.AdmissionReasonRateLimited}); got != http.StatusTooManyRequests {
		t.Fatalf("rate-limited admission HTTP status=%d, want 429", got)
	}
	if got := operationAdmissionHTTPStatus(contracts.AdmissionDecision{ReasonCode: contracts.AdmissionReasonQuotaExceeded}); got != http.StatusConflict {
		t.Fatalf("non-rate admission denial HTTP status=%d, want conflict", got)
	}
}

func TestGenericOperationHTTPIsIdempotentFencedReplayableAndWorkspaceScoped(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{
		contracts.PermissionOperationRead,
		contracts.PermissionOperationCreate,
		contracts.PermissionOperationExecute,
	})
	handler := withRequestMiddleware(srv.securityMiddleware(srv.routes()), 2<<20)
	request := genericOperationRequest(workspaceID)
	body, err := json.Marshal(operationCreateRequest{Request: request, Idempotency: request.IdempotencyKey})
	if err != nil {
		t.Fatal(err)
	}

	create := func(requestBody []byte) map[string]any {
		response := performOperationRequest(handler, token, workspaceID, http.MethodPost, "/v1/operations", requestBody, nil)
		if response.Code != http.StatusOK {
			t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
		}
		return decodeOperationJSON(t, response)
	}
	created := create(body)
	operation := objectValue(t, created, "operation")
	operationID := stringValue(t, operation, "id")
	if operationID == "" {
		t.Fatal("create did not return operation id")
	}
	mismatch := performOperationRequest(handler, token, workspaceID, http.MethodPost, "/v1/operations", body, map[string]string{"Idempotency-Key": "different-create-key"})
	if mismatch.Code != http.StatusBadRequest {
		t.Fatalf("idempotency mismatch status=%d body=%s", mismatch.Code, mismatch.Body.String())
	}
	duplicate := create(body)
	if boolValue(t, duplicate, "duplicate") != true {
		t.Fatalf("duplicate create response=%s", responseJSON(duplicate))
	}

	leasePath := "/v1/operations/" + operationID + "/lease?workspace_id=" + workspaceID
	leaseResponse := performOperationRequest(handler, token, workspaceID, http.MethodPost, leasePath, nil, nil)
	if leaseResponse.Code != http.StatusOK {
		t.Fatalf("lease status=%d body=%s", leaseResponse.Code, leaseResponse.Body.String())
	}
	lease := objectValue(t, decodeOperationJSON(t, leaseResponse), "lease")
	fence := stringValue(t, lease, "fence")
	if fence == "" || fence == "0" {
		t.Fatalf("invalid operation fence=%s", fence)
	}
	leaseHeaders := map[string]string{"X-Operation-Fence": fence}
	renewResponse := performOperationRequest(handler, token, workspaceID, http.MethodPost, "/v1/operations/"+operationID+"/renew?workspace_id="+workspaceID, []byte(`{"ttl_ms":30000}`), leaseHeaders)
	if renewResponse.Code != http.StatusOK {
		t.Fatalf("renew status=%d body=%s", renewResponse.Code, renewResponse.Body.String())
	}

	transitionPath := "/v1/operations/" + operationID + "/transition?workspace_id=" + workspaceID
	transitionBody := []byte(`{"to_status":"planned","reason_code":"operator-test","idempotency_key":"operation-transition-http-1"}`)
	transitionHeaders := map[string]string{"X-Operation-Fence": fence, "Idempotency-Key": "operation-transition-http-1"}
	transitionResponse := performOperationRequest(handler, token, workspaceID, http.MethodPost, transitionPath, transitionBody, transitionHeaders)
	if transitionResponse.Code != http.StatusOK {
		t.Fatalf("transition status=%d body=%s", transitionResponse.Code, transitionResponse.Body.String())
	}
	transition := decodeOperationJSON(t, transitionResponse)
	if stringValue(t, objectValue(t, transition, "operation"), "status") != contracts.OperationStatusPlanned {
		t.Fatalf("unexpected transition response=%s", responseJSON(transition))
	}
	duplicateTransitionResponse := performOperationRequest(handler, token, workspaceID, http.MethodPost, transitionPath, transitionBody, transitionHeaders)
	if duplicateTransitionResponse.Code != http.StatusOK {
		t.Fatalf("duplicate transition status=%d body=%s", duplicateTransitionResponse.Code, duplicateTransitionResponse.Body.String())
	}
	if !boolValue(t, decodeOperationJSON(t, duplicateTransitionResponse), "duplicate") {
		t.Fatal("duplicate transition was not reported as duplicate")
	}

	releaseResponse := performOperationRequest(handler, token, workspaceID, http.MethodPost, "/v1/operations/"+operationID+"/release?workspace_id="+workspaceID, nil, leaseHeaders)
	if releaseResponse.Code != http.StatusOK {
		t.Fatalf("release status=%d body=%s", releaseResponse.Code, releaseResponse.Body.String())
	}
	if _, err := srv.operations.AcquireLease(context.Background(), workspaceID, operationID, "takeover-owner", time.Minute); err != nil {
		t.Fatalf("take over operation lease: %v", err)
	}
	// Use the old fence after a durable takeover. A zero fence is rejected at
	// input normalization, so it would not exercise the stale-owner guard.
	staleResponse := performOperationRequest(handler, token, workspaceID, http.MethodPost, transitionPath, []byte(`{"to_status":"admitted","reason_code":"stale","idempotency_key":"operation-transition-http-stale"}`), map[string]string{"X-Operation-Fence": fence})
	if staleResponse.Code != http.StatusConflict {
		t.Fatalf("stale fence status=%d body=%s", staleResponse.Code, staleResponse.Body.String())
	}

	replayPath := "/v1/operations/" + operationID + "/replay?workspace_id=" + workspaceID
	replayResponse := performOperationRequest(handler, token, workspaceID, http.MethodPost, replayPath, nil, nil)
	if replayResponse.Code != http.StatusOK {
		t.Fatalf("replay status=%d body=%s", replayResponse.Code, replayResponse.Body.String())
	}
	replay := decodeOperationJSON(t, replayResponse)
	if !boolValue(t, replay, "verified") || stringValue(t, replay, "replay_hash") == "" {
		t.Fatalf("replay was not verified=%s", responseJSON(replay))
	}

	foreign := performOperationRequest(handler, token, "foreign-workspace", http.MethodGet, "/v1/operations/"+operationID+"?workspace_id=foreign-workspace", nil, nil)
	if foreign.Code != http.StatusForbidden {
		t.Fatalf("cross-workspace read status=%d body=%s", foreign.Code, foreign.Body.String())
	}
}

func TestGenericOperationHTTPClaimsReadyWorkWithWorkspaceFence(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionOperationCreate, contracts.PermissionOperationExecute})
	handler := withRequestMiddleware(srv.securityMiddleware(srv.routes()), 2<<20)
	for index := 0; index < 2; index++ {
		request := genericOperationRequest(workspaceID)
		request.ID = "operation-claim-http-" + strconv.Itoa(index)
		request.RequestID = "request-claim-http-" + strconv.Itoa(index)
		request.IdempotencyKey = "operation-claim-http-" + strconv.Itoa(index)
		body, err := json.Marshal(operationCreateRequest{Request: request, Idempotency: request.IdempotencyKey})
		if err != nil {
			t.Fatal(err)
		}
		response := performOperationRequest(handler, token, workspaceID, http.MethodPost, "/v1/operations", body, nil)
		if response.Code != http.StatusOK {
			t.Fatalf("create status=%d body=%s", response.Code, response.Body.String())
		}
	}
	claimPath := "/v1/operations/claims?workspace_id=" + workspaceID
	claimed := performOperationRequest(handler, token, workspaceID, http.MethodPost, claimPath, []byte(`{"limit":2,"ttl_ms":30000}`), nil)
	if claimed.Code != http.StatusOK {
		t.Fatalf("claim status=%d body=%s", claimed.Code, claimed.Body.String())
	}
	claimJSON := decodeOperationJSON(t, claimed)
	claimItems, ok := claimJSON["claims"].([]any)
	if !ok || len(claimItems) != 2 {
		t.Fatalf("claims=%s, want two", responseJSON(claimJSON))
	}
	for _, raw := range claimItems {
		item, ok := raw.(map[string]any)
		if !ok {
			t.Fatalf("claim item is not an object: %#v", raw)
		}
		lease := objectValue(t, item, "lease")
		if stringValue(t, lease, "workspace_id") != workspaceID || stringValue(t, lease, "owner_id") == "" || stringValue(t, lease, "fence") != "1" {
			t.Fatalf("invalid claim lease=%s", responseJSON(lease))
		}
	}
	duplicate := performOperationRequest(handler, token, workspaceID, http.MethodPost, claimPath, []byte(`{"limit":2,"ttl_ms":30000}`), nil)
	if duplicate.Code != http.StatusOK || stringValue(t, decodeOperationJSON(t, duplicate), "count") != "0" {
		t.Fatalf("active leases were claimed again status=%d body=%s", duplicate.Code, duplicate.Body.String())
	}
	foreign := performOperationRequest(handler, token, "foreign-workspace", http.MethodPost, "/v1/operations/claims?workspace_id=foreign-workspace", []byte(`{"limit":1}`), nil)
	if foreign.Code != http.StatusForbidden {
		t.Fatalf("cross-workspace claim status=%d body=%s", foreign.Code, foreign.Body.String())
	}
}

func TestGenericOperationHTTPQualificationLatency(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionOperationCreate})
	handler := withRequestMiddleware(srv.securityMiddleware(srv.routes()), 2<<20)
	samples := make([]time.Duration, 0, 20)
	for index := 0; index < 20; index++ {
		request := genericOperationRequest(workspaceID)
		request.ID = "operation-http-latency-" + strconv.Itoa(index)
		request.RequestID = "request-http-latency-" + strconv.Itoa(index)
		request.IdempotencyKey = "operation-create-http-latency-" + strconv.Itoa(index)
		body, err := json.Marshal(operationCreateRequest{Request: request, Idempotency: request.IdempotencyKey})
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		response := performOperationRequest(handler, token, workspaceID, http.MethodPost, "/v1/operations", body, nil)
		if response.Code != http.StatusOK {
			t.Fatalf("latency create status=%d body=%s", response.Code, response.Body.String())
		}
		samples = append(samples, time.Since(started))
	}
	sort.Slice(samples, func(left, right int) bool { return samples[left] < samples[right] })
	t.Logf("generic operation HTTP create samples=%d p50=%s p95=%s max=%s", len(samples), samples[len(samples)/2], samples[len(samples)*95/100], samples[len(samples)-1])
}

func TestGenericOperationHTTPExecutesTrustedReadAndDeduplicates(t *testing.T) {
	srv, pool, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionOperationRead, contracts.PermissionOperationCreate, contracts.PermissionOperationExecute})
	registry := connectorruntime.NewRegistry()
	adapter, err := fakeincident.NewConnector(workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if err := registry.TrustWorkspace(workspaceID, "test-v1"); err != nil {
		t.Fatal(err)
	}
	registry.RequireTrustPolicy(true)
	srv.connectorRegistry = registry
	srv.connectorExecutor = &connectorruntime.Executor{Registry: registry}
	handler := withRequestMiddleware(srv.securityMiddleware(srv.routes()), 2<<20)
	capability, ok := registry.LookupIdentity(workspaceID, fakeincident.ConnectorName, fakeincident.ConnectorVersion, "incident.read", "1")
	if !ok {
		t.Fatal("incident read capability was not registered")
	}
	definition := capability.Definition()
	request := contracts.OperationRequest{
		WorkspaceID: workspaceID, IdempotencyKey: "generic-execute-key",
		Capability: definition.Ref, Target: contracts.ResourceRef{WorkspaceID: workspaceID, System: contracts.SystemRef{WorkspaceID: workspaceID, Type: "incident", ID: "monitor", Version: "1"}, Kind: fakeincident.ResourceKind, ID: "incident-1", Version: "1"},
		InputType: fakeincident.InputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: contracts.HashStrings("incident-input"), Profile: definition.Profile,
	}
	body, err := json.Marshal(operationCreateRequest{Request: request, Idempotency: request.IdempotencyKey})
	if err != nil {
		t.Fatal(err)
	}
	createdResponse := performOperationRequest(handler, token, workspaceID, http.MethodPost, "/v1/operations", body, nil)
	if createdResponse.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", createdResponse.Code, createdResponse.Body.String())
	}
	operationID := stringValue(t, objectValue(t, decodeOperationJSON(t, createdResponse), "operation"), "id")
	executePath := "/v1/operations/" + operationID + "/execute?workspace_id=" + workspaceID
	executeResponse := performOperationRequest(handler, token, workspaceID, http.MethodPost, executePath, []byte(`{"idempotency_key":"generic-execute-delivery"}`), nil)
	if executeResponse.Code != http.StatusOK {
		t.Fatalf("execute status=%d body=%s", executeResponse.Code, executeResponse.Body.String())
	}
	executed := decodeOperationJSON(t, executeResponse)
	if stringValue(t, objectValue(t, executed, "operation"), "status") != contracts.OperationStatusSucceeded {
		t.Fatalf("operation did not succeed: %s", responseJSON(executed))
	}
	result := objectValue(t, executed, "result")
	if stringValue(t, result, "result_hash") == "" {
		t.Fatalf("missing durable result: %s", responseJSON(executed))
	}
	duplicateResponse := performOperationRequest(handler, token, workspaceID, http.MethodPost, executePath, []byte(`{"idempotency_key":"different-delivery"}`), nil)
	if duplicateResponse.Code != http.StatusOK {
		t.Fatalf("duplicate execute status=%d body=%s", duplicateResponse.Code, duplicateResponse.Body.String())
	}
	if !boolValue(t, decodeOperationJSON(t, duplicateResponse), "duplicate") {
		t.Fatal("duplicate execution was not reported")
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.operation_results WHERE workspace_id=$1 AND operation_id=$2`, workspaceID, operationID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("durable result count=%d, want 1", count)
	}
	var authorityLinks int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.operation_authority_links WHERE workspace_id=$1 AND operation_id=$2`, workspaceID, operationID).Scan(&authorityLinks); err != nil {
		t.Fatal(err)
	}
	if authorityLinks != 2 {
		t.Fatalf("generic execution authority link count=%d, want admission and result", authorityLinks)
	}
	inspected := performOperationRequest(handler, token, workspaceID, http.MethodGet, "/v1/operations/"+operationID+"/authority-links?workspace_id="+workspaceID, nil, nil)
	if inspected.Code != http.StatusOK {
		t.Fatalf("authority inspection status=%d body=%s", inspected.Code, inspected.Body.String())
	}
	inspectedJSON := decodeOperationJSON(t, inspected)
	links, ok := inspectedJSON["links"].([]any)
	if !ok || len(links) != 2 {
		t.Fatalf("authority inspection did not return admission and result links: %s", inspected.Body.String())
	}
}

func TestGenericOperationHTTPRejectsEffectfulCapabilityWithoutReservation(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionOperationRead, contracts.PermissionOperationCreate, contracts.PermissionOperationExecute})
	registry := connectorruntime.NewRegistry()
	adapter, err := fakeincident.NewConnector(workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if err := registry.TrustWorkspace(workspaceID, "test-v1"); err != nil {
		t.Fatal(err)
	}
	registry.RequireTrustPolicy(true)
	srv.connectorRegistry = registry
	srv.connectorExecutor = &connectorruntime.Executor{Registry: registry}
	handler := withRequestMiddleware(srv.securityMiddleware(srv.routes()), 2<<20)
	capability, ok := registry.LookupIdentity(workspaceID, fakeincident.ConnectorName, fakeincident.ConnectorVersion, "incident.remediate", "1")
	if !ok {
		t.Fatal("incident remediation capability was not registered")
	}
	definition := capability.Definition()
	request := contracts.OperationRequest{WorkspaceID: workspaceID, IdempotencyKey: "generic-effect-key", Capability: definition.Ref, Target: contracts.ResourceRef{WorkspaceID: workspaceID, System: contracts.SystemRef{WorkspaceID: workspaceID, Type: "incident", ID: "monitor", Version: "1"}, Kind: fakeincident.ResourceKind, ID: "incident-1", Version: "1"}, InputType: fakeincident.InputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: contracts.HashStrings("incident-input"), Profile: definition.Profile}
	body, err := json.Marshal(operationCreateRequest{Request: request, Idempotency: request.IdempotencyKey})
	if err != nil {
		t.Fatal(err)
	}
	createdResponse := performOperationRequest(handler, token, workspaceID, http.MethodPost, "/v1/operations", body, nil)
	if createdResponse.Code != http.StatusOK {
		t.Fatalf("create status=%d body=%s", createdResponse.Code, createdResponse.Body.String())
	}
	operationID := stringValue(t, objectValue(t, decodeOperationJSON(t, createdResponse), "operation"), "id")
	executeResponse := performOperationRequest(handler, token, workspaceID, http.MethodPost, "/v1/operations/"+operationID+"/execute?workspace_id="+workspaceID, []byte(`{}`), nil)
	if executeResponse.Code != http.StatusConflict {
		t.Fatalf("effectful execute status=%d body=%s", executeResponse.Code, executeResponse.Body.String())
	}
}

func TestGenericOperationHTTPReservesAndReconcilesExternalEffect(t *testing.T) {
	srv, pool, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionOperationRead, contracts.PermissionOperationCreate, contracts.PermissionOperationExecute})
	srv.admission = store.NewAdmissionStore(pool, srv.events)
	registry := connectorruntime.NewRegistry()
	adapter, err := fakeincident.NewConnector(workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if err := registry.TrustWorkspace(workspaceID, "test-v1"); err != nil {
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
		t.Fatal("incident remediation capability was not registered")
	}
	definition := capability.Definition()
	request := contracts.OperationRequest{WorkspaceID: workspaceID, Capability: definition.Ref, Target: contracts.ResourceRef{WorkspaceID: workspaceID, System: contracts.SystemRef{WorkspaceID: workspaceID, Type: "incident", ID: "monitor", Version: "1"}, Kind: fakeincident.ResourceKind, ID: "incident-1", Version: "1"}, InputType: fakeincident.InputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: contracts.HashStrings("incident-input"), Profile: definition.Profile}
	request.ID = "operation-effect-http"
	request.RequestID = "request-effect-http"
	request.IdempotencyKey = "operation-effect-http-create"
	request.Actor = principal.Actor()
	hash := strings.Repeat("b", 64)
	plan := contracts.OperationPlan{ID: "plan-effect-http", WorkspaceID: workspaceID, Actor: principal.Actor(), Steps: []contracts.OperationStep{{
		ID: "step-effect", Ordinal: 0, Kind: "dispatch", Capability: request.Capability, Target: request.Target,
		Effect: definition.Effect, Profile: definition.Profile, InputHash: hash,
	}}}
	created, err := srv.operations.Create(context.Background(), store.OperationCreateInput{Request: request, Plan: &plan})
	if err != nil {
		t.Fatal(err)
	}
	policy := contracts.AdmissionPolicy{WorkspaceID: workspaceID, PolicyID: "http-effect-policy", Version: "1", AllowedConnectors: []contracts.ConnectorRef{definition.Ref.Connector}, AllowedResourceKinds: []string{fakeincident.ResourceKind}, AllowedActorIDs: []string{principal.ID}, MaxCostMicros: 100, MaxOperationsPerWindow: 10, MaxCostPerWindowMicros: 1000}
	if err := policy.Normalize(); err != nil {
		t.Fatal(err)
	}
	durableAdmission, err := srv.admission.Admit(context.Background(), contracts.AdmissionInput{WorkspaceID: workspaceID, OperationID: created.Operation.ID, OperationHash: created.Operation.OperationHash, RequestID: request.RequestID, IdempotencyKey: "operation-effect-http-admission", Actor: principal.Actor(), Capability: definition, Target: request.Target, Policy: policy, ConnectorAvailable: true, ResourceAllowed: true, EvidenceSatisfied: true, TaskFenceValid: true, RequestedCostMicros: 10})
	if err != nil {
		t.Fatalf("durable effect admission: %v", err)
	}
	if durableAdmission.Approval != nil {
		if _, _, err := srv.admission.DecideApproval(context.Background(), contracts.OperationApprovalDecision{WorkspaceID: workspaceID, ApprovalID: durableAdmission.Approval.ID, RequestID: "approver-request", IdempotencyKey: "approver-decision", Decision: contracts.ApprovalRequestApproved, Actor: contracts.ActorRef{ID: "approver", Kind: "human", WorkspaceID: workspaceID}}); err != nil {
			t.Fatalf("approve durable effect admission: %v", err)
		}
	}
	lease, err := srv.operations.AcquireLease(context.Background(), workspaceID, created.Operation.ID, principal.ID, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, inserted, err := srv.operations.ReserveAttempt(context.Background(), store.OperationAttemptInput{WorkspaceID: workspaceID, OperationID: created.Operation.ID, StepID: "step-effect", Attempt: 1, AttemptID: "attempt-effect-http", OwnerID: principal.ID, Fence: lease.Lease.Fence, RequestHash: hash, IdempotencyKey: "attempt-effect-http"}); err != nil || !inserted {
		t.Fatalf("reserve attempt inserted=%v err=%v", inserted, err)
	}
	handler := withRequestMiddleware(srv.securityMiddleware(srv.routes()), 2<<20)
	fenceHeaders := map[string]string{"X-Operation-Fence": strconv.FormatUint(lease.Lease.Fence, 10)}
	effectBody := []byte(`{"step_id":"step-effect","attempt_id":"attempt-effect-http","request_hash":"` + hash + `","effect":{"workspace_id":"` + workspaceID + `","boundary":"fakeincident.remediation","class":"approval_required_write","delivery_guarantee":"at_least_once","idempotency_key":"operation-effect-http-create","provider_idempotency_supported":true,"verification_required":true,"verification_status":"pending","compensation_status":"unavailable"}}`)
	reservePath := "/v1/operations/" + created.Operation.ID + "/effects/reserve?workspace_id=" + workspaceID
	reserved := performOperationRequest(handler, token, workspaceID, http.MethodPost, reservePath, effectBody, fenceHeaders)
	if reserved.Code != http.StatusOK {
		t.Fatalf("reserve status=%d body=%s", reserved.Code, reserved.Body.String())
	}
	reservedJSON := decodeOperationJSON(t, reserved)
	effect := objectValue(t, reservedJSON, "effect")
	effectID := stringValue(t, effect, "effect_id")
	if effectID == "" || stringValue(t, effect, "request_hash") != hash {
		t.Fatalf("reserved effect=%s", responseJSON(reservedJSON))
	}
	getPath := "/v1/operations/" + created.Operation.ID + "/effects/" + effectID + "?workspace_id=" + workspaceID
	got := performOperationRequest(handler, token, workspaceID, http.MethodGet, getPath, nil, nil)
	if got.Code != http.StatusOK || stringValue(t, objectValue(t, decodeOperationJSON(t, got), "effect"), "state") != contracts.ExternalEffectReserved {
		t.Fatalf("get reserved status=%d body=%s", got.Code, got.Body.String())
	}
	statePath := "/v1/operations/" + created.Operation.ID + "/effects/" + effectID + "/state?workspace_id=" + workspaceID
	stateBody := []byte(`{"idempotency_key":"effect-dispatch-intent-http","state":"dispatching"}`)
	dispatching := performOperationRequest(handler, token, workspaceID, http.MethodPost, statePath, stateBody, fenceHeaders)
	if dispatching.Code != http.StatusOK {
		t.Fatalf("dispatch intent status=%d body=%s", dispatching.Code, dispatching.Body.String())
	}
	stateBody = []byte(`{"idempotency_key":"effect-dispatch-http","state":"dispatched","provider_request_id":"provider-request-http"}`)
	dispatched := performOperationRequest(handler, token, workspaceID, http.MethodPost, statePath, stateBody, fenceHeaders)
	if dispatched.Code != http.StatusOK || stringValue(t, objectValue(t, decodeOperationJSON(t, dispatched), "effect"), "state") != contracts.ExternalEffectDispatched {
		t.Fatalf("dispatch status=%d body=%s", dispatched.Code, dispatched.Body.String())
	}
	recoveryList := performOperationRequest(handler, token, workspaceID, http.MethodGet, "/v1/operation-effects/recovery?workspace_id="+workspaceID+"&limit=8", nil, nil)
	if recoveryList.Code != http.StatusOK || len(objectSliceValue(t, decodeOperationJSON(t, recoveryList), "effects")) != 1 {
		t.Fatalf("recovery list status=%d body=%s", recoveryList.Code, recoveryList.Body.String())
	}
	effectLeasePath := "/v1/operations/" + created.Operation.ID + "/effects/" + effectID + "/lease?workspace_id=" + workspaceID
	recoveryLease := performOperationRequest(handler, token, workspaceID, http.MethodPost, effectLeasePath, []byte(`{"ttl_ms":30000}`), nil)
	if recoveryLease.Code != http.StatusOK {
		t.Fatalf("recovery lease status=%d body=%s", recoveryLease.Code, recoveryLease.Body.String())
	}
	recoveryLeaseJSON := decodeOperationJSON(t, recoveryLease)
	effectFence := stringValue(t, objectValue(t, recoveryLeaseJSON, "lease"), "fence")
	if effectFence == "" || effectFence == "0" {
		t.Fatalf("invalid effect fence=%s", effectFence)
	}
	effectAckPath := "/v1/operations/" + created.Operation.ID + "/effects/" + effectID + "/state?workspace_id=" + workspaceID
	effectAck := performOperationRequest(handler, token, workspaceID, http.MethodPost, effectAckPath, []byte(`{"idempotency_key":"effect-ack-recovery-http","state":"acknowledged","response_hash":"`+hash+`"}`), map[string]string{"X-Effect-Fence": effectFence})
	if effectAck.Code != http.StatusOK || stringValue(t, objectValue(t, decodeOperationJSON(t, effectAck), "effect"), "state") != contracts.ExternalEffectAcknowledged {
		t.Fatalf("effect recovery ack status=%d body=%s", effectAck.Code, effectAck.Body.String())
	}
	effectReleasePath := "/v1/operations/" + created.Operation.ID + "/effects/" + effectID + "/release?workspace_id=" + workspaceID
	effectRelease := performOperationRequest(handler, token, workspaceID, http.MethodPost, effectReleasePath, nil, map[string]string{"X-Effect-Fence": effectFence})
	if effectRelease.Code != http.StatusOK {
		t.Fatalf("effect release status=%d body=%s", effectRelease.Code, effectRelease.Body.String())
	}
	duplicate := performOperationRequest(handler, token, workspaceID, http.MethodPost, statePath, stateBody, fenceHeaders)
	if duplicate.Code != http.StatusOK || !boolValue(t, decodeOperationJSON(t, duplicate), "duplicate") {
		t.Fatalf("duplicate dispatch status=%d body=%s", duplicate.Code, duplicate.Body.String())
	}
	conflict := performOperationRequest(handler, token, workspaceID, http.MethodPost, statePath, []byte(`{"idempotency_key":"effect-dispatch-http","state":"acknowledged"}`), fenceHeaders)
	if conflict.Code != http.StatusConflict {
		t.Fatalf("idempotency conflict status=%d body=%s", conflict.Code, conflict.Body.String())
	}
	foreign := performOperationRequest(handler, token, "foreign-workspace", http.MethodGet, getPath+"&workspace_id=foreign-workspace", nil, nil)
	if foreign.Code != http.StatusForbidden {
		t.Fatalf("cross-workspace effect status=%d body=%s", foreign.Code, foreign.Body.String())
	}

	if err := srv.operations.ReleaseLease(context.Background(), store.OperationLease{WorkspaceID: workspaceID, OperationID: created.Operation.ID, OwnerID: principal.ID, Fence: lease.Lease.Fence}); err != nil {
		t.Fatal(err)
	}
	if _, err := srv.operations.AcquireLease(context.Background(), workspaceID, created.Operation.ID, "takeover-effect-http", time.Minute); err != nil {
		t.Fatal(err)
	}
	stale := performOperationRequest(handler, token, workspaceID, http.MethodPost, statePath, []byte(`{"idempotency_key":"effect-ack-stale","state":"acknowledged"}`), fenceHeaders)
	if stale.Code != http.StatusConflict {
		t.Fatalf("stale reconciliation status=%d body=%s", stale.Code, stale.Body.String())
	}
}

func genericOperationRequest(workspaceID string) contracts.OperationRequest {
	hash := strings.Repeat("a", 64)
	return contracts.OperationRequest{
		ID:             "operation-http-1",
		RequestID:      "request-http-1",
		IdempotencyKey: "operation-create-http-1",
		WorkspaceID:    workspaceID,
		Capability: contracts.CapabilityRef{
			WorkspaceID: workspaceID,
			Connector:   contracts.ConnectorRef{WorkspaceID: workspaceID, Name: "test-connector", Version: "v1"},
			Name:        "read-resource", Version: "v1", DefinitionHash: hash,
		},
		Target: contracts.ResourceRef{
			WorkspaceID: workspaceID,
			System:      contracts.SystemRef{WorkspaceID: workspaceID, Type: "test-system", ID: "system-1", Version: "v1"},
			Kind:        "record", ID: "record-1", Version: "v1", ContentHash: hash,
		},
		InputType: "test.input", InputSchemaVersion: 1, InputSchemaHash: hash, InputHash: hash,
		Profile:  contracts.DefaultExecutionProfile(),
		Metadata: map[string]string{"test_case": "http"},
	}
}

func performOperationRequest(handler http.Handler, token, workspaceID, method, path string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(method, path, bytes.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Workspace-ID", workspaceID)
	request.Header.Set("X-Request-ID", "operation-http-"+sha256Short(method+path+string(body)))
	for name, value := range headers {
		request.Header.Set(name, value)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func decodeOperationJSON(t *testing.T, response *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var value map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &value); err != nil {
		t.Fatalf("decode response: %v body=%s", err, response.Body.String())
	}
	return value
}

func objectValue(t *testing.T, value map[string]any, key string) map[string]any {
	t.Helper()
	object, ok := value[key].(map[string]any)
	if !ok {
		t.Fatalf("response key %q is not an object: %s", key, responseJSON(value))
	}
	return object
}

func objectSliceValue(t *testing.T, value map[string]any, key string) []any {
	t.Helper()
	items, ok := value[key].([]any)
	if !ok {
		t.Fatalf("%s is not an array in %s", key, responseJSON(value))
	}
	return items
}

func stringValue(t *testing.T, value map[string]any, key string) string {
	t.Helper()
	if text, ok := value[key].(string); ok {
		return text
	}
	if number, ok := value[key].(float64); ok {
		return formatFloat(number)
	}
	return ""
}

func boolValue(t *testing.T, value map[string]any, key string) bool {
	t.Helper()
	result, _ := value[key].(bool)
	return result
}

func responseJSON(value any) string {
	raw, _ := json.Marshal(value)
	return string(raw)
}

func sha256Short(value string) string {
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])[:16]
}

func formatFloat(value float64) string {
	return strconv.FormatInt(int64(value), 10)
}
