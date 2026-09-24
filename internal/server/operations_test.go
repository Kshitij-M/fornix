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

	"github.com/omaveda/fornix/internal/contracts"
)

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
