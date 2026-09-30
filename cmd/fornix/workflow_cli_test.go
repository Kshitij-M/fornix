package main

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestWorkflowCLIUsesExplicitFenceAndWorkspaceHeaders(t *testing.T) {
	var seenPath, seenFence, seenWorkspace, seenIdempotency string
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		seenPath = r.URL.Path
		seenFence = r.Header.Get("X-Operation-Fence")
		seenWorkspace = r.Header.Get("X-Workspace-ID")
		seenIdempotency = r.Header.Get("Idempotency-Key")
		payload, _ := json.Marshal(map[string]any{"ok": true})
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(string(payload))), Request: r}, nil
	})}

	cli := &operatorCLI{baseURL: "http://operator.invalid", workspace: "workspace-a", client: client, suppressOutput: true}
	if err := cli.workflowCommand([]string{"advance", "--id", "run-1", "--fence", "7"}); err != nil {
		t.Fatal(err)
	}
	if seenPath != "/v1/workflows/run-1/advance" || seenFence != "7" || seenWorkspace != "workspace-a" {
		t.Fatalf("unexpected workflow request path=%q fence=%q workspace=%q", seenPath, seenFence, seenWorkspace)
	}
	if !strings.Contains(seenIdempotency, "workflow:advance:workspace-a:run-1") {
		t.Fatalf("missing deterministic idempotency key: %q", seenIdempotency)
	}
}

func TestWorkflowCLIVerifyRequiresExplicitProofInputs(t *testing.T) {
	cli := &operatorCLI{workspace: "workspace-a"}
	if err := cli.workflowCommand([]string{"verify", "--id", "run-1", "--fence", "7"}); err == nil {
		t.Fatal("expected verification without effect fence to fail")
	}
	if err := cli.workflowCommand([]string{"verify", "--id", "run-1", "--fence", "7", "--effect-fence", "9", "--step-id", "step-1"}); err == nil {
		t.Fatal("expected verification without versions to fail")
	}
}

func TestWorkflowCLIVerificationIdempotencyTracksProofVersions(t *testing.T) {
	var keys []string
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		keys = append(keys, r.Header.Get("Idempotency-Key"))
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
			Request:    r,
		}, nil
	})}
	cli := &operatorCLI{baseURL: "http://operator.invalid", workspace: "workspace-a", client: client}
	verify := func(effectVersion, linkVersion string) []string {
		return []string{
			"verify", "--id", "run-1", "--fence", "7", "--effect-fence", "9",
			"--step-id", "reply", "--expected-effect-version", effectVersion,
			"--expected-link-version", linkVersion,
		}
	}
	for _, command := range [][]string{verify("4", "6"), verify("4", "6"), verify("5", "7")} {
		if err := cli.workflowCommand(command); err != nil {
			t.Fatal(err)
		}
	}
	if len(keys) != 3 || keys[0] == "" || keys[0] != keys[1] || keys[1] == keys[2] {
		t.Fatalf("verification keys must replay for identical versions and change for new proof versions: %q", keys)
	}
	if !strings.HasPrefix(keys[0], "workflow:verify:") || len(keys[0]) > 100 {
		t.Fatalf("default verification idempotency key is not bounded and namespaced: %q", keys[0])
	}
}

func TestWorkflowCLIRequiresFenceForMutation(t *testing.T) {
	cli := &operatorCLI{workspace: "workspace-a"}
	if err := cli.workflowCommand([]string{"cancel", "--id", "run-1"}); err == nil {
		t.Fatal("expected cancellation without a fence to fail")
	}
}

func TestWorkflowCLICommandSequencePreservesWorkspaceFencesAndIdempotency(t *testing.T) {
	type capturedRequest struct {
		method, path, workspace, operationFence, taskFence, effectFence, idempotency, authorization string
		body                                                                                        map[string]any
	}
	var requests []capturedRequest
	client := &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		captured := capturedRequest{
			method: r.Method, path: r.URL.EscapedPath(), workspace: r.Header.Get("X-Workspace-ID"),
			operationFence: r.Header.Get("X-Operation-Fence"), taskFence: r.Header.Get("X-Task-Fence"),
			effectFence: r.Header.Get("X-Effect-Fence"), idempotency: r.Header.Get("Idempotency-Key"),
			authorization: r.Header.Get("Authorization"),
		}
		if r.Body != nil {
			payload, err := io.ReadAll(r.Body)
			if err != nil {
				return nil, err
			}
			if len(payload) > 0 {
				if err := json.Unmarshal(payload, &captured.body); err != nil {
					return nil, err
				}
			}
		}
		requests = append(requests, captured)
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
			Request:    r,
		}, nil
	})}
	cli := &operatorCLI{baseURL: "http://operator.invalid", workspace: "workspace-a", key: "test-only-key", client: client, suppressOutput: true}
	requestPath := filepath.Join(t.TempDir(), "workflow.json")
	if err := os.WriteFile(requestPath, []byte(`{"operation":{"id":"op-1"}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	commands := [][]string{
		{"create", "--request-file", requestPath, "--idempotency", "create-1"},
		{"lease", "--id", "run-1", "--ttl-ms", "45000"},
		{"advance", "--id", "run-1", "--fence", "7", "--task-fence", "11"},
		{"approve", "--id", "run-1", "--fence", "7", "--step-id", "approval"},
		{"verify", "--id", "run-1", "--fence", "7", "--effect-fence", "9", "--step-id", "reply", "--expected-effect-version", "4", "--expected-link-version", "6", "--task-fence", "11", "--idempotency", "verify-2"},
		{"replay", "--id", "run-1", "--from-version", "2", "--limit", "100"},
		{"receipt", "--id", "run-1", "--finalize", "true"},
	}
	for _, command := range commands {
		if err := cli.workflowCommand(command); err != nil {
			t.Fatalf("workflow %s failed: %v", command[0], err)
		}
	}
	if len(requests) != len(commands) {
		t.Fatalf("captured requests=%d, want %d", len(requests), len(commands))
	}
	for i, request := range requests {
		if request.workspace != "workspace-a" || request.authorization != "Bearer test-only-key" {
			t.Errorf("request %d lost workspace/auth scope: workspace=%q authorization=%q", i, request.workspace, request.authorization)
		}
	}
	wantPaths := []string{
		"/v1/workflows",
		"/v1/workflows/run-1/lease",
		"/v1/workflows/run-1/advance",
		"/v1/workflows/run-1/approve",
		"/v1/workflows/run-1/verify",
		"/v1/workflows/run-1/replay",
		"/v1/workflows/run-1/receipt",
	}
	for i, path := range wantPaths {
		if requests[i].path != path {
			t.Errorf("request %d path=%q, want %q", i, requests[i].path, path)
		}
	}
	if requests[0].method != http.MethodPost || requests[0].idempotency != "create-1" || requests[0].body["workspace_id"] != "workspace-a" {
		t.Errorf("create request did not bind workspace/idempotency: %+v", requests[0])
	}
	if requests[2].operationFence != "7" || requests[2].taskFence != "11" || requests[2].idempotency != "workflow:advance:workspace-a:run-1" {
		t.Errorf("advance request lost fences or deterministic idempotency: %+v", requests[2])
	}
	if requests[3].body["step_id"] != "approval" || requests[3].operationFence != "7" {
		t.Errorf("approval request lost step or fence: %+v", requests[3])
	}
	if requests[4].operationFence != "7" || requests[4].effectFence != "9" || requests[4].taskFence != "11" || requests[4].idempotency != "verify-2" {
		t.Errorf("verification request lost fences or idempotency: %+v", requests[4])
	}
	if requests[4].body["expected_effect_version"] != float64(4) || requests[4].body["expected_link_version"] != float64(6) {
		t.Errorf("verification request lost expected versions: %+v", requests[4].body)
	}
	if requests[5].method != http.MethodPost || requests[6].method != http.MethodPost || requests[6].idempotency != "workflow:receipt:workspace-a:run-1" {
		t.Errorf("replay/finalize receipt method or idempotency mismatch: replay=%+v receipt=%+v", requests[5], requests[6])
	}
}
