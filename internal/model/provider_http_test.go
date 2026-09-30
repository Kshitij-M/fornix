package model

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/credentials"
	"github.com/omaveda/fornix/internal/testutil"
)

func TestOpenAIRequestConstructionPreservesIdempotencyKeyAcrossAttemptBuilds(t *testing.T) {
	request := modelTestRequest("openai")
	provider := &OpenAIProvider{endpoint: contracts.ModelEndpoint{BaseURL: "https://api.example.invalid/v1"}}
	for attempt := 1; attempt <= 2; attempt++ {
		httpRequest, err := provider.newRequest(context.Background(), []byte(`{"model":"gpt-test"}`), "test-key", false, request.IdempotencyKey)
		if err != nil {
			t.Fatalf("construct provider attempt %d: %v", attempt, err)
		}
		if got := httpRequest.Header.Get("Idempotency-Key"); got != request.IdempotencyKey {
			t.Fatalf("attempt %d provider idempotency key = %q", attempt, got)
		}
	}
}

func TestOpenAICompatibleProviderSerializesRequestAndReconcilesCost(t *testing.T) {
	testutil.RequireLocalHTTP(t)
	const secret = "sk-test-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/chat/completions" {
			t.Fatalf("request path = %s", r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer "+secret {
			t.Fatalf("authorization header was not sent as expected")
		}
		if r.Header.Get("Idempotency-Key") != "model-idempotency-1" {
			t.Fatalf("idempotency key = %q", r.Header.Get("Idempotency-Key"))
		}
		var request struct {
			Model    string `json:"model"`
			Messages []struct {
				Role    string `json:"role"`
				Content string `json:"content"`
			} `json:"messages"`
			Stream    bool `json:"stream"`
			MaxTokens int  `json:"max_tokens"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request.Model != "gpt-test" || len(request.Messages) != 1 || request.Messages[0].Content != "return a stable answer" || request.Stream || request.MaxTokens != contracts.DefaultModelOutputTokens {
			t.Fatalf("serialized request = %+v", request)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"id":"chatcmpl-test","choices":[{"message":{"role":"assistant","content":"hello"},"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":5,"total_tokens":15}}`))
	}))
	defer server.Close()

	provider, err := NewOpenAIProvider(OpenAIConfig{
		Endpoint: contracts.ModelEndpoint{Provider: "openai", BaseURL: server.URL + "/v1", DefaultModel: "gpt-test", AllowPrivate: true, InputCostPer1KUSD: 0.01, OutputCostPer1KUSD: 0.02},
		APIKey:   secret, RequireAPIKey: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	request := modelTestRequest("openai")
	request.Provider.Model = "gpt-test"
	response, err := provider.Complete(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if response.Content != "hello" || response.ProviderRequestID != "chatcmpl-test" || response.Usage.TotalTokens != 15 {
		t.Fatalf("response = %+v", response)
	}
	if response.Cost.TotalCostUSD != 0.0002 {
		t.Fatalf("cost = %v, want 0.0002", response.Cost.TotalCostUSD)
	}
	if strings.Contains(string(response.WirePayload), secret) {
		t.Fatal("provider credential appeared in response evidence")
	}
}

func TestOpenAIProviderProjectsOnlyProviderStandardToolFields(t *testing.T) {
	provider := &OpenAIProvider{endpoint: contracts.ModelEndpoint{DefaultModel: "gpt-test"}}
	request := modelTestRequest("openai")
	request.Tools = []contracts.ModelToolDefinition{{
		Name: "fornix.inspect", Description: "Inspect a bounded object.",
		Parameters:     json.RawMessage(`{"type":"object","properties":{"argv":{"type":"array"}},"required":["argv"]}`),
		DefinitionHash: strings.Repeat("a", 64),
	}}
	request.Messages = []contracts.ModelMessage{{Role: "assistant", ToolCalls: []contracts.ModelToolCall{{ID: "call-1", ToolID: "fornix.inspect", Arguments: json.RawMessage(`{"argv":["safe"]}`)}}}}
	providerNames, internalNames, err := openAIToolNameMappings(request.Tools)
	if err != nil {
		t.Fatal(err)
	}
	built, _, err := provider.buildRequest(request, false)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(built)
	if err != nil {
		t.Fatal(err)
	}
	var wire map[string]any
	if err := json.Unmarshal(encoded, &wire); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "definition_hash") || strings.Contains(string(encoded), "/private/") {
		t.Fatalf("internal authority metadata leaked onto provider wire: %s", encoded)
	}
	tools, ok := wire["tools"].([]any)
	if !ok || len(tools) != 1 {
		t.Fatalf("provider tool projection = %#v", wire["tools"])
	}
	toolWire := tools[0].(map[string]any)
	function := toolWire["function"].(map[string]any)
	if function["name"] != providerNames["fornix.inspect"] || !validOpenAIToolName(function["name"].(string)) || function["description"] != "Inspect a bounded object." {
		t.Fatalf("provider-standard tool metadata changed: %#v", function)
	}
	if _, ok := function["parameters"].(map[string]any); !ok {
		t.Fatalf("canonical parameters were omitted from provider request: %#v", function)
	}
	messages := wire["messages"].([]any)
	assistantCall := messages[0].(map[string]any)["tool_calls"].([]any)[0].(map[string]any)["function"].(map[string]any)
	if assistantCall["name"] != providerNames["fornix.inspect"] {
		t.Fatalf("historical tool call did not use the provider alias: %#v", assistantCall)
	}
	calls := []contracts.ModelToolCall{{ID: "call-1", ToolID: providerNames["fornix.inspect"], Arguments: json.RawMessage(`{"argv":["safe"]}`)}}
	if err := restoreOpenAIToolNames(calls, internalNames); err != nil {
		t.Fatal(err)
	}
	if calls[0].ToolID != "fornix.inspect" {
		t.Fatalf("provider response alias did not map back to Fornix ID: %+v", calls[0])
	}
	unknown := []contracts.ModelToolCall{{ID: "call-unknown", ToolID: "unregistered", Arguments: json.RawMessage(`{}`)}}
	if err := restoreOpenAIToolNames(unknown, internalNames); err == nil {
		t.Fatal("provider response outside the registered tool catalog was accepted")
	}
}

func TestOpenAIToolNameMappingRejectsCaseFoldAmbiguity(t *testing.T) {
	_, _, err := openAIToolNameMappings([]contracts.ModelToolDefinition{{Name: "Inspect"}, {Name: "inspect"}})
	if err == nil {
		t.Fatal("ambiguous case-insensitive function names were accepted")
	}
}

func TestOpenAIToolNameMappingIsDeterministicAndCollisionChecked(t *testing.T) {
	definitions := []contracts.ModelToolDefinition{
		{Name: "fornix.repository.read"},
		{Name: "simple_tool"},
	}
	toProvider, fromProvider, err := openAIToolNameMappings(definitions)
	if err != nil {
		t.Fatal(err)
	}
	if !validOpenAIToolName(toProvider["fornix.repository.read"]) || toProvider["simple_tool"] != "simple_tool" || fromProvider[toProvider["fornix.repository.read"]] != "fornix.repository.read" {
		t.Fatalf("provider name mapping is not safe/reversible: to=%v from=%v", toProvider, fromProvider)
	}
	second, _, err := openAIToolNameMappings(definitions)
	if err != nil || second["fornix.repository.read"] != toProvider["fornix.repository.read"] {
		t.Fatalf("provider name mapping changed across runs: first=%v second=%v err=%v", toProvider, second, err)
	}
}

func TestParseOpenAIToolCallsRejectsDuplicateIDs(t *testing.T) {
	_, err := parseOpenAIToolCalls([]openAIToolCall{
		{ID: "call-duplicate", Type: "function", Function: openAIFunctionCall{Name: "tool.inspect", Arguments: `{"argv":["first"]}`}},
		{ID: " call-duplicate ", Type: "function", Function: openAIFunctionCall{Name: "tool.inspect", Arguments: `{"argv":["second"]}`}},
	})
	if err == nil {
		t.Fatal("duplicate normalized provider tool-call IDs were accepted")
	}
	if strings.Contains(err.Error(), "first") || strings.Contains(err.Error(), "second") {
		t.Fatalf("duplicate-ID rejection leaked provider arguments: %v", err)
	}
	unique, err := parseOpenAIToolCalls([]openAIToolCall{
		{ID: "call-one", Type: "function", Function: openAIFunctionCall{Name: "tool.inspect", Arguments: `{"argv":["one"]}`}},
		{ID: "call-two", Type: "function", Function: openAIFunctionCall{Name: "tool.inspect", Arguments: `{"argv":["two"]}`}},
	})
	if err != nil || len(unique) != 2 || unique[0].ID != "call-one" || unique[1].ID != "call-two" {
		t.Fatalf("unique tool-call IDs were rejected or changed: calls=%+v err=%v", unique, err)
	}
}

func TestOpenAICompatibleProviderStreamIsProviderNeutral(t *testing.T) {
	testutil.RequireLocalHTTP(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		writer := bufio.NewWriter(w)
		_, _ = writer.WriteString("data: {\"id\":\"stream-1\",\"choices\":[{\"delta\":{\"content\":\"hello\"}}]}\n\n")
		_, _ = writer.WriteString("data: [DONE]\n\n")
		_ = writer.Flush()
	}))
	defer server.Close()
	provider, err := NewOpenAIProvider(OpenAIConfig{Endpoint: contracts.ModelEndpoint{Provider: "openai", BaseURL: server.URL + "/v1", DefaultModel: "gpt-test", AllowPrivate: true}})
	if err != nil {
		t.Fatal(err)
	}
	request := modelTestRequest("openai")
	var deltas []string
	response, err := provider.Stream(context.Background(), request, func(event contracts.ModelStreamEvent) {
		if event.Type == contracts.ModelStreamTextDelta {
			deltas = append(deltas, event.Delta)
		}
	})
	if err != nil || response.Content != "hello" || strings.Join(deltas, "") != "hello" {
		t.Fatalf("stream response=%+v deltas=%v err=%v", response, deltas, err)
	}
}

func TestOpenAICredentialIsRedactedFromFailureAndEvidence(t *testing.T) {
	testutil.RequireLocalHTTP(t)
	const secret = "sk-test-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"error":{"message":"api_key=sk-test-secret"}}`))
	}))
	defer server.Close()
	provider, err := NewOpenAIProvider(OpenAIConfig{
		Endpoint: contracts.ModelEndpoint{Provider: "openai", BaseURL: server.URL + "/v1", DefaultModel: "gpt-test", AllowPrivate: true},
		APIKey:   secret, RequireAPIKey: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Complete(context.Background(), modelTestRequest("openai"))
	if err == nil {
		t.Fatal("unauthorized request unexpectedly succeeded")
	}
	var failureErr *FailureError
	if !errors.As(err, &failureErr) || failureErr.Failure.Code != contracts.ModelFailureAuthentication {
		t.Fatalf("failure = %v", err)
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(string(failureErr.Evidence), secret) {
		t.Fatalf("credential leaked through failure: %v evidence=%s", err, failureErr.Evidence)
	}
}

func TestOpenAIProviderUsesScopedCredentialLeaseAndReleasesIt(t *testing.T) {
	testutil.RequireLocalHTTP(t)
	const secret = "sk-lease-secret"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+secret {
			t.Fatalf("lease credential was not used")
		}
		_, _ = w.Write([]byte(`{"id":"lease-call","choices":[{"message":{"content":"leased"},"finish_reason":"stop"}]}`))
	}))
	defer server.Close()
	ref, err := credentials.ParseRef("provider/openai")
	if err != nil {
		t.Fatal(err)
	}
	secretValue, err := credentials.NewSecret([]byte(secret))
	if err != nil {
		t.Fatal(err)
	}
	released := false
	provider, err := NewOpenAIProvider(OpenAIConfig{
		Endpoint:      contracts.ModelEndpoint{Provider: "openai", BaseURL: server.URL + "/v1", DefaultModel: "gpt-test", AllowPrivate: true, CredentialRef: "provider/openai"},
		RequireAPIKey: true,
		CredentialLease: credentials.LeaseResolverFunc{
			AcquireFunc: func(_ context.Context, workspace, reference, purpose string, _ time.Duration) (credentials.Lease, error) {
				return credentials.Lease{Reference: ref, WorkspaceID: workspace, LeaseID: "lease-1", Purpose: purpose, Fence: 1, RevocationEpoch: 1, ExpiresAt: time.Now().UTC().Add(time.Minute), Secret: secretValue}, nil
			},
			ReleaseFunc: func(_ context.Context, lease credentials.Lease) error {
				released = lease.LeaseID == "lease-1"
				return nil
			},
			ValidateLeaseFunc: func(context.Context, credentials.Lease) error { return nil },
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := provider.Complete(context.Background(), modelTestRequest("openai")); err != nil {
		t.Fatal(err)
	}
	if !released {
		t.Fatal("credential lease was not released")
	}
}

func TestOpenAIRevalidatesCredentialLeaseImmediatelyBeforeEgress(t *testing.T) {
	var validationCalls atomic.Int32
	ref, _ := credentials.ParseRef("provider/openai")
	secret, _ := credentials.NewSecret([]byte("lease-secret"))
	released := atomic.Bool{}
	resolver := credentials.LeaseResolverFunc{
		AcquireFunc: func(_ context.Context, workspace, _ string, purpose string, _ time.Duration) (credentials.Lease, error) {
			return credentials.Lease{Reference: ref, WorkspaceID: workspace, LeaseID: "lease-revoked-before-send", Purpose: purpose, Fence: 4, RevocationEpoch: 2, ExpiresAt: time.Now().UTC().Add(time.Minute), Secret: secret}, nil
		},
		ValidateLeaseFunc: func(context.Context, credentials.Lease) error {
			if validationCalls.Add(1) == 3 {
				return credentials.ErrLeaseRevoked
			}
			return nil
		},
		ReleaseFunc: func(context.Context, credentials.Lease) error { released.Store(true); return nil },
	}
	provider, err := NewOpenAIProvider(OpenAIConfig{
		Endpoint:        contracts.ModelEndpoint{Provider: "openai", BaseURL: "http://127.0.0.1:1/v1", DefaultModel: "gpt-test", AllowPrivate: true, CredentialRef: "provider/openai"},
		CredentialLease: resolver, RequireAPIKey: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = provider.Complete(context.Background(), modelTestRequest("openai"))
	var failure *FailureError
	if !errors.As(err, &failure) || failure.Failure.Code != contracts.ModelFailureAuthentication || validationCalls.Load() != 3 || !released.Load() {
		t.Fatalf("revoked credential did not fail before provider transport: validations=%d released=%v err=%v", validationCalls.Load(), released.Load(), err)
	}
}

func TestOllamaEmbeddingProviderPreservesExistingEmbeddingContract(t *testing.T) {
	testutil.RequireLocalHTTP(t)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/embeddings" {
			t.Fatalf("embedding path = %s", r.URL.Path)
		}
		var request struct {
			Model  string `json:"model"`
			Prompt string `json:"prompt"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Fatal(err)
		}
		if request.Model != "nomic-test" || request.Prompt != "embedding input" {
			t.Fatalf("embedding request = %+v", request)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"embedding":[0.1,0.2,0.3]}`))
	}))
	defer server.Close()
	provider, err := NewOllamaProvider(OllamaConfig{
		Endpoint:       contracts.ModelEndpoint{Provider: "ollama", BaseURL: server.URL, DefaultModel: "nomic-test", AllowPrivate: true},
		EmbeddingModel: "nomic-test", EmbeddingDim: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	embedding, err := provider.Embed(context.Background(), EmbeddingRequest{Text: "embedding input", Model: "nomic-test", MaxInputBytes: 2000})
	if err != nil || len(embedding) != 3 || embedding[1] != 0.2 {
		t.Fatalf("embedding=%v err=%v", embedding, err)
	}
}

func TestOllamaChatPreservesToolCatalogAndMultiTurnToolHistory(t *testing.T) {
	provider, err := NewOllamaProvider(OllamaConfig{Endpoint: contracts.ModelEndpoint{Provider: "ollama", BaseURL: "http://127.0.0.1:11434", DefaultModel: "qwen-test", AllowPrivate: true}})
	if err != nil {
		t.Fatal(err)
	}
	request := modelTestRequest("ollama")
	request.Provider.Model = "qwen-test"
	request.Tools = []contracts.ModelToolDefinition{{Name: "fornix.inspect", Description: "Inspect safely.", Parameters: json.RawMessage(`{"type":"object","properties":{"argv":{"type":"array"}},"required":["argv"]}`), DefinitionHash: strings.Repeat("a", 64)}}
	modelName, messages, tools, err := provider.chatRequest(request)
	if err != nil {
		t.Fatal(err)
	}
	if modelName != "qwen-test" || len(tools) != 1 || tools[0].Function.Name != "fornix.inspect" || tools[0].Function.Description != "Inspect safely." || len(messages) != 1 || messages[0].Role != "user" {
		t.Fatalf("Ollama catalog/request projection changed: model=%q messages=%+v tools=%+v", modelName, messages, tools)
	}
	encodedRequest, err := json.Marshal(ollamaChatRequest{Model: modelName, Messages: messages, Tools: tools})
	if err != nil || strings.Contains(string(encodedRequest), "definition_hash") {
		t.Fatalf("internal tool fingerprint leaked to Ollama: payload=%s err=%v", encodedRequest, err)
	}
	first, err := normalizeOllamaToolCalls(request.RequestID, []ollamaToolCall{{Type: "function", Function: ollamaToolCallFunction{Name: "fornix.inspect", Arguments: json.RawMessage(`{"argv":["safe"]}`)}}})
	if err != nil || len(first) != 1 || first[0].ToolID != "fornix.inspect" || first[0].ID == "" || string(first[0].Arguments) != `{"argv":["safe"]}` {
		t.Fatalf("normalized Ollama tool call = %+v err=%v", first, err)
	}
	replayed, err := normalizeOllamaToolCalls(request.RequestID, []ollamaToolCall{{Type: "function", Function: ollamaToolCallFunction{Name: "fornix.inspect", Arguments: json.RawMessage(`{"argv":["safe"]}`)}}})
	if err != nil || replayed[0].ID != first[0].ID {
		t.Fatalf("tool call identity is not deterministic: first=%+v replayed=%+v err=%v", first, replayed, err)
	}
	if _, err := normalizeOllamaToolCalls(request.RequestID, []ollamaToolCall{{Function: ollamaToolCallFunction{Name: "fornix.inspect", Arguments: json.RawMessage(`{invalid}`)}}}); err == nil {
		t.Fatal("malformed Ollama tool arguments were silently accepted")
	}
	followup := request
	followup.RequestID, followup.IdempotencyKey = "ollama-followup", "ollama-followup-idempotency"
	followup.Messages = []contracts.ModelMessage{
		{Role: "user", Content: "inspect safely"},
		{Role: "assistant", ToolCalls: first},
		{Role: "tool", Name: first[0].ToolID, ToolCallID: first[0].ID, Content: "inspected"},
	}
	_, history, followupTools, err := provider.chatRequest(followup)
	if err != nil {
		t.Fatal(err)
	}
	if len(followupTools) != 1 || len(history) != 3 || history[1].Role != "assistant" || len(history[1].ToolCalls) != 1 || history[1].ToolCalls[0].Function.Name != "fornix.inspect" || history[2].Role != "tool" || history[2].ToolName != "fornix.inspect" || history[2].Content != "inspected" {
		t.Fatalf("Ollama tool history was not preserved: messages=%+v tools=%+v", history, followupTools)
	}
	if _, err := provider.Stream(context.Background(), request, nil); err == nil {
		t.Fatal("streaming tool request was not rejected before provider egress")
	}
}

func TestProviderFailureClassificationAndRetryAfter(t *testing.T) {
	quota := classifyHTTPError("openai", nil, http.StatusTooManyRequests, []byte(`{"error":{"message":"quota exceeded"}}`))
	var quotaFailure *FailureError
	if !errors.As(quota, &quotaFailure) || quotaFailure.Failure.Code != contracts.ModelFailureQuota || quotaFailure.Failure.Retryable {
		t.Fatalf("quota classification = %v", quota)
	}
	rateLimit := withRetryAfter(classifyHTTPError("openai", nil, http.StatusTooManyRequests, []byte(`{"error":{"message":"busy"}}`)), "3")
	var rateFailure *FailureError
	if !errors.As(rateLimit, &rateFailure) || rateFailure.Failure.Code != contracts.ModelFailureRateLimit || !rateFailure.Failure.Retryable || rateFailure.Failure.RetryAfterMS != 3000 {
		t.Fatalf("rate-limit classification = %v", rateLimit)
	}
	timeout := classifyHTTPError("openai", context.DeadlineExceeded, 0, nil)
	var timeoutFailure *FailureError
	if !errors.As(timeout, &timeoutFailure) || timeoutFailure.Failure.Code != contracts.ModelFailureTimeout || !timeoutFailure.Failure.Retryable {
		t.Fatalf("timeout classification = %v", timeout)
	}
}
