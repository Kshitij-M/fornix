package model

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestOpenAICompatibleProviderSerializesRequestAndReconcilesCost(t *testing.T) {
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

func TestOpenAICompatibleProviderStreamIsProviderNeutral(t *testing.T) {
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

func TestOllamaEmbeddingProviderPreservesExistingEmbeddingContract(t *testing.T) {
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
