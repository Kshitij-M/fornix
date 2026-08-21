package model

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/omaveda/fornix/internal/contracts"
)

type OllamaConfig struct {
	Endpoint       contracts.ModelEndpoint
	EmbeddingModel string
	EmbeddingDim   int
	HTTPClient     *http.Client
	Timeout        time.Duration
}

type OllamaProvider struct {
	endpoint       contracts.ModelEndpoint
	embeddingModel string
	embeddingDim   int
	client         *http.Client
	timeout        time.Duration
}

func NewOllamaProvider(cfg OllamaConfig) (*OllamaProvider, error) {
	endpoint := cfg.Endpoint
	if endpoint.Provider == "" {
		endpoint.Provider = "ollama"
	}
	endpoint.Provider = strings.ToLower(strings.TrimSpace(endpoint.Provider))
	if endpoint.BaseURL == "" {
		endpoint.BaseURL = "http://127.0.0.1:11434"
	}
	if endpoint.DefaultModel == "" {
		endpoint.DefaultModel = "llama3.2"
	}
	if err := validateProviderURL(endpoint.BaseURL, true); err != nil {
		return nil, err
	}
	if cfg.EmbeddingModel == "" {
		cfg.EmbeddingModel = "nomic-embed-text"
	}
	if cfg.EmbeddingDim <= 0 {
		cfg.EmbeddingDim = 768
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = contracts.DefaultModelTimeout
	}
	if cfg.Timeout > contracts.MaxModelTimeout {
		return nil, fmt.Errorf("ollama timeout exceeds %s", contracts.MaxModelTimeout)
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: cfg.Timeout}
	}
	if client.Timeout == 0 {
		client.Timeout = cfg.Timeout
	}
	return &OllamaProvider{endpoint: endpoint, embeddingModel: cfg.EmbeddingModel, embeddingDim: cfg.EmbeddingDim, client: client, timeout: cfg.Timeout}, nil
}

func (p *OllamaProvider) Name() string                      { return "ollama" }
func (p *OllamaProvider) Aliases() []string                 { return []string{"ollama-local"} }
func (p *OllamaProvider) Endpoint() contracts.ModelEndpoint { return p.endpoint }

func (p *OllamaProvider) Embed(ctx context.Context, request EmbeddingRequest) ([]float32, error) {
	text := request.Text
	limit := request.MaxInputBytes
	if limit <= 0 || limit > 2000 {
		limit = 2000
	}
	text = truncateUTF8(text, limit)
	modelName := strings.TrimSpace(request.Model)
	if modelName == "" {
		modelName = p.embeddingModel
	}
	payload, err := json.Marshal(ollamaEmbeddingRequest{Model: modelName, Prompt: text})
	if err != nil {
		return nil, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "marshal embedding request failed", Provider: p.Name()}}
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(p.endpoint.BaseURL, "/")+"/api/embeddings", bytes.NewReader(payload))
	if err != nil {
		return nil, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "build embedding request failed", Provider: p.Name()}}
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	response, err := p.client.Do(httpRequest)
	if err != nil {
		return nil, classifyHTTPError(p.Name(), err, 0, nil)
	}
	defer response.Body.Close()
	wire, err := readBounded(response.Body, contracts.MaxModelEvidenceBytes)
	if err != nil {
		return nil, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureTransport, Message: "read embedding response failed", Provider: p.Name(), Retryable: true}, Evidence: []byte(err.Error())}
	}
	if response.StatusCode >= http.StatusBadRequest {
		return nil, classifyHTTPError(p.Name(), nil, response.StatusCode, wire)
	}
	var decoded ollamaEmbeddingResponse
	if err := json.Unmarshal(wire, &decoded); err != nil {
		return nil, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureProvider, Message: "decode embedding response failed", Provider: p.Name()}, Evidence: wire}
	}
	if len(decoded.Embedding) != p.embeddingDim {
		return nil, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureProvider, Message: fmt.Sprintf("ollama returned %d dimensions, expected %d", len(decoded.Embedding), p.embeddingDim), Provider: p.Name()}, Evidence: wire}
	}
	return decoded.Embedding, nil
}

func (p *OllamaProvider) Complete(ctx context.Context, request contracts.ModelRequest) (contracts.ModelResponse, error) {
	modelName, messages, err := p.chatRequest(request, false)
	if err != nil {
		return contracts.ModelResponse{}, err
	}
	payload, err := json.Marshal(ollamaChatRequest{Model: modelName, Messages: messages, Stream: false, Options: map[string]any{"num_predict": request.Budget.MaxOutputTokens}})
	if err != nil {
		return contracts.ModelResponse{}, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "marshal Ollama chat request failed", Provider: p.Name()}}
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(p.endpoint.BaseURL, "/")+"/api/chat", bytes.NewReader(payload))
	if err != nil {
		return contracts.ModelResponse{}, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "build Ollama chat request failed", Provider: p.Name()}}
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	if request.IdempotencyKey != "" {
		httpRequest.Header.Set("Idempotency-Key", request.IdempotencyKey)
	}
	response, err := p.client.Do(httpRequest)
	if err != nil {
		return contracts.ModelResponse{}, classifyHTTPError(p.Name(), err, 0, nil)
	}
	defer response.Body.Close()
	wire, err := readBounded(response.Body, contracts.MaxModelEvidenceBytes*4)
	if err != nil {
		return contracts.ModelResponse{}, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureTransport, Message: "read Ollama chat response failed", Provider: p.Name(), Retryable: true}, Evidence: []byte(err.Error())}
	}
	if response.StatusCode >= http.StatusBadRequest {
		return contracts.ModelResponse{}, classifyHTTPError(p.Name(), nil, response.StatusCode, wire)
	}
	var decoded ollamaChatResponse
	if err := json.Unmarshal(wire, &decoded); err != nil {
		return contracts.ModelResponse{}, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureProvider, Message: "decode Ollama chat response failed", Provider: p.Name()}, Evidence: wire}
	}
	usage := contracts.ModelUsage{InputTokens: decoded.PromptEvalCount, OutputTokens: decoded.EvalCount, Source: "provider"}
	return contracts.ModelResponse{Provider: contracts.ProviderRef{Provider: p.Name(), Model: modelName}, Content: decoded.Message.Content, FinishReason: "stop", Usage: usage, Cost: contracts.ModelCost{Currency: "USD", Source: "not_configured"}, WirePayload: wire}, nil
}

func (p *OllamaProvider) Stream(ctx context.Context, request contracts.ModelRequest, sink StreamSink) (contracts.ModelResponse, error) {
	modelName, messages, err := p.chatRequest(request, true)
	if err != nil {
		return contracts.ModelResponse{}, err
	}
	payload, err := json.Marshal(ollamaChatRequest{Model: modelName, Messages: messages, Stream: true, Options: map[string]any{"num_predict": request.Budget.MaxOutputTokens}})
	if err != nil {
		return contracts.ModelResponse{}, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "marshal Ollama stream request failed", Provider: p.Name()}}
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(p.endpoint.BaseURL, "/")+"/api/chat", bytes.NewReader(payload))
	if err != nil {
		return contracts.ModelResponse{}, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "build Ollama stream request failed", Provider: p.Name()}}
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	if request.IdempotencyKey != "" {
		httpRequest.Header.Set("Idempotency-Key", request.IdempotencyKey)
	}
	response, err := p.client.Do(httpRequest)
	if err != nil {
		return contracts.ModelResponse{}, classifyHTTPError(p.Name(), err, 0, nil)
	}
	defer response.Body.Close()
	if response.StatusCode >= http.StatusBadRequest {
		wire, readErr := readBounded(response.Body, contracts.MaxModelEvidenceBytes*4)
		if readErr != nil {
			return contracts.ModelResponse{}, classifyHTTPError(p.Name(), readErr, response.StatusCode, nil)
		}
		return contracts.ModelResponse{}, classifyHTTPError(p.Name(), nil, response.StatusCode, wire)
	}
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 4096), contracts.MaxModelEvidenceBytes)
	var content strings.Builder
	contentRunes := 0
	var usage contracts.ModelUsage
	var wire bytes.Buffer
	completed := false
	for scanner.Scan() {
		line := scanner.Bytes()
		if wire.Len()+len(line)+1 > contracts.MaxModelEvidenceBytes {
			return contracts.ModelResponse{}, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureBudget, Message: "provider stream evidence budget exceeded", Provider: p.Name(), ContentEmitted: content.Len() > 0}, Evidence: wire.Bytes()}
		}
		wire.Write(line)
		wire.WriteByte('\n')
		var chunk ollamaChatResponse
		if err := json.Unmarshal(line, &chunk); err != nil {
			return contracts.ModelResponse{}, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureProvider, Message: "decode Ollama stream event failed", Provider: p.Name(), ContentEmitted: content.Len() > 0}, Evidence: wire.Bytes()}
		}
		if chunk.Message.Content != "" {
			if budgetErr := appendBoundedStreamContent(&content, &contentRunes, chunk.Message.Content, request.Budget, p.Name(), wire.Bytes()); budgetErr != nil {
				return contracts.ModelResponse{}, budgetErr
			}
			if sink != nil {
				sink(contracts.ModelStreamEvent{Type: contracts.ModelStreamTextDelta, Delta: chunk.Message.Content})
			}
		}
		if chunk.PromptEvalCount > 0 || chunk.EvalCount > 0 {
			usage = contracts.ModelUsage{InputTokens: chunk.PromptEvalCount, OutputTokens: chunk.EvalCount, Source: "provider"}
		}
		if chunk.Done {
			completed = true
		}
	}
	if err := scanner.Err(); err != nil {
		return contracts.ModelResponse{}, classifyHTTPError(p.Name(), err, 0, wire.Bytes())
	}
	if !completed {
		return contracts.ModelResponse{}, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureTransport, Message: "Ollama stream ended before completion", Provider: p.Name(), Retryable: true, ContentEmitted: content.Len() > 0}, Evidence: wire.Bytes()}
	}
	return contracts.ModelResponse{Provider: contracts.ProviderRef{Provider: p.Name(), Model: modelName}, Content: content.String(), FinishReason: "stop", Usage: usage, Cost: contracts.ModelCost{Currency: "USD", Source: "not_configured"}, WirePayload: wire.Bytes()}, nil
}

func (p *OllamaProvider) chatRequest(request contracts.ModelRequest, _ bool) (string, []ollamaMessage, error) {
	modelName := strings.TrimSpace(request.Provider.Model)
	if modelName == "" {
		modelName = p.endpoint.DefaultModel
	}
	if modelName == "" {
		return "", nil, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "Ollama model is required", Provider: p.Name()}}
	}
	messages := make([]ollamaMessage, 0, len(request.Messages)+1)
	for _, message := range request.Messages {
		messages = append(messages, ollamaMessage{Role: message.Role, Content: message.Content})
	}
	if len(messages) == 0 {
		messages = append(messages, ollamaMessage{Role: "user", Content: request.Prompt})
	}
	return modelName, messages, nil
}

type ollamaEmbeddingRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
}

type ollamaEmbeddingResponse struct {
	Embedding []float32 `json:"embedding"`
}

type ollamaChatRequest struct {
	Model    string          `json:"model"`
	Messages []ollamaMessage `json:"messages"`
	Stream   bool            `json:"stream"`
	Options  map[string]any  `json:"options,omitempty"`
}

type ollamaMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type ollamaChatResponse struct {
	Message         ollamaMessage `json:"message"`
	Done            bool          `json:"done"`
	PromptEvalCount int           `json:"prompt_eval_count"`
	EvalCount       int           `json:"eval_count"`
}

func truncateUTF8(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	value = value[:maxBytes]
	for len(value) > 0 && !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}
