package model

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/omaveda/fornix/internal/contracts"
)

// OllamaConfig configures the local Ollama-compatible endpoint and embedding
// capability. It contains endpoint metadata, not credentials.
type OllamaConfig struct {
	Endpoint       contracts.ModelEndpoint
	EmbeddingModel string
	EmbeddingDim   int
	HTTPClient     *http.Client
	Timeout        time.Duration
}

// OllamaProvider adapts Ollama chat and embedding endpoints to the provider
// contract while retaining bounded response evidence.
type OllamaProvider struct {
	endpoint       contracts.ModelEndpoint
	embeddingModel string
	embeddingDim   int
	client         *http.Client
	timeout        time.Duration
	boundary       contracts.ExternalBoundaryAuthority
}

// NewOllamaProvider validates and constructs an Ollama provider. Network use
// happens only when Complete, Stream, or Embed is called.
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
	client, err := providerHTTPClient(endpoint, cfg.Timeout, cfg.HTTPClient, contracts.MaxModelEvidenceBytes*4, contracts.MaxModelEvidenceBytes*4)
	if err != nil {
		return nil, err
	}
	boundary, err := providerBoundaryAuthority(endpoint, cfg.Timeout, contracts.MaxModelEvidenceBytes*4, contracts.MaxModelEvidenceBytes*4)
	if err != nil {
		return nil, err
	}
	return &OllamaProvider{endpoint: endpoint, embeddingModel: cfg.EmbeddingModel, embeddingDim: cfg.EmbeddingDim, client: client, timeout: cfg.Timeout, boundary: boundary}, nil
}

func (p *OllamaProvider) Name() string                                           { return "ollama" }
func (p *OllamaProvider) Aliases() []string                                      { return []string{"ollama-local"} }
func (p *OllamaProvider) Endpoint() contracts.ModelEndpoint                      { return p.endpoint }
func (p *OllamaProvider) BoundaryAuthority() contracts.ExternalBoundaryAuthority { return p.boundary }

// Embed requests a bounded embedding from Ollama.
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

// EmbedScoped adapts the legacy Ollama embedding wire call to Fornix's typed
// provider capability. Raw text stays inside the immediate provider call and
// is never part of the durable request contract.
func (p *OllamaProvider) EmbedScoped(ctx context.Context, request contracts.EmbeddingRequest) (contracts.EmbeddingResponse, error) {
	vector, err := p.Embed(ctx, EmbeddingRequest{Model: request.Model, Text: request.Text, MaxInputBytes: request.Budget.MaxInputBytes, Timeout: time.Duration(request.Budget.TimeoutMS) * time.Millisecond})
	if err != nil {
		return contracts.EmbeddingResponse{}, err
	}
	hash, err := contracts.EmbeddingVectorHash(vector)
	if err != nil {
		return contracts.EmbeddingResponse{}, err
	}
	return contracts.EmbeddingResponse{RequestID: request.RequestID, Provider: request.Provider, SourceHash: request.SourceHash, Vector: vector, VectorHash: hash, Dimension: len(vector), Usage: contracts.EmbeddingUsage{InputBytes: int64(len([]byte(request.Text))), Dimension: len(vector), Source: "measured", Measured: true}}, nil
}

// Complete executes one bounded non-streaming Ollama chat request.
func (p *OllamaProvider) Complete(ctx context.Context, request contracts.ModelRequest) (contracts.ModelResponse, error) {
	modelName, messages, tools, err := p.chatRequest(request)
	if err != nil {
		return contracts.ModelResponse{}, err
	}
	payload, err := json.Marshal(ollamaChatRequest{Model: modelName, Messages: messages, Tools: tools, Stream: false, Options: map[string]any{"num_predict": request.Budget.MaxOutputTokens}})
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
	toolCalls, err := normalizeOllamaToolCalls(request.RequestID, decoded.Message.ToolCalls)
	if err != nil {
		return contracts.ModelResponse{}, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureProvider, Message: "Ollama returned an invalid tool call", Provider: p.Name()}, Evidence: wire}
	}
	usage := contracts.ModelUsage{InputTokens: decoded.PromptEvalCount, OutputTokens: decoded.EvalCount, Source: "provider"}
	finishReason := "stop"
	if len(toolCalls) > 0 {
		finishReason = "tool_calls"
	}
	return contracts.ModelResponse{Provider: contracts.ProviderRef{Provider: p.Name(), Model: modelName}, Content: decoded.Message.Content, ToolCalls: toolCalls, FinishReason: finishReason, Usage: usage, Cost: contracts.ModelCost{Currency: "USD", Source: "not_configured"}, WirePayload: wire}, nil
}

// Stream adapts Ollama's newline-delimited response stream to provider-neutral
// events and stops at the configured output/evidence limits.
func (p *OllamaProvider) Stream(ctx context.Context, request contracts.ModelRequest, sink StreamSink) (contracts.ModelResponse, error) {
	if requestHasToolFlow(request) {
		return contracts.ModelResponse{}, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "Ollama tool-call streaming is not supported by this provider adapter", Provider: p.Name()}}
	}
	modelName, messages, tools, err := p.chatRequest(request)
	if err != nil {
		return contracts.ModelResponse{}, err
	}
	payload, err := json.Marshal(ollamaChatRequest{Model: modelName, Messages: messages, Tools: tools, Stream: true, Options: map[string]any{"num_predict": request.Budget.MaxOutputTokens}})
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
		if len(chunk.Message.ToolCalls) > 0 {
			return contracts.ModelResponse{}, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "Ollama returned a tool call on a text-only stream", Provider: p.Name(), ContentEmitted: content.Len() > 0}, Evidence: wire.Bytes()}
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

func (p *OllamaProvider) chatRequest(request contracts.ModelRequest) (string, []ollamaMessage, []ollamaTool, error) {
	modelName := strings.TrimSpace(request.Provider.Model)
	if modelName == "" {
		modelName = p.endpoint.DefaultModel
	}
	if modelName == "" {
		return "", nil, nil, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "Ollama model is required", Provider: p.Name()}}
	}
	messages := make([]ollamaMessage, 0, len(request.Messages)+1)
	for _, message := range request.Messages {
		converted := ollamaMessage{Role: message.Role, Content: message.Content}
		if message.Role == "tool" {
			converted.ToolName = message.Name
			if converted.ToolName == "" {
				return "", nil, nil, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "Ollama tool-result history requires a tool name", Provider: p.Name()}}
			}
		}
		for _, call := range message.ToolCalls {
			converted.ToolCalls = append(converted.ToolCalls, ollamaToolCall{
				Type: "function", Function: ollamaToolCallFunction{Name: call.ToolID, Arguments: append(json.RawMessage(nil), call.Arguments...)},
			})
		}
		messages = append(messages, converted)
	}
	if len(messages) == 0 {
		messages = append(messages, ollamaMessage{Role: "user", Content: request.Prompt})
	}
	toolNames := make(map[string]struct{}, len(request.Tools))
	tools := make([]ollamaTool, 0, len(request.Tools))
	for _, definition := range request.Tools {
		name := strings.TrimSpace(definition.Name)
		if name == "" {
			return "", nil, nil, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "Ollama tool catalog contains an empty function name", Provider: p.Name()}}
		}
		if _, duplicate := toolNames[strings.ToLower(name)]; duplicate {
			return "", nil, nil, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "Ollama tool catalog contains duplicate function names", Provider: p.Name()}}
		}
		toolNames[strings.ToLower(name)] = struct{}{}
		if len(definition.Parameters) == 0 || !json.Valid(definition.Parameters) {
			return "", nil, nil, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "Ollama tool catalog contains an invalid parameter schema", Provider: p.Name()}}
		}
		tools = append(tools, ollamaTool{Type: "function", Function: ollamaToolDefinition{Name: name, Description: definition.Description, Parameters: append(json.RawMessage(nil), definition.Parameters...)}})
	}
	for _, message := range request.Messages {
		for _, call := range message.ToolCalls {
			if _, ok := toolNames[strings.ToLower(call.ToolID)]; !ok {
				return "", nil, nil, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "Ollama tool-call history references a function outside the current catalog", Provider: p.Name()}}
			}
		}
	}
	return modelName, messages, tools, nil
}

func requestHasToolFlow(request contracts.ModelRequest) bool {
	if len(request.Tools) > 0 {
		return true
	}
	for _, message := range request.Messages {
		if message.Role == "tool" || len(message.ToolCalls) > 0 {
			return true
		}
	}
	return false
}

func normalizeOllamaToolCalls(requestID string, calls []ollamaToolCall) ([]contracts.ModelToolCall, error) {
	out := make([]contracts.ModelToolCall, 0, len(calls))
	for index, call := range calls {
		arguments := append(json.RawMessage(nil), call.Function.Arguments...)
		if len(arguments) == 0 {
			arguments = json.RawMessage(`{}`)
		}
		if !json.Valid(arguments) {
			return nil, fmt.Errorf("tool call arguments are not valid JSON")
		}
		name := strings.TrimSpace(call.Function.Name)
		if name == "" {
			return nil, fmt.Errorf("tool call function name is empty")
		}
		identity := requestID + "\x00" + fmt.Sprint(index) + "\x00" + name + "\x00" + string(arguments)
		digest := sha256.Sum256([]byte(identity))
		out = append(out, contracts.ModelToolCall{ID: "ollama_call_" + hex.EncodeToString(digest[:16]), ToolID: name, Arguments: arguments})
	}
	return out, nil
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
	Tools    []ollamaTool    `json:"tools,omitempty"`
	Stream   bool            `json:"stream"`
	Options  map[string]any  `json:"options,omitempty"`
}

type ollamaTool struct {
	Type     string               `json:"type"`
	Function ollamaToolDefinition `json:"function"`
}

type ollamaToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters"`
}

type ollamaMessage struct {
	Role      string           `json:"role"`
	Content   string           `json:"content,omitempty"`
	Name      string           `json:"name,omitempty"`
	ToolName  string           `json:"tool_name,omitempty"`
	ToolCalls []ollamaToolCall `json:"tool_calls,omitempty"`
}

type ollamaToolCall struct {
	Type     string                 `json:"type,omitempty"`
	Function ollamaToolCallFunction `json:"function"`
}

type ollamaToolCallFunction struct {
	Name      string          `json:"name"`
	Arguments json.RawMessage `json:"arguments"`
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
