package model

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

// CredentialResolver resolves a named credential reference at call time. The
// resolved secret must remain inside the provider boundary.
type CredentialResolver func(string) (string, error)

// OpenAIConfig configures an OpenAI-compatible endpoint. Live use is explicit
// and credentials may be supplied directly only by trusted process setup.
type OpenAIConfig struct {
	Endpoint          contracts.ModelEndpoint
	APIKey            string
	RequireAPIKey     bool
	HTTPClient        *http.Client
	Timeout           time.Duration
	ResolveCredential CredentialResolver
}

// OpenAIProvider adapts chat-completions-compatible HTTP endpoints with
// bounded retries, redaction, and optional provider idempotency headers.
type OpenAIProvider struct {
	endpoint          contracts.ModelEndpoint
	apiKey            string
	requireAPIKey     bool
	client            *http.Client
	timeout           time.Duration
	resolveCredential CredentialResolver
}

// NewOpenAIProvider validates an OpenAI-compatible endpoint without making a
// network request.
func NewOpenAIProvider(cfg OpenAIConfig) (*OpenAIProvider, error) {
	endpoint := cfg.Endpoint
	if endpoint.Provider == "" {
		endpoint.Provider = "openai"
	}
	endpoint.Provider = strings.ToLower(strings.TrimSpace(endpoint.Provider))
	if endpoint.BaseURL == "" {
		endpoint.BaseURL = "https://api.openai.com/v1"
	}
	if endpoint.DefaultModel == "" {
		endpoint.DefaultModel = "gpt-4o-mini"
	}
	if err := validateProviderURL(endpoint.BaseURL, endpoint.AllowPrivate); err != nil {
		return nil, err
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = contracts.DefaultModelTimeout
	}
	if cfg.Timeout > contracts.MaxModelTimeout {
		return nil, fmt.Errorf("openai timeout exceeds %s", contracts.MaxModelTimeout)
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: cfg.Timeout}
	}
	if client.Timeout == 0 {
		client.Timeout = cfg.Timeout
	}
	return &OpenAIProvider{
		endpoint: endpoint, apiKey: strings.TrimSpace(cfg.APIKey),
		requireAPIKey: cfg.RequireAPIKey, client: client,
		timeout: cfg.Timeout, resolveCredential: cfg.ResolveCredential,
	}, nil
}

func (p *OpenAIProvider) Name() string                      { return "openai" }
func (p *OpenAIProvider) Aliases() []string                 { return []string{"openai-compatible"} }
func (p *OpenAIProvider) Endpoint() contracts.ModelEndpoint { return p.endpoint }

// Complete executes one bounded non-streaming chat-completion request.
func (p *OpenAIProvider) Complete(ctx context.Context, request contracts.ModelRequest) (contracts.ModelResponse, error) {
	apiKey, err := p.credential()
	if err != nil {
		return contracts.ModelResponse{}, err
	}
	body, model, err := p.buildRequest(request, false)
	if err != nil {
		return contracts.ModelResponse{}, p.safeError(err, apiKey)
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return contracts.ModelResponse{}, p.safeError(&FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "marshal model request failed", Provider: p.Name()}}, apiKey)
	}
	httpRequest, err := p.newRequest(ctx, payload, apiKey, false, request.IdempotencyKey)
	if err != nil {
		return contracts.ModelResponse{}, p.safeError(err, apiKey)
	}
	response, err := p.client.Do(httpRequest)
	if err != nil {
		return contracts.ModelResponse{}, p.safeError(classifyHTTPError(p.Name(), err, 0, nil), apiKey)
	}
	defer response.Body.Close()
	wire, err := readBounded(response.Body, contracts.MaxModelEvidenceBytes*4)
	if err != nil {
		return contracts.ModelResponse{}, p.safeError(&FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureTransport, Message: "read model response failed", Provider: p.Name(), Retryable: true}, Evidence: []byte(err.Error())}, apiKey)
	}
	if response.StatusCode >= http.StatusBadRequest {
		return contracts.ModelResponse{}, p.safeError(withRetryAfter(classifyHTTPError(p.Name(), nil, response.StatusCode, wire), response.Header.Get("Retry-After")), apiKey)
	}
	parsed := openAIResponse{}
	if err := json.Unmarshal(wire, &parsed); err != nil {
		return contracts.ModelResponse{}, p.safeError(&FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureProvider, Message: "decode model response failed", Provider: p.Name()}, Evidence: wire}, apiKey)
	}
	if parsed.Error != nil {
		return contracts.ModelResponse{}, p.safeError(classifyHTTPError(p.Name(), nil, response.StatusCode, wire), apiKey)
	}
	if len(parsed.Choices) == 0 {
		return contracts.ModelResponse{}, p.safeError(&FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureEmptyResponse, Message: "model response has no choices", Provider: p.Name()}, Evidence: wire}, apiKey)
	}
	usage := parseOpenAIUsage(parsed.Usage)
	result := contracts.ModelResponse{
		Provider:          contracts.ProviderRef{Provider: p.Name(), Model: model},
		Content:           redactCredentialText(parsed.Choices[0].Message.Content, apiKey),
		ToolCalls:         parseOpenAIToolCalls(parsed.Choices[0].Message.ToolCalls),
		FinishReason:      parsed.Choices[0].FinishReason,
		Usage:             usage,
		Cost:              p.cost(usage),
		ProviderRequestID: strings.TrimSpace(parsed.ID),
		WirePayload:       redactCredential(wire, apiKey),
	}
	return result, nil
}

// Stream adapts server-sent chat completion events and refuses fallback after
// content has been emitted.
func (p *OpenAIProvider) Stream(ctx context.Context, request contracts.ModelRequest, sink StreamSink) (contracts.ModelResponse, error) {
	apiKey, err := p.credential()
	if err != nil {
		return contracts.ModelResponse{}, err
	}
	body, model, err := p.buildRequest(request, true)
	if err != nil {
		return contracts.ModelResponse{}, p.safeError(err, apiKey)
	}
	payload, err := json.Marshal(body)
	if err != nil {
		return contracts.ModelResponse{}, p.safeError(&FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "marshal model request failed", Provider: p.Name()}}, apiKey)
	}
	httpRequest, err := p.newRequest(ctx, payload, apiKey, true, request.IdempotencyKey)
	if err != nil {
		return contracts.ModelResponse{}, p.safeError(err, apiKey)
	}
	response, err := p.client.Do(httpRequest)
	if err != nil {
		return contracts.ModelResponse{}, p.safeError(classifyHTTPError(p.Name(), err, 0, nil), apiKey)
	}
	defer response.Body.Close()
	if response.StatusCode >= http.StatusBadRequest {
		wire, readErr := readBounded(response.Body, contracts.MaxModelEvidenceBytes*4)
		if readErr != nil {
			return contracts.ModelResponse{}, p.safeError(classifyHTTPError(p.Name(), readErr, response.StatusCode, nil), apiKey)
		}
		return contracts.ModelResponse{}, p.safeError(withRetryAfter(classifyHTTPError(p.Name(), nil, response.StatusCode, wire), response.Header.Get("Retry-After")), apiKey)
	}

	var content strings.Builder
	contentRunes := 0
	var usage contracts.ModelUsage
	providerRequestID := ""
	finishReason := "stop"
	completed := false
	wire := bytes.Buffer{}
	scanner := bufio.NewScanner(response.Body)
	scanner.Buffer(make([]byte, 4096), contracts.MaxModelEvidenceBytes)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data:") {
			data := strings.TrimSpace(strings.TrimPrefix(line, "data:"))
			if data == "" {
				continue
			}
			if wire.Len()+len(data)+1 > contracts.MaxModelEvidenceBytes {
				return contracts.ModelResponse{}, p.safeError(&FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureBudget, Message: "provider stream evidence budget exceeded", Provider: p.Name(), ContentEmitted: content.Len() > 0}, Evidence: wire.Bytes()}, apiKey)
			}
			wire.WriteString(data)
			wire.WriteByte('\n')
			if data == "[DONE]" {
				completed = true
				continue
			}
			chunk := openAIStreamChunk{}
			if err := json.Unmarshal([]byte(data), &chunk); err != nil {
				return contracts.ModelResponse{}, p.safeError(&FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureProvider, Message: "decode model stream event failed", Provider: p.Name(), ContentEmitted: content.Len() > 0}, Evidence: wire.Bytes()}, apiKey)
			}
			if chunk.Error != nil {
				return contracts.ModelResponse{}, p.safeError(classifyHTTPError(p.Name(), nil, http.StatusBadGateway, []byte(chunk.Error.Message)), apiKey)
			}
			if chunk.ID != "" {
				providerRequestID = chunk.ID
			}
			if chunk.Usage != nil {
				usage = parseOpenAIUsage(chunk.Usage)
				usageEvent := usage
				if sink != nil {
					sink(contracts.ModelStreamEvent{Type: contracts.ModelStreamUsage, Usage: &usageEvent, ProviderRequestID: providerRequestID})
				}
			}
			for _, choice := range chunk.Choices {
				if choice.FinishReason != "" {
					finishReason = choice.FinishReason
				}
				if choice.Delta.Content != "" {
					delta := redactCredentialText(choice.Delta.Content, apiKey)
					if budgetErr := appendBoundedStreamContent(&content, &contentRunes, delta, request.Budget, p.Name(), wire.Bytes()); budgetErr != nil {
						return contracts.ModelResponse{}, p.safeError(budgetErr, apiKey)
					}
					if sink != nil {
						sink(contracts.ModelStreamEvent{Type: contracts.ModelStreamTextDelta, Delta: delta, ProviderRequestID: providerRequestID})
					}
				}
			}
		}
	}
	if err := scanner.Err(); err != nil {
		return contracts.ModelResponse{}, p.safeError(classifyHTTPError(p.Name(), err, 0, wire.Bytes()), apiKey)
	}
	if !completed {
		return contracts.ModelResponse{}, p.safeError(&FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureTransport, Message: "model stream ended before completion", Provider: p.Name(), Retryable: true, ContentEmitted: content.Len() > 0}, Evidence: wire.Bytes()}, apiKey)
	}
	result := contracts.ModelResponse{
		Provider:          contracts.ProviderRef{Provider: p.Name(), Model: model},
		Content:           content.String(),
		FinishReason:      finishReason,
		Usage:             usage,
		Cost:              p.cost(usage),
		ProviderRequestID: providerRequestID,
		WirePayload:       redactCredential(wire.Bytes(), apiKey),
	}
	return result, nil
}

// Embed reports that this provider has no configured embedding capability.
func (p *OpenAIProvider) Embed(context.Context, EmbeddingRequest) ([]float32, error) {
	return nil, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureProvider, Message: "openai embedding capability is not configured", Provider: p.Name()}}
}

func (p *OpenAIProvider) credential() (string, error) {
	if p.resolveCredential != nil && p.endpoint.CredentialRef != "" {
		value, err := p.resolveCredential(p.endpoint.CredentialRef)
		if err != nil {
			return "", &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureAuthentication, Message: "credential resolution failed", Provider: p.Name()}}
		}
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value), nil
		}
	}
	if strings.TrimSpace(p.apiKey) != "" {
		return p.apiKey, nil
	}
	if p.requireAPIKey {
		return "", &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureAuthentication, Message: "provider credential is not configured", Provider: p.Name()}}
	}
	return "", nil
}

func (p *OpenAIProvider) safeError(err error, credential string) error {
	if err == nil || strings.TrimSpace(credential) == "" {
		return err
	}
	var failureErr *FailureError
	if !errors.As(err, &failureErr) {
		return err
	}
	failure := failureErr.Failure
	failure.Message = redactCredentialText(failure.Message, credential)
	failure.ProviderRequestID = redactCredentialText(failure.ProviderRequestID, credential)
	return &FailureError{Failure: failure, Evidence: redactCredential(failureErr.Evidence, credential)}
}

func (p *OpenAIProvider) buildRequest(request contracts.ModelRequest, stream bool) (openAIRequest, string, error) {
	model := strings.TrimSpace(request.Provider.Model)
	if model == "" {
		model = p.endpoint.DefaultModel
	}
	if model == "" {
		return openAIRequest{}, "", &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "model is required", Provider: p.Name()}}
	}
	messages := make([]openAIMessage, 0, len(request.Messages)+2)
	for _, message := range request.Messages {
		calls := make([]openAIToolCall, 0, len(message.ToolCalls))
		for _, call := range message.ToolCalls {
			calls = append(calls, openAIToolCall{ID: call.ID, Type: "function", Function: openAIFunctionCall{Arguments: string(call.Arguments), Name: call.ToolID}})
		}
		messages = append(messages, openAIMessage{Role: message.Role, Content: message.Content, Name: message.Name, ToolCallID: message.ToolCallID, ToolCalls: calls})
	}
	if len(messages) == 0 {
		messages = append(messages, openAIMessage{Role: "user", Content: request.Prompt})
	}
	tools := make([]openAIToolDefinition, 0, len(request.Tools))
	for _, definition := range request.Tools {
		tools = append(tools, openAIToolDefinition{Type: "function", Function: openAIFunctionDefinition{Name: definition.Name, Description: definition.Description, Parameters: definition.Parameters}})
	}
	return openAIRequest{Model: model, Messages: messages, Tools: tools, Stream: stream, StreamOptions: streamOptions(stream), MaxTokens: request.Budget.MaxOutputTokens}, model, nil
}

func streamOptions(stream bool) *openAIStreamOptions {
	if !stream {
		return nil
	}
	include := true
	return &openAIStreamOptions{IncludeUsage: &include}
}

func (p *OpenAIProvider) newRequest(ctx context.Context, payload []byte, apiKey string, stream bool, idempotencyKey string) (*http.Request, error) {
	base := strings.TrimRight(p.endpoint.BaseURL, "/")
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/chat/completions", bytes.NewReader(payload))
	if err != nil {
		return nil, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "build provider request failed", Provider: p.Name()}}
	}
	request.Header.Set("Content-Type", "application/json")
	if stream {
		request.Header.Set("Accept", "text/event-stream")
	}
	if apiKey != "" {
		request.Header.Set("Authorization", "Bearer "+apiKey)
	}
	if strings.TrimSpace(idempotencyKey) != "" {
		request.Header.Set("Idempotency-Key", strings.TrimSpace(idempotencyKey))
	}
	return request, nil
}

func (p *OpenAIProvider) cost(usage contracts.ModelUsage) contracts.ModelCost {
	cost := contracts.ModelCost{Currency: "USD", Source: "configured_endpoint"}
	if p.endpoint.InputCostPer1KUSD <= 0 && p.endpoint.OutputCostPer1KUSD <= 0 {
		cost.Source = "not_configured"
		return cost
	}
	cost.InputCostUSD = float64(usage.InputTokens) / 1000 * p.endpoint.InputCostPer1KUSD
	cost.OutputCostUSD = float64(usage.OutputTokens) / 1000 * p.endpoint.OutputCostPer1KUSD
	cost.TotalCostUSD = cost.InputCostUSD + cost.OutputCostUSD
	return cost
}

type openAIRequest struct {
	Model         string                 `json:"model"`
	Messages      []openAIMessage        `json:"messages"`
	Tools         []openAIToolDefinition `json:"tools,omitempty"`
	Stream        bool                   `json:"stream,omitempty"`
	StreamOptions *openAIStreamOptions   `json:"stream_options,omitempty"`
	MaxTokens     int                    `json:"max_tokens,omitempty"`
}

type openAIStreamOptions struct {
	IncludeUsage *bool `json:"include_usage"`
}

type openAIMessage struct {
	Role       string           `json:"role"`
	Content    string           `json:"content,omitempty"`
	Name       string           `json:"name,omitempty"`
	ToolCallID string           `json:"tool_call_id,omitempty"`
	ToolCalls  []openAIToolCall `json:"tool_calls,omitempty"`
}

type openAIToolDefinition struct {
	Type     string                   `json:"type"`
	Function openAIFunctionDefinition `json:"function"`
}

type openAIFunctionDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

type openAIToolCall struct {
	ID       string             `json:"id"`
	Type     string             `json:"type"`
	Function openAIFunctionCall `json:"function"`
}

type openAIFunctionCall struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

func parseOpenAIToolCalls(raw []openAIToolCall) []contracts.ModelToolCall {
	out := make([]contracts.ModelToolCall, 0, len(raw))
	for _, call := range raw {
		arguments := json.RawMessage(call.Function.Arguments)
		if len(arguments) == 0 {
			arguments = json.RawMessage(`{}`)
		}
		if !json.Valid(arguments) {
			continue
		}
		out = append(out, contracts.ModelToolCall{ID: strings.TrimSpace(call.ID), ToolID: strings.TrimSpace(call.Function.Name), Arguments: arguments})
	}
	return out
}

type openAIResponse struct {
	ID      string         `json:"id"`
	Choices []openAIChoice `json:"choices"`
	Usage   *openAIUsage   `json:"usage,omitempty"`
	Error   *openAIError   `json:"error,omitempty"`
}

type openAIStreamChunk struct {
	ID      string               `json:"id"`
	Choices []openAIStreamChoice `json:"choices"`
	Usage   *openAIUsage         `json:"usage,omitempty"`
	Error   *openAIError         `json:"error,omitempty"`
}

type openAIChoice struct {
	Message      openAIMessage `json:"message"`
	FinishReason string        `json:"finish_reason"`
}

type openAIStreamChoice struct {
	Delta        openAIMessage `json:"delta"`
	FinishReason string        `json:"finish_reason"`
}

type openAIUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type openAIError struct {
	Message string `json:"message"`
	Type    string `json:"type"`
	Code    any    `json:"code"`
}

func parseOpenAIUsage(usage *openAIUsage) contracts.ModelUsage {
	if usage == nil {
		return contracts.ModelUsage{Source: "provider"}
	}
	return contracts.ModelUsage{InputTokens: usage.PromptTokens, OutputTokens: usage.CompletionTokens, TotalTokens: usage.TotalTokens, Source: "provider"}
}

func classifyHTTPError(provider string, requestErr error, status int, body []byte) error {
	detail := strings.TrimSpace(string(body))
	if requestErr != nil {
		detail = requestErr.Error()
	}
	lower := strings.ToLower(detail)
	code := contracts.ModelFailureProvider
	retryable := false
	if requestErr != nil {
		var networkErr net.Error
		switch {
		case errors.Is(requestErr, context.DeadlineExceeded):
			code = contracts.ModelFailureTimeout
			retryable = true
		case errors.As(requestErr, &networkErr) && networkErr.Timeout():
			code = contracts.ModelFailureTimeout
			retryable = true
		default:
			code = contracts.ModelFailureTransport
			retryable = true
		}
	} else {
		switch {
		case status == http.StatusUnauthorized || status == http.StatusForbidden:
			code = contracts.ModelFailureAuthentication
		case status == http.StatusPaymentRequired:
			code = contracts.ModelFailureQuota
		case status == http.StatusTooManyRequests:
			if strings.Contains(lower, "quota") || strings.Contains(lower, "credit") || strings.Contains(lower, "balance") {
				code = contracts.ModelFailureQuota
			} else {
				code = contracts.ModelFailureRateLimit
				retryable = true
			}
		case status == http.StatusRequestTimeout:
			code = contracts.ModelFailureTimeout
			retryable = true
		case status >= 500:
			code = contracts.ModelFailureTransport
			retryable = true
		case strings.Contains(lower, "context") && (strings.Contains(lower, "window") || strings.Contains(lower, "length") || strings.Contains(lower, "token")):
			code = contracts.ModelFailureContextWindow
		case status >= 400:
			code = contracts.ModelFailureInvalidRequest
		}
	}
	if detail == "" {
		detail = "provider request failed"
	}
	return &FailureError{Failure: contracts.ModelFailure{Code: code, Message: redactText(detail), Provider: provider, StatusCode: status, Retryable: retryable}, Evidence: RedactBytes(body)}
}

func withRetryAfter(err error, value string) error {
	if err == nil {
		return nil
	}
	var failureErr *FailureError
	if !errors.As(err, &failureErr) {
		return err
	}
	failure := failureErr.Failure
	failure.RetryAfterMS = parseRetryAfter(value)
	return &FailureError{Failure: failure, Evidence: failureErr.Evidence}
}

func readBounded(reader io.Reader, limit int) ([]byte, error) {
	if limit <= 0 {
		limit = contracts.MaxModelEvidenceBytes
	}
	value, err := io.ReadAll(io.LimitReader(reader, int64(limit)+1))
	if err != nil {
		return nil, err
	}
	if len(value) > limit {
		return nil, fmt.Errorf("provider response exceeds %d bytes", limit)
	}
	return value, nil
}

func validateProviderURL(raw string, allowPrivate bool) error {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("provider base URL must be an absolute HTTP(S) URL without credentials or query parameters")
	}
	if !allowPrivate && isPrivateHost(parsed.Hostname()) {
		return fmt.Errorf("private provider base URL requires allow_private")
	}
	return nil
}

func isPrivateHost(host string) bool {
	host = strings.ToLower(strings.TrimSpace(host))
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
		return true
	}
	return false
}

func parseRetryAfter(value string) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	seconds, err := strconv.Atoi(value)
	if err != nil || seconds < 0 {
		return 0
	}
	return seconds * 1000
}
