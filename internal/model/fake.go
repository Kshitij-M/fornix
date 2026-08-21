package model

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/omaveda/fornix/internal/contracts"
)

// FakeProvider is deterministic by default and supports scripted failures for
// retry/fallback tests. It never performs network or filesystem I/O.
type FakeProvider struct {
	mu           sync.Mutex
	endpoint     contracts.ModelEndpoint
	response     string
	toolCalls    []contracts.ModelToolCall
	streamChunks []string
	failures     []contracts.ModelFailure
	calls        int
}

type FakeConfig struct {
	Response     string
	ToolCalls    []contracts.ModelToolCall
	StreamChunks []string
	Failures     []contracts.ModelFailure
	Model        string
}

func NewFakeProvider(cfg FakeConfig) *FakeProvider {
	modelName := strings.TrimSpace(cfg.Model)
	if modelName == "" {
		modelName = "fake-model"
	}
	return &FakeProvider{
		endpoint: contracts.ModelEndpoint{ID: "fake", Provider: "fake", DefaultModel: modelName, Enabled: true},
		response: cfg.Response, toolCalls: cloneToolCalls(cfg.ToolCalls), streamChunks: append([]string(nil), cfg.StreamChunks...), failures: append([]contracts.ModelFailure(nil), cfg.Failures...),
	}
}

func (p *FakeProvider) Name() string                      { return "fake" }
func (p *FakeProvider) Aliases() []string                 { return []string{"test", "mock"} }
func (p *FakeProvider) Endpoint() contracts.ModelEndpoint { return p.endpoint }

func (p *FakeProvider) Complete(_ context.Context, request contracts.ModelRequest) (contracts.ModelResponse, error) {
	p.mu.Lock()
	p.calls++
	failure := p.nextFailureLocked()
	callNumber := p.calls
	p.mu.Unlock()
	if failure != nil {
		failure.Provider = p.Name()
		return contracts.ModelResponse{}, &FailureError{Failure: *failure}
	}
	content := p.responseFor(request)
	toolCalls := p.toolCallsFor(request)
	usage := contracts.ModelUsage{InputTokens: request.EstimatedInputTokens(), OutputTokens: contracts.EstimateModelTokens(content), Source: "fake"}
	finish := "stop"
	if len(toolCalls) > 0 {
		finish = "tool_calls"
	}
	return contracts.ModelResponse{RequestID: request.RequestID, Provider: contracts.ProviderRef{Provider: p.Name(), Model: modelFor(request, p.endpoint.DefaultModel)}, Content: content, ToolCalls: toolCalls, FinishReason: finish, Usage: usage, Cost: contracts.ModelCost{Currency: "USD", Source: "fake"}, ProviderRequestID: fmt.Sprintf("fake-%d", callNumber)}, nil
}

func (p *FakeProvider) Stream(_ context.Context, request contracts.ModelRequest, sink StreamSink) (contracts.ModelResponse, error) {
	p.mu.Lock()
	p.calls++
	failure := p.nextFailureLocked()
	callNumber := p.calls
	chunks := append([]string(nil), p.streamChunks...)
	p.mu.Unlock()
	if failure != nil {
		failure.Provider = p.Name()
		return contracts.ModelResponse{}, &FailureError{Failure: *failure}
	}
	content := p.responseFor(request)
	toolCalls := p.toolCallsFor(request)
	if len(chunks) == 0 {
		for start := 0; start < len(content); start += 8 {
			end := start + 8
			if end > len(content) {
				end = len(content)
			}
			chunks = append(chunks, content[start:end])
		}
	}
	var assembled strings.Builder
	for _, chunk := range chunks {
		assembled.WriteString(chunk)
		if sink != nil {
			sink(contracts.ModelStreamEvent{Type: contracts.ModelStreamTextDelta, Delta: chunk, ProviderRequestID: fmt.Sprintf("fake-%d", callNumber)})
		}
	}
	usage := contracts.ModelUsage{InputTokens: request.EstimatedInputTokens(), OutputTokens: contracts.EstimateModelTokens(assembled.String()), Source: "fake"}
	if sink != nil {
		usageEvent := usage
		sink(contracts.ModelStreamEvent{Type: contracts.ModelStreamUsage, Usage: &usageEvent, ProviderRequestID: fmt.Sprintf("fake-%d", callNumber)})
	}
	finish := "stop"
	if len(toolCalls) > 0 {
		finish = "tool_calls"
	}
	response := contracts.ModelResponse{RequestID: request.RequestID, Provider: contracts.ProviderRef{Provider: p.Name(), Model: modelFor(request, p.endpoint.DefaultModel)}, Content: assembled.String(), ToolCalls: toolCalls, FinishReason: finish, Usage: usage, Cost: contracts.ModelCost{Currency: "USD", Source: "fake"}, ProviderRequestID: fmt.Sprintf("fake-%d", callNumber)}
	if sink != nil {
		sink(contracts.ModelStreamEvent{Type: contracts.ModelStreamCompletion, Response: &response, ProviderRequestID: response.ProviderRequestID})
	}
	return response, nil
}

func (p *FakeProvider) Embed(_ context.Context, request EmbeddingRequest) ([]float32, error) {
	if strings.TrimSpace(request.Text) == "" {
		return nil, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureInvalidRequest, Message: "fake embedding text is empty", Provider: p.Name()}}
	}
	const dimensions = 768
	result := make([]float32, dimensions)
	for i := range result {
		result[i] = float32((i%31)+1) / 31
	}
	return result, nil
}

func (p *FakeProvider) Calls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *FakeProvider) nextFailureLocked() *contracts.ModelFailure {
	if len(p.failures) == 0 {
		return nil
	}
	failure := p.failures[0]
	p.failures = p.failures[1:]
	return &failure
}

func (p *FakeProvider) responseFor(request contracts.ModelRequest) string {
	if p.response != "" {
		return p.response
	}
	hash, err := request.RequestHash()
	if err != nil || len(hash) < 16 {
		return "fake response"
	}
	return "fake response " + hash[:16]
}

func (p *FakeProvider) toolCallsFor(request contracts.ModelRequest) []contracts.ModelToolCall {
	p.mu.Lock()
	scripted := cloneToolCalls(p.toolCalls)
	p.mu.Unlock()
	if len(scripted) > 0 {
		return scripted
	}
	referenceWorkflow := strings.EqualFold(strings.TrimSpace(request.Metadata["fornix.reference_workflow"]), "true") || strings.Contains(strings.ToLower(request.Prompt), "[fornix-reference-workflow]")
	hasToolResult := false
	if !referenceWorkflow {
		for _, message := range request.Messages {
			if strings.Contains(strings.ToLower(message.Content), "[fornix-reference-workflow]") {
				referenceWorkflow = true
				break
			}
		}
	}
	for _, message := range request.Messages {
		if message.Role == "tool" {
			hasToolResult = true
			break
		}
	}
	if hasToolResult {
		return nil
	}
	if referenceWorkflow {
		for _, definition := range request.Tools {
			if definition.Name != "fornix.repository.read" && definition.Name != "repository.read" {
				continue
			}
			workdir := strings.TrimSpace(request.Metadata["fornix.reference_workdir"])
			if workdir == "" {
				workdir = "/workspace/fixtures/reference-repo"
				for _, message := range request.Messages {
					for _, field := range strings.Fields(message.Content) {
						if strings.HasPrefix(field, "workdir=") {
							workdir = strings.TrimPrefix(field, "workdir=")
						}
					}
				}
			}
			arguments, _ := json.Marshal(map[string]any{"argv": []string{"README.md"}, "workdir": workdir})
			return []contracts.ModelToolCall{{ID: "reference-repository-read", ToolID: definition.Name, Arguments: arguments}}
		}
	}
	return nil
}

func modelFor(request contracts.ModelRequest, fallback string) string {
	if strings.TrimSpace(request.Provider.Model) != "" {
		return strings.TrimSpace(request.Provider.Model)
	}
	return fallback
}

func cloneToolCalls(in []contracts.ModelToolCall) []contracts.ModelToolCall {
	out := append([]contracts.ModelToolCall(nil), in...)
	for i := range out {
		out[i].Arguments = append([]byte(nil), in[i].Arguments...)
	}
	return out
}
