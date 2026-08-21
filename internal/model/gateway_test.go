package model

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

type memoryCallRecorder struct {
	mu      sync.Mutex
	records map[string]contracts.ModelCallRecord
}

func newMemoryCallRecorder() *memoryCallRecorder {
	return &memoryCallRecorder{records: make(map[string]contracts.ModelCallRecord)}
}

func (r *memoryCallRecorder) Start(_ context.Context, request contracts.ModelRequest, evidence []byte) (CallStart, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	hash, err := request.RequestHash()
	if err != nil {
		return CallStart{}, err
	}
	if record, ok := r.records[request.WorkspaceID+"\x00"+request.IdempotencyKey]; ok {
		if record.RequestHash != hash {
			return CallStart{}, ErrModelCallCompleted
		}
		return CallStart{Record: record, Existing: true}, nil
	}
	record := contracts.ModelCallRecord{
		WorkspaceID: request.WorkspaceID, RequestID: request.RequestID,
		IdempotencyKey: request.IdempotencyKey, RequestHash: hash,
		Provider: request.Provider, Status: contracts.ModelCallRunning,
		RequestEvidence: evidence, CreatedAt: time.Now().UTC(),
	}
	r.records[request.WorkspaceID+"\x00"+request.IdempotencyKey] = record
	return CallStart{Record: record}, nil
}

func (r *memoryCallRecorder) Attempt(_ context.Context, workspaceID, requestID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, record := range r.records {
		if record.WorkspaceID == workspaceID && record.RequestID == requestID {
			record.AttemptCount++
			r.records[key] = record
			return nil
		}
	}
	return ErrModelCallCompleted
}

func (r *memoryCallRecorder) Finish(_ context.Context, result contracts.ModelCallResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	for key, record := range r.records {
		if record.WorkspaceID != result.WorkspaceID || record.RequestID != result.RequestID {
			continue
		}
		record.Status = result.Status
		if result.AttemptCount > record.AttemptCount {
			record.AttemptCount = result.AttemptCount
		}
		record.ContentEmitted = result.ContentEmitted
		record.ProviderRequestID = result.ProviderRequestID
		record.Usage = result.Usage
		record.Cost = result.Cost
		record.Failure = result.Failure
		record.Response = result.Response
		record.ResponseEvidence = result.ResponseEvidence
		r.records[key] = record
		return nil
	}
	return ErrModelCallCompleted
}

func modelTestRequest(provider string) contracts.ModelRequest {
	request := contracts.NewModelRequest("workspace-model-test", provider, "test-model", "return a stable answer")
	request.RequestID = "model-request-1"
	request.IdempotencyKey = "model-idempotency-1"
	request.RetryPolicy = contracts.RetryPolicy{MaxAttempts: 3, InitialDelayMS: 0, MaxDelayMS: 0, RetryableCodes: []string{contracts.ModelFailureTransport, contracts.ModelFailureRateLimit, contracts.ModelFailureTimeout}}
	return request
}

func TestFakeProviderIsDeterministicAndDurablyDeduplicated(t *testing.T) {
	provider := NewFakeProvider(FakeConfig{Response: "stable fake response"})
	registry := NewRegistry()
	if err := registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	recorder := newMemoryCallRecorder()
	gateway := NewGateway(registry, recorder)
	request := modelTestRequest("fake")

	first, err := gateway.Complete(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := gateway.Complete(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.Content != second.Content || first.ContentHash != second.ContentHash {
		t.Fatalf("replay changed response: first=%+v second=%+v", first, second)
	}
	if provider.Calls() != 1 {
		t.Fatalf("provider calls = %d, want one durable effect", provider.Calls())
	}
	record := recorder.records[request.WorkspaceID+"\x00"+request.IdempotencyKey]
	if record.Status != contracts.ModelCallSucceeded || record.AttemptCount != 1 {
		t.Fatalf("durable record = %+v", record)
	}
}

func TestGatewayRetriesOnlyRetryableFailures(t *testing.T) {
	registry := NewRegistry()
	retryProvider := NewFakeProvider(FakeConfig{Response: "recovered", Failures: []contracts.ModelFailure{
		{Code: contracts.ModelFailureTransport, Message: "temporary", Retryable: true},
		{Code: contracts.ModelFailureTransport, Message: "temporary", Retryable: true},
	}})
	if err := registry.Register(retryProvider); err != nil {
		t.Fatal(err)
	}
	gateway := NewGateway(registry, nil)
	request := modelTestRequest("fake")
	response, err := gateway.Complete(context.Background(), request)
	if err != nil || response.Content != "recovered" {
		t.Fatalf("retry result=%+v err=%v", response, err)
	}
	if retryProvider.Calls() != 3 {
		t.Fatalf("retry calls = %d, want 3", retryProvider.Calls())
	}

	nonRetryProvider := NewFakeProvider(FakeConfig{Failures: []contracts.ModelFailure{{Code: contracts.ModelFailureAuthentication, Message: "denied", Retryable: false}}})
	registry = NewRegistry()
	if err := registry.Register(nonRetryProvider); err != nil {
		t.Fatal(err)
	}
	request = modelTestRequest("fake")
	request.RetryPolicy.MaxAttempts = 3
	_, err = NewGateway(registry, nil).Complete(context.Background(), request)
	if err == nil {
		t.Fatal("non-retryable failure unexpectedly succeeded")
	}
	if nonRetryProvider.Calls() != 1 {
		t.Fatalf("non-retryable calls = %d, want 1", nonRetryProvider.Calls())
	}
}

func TestGatewayEnforcesOutputAndRequestTimeoutBudgets(t *testing.T) {
	registry := NewRegistry()
	provider := NewFakeProvider(FakeConfig{Response: "response exceeds byte budget"})
	if err := registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	request := modelTestRequest("fake")
	request.Budget.MaxOutputBytes = 4
	request.RetryPolicy.MaxAttempts = 1
	_, err := NewGateway(registry, nil).Complete(context.Background(), request)
	var failureErr *FailureError
	if !errors.As(err, &failureErr) || failureErr.Failure.Code != contracts.ModelFailureBudget {
		t.Fatalf("budget failure = %v", err)
	}

	timeoutProvider := &scriptedProvider{name: "timeout", completeCtx: func(ctx context.Context, _ contracts.ModelRequest) (contracts.ModelResponse, error) {
		<-ctx.Done()
		return contracts.ModelResponse{}, ctx.Err()
	}}
	registry = NewRegistry()
	if err := registry.Register(timeoutProvider); err != nil {
		t.Fatal(err)
	}
	request = modelTestRequest("timeout")
	request.Budget.TimeoutMS = 1
	request.RetryPolicy.MaxAttempts = 1
	_, err = NewGateway(registry, nil).Complete(context.Background(), request)
	if !errors.As(err, &failureErr) || failureErr.Failure.Code != contracts.ModelFailureTimeout {
		t.Fatalf("timeout failure = %v", err)
	}
}

type scriptedProvider struct {
	name        string
	complete    func(contracts.ModelRequest) (contracts.ModelResponse, error)
	completeCtx func(context.Context, contracts.ModelRequest) (contracts.ModelResponse, error)
	stream      func(contracts.ModelRequest, StreamSink) (contracts.ModelResponse, error)
	completeN   int
	streamN     int
	mu          sync.Mutex
}

func (p *scriptedProvider) Name() string      { return p.name }
func (p *scriptedProvider) Aliases() []string { return nil }
func (p *scriptedProvider) Endpoint() contracts.ModelEndpoint {
	return contracts.ModelEndpoint{ID: p.name, Provider: p.name, DefaultModel: "scripted", Enabled: true}
}
func (p *scriptedProvider) Complete(ctx context.Context, request contracts.ModelRequest) (contracts.ModelResponse, error) {
	return p.completeWithContext(ctx, request)
}
func (p *scriptedProvider) completeWithContext(ctx context.Context, request contracts.ModelRequest) (contracts.ModelResponse, error) {
	p.mu.Lock()
	p.completeN++
	p.mu.Unlock()
	if p.completeCtx != nil {
		return p.completeCtx(ctx, request)
	}
	if p.complete != nil {
		return p.complete(request)
	}
	return contracts.ModelResponse{}, errors.New("scripted complete not configured")
}
func (p *scriptedProvider) Stream(_ context.Context, request contracts.ModelRequest, sink StreamSink) (contracts.ModelResponse, error) {
	p.mu.Lock()
	p.streamN++
	p.mu.Unlock()
	if p.stream != nil {
		return p.stream(request, sink)
	}
	return contracts.ModelResponse{}, errors.New("scripted stream not configured")
}
func (p *scriptedProvider) Embed(context.Context, EmbeddingRequest) ([]float32, error) {
	return nil, errors.New("unsupported")
}

func TestGatewayFallsBackBeforeStreamContentOnly(t *testing.T) {
	primary := &scriptedProvider{name: "primary", stream: func(contracts.ModelRequest, StreamSink) (contracts.ModelResponse, error) {
		return contracts.ModelResponse{}, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureTransport, Message: "primary unavailable", Retryable: false}}
	}}
	partial := &scriptedProvider{name: "partial", stream: func(contracts.ModelRequest, StreamSink) (contracts.ModelResponse, error) {
		return contracts.ModelResponse{}, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureTransport, Message: "stream interrupted", Retryable: true, ContentEmitted: true}}
	}}
	fallback := NewFakeProvider(FakeConfig{Response: "fallback response"})
	registry := NewRegistry()
	for _, provider := range []Provider{primary, partial, fallback} {
		if err := registry.Register(provider); err != nil {
			t.Fatal(err)
		}
	}
	gateway := NewGateway(registry, nil)
	request := modelTestRequest("primary")
	request.RetryPolicy.MaxAttempts = 1
	var events []contracts.ModelStreamEvent
	response, err := gateway.Stream(context.Background(), request, func(event contracts.ModelStreamEvent) { events = append(events, event) }, contracts.ProviderRef{Provider: "fake"})
	if err != nil || response.Content != "fallback response" {
		t.Fatalf("pre-content fallback response=%+v err=%v", response, err)
	}
	if fallback.Calls() != 1 || len(events) == 0 {
		t.Fatalf("fallback events/calls = %d/%d", fallback.Calls(), len(events))
	}

	request = modelTestRequest("partial")
	request.IdempotencyKey = "partial-stream"
	request.RetryPolicy.MaxAttempts = 1
	partial.stream = func(_ contracts.ModelRequest, sink StreamSink) (contracts.ModelResponse, error) {
		sink(contracts.ModelStreamEvent{Type: contracts.ModelStreamTextDelta, Delta: "visible"})
		return contracts.ModelResponse{}, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureTransport, Message: "stream interrupted", Retryable: true, ContentEmitted: true}}
	}
	events = nil
	_, err = gateway.Stream(context.Background(), request, func(event contracts.ModelStreamEvent) { events = append(events, event) }, contracts.ProviderRef{Provider: "fake"})
	if err == nil || fallback.Calls() != 1 {
		t.Fatalf("partial stream fallback was incorrectly used: err=%v calls=%d", err, fallback.Calls())
	}
	if len(events) < 2 || events[0].Delta != "visible" {
		t.Fatalf("partial stream events = %+v", events)
	}
}

func TestRegistryLookupIsExplicitAndStable(t *testing.T) {
	registry := NewRegistry()
	provider := NewFakeProvider(FakeConfig{})
	if err := registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	if got, ok := registry.Lookup(" MOCK "); !ok || got.Name() != "fake" {
		t.Fatalf("alias lookup failed: %v %v", got, ok)
	}
	if got := strings.Join(registry.Names(), ","); got != "fake" {
		t.Fatalf("registry names = %q", got)
	}
	if err := registry.Register(NewFakeProvider(FakeConfig{})); !errors.Is(err, ErrProviderDuplicate) {
		t.Fatalf("duplicate registration error = %v", err)
	}
}

func BenchmarkGatewayFakeComplete(b *testing.B) {
	provider := NewFakeProvider(FakeConfig{Response: "benchmark response"})
	registry := NewRegistry()
	if err := registry.Register(provider); err != nil {
		b.Fatal(err)
	}
	gateway := NewGateway(registry, nil)
	request := modelTestRequest("fake")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := gateway.Complete(context.Background(), request); err != nil {
			b.Fatal(err)
		}
	}
}
