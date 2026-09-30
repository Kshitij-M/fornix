package model

import (
	"context"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

type countingEmbeddingProvider struct {
	*FakeProvider
	mu       sync.Mutex
	embedN   int
	embedErr error
}

type passthroughEmbeddingEffects struct{}

func (passthroughEmbeddingEffects) RunEmbedding(ctx context.Context, _ contracts.EmbeddingRequest, _ contracts.ProviderRef, invoke func(context.Context) ([]float32, error)) ([]float32, error) {
	return invoke(ctx)
}

func (passthroughEmbeddingEffects) RunEmbeddingReconciliation(ctx context.Context, _ contracts.EmbeddingReconciliationRequest, _ contracts.ProviderRef, invoke func(context.Context) (contracts.EmbeddingResponse, error)) (contracts.EmbeddingResponse, error) {
	return invoke(ctx)
}

func (p *countingEmbeddingProvider) Embed(ctx context.Context, request EmbeddingRequest) ([]float32, error) {
	p.mu.Lock()
	p.embedN++
	p.mu.Unlock()
	if p.embedErr != nil {
		return nil, p.embedErr
	}
	return p.FakeProvider.Embed(ctx, request)
}

func (p *countingEmbeddingProvider) EmbedScoped(ctx context.Context, request contracts.EmbeddingRequest) (contracts.EmbeddingResponse, error) {
	vector, err := p.Embed(ctx, EmbeddingRequest{Model: request.Model, Text: request.Text, MaxInputBytes: request.Budget.MaxInputBytes, Timeout: time.Duration(request.Budget.TimeoutMS) * time.Millisecond})
	if err != nil {
		return contracts.EmbeddingResponse{}, err
	}
	hash, err := contracts.EmbeddingVectorHash(vector)
	if err != nil {
		return contracts.EmbeddingResponse{}, err
	}
	return contracts.EmbeddingResponse{RequestID: request.RequestID, Provider: request.Provider, Vector: vector, VectorHash: hash, Dimension: len(vector)}, nil
}

func (p *countingEmbeddingProvider) EmbeddingCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.embedN
}

type memoryEmbeddingRecorder struct {
	mu     sync.Mutex
	record contracts.EmbeddingCallRecord
}

func (r *memoryEmbeddingRecorder) Start(_ context.Context, request contracts.EmbeddingRequest, _ []byte) (contracts.EmbeddingCallStart, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	requestHash := request.RequestHash()
	if r.record.RequestID != "" {
		if r.record.RequestHash != requestHash {
			return contracts.EmbeddingCallStart{}, errors.New("request conflict")
		}
		return contracts.EmbeddingCallStart{Record: r.record, Existing: true}, nil
	}
	r.record = contracts.EmbeddingCallRecord{WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey, RequestHash: requestHash, SourceKind: request.SourceKind, SourceID: request.SourceID, SourceHash: request.SourceHash, Provider: request.Provider, Actor: request.Actor, Task: request.Task, Session: request.Session, Status: contracts.EmbeddingCallPending, Budget: request.Budget}
	return contracts.EmbeddingCallStart{Record: r.record}, nil
}

func (r *memoryEmbeddingRecorder) Attempt(_ context.Context, _, _ string) error {
	r.mu.Lock()
	r.record.Status = contracts.EmbeddingCallRunning
	r.record.AttemptCount++
	r.mu.Unlock()
	return nil
}

func (r *memoryEmbeddingRecorder) Finish(_ context.Context, result contracts.EmbeddingCallResult) error {
	r.mu.Lock()
	r.record.Status = result.Status
	r.record.Vector = append([]float32(nil), result.Vector...)
	if len(result.Vector) > 0 {
		r.record.VectorHash, _ = contracts.EmbeddingVectorHash(result.Vector)
		r.record.VectorDimension = len(result.Vector)
	}
	r.record.Failure = result.Failure
	r.record.ProviderRequestID = result.ProviderRequestID
	r.record.Usage = result.Usage
	r.mu.Unlock()
	return nil
}

func (r *memoryEmbeddingRecorder) Get(_ context.Context, workspaceID, requestID string) (contracts.EmbeddingCallRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.record.WorkspaceID != workspaceID || r.record.RequestID != requestID {
		return contracts.EmbeddingCallRecord{}, errors.New("embedding call not found")
	}
	copy := r.record
	copy.Vector = append([]float32(nil), r.record.Vector...)
	return copy, nil
}

func (r *memoryEmbeddingRecorder) ResolveRecovery(_ context.Context, result contracts.EmbeddingReconciliationResult) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.record.Status != contracts.EmbeddingCallRecoveryRequired {
		resultHash, _ := contracts.EmbeddingVectorHash(result.Vector)
		if r.record.Status == result.Status && r.record.VectorHash == resultHash {
			return nil
		}
		return errors.New("embedding call is not recovery-required")
	}
	r.record.Status = result.Status
	r.record.ProviderRequestID = result.ProviderRequestID
	r.record.SourceHash = result.SourceHash
	r.record.Vector = append([]float32(nil), result.Vector...)
	r.record.VectorHash, _ = contracts.EmbeddingVectorHash(result.Vector)
	r.record.VectorDimension = len(result.Vector)
	r.record.Failure = result.Failure
	return nil
}

type reconcilingEmbeddingProvider struct {
	*countingEmbeddingProvider
	providerRequestID string
}

type memoryEmbeddingFinalizer struct {
	request contracts.EmbeddingRecoveryFinalizeRequest
}

func (f *memoryEmbeddingFinalizer) FinalizeEmbeddingRecovery(_ context.Context, request contracts.EmbeddingRecoveryFinalizeRequest) (contracts.EmbeddingCallRecord, error) {
	if err := request.Normalize(); err != nil {
		return contracts.EmbeddingCallRecord{}, err
	}
	f.request = request
	return contracts.EmbeddingCallRecord{
		WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, Status: request.Result.Status,
		Provider: request.Result.Provider, SourceHash: request.Result.SourceHash,
		ProviderRequestID: request.Result.ProviderRequestID, Vector: append([]float32(nil), request.Result.Vector...),
		VectorHash: func() string { value, _ := contracts.EmbeddingVectorHash(request.Result.Vector); return value }(),
	}, nil
}

func (p *reconcilingEmbeddingProvider) ReconcileEmbedding(_ context.Context, request contracts.EmbeddingReconciliationRequest) (contracts.EmbeddingResponse, error) {
	vector := make([]float32, contracts.EmbeddingDimension)
	for i := range vector {
		vector[i] = float32((i%17)+1) / 17
	}
	hash, err := contracts.EmbeddingVectorHash(vector)
	if err != nil {
		return contracts.EmbeddingResponse{}, err
	}
	return contracts.EmbeddingResponse{RequestID: request.RequestID, Provider: request.Provider, SourceHash: request.SourceHash, ProviderRequestID: p.providerRequestID, Vector: vector, VectorHash: hash, Dimension: len(vector)}, nil
}

func embeddingTestRequest() contracts.EmbeddingRequest {
	return contracts.EmbeddingRequest{
		RequestID: "embedding-request-1", IdempotencyKey: "embedding-idempotency-1",
		WorkspaceID: "workspace-a", Actor: contracts.ActorRef{ID: "actor-a", Kind: "user", WorkspaceID: "workspace-a"},
		Provider: contracts.ProviderRef{Provider: "fake", Model: "fake-model"}, Model: "fake-model",
		SourceKind: "memo", SourceID: "memo-1", Text: "stable embedding input",
		Budget: contracts.EmbeddingBudget{MaxInputBytes: contracts.MaxEmbeddingInputBytes, Dimension: contracts.EmbeddingDimension, TimeoutMS: 1000},
	}
}

func TestEmbeddingGatewayReplaysCompletedCallWithoutProvider(t *testing.T) {
	provider := &countingEmbeddingProvider{FakeProvider: NewFakeProvider(FakeConfig{Model: "fake-model"})}
	registry := NewRegistry()
	if err := registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	recorder := &memoryEmbeddingRecorder{}
	gateway := NewEmbeddingGateway(registry, recorder)
	gateway.Effects = passthroughEmbeddingEffects{}
	request := embeddingTestRequest()
	first, err := gateway.Embed(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := gateway.Embed(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if provider.EmbeddingCalls() != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.EmbeddingCalls())
	}
	firstHash, _ := contracts.EmbeddingVectorHash(first)
	secondHash, _ := contracts.EmbeddingVectorHash(second)
	if firstHash != secondHash {
		t.Fatalf("replayed vector hash = %s, want %s", secondHash, firstHash)
	}
}

func TestEmbeddingGatewayRequiresDurableEffectBoundary(t *testing.T) {
	provider := &countingEmbeddingProvider{FakeProvider: NewFakeProvider(FakeConfig{Model: "fake-model"})}
	registry := NewRegistry()
	if err := registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	gateway := NewEmbeddingGateway(registry, &memoryEmbeddingRecorder{})
	if _, err := gateway.Embed(context.Background(), embeddingTestRequest()); !errors.Is(err, ErrEmbeddingNotConfigured) {
		t.Fatalf("error = %v, want durable boundary error", err)
	}
	if provider.EmbeddingCalls() != 0 {
		t.Fatalf("provider calls = %d, want 0", provider.EmbeddingCalls())
	}
}

func TestEmbeddingGatewayDoesNotBlindlyRetryUncertainProvider(t *testing.T) {
	provider := &countingEmbeddingProvider{
		FakeProvider: NewFakeProvider(FakeConfig{Model: "fake-model"}),
		embedErr:     &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureTransport, Message: "transport", Provider: "fake"}},
	}
	registry := NewRegistry()
	if err := registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	recorder := &memoryEmbeddingRecorder{}
	gateway := NewEmbeddingGateway(registry, recorder)
	gateway.Effects = passthroughEmbeddingEffects{}
	request := embeddingTestRequest()
	if _, err := gateway.Embed(context.Background(), request); err == nil {
		t.Fatal("expected uncertain provider failure")
	}
	if _, err := gateway.Embed(context.Background(), request); !errors.Is(err, ErrEmbeddingRecoveryRequired) {
		t.Fatalf("second call error = %v, want recovery-required", err)
	}
	if provider.EmbeddingCalls() != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.EmbeddingCalls())
	}
	if recorder.record.Status != contracts.EmbeddingCallRecoveryRequired {
		t.Fatalf("record status = %q, want recovery_required", recorder.record.Status)
	}
}

func TestEmbeddingGatewayReconcileWithLeaseUsesOriginalRecoveryAuthority(t *testing.T) {
	provider := &reconcilingEmbeddingProvider{countingEmbeddingProvider: &countingEmbeddingProvider{FakeProvider: NewFakeProvider(FakeConfig{Model: "fake-model"})}, providerRequestID: "provider-receipt"}
	registry := NewRegistry()
	if err := registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	recorder := &memoryEmbeddingRecorder{}
	request := embeddingTestRequest()
	if err := request.Normalize(); err != nil {
		t.Fatal(err)
	}
	recorder.record = contracts.EmbeddingCallRecord{
		WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, IdempotencyKey: request.IdempotencyKey,
		RequestHash: request.RequestHash(), SourceKind: request.SourceKind, SourceID: request.SourceID,
		SourceHash: request.SourceHash, Provider: request.Provider, Actor: request.Actor,
		Status: contracts.EmbeddingCallRecoveryRequired, ProviderRequestID: "provider-receipt", Budget: request.Budget,
	}
	finalizer := &memoryEmbeddingFinalizer{}
	gateway := NewEmbeddingGateway(registry, recorder)
	gateway.RecoveryFinalizer = finalizer
	resolved, err := gateway.ReconcileWithLease(context.Background(), contracts.EmbeddingRecoveryFinalizeRequest{
		WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, OwnerID: "recovery-worker", Fence: 7,
		ExpectedEffectVersion: 3, ExpectedLinkVersion: 2, IdempotencyKey: "recovery-command",
		Actor: request.Actor,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resolved.Status != contracts.EmbeddingCallSucceeded || finalizer.request.Result.ProviderRequestID != "provider-receipt" || len(finalizer.request.Result.Vector) != contracts.EmbeddingDimension {
		t.Fatalf("resolved=%+v finalizer=%+v", resolved, finalizer.request)
	}
	if provider.EmbeddingCalls() != 0 {
		t.Fatalf("generation provider calls=%d, want 0", provider.EmbeddingCalls())
	}
}

func TestEmbeddingGatewayReconcilesOnlyWithProviderProof(t *testing.T) {
	provider := &reconcilingEmbeddingProvider{countingEmbeddingProvider: &countingEmbeddingProvider{FakeProvider: NewFakeProvider(FakeConfig{Model: "fake-model"})}, providerRequestID: "provider-receipt-1"}
	registry := NewRegistry()
	if err := registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	recorder := &memoryEmbeddingRecorder{}
	gateway := NewEmbeddingGateway(registry, recorder)
	gateway.Effects = passthroughEmbeddingEffects{}
	request := embeddingTestRequest()
	request.Provider.Provider = "fake"
	if _, err := gateway.Embed(context.Background(), request); err != nil {
		// The fake generation succeeds; put the durable record into the
		// explicit recovery state to exercise the proof-only path.
		t.Fatal(err)
	}
	recorder.mu.Lock()
	recorder.record.Status = contracts.EmbeddingCallRecoveryRequired
	recorder.record.ProviderRequestID = provider.providerRequestID
	recorder.mu.Unlock()
	vector, err := gateway.Reconcile(context.Background(), request.WorkspaceID, request.RequestID)
	if err != nil {
		t.Fatal(err)
	}
	if len(vector) != contracts.EmbeddingDimension || recorder.record.Status != contracts.EmbeddingCallSucceeded {
		t.Fatalf("reconciled vector/status = %d/%s", len(vector), recorder.record.Status)
	}
	if _, err := gateway.Reconcile(context.Background(), request.WorkspaceID, request.RequestID); err != nil {
		t.Fatal(err)
	}
}

func TestEmbeddingGatewayRejectsReconciliationWithoutProviderRequestIdentity(t *testing.T) {
	provider := &reconcilingEmbeddingProvider{countingEmbeddingProvider: &countingEmbeddingProvider{FakeProvider: NewFakeProvider(FakeConfig{Model: "fake-model"})}, providerRequestID: "provider-receipt-1"}
	registry := NewRegistry()
	if err := registry.Register(provider); err != nil {
		t.Fatal(err)
	}
	recorder := &memoryEmbeddingRecorder{}
	gateway := NewEmbeddingGateway(registry, recorder)
	gateway.Effects = passthroughEmbeddingEffects{}
	request := embeddingTestRequest()
	if _, err := gateway.Embed(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	recorder.mu.Lock()
	recorder.record.Status = contracts.EmbeddingCallRecoveryRequired
	recorder.record.ProviderRequestID = ""
	recorder.mu.Unlock()
	if _, err := gateway.Reconcile(context.Background(), request.WorkspaceID, request.RequestID); err == nil {
		t.Fatal("expected missing provider identity failure")
	}
}

func TestEmbeddingVectorHashRejectsNonFiniteValues(t *testing.T) {
	vector := make([]float32, contracts.EmbeddingDimension)
	vector[17] = float32(math.NaN())
	if _, err := contracts.EmbeddingVectorHash(vector); err == nil {
		t.Fatal("expected NaN rejection")
	}
}
