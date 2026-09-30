package model

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrEmbeddingNotConfigured             = errors.New("embedding gateway is not configured")
	ErrEmbeddingInFlight                  = errors.New("embedding call is already in progress")
	ErrEmbeddingRecoveryRequired          = errors.New("embedding call requires external outcome recovery")
	ErrEmbeddingReconciliationUnsupported = errors.New("embedding provider does not support outcome reconciliation")
	ErrEmbeddingReconciliationConflict    = errors.New("embedding reconciliation result conflicts with durable call")
)

// EmbeddingCallRecorder is the durable ledger seam used by the gateway. The
// provider package remains independent from the Postgres implementation.
type EmbeddingCallRecorder interface {
	Start(context.Context, contracts.EmbeddingRequest, []byte) (contracts.EmbeddingCallStart, error)
	Attempt(context.Context, string, string) error
	Finish(context.Context, contracts.EmbeddingCallResult) error
}

// EmbeddingEffectRunner reserves the generic fenced external effect before
// invoke is called. It is intentionally narrower than chat execution because
// embeddings never stream content or fall back after dispatch.
type EmbeddingEffectRunner interface {
	RunEmbedding(context.Context, contracts.EmbeddingRequest, contracts.ProviderRef, func(context.Context) ([]float32, error)) ([]float32, error)
}

// EmbeddingReconciliationEffectRunner is the distinct durable boundary for a
// provider outcome query. It is optional in unit-test gateways but mandatory
// for production reconciliation.
type EmbeddingReconciliationEffectRunner interface {
	RunEmbeddingReconciliation(context.Context, contracts.EmbeddingReconciliationRequest, contracts.ProviderRef, func(context.Context) (contracts.EmbeddingResponse, error)) (contracts.EmbeddingResponse, error)
}

// EmbeddingRecoveryRecorder extends the normal ledger with explicit
// recovery-only transitions. Keeping it optional preserves small offline
// recorders while production gateways fail closed when recovery is requested.
type EmbeddingRecoveryRecorder interface {
	EmbeddingCallRecorder
	Get(context.Context, string, string) (contracts.EmbeddingCallRecord, error)
	ResolveRecovery(context.Context, contracts.EmbeddingReconciliationResult) error
}

// EmbeddingRecoveryFinalizer is the local, transactionally composed recovery
// authority. The model package depends only on the contract, so the provider
// gateway cannot accidentally own Postgres transaction or lease semantics.
type EmbeddingRecoveryFinalizer interface {
	FinalizeEmbeddingRecovery(context.Context, contracts.EmbeddingRecoveryFinalizeRequest) (contracts.EmbeddingCallRecord, error)
}

// EmbeddingFailureError is a redacted, provider-neutral embedding failure.
// The original provider error is not wrapped so it cannot accidentally leak a
// credential or wire payload through a generic logger.
type EmbeddingFailureError struct {
	Failure contracts.EmbeddingFailure
}

func (e *EmbeddingFailureError) Error() string {
	if e == nil {
		return "embedding provider failure"
	}
	return fmt.Sprintf("embedding provider failure code=%s provider=%s", e.Failure.Code, e.Failure.Provider)
}

// EmbeddingGateway owns provider selection, durable call identity, bounded
// execution, vector validation, and replay. It does not write domain vector
// projections; callers attach the replayable result through their own scoped
// transaction.
type EmbeddingGateway struct {
	Registry          *Registry
	Recorder          EmbeddingCallRecorder
	Effects           EmbeddingEffectRunner
	RecoveryFinalizer EmbeddingRecoveryFinalizer
}

func NewEmbeddingGateway(registry *Registry, recorder EmbeddingCallRecorder) *EmbeddingGateway {
	return &EmbeddingGateway{Registry: registry, Recorder: recorder}
}

// Embed executes or replays one scoped embedding request. A completed call
// never invokes a provider; a recovery-required call never receives an
// implicit retry.
func (g *EmbeddingGateway) Embed(ctx context.Context, request contracts.EmbeddingRequest) ([]float32, error) {
	if g == nil || g.Registry == nil || g.Recorder == nil || g.Effects == nil {
		return nil, ErrEmbeddingNotConfigured
	}
	if err := request.Normalize(); err != nil {
		return nil, fmt.Errorf("normalize embedding request: %w", err)
	}
	if provider, found := g.Registry.Lookup(request.Provider.Provider); found {
		if enriched, enrichErr := enrichProviderRef(g.Registry, request.Provider); enrichErr == nil {
			request.Provider = enriched
		} else if _, boundaryProvider := provider.(BoundaryProvider); boundaryProvider {
			return nil, enrichErr
		}
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(request.Budget.TimeoutMS)*time.Millisecond)
	defer cancel()
	var start contracts.EmbeddingCallStart
	evidence := embeddingRequestEvidence(request)
	var err error
	start, err = g.Recorder.Start(ctx, request, evidence)
	if err != nil {
		return nil, err
	}
	if start.Existing {
		return replayEmbeddingCall(start.Record)
	}
	if err := g.Recorder.Attempt(ctx, request.WorkspaceID, request.RequestID); err != nil {
		return nil, err
	}
	provider, ok := g.Registry.Lookup(request.Provider.Provider)
	if !ok {
		return g.finishFailure(ctx, request, start, contracts.EmbeddingFailure{Code: contracts.EmbeddingFailureProvider, Message: "provider is not registered", Provider: request.Provider.Provider})
	}
	embeddingProvider, ok := provider.(EmbeddingProvider)
	if !ok {
		return g.finishFailure(ctx, request, start, contracts.EmbeddingFailure{Code: contracts.EmbeddingFailureProvider, Message: "provider does not support embeddings", Provider: request.Provider.Provider})
	}
	if request.Text == "" {
		return g.finishFailure(ctx, request, start, contracts.EmbeddingFailure{Code: contracts.EmbeddingFailureInvalidRequest, Message: "embedding text is unavailable for a new provider call", Provider: request.Provider.Provider})
	}
	providerCalled := false
	providerUsage := contracts.EmbeddingUsage{}
	providerRequestID := ""
	invoke := func(invokeCtx context.Context) ([]float32, error) {
		providerCalled = true
		response, invokeErr := embeddingProvider.EmbedScoped(invokeCtx, request)
		if invokeErr != nil {
			return nil, invokeErr
		}
		providerUsage = response.Usage
		providerRequestID = response.ProviderRequestID
		if response.SourceHash != "" && strings.ToLower(strings.TrimSpace(response.SourceHash)) != request.SourceHash {
			return nil, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureProvider, Message: "provider returned a mismatched source hash", Provider: request.Provider.Provider}}
		}
		if response.VectorHash != "" {
			expected, hashErr := contracts.EmbeddingVectorHash(response.Vector)
			if hashErr != nil || expected != response.VectorHash {
				return nil, &FailureError{Failure: contracts.ModelFailure{Code: contracts.ModelFailureProvider, Message: "provider returned an invalid vector hash", Provider: request.Provider.Provider}}
			}
		}
		return response.Vector, nil
	}
	var vector []float32
	vector, err = g.Effects.RunEmbedding(ctx, request, request.Provider, invoke)
	if err != nil {
		failure := classifyEmbeddingFailure(err, request.Provider.Provider, providerCalled)
		return g.finishFailure(ctx, request, start, failure)
	}
	if _, err := contracts.EmbeddingVectorHash(vector); err != nil {
		failure := contracts.EmbeddingFailure{Code: contracts.EmbeddingFailureProvider, Message: "provider returned an invalid embedding vector", Provider: request.Provider.Provider, PossiblyStarted: providerCalled}
		return g.finishFailure(ctx, request, start, failure)
	}
	if providerUsage.Dimension == 0 {
		providerUsage = contracts.EmbeddingUsage{InputBytes: int64(len([]byte(request.Text))), Dimension: len(vector), Source: "estimated", Measured: false}
	}
	if err := g.Recorder.Finish(ctx, contracts.EmbeddingCallResult{
		WorkspaceID: request.WorkspaceID, RequestID: request.RequestID,
		TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence,
		Status: contracts.EmbeddingCallSucceeded, AttemptCount: 1, ProviderRequestID: providerRequestID,
		Usage:  providerUsage,
		Vector: vector, ResponseEvidence: embeddingResponseEvidence(vector),
	}); err != nil {
		return nil, err
	}
	return append([]float32(nil), vector...), nil
}

// Reconcile resolves a recovery_required call only through a provider-specific
// proof path. It never accepts a caller-supplied vector and never invokes the
// generation capability again.
func (g *EmbeddingGateway) Reconcile(ctx context.Context, workspaceID, requestID string) ([]float32, error) {
	if g == nil || g.Registry == nil || g.Recorder == nil || g.Effects == nil {
		return nil, ErrEmbeddingNotConfigured
	}
	recorder, ok := g.Recorder.(EmbeddingRecoveryRecorder)
	if !ok {
		return nil, ErrEmbeddingReconciliationUnsupported
	}
	recoveryEffects, ok := g.Effects.(EmbeddingReconciliationEffectRunner)
	if !ok {
		return nil, ErrEmbeddingReconciliationUnsupported
	}
	workspaceID, requestID = strings.TrimSpace(workspaceID), strings.TrimSpace(requestID)
	if workspaceID == "" || requestID == "" {
		return nil, fmt.Errorf("workspace_id and request_id are required")
	}
	record, err := recorder.Get(ctx, workspaceID, requestID)
	if err != nil {
		return nil, err
	}
	if record.Status != contracts.EmbeddingCallRecoveryRequired {
		return replayEmbeddingCall(record)
	}
	reconciliation := contracts.EmbeddingReconciliationRequest{
		SchemaVersion: contracts.EmbeddingSchemaVersion, WorkspaceID: record.WorkspaceID,
		RequestID: record.RequestID, IdempotencyKey: record.IdempotencyKey,
		RequestHash: record.RequestHash, Provider: record.Provider, Model: record.Provider.Model,
		CausationID: record.CausationID, CorrelationID: record.CorrelationID,
		SourceKind: record.SourceKind, SourceID: record.SourceID, SourceHash: record.SourceHash,
		ProviderRequestID: record.ProviderRequestID, Actor: record.Actor, Task: record.Task,
		TaskOwnerID: record.TaskOwnerID, TaskFence: record.TaskFence, Session: record.Session,
		Budget: record.Budget,
	}
	if err := reconciliation.Normalize(); err != nil {
		return nil, fmt.Errorf("normalize embedding reconciliation: %w", err)
	}
	provider, found := g.Registry.Lookup(record.Provider.Provider)
	if !found {
		return nil, ErrEmbeddingReconciliationUnsupported
	}
	reconciler, ok := provider.(EmbeddingReconciler)
	if !ok {
		return nil, ErrEmbeddingReconciliationUnsupported
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(maxEmbeddingTimeout(record.Budget.TimeoutMS))*time.Millisecond)
	defer cancel()
	response, err := recoveryEffects.RunEmbeddingReconciliation(ctx, reconciliation, record.Provider, func(invokeCtx context.Context) (contracts.EmbeddingResponse, error) {
		return reconciler.ReconcileEmbedding(invokeCtx, reconciliation)
	})
	if err != nil {
		return nil, err
	}
	if err := validateReconciliationResponse(response, reconciliation); err != nil {
		return nil, err
	}
	status := contracts.EmbeddingCallSucceeded
	responseEvidence := embeddingResponseEvidence(response.Vector)
	if response.Failure != nil {
		status = contracts.EmbeddingCallFailed
		responseEvidence = embeddingFailureEvidence(*response.Failure)
	}
	result := contracts.EmbeddingReconciliationResult{WorkspaceID: record.WorkspaceID, RequestID: record.RequestID, TaskOwnerID: record.TaskOwnerID, TaskFence: record.TaskFence, Status: status, Provider: response.Provider, SourceHash: response.SourceHash, ProviderRequestID: response.ProviderRequestID, Usage: response.Usage, Vector: response.Vector, Failure: response.Failure, ResponseEvidence: responseEvidence}
	if err := recorder.ResolveRecovery(ctx, result); err != nil {
		return nil, err
	}
	resolved, err := recorder.Get(ctx, workspaceID, requestID)
	if err != nil {
		return nil, err
	}
	return replayEmbeddingCall(resolved)
}

// ReconcileWithLease resolves an ambiguous embedding through the original
// effect-recovery fence. It intentionally invokes only the provider's
// reconciliation capability; the caller supplies a valid effect lease and
// observed versions, and the local finalizer commits all authorities once.
// This is the production path. Reconcile remains for small compatibility
// recorders and tests, but production callers must use this method.
func (g *EmbeddingGateway) ReconcileWithLease(ctx context.Context, command contracts.EmbeddingRecoveryFinalizeRequest) (contracts.EmbeddingCallRecord, error) {
	if g == nil || g.Registry == nil || g.Recorder == nil || g.RecoveryFinalizer == nil {
		return contracts.EmbeddingCallRecord{}, ErrEmbeddingReconciliationUnsupported
	}
	command.WorkspaceID, command.RequestID, command.OwnerID, command.IdempotencyKey = strings.TrimSpace(command.WorkspaceID), strings.TrimSpace(command.RequestID), strings.TrimSpace(command.OwnerID), strings.TrimSpace(command.IdempotencyKey)
	if command.WorkspaceID == "" || command.RequestID == "" || command.OwnerID == "" || command.Fence == 0 || command.ExpectedEffectVersion < 1 || command.ExpectedLinkVersion < 1 || command.IdempotencyKey == "" {
		return contracts.EmbeddingCallRecord{}, fmt.Errorf("embedding recovery lease and expected versions are required")
	}
	recorder, ok := g.Recorder.(EmbeddingRecoveryRecorder)
	if !ok {
		return contracts.EmbeddingCallRecord{}, ErrEmbeddingReconciliationUnsupported
	}
	record, err := recorder.Get(ctx, command.WorkspaceID, command.RequestID)
	if err != nil {
		return contracts.EmbeddingCallRecord{}, err
	}
	if record.Status != contracts.EmbeddingCallRecoveryRequired {
		return record, nil
	}
	reconciliation := contracts.EmbeddingReconciliationRequest{
		SchemaVersion: contracts.EmbeddingSchemaVersion, WorkspaceID: record.WorkspaceID,
		RequestID: record.RequestID, IdempotencyKey: record.IdempotencyKey,
		RequestHash: record.RequestHash, Provider: record.Provider, Model: record.Provider.Model,
		CausationID: record.CausationID, CorrelationID: record.CorrelationID,
		SourceKind: record.SourceKind, SourceID: record.SourceID, SourceHash: record.SourceHash,
		ProviderRequestID: record.ProviderRequestID, Actor: record.Actor, Task: record.Task,
		TaskOwnerID: record.TaskOwnerID, TaskFence: record.TaskFence, Session: record.Session,
		Budget: record.Budget,
	}
	if err := reconciliation.Normalize(); err != nil {
		return contracts.EmbeddingCallRecord{}, fmt.Errorf("normalize embedding reconciliation: %w", err)
	}
	provider, found := g.Registry.Lookup(record.Provider.Provider)
	if !found {
		return contracts.EmbeddingCallRecord{}, ErrEmbeddingReconciliationUnsupported
	}
	reconciler, ok := provider.(EmbeddingReconciler)
	if !ok {
		return contracts.EmbeddingCallRecord{}, ErrEmbeddingReconciliationUnsupported
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(maxEmbeddingTimeout(record.Budget.TimeoutMS))*time.Millisecond)
	defer cancel()
	response, err := reconciler.ReconcileEmbedding(ctx, reconciliation)
	if err != nil {
		return contracts.EmbeddingCallRecord{}, err
	}
	if err := validateReconciliationResponse(response, reconciliation); err != nil {
		return contracts.EmbeddingCallRecord{}, err
	}
	status := contracts.EmbeddingCallSucceeded
	responseEvidence := embeddingResponseEvidence(response.Vector)
	if response.Failure != nil {
		status = contracts.EmbeddingCallFailed
		responseEvidence = embeddingFailureEvidence(*response.Failure)
	}
	command.Result = contracts.EmbeddingReconciliationResult{
		WorkspaceID: record.WorkspaceID, RequestID: record.RequestID, Actor: command.Actor,
		TaskOwnerID: record.TaskOwnerID, TaskFence: record.TaskFence, Status: status,
		Provider: response.Provider, SourceHash: response.SourceHash,
		ProviderRequestID: response.ProviderRequestID, Usage: response.Usage,
		Vector: response.Vector, Failure: response.Failure, ResponseEvidence: responseEvidence,
	}
	if err := command.Normalize(); err != nil {
		return contracts.EmbeddingCallRecord{}, err
	}
	return g.RecoveryFinalizer.FinalizeEmbeddingRecovery(ctx, command)
}

func validateReconciliationResponse(response contracts.EmbeddingResponse, request contracts.EmbeddingReconciliationRequest) error {
	if strings.ToLower(strings.TrimSpace(response.Provider.Provider)) != request.Provider.Provider || response.Provider.Model != request.Model {
		return ErrEmbeddingReconciliationConflict
	}
	if response.RequestID != "" && response.RequestID != request.RequestID {
		return ErrEmbeddingReconciliationConflict
	}
	if strings.TrimSpace(response.ProviderRequestID) == "" || response.ProviderRequestID != request.ProviderRequestID {
		return ErrEmbeddingReconciliationConflict
	}
	if strings.ToLower(strings.TrimSpace(response.SourceHash)) != request.SourceHash {
		return ErrEmbeddingReconciliationConflict
	}
	if response.Failure != nil {
		if len(response.Vector) != 0 {
			return ErrEmbeddingReconciliationConflict
		}
		return nil
	}
	if _, err := contracts.EmbeddingVectorHash(response.Vector); err != nil {
		return fmt.Errorf("validate reconciled embedding vector: %w", err)
	}
	return nil
}

func maxEmbeddingTimeout(value int) int {
	if value < 1 || value > 600000 {
		return 30000
	}
	return value
}

func (g *EmbeddingGateway) finishFailure(ctx context.Context, request contracts.EmbeddingRequest, start contracts.EmbeddingCallStart, failure contracts.EmbeddingFailure) ([]float32, error) {
	if failure.Provider == "" {
		failure.Provider = request.Provider.Provider
	}
	if !start.Existing {
		status := contracts.EmbeddingCallFailed
		if failure.PossiblyStarted {
			status = contracts.EmbeddingCallRecoveryRequired
		}
		if err := g.Recorder.Finish(ctx, contracts.EmbeddingCallResult{WorkspaceID: request.WorkspaceID, RequestID: request.RequestID, TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence, Status: status, AttemptCount: 1, ProviderRequestID: failure.ProviderRequestID, Failure: &failure, ResponseEvidence: embeddingFailureEvidence(failure)}); err != nil {
			return nil, err
		}
	}
	if failure.PossiblyStarted {
		return nil, &EmbeddingFailureError{Failure: failure}
	}
	return nil, &EmbeddingFailureError{Failure: failure}
}

func replayEmbeddingCall(record contracts.EmbeddingCallRecord) ([]float32, error) {
	switch record.Status {
	case contracts.EmbeddingCallSucceeded:
		computedHash, err := contracts.EmbeddingVectorHash(record.Vector)
		if err != nil {
			return nil, fmt.Errorf("stored embedding vector failed integrity validation: %w", err)
		}
		if strings.ToLower(strings.TrimSpace(record.VectorHash)) != computedHash {
			return nil, fmt.Errorf("stored embedding vector hash failed integrity validation")
		}
		return append([]float32(nil), record.Vector...), nil
	case contracts.EmbeddingCallRecoveryRequired:
		return nil, ErrEmbeddingRecoveryRequired
	case contracts.EmbeddingCallPending, contracts.EmbeddingCallRunning:
		return nil, ErrEmbeddingInFlight
	case contracts.EmbeddingCallFailed, contracts.EmbeddingCallCancelled:
		if record.Failure != nil {
			return nil, &EmbeddingFailureError{Failure: *record.Failure}
		}
		return nil, &EmbeddingFailureError{Failure: contracts.EmbeddingFailure{Code: contracts.EmbeddingFailureProvider, Message: "embedding call failed", Provider: record.Provider.Provider}}
	case contracts.EmbeddingCallExpired:
		return nil, ErrEmbeddingRecoveryRequired
	default:
		return nil, fmt.Errorf("unknown embedding call status %q", record.Status)
	}
}

func classifyEmbeddingFailure(err error, provider string, providerCalled bool) contracts.EmbeddingFailure {
	failure := contracts.EmbeddingFailure{Code: contracts.EmbeddingFailureProvider, Message: "embedding provider request failed", Provider: provider, PossiblyStarted: providerCalled}
	var modelFailure *FailureError
	if errors.As(err, &modelFailure) {
		failure.Code = strings.ToLower(strings.TrimSpace(modelFailure.Failure.Code))
		failure.Message = "embedding provider request failed"
		failure.Provider = provider
		failure.Retryable = modelFailure.Failure.Retryable
		failure.ProviderRequestID = modelFailure.Failure.ProviderRequestID
		failure.PossiblyStarted = providerCalled
		switch failure.Code {
		case contracts.ModelFailureAuthentication, contracts.ModelFailureQuota, contracts.ModelFailureRateLimit, contracts.ModelFailureContextWindow, contracts.ModelFailureInvalidRequest, contracts.ModelFailureBudget:
			failure.PossiblyStarted = false
		case contracts.ModelFailureTransport, contracts.ModelFailureTimeout:
			failure.PossiblyStarted = true
		}
	}
	if failure.Code == "" {
		failure.Code = contracts.EmbeddingFailureProvider
	}
	return failure
}

func embeddingRequestEvidence(request contracts.EmbeddingRequest) []byte {
	return []byte(fmt.Sprintf(`{"schema_version":%d,"workspace_id":%q,"request_id":%q,"provider":%q,"model":%q,"source_kind":%q,"source_id":%q,"source_hash":%q,"input_bytes":%d}`,
		contracts.EmbeddingSchemaVersion, request.WorkspaceID, request.RequestID, request.Provider.Provider, request.Model, request.SourceKind, request.SourceID, request.SourceHash, len([]byte(request.Text))))
}

func embeddingResponseEvidence(vector []float32) []byte {
	hash, _ := contracts.EmbeddingVectorHash(vector)
	return []byte(fmt.Sprintf(`{"vector_hash":%q,"vector_dimension":%d}`, hash, len(vector)))
}

func embeddingFailureEvidence(failure contracts.EmbeddingFailure) []byte {
	return []byte(fmt.Sprintf(`{"code":%q,"provider":%q,"provider_request_id":%q,"possibly_started":%t}`, failure.Code, failure.Provider, failure.ProviderRequestID, failure.PossiblyStarted))
}
