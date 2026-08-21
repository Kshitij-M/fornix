package model

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/omaveda/fornix/internal/contracts"
)

// Gateway owns admission, provider selection, bounded retries, safe fallback,
// and optional durable call recording. Provider implementations remain
// unaware of SQL and may be tested independently.
type Gateway struct {
	Registry *Registry
	Recorder CallRecorder
	Sleep    func(context.Context, time.Duration) error
}

// NewGateway creates a gateway with deterministic retry timing and optional
// durable call recording.
func NewGateway(registry *Registry, recorder CallRecorder) *Gateway {
	return &Gateway{Registry: registry, Recorder: recorder, Sleep: sleepContext}
}

// Complete executes a bounded non-streaming request. Existing durable results
// are replayed; in-flight duplicates fail closed to avoid a second call.
func (g *Gateway) Complete(ctx context.Context, request contracts.ModelRequest, fallbacks ...contracts.ProviderRef) (contracts.ModelResponse, error) {
	request = cloneModelRequest(request)
	if err := request.Normalize(); err != nil {
		return contracts.ModelResponse{}, fmt.Errorf("normalize model request: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(request.Budget.TimeoutMS)*time.Millisecond)
	defer cancel()
	normalized, start, err := g.begin(ctx, request)
	if err != nil {
		return contracts.ModelResponse{}, err
	}
	if start.Existing {
		return replayExistingCall(start.Record)
	}

	refs := providerRefs(normalized.Provider, fallbacks)
	var lastErr error
	attemptCount := 0
	for _, ref := range refs {
		provider, ok := g.Registry.Lookup(ref.Provider)
		if !ok {
			lastErr = newFailure(ref.Provider, contracts.ModelFailureProvider, "provider is not registered", 0, false)
			continue
		}
		routed := normalized
		routed.Provider = ref
		for attempt := 1; attempt <= normalized.RetryPolicy.MaxAttempts; attempt++ {
			attemptCount++
			if err := g.recordAttempt(ctx, normalized, attempt); err != nil {
				return g.finishError(ctx, normalized, attemptCount, err, nil, false)
			}
			response, callErr := provider.Complete(ctx, routed)
			if callErr == nil {
				response.RequestID = normalized.RequestID
				if validationErr := validateResponse(normalized, &response); validationErr != nil {
					callErr = validationErr
				} else {
					if err := g.finishSuccess(ctx, normalized, attemptCount, response); err != nil {
						return contracts.ModelResponse{}, err
					}
					return response, nil
				}
			}
			failure, evidence, _ := normalizeFailure(callErr, ref.Provider, false)
			lastErr = &FailureError{Failure: failure, Evidence: evidence}
			if !failure.Retryable || !normalized.RetryPolicy.Allows(failure.Code) || attempt == normalized.RetryPolicy.MaxAttempts {
				break
			}
			if err := g.waitRetry(ctx, normalized.RetryPolicy, attempt); err != nil {
				failure, evidence, _ = normalizeFailure(err, ref.Provider, false)
				lastErr = &FailureError{Failure: failure, Evidence: evidence}
				break
			}
		}
		if lastErr != nil {
			failure, _, _ := failureFromError(lastErr)
			if failure.ContentEmitted {
				break
			}
		}
	}
	if lastErr == nil {
		lastErr = newFailure(normalized.Provider.Provider, contracts.ModelFailureProvider, "model provider execution failed", 0, false)
	}
	return g.finishError(ctx, normalized, attemptCount, lastErr, failureEvidence(lastErr), false)
}

// Stream executes a provider stream. Non-content events are buffered until
// the first content event so a provider can fail over before any visible
// output. Once content is emitted, fallback and retry are forbidden.
func (g *Gateway) Stream(ctx context.Context, request contracts.ModelRequest, sink StreamSink, fallbacks ...contracts.ProviderRef) (contracts.ModelResponse, error) {
	request = cloneModelRequest(request)
	if err := request.Normalize(); err != nil {
		return contracts.ModelResponse{}, fmt.Errorf("normalize model request: %w", err)
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(request.Budget.TimeoutMS)*time.Millisecond)
	defer cancel()
	normalized, start, err := g.begin(ctx, request)
	if err != nil {
		return contracts.ModelResponse{}, err
	}
	if start.Existing {
		response, replayErr := replayExistingCall(start.Record)
		if replayErr == nil && sink != nil {
			sink(contracts.ModelStreamEvent{Type: contracts.ModelStreamCompletion, Response: &response})
		}
		return response, replayErr
	}
	if sink == nil {
		sink = func(contracts.ModelStreamEvent) {}
	}

	refs := providerRefs(normalized.Provider, fallbacks)
	var lastErr error
	attemptCount := 0
	for _, ref := range refs {
		provider, ok := g.Registry.Lookup(ref.Provider)
		if !ok {
			lastErr = newFailure(ref.Provider, contracts.ModelFailureProvider, "provider is not registered", 0, false)
			continue
		}
		routed := normalized
		routed.Provider = ref
		for attempt := 1; attempt <= normalized.RetryPolicy.MaxAttempts; attempt++ {
			attemptCount++
			if err := g.recordAttempt(ctx, normalized, attempt); err != nil {
				return g.finishError(ctx, normalized, attemptCount, err, nil, false)
			}
			contentEmitted := false
			streamBytes := 0
			streamRunes := 0
			var streamBudgetErr error
			pending := make([]contracts.ModelStreamEvent, 0, 4)
			completedEvent := false
			attemptSink := func(event contracts.ModelStreamEvent) {
				if streamBudgetErr != nil {
					return
				}
				if event.Type == contracts.ModelStreamCompletion {
					completedEvent = true
				}
				if event.HasContent() {
					content := event.Delta
					if content == "" && event.Response != nil {
						content = event.Response.Content
					}
					streamBytes += len(content)
					streamRunes += utf8.RuneCountInString(content)
					if streamBytes > normalized.Budget.MaxOutputBytes || (streamRunes+3)/4 > normalized.Budget.MaxOutputTokens {
						streamBudgetErr = newFailure(ref.Provider, contracts.ModelFailureBudget, "stream output budget exceeded", 0, false)
						return
					}
					contentEmitted = true
					for _, buffered := range pending {
						sink(buffered)
					}
					pending = pending[:0]
					sink(event)
					return
				}
				if !contentEmitted {
					if len(pending) < 256 {
						pending = append(pending, event)
					}
					return
				}
				sink(event)
			}

			response, callErr := provider.Stream(ctx, routed, attemptSink)
			if callErr == nil && streamBudgetErr != nil {
				callErr = streamBudgetErr
			}
			if callErr == nil {
				response.RequestID = normalized.RequestID
				if validationErr := validateResponse(normalized, &response); validationErr != nil {
					callErr = validationErr
				} else {
					for _, buffered := range pending {
						sink(buffered)
					}
					if !completedEvent {
						sink(contracts.ModelStreamEvent{Type: contracts.ModelStreamCompletion, Response: &response})
					}
					if finishErr := g.finishSuccess(ctx, normalized, attemptCount, response); finishErr != nil {
						return contracts.ModelResponse{}, finishErr
					}
					return response, nil
				}
			}
			failure, evidence, _ := normalizeFailure(callErr, ref.Provider, contentEmitted)
			lastErr = &FailureError{Failure: failure, Evidence: evidence}
			if contentEmitted {
				sink(contracts.ModelStreamEvent{Type: contracts.ModelStreamError, Failure: &failure})
				break
			}
			if !failure.Retryable || !normalized.RetryPolicy.Allows(failure.Code) || attempt == normalized.RetryPolicy.MaxAttempts {
				break
			}
			if err := g.waitRetry(ctx, normalized.RetryPolicy, attempt); err != nil {
				failure, evidence, _ = normalizeFailure(err, ref.Provider, false)
				lastErr = &FailureError{Failure: failure, Evidence: evidence}
				break
			}
		}
		if lastErr != nil {
			failure, _, _ := failureFromError(lastErr)
			if failure.ContentEmitted {
				break
			}
		}
	}
	if lastErr == nil {
		lastErr = newFailure(normalized.Provider.Provider, contracts.ModelFailureProvider, "model provider execution failed", 0, false)
	}
	return g.finishError(ctx, normalized, attemptCount, lastErr, failureEvidence(lastErr), false)
}

func (g *Gateway) begin(ctx context.Context, request contracts.ModelRequest) (contracts.ModelRequest, CallStart, error) {
	if g == nil || g.Registry == nil {
		return contracts.ModelRequest{}, CallStart{}, fmt.Errorf("model gateway is not configured")
	}
	request = cloneModelRequest(request)
	if err := request.Normalize(); err != nil {
		return contracts.ModelRequest{}, CallStart{}, fmt.Errorf("normalize model request: %w", err)
	}
	if err := validateRequestBudget(request); err != nil {
		return contracts.ModelRequest{}, CallStart{}, err
	}
	if _, ok := g.Registry.Lookup(request.Provider.Provider); !ok {
		return contracts.ModelRequest{}, CallStart{}, fmt.Errorf("%w: %s", ErrProviderNotFound, request.Provider.Provider)
	}
	if g.Recorder == nil {
		return request, CallStart{}, nil
	}
	evidence, err := RedactJSON(request)
	if err != nil {
		return contracts.ModelRequest{}, CallStart{}, fmt.Errorf("redact model request: %w", err)
	}
	start, err := g.Recorder.Start(ctx, request, evidence)
	if err != nil {
		return contracts.ModelRequest{}, CallStart{}, err
	}
	if start.Existing {
		switch start.Record.Status {
		case contracts.ModelCallSucceeded, contracts.ModelCallFailed:
		default:
			return contracts.ModelRequest{}, CallStart{}, ErrModelCallInFlight
		}
	}
	return request, start, nil
}

func (g *Gateway) recordAttempt(ctx context.Context, request contracts.ModelRequest, attempt int) error {
	if g.Recorder == nil {
		return nil
	}
	return g.Recorder.Attempt(ctx, request.WorkspaceID, request.RequestID)
}

func (g *Gateway) finishSuccess(ctx context.Context, request contracts.ModelRequest, attempts int, response contracts.ModelResponse) error {
	if g.Recorder == nil {
		return nil
	}
	evidence, err := responseEvidence(response)
	if err != nil {
		return err
	}
	return g.Recorder.Finish(ctx, contracts.ModelCallResult{
		WorkspaceID:       request.WorkspaceID,
		RequestID:         request.RequestID,
		Status:            contracts.ModelCallSucceeded,
		AttemptCount:      attempts,
		ContentEmitted:    response.Content != "",
		ProviderRequestID: response.ProviderRequestID,
		Usage:             response.Usage,
		Cost:              response.Cost,
		Response:          &response,
		ResponseEvidence:  evidence,
	})
}

func (g *Gateway) finishError(ctx context.Context, request contracts.ModelRequest, attempts int, err error, evidence []byte, contentEmitted bool) (contracts.ModelResponse, error) {
	failure, failureEvidence, ok := normalizeFailure(err, request.Provider.Provider, contentEmitted)
	if !ok {
		failureEvidence = evidence
	}
	if len(failureEvidence) == 0 {
		failureEvidence = evidence
	}
	if g.Recorder != nil {
		finishErr := g.Recorder.Finish(ctx, contracts.ModelCallResult{
			WorkspaceID:       request.WorkspaceID,
			RequestID:         request.RequestID,
			Status:            contracts.ModelCallFailed,
			AttemptCount:      attempts,
			ContentEmitted:    failure.ContentEmitted,
			ProviderRequestID: failure.ProviderRequestID,
			Failure:           &failure,
			ResponseEvidence:  failureEvidence,
		})
		if finishErr != nil {
			return contracts.ModelResponse{}, fmt.Errorf("record model call failure: %w; original=%v", finishErr, err)
		}
	}
	return contracts.ModelResponse{}, &FailureError{Failure: failure, Evidence: failureEvidence}
}

func (g *Gateway) waitRetry(ctx context.Context, policy contracts.RetryPolicy, attempt int) error {
	delay := time.Duration(policy.InitialDelayMS) * time.Millisecond
	for i := 1; i < attempt; i++ {
		if delay >= time.Duration(policy.MaxDelayMS)*time.Millisecond {
			delay = time.Duration(policy.MaxDelayMS) * time.Millisecond
			break
		}
		delay *= 2
	}
	if delay > time.Duration(policy.MaxDelayMS)*time.Millisecond {
		delay = time.Duration(policy.MaxDelayMS) * time.Millisecond
	}
	if delay == 0 {
		return nil
	}
	if g.Sleep == nil {
		return sleepContext(ctx, delay)
	}
	return g.Sleep(ctx, delay)
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func providerRefs(primary contracts.ProviderRef, fallbacks []contracts.ProviderRef) []contracts.ProviderRef {
	refs := make([]contracts.ProviderRef, 0, 1+len(fallbacks))
	seen := make(map[string]struct{})
	for _, ref := range append([]contracts.ProviderRef{primary}, fallbacks...) {
		ref.Provider = strings.ToLower(strings.TrimSpace(ref.Provider))
		ref.Endpoint = strings.TrimSpace(ref.Endpoint)
		ref.Model = strings.TrimSpace(ref.Model)
		key := ref.Provider + "\x00" + ref.Endpoint + "\x00" + ref.Model
		if ref.Provider == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		refs = append(refs, ref)
	}
	return refs
}

func cloneModelRequest(request contracts.ModelRequest) contracts.ModelRequest {
	clone := request
	clone.Messages = append([]contracts.ModelMessage(nil), request.Messages...)
	for i := range clone.Messages {
		clone.Messages[i].ToolCalls = append([]contracts.ModelToolCall(nil), request.Messages[i].ToolCalls...)
		for j := range clone.Messages[i].ToolCalls {
			clone.Messages[i].ToolCalls[j].Arguments = append([]byte(nil), request.Messages[i].ToolCalls[j].Arguments...)
		}
	}
	clone.Tools = append([]contracts.ModelToolDefinition(nil), request.Tools...)
	for i := range clone.Tools {
		clone.Tools[i].Parameters = append([]byte(nil), request.Tools[i].Parameters...)
	}
	if request.Metadata != nil {
		clone.Metadata = make(map[string]string, len(request.Metadata))
		for key, value := range request.Metadata {
			clone.Metadata[key] = value
		}
	}
	clone.Task = cloneEntityRef(request.Task)
	clone.Session = cloneEntityRef(request.Session)
	return clone
}

func cloneEntityRef(ref *contracts.EntityRef) *contracts.EntityRef {
	if ref == nil {
		return nil
	}
	clone := *ref
	return &clone
}

func validateRequestBudget(request contracts.ModelRequest) error {
	if request.EstimatedInputBytes() > request.Budget.MaxInputBytes {
		return newFailure(request.Provider.Provider, contracts.ModelFailureBudget, "input byte budget exceeded", 0, false)
	}
	if request.Budget.MaxInputTokens > 0 && request.EstimatedInputTokens() > request.Budget.MaxInputTokens {
		return newFailure(request.Provider.Provider, contracts.ModelFailureBudget, "input token budget exceeded", 0, false)
	}
	return nil
}

func validateResponse(request contracts.ModelRequest, response *contracts.ModelResponse) error {
	if err := response.Normalize(); err != nil {
		return newFailure(request.Provider.Provider, contracts.ModelFailureEmptyResponse, err.Error(), 0, false)
	}
	if response.Usage.InputTokens == 0 {
		response.Usage.InputTokens = request.EstimatedInputTokens()
	}
	if response.Usage.OutputTokens == 0 && response.Content != "" {
		response.Usage.OutputTokens = contracts.EstimateModelTokens(response.Content)
	}
	if response.Usage.TotalTokens < response.Usage.InputTokens+response.Usage.OutputTokens {
		response.Usage.TotalTokens = response.Usage.InputTokens + response.Usage.OutputTokens
	}
	if len(response.Content) > request.Budget.MaxOutputBytes {
		return newFailure(request.Provider.Provider, contracts.ModelFailureBudget, "output byte budget exceeded", 0, false)
	}
	if response.Usage.OutputTokens > request.Budget.MaxOutputTokens {
		return newFailure(request.Provider.Provider, contracts.ModelFailureBudget, "output token budget exceeded", 0, false)
	}
	if request.Budget.MaxTotalTokens > 0 && response.Usage.TotalTokens > request.Budget.MaxTotalTokens {
		return newFailure(request.Provider.Provider, contracts.ModelFailureBudget, "total token budget exceeded", 0, false)
	}
	if request.Budget.MaxCostUSD > 0 && response.Cost.TotalCostUSD > request.Budget.MaxCostUSD {
		return newFailure(request.Provider.Provider, contracts.ModelFailureBudget, "cost budget exceeded", 0, false)
	}
	return nil
}

func normalizeFailure(err error, provider string, contentEmitted bool) (contracts.ModelFailure, []byte, bool) {
	if err == nil {
		return contracts.ModelFailure{}, nil, false
	}
	if failure, evidence, ok := failureFromError(err); ok {
		failure.Provider = strings.TrimSpace(failure.Provider)
		if failure.Provider == "" {
			failure.Provider = provider
		}
		failure.ContentEmitted = failure.ContentEmitted || contentEmitted
		return failure, RedactBytes(evidence), true
	}
	code := contracts.ModelFailureProvider
	retryable := false
	if errors.Is(err, context.Canceled) {
		code = contracts.ModelFailureCancelled
	} else if errors.Is(err, context.DeadlineExceeded) {
		code = contracts.ModelFailureTimeout
		retryable = true
	} else {
		var networkErr net.Error
		if errors.As(err, &networkErr) && networkErr.Timeout() {
			code = contracts.ModelFailureTimeout
			retryable = true
		}
	}
	failure := contracts.ModelFailure{Code: code, Message: redactText(err.Error()), Provider: provider, Retryable: retryable, ContentEmitted: contentEmitted}
	return failure, nil, true
}

func newFailure(provider, code, message string, status int, retryable bool) error {
	return &FailureError{Failure: contracts.ModelFailure{Code: code, Message: redactText(message), Provider: provider, StatusCode: status, Retryable: retryable}}
}

func failureEvidence(err error) []byte {
	if failure, evidence, ok := failureFromError(err); ok {
		_ = failure
		return RedactBytes(evidence)
	}
	return nil
}

func replayExistingCall(record contracts.ModelCallRecord) (contracts.ModelResponse, error) {
	if record.Status == contracts.ModelCallSucceeded && record.Response != nil {
		return *record.Response, nil
	}
	if record.Status == contracts.ModelCallFailed && record.Failure != nil {
		return contracts.ModelResponse{}, &FailureError{Failure: *record.Failure, Evidence: record.ResponseEvidence}
	}
	return contracts.ModelResponse{}, ErrModelCallInFlight
}

func responseEvidence(response contracts.ModelResponse) ([]byte, error) {
	if len(response.WirePayload) > 0 {
		return RedactBytes(response.WirePayload), nil
	}
	b, err := json.Marshal(response)
	if err != nil {
		return nil, fmt.Errorf("marshal model response evidence: %w", err)
	}
	return RedactBytes(b), nil
}
