// Package model provides the provider-neutral gateway for bounded model and
// embedding calls. It keeps credentials at the provider boundary and records
// durable at-least-once execution through the model-call ledger.
package model

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrProviderNotFound   = errors.New("model provider not found")
	ErrProviderDuplicate  = errors.New("model provider already registered")
	ErrModelCallInFlight  = errors.New("model call already in progress")
	ErrModelCallCompleted = errors.New("model call already completed")
)

// StreamSink receives provider-neutral events. Providers must not put secrets
// or unbounded wire payloads in events.
type StreamSink func(contracts.ModelStreamEvent)

// EmbeddingRequest is the existing deterministic retrieval embedding seam
// expressed as a provider capability.
type EmbeddingRequest struct {
	Model         string
	Text          string
	MaxInputBytes int
	Timeout       time.Duration
}

// Provider is the explicit model capability seam. Embed may be unsupported by
// chat-only providers; Complete/Stream may be unsupported by embedding-only
// providers.
type Provider interface {
	Name() string
	Aliases() []string
	Endpoint() contracts.ModelEndpoint
	Complete(context.Context, contracts.ModelRequest) (contracts.ModelResponse, error)
	Stream(context.Context, contracts.ModelRequest, StreamSink) (contracts.ModelResponse, error)
	Embed(context.Context, EmbeddingRequest) ([]float32, error)
}

// Registry is an explicit provider registry. Registration is all-or-nothing;
// aliases cannot shadow another provider and List is stable-sorted.
type Registry struct {
	mu        sync.RWMutex
	providers map[string]Provider
}

// NewRegistry creates an empty deterministic provider registry.
func NewRegistry() *Registry {
	return &Registry{providers: make(map[string]Provider)}
}

// Register adds a provider and all of its aliases atomically. Collisions are
// rejected rather than resolved by registration order.
func (r *Registry) Register(provider Provider) error {
	if r == nil {
		return fmt.Errorf("%w: registry is nil", ErrProviderNotFound)
	}
	if provider == nil {
		return fmt.Errorf("provider is nil")
	}
	keys := providerKeys(provider)
	if len(keys) == 0 {
		return fmt.Errorf("provider name is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, key := range keys {
		if _, exists := r.providers[key]; exists {
			return fmt.Errorf("%w: %s", ErrProviderDuplicate, key)
		}
	}
	for _, key := range keys {
		r.providers[key] = provider
	}
	return nil
}

// Lookup returns the provider registered under a case-insensitive name or
// alias.
func (r *Registry) Lookup(name string) (Provider, bool) {
	if r == nil {
		return nil, false
	}
	key := strings.ToLower(strings.TrimSpace(name))
	if key == "" {
		return nil, false
	}
	r.mu.RLock()
	provider, ok := r.providers[key]
	r.mu.RUnlock()
	return provider, ok
}

// Names returns canonical provider names in stable order.
func (r *Registry) Names() []string {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.providers))
	for name, provider := range r.providers {
		if strings.EqualFold(name, provider.Name()) {
			names = append(names, name)
		}
	}
	sortStrings(names)
	return names
}

func providerKeys(provider Provider) []string {
	seen := make(map[string]struct{})
	keys := make([]string, 0, 1+len(provider.Aliases()))
	for _, raw := range append([]string{provider.Name()}, provider.Aliases()...) {
		key := strings.ToLower(strings.TrimSpace(raw))
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		keys = append(keys, key)
	}
	return keys
}

func sortStrings(values []string) {
	for i := 1; i < len(values); i++ {
		for j := i; j > 0 && values[j] < values[j-1]; j-- {
			values[j], values[j-1] = values[j-1], values[j]
		}
	}
}

// CallStart is returned by a durable call recorder before external execution.
// Existing completed calls are replayable; existing in-flight calls fail
// closed so duplicate delivery cannot issue a second provider request.
type CallStart struct {
	Record   contracts.ModelCallRecord
	Existing bool
}

// CallRecorder is implemented by the Postgres model-call ledger. Keeping this
// interface in the runtime package avoids coupling providers to SQL.
type CallRecorder interface {
	Start(context.Context, contracts.ModelRequest, []byte) (CallStart, error)
	Attempt(context.Context, string, string) error
	Finish(context.Context, contracts.ModelCallResult) error
}

// FailureError carries stable provider-neutral failure facts and optional
// already-redacted wire evidence.
type FailureError struct {
	Failure  contracts.ModelFailure
	Evidence []byte
}

// Error returns a redacted provider failure summary. Evidence is kept
// separately and is never interpolated into this message.
func (e *FailureError) Error() string {
	if e == nil {
		return "model provider failure"
	}
	return fmt.Sprintf("model provider failure code=%s provider=%s: %s", e.Failure.Code, e.Failure.Provider, e.Failure.Message)
}

func failureFromError(err error) (contracts.ModelFailure, []byte, bool) {
	var failureErr *FailureError
	if errors.As(err, &failureErr) {
		failure, normalizeErr := failureErr.Failure.Normalize()
		if normalizeErr == nil {
			return failure, failureErr.Evidence, true
		}
	}
	return contracts.ModelFailure{}, nil, false
}

func appendBoundedStreamContent(content *strings.Builder, runes *int, delta string, budget contracts.ModelBudget, provider string, evidence []byte) error {
	maxBytes := budget.MaxOutputBytes
	if maxBytes <= 0 || maxBytes > contracts.MaxModelOutputBytes {
		maxBytes = contracts.MaxModelOutputBytes
	}
	maxTokens := budget.MaxOutputTokens
	if maxTokens <= 0 || maxTokens > contracts.MaxModelOutputTokens {
		maxTokens = contracts.MaxModelOutputTokens
	}
	nextBytes := content.Len() + len(delta)
	nextRunes := *runes + utf8.RuneCountInString(delta)
	if nextBytes > maxBytes || (nextRunes+3)/4 > maxTokens {
		return &FailureError{
			Failure: contracts.ModelFailure{
				Code: contracts.ModelFailureBudget, Message: "stream output budget exceeded",
				Provider: provider, ContentEmitted: content.Len() > 0,
			},
			Evidence: evidence,
		}
	}
	content.WriteString(delta)
	*runes = nextRunes
	return nil
}
