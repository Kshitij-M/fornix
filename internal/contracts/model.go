package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	ModelSchemaVersion       = 1
	DefaultModelTimeout      = 30 * time.Second
	MaxModelTimeout          = 10 * time.Minute
	MaxModelInputTokens      = 1 << 20
	DefaultModelOutputTokens = 8192
	MaxModelOutputTokens     = 8192
	MaxModelTotalTokens      = MaxModelInputTokens + MaxModelOutputTokens
	MaxModelInputBytes       = 4 << 20
	MaxModelOutputBytes      = 4 << 20
	MaxModelEvidenceBytes    = 1 << 20
	MaxModelMetadataEntries  = 64
)

const (
	ModelFailureAuthentication = "authentication"
	ModelFailureQuota          = "quota"
	ModelFailureRateLimit      = "rate_limit"
	ModelFailureContextWindow  = "context_window"
	ModelFailureTransport      = "transport"
	ModelFailureTimeout        = "timeout"
	ModelFailureProvider       = "provider"
	ModelFailureInvalidRequest = "invalid_request"
	ModelFailureEmptyResponse  = "empty_response"
	ModelFailureBudget         = "budget"
	ModelFailureInProgress     = "in_progress"
	ModelFailureCancelled      = "cancelled"
)

const (
	ModelCallPending   = "pending"
	ModelCallRunning   = "running"
	ModelCallSucceeded = "succeeded"
	ModelCallFailed    = "failed"
)

// ProviderRef identifies a provider route without carrying a credential.
// Endpoint is a logical endpoint name or URL selected by configuration; the
// provider owns how it resolves that value.
type ProviderRef struct {
	Provider string `json:"provider"`
	Endpoint string `json:"endpoint,omitempty"`
	Model    string `json:"model,omitempty"`
}

// ModelEndpoint is the non-secret provider configuration used by a gateway.
// CredentialRef is a reference such as an environment variable name, never a
// credential value.
type ModelEndpoint struct {
	ID                 string  `json:"id"`
	Provider           string  `json:"provider"`
	BaseURL            string  `json:"base_url,omitempty"`
	DefaultModel       string  `json:"default_model,omitempty"`
	CredentialRef      string  `json:"credential_ref,omitempty"`
	Enabled            bool    `json:"enabled"`
	AllowPrivate       bool    `json:"allow_private,omitempty"`
	InputCostPer1KUSD  float64 `json:"input_cost_per_1k_usd,omitempty"`
	OutputCostPer1KUSD float64 `json:"output_cost_per_1k_usd,omitempty"`
}

// ModelMessage is the provider-neutral text message vocabulary for this
// slice. Tool blocks are intentionally deferred to the tool/agent-loop slice.
type ModelMessage struct {
	Role       string          `json:"role"`
	Content    string          `json:"content,omitempty"`
	Name       string          `json:"name,omitempty"`
	ToolCallID string          `json:"tool_call_id,omitempty"`
	ToolCalls  []ModelToolCall `json:"tool_calls,omitempty"`
}

// ModelToolDefinition is the provider-neutral capability description sent to
// a model. Parameters are JSON Schema and are treated as untrusted model
// output at the loop boundary.
type ModelToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	Parameters  json.RawMessage `json:"parameters,omitempty"`
}

// ModelToolCall is a structured model request to invoke one registered tool.
// Arguments remain raw JSON until the deterministic tool adapter validates the
// argv envelope; they are never interpreted as shell text.
type ModelToolCall struct {
	ID        string          `json:"id"`
	ToolID    string          `json:"tool_id"`
	Arguments json.RawMessage `json:"arguments,omitempty"`
}

// ModelBudget is an admission and response bound. Zero means the contract
// default for input/output token, output byte, and timeout limits;
// MaxCostUSD zero means no configured monetary ceiling.
type ModelBudget struct {
	MaxInputBytes   int     `json:"max_input_bytes,omitempty"`
	MaxOutputBytes  int     `json:"max_output_bytes,omitempty"`
	MaxInputTokens  int     `json:"max_input_tokens,omitempty"`
	MaxOutputTokens int     `json:"max_output_tokens,omitempty"`
	MaxTotalTokens  int     `json:"max_total_tokens,omitempty"`
	MaxCostUSD      float64 `json:"max_cost_usd,omitempty"`
	TimeoutMS       int     `json:"timeout_ms,omitempty"`
}

// ModelRequest is the immutable, provider-neutral input to one external
// model operation. It deliberately contains no API key or other credential.
type ModelRequest struct {
	SchemaVersion  int                   `json:"schema_version"`
	RequestID      string                `json:"request_id"`
	IdempotencyKey string                `json:"idempotency_key"`
	CausationID    string                `json:"causation_id,omitempty"`
	CorrelationID  string                `json:"correlation_id,omitempty"`
	WorkspaceID    string                `json:"workspace_id"`
	Actor          ActorRef              `json:"actor,omitempty"`
	Task           *EntityRef            `json:"task,omitempty"`
	Session        *EntityRef            `json:"session,omitempty"`
	Provider       ProviderRef           `json:"provider"`
	Messages       []ModelMessage        `json:"messages,omitempty"`
	Tools          []ModelToolDefinition `json:"tools,omitempty"`
	Prompt         string                `json:"prompt,omitempty"`
	Budget         ModelBudget           `json:"budget"`
	RetryPolicy    RetryPolicy           `json:"retry_policy"`
	Metadata       map[string]string     `json:"metadata,omitempty"`
}

// NewModelRequest creates a request with a safe identity. Callers that want
// idempotency across process retries should replace the generated key with a
// stable task/run key before invoking the gateway.
func NewModelRequest(workspaceID, provider, model, prompt string) ModelRequest {
	id := NewID("mdlreq")
	return ModelRequest{
		SchemaVersion:  ModelSchemaVersion,
		RequestID:      id,
		IdempotencyKey: id,
		WorkspaceID:    strings.TrimSpace(workspaceID),
		Provider:       ProviderRef{Provider: strings.TrimSpace(provider), Model: strings.TrimSpace(model)},
		Prompt:         prompt,
		Budget:         ModelBudget{MaxOutputTokens: DefaultModelOutputTokens},
		RetryPolicy:    DefaultRetryPolicy(),
	}
}

// Normalize validates and fills bounded deterministic defaults. It does not
// resolve provider configuration or credentials.
func (r *ModelRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("model request is nil")
	}
	// Normalize is a validation boundary and may be called concurrently for
	// duplicate delivery. Detach all mutable reference fields before applying
	// defaults or canonicalizing nested values.
	r.Messages = cloneModelMessagesForNormalize(r.Messages)
	r.Tools = cloneModelToolDefinitionsForNormalize(r.Tools)
	r.RetryPolicy.RetryableCodes = append([]string(nil), r.RetryPolicy.RetryableCodes...)
	if r.Metadata != nil {
		metadata := make(map[string]string, len(r.Metadata))
		for key, value := range r.Metadata {
			metadata[key] = value
		}
		r.Metadata = metadata
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = ModelSchemaVersion
	}
	if r.SchemaVersion != ModelSchemaVersion {
		return fmt.Errorf("unsupported model schema_version %d", r.SchemaVersion)
	}
	r.RequestID = strings.TrimSpace(r.RequestID)
	if r.RequestID == "" {
		r.RequestID = NewID("mdlreq")
	}
	r.IdempotencyKey = strings.TrimSpace(r.IdempotencyKey)
	if r.IdempotencyKey == "" {
		return fmt.Errorf("idempotency_key is required")
	}
	if len(r.IdempotencyKey) > MaxIdempotencyLength {
		return fmt.Errorf("idempotency_key exceeds %d characters", MaxIdempotencyLength)
	}
	r.CausationID = strings.TrimSpace(r.CausationID)
	r.CorrelationID = strings.TrimSpace(r.CorrelationID)
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	if r.WorkspaceID == "" {
		return fmt.Errorf("workspace_id is required")
	}
	if len(r.WorkspaceID) > 256 {
		return fmt.Errorf("workspace_id is too large")
	}
	if err := normalizeModelProviderRef(&r.Provider); err != nil {
		return err
	}
	if r.Task != nil {
		if err := validateModelEntityRef(r.Task, "task", r.WorkspaceID); err != nil {
			return err
		}
	}
	if r.Session != nil {
		if err := validateModelEntityRef(r.Session, "session", r.WorkspaceID); err != nil {
			return err
		}
	}
	r.Prompt = strings.TrimSpace(r.Prompt)
	if len(r.Messages) == 0 && r.Prompt == "" {
		return fmt.Errorf("prompt or messages are required")
	}
	for i := range r.Messages {
		r.Messages[i].Role = strings.ToLower(strings.TrimSpace(r.Messages[i].Role))
		r.Messages[i].Name = strings.TrimSpace(r.Messages[i].Name)
		if r.Messages[i].Role != "system" && r.Messages[i].Role != "user" && r.Messages[i].Role != "assistant" && r.Messages[i].Role != "tool" {
			return fmt.Errorf("messages[%d].role %q is unsupported", i, r.Messages[i].Role)
		}
		if strings.TrimSpace(r.Messages[i].Content) == "" {
			if len(r.Messages[i].ToolCalls) > 0 && r.Messages[i].Role == "assistant" {
				for j := range r.Messages[i].ToolCalls {
					if err := r.Messages[i].ToolCalls[j].Normalize(); err != nil {
						return fmt.Errorf("messages[%d].tool_calls[%d]: %w", i, j, err)
					}
				}
				continue
			}
			return fmt.Errorf("messages[%d].content is required", i)
		}
		if r.Messages[i].Role == "tool" && r.Messages[i].ToolCallID == "" {
			return fmt.Errorf("messages[%d].tool_call_id is required", i)
		}
	}
	for i := range r.Tools {
		if err := r.Tools[i].Normalize(); err != nil {
			return fmt.Errorf("tools[%d]: %w", i, err)
		}
	}
	if err := r.Budget.Normalize(); err != nil {
		return err
	}
	if err := r.RetryPolicy.Normalize(); err != nil {
		return err
	}
	if len(r.Metadata) > MaxModelMetadataEntries {
		return fmt.Errorf("metadata cannot contain more than %d entries", MaxModelMetadataEntries)
	}
	for key, value := range r.Metadata {
		if strings.TrimSpace(key) == "" || len(key) > 128 || len(value) > 1024 {
			return fmt.Errorf("metadata entry is invalid")
		}
	}
	return nil
}

func cloneModelMessagesForNormalize(messages []ModelMessage) []ModelMessage {
	if messages == nil {
		return nil
	}
	cloned := make([]ModelMessage, len(messages))
	for i, message := range messages {
		cloned[i] = message
		if message.ToolCalls != nil {
			cloned[i].ToolCalls = make([]ModelToolCall, len(message.ToolCalls))
			for j, call := range message.ToolCalls {
				cloned[i].ToolCalls[j] = call
				cloned[i].ToolCalls[j].Arguments = append(json.RawMessage(nil), call.Arguments...)
			}
		}
	}
	return cloned
}

func cloneModelToolDefinitionsForNormalize(tools []ModelToolDefinition) []ModelToolDefinition {
	if tools == nil {
		return nil
	}
	cloned := make([]ModelToolDefinition, len(tools))
	for i, tool := range tools {
		cloned[i] = tool
		cloned[i].Parameters = append(json.RawMessage(nil), tool.Parameters...)
	}
	return cloned
}

// Normalize validates the JSON Schema boundary exposed to a provider.
func (c *ModelToolDefinition) Normalize() error {
	if c == nil {
		return fmt.Errorf("tool definition is nil")
	}
	c.Name = strings.TrimSpace(c.Name)
	c.Description = strings.TrimSpace(c.Description)
	if c.Name == "" || len(c.Name) > 128 {
		return fmt.Errorf("tool name is required and must be at most 128 characters")
	}
	if len(c.Parameters) == 0 {
		c.Parameters = json.RawMessage(`{"type":"object"}`)
	}
	if !json.Valid(c.Parameters) {
		return fmt.Errorf("tool parameters must be valid JSON")
	}
	return nil
}

// Normalize validates a structured model tool call before it can reach the
// tool policy and argv adapter.
func (c *ModelToolCall) Normalize() error {
	if c == nil {
		return fmt.Errorf("tool call is nil")
	}
	c.ID = strings.TrimSpace(c.ID)
	c.ToolID = strings.TrimSpace(c.ToolID)
	if c.ID == "" {
		return fmt.Errorf("tool call id is required")
	}
	if c.ToolID == "" {
		return fmt.Errorf("tool call tool_id is required")
	}
	if len(c.Arguments) == 0 {
		c.Arguments = json.RawMessage(`{}`)
	}
	if !json.Valid(c.Arguments) {
		return fmt.Errorf("tool call arguments must be valid JSON")
	}
	return nil
}

func normalizeModelProviderRef(ref *ProviderRef) error {
	ref.Provider = strings.ToLower(strings.TrimSpace(ref.Provider))
	ref.Endpoint = strings.TrimSpace(ref.Endpoint)
	ref.Model = strings.TrimSpace(ref.Model)
	if ref.Provider == "" {
		return fmt.Errorf("provider.provider is required")
	}
	if len(ref.Provider) > 128 || len(ref.Endpoint) > 512 || len(ref.Model) > 256 {
		return fmt.Errorf("provider reference is too large")
	}
	return nil
}

func validateModelEntityRef(ref *EntityRef, kind, workspaceID string) error {
	ref.ID = strings.TrimSpace(ref.ID)
	ref.Kind = strings.ToLower(strings.TrimSpace(ref.Kind))
	ref.WorkspaceID = strings.TrimSpace(ref.WorkspaceID)
	if ref.ID == "" || ref.Kind != kind {
		return fmt.Errorf("%s reference requires kind %q and id", kind, kind)
	}
	if ref.WorkspaceID != "" && ref.WorkspaceID != workspaceID {
		return fmt.Errorf("%s reference crosses workspace boundary", kind)
	}
	ref.WorkspaceID = workspaceID
	return nil
}

// Normalize fills bounded request limits and rejects impossible budgets.
func (b *ModelBudget) Normalize() error {
	if b.MaxInputBytes == 0 {
		b.MaxInputBytes = MaxModelInputBytes
	}
	if b.MaxOutputBytes == 0 {
		b.MaxOutputBytes = MaxModelOutputBytes
	}
	if b.MaxOutputTokens == 0 {
		b.MaxOutputTokens = DefaultModelOutputTokens
	}
	if b.MaxInputTokens == 0 {
		b.MaxInputTokens = MaxModelInputTokens
	}
	if b.MaxTotalTokens == 0 {
		b.MaxTotalTokens = b.MaxInputTokens + b.MaxOutputTokens
	}
	if b.TimeoutMS == 0 {
		b.TimeoutMS = int(DefaultModelTimeout / time.Millisecond)
	}
	if b.MaxInputBytes < 1 || b.MaxInputBytes > MaxModelInputBytes {
		return fmt.Errorf("max_input_bytes must be between 1 and %d", MaxModelInputBytes)
	}
	if b.MaxOutputBytes < 1 || b.MaxOutputBytes > MaxModelOutputBytes {
		return fmt.Errorf("max_output_bytes must be between 1 and %d", MaxModelOutputBytes)
	}
	if b.MaxInputTokens < 1 || b.MaxInputTokens > MaxModelInputTokens || b.MaxOutputTokens < 1 || b.MaxOutputTokens > MaxModelOutputTokens || b.MaxTotalTokens < 1 || b.MaxTotalTokens > MaxModelTotalTokens {
		return fmt.Errorf("model token budget is invalid")
	}
	if b.MaxTotalTokens > 0 && b.MaxInputTokens > 0 && b.MaxTotalTokens < b.MaxInputTokens {
		return fmt.Errorf("max_total_tokens cannot be below max_input_tokens")
	}
	if b.MaxCostUSD < 0 {
		return fmt.Errorf("max_cost_usd cannot be negative")
	}
	if b.TimeoutMS < 1 || time.Duration(b.TimeoutMS)*time.Millisecond > MaxModelTimeout {
		return fmt.Errorf("timeout_ms must be between 1 and %d", MaxModelTimeout/time.Millisecond)
	}
	return nil
}

// ModelUsage uses disjoint input/output counts. TotalTokens is normalized to
// the sum when a provider omits it.
type ModelUsage struct {
	InputTokens      int    `json:"input_tokens"`
	OutputTokens     int    `json:"output_tokens"`
	TotalTokens      int    `json:"total_tokens"`
	CacheReadTokens  int    `json:"cache_read_tokens,omitempty"`
	CacheWriteTokens int    `json:"cache_write_tokens,omitempty"`
	Source           string `json:"source,omitempty"`
}

// Normalize validates usage and derives total tokens when a provider omits
// them. Source identifies whether the value was measured or estimated.
func (u *ModelUsage) Normalize() error {
	if u.InputTokens < 0 || u.OutputTokens < 0 || u.TotalTokens < 0 || u.CacheReadTokens < 0 || u.CacheWriteTokens < 0 {
		return fmt.Errorf("model usage cannot be negative")
	}
	if u.TotalTokens == 0 {
		u.TotalTokens = u.InputTokens + u.OutputTokens
	}
	if u.TotalTokens < u.InputTokens+u.OutputTokens {
		return fmt.Errorf("total_tokens cannot be below input plus output tokens")
	}
	u.Source = strings.TrimSpace(u.Source)
	return nil
}

// ModelCost is the reconciled monetary observation for one model call. It is
// accounting metadata, not a guarantee that a provider billed exactly this
// amount.
type ModelCost struct {
	Currency      string  `json:"currency"`
	InputCostUSD  float64 `json:"input_cost_usd"`
	OutputCostUSD float64 `json:"output_cost_usd"`
	TotalCostUSD  float64 `json:"total_cost_usd"`
	Source        string  `json:"source,omitempty"`
}

// Normalize applies the currency default and validates non-negative costs.
func (c *ModelCost) Normalize() error {
	if c.Currency == "" {
		c.Currency = "USD"
	}
	c.Currency = strings.ToUpper(strings.TrimSpace(c.Currency))
	if c.InputCostUSD < 0 || c.OutputCostUSD < 0 || c.TotalCostUSD < 0 {
		return fmt.Errorf("model cost cannot be negative")
	}
	if c.TotalCostUSD == 0 {
		c.TotalCostUSD = c.InputCostUSD + c.OutputCostUSD
	}
	c.Source = strings.TrimSpace(c.Source)
	return nil
}

// ModelResponse is the normalized provider result. Raw wire payloads are
// stored separately as redacted evidence by the execution ledger.
type ModelResponse struct {
	RequestID         string          `json:"request_id"`
	Provider          ProviderRef     `json:"provider"`
	Content           string          `json:"content,omitempty"`
	ToolCalls         []ModelToolCall `json:"tool_calls,omitempty"`
	FinishReason      string          `json:"finish_reason"`
	Usage             ModelUsage      `json:"usage"`
	Cost              ModelCost       `json:"cost"`
	ProviderRequestID string          `json:"provider_request_id,omitempty"`
	ContentHash       string          `json:"content_hash,omitempty"`
	// WirePayload is available only inside the provider/runtime boundary and is
	// excluded from normal JSON serialization. The gateway redacts and bounds
	// it before putting it into the model-call evidence ledger.
	WirePayload json.RawMessage `json:"-"`
}

// Normalize validates and hashes a provider-neutral response while leaving
// raw wire data outside normal serialization.
func (r *ModelResponse) Normalize() error {
	if r == nil {
		return fmt.Errorf("model response is nil")
	}
	r.RequestID = strings.TrimSpace(r.RequestID)
	if r.RequestID == "" {
		return fmt.Errorf("response request_id is required")
	}
	if err := normalizeModelProviderRef(&r.Provider); err != nil {
		return err
	}
	r.FinishReason = strings.TrimSpace(r.FinishReason)
	r.ProviderRequestID = strings.TrimSpace(r.ProviderRequestID)
	for i := range r.ToolCalls {
		if err := r.ToolCalls[i].Normalize(); err != nil {
			return fmt.Errorf("tool_calls[%d]: %w", i, err)
		}
	}
	if strings.TrimSpace(r.Content) == "" && len(r.ToolCalls) == 0 && r.FinishReason != "tool_calls" {
		return fmt.Errorf("model response content is empty")
	}
	if err := r.Usage.Normalize(); err != nil {
		return err
	}
	if err := r.Cost.Normalize(); err != nil {
		return err
	}
	canonical, _ := json.Marshal(struct {
		Content   string          `json:"content"`
		Finish    string          `json:"finish_reason"`
		ToolCalls []ModelToolCall `json:"tool_calls,omitempty"`
	}{r.Content, r.FinishReason, r.ToolCalls})
	digest := sha256.Sum256(canonical)
	r.ContentHash = hex.EncodeToString(digest[:])
	return nil
}

const (
	ModelStreamTextDelta  = "text_delta"
	ModelStreamUsage      = "usage"
	ModelStreamCompletion = "completion"
	ModelStreamError      = "error"
)

// ModelStreamEvent is the provider-neutral stream vocabulary. A gateway may
// buffer non-content events while deciding whether a provider fallback is
// still safe.
type ModelStreamEvent struct {
	Type              string         `json:"type"`
	Delta             string         `json:"delta,omitempty"`
	Usage             *ModelUsage    `json:"usage,omitempty"`
	Response          *ModelResponse `json:"response,omitempty"`
	Failure           *ModelFailure  `json:"failure,omitempty"`
	ProviderRequestID string         `json:"provider_request_id,omitempty"`
}

// HasContent reports whether an event has crossed the point after which a
// gateway must not retry through a fallback provider.
func (e ModelStreamEvent) HasContent() bool {
	if e.Delta != "" {
		return true
	}
	return e.Response != nil && (e.Response.Content != "" || e.Response.FinishReason == "tool_calls")
}

// ModelFailure is stable machine-readable failure metadata. Message is
// diagnostic text and must already be redacted by the provider boundary.
type ModelFailure struct {
	Code              string `json:"code"`
	Message           string `json:"message"`
	Provider          string `json:"provider,omitempty"`
	StatusCode        int    `json:"status_code,omitempty"`
	RetryAfterMS      int    `json:"retry_after_ms,omitempty"`
	ProviderRequestID string `json:"provider_request_id,omitempty"`
	Retryable         bool   `json:"retryable"`
	ContentEmitted    bool   `json:"content_emitted"`
}

// Normalize validates the redacted, machine-readable failure classification.
func (f ModelFailure) Normalize() (ModelFailure, error) {
	f.Code = strings.TrimSpace(strings.ToLower(f.Code))
	f.Message = strings.TrimSpace(f.Message)
	f.Provider = strings.TrimSpace(f.Provider)
	f.ProviderRequestID = strings.TrimSpace(f.ProviderRequestID)
	if f.Code == "" || f.Message == "" {
		return ModelFailure{}, fmt.Errorf("model failure code and message are required")
	}
	if f.StatusCode < 0 || f.RetryAfterMS < 0 {
		return ModelFailure{}, fmt.Errorf("model failure status and retry delay cannot be negative")
	}
	return f, nil
}

// RetryPolicy is intentionally deterministic. A future replay-aware jitter
// policy can be added without changing the provider contracts.
type RetryPolicy struct {
	MaxAttempts    int      `json:"max_attempts"`
	InitialDelayMS int      `json:"initial_delay_ms"`
	MaxDelayMS     int      `json:"max_delay_ms"`
	RetryableCodes []string `json:"retryable_codes"`
}

// DefaultRetryPolicy returns bounded retryable failure classes with no jitter,
// so retry schedules are reproducible during replay.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxAttempts:    3,
		InitialDelayMS: 100,
		MaxDelayMS:     10_000,
		RetryableCodes: []string{ModelFailureRateLimit, ModelFailureTimeout, ModelFailureTransport, ModelFailureProvider},
	}
}

// Normalize fills the default retry policy and canonicalizes its failure codes.
func (p *RetryPolicy) Normalize() error {
	if p.MaxAttempts == 0 && p.InitialDelayMS == 0 && p.MaxDelayMS == 0 && len(p.RetryableCodes) == 0 {
		*p = DefaultRetryPolicy()
	}
	if p.MaxAttempts < 1 || p.MaxAttempts > 5 || p.InitialDelayMS < 0 || p.MaxDelayMS < p.InitialDelayMS || p.MaxDelayMS > 60_000 {
		return fmt.Errorf("retry policy is invalid")
	}
	seen := make(map[string]struct{}, len(p.RetryableCodes))
	for i, code := range p.RetryableCodes {
		code = strings.TrimSpace(strings.ToLower(code))
		if code == "" {
			return fmt.Errorf("retryable_codes[%d] is empty", i)
		}
		if _, ok := seen[code]; ok {
			continue
		}
		seen[code] = struct{}{}
		p.RetryableCodes[i] = code
	}
	return nil
}

// Allows reports whether a failure class may be retried by this policy.
func (p RetryPolicy) Allows(code string) bool {
	code = strings.TrimSpace(strings.ToLower(code))
	for _, candidate := range p.RetryableCodes {
		if candidate == code {
			return true
		}
	}
	return false
}

// ModelCallRecord is the durable execution ledger representation. Request and
// response evidence contain only redacted bounded JSON.
type ModelCallRecord struct {
	ID                int64             `json:"id"`
	WorkspaceID       string            `json:"workspace_id"`
	RequestID         string            `json:"request_id"`
	IdempotencyKey    string            `json:"idempotency_key"`
	RequestHash       string            `json:"request_hash"`
	SchemaVersion     int               `json:"schema_version"`
	CausationID       string            `json:"causation_id,omitempty"`
	CorrelationID     string            `json:"correlation_id,omitempty"`
	Provider          ProviderRef       `json:"provider"`
	Metadata          map[string]string `json:"metadata,omitempty"`
	Actor             ActorRef          `json:"actor,omitempty"`
	Task              *EntityRef        `json:"task,omitempty"`
	Session           *EntityRef        `json:"session,omitempty"`
	Status            string            `json:"status"`
	AttemptCount      int               `json:"attempt_count"`
	ContentEmitted    bool              `json:"content_emitted"`
	ProviderRequestID string            `json:"provider_request_id,omitempty"`
	Usage             ModelUsage        `json:"usage"`
	Cost              ModelCost         `json:"cost"`
	Failure           *ModelFailure     `json:"failure,omitempty"`
	Response          *ModelResponse    `json:"response,omitempty"`
	RequestEvidence   json.RawMessage   `json:"request_evidence,omitempty"`
	ResponseEvidence  json.RawMessage   `json:"response_evidence,omitempty"`
	ResponseArtifact  *ArtifactRef      `json:"response_artifact,omitempty"`
	CreatedAt         time.Time         `json:"created_at"`
	StartedAt         *time.Time        `json:"started_at,omitempty"`
	FinishedAt        *time.Time        `json:"finished_at,omitempty"`
	DurationMS        int64             `json:"duration_ms"`
}

// ModelCallResult is the store-neutral terminal update passed to the model
// call recorder. External provider execution remains at-least-once.
type ModelCallResult struct {
	WorkspaceID       string
	RequestID         string
	Status            string
	AttemptCount      int
	ContentEmitted    bool
	ProviderRequestID string
	Usage             ModelUsage
	Cost              ModelCost
	Failure           *ModelFailure
	Response          *ModelResponse
	ResponseEvidence  json.RawMessage
}

// EstimateModelTokens provides a conservative, provider-independent token
// estimate for admission and cost budgeting.
func EstimateModelTokens(text string) int {
	if text == "" {
		return 0
	}
	return (utf8.RuneCountInString(text) + 3) / 4
}

// EstimatedInputBytes returns the bounded input-size estimate used before a
// provider call is admitted.
func (r ModelRequest) EstimatedInputBytes() int {
	value := r.Prompt
	for _, message := range r.Messages {
		value += message.Role + message.Name + message.Content
	}
	return len(value)
}

// EstimatedInputTokens returns the provider-independent input token estimate.
func (r ModelRequest) EstimatedInputTokens() int {
	value := r.Prompt
	for _, message := range r.Messages {
		value += message.Role + message.Name + message.Content
	}
	return EstimateModelTokens(value)
}

// RequestHash is stable across generated request/correlation identities while
// retaining the provider, prompt, messages, and all execution budgets.
func (r ModelRequest) RequestHash() (string, error) {
	clone := r
	if err := clone.Normalize(); err != nil {
		return "", err
	}
	clone.RequestID = ""
	clone.IdempotencyKey = ""
	clone.CausationID = ""
	clone.CorrelationID = ""
	b, err := json.Marshal(clone)
	if err != nil {
		return "", fmt.Errorf("hash model request: %w", err)
	}
	digest := sha256.Sum256(b)
	return hex.EncodeToString(digest[:]), nil
}
