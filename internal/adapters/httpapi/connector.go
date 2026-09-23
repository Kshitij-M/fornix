// Package httpapi provides a deliberately narrow HTTP connector. It builds
// URLs only from a configured binding and a typed relative-path payload; it
// is not an arbitrary URL fetcher.
package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	pathpkg "path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/credentials"
)

const (
	ConnectorName        = "httpapi"
	ConnectorVersion     = "1"
	ReadCapabilityName   = "read"
	ListCapabilityName   = "list"
	SubmitCapabilityName = "submit_idempotent"
	ResourceKind         = "http_endpoint"
	InputType            = "http.request"
	DefaultMaxRequest    = 1 << 20
	DefaultMaxResponse   = 4 << 20
	MaxMaxHTTPBytes      = 16 << 20
	DefaultTimeout       = 30 * time.Second
	MaxTimeout           = 10 * time.Minute
	MaxPathLength        = 2048
	MaxQueryEntries      = 64
	MaxHeaderEntries     = 16
)

var (
	ErrUnsafeURL        = errors.New("http target is not allowed by connector binding")
	ErrResponseTooLarge = errors.New("http response exceeds connector budget")
)

// Binding is the non-secret runtime configuration for one workspace HTTP
// target. It can be serialized into contracts.ConnectorBinding.Configuration.
type Binding struct {
	ID                          string   `json:"id"`
	WorkspaceID                 string   `json:"workspace_id"`
	BaseURL                     string   `json:"base_url"`
	AllowedHosts                []string `json:"allowed_hosts"`
	AllowedPathPrefixes         []string `json:"allowed_path_prefixes"`
	CredentialRef               string   `json:"credential_ref,omitempty"`
	MaxRequestBytes             int64    `json:"max_request_bytes"`
	MaxResponseBytes            int64    `json:"max_response_bytes"`
	MaxPages                    int      `json:"max_pages"`
	TimeoutMS                   int64    `json:"timeout_ms"`
	AllowPrivateNetworks        bool     `json:"allow_private_networks"`
	AllowRedirects              bool     `json:"allow_redirects"`
	ProviderSupportsIdempotency bool     `json:"provider_supports_idempotency"`
	VerificationRequired        bool     `json:"verification_required"`
}

// Payload is the typed, bounded HTTP request envelope resolved by InputHash.
// Credentials and absolute URLs are intentionally not representable.
type Payload struct {
	SchemaVersion  int               `json:"schema_version"`
	Method         string            `json:"method"`
	Path           string            `json:"path"`
	Query          map[string]string `json:"query,omitempty"`
	Headers        map[string]string `json:"headers,omitempty"`
	Body           json.RawMessage   `json:"body,omitempty"`
	PageSize       int               `json:"page_size,omitempty"`
	PageToken      string            `json:"page_token,omitempty"`
	ExpectedStatus []int             `json:"expected_status,omitempty"`
}

// CredentialResolver supplies a secret only for the immediate outbound
// request. The connector never returns or persists the value.
type CredentialResolver func(context.Context, string, string) (credentials.Secret, error)

// Connector implements the shared typed adapter boundary for three explicit
// capabilities: read, list, and idempotent submit.
type Connector struct {
	binding            Binding
	definitionRef      contracts.ConnectorRef
	resolver           connector.PayloadResolver
	credentialResolver CredentialResolver
	client             *http.Client
	definitions        map[string]contracts.CapabilityDefinition
}

func NewConnector(binding Binding, resolver connector.PayloadResolver, credentialResolver CredentialResolver, client *http.Client) (*Connector, error) {
	if err := binding.Normalize(); err != nil {
		return nil, err
	}
	if resolver == nil {
		return nil, fmt.Errorf("http payload resolver is required")
	}
	if binding.CredentialRef != "" && credentialResolver == nil {
		return nil, fmt.Errorf("http credential resolver is required for configured credential reference")
	}
	ref := contracts.ConnectorRef{WorkspaceID: binding.WorkspaceID, Name: ConnectorName, Version: ConnectorVersion}
	if err := ref.Normalize(); err != nil {
		return nil, err
	}
	transportClient, err := configuredClient(binding, client)
	if err != nil {
		return nil, err
	}
	credentialRefs := []string(nil)
	if binding.CredentialRef != "" {
		credentialRefs = []string{binding.CredentialRef}
	}
	definitions := make(map[string]contracts.CapabilityDefinition, 3)
	for _, spec := range []struct {
		name    string
		effect  contracts.EffectClass
		input   string
		output  string
		approve bool
	}{
		{name: ReadCapabilityName, effect: contracts.EffectClassReadOnly, input: "http.read.input.v1", output: "http.read.output.v1"},
		{name: ListCapabilityName, effect: contracts.EffectClassReadOnly, input: "http.list.input.v1", output: "http.list.output.v1"},
		{name: SubmitCapabilityName, effect: contracts.EffectClassExternalCommunication, input: "http.submit.input.v1", output: "http.submit.output.v1", approve: true},
	} {
		definition := contracts.CapabilityDefinition{
			WorkspaceID:        binding.WorkspaceID,
			Ref:                contracts.CapabilityRef{WorkspaceID: binding.WorkspaceID, Connector: ref, Name: spec.name, Version: "1"},
			Description:        "bounded HTTP API capability",
			InputSchemaVersion: 1, InputSchemaHash: schemaHash(spec.input), OutputSchemaVersion: 1, OutputSchemaHash: schemaHash(spec.output),
			Effect: spec.effect, Profile: httpProfile(spec.effect == contracts.EffectClassExternalCommunication),
			Evidence:      []contracts.EvidenceRequirement{{WorkspaceID: binding.WorkspaceID, Kind: "http_response", MinItems: 1, MaxItems: 1, RequireHash: true, RequireProvenance: true}},
			ResourceKinds: []string{ResourceKind}, RetryPolicy: contracts.CapabilityRetryPolicy{MaxAttempts: 2, BackoffMS: 10, MaxBackoffMS: 100, Jitter: "none", RetryableCodes: []string{"transport", "timeout", "rate_limit"}},
			MaxRows: 1000, RateLimitPerMinute: 60, RequiredCredentialRefs: credentialRefs, RequiresApproval: spec.approve,
			SupportsCancellation: true, SupportsIdempotency: true, SupportsVerification: true, Enabled: true,
		}
		if err := definition.Normalize(); err != nil {
			return nil, fmt.Errorf("http capability %s: %w", spec.name, err)
		}
		definitions[spec.name] = definition
	}
	return &Connector{binding: binding, definitionRef: ref, resolver: resolver, credentialResolver: credentialResolver, client: transportClient, definitions: definitions}, nil
}

func (b *Binding) Normalize() error {
	if b == nil {
		return fmt.Errorf("http binding is nil")
	}
	if b.WorkspaceID == "" || b.ID == "" {
		return fmt.Errorf("http binding workspace and id are required")
	}
	parsed, err := url.Parse(b.BaseURL)
	if err != nil || parsed.Scheme != "http" && parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("http binding base_url must be an absolute URL without credentials, query, or fragment")
	}
	if parsed.Path == "" {
		parsed.Path = "/"
	}
	cleanBase := pathpkg.Clean(parsed.Path)
	if !strings.HasPrefix(cleanBase, "/") || strings.Contains(cleanBase, "..") {
		return fmt.Errorf("http binding base_url path is invalid")
	}
	parsed.Path = strings.TrimSuffix(cleanBase, "/")
	b.BaseURL = strings.TrimSuffix(parsed.String(), "/")
	if len(b.BaseURL) > MaxPathLength {
		return fmt.Errorf("http binding base_url is too large")
	}
	if len(b.AllowedHosts) == 0 {
		b.AllowedHosts = []string{strings.ToLower(parsed.Host)}
	}
	b.AllowedHosts = uniqueSorted(b.AllowedHosts)
	for _, host := range b.AllowedHosts {
		if host == "" || strings.ContainsAny(host, "/\\@\r\n") {
			return fmt.Errorf("http binding host allowlist is invalid")
		}
	}
	if len(b.AllowedPathPrefixes) == 0 {
		b.AllowedPathPrefixes = []string{"/"}
	}
	b.AllowedPathPrefixes = uniqueSorted(b.AllowedPathPrefixes)
	for index, prefix := range b.AllowedPathPrefixes {
		if err := validateRelativePath(prefix); err != nil {
			return fmt.Errorf("http binding path prefix %d: %w", index, err)
		}
	}
	if b.MaxRequestBytes == 0 {
		b.MaxRequestBytes = DefaultMaxRequest
	}
	if b.MaxResponseBytes == 0 {
		b.MaxResponseBytes = DefaultMaxResponse
	}
	if b.TimeoutMS == 0 {
		b.TimeoutMS = DefaultTimeout.Milliseconds()
	}
	if b.MaxPages == 0 {
		b.MaxPages = 1
	}
	if b.MaxRequestBytes < 1 || b.MaxRequestBytes > MaxMaxHTTPBytes || b.MaxResponseBytes < 1 || b.MaxResponseBytes > MaxMaxHTTPBytes || b.MaxPages < 1 || b.MaxPages > 10 || b.TimeoutMS < 1 || time.Duration(b.TimeoutMS)*time.Millisecond > MaxTimeout {
		return fmt.Errorf("http binding budgets are outside bounds")
	}
	if b.CredentialRef != "" {
		if _, err := credentials.ParseRef(b.CredentialRef); err != nil {
			return fmt.Errorf("http binding credential reference is invalid")
		}
	}
	return nil
}

func (b Binding) Contract(actor contracts.ActorRef) (contracts.ConnectorBinding, error) {
	if err := b.Normalize(); err != nil {
		return contracts.ConnectorBinding{}, err
	}
	configuration, err := json.Marshal(b)
	if err != nil {
		return contracts.ConnectorBinding{}, err
	}
	refs := []string(nil)
	if b.CredentialRef != "" {
		refs = []string{b.CredentialRef}
	}
	value := contracts.ConnectorBinding{ID: b.ID, WorkspaceID: b.WorkspaceID, Connector: contracts.ConnectorRef{WorkspaceID: b.WorkspaceID, Name: ConnectorName, Version: ConnectorVersion}, Kind: contracts.ConnectorBindingHTTPAPI, Version: 1, Configuration: configuration, CredentialRefs: refs, Status: contracts.ConnectorBindingActive, CreatedBy: actor}
	if err := value.Normalize(); err != nil {
		return contracts.ConnectorBinding{}, err
	}
	return value, nil
}

func (c *Connector) Definition() contracts.ConnectorRef { return c.definitionRef }

func (c *Connector) Capabilities() []connector.Capability {
	if c == nil {
		return nil
	}
	names := []string{ReadCapabilityName, ListCapabilityName, SubmitCapabilityName}
	out := make([]connector.Capability, 0, len(names))
	for _, name := range names {
		out = append(out, &capability{parent: c, definition: c.definitions[name], name: name})
	}
	return out
}

func (c *Connector) Health(ctx context.Context) connector.HealthStatus {
	if c == nil || c.client == nil || c.resolver == nil || c.binding.CredentialRef != "" && c.credentialResolver == nil {
		return connector.HealthStatus{Status: connector.HealthUnavailable, Reason: "http connector is not configured"}
	}
	if err := ctx.Err(); err != nil {
		return connector.HealthStatus{Status: connector.HealthUnavailable, Reason: "request cancelled"}
	}
	return connector.HealthStatus{Status: connector.HealthReady}
}

type capability struct {
	parent     *Connector
	definition contracts.CapabilityDefinition
	name       string
}

func (c *capability) Definition() contracts.CapabilityDefinition { return c.definition }

func (c *capability) Validate(request contracts.OperationRequest) error {
	if c == nil || c.parent == nil {
		return fmt.Errorf("http capability is not configured")
	}
	if err := contracts.ValidateOperationRequest(request, c.definition); err != nil {
		return err
	}
	if request.Target.System.Type != "http" || request.Target.System.ID != c.parent.binding.ID {
		return fmt.Errorf("http request target is not bound to connector")
	}
	if c.name == SubmitCapabilityName && request.IdempotencyKey == "" {
		return fmt.Errorf("http submission requires an idempotency key")
	}
	return nil
}

func (c *capability) Plan(request contracts.OperationRequest) (contracts.OperationPlan, error) {
	if err := c.Validate(request); err != nil {
		return contracts.OperationPlan{}, err
	}
	plan := contracts.OperationPlan{ID: request.ID + "-plan", OperationID: request.ID, OperationHash: request.StableHash(), WorkspaceID: request.WorkspaceID, Actor: request.Actor, Steps: []contracts.OperationStep{{ID: request.ID + "-http", Ordinal: 0, Kind: "http." + c.name, Capability: request.Capability, Target: request.Target, Effect: c.definition.Effect, Profile: request.Profile, Evidence: c.definition.Evidence, InputHash: request.InputHash}}}
	if err := plan.Normalize(); err != nil {
		return contracts.OperationPlan{}, err
	}
	return plan, nil
}

func (c *capability) Execute(ctx context.Context, request contracts.OperationRequest, plan contracts.OperationPlan) (contracts.OperationResult, error) {
	if err := c.Validate(request); err != nil {
		return contracts.OperationResult{}, err
	}
	if err := plan.Normalize(); err != nil {
		return contracts.OperationResult{}, err
	}
	payloadBytes, err := c.parent.resolver(ctx, request.WorkspaceID, request.InputHash)
	if err != nil {
		return contracts.OperationResult{}, &connector.FailureError{Code: "input_unavailable", Retryable: false, Err: err}
	}
	if int64(len(payloadBytes)) > c.parent.binding.MaxRequestBytes || !connector.VerifyPayload(payloadBytes, request.InputHash) {
		return contracts.OperationResult{}, &connector.FailureError{Code: "invalid_request", Retryable: false}
	}
	var payload Payload
	if err := json.Unmarshal(payloadBytes, &payload); err != nil {
		return contracts.OperationResult{}, &connector.FailureError{Code: "invalid_request", Retryable: false, Err: err}
	}
	if err := payload.Normalize(c.name == SubmitCapabilityName); err != nil {
		return contracts.OperationResult{}, &connector.FailureError{Code: "invalid_request", Retryable: false, Err: err}
	}
	if c.name == ReadCapabilityName && payload.Method != http.MethodGet && payload.Method != http.MethodHead {
		return contracts.OperationResult{}, &connector.FailureError{Code: "invalid_request", Retryable: false}
	}
	if c.name == ListCapabilityName && payload.Method != http.MethodGet {
		return contracts.OperationResult{}, &connector.FailureError{Code: "invalid_request", Retryable: false}
	}
	requestURL, err := c.parent.urlFor(payload, c.name == ListCapabilityName)
	if err != nil {
		return contracts.OperationResult{}, &connector.FailureError{Code: "unauthorized", Retryable: false, Err: err}
	}
	body := []byte(nil)
	if len(payload.Body) > 0 && string(payload.Body) != "null" {
		body = append([]byte(nil), payload.Body...)
	}
	if int64(len(body)) > c.parent.binding.MaxRequestBytes {
		return contracts.OperationResult{}, &connector.FailureError{Code: "budget", Retryable: false}
	}
	runCtx, cancel := context.WithTimeout(ctx, time.Duration(c.parent.binding.TimeoutMS)*time.Millisecond)
	defer cancel()
	req, err := http.NewRequestWithContext(runCtx, payload.Method, requestURL, strings.NewReader(string(body)))
	if err != nil {
		return contracts.OperationResult{}, &connector.FailureError{Code: "invalid_request", Retryable: false, Err: err}
	}
	for key, value := range payload.Headers {
		req.Header.Set(key, value)
	}
	if len(body) > 0 && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.parent.credentialResolver != nil && c.parent.binding.CredentialRef != "" {
		secret, resolveErr := c.parent.credentialResolver(runCtx, request.WorkspaceID, c.parent.binding.CredentialRef)
		if resolveErr != nil {
			return contracts.OperationResult{}, &connector.FailureError{Code: "credential_unavailable", Retryable: false, Err: resolveErr}
		}
		secretBytes := secret.Bytes()
		req.Header.Set("Authorization", "Bearer "+string(secretBytes))
		secret.Clear()
		for index := range secretBytes {
			secretBytes[index] = 0
		}
	}
	if c.name == SubmitCapabilityName {
		req.Header.Set("Idempotency-Key", request.IdempotencyKey)
	}
	response, err := c.parent.client.Do(req)
	if err != nil {
		code := "transport"
		if errors.Is(runCtx.Err(), context.DeadlineExceeded) {
			code = "timeout"
		}
		return contracts.OperationResult{}, &connector.FailureError{Code: code, Retryable: c.name != SubmitCapabilityName, ExternalEffectStarted: c.name == SubmitCapabilityName, Err: err}
	}
	defer response.Body.Close()
	if response.ContentLength > c.parent.binding.MaxResponseBytes {
		return contracts.OperationResult{}, &connector.FailureError{Code: "budget", Retryable: false, ExternalEffectStarted: c.name == SubmitCapabilityName}
	}
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, c.parent.binding.MaxResponseBytes+1))
	if err != nil {
		return contracts.OperationResult{}, &connector.FailureError{Code: "transport", Retryable: c.name != SubmitCapabilityName, ExternalEffectStarted: c.name == SubmitCapabilityName, Err: err}
	}
	if int64(len(responseBody)) > c.parent.binding.MaxResponseBytes {
		return contracts.OperationResult{}, &connector.FailureError{Code: "budget", Retryable: false, ExternalEffectStarted: c.name == SubmitCapabilityName, Err: ErrResponseTooLarge}
	}
	if !statusAllowed(response.StatusCode, payload.ExpectedStatus) {
		code, retryable := "http_error", false
		if response.StatusCode == http.StatusTooManyRequests {
			code, retryable = "rate_limit", true
		} else if response.StatusCode >= 500 {
			code, retryable = "transport", c.name != SubmitCapabilityName
		}
		return contracts.OperationResult{}, &connector.FailureError{Code: code, Retryable: retryable, ExternalEffectStarted: c.name == SubmitCapabilityName}
	}
	bodyHash := connector.HashPayload(responseBody)
	nextToken := ""
	if c.name == ListCapabilityName {
		nextToken = extractNextPageToken(responseBody)
	}
	summary := map[string]any{"status": response.StatusCode, "bytes": len(responseBody), "body_hash": bodyHash, "content_type": response.Header.Get("Content-Type"), "next_page_token_hash": hashOptional(nextToken)}
	summaryBytes, _ := json.Marshal(summary)
	outputHash := connector.HashPayload(summaryBytes)
	evidenceHash := connector.HashPayload(append(append([]byte(nil), summaryBytes...), []byte("\x00"+request.InputHash)...))
	evidence := contracts.OperationEvidenceRef{WorkspaceID: request.WorkspaceID, SourceReference: "http:" + c.parent.binding.ID + ":" + request.InputHash[:16], EvidenceHash: evidenceHash, Role: "http_response"}
	result := contracts.OperationResult{ID: request.ID + "-result", OperationID: request.ID, OperationHash: request.StableHash(), RequestID: request.RequestID, WorkspaceID: request.WorkspaceID, Actor: request.Actor, Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: c.definition.OutputSchemaVersion, OutputSchemaHash: c.definition.OutputSchemaHash, OutputHash: outputHash, Evidence: []contracts.OperationEvidenceRef{evidence}, Steps: []contracts.OperationStepResult{{StepID: plan.Steps[0].ID, Status: contracts.OperationStatusSucceeded, OutputSchemaVersion: c.definition.OutputSchemaVersion, OutputSchemaHash: c.definition.OutputSchemaHash, OutputHash: outputHash, Evidence: []contracts.OperationEvidenceRef{evidence}}}}
	if c.name == SubmitCapabilityName {
		providerRequestID := boundedHeader(response.Header.Get("X-Request-ID"))
		if providerRequestID == "" {
			providerRequestID = boundedHeader(response.Header.Get("Request-ID"))
		}
		effect := contracts.ExternalEffect{ID: effectID(request.ID), WorkspaceID: request.WorkspaceID, Boundary: "http:" + c.parent.binding.ID, Class: c.definition.Effect, DeliveryGuarantee: contracts.ExternalDeliveryAtLeastOnce, IdempotencyKey: request.IdempotencyKey, ProviderRequestID: providerRequestID, ProviderIdempotency: c.parent.binding.ProviderSupportsIdempotency, VerificationRequired: c.parent.binding.VerificationRequired, VerificationStatus: contracts.ExternalVerificationNotRequired, CompensationStatus: contracts.ExternalCompensationUnavailable}
		if c.parent.binding.VerificationRequired {
			effect.VerificationStatus = contracts.ExternalVerificationPending
		}
		result.ExternalEffects = []contracts.ExternalEffect{effect}
		result.Steps[0].ExternalEffect = &result.ExternalEffects[0]
	}
	if err := result.Normalize(); err != nil {
		return contracts.OperationResult{}, err
	}
	return result, nil
}

func (p *Payload) Normalize(submit bool) error {
	if p == nil {
		return fmt.Errorf("http payload is nil")
	}
	if p.SchemaVersion == 0 {
		p.SchemaVersion = 1
	}
	if p.SchemaVersion != 1 {
		return fmt.Errorf("unsupported http payload schema_version")
	}
	p.Method = strings.ToUpper(strings.TrimSpace(p.Method))
	if p.Method == "" {
		p.Method = http.MethodGet
	}
	if submit {
		switch p.Method {
		case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
		default:
			return fmt.Errorf("http submission method is not allowed")
		}
	} else if p.Method != http.MethodGet && p.Method != http.MethodHead {
		return fmt.Errorf("http read method is not allowed")
	}
	if err := validateRelativePath(p.Path); err != nil {
		return err
	}
	if len(p.Query) > MaxQueryEntries || len(p.Headers) > MaxHeaderEntries {
		return fmt.Errorf("http payload query or header budget exceeded")
	}
	for key, value := range p.Query {
		if key == "" || len(key) > 64 || len(value) > 256 || strings.ContainsAny(key+value, "\x00\r\n") {
			return fmt.Errorf("http query is invalid")
		}
	}
	for key, value := range p.Headers {
		lower := strings.ToLower(strings.TrimSpace(key))
		if lower != "accept" && lower != "content-type" && lower != "if-none-match" && lower != "if-modified-since" {
			return fmt.Errorf("http header %q is not allowed", key)
		}
		if value == "" || len(value) > 256 || strings.ContainsAny(value, "\x00\r\n") {
			return fmt.Errorf("http header %q is invalid", key)
		}
	}
	if len(p.Body) > MaxMaxHTTPBytes {
		return fmt.Errorf("http request body is too large")
	}
	if len(p.Body) > 0 && string(p.Body) != "null" && !json.Valid(p.Body) {
		return fmt.Errorf("http request body is not valid JSON")
	}
	if p.PageSize == 0 {
		p.PageSize = 100
	}
	if p.PageSize < 1 || p.PageSize > 1000 || len(p.PageToken) > 512 {
		return fmt.Errorf("http pagination budget is invalid")
	}
	if len(p.ExpectedStatus) > 16 {
		return fmt.Errorf("http expected status budget is invalid")
	}
	for _, status := range p.ExpectedStatus {
		if status < 100 || status > 599 {
			return fmt.Errorf("http expected status is invalid")
		}
	}
	sort.Ints(p.ExpectedStatus)
	return nil
}

func (c *Connector) urlFor(payload Payload, paginate bool) (string, error) {
	base, err := url.Parse(c.binding.BaseURL)
	if err != nil {
		return "", err
	}
	if !hostAllowed(base, c.binding.AllowedHosts) {
		return "", ErrUnsafeURL
	}
	joined := pathpkg.Join(base.Path, payload.Path)
	if !strings.HasPrefix(joined, "/") || !pathAllowed(joined, c.binding.AllowedPathPrefixes) {
		return "", ErrUnsafeURL
	}
	base.Path, base.RawPath, base.RawQuery, base.Fragment = joined, "", "", ""
	values := url.Values{}
	for key, value := range payload.Query {
		values.Set(key, value)
	}
	if paginate {
		values.Set("page_size", strconv.Itoa(payload.PageSize))
		if payload.PageToken != "" {
			values.Set("page_token", payload.PageToken)
		}
	}
	base.RawQuery = values.Encode()
	return base.String(), nil
}

func configuredClient(binding Binding, supplied *http.Client) (*http.Client, error) {
	client := &http.Client{}
	if supplied != nil {
		*client = *supplied
	}
	if client.Timeout == 0 || client.Timeout > time.Duration(binding.TimeoutMS)*time.Millisecond {
		client.Timeout = time.Duration(binding.TimeoutMS) * time.Millisecond
	}
	client.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if !binding.AllowRedirects {
			return http.ErrUseLastResponse
		}
		if len(via) >= 5 || request.URL == nil || request.URL.User != nil || !hostAllowed(request.URL, binding.AllowedHosts) || !pathAllowed(request.URL.Path, binding.AllowedPathPrefixes) {
			return ErrUnsafeURL
		}
		return nil
	}
	transport, ok := client.Transport.(*http.Transport)
	if client.Transport == nil {
		transport = http.DefaultTransport.(*http.Transport).Clone()
		ok = true
	}
	if !ok && !binding.AllowPrivateNetworks {
		return nil, fmt.Errorf("custom HTTP transports require explicit private-network policy")
	}
	if ok {
		transport = transport.Clone()
		transport.Proxy = nil
		if !binding.AllowPrivateNetworks {
			transport.DialContext = safeDialContext(false)
		}
		client.Transport = transport
	}
	return client, nil
}

func safeDialContext(allowPrivate bool) func(context.Context, string, string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		ips, err := net.DefaultResolver.LookupIP(ctx, "ip", host)
		if err != nil {
			return nil, err
		}
		for _, ip := range ips {
			if !allowPrivate && privateIP(ip) {
				return nil, ErrUnsafeURL
			}
		}
		for _, ip := range ips {
			conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if dialErr == nil {
				return conn, nil
			}
		}
		return nil, fmt.Errorf("http connection failed")
	}
}

func privateIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast()
}

func validateRelativePath(value string) error {
	if value == "" {
		value = "/"
	}
	if len(value) > MaxPathLength || !strings.HasPrefix(value, "/") || strings.ContainsAny(value, "\\\x00\r\n") {
		return ErrUnsafeURL
	}
	parsed, err := url.Parse(value)
	if err != nil || parsed.IsAbs() || parsed.Host != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return ErrUnsafeURL
	}
	clean := pathpkg.Clean(value)
	if clean != value || strings.Contains(value, "..") {
		return ErrUnsafeURL
	}
	return nil
}

func pathAllowed(path string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if prefix == "/" || path == prefix || strings.HasPrefix(path, strings.TrimSuffix(prefix, "/")+"/") {
			return true
		}
	}
	return false
}

func hostAllowed(value *url.URL, hosts []string) bool {
	host := strings.ToLower(value.Host)
	hostname := strings.ToLower(value.Hostname())
	for _, allowed := range hosts {
		allowed = strings.ToLower(strings.TrimSpace(allowed))
		if host == allowed || (!strings.Contains(allowed, ":") && hostname == allowed) {
			return true
		}
	}
	return false
}

func statusAllowed(status int, expected []int) bool {
	if len(expected) == 0 {
		return status >= 200 && status < 300
	}
	for _, value := range expected {
		if status == value {
			return true
		}
	}
	return false
}

func extractNextPageToken(body []byte) string {
	var value struct {
		NextPageToken string `json:"next_page_token"`
	}
	if json.Unmarshal(body, &value) != nil || len(value.NextPageToken) > 512 {
		return ""
	}
	return value.NextPageToken
}

func hashOptional(value string) string {
	if value == "" {
		return ""
	}
	return connector.HashPayload([]byte(value))
}

func boundedHeader(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 128 || strings.ContainsAny(value, "\x00\r\n") {
		return ""
	}
	return value
}

func effectID(operationID string) string {
	digest := sha256.Sum256([]byte(operationID + ":http-effect"))
	return "effect-" + hex.EncodeToString(digest[:])[:32]
}

func schemaHash(value string) string { return connector.HashPayload([]byte(value)) }

func httpProfile(external bool) contracts.ExecutionProfile {
	profile := contracts.DefaultExecutionProfile()
	profile.TimeoutMS = int64(DefaultTimeout.Milliseconds())
	profile.MaxInputBytes = DefaultMaxRequest
	profile.MaxOutputBytes = DefaultMaxResponse
	profile.MaxRetries = 1
	profile.AllowExternalEffects = external
	return profile
}

func uniqueSorted(values []string) []string {
	set := make(map[string]struct{}, len(values))
	for _, value := range values {
		set[strings.ToLower(strings.TrimSpace(value))] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for value := range set {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}
