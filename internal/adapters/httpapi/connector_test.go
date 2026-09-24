package httpapi_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/adapters/httpapi"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/credentials"
)

func TestHTTPReadUsesConfiguredTargetAndRedactsCredential(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.URL.Path != "/v1/items" || request.URL.Query().Get("page_size") != "100" {
			t.Fatalf("unexpected target: %s", request.URL.String())
		}
		if request.Header.Get("Authorization") != "Bearer test-secret" {
			t.Fatalf("credential was not injected at request boundary")
		}
		writer.Header().Set("Content-Type", "application/json")
		_, _ = writer.Write([]byte(`{"items":[{"id":"one"}],"next_page_token":"next"}`))
	}))
	defer server.Close()

	resolver := connector.NewStaticPayloadResolver()
	payload := httpapi.Payload{Method: http.MethodGet, Path: "/v1/items", PageSize: 100}
	request, adapter := httpRequest(t, server.URL, resolver, payload, true, func(_ context.Context, _ string, _ string) (credentials.Secret, error) {
		return credentials.NewSecret([]byte("test-secret"))
	})
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	outcome, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), request, connector.AdmissionOptions{Credentials: map[string]bool{"provider/api": true}})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Attempts != 1 || outcome.Result.OutputHash == "" || len(outcome.Result.Evidence) != 1 {
		t.Fatalf("unexpected HTTP outcome: %+v", outcome)
	}
	raw, _ := json.Marshal(outcome.Result)
	if strings.Contains(string(raw), "test-secret") {
		t.Fatal("credential appeared in operation result")
	}
	if calls.Load() != 1 {
		t.Fatalf("HTTP calls=%d, want one", calls.Load())
	}
}

func TestHTTPRejectsAbsolutePathsAndOversizedResponses(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write(make([]byte, 128))
	}))
	defer server.Close()
	for name, payload := range map[string]httpapi.Payload{
		"absolute":  {Method: http.MethodGet, Path: server.URL + "/escape"},
		"traversal": {Method: http.MethodGet, Path: "/v1/../escape"},
	} {
		t.Run(name, func(t *testing.T) {
			resolver := connector.NewStaticPayloadResolver()
			request, adapter := httpRequestWithBinding(t, server.URL, resolver, payload, httpapi.Binding{MaxResponseBytes: 256})
			registry := connector.NewRegistry()
			if err := registry.Register(adapter); err != nil {
				t.Fatal(err)
			}
			if _, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), request, connector.AdmissionOptions{}); err == nil {
				t.Fatal("unsafe HTTP payload unexpectedly executed")
			}
		})
	}

	resolver := connector.NewStaticPayloadResolver()
	request, adapter := httpRequestWithBinding(t, server.URL, resolver, httpapi.Payload{Method: http.MethodGet, Path: "/v1/items"}, httpapi.Binding{MaxResponseBytes: 16})
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if _, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), request, connector.AdmissionOptions{}); err == nil {
		t.Fatal("oversized response unexpectedly succeeded")
	}
}

func TestHTTPReadRetriesRateLimitButSubmitDoesNotRetryAfterEffectStart(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		call := calls.Add(1)
		if request.Method == http.MethodGet && call == 1 {
			writer.WriteHeader(http.StatusTooManyRequests)
			return
		}
		if request.Method == http.MethodPost {
			writer.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()
	resolver := connector.NewStaticPayloadResolver()
	readRequest, readAdapter := httpRequest(t, server.URL, resolver, httpapi.Payload{Method: http.MethodGet, Path: "/v1/items"}, false, nil)
	registry := connector.NewRegistry()
	if err := registry.Register(readAdapter); err != nil {
		t.Fatal(err)
	}
	readOutcome, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), readRequest, connector.AdmissionOptions{})
	if err != nil || readOutcome.Attempts != 2 {
		t.Fatalf("rate-limited read outcome=%+v err=%v", readOutcome, err)
	}

	writePayload := httpapi.Payload{Method: http.MethodPost, Path: "/v1/items", Body: json.RawMessage(`{"value":"bounded"}`)}
	writeRequest, writeAdapter := httpRequest(t, server.URL, resolver, writePayload, false, nil)
	// The resolver is immutable by design, so the write uses a second resolver.
	writeResolver := connector.NewStaticPayloadResolver()
	writeRequest, writeAdapter = httpRequest(t, server.URL, writeResolver, writePayload, false, nil)
	registry = connector.NewRegistry()
	if err := registry.Register(writeAdapter); err != nil {
		t.Fatal(err)
	}
	if _, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), writeRequest, connector.AdmissionOptions{ApprovalGranted: true}); err == nil {
		t.Fatal("submit unexpectedly succeeded")
	}
	if calls.Load() != 3 {
		t.Fatalf("calls=%d, want read retry twice plus one submit", calls.Load())
	}
}

func TestHTTPSubmitRequiresApprovalAndRecordsEffectMetadata(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Header.Get("Idempotency-Key") != "http-idempotency" {
			t.Fatalf("idempotency key was not forwarded")
		}
		writer.Header().Set("X-Request-ID", "provider-request-1")
		writer.WriteHeader(http.StatusAccepted)
		_, _ = writer.Write([]byte(`{"accepted":true}`))
	}))
	defer server.Close()
	resolver := connector.NewStaticPayloadResolver()
	binding := httpapi.Binding{ID: "billing-api", WorkspaceID: "workspace-a", BaseURL: server.URL, AllowPrivateNetworks: true, ProviderSupportsIdempotency: true, VerificationRequired: true}
	request, adapter := httpRequestWithBindingAndCredential(t, resolver, httpapi.Payload{Method: http.MethodPost, Path: "/v1/items", Body: json.RawMessage(`{"value":"bounded"}`)}, binding, nil)
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	executor := &connector.Executor{Registry: registry}
	if _, err := executor.Execute(context.Background(), request, connector.AdmissionOptions{}); err == nil {
		t.Fatal("HTTP submission without approval unexpectedly executed")
	}
	if calls.Load() != 0 {
		t.Fatalf("approval rejection reached HTTP server: calls=%d", calls.Load())
	}
	outcome, err := executor.Execute(context.Background(), request, connector.AdmissionOptions{ApprovalGranted: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(outcome.Result.ExternalEffects) != 1 {
		t.Fatalf("external effects=%d, want one", len(outcome.Result.ExternalEffects))
	}
	effect := outcome.Result.ExternalEffects[0]
	if effect.ProviderRequestID != "provider-request-1" || !effect.ProviderIdempotency || effect.VerificationStatus != contracts.ExternalVerificationPending {
		t.Fatalf("unexpected external effect metadata: %+v", effect)
	}
	if calls.Load() != 1 {
		t.Fatalf("HTTP calls=%d, want one approved submission", calls.Load())
	}
}

func TestHTTPCredentialReferenceFailsClosedWithoutResolver(t *testing.T) {
	resolver := connector.NewStaticPayloadResolver()
	_, err := httpapi.NewConnector(httpapi.Binding{ID: "billing-api", WorkspaceID: "workspace-a", BaseURL: "https://api.example.com", CredentialRef: "provider/api"}, resolver.Resolve, nil, nil)
	if err == nil {
		t.Fatal("credential-bearing HTTP connector was configured without a resolver")
	}
}

func TestHTTPUsesExpiringCredentialLeaseAndReleasesIt(t *testing.T) {
	var calls, acquired, released atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Header.Get("Authorization") != "Bearer leased-secret" {
			t.Fatalf("lease credential was not injected at request boundary")
		}
		_, _ = writer.Write([]byte(`{"ok":true}`))
	}))
	defer server.Close()

	resolver := connector.NewStaticPayloadResolver()
	binding := httpapi.Binding{ID: "leased-api", WorkspaceID: "workspace-a", BaseURL: server.URL, CredentialRef: "provider/api", AllowPrivateNetworks: true}
	payload := httpapi.Payload{Method: http.MethodGet, Path: "/v1/items"}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	inputHash := hash(payloadBytes)
	if err := resolver.Put(binding.WorkspaceID, inputHash, payloadBytes); err != nil {
		t.Fatal(err)
	}
	ref, err := credentials.ParseRef(binding.CredentialRef)
	if err != nil {
		t.Fatal(err)
	}
	leaseResolver := credentials.LeaseResolverFunc{
		AcquireFunc: func(_ context.Context, workspace, reference, purpose string, ttl time.Duration) (credentials.Lease, error) {
			acquired.Add(1)
			if workspace != binding.WorkspaceID || reference != binding.CredentialRef || purpose != "http:"+binding.ID || ttl != credentials.DefaultLeaseTTL {
				t.Fatalf("unexpected lease request workspace=%s reference=%s purpose=%s ttl=%s", workspace, reference, purpose, ttl)
			}
			secret, secretErr := credentials.NewSecret([]byte("leased-secret"))
			if secretErr != nil {
				return credentials.Lease{}, secretErr
			}
			return credentials.Lease{Reference: ref, WorkspaceID: workspace, LeaseID: "lease-1", Purpose: purpose, ExpiresAt: time.Now().UTC().Add(time.Minute), Secret: secret}, nil
		},
		ReleaseFunc: func(_ context.Context, lease credentials.Lease) error {
			released.Add(1)
			if lease.LeaseID != "lease-1" {
				t.Fatalf("unexpected released lease=%s", lease.LeaseID)
			}
			return nil
		},
	}
	adapter, err := httpapi.NewConnectorWithLeaseResolver(binding, resolver.Resolve, leaseResolver, nil)
	if err != nil {
		t.Fatal(err)
	}
	definition := adapter.Capabilities()[0].Definition()
	request := contracts.OperationRequest{ID: "leased-operation", RequestID: "leased-request", IdempotencyKey: "leased-idempotency", WorkspaceID: binding.WorkspaceID, Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: binding.WorkspaceID}, Capability: definition.Ref, Target: contracts.ResourceRef{WorkspaceID: binding.WorkspaceID, System: contracts.SystemRef{WorkspaceID: binding.WorkspaceID, Type: "http", ID: binding.ID, Version: "1"}, Kind: httpapi.ResourceKind, ID: binding.ID, Version: "1"}, InputType: httpapi.InputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: inputHash, Profile: definition.Profile}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	outcome, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), request, connector.AdmissionOptions{Credentials: map[string]bool{binding.CredentialRef: true}})
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Result.OutputHash == "" || calls.Load() != 1 || acquired.Load() != 1 || released.Load() != 1 {
		t.Fatalf("unexpected lease execution calls=%d acquired=%d released=%d outcome=%+v", calls.Load(), acquired.Load(), released.Load(), outcome)
	}
	raw, _ := json.Marshal(outcome.Result)
	if strings.Contains(string(raw), "leased-secret") {
		t.Fatal("lease secret appeared in operation result")
	}
}

func TestHTTPRejectsPrivateNetworkWhenNotExplicitlyAllowed(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { _, _ = writer.Write([]byte(`ok`)) }))
	defer server.Close()
	resolver := connector.NewStaticPayloadResolver()
	payload := httpapi.Payload{Method: http.MethodGet, Path: "/"}
	request, adapter := httpRequestWithBindingAndCredential(t, resolver, payload, httpapi.Binding{ID: "billing-api", WorkspaceID: "workspace-a", BaseURL: server.URL, AllowPrivateNetworks: false}, nil)
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if _, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), request, connector.AdmissionOptions{}); err == nil {
		t.Fatal("private network request unexpectedly succeeded")
	}
}

func TestHTTPRedirectCannotEscapeConfiguredHost(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte(`escaped`))
	}))
	defer target.Close()
	redirect := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		writer.Header().Set("Location", target.URL)
		writer.WriteHeader(http.StatusFound)
	}))
	defer redirect.Close()
	resolver := connector.NewStaticPayloadResolver()
	request, adapter := httpRequestWithBinding(t, redirect.URL, resolver, httpapi.Payload{Method: http.MethodGet, Path: "/"}, httpapi.Binding{AllowRedirects: true})
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if _, err := (&connector.Executor{Registry: registry}).Execute(context.Background(), request, connector.AdmissionOptions{}); err == nil {
		t.Fatal("redirect escaped configured host")
	}
}

func httpRequest(t *testing.T, baseURL string, resolver *connector.StaticPayloadResolver, payload httpapi.Payload, withCredential bool, credentialResolver httpapi.CredentialResolver) (contracts.OperationRequest, *httpapi.Connector) {
	t.Helper()
	binding := httpapi.Binding{ID: "billing-api", WorkspaceID: "workspace-a", BaseURL: baseURL, AllowPrivateNetworks: true}
	if withCredential {
		binding.CredentialRef = "provider/api"
	}
	return httpRequestWithBindingAndCredential(t, resolver, payload, binding, credentialResolver)
}

func httpRequestWithBinding(t *testing.T, baseURL string, resolver *connector.StaticPayloadResolver, payload httpapi.Payload, overrides httpapi.Binding) (contracts.OperationRequest, *httpapi.Connector) {
	t.Helper()
	binding := httpapi.Binding{ID: "billing-api", WorkspaceID: "workspace-a", BaseURL: baseURL, AllowPrivateNetworks: true}
	binding.MaxResponseBytes = overrides.MaxResponseBytes
	// The httptest server is loopback. Keep the helper's default explicit and
	// use httpRequestWithBindingAndCredential directly for the negative SSRF
	// case so that this helper cannot accidentally pass a test before it reaches
	// the intended response/redirect assertion.
	binding.AllowPrivateNetworks = true
	if overrides.MaxResponseBytes == 0 {
		binding.MaxResponseBytes = 256
	}
	return httpRequestWithBindingAndCredential(t, resolver, payload, binding, nil)
}

func httpRequestWithBindingAndCredential(t *testing.T, resolver *connector.StaticPayloadResolver, payload httpapi.Payload, binding httpapi.Binding, credentialResolver httpapi.CredentialResolver) (contracts.OperationRequest, *httpapi.Connector) {
	t.Helper()
	if binding.MaxResponseBytes == 0 {
		binding.MaxResponseBytes = 1 << 20
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	inputHash := hash(payloadBytes)
	if err := resolver.Put(binding.WorkspaceID, inputHash, payloadBytes); err != nil {
		t.Fatal(err)
	}
	adapter, err := httpapi.NewConnector(binding, resolver.Resolve, credentialResolver, nil)
	if err != nil {
		t.Fatal(err)
	}
	definition := adapter.Capabilities()[0].Definition()
	if payload.Method == http.MethodPost {
		definition = adapter.Capabilities()[2].Definition()
	} else if payload.PageSize > 0 {
		definition = adapter.Capabilities()[1].Definition()
	}
	request := contracts.OperationRequest{ID: "operation-http", RequestID: "request-http", IdempotencyKey: "http-idempotency", WorkspaceID: binding.WorkspaceID, Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: binding.WorkspaceID}, Capability: definition.Ref, Target: contracts.ResourceRef{WorkspaceID: binding.WorkspaceID, System: contracts.SystemRef{WorkspaceID: binding.WorkspaceID, Type: "http", ID: binding.ID, Version: "1"}, Kind: httpapi.ResourceKind, ID: binding.ID, Version: "1"}, InputType: httpapi.InputType, InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: inputHash, Profile: definition.Profile}
	return request, adapter
}

func hash(value []byte) string { digest := sha256.Sum256(value); return hex.EncodeToString(digest[:]) }
