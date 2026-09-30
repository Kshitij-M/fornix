package credentials

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"os"
	pathpkg "path"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/connector"
)

const (
	DefaultManagerRequestBytes  int64 = 16 << 10
	DefaultManagerResponseBytes int64 = 64 << 10
	MaxManagerResponseBytes     int64 = 1 << 20
)

// TokenSource supplies only the manager authentication token. It is separate
// from SecretManager so a deployment can use mTLS, workload identity, or a
// short-lived bearer token without changing the authority contract.
type TokenSource interface {
	Token(context.Context) (Secret, error)
}

type TokenSourceFunc func(context.Context) (Secret, error)

func (f TokenSourceFunc) Token(ctx context.Context) (Secret, error) {
	if f == nil {
		return Secret{}, ErrManagedUnavailable
	}
	return f(ctx)
}

// EnvTokenSource is an explicit process-boundary adapter for local and simple
// deployments. It stores only the environment variable name, reads the token
// for the single manager request, and returns a Secret that the caller clears.
// Hosted deployments should prefer workload identity or mTLS and inject a
// TokenSource directly instead of using this adapter.
type EnvTokenSource struct{ Name string }

func (s EnvTokenSource) Token(ctx context.Context) (Secret, error) {
	if err := ctx.Err(); err != nil {
		return Secret{}, err
	}
	name := strings.TrimSpace(s.Name)
	if !validEnvironmentName(name) {
		return Secret{}, ErrManagedDenied
	}
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return Secret{}, ErrManagedDenied
	}
	return NewSecret([]byte(value))
}

func validEnvironmentName(name string) bool {
	if name == "" {
		return false
	}
	for index, char := range name {
		if (char >= 'A' && char <= 'Z') || (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9' && index > 0) || (char == '_' && index > 0) {
			continue
		}
		return false
	}
	return true
}

// HTTPSecretManager is a bounded adapter for deployments exposing the Fornix
// metadata-only secret-manager protocol. The supplied client must be created
// with the deployment's controlled egress policy; this adapter never follows
// redirects and never logs response bodies.
type HTTPSecretManager struct {
	Endpoint         string
	Client           *http.Client
	Token            TokenSource
	MaxRequestBytes  int64
	MaxResponseBytes int64
	Timeout          time.Duration
	controlled       bool
}

// HTTPSecretManagerConfig is the deployment-facing constructor input. The
// constructor wraps the client in the same destination, DNS, redirect, byte,
// and timeout boundary used by the other Fornix HTTP adapters.
type HTTPSecretManagerConfig struct {
	Endpoint             string
	Client               *http.Client
	Token                TokenSource
	AllowPrivateNetworks bool
	MaxRequestBytes      int64
	MaxResponseBytes     int64
	Timeout              time.Duration
	Resolver             connector.IPResolver
}

// NewHTTPSecretManager constructs a controlled manager adapter. A literal
// HTTPSecretManager is intentionally not executable; deployments must use
// this constructor so an arbitrary client cannot bypass egress policy.
func NewHTTPSecretManager(config HTTPSecretManagerConfig) (*HTTPSecretManager, error) {
	parsed, err := url.Parse(strings.TrimSpace(config.Endpoint))
	if err != nil || parsed.Scheme == "" || parsed.Hostname() == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, ErrManagedScope
	}
	basePath := pathpkg.Clean(parsed.Path)
	if basePath == "." || basePath == "" {
		basePath = "/"
	}
	if config.MaxRequestBytes <= 0 {
		config.MaxRequestBytes = DefaultManagerRequestBytes
	}
	if config.MaxResponseBytes <= 0 {
		config.MaxResponseBytes = DefaultManagerResponseBytes
	}
	if config.Timeout <= 0 {
		config.Timeout = 10 * time.Second
	}
	policy := connector.EgressPolicy{
		Destination: connector.DestinationPolicy{
			AllowedSchemes:       []string{strings.ToLower(parsed.Scheme)},
			AllowedHosts:         []string{strings.ToLower(parsed.Hostname())},
			AllowedPathPrefixes:  []string{basePath},
			AllowPrivateNetworks: config.AllowPrivateNetworks,
			AllowRedirects:       false,
		},
		MaxRequestBytes:  config.MaxRequestBytes,
		MaxResponseBytes: config.MaxResponseBytes,
		Timeout:          config.Timeout,
	}
	client, err := connector.NewEgressClient(policy, config.Client, connector.EgressOptions{Resolver: config.Resolver})
	if err != nil {
		return nil, err
	}
	return &HTTPSecretManager{
		Endpoint:         parsed.String(),
		Client:           client,
		Token:            config.Token,
		MaxRequestBytes:  config.MaxRequestBytes,
		MaxResponseBytes: config.MaxResponseBytes,
		Timeout:          config.Timeout,
		controlled:       true,
	}, nil
}

func (m HTTPSecretManager) Normalize() error {
	if !m.controlled || strings.TrimSpace(m.Endpoint) == "" || m.Client == nil {
		return ErrManagedUnavailable
	}
	if m.MaxRequestBytes <= 0 {
		m.MaxRequestBytes = DefaultManagerRequestBytes
	}
	if m.MaxResponseBytes <= 0 {
		m.MaxResponseBytes = DefaultManagerResponseBytes
	}
	if m.MaxResponseBytes > MaxManagerResponseBytes || m.MaxRequestBytes > DefaultManagerRequestBytes {
		return ErrManagedResponse
	}
	if m.Timeout <= 0 || m.Timeout > 2*time.Minute {
		return ErrManagedUnavailable
	}
	return nil
}

type managerRequest struct {
	WorkspaceID string `json:"workspace_id"`
	Provider    string `json:"provider"`
	Reference   string `json:"reference"`
	Purpose     string `json:"purpose"`
}

type managerResponse struct {
	SecretBase64 []byte `json:"secret_base64"`
	Version      string `json:"version"`
	ExpiresAt    string `json:"expires_at,omitempty"`
}

func (m HTTPSecretManager) Resolve(ctx context.Context, request ResolveRequest) (ResolvedSecret, error) {
	if err := request.Normalize(); err != nil {
		return ResolvedSecret{}, err
	}
	if m.MaxRequestBytes == 0 {
		m.MaxRequestBytes = DefaultManagerRequestBytes
	}
	if m.MaxResponseBytes == 0 {
		m.MaxResponseBytes = DefaultManagerResponseBytes
	}
	if m.Timeout == 0 {
		m.Timeout = 10 * time.Second
	}
	if err := m.Normalize(); err != nil {
		return ResolvedSecret{}, err
	}

	payload, err := json.Marshal(managerRequest{WorkspaceID: request.WorkspaceID, Provider: request.Provider, Reference: request.Reference.String(), Purpose: request.Purpose})
	if err != nil || int64(len(payload)) > m.MaxRequestBytes {
		return ResolvedSecret{}, ErrManagedResponse
	}
	callCtx, cancel := context.WithTimeout(ctx, m.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(callCtx, http.MethodPost, m.Endpoint, strings.NewReader(string(payload)))
	if err != nil {
		return ResolvedSecret{}, ErrManagedUnavailable
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	if m.Token != nil {
		token, tokenErr := m.Token.Token(callCtx)
		if tokenErr != nil || token.Len() == 0 {
			token.Clear()
			return ResolvedSecret{}, ErrManagedDenied
		}
		value := token.Bytes()
		req.Header.Set("Authorization", "Bearer "+string(value))
		clearBytes(value)
		token.Clear()
	}
	client := *m.Client
	client.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return ErrManagedUnavailable
	}
	response, err := DoCredentialRequest(&client, req)
	if err != nil {
		return ResolvedSecret{}, ErrManagedUnavailable
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden {
		return ResolvedSecret{}, ErrManagedDenied
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return ResolvedSecret{}, ErrManagedUnavailable
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, m.MaxResponseBytes+1))
	defer clearBytes(body)
	if err != nil || int64(len(body)) > m.MaxResponseBytes {
		return ResolvedSecret{}, ErrManagedResponse
	}
	resolved, err := decodeManagerResponse(body)
	if err != nil {
		return ResolvedSecret{}, err
	}
	return resolved, nil
}

func decodeManagerResponse(body []byte) (ResolvedSecret, error) {
	var decoded managerResponse
	defer func() { clearBytes(decoded.SecretBase64) }()
	if err := json.Unmarshal(body, &decoded); err != nil {
		return ResolvedSecret{}, ErrManagedResponse
	}
	secretBytes := decoded.SecretBase64
	decoded.SecretBase64 = nil
	defer clearBytes(secretBytes)
	if len(secretBytes) == 0 {
		return ResolvedSecret{}, ErrManagedResponse
	}
	secret, err := NewSecret(secretBytes)
	if err != nil {
		return ResolvedSecret{}, ErrManagedResponse
	}
	result := ResolvedSecret{Secret: secret, Version: strings.TrimSpace(decoded.Version)}
	if strings.TrimSpace(decoded.ExpiresAt) != "" {
		result.ExpiresAt, err = time.Parse(time.RFC3339Nano, decoded.ExpiresAt)
		if err != nil {
			result.Clear()
			return ResolvedSecret{}, ErrManagedResponse
		}
	}
	return result, nil
}
