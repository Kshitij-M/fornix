package connector

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
	"strings"
	"time"
)

var (
	ErrDestinationScheme = errors.New("destination scheme is not allowed")
	ErrDestinationHost   = errors.New("destination host is not allowed")
	ErrDestinationPath   = errors.New("destination path is not allowed")
	ErrDestinationPolicy = errors.New("destination policy is invalid")
	ErrEgressTransport   = errors.New("outbound transport is not controlled by egress policy")
	ErrEgressRedirect    = errors.New("outbound redirect is not allowed by egress policy")
	ErrEgressRequest     = errors.New("outbound request exceeds egress budget")
	ErrEgressResponse    = errors.New("outbound response exceeds egress budget")
)

// DestinationPolicy is the reusable, secret-free part of outbound connector
// admission. New network adapters should use NewEgressClient so these rules
// are enforced at the actual HTTP transport boundary, rather than only while
// constructing a URL. The policy remains intentionally secret-free.
type DestinationPolicy struct {
	AllowedSchemes       []string `json:"allowed_schemes"`
	AllowedHosts         []string `json:"allowed_hosts"`
	AllowedPathPrefixes  []string `json:"allowed_path_prefixes"`
	AllowPrivateNetworks bool     `json:"allow_private_networks"`
	AllowRedirects       bool     `json:"allow_redirects"`
	MaxRedirects         int      `json:"max_redirects"`
}

// EgressPolicy is the common bounded HTTP egress contract. A zero value is
// not executable: callers must provide a restrictive destination policy and
// explicit request/response budgets. AllowProxy is deliberately opt-in; a
// proxy can prevent the process from proving the destination IP itself.
type EgressPolicy struct {
	Destination      DestinationPolicy `json:"destination"`
	MaxRequestBytes  int64             `json:"max_request_bytes"`
	MaxResponseBytes int64             `json:"max_response_bytes"`
	Timeout          time.Duration     `json:"timeout"`
	AllowProxy       bool              `json:"allow_proxy"`
}

func (p *EgressPolicy) Normalize() error {
	if p == nil {
		return ErrDestinationPolicy
	}
	if err := p.Destination.Normalize(); err != nil {
		return err
	}
	if p.MaxRequestBytes <= 0 || p.MaxResponseBytes <= 0 {
		return fmt.Errorf("%w: request and response budgets are required", ErrDestinationPolicy)
	}
	const maxEgressBytes = 64 << 20
	if p.MaxRequestBytes > maxEgressBytes || p.MaxResponseBytes > maxEgressBytes {
		return fmt.Errorf("%w: egress byte budget is too large", ErrDestinationPolicy)
	}
	if p.Timeout <= 0 || p.Timeout > 10*time.Minute {
		return fmt.Errorf("%w: egress timeout is out of bounds", ErrDestinationPolicy)
	}
	return nil
}

// StableHash identifies an egress policy without credentials or transport
// implementation details.
func (p EgressPolicy) StableHash() string {
	if err := p.Normalize(); err != nil {
		return ""
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

// IPResolver is the only name-resolution capability needed by the controlled
// transport. It is injectable so tests can exercise DNS rebinding and private
// address rejection without depending on host configuration.
type IPResolver interface {
	LookupIP(context.Context, string, string) ([]net.IP, error)
}

// IPResolverFunc adapts a function to IPResolver.
type IPResolverFunc func(context.Context, string, string) ([]net.IP, error)

func (f IPResolverFunc) LookupIP(ctx context.Context, network, host string) ([]net.IP, error) {
	return f(ctx, network, host)
}

// EgressOptions are intentionally narrow. The resolver is normally nil,
// which selects net.DefaultResolver. A custom HTTP transport is cloned and
// its proxy and dial hooks are replaced with policy-controlled equivalents.
type EgressOptions struct {
	Resolver IPResolver
}

// NewEgressClient returns an HTTP client whose redirects, DNS resolution,
// private-network access, proxy use, request bytes, response bytes, and wall
// time are bounded by policy. It copies the supplied client; callers retain
// ownership of the input client and its transport.
func NewEgressClient(policy EgressPolicy, supplied *http.Client, options EgressOptions) (*http.Client, error) {
	if err := policy.Normalize(); err != nil {
		return nil, err
	}
	client := &http.Client{}
	if supplied != nil {
		*client = *supplied
	}
	if client.Timeout <= 0 || client.Timeout > policy.Timeout {
		client.Timeout = policy.Timeout
	}
	transport, err := controlledTransport(policy, client.Transport, options.Resolver)
	if err != nil {
		return nil, err
	}
	client.Transport = transport
	client.CheckRedirect = func(request *http.Request, via []*http.Request) error {
		if !policy.Destination.AllowRedirects {
			return ErrEgressRedirect
		}
		if request == nil || request.URL == nil || request.URL.User != nil {
			return ErrEgressRedirect
		}
		if policy.Destination.MaxRedirects <= 0 || len(via) >= policy.Destination.MaxRedirects {
			return ErrEgressRedirect
		}
		if err := policy.Destination.AuthorizeURL(request.URL); err != nil {
			return fmt.Errorf("%w: %v", ErrEgressRedirect, err)
		}
		return nil
	}
	return client, nil
}

func controlledTransport(policy EgressPolicy, supplied http.RoundTripper, resolver IPResolver) (http.RoundTripper, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if supplied != nil {
		candidate, ok := supplied.(*http.Transport)
		if !ok {
			return nil, fmt.Errorf("%w: custom round tripper is not inspectable", ErrEgressTransport)
		}
		transport = candidate.Clone()
	}
	if !policy.AllowProxy {
		transport.Proxy = nil
	}
	if resolver == nil {
		resolver = net.DefaultResolver
	}
	// A custom DialTLSContext could bypass DialContext and the DNS policy. The
	// cloned transport therefore always uses the controlled plain dial path;
	// TLS negotiation remains owned by net/http after that dial.
	transport.DialTLSContext = nil
	transport.DialContext = controlledDialContext(policy.Destination.AllowPrivateNetworks, resolver, &net.Dialer{Timeout: policy.Timeout, KeepAlive: 30 * time.Second})
	return &boundedRoundTripper{base: transport, policy: policy}, nil
}

func controlledDialContext(allowPrivate bool, resolver IPResolver, dialer *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		if ip := net.ParseIP(host); ip != nil {
			if !allowPrivate && privateIP(ip) {
				return nil, ErrDestinationHost
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
		}
		ips, err := resolver.LookupIP(ctx, "ip", host)
		if err != nil {
			return nil, err
		}
		if len(ips) == 0 {
			return nil, fmt.Errorf("no addresses returned for %s", host)
		}
		for _, ip := range ips {
			if !allowPrivate && privateIP(ip) {
				return nil, ErrDestinationHost
			}
		}
		var lastErr error
		for _, ip := range ips {
			conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if dialErr == nil {
				return conn, nil
			}
			lastErr = dialErr
		}
		return nil, lastErr
	}
}

func privateIP(ip net.IP) bool {
	return ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() || ip.IsMulticast()
}

type boundedRoundTripper struct {
	base   http.RoundTripper
	policy EgressPolicy
}

func (t *boundedRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	if request == nil || request.URL == nil {
		return nil, ErrDestinationHost
	}
	if err := t.policy.Destination.AuthorizeURL(request.URL); err != nil {
		return nil, err
	}
	if request.ContentLength > t.policy.MaxRequestBytes {
		return nil, ErrEgressRequest
	}
	if request.Body != nil {
		request = request.Clone(request.Context())
		request.Body = &boundedRequestBody{body: request.Body, remaining: t.policy.MaxRequestBytes}
	}
	response, err := t.base.RoundTrip(request)
	if err != nil {
		return nil, err
	}
	if response != nil && response.Body != nil {
		response.Body = &boundedResponseBody{body: response.Body, remaining: t.policy.MaxResponseBytes}
	}
	return response, nil
}

type boundedRequestBody struct {
	body      io.ReadCloser
	remaining int64
}

func (b *boundedRequestBody) Read(p []byte) (int, error) {
	if b.remaining <= 0 {
		return 0, ErrEgressRequest
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.body.Read(p)
	b.remaining -= int64(n)
	return n, err
}

func (b *boundedRequestBody) Close() error { return b.body.Close() }

type boundedResponseBody struct {
	body      io.ReadCloser
	remaining int64
}

func (b *boundedResponseBody) Read(p []byte) (int, error) {
	if b.remaining <= 0 {
		var probe [1]byte
		n, _ := b.body.Read(probe[:])
		if n > 0 {
			return 0, ErrEgressResponse
		}
		return 0, io.EOF
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.body.Read(p)
	b.remaining -= int64(n)
	return n, err
}

func (b *boundedResponseBody) Close() error { return b.body.Close() }

// Normalize canonicalizes the allowlists and requires a restrictive host
// policy. Empty host lists are rejected so an adapter cannot accidentally
// become an arbitrary URL fetcher.
func (p *DestinationPolicy) Normalize() error {
	if p == nil {
		return ErrDestinationPolicy
	}
	p.AllowedSchemes = normalizePolicyStrings(p.AllowedSchemes)
	p.AllowedHosts = normalizePolicyStrings(p.AllowedHosts)
	p.AllowedPathPrefixes = normalizePolicyStrings(p.AllowedPathPrefixes)
	if len(p.AllowedSchemes) == 0 || len(p.AllowedHosts) == 0 {
		return fmt.Errorf("%w: schemes and hosts are required", ErrDestinationPolicy)
	}
	for _, scheme := range p.AllowedSchemes {
		if scheme != "http" && scheme != "https" {
			return fmt.Errorf("%w: unsupported scheme", ErrDestinationPolicy)
		}
	}
	for _, host := range p.AllowedHosts {
		if host == "" || strings.ContainsAny(host, "/:@?#\x00\r\n") {
			return fmt.Errorf("%w: invalid host", ErrDestinationPolicy)
		}
	}
	if len(p.AllowedPathPrefixes) == 0 {
		p.AllowedPathPrefixes = []string{"/"}
	}
	for i, prefix := range p.AllowedPathPrefixes {
		if prefix == "" || !strings.HasPrefix(prefix, "/") || strings.ContainsAny(prefix, "\x00\r\n") {
			return fmt.Errorf("%w: invalid path prefix", ErrDestinationPolicy)
		}
		p.AllowedPathPrefixes[i] = pathpkg.Clean(prefix)
	}
	if p.MaxRedirects < 0 || p.MaxRedirects > 10 {
		return fmt.Errorf("%w: redirect budget is out of bounds", ErrDestinationPolicy)
	}
	return nil
}

// AuthorizeURL validates scheme, host, and normalized path. It deliberately
// does not resolve DNS or declare private-network safety; those checks need
// the adapter's transport and resolver and cannot be proven by this value.
func (p DestinationPolicy) AuthorizeURL(value *url.URL) error {
	if err := p.Normalize(); err != nil {
		return err
	}
	if value == nil || value.User != nil || value.Hostname() == "" {
		return ErrDestinationHost
	}
	scheme := strings.ToLower(value.Scheme)
	if !containsPolicyString(p.AllowedSchemes, scheme) {
		return ErrDestinationScheme
	}
	host := strings.ToLower(value.Hostname())
	allowedHost := false
	for _, candidate := range p.AllowedHosts {
		candidate = strings.ToLower(candidate)
		if host == candidate || strings.HasSuffix(host, "."+candidate) {
			allowedHost = true
			break
		}
	}
	if !allowedHost {
		return ErrDestinationHost
	}
	path := value.EscapedPath()
	if path == "" {
		path = "/"
	}
	for _, prefix := range p.AllowedPathPrefixes {
		if path == prefix || strings.HasPrefix(path, strings.TrimRight(prefix, "/")+"/") {
			return nil
		}
	}
	return ErrDestinationPath
}

// StableHash identifies the normalized policy without including secrets or
// adapter implementation details. It is suitable for admission evidence and
// regression comparisons.
func (p DestinationPolicy) StableHash() string {
	if err := p.Normalize(); err != nil {
		return ""
	}
	payload, err := json.Marshal(p)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:])
}

func normalizePolicyStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.ToLower(strings.TrimSpace(value))
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func containsPolicyString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}
