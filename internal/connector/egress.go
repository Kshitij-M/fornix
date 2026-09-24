package connector

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	pathpkg "path"
	"sort"
	"strings"
)

var (
	ErrDestinationScheme = errors.New("destination scheme is not allowed")
	ErrDestinationHost   = errors.New("destination host is not allowed")
	ErrDestinationPath   = errors.New("destination path is not allowed")
	ErrDestinationPolicy = errors.New("destination policy is invalid")
)

// DestinationPolicy is the reusable, secret-free part of outbound connector
// admission. Adapters must still enforce transport-specific controls such as
// DNS rebinding resistance, private-network resolution, SQL read-only modes,
// or provider-specific redirect behavior at their actual I/O boundary.
type DestinationPolicy struct {
	AllowedSchemes       []string `json:"allowed_schemes"`
	AllowedHosts         []string `json:"allowed_hosts"`
	AllowedPathPrefixes  []string `json:"allowed_path_prefixes"`
	AllowPrivateNetworks bool     `json:"allow_private_networks"`
	AllowRedirects       bool     `json:"allow_redirects"`
	MaxRedirects         int      `json:"max_redirects"`
}

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
