package model

import (
	"fmt"
	"net/http"
	"net/url"
	pathpkg "path"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

func providerHTTPClient(endpoint contracts.ModelEndpoint, timeout time.Duration, supplied *http.Client, requestBytes, responseBytes int64) (*http.Client, error) {
	policy, err := providerEgressPolicy(endpoint, timeout, requestBytes, responseBytes)
	if err != nil {
		return nil, err
	}
	return connector.NewEgressClient(policy, supplied, connector.EgressOptions{})
}

func providerEgressPolicy(endpoint contracts.ModelEndpoint, timeout time.Duration, requestBytes, responseBytes int64) (connector.EgressPolicy, error) {
	destination, err := providerDestinationPolicy(endpoint.BaseURL, endpoint.AllowPrivate)
	if err != nil {
		return connector.EgressPolicy{}, err
	}
	policy := connector.EgressPolicy{
		Destination:      destination,
		MaxRequestBytes:  requestBytes,
		MaxResponseBytes: responseBytes,
		Timeout:          timeout,
	}
	if err := policy.Normalize(); err != nil {
		return connector.EgressPolicy{}, err
	}
	return policy, nil
}

func providerBoundaryAuthority(endpoint contracts.ModelEndpoint, timeout time.Duration, requestBytes, responseBytes int64) (contracts.ExternalBoundaryAuthority, error) {
	policy, err := providerEgressPolicy(endpoint, timeout, requestBytes, responseBytes)
	if err != nil {
		return contracts.ExternalBoundaryAuthority{}, err
	}
	egressHash := policy.StableHash()
	destinationHash := policy.Destination.StableHash()
	if egressHash == "" || destinationHash == "" {
		return contracts.ExternalBoundaryAuthority{}, connector.ErrDestinationPolicy
	}
	return contracts.ExternalBoundaryAuthority{
		SchemaVersion:         contracts.ExternalBoundarySchemaVersion,
		EgressPolicyHash:      egressHash,
		DestinationPolicyHash: destinationHash,
		NetworkBoundary:       contracts.NetworkBoundaryControlledTransport,
		NetworkBoundaryHash:   contracts.HashStrings("fornix-network-boundary", contracts.NetworkBoundaryControlledTransport, egressHash, destinationHash),
	}, nil
}

func providerDestinationPolicy(raw string, allowPrivate bool) (connector.DestinationPolicy, error) {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || parsed.Scheme == "" || parsed.Hostname() == "" {
		return connector.DestinationPolicy{}, fmt.Errorf("provider destination: %w", connector.ErrDestinationPolicy)
	}
	basePath := pathpkg.Clean(parsed.Path)
	if basePath == "." || basePath == "" {
		basePath = "/"
	}
	policy := connector.DestinationPolicy{
		AllowedSchemes:       []string{strings.ToLower(parsed.Scheme)},
		AllowedHosts:         []string{strings.ToLower(parsed.Hostname())},
		AllowedPathPrefixes:  []string{basePath},
		AllowPrivateNetworks: allowPrivate,
		AllowRedirects:       false,
	}
	if err := policy.Normalize(); err != nil {
		return connector.DestinationPolicy{}, err
	}
	return policy, nil
}
