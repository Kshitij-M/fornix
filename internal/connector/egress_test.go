package connector_test

import (
	"errors"
	"net/url"
	"testing"

	"github.com/omaveda/fornix/internal/connector"
)

func TestDestinationPolicyAuthorizesOnlyConfiguredOriginSurface(t *testing.T) {
	policy := connector.DestinationPolicy{
		AllowedSchemes:      []string{"HTTPS", "https"},
		AllowedHosts:        []string{"api.example.com"},
		AllowedPathPrefixes: []string{"/v1", "/v1"},
	}
	if err := policy.Normalize(); err != nil {
		t.Fatalf("normalize policy: %v", err)
	}

	for _, target := range []string{
		"https://api.example.com/v1/items",
		"https://child.api.example.com/v1/items",
		"https://api.example.com/v1",
	} {
		parsed, err := url.Parse(target)
		if err != nil {
			t.Fatalf("parse %s: %v", target, err)
		}
		if err := policy.AuthorizeURL(parsed); err != nil {
			t.Errorf("expected %s to be authorized: %v", target, err)
		}
	}

	for _, test := range []struct {
		name   string
		target string
		want   error
	}{
		{name: "scheme", target: "http://api.example.com/v1/items", want: connector.ErrDestinationScheme},
		{name: "host", target: "https://other.example.com/v1/items", want: connector.ErrDestinationHost},
		{name: "path", target: "https://api.example.com/admin", want: connector.ErrDestinationPath},
		{name: "credentials", target: "https://user:pass@api.example.com/v1/items", want: connector.ErrDestinationHost},
	} {
		t.Run(test.name, func(t *testing.T) {
			parsed, err := url.Parse(test.target)
			if err != nil {
				t.Fatalf("parse %s: %v", test.target, err)
			}
			if err := policy.AuthorizeURL(parsed); !errors.Is(err, test.want) {
				t.Fatalf("expected %v, got %v", test.want, err)
			}
		})
	}
}

func TestDestinationPolicyRejectsUnboundedConfiguration(t *testing.T) {
	tests := []connector.DestinationPolicy{
		{AllowedSchemes: []string{"file"}, AllowedHosts: []string{"example.com"}},
		{AllowedSchemes: []string{"https"}},
		{AllowedSchemes: []string{"https"}, AllowedHosts: []string{"example.com"}, AllowedPathPrefixes: []string{"relative"}},
		{AllowedSchemes: []string{"https"}, AllowedHosts: []string{"example.com"}, MaxRedirects: 11},
	}
	for index, policy := range tests {
		if err := policy.Normalize(); !errors.Is(err, connector.ErrDestinationPolicy) {
			t.Errorf("case %d: expected policy error, got %v", index, err)
		}
	}
}

func TestDestinationPolicyStableHashIsOrderIndependent(t *testing.T) {
	first := connector.DestinationPolicy{
		AllowedSchemes:      []string{"https", "http"},
		AllowedHosts:        []string{"b.example.com", "a.example.com"},
		AllowedPathPrefixes: []string{"/v2", "/v1"},
		AllowRedirects:      true,
		MaxRedirects:        2,
	}
	second := connector.DestinationPolicy{
		AllowedSchemes:      []string{"http", "https"},
		AllowedHosts:        []string{"a.example.com", "b.example.com"},
		AllowedPathPrefixes: []string{"/v1", "/v2"},
		AllowRedirects:      true,
		MaxRedirects:        2,
	}
	if err := first.Normalize(); err != nil {
		t.Fatal(err)
	}
	if err := second.Normalize(); err != nil {
		t.Fatal(err)
	}
	if first.StableHash() != second.StableHash() {
		t.Fatalf("expected equivalent policies to have the same hash: %s != %s", first.StableHash(), second.StableHash())
	}
}
