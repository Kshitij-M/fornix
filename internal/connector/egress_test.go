package connector

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/testutil"
)

func testEgressPolicy(host string, private, redirects bool) EgressPolicy {
	maxRedirects := 0
	if redirects {
		maxRedirects = 2
	}
	return EgressPolicy{
		Destination: DestinationPolicy{
			AllowedSchemes:       []string{"http"},
			AllowedHosts:         []string{host},
			AllowedPathPrefixes:  []string{"/"},
			AllowPrivateNetworks: private,
			AllowRedirects:       redirects,
			MaxRedirects:         maxRedirects,
		},
		MaxRequestBytes:  3,
		MaxResponseBytes: 3,
		Timeout:          time.Second,
	}
}

func TestEgressClientEnforcesRequestAndResponseBudgets(t *testing.T) {
	testutil.RequireLocalHTTP(t)
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/request" {
			_, _ = io.ReadAll(request.Body)
			return
		}
		_, _ = writer.Write([]byte("1234"))
	}))
	defer server.Close()
	parsed, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewEgressClient(testEgressPolicy(parsed.Hostname(), true, false), nil, EgressOptions{})
	if err != nil {
		t.Fatal(err)
	}

	request, err := http.NewRequest(http.MethodPost, server.URL+"/request", strings.NewReader("1234"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Do(request); !errors.Is(err, ErrEgressRequest) {
		t.Fatalf("oversized request error = %v, want ErrEgressRequest", err)
	}

	response, err := client.Get(server.URL + "/response")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if _, err := io.ReadAll(response.Body); !errors.Is(err, ErrEgressResponse) {
		t.Fatalf("oversized response error = %v, want ErrEgressResponse", err)
	}
}

func TestEgressClientRejectsPrivateDNSResolution(t *testing.T) {
	client, err := NewEgressClient(testEgressPolicy("public.example.test", false, false), nil, EgressOptions{
		Resolver: IPResolverFunc(func(context.Context, string, string) ([]net.IP, error) {
			return []net.IP{net.ParseIP("127.0.0.1")}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := http.NewRequest(http.MethodGet, "http://public.example.test/", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Do(request); !errors.Is(err, ErrDestinationHost) {
		t.Fatalf("private DNS error = %v, want ErrDestinationHost", err)
	}
}

func TestEgressClientRejectsRedirectsOutsidePolicy(t *testing.T) {
	testutil.RequireLocalHTTP(t)
	target := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		_, _ = writer.Write([]byte("target"))
	}))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL, http.StatusFound)
	}))
	defer source.Close()
	host, err := url.Parse(source.URL)
	if err != nil {
		t.Fatal(err)
	}
	client, err := NewEgressClient(testEgressPolicy(host.Hostname(), true, false), nil, EgressOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.Get(source.URL); !errors.Is(err, ErrEgressRedirect) {
		t.Fatalf("redirect error = %v, want ErrEgressRedirect", err)
	}
}

func TestEgressClientRejectsUninspectableTransport(t *testing.T) {
	policy := testEgressPolicy("example.test", false, false)
	_, err := NewEgressClient(policy, &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) { return nil, nil })}, EgressOptions{})
	if !errors.Is(err, ErrEgressTransport) {
		t.Fatalf("custom transport error = %v, want ErrEgressTransport", err)
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(request *http.Request) (*http.Response, error) { return f(request) }
