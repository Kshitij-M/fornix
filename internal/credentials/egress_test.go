package credentials

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
)

type credentialRoundTripper func(*http.Request) (*http.Response, error)

func (f credentialRoundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func TestDoCredentialRequestClearsAuthorizationAfterTransport(t *testing.T) {
	var transported *http.Request
	client := &http.Client{Transport: credentialRoundTripper(func(request *http.Request) (*http.Response, error) {
		transported = request
		if request.Header.Get("Authorization") != "Bearer ephemeral" {
			t.Fatal("authorization header missing at transport boundary")
		}
		return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader("ok")), Request: request}, nil
	})}
	request, err := http.NewRequest(http.MethodGet, "https://example.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer ephemeral")

	response, err := DoCredentialRequest(client, request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if transported == nil || transported.Header.Get("Authorization") != "" {
		t.Fatal("transported request retained its authorization header after Do returned")
	}
}

func TestDoCredentialRequestClearsAuthorizationWhenTransportFails(t *testing.T) {
	var transported *http.Request
	wantErr := errors.New("transport failed")
	client := &http.Client{Transport: credentialRoundTripper(func(request *http.Request) (*http.Response, error) {
		transported = request
		if request.Header.Get("Authorization") != "Bearer ephemeral" {
			t.Fatal("authorization header missing at transport boundary")
		}
		return nil, wantErr
	})}
	request, err := http.NewRequest(http.MethodGet, "https://example.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer ephemeral")
	if _, err := DoCredentialRequest(client, request); !errors.Is(err, wantErr) {
		t.Fatalf("transport error = %v, want injected failure", err)
	}
	if transported == nil || transported.Header.Get("Authorization") != "" {
		t.Fatal("failed transport retained its authorization header")
	}
}
