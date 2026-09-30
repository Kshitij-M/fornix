package credentials

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/testutil"
)

func TestManagedResolverCarriesWorkspaceAndNeverReturnsManagerError(t *testing.T) {
	ref, err := ParseRef("provider/openai")
	if err != nil {
		t.Fatal(err)
	}
	wantSecret := "manager-secret-do-not-leak"
	manager := SecretManagerFunc(func(_ context.Context, request ResolveRequest) (ResolvedSecret, error) {
		if request.WorkspaceID != "workspace-a" || request.Provider != "openai" || request.Purpose != "model:openai" {
			t.Fatalf("unexpected manager request: %+v", request)
		}
		return ResolvedSecret{}, errors.New("manager rejected " + wantSecret)
	})
	resolver := NewManagedSecretResolver(manager)
	_, err = resolver.Resolve(context.Background(), "workspace-a", ref, "model:openai")
	if !errors.Is(err, ErrManagedUnavailable) {
		t.Fatalf("resolve error=%v, want unavailable", err)
	}
	if strings.Contains(err.Error(), wantSecret) {
		t.Fatalf("manager secret leaked through error: %v", err)
	}
}

func TestManagedResolverClearsPartialSecretReturnedWithError(t *testing.T) {
	ref, err := ParseRef("provider/openai")
	if err != nil {
		t.Fatal(err)
	}
	partial, err := NewSecret([]byte("partial-secret"))
	if err != nil {
		t.Fatal(err)
	}
	manager := SecretManagerFunc(func(context.Context, ResolveRequest) (ResolvedSecret, error) {
		return ResolvedSecret{Secret: partial, Version: "vault-v1"}, errors.New("manager failed")
	})
	_, err = NewManagedSecretResolver(manager).ResolveVersioned(context.Background(), "workspace-a", ref, "model:openai")
	for _, value := range partial.Bytes() {
		if value != 0 {
			t.Fatalf("partial manager secret bytes were not zeroed: err=%v", err)
		}
	}
	if !errors.Is(err, ErrManagedUnavailable) {
		t.Fatalf("partial manager failure was not classified safely: err=%v", err)
	}
}

func TestDecodeManagerResponseReturnsOwnedSecretBytes(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("resolved-secret"))
	body := []byte(`{"secret_base64":"` + encoded + `","version":"vault-v2"}`)
	resolved, err := decodeManagerResponse(body)
	if err != nil {
		t.Fatal(err)
	}
	defer resolved.Clear()
	if got := string(resolved.Secret.Bytes()); got != "resolved-secret" || resolved.Version != "vault-v2" {
		t.Fatalf("decoded managed credential = version %q, secret length %d", resolved.Version, resolved.Secret.Len())
	}
}

func TestDecodeManagerResponseRejectsMalformedSecretEncoding(t *testing.T) {
	if _, err := decodeManagerResponse([]byte(`{"secret_base64":"not base64!","version":"vault-v2"}`)); !errors.Is(err, ErrManagedResponse) {
		t.Fatalf("malformed secret encoding error = %v, want managed response error", err)
	}
}

func TestDecodeManagerResponseRejectsMalformedMetadataAfterSecretField(t *testing.T) {
	encoded := base64.StdEncoding.EncodeToString([]byte("temporary-secret"))
	body := []byte(`{"secret_base64":"` + encoded + `","version":"vault-v2","expires_at":false}`)
	if _, err := decodeManagerResponse(body); !errors.Is(err, ErrManagedResponse) {
		t.Fatalf("malformed metadata error = %v, want managed response error", err)
	}
}

func TestHTTPSecretManagerUsesMetadataOnlyRequestAndRejectsRedirects(t *testing.T) {
	testutil.RequireLocalHTTP(t)
	secret := []byte("external-secret")
	var requestBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requestBody = string(body)
		if r.Header.Get("Authorization") != "" {
			t.Errorf("unexpected authorization header without token source")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"secret_base64":"` + base64.StdEncoding.EncodeToString(secret) + `","version":"vault-v7","expires_at":"2030-01-01T00:00:00Z"}`))
	}))
	defer server.Close()
	manager, err := NewHTTPSecretManager(HTTPSecretManagerConfig{Endpoint: server.URL, Client: server.Client(), Timeout: time.Second, AllowPrivateNetworks: true})
	if err != nil {
		t.Fatalf("construct manager: %v", err)
	}
	ref, err := ParseRef("provider/openai")
	if err != nil {
		t.Fatal(err)
	}
	resolved, err := manager.Resolve(context.Background(), ResolveRequest{WorkspaceID: "workspace-a", Provider: "openai", Reference: ref, Purpose: "model:openai"})
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if string(resolved.Secret.Bytes()) != string(secret) || resolved.Version != "vault-v7" {
		t.Fatalf("unexpected resolved secret metadata: version=%q length=%d", resolved.Version, resolved.Secret.Len())
	}
	resolved.Clear()
	if strings.Contains(requestBody, string(secret)) {
		t.Fatalf("secret appeared in manager request: %s", requestBody)
	}

	redirect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Location", server.URL)
		w.WriteHeader(http.StatusFound)
	}))
	defer redirect.Close()
	redirectManager, err := NewHTTPSecretManager(HTTPSecretManagerConfig{Endpoint: redirect.URL, Client: redirect.Client(), Timeout: time.Second, AllowPrivateNetworks: true})
	if err != nil {
		t.Fatalf("construct redirect manager: %v", err)
	}
	if _, err := redirectManager.Resolve(context.Background(), ResolveRequest{WorkspaceID: "workspace-a", Provider: "openai", Reference: ref, Purpose: "model:openai"}); !errors.Is(err, ErrManagedUnavailable) {
		t.Fatalf("redirect error=%v, want unavailable", err)
	}
}

func TestManagedResolverRejectsExpiredOrUnversionedSources(t *testing.T) {
	ref, err := ParseRef("provider/openai")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := NewSecret([]byte("secret"))
	if err != nil {
		t.Fatal(err)
	}
	for name, response := range map[string]ResolvedSecret{
		"missing version":           {Secret: secret},
		"expired":                   {Secret: secret, Version: "v1", ExpiresAt: time.Now().UTC().Add(-time.Second)},
		"control character version": {Secret: secret, Version: "v1\nlog-injection", ExpiresAt: time.Now().UTC().Add(time.Minute)},
	} {
		t.Run(name, func(t *testing.T) {
			resolver := NewManagedSecretResolver(SecretManagerFunc(func(context.Context, ResolveRequest) (ResolvedSecret, error) {
				copySecret, copyErr := NewSecret(response.Secret.Bytes())
				if copyErr != nil {
					t.Fatal(copyErr)
				}
				response.Secret = copySecret
				return response, nil
			}))
			if _, err := resolver.Resolve(context.Background(), "workspace-a", ref, "model:openai"); !errors.Is(err, ErrManagedVersion) && !errors.Is(err, ErrManagedExpired) {
				t.Fatalf("error=%v, want version or expiry", err)
			}
		})
	}
	secret.Clear()
}
