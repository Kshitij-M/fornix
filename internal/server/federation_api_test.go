package server

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func TestFederationPeerAPIIsWorkspaceScopedRedactedAndIdempotent(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionWorkspaceRead, contracts.PermissionWorkspaceWrite})
	srv.federation = store.NewFederationStore(srv.pool, srv.events, store.NewWorkspaceCoordinationStore(srv.pool, srv.events))
	handler := withRequestMiddleware(srv.securityMiddleware(srv.routes()), 2<<20)
	body := `{"workspace_id":"` + workspaceID + `","request_id":"peer-request","idempotency_key":"peer-key","id":"peer-a","remote_workspace_id":"remote-a","endpoint_url":"https://peer.example.test/root","credential_ref":"provider/federation","max_messages":10}`
	request := httptest.NewRequest(http.MethodPost, "/v1/federation/peer", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Workspace-ID", workspaceID)
	request.Header.Set("X-Request-ID", "peer-request")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("peer registration status=%d body=%s", response.Code, response.Body.String())
	}
	if strings.Contains(response.Body.String(), "bearer") || strings.Contains(response.Body.String(), "secret") {
		t.Fatalf("peer response contains credential material: %s", response.Body.String())
	}
	var first map[string]any
	if err := json.Unmarshal(response.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if first["duplicate"] != false {
		t.Fatalf("first registration duplicate=%v", first["duplicate"])
	}
	request = httptest.NewRequest(http.MethodPost, "/v1/federation/peer", strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Workspace-ID", workspaceID)
	request.Header.Set("X-Request-ID", "peer-request")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"duplicate":true`) {
		t.Fatalf("duplicate registration status=%d body=%s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "/v1/federation/peers?workspace_id="+workspaceID, nil)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Workspace-ID", workspaceID)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), `"peer-a"`) {
		t.Fatalf("peer list status=%d body=%s", response.Code, response.Body.String())
	}

	unsafe := strings.Replace(body, `"max_messages":10`, `"bearer_token":"inline-secret"`, 1)
	request = httptest.NewRequest(http.MethodPost, "/v1/federation/peer", strings.NewReader(unsafe))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Workspace-ID", workspaceID)
	request.Header.Set("X-Request-ID", "peer-unsafe")
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("inline bearer status=%d body=%s", response.Code, response.Body.String())
	}

	request = httptest.NewRequest(http.MethodGet, "/v1/federation/peers?workspace_id=foreign", nil)
	request.Header.Set("Authorization", "Bearer "+token)
	response = httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("cross-workspace peer list status=%d body=%s", response.Code, response.Body.String())
	}
}
