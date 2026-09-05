package server

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func newServerAuthTest(t *testing.T, permissions []contracts.Permission) (*server, *pgxpool.Pool, string, string) {
	t.Helper()
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping pool: %v", err)
	}
	if err := store.ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatalf("migrations: %v", err)
	}
	workspaceID := fmt.Sprintf("test-server-auth-%d", time.Now().UnixNano())
	auth := store.NewAuthStore(pool)
	identity, err := auth.CreateIdentity(ctx, contracts.IdentityInput{WorkspaceID: workspaceID, Subject: "http-user", Kind: "user", Permissions: permissions})
	if err != nil {
		pool.Close()
		t.Fatalf("identity: %v", err)
	}
	_, token, err := auth.CreateAPIKey(ctx, contracts.APIKeyInput{WorkspaceID: workspaceID, IdentityID: identity.ID})
	if err != nil {
		pool.Close()
		t.Fatalf("api key: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.authorization_audit WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.api_keys WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.identity_role_bindings WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.roles WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.identities WHERE workspace_id=$1`, workspaceID)
		pool.Close()
	})
	return &server{pool: pool, auth: auth, authMode: "workspace"}, pool, workspaceID, token
}

func TestSecurityMiddlewareEnforcesWorkspaceAndAuthenticatedActor(t *testing.T) {
	srv, pool, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionModelInvoke})
	_ = pool
	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		principal, ok := principalFromRequest(r)
		if !ok {
			t.Fatal("principal missing from request context")
		}
		actor := requestActor(r)
		if principal.WorkspaceID != workspaceID || actor.ID != principal.ID || actor.WorkspaceID != workspaceID {
			t.Fatalf("principal=%+v actor=%+v", principal, actor)
		}
		w.WriteHeader(http.StatusNoContent)
	})
	handler := withRequestMiddleware(srv.securityMiddleware(next), 1<<20)

	request := httptest.NewRequest(http.MethodPost, "/v1/model/complete", strings.NewReader(`{"workspace_id":"`+workspaceID+`","actor":{"id":"spoofed"}}`))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Request-ID", "server-auth-1")
	request.Header.Set("X-Workspace-ID", workspaceID)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusNoContent {
		t.Fatalf("authorized response=%d body=%s", response.Code, response.Body.String())
	}

	foreign := httptest.NewRequest(http.MethodPost, "/v1/model/complete", strings.NewReader(`{"workspace_id":"foreign"}`))
	foreign.Header.Set("Authorization", "Bearer "+token)
	foreign.Header.Set("X-Request-ID", "server-auth-2")
	foreignResponse := httptest.NewRecorder()
	handler.ServeHTTP(foreignResponse, foreign)
	if foreignResponse.Code != http.StatusForbidden {
		t.Fatalf("cross-workspace response=%d body=%s", foreignResponse.Code, foreignResponse.Body.String())
	}
}

func TestSecurityMiddlewareDenyByDefaultAndAudit(t *testing.T) {
	srv, pool, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionModelInvoke})
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { t.Fatal("denied request reached handler") })
	handler := withRequestMiddleware(srv.securityMiddleware(next), 1<<20)
	request := httptest.NewRequest(http.MethodPost, "/v1/tools/execute", strings.NewReader(`{"workspace_id":"`+workspaceID+`"}`))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Request-ID", "server-auth-deny")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("denied response=%d body=%s", response.Code, response.Body.String())
	}
	var count int
	if err := pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.authorization_audit WHERE workspace_id=$1 AND request_id='server-auth-deny' AND decision=false`, workspaceID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("deny audit count=%d", count)
	}
}

func TestSecurityMiddlewareAuthorizesEvaluationOperatorSurface(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{
		contracts.PermissionEvaluationRead,
		contracts.PermissionEvaluationRun,
		contracts.PermissionEvaluationWrite,
	})
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := withRequestMiddleware(srv.securityMiddleware(next), 1<<20)
	cases := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/v1/evaluations/retrieval/surfaces?workspace_id=" + workspaceID, ""},
		{http.MethodPost, "/v1/evaluations/retrieval/surfaces", `{"workspace_id":"` + workspaceID + `"}`},
		{http.MethodPost, "/v1/evaluations/datasets", `{"workspace_id":"` + workspaceID + `"}`},
		{http.MethodPost, "/v1/evaluations/retrieval/runs", `{"workspace_id":"` + workspaceID + `"}`},
		{http.MethodGet, "/v1/evaluations/runs/eval-1?workspace_id=" + workspaceID, ""},
	}
	for _, item := range cases {
		request := httptest.NewRequest(item.method, item.path, strings.NewReader(item.body))
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("X-Request-ID", "evaluation-auth-"+item.method+"-"+item.path)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("authorized %s %s response=%d body=%s", item.method, item.path, response.Code, response.Body.String())
		}
	}
}

func TestSecurityMiddlewareAuthorizesWorkReceiptSurface(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{
		contracts.PermissionReceiptRead,
		contracts.PermissionReceiptWrite,
	})
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := withRequestMiddleware(srv.securityMiddleware(next), 1<<20)
	cases := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/v1/work-receipts", `{"workspace_id":"` + workspaceID + `"}`},
		{http.MethodGet, "/v1/work-receipts/receipt-1?workspace_id=" + workspaceID, ""},
		{http.MethodPost, "/v1/work-receipts/disclose", `{"workspace_id":"` + workspaceID + `","receipt_id":"receipt-1"}`},
	}
	for _, item := range cases {
		request := httptest.NewRequest(item.method, item.path, strings.NewReader(item.body))
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("X-Request-ID", "receipt-auth-"+item.method+"-"+item.path)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("authorized %s %s response=%d body=%s", item.method, item.path, response.Code, response.Body.String())
		}
	}
}

func TestSecurityMiddlewareAuthorizesValidationAndHandoffSurface(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{
		contracts.PermissionChangeRead,
		contracts.PermissionChangeValidate,
	})
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := withRequestMiddleware(srv.securityMiddleware(next), 1<<20)
	cases := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/v1/validations", `{"workspace_id":"` + workspaceID + `"}`},
		{http.MethodGet, "/v1/validations/run-1?workspace_id=" + workspaceID, ""},
		{http.MethodGet, "/v1/validations/run-1/replay?workspace_id=" + workspaceID, ""},
		{http.MethodPost, "/v1/validations/run-1/resume?workspace_id=" + workspaceID, ""},
		{http.MethodPost, "/v1/validations/disclose", `{"workspace_id":"` + workspaceID + `","validation_run_id":"run-1"}`},
		{http.MethodGet, "/v1/reindex-handoffs/handoff-1?workspace_id=" + workspaceID, ""},
		{http.MethodPost, "/v1/reindex-handoffs/handoff-1/submit?workspace_id=" + workspaceID, ""},
	}
	for _, item := range cases {
		request := httptest.NewRequest(item.method, item.path, strings.NewReader(item.body))
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("X-Request-ID", "validation-auth-"+item.method+"-"+item.path)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("authorized %s %s response=%d body=%s", item.method, item.path, response.Code, response.Body.String())
		}
	}
}
