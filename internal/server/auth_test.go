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
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_leases WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_transitions WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_links WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_resources WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_callbacks WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_effects WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_attempts WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_results WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_authority_links WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_admission_decisions WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operation_idempotency WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.operations WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.authorization_audit WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.workspace_federation_poll_attempts WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.workspace_federation_peer_leases WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.workspace_federation_peer_commands WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.workspace_federation_peers WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.workspace_coordination_messages WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.api_keys WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.identity_role_bindings WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.roles WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.identities WHERE workspace_id=$1`, workspaceID)
		pool.Close()
	})
	events := store.NewEventStore(pool)
	return &server{pool: pool, events: events, operations: store.NewOperationStore(pool, events), admission: store.NewAdmissionStore(pool, events), auth: auth, authMode: "workspace"}, pool, workspaceID, token
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

func TestSecurityMiddlewareDoesNotReplayAuthorizationAfterRoleRevocation(t *testing.T) {
	srv, pool, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionTaskRead})
	var handlerCalls int
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		handlerCalls++
		w.WriteHeader(http.StatusNoContent)
	})
	handler := withRequestMiddleware(srv.securityMiddleware(next), 1<<20)
	request := func() *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodGet, "/v1/tasks?workspace_id="+workspaceID, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-Request-ID", "authorization-replay-after-revoke")
		r.Header.Set("X-Workspace-ID", workspaceID)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, r)
		return response
	}
	if response := request(); response.Code != http.StatusNoContent {
		t.Fatalf("initial authorized request=%d body=%s", response.Code, response.Body.String())
	}
	principal, err := srv.auth.Authenticate(context.Background(), token)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(context.Background(), `DELETE FROM fornix.identity_role_bindings WHERE workspace_id=$1 AND identity_id=$2`, workspaceID, principal.ID); err != nil {
		t.Fatalf("revoke role binding: %v", err)
	}
	if response := request(); response.Code != http.StatusForbidden {
		t.Fatalf("request after role revocation=%d body=%s, want forbidden", response.Code, response.Body.String())
	}
	if handlerCalls != 1 {
		t.Fatalf("protected handler calls=%d after permission revocation, want 1", handlerCalls)
	}
	var auditRows, denialRows int
	if err := pool.QueryRow(context.Background(), `
		SELECT count(*), count(*) FILTER (WHERE decision=false)
		FROM fornix.authorization_audit
		WHERE workspace_id=$1 AND request_id='authorization-replay-after-revoke'`, workspaceID).Scan(&auditRows, &denialRows); err != nil {
		t.Fatal(err)
	}
	if auditRows != 2 || denialRows != 1 {
		t.Fatalf("authorization replay audit rows=%d denials=%d, want 2 and 1", auditRows, denialRows)
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

func TestSecurityMiddlewareRejectsUnknownAndLegacyGlobalRoutes(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionWorkspaceRead})
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { t.Fatal("unreviewed route reached handler") })
	handler := withRequestMiddleware(srv.securityMiddleware(next), 1<<20)
	for _, path := range []string{
		"/v1/not-reviewed?workspace_id=" + workspaceID,
	} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("X-Request-ID", "route-deny-"+strings.ReplaceAll(path, "/", "-"))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNotFound {
			t.Fatalf("unreviewed path %s response=%d body=%s", path, response.Code, response.Body.String())
		}
	}
}

func TestSecurityMiddlewareAllowsWorkspaceFederationRoutes(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionWorkspaceRead, contracts.PermissionWorkspaceWrite})
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := withRequestMiddleware(srv.securityMiddleware(next), 1<<20)
	cases := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/v1/federation/peers?workspace_id=" + workspaceID, ""},
		{http.MethodPost, "/v1/federation/peer", `{"workspace_id":"` + workspaceID + `"}`},
		{http.MethodPost, "/v1/federation/poll/reconcile", `{"workspace_id":"` + workspaceID + `","attempt_id":"attempt","owner_id":"owner","response_payload":{"messages":[]}}`},
		{http.MethodGet, "/v1/federation/legacy-quarantine?workspace_id=" + workspaceID, ""},
		{http.MethodPost, "/v1/federation/legacy-quarantine", `{"audit_workspace_id":"` + workspaceID + `","request_id":"request","idempotency_key":"key","reason":"qualification"}`},
	}
	for _, item := range cases {
		request := httptest.NewRequest(item.method, item.path, strings.NewReader(item.body))
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("X-Request-ID", "route-federation-"+strings.ReplaceAll(item.path, "/", "-"))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("workspace federation path %s response=%d body=%s", item.path, response.Code, response.Body.String())
		}
	}
}

func TestQualificationRoutesUseSeparateTrustPermissions(t *testing.T) {
	cases := []struct {
		method string
		path   string
		want   contracts.Permission
	}{
		{http.MethodGet, "/v1/qualification/signers?workspace_id=w&deployment_id=d", contracts.PermissionQualificationRead},
		{http.MethodPost, "/v1/qualification/signers", contracts.PermissionQualificationAdmin},
		{http.MethodPost, "/v1/qualification/signers/key-1/revoke?workspace_id=w&deployment_id=d", contracts.PermissionQualificationAdmin},
		{http.MethodPost, "/v1/qualification/import", contracts.PermissionQualificationImport},
		{http.MethodGet, "/v1/qualification/imports?workspace_id=w&deployment_id=d", contracts.PermissionQualificationRead},
		{http.MethodGet, "/v1/qualification/imports/import-1?workspace_id=w&deployment_id=d", contracts.PermissionQualificationRead},
		{http.MethodGet, "/v1/qualification/snapshots?workspace_id=w&deployment_id=d", contracts.PermissionQualificationRead},
		{http.MethodGet, "/v1/qualification/readiness/snapshots?workspace_id=w&deployment_id=d&release_id=r", contracts.PermissionQualificationRead},
		{http.MethodPost, "/v1/qualification/readiness/snapshots", contracts.PermissionQualificationAdmin},
		{http.MethodPost, "/v1/qualification/readiness/snapshots/snapshot-1/annotations", contracts.PermissionQualificationAdmin},
		{http.MethodPost, "/v1/qualification/readiness/policies", contracts.PermissionQualificationAdmin},
		{http.MethodGet, "/v1/qualification/readiness/policies/current?workspace_id=w&deployment_id=d", contracts.PermissionQualificationRead},
		{http.MethodPost, "/v1/qualification/readiness/review", contracts.PermissionQualificationRead},
		{http.MethodGet, "/v1/qualification/readiness/retention/policies?workspace_id=w&deployment_id=d", contracts.PermissionQualificationRead},
		{http.MethodPost, "/v1/qualification/readiness/retention/policies", contracts.PermissionQualificationAdmin},
		{http.MethodPost, "/v1/qualification/readiness/retention/metadata/sync", contracts.PermissionQualificationAdmin},
		{http.MethodPost, "/v1/qualification/readiness/retention/plan", contracts.PermissionQualificationRead},
		{http.MethodPost, "/v1/qualification/readiness/retention/recovery", contracts.PermissionQualificationRead},
		{http.MethodGet, "/v1/qualification/snapshots/snapshot-1?workspace_id=w&deployment_id=d", contracts.PermissionQualificationRead},
		{http.MethodPost, "/v1/qualification/snapshots", contracts.PermissionQualificationAdmin},
		{http.MethodPost, "/v1/qualification/snapshots/snapshot-1/revoke?workspace_id=w&deployment_id=d", contracts.PermissionQualificationAdmin},
		{http.MethodGet, "/v1/qualification/releases?workspace_id=w&deployment_id=d", contracts.PermissionQualificationRead},
		{http.MethodPost, "/v1/qualification/releases", contracts.PermissionQualificationAdmin},
		{http.MethodGet, "/v1/qualification/releases/release-1?workspace_id=w&deployment_id=d", contracts.PermissionQualificationRead},
		{http.MethodGet, "/v1/qualification/releases/release-1/evidence?workspace_id=w&deployment_id=d", contracts.PermissionQualificationRead},
		{http.MethodPost, "/v1/qualification/releases/release-1/evidence", contracts.PermissionQualificationAdmin},
		{http.MethodPost, "/v1/qualification/releases/release-1/evidence/link-1/revoke", contracts.PermissionQualificationAdmin},
		{http.MethodGet, "/v1/qualification/releases/release-1/gate?workspace_id=w&deployment_id=d", contracts.PermissionQualificationRead},
		{http.MethodGet, "/v1/qualification/releases/release-1/verification?workspace_id=w&deployment_id=d&artifact_kind=release", contracts.PermissionQualificationRead},
		{http.MethodPost, "/v1/qualification/releases/release-1/verification", contracts.PermissionQualificationAdmin},
		{http.MethodPost, "/v1/qualification/releases/release-1/verification/revoke?workspace_id=w&deployment_id=d&artifact_kind=release", contracts.PermissionQualificationAdmin},
		{http.MethodGet, "/v1/qualification/releases/release-1/admission?workspace_id=w&deployment_id=d&artifact_kind=release", contracts.PermissionQualificationRead},
		{http.MethodPost, "/v1/qualification/releases/release-1/refresh", contracts.PermissionQualificationAdmin},
		{http.MethodPost, "/v1/qualification/releases/release-1/refresh/plan", contracts.PermissionQualificationRead},
		{http.MethodGet, "/v1/qualification/releases/release-1/refreshes?workspace_id=w&deployment_id=d", contracts.PermissionQualificationRead},
		{http.MethodGet, "/v1/qualification/releases/release-1/refreshes/refresh-1?workspace_id=w&deployment_id=d", contracts.PermissionQualificationRead},
		{http.MethodGet, "/v1/qualification/refresh-schedules?workspace_id=w&deployment_id=d", contracts.PermissionQualificationRead},
		{http.MethodPost, "/v1/qualification/refresh-schedules", contracts.PermissionQualificationAdmin},
		{http.MethodGet, "/v1/qualification/refresh-schedules/plan?workspace_id=w&deployment_id=d", contracts.PermissionQualificationRead},
		{http.MethodPost, "/v1/qualification/refresh-schedules/claim", contracts.PermissionQualificationAdmin},
		{http.MethodGet, "/v1/qualification/refresh-schedules/schedule-1/attempts?workspace_id=w", contracts.PermissionQualificationRead},
		{http.MethodPost, "/v1/qualification/refresh-schedules/schedule-1/complete", contracts.PermissionQualificationAdmin},
	}
	for _, item := range cases {
		r := httptest.NewRequest(item.method, item.path, nil)
		if got := permissionForRequest(r); got != item.want {
			t.Errorf("permissionForRequest(%s %s)=%q, want %q", item.method, item.path, got, item.want)
		}
	}
}

func TestGenericWorkflowRoutesUseScopedPermissions(t *testing.T) {
	cases := []struct {
		method string
		path   string
		want   contracts.Permission
	}{
		{http.MethodPost, "/v1/workflows", contracts.PermissionOperationCreate},
		{http.MethodGet, "/v1/workflows/run-1", contracts.PermissionOperationRead},
		{http.MethodPost, "/v1/workflows/run-1/lease", contracts.PermissionOperationExecute},
		{http.MethodPost, "/v1/workflows/run-1/advance", contracts.PermissionOperationExecute},
		{http.MethodPost, "/v1/workflows/run-1/approve", contracts.PermissionToolApprove},
		{http.MethodPost, "/v1/workflows/run-1/verify", contracts.PermissionOperationExecute},
		{http.MethodPost, "/v1/workflows/run-1/replay", contracts.PermissionOperationRead},
		{http.MethodPost, "/v1/tools/recovery", contracts.PermissionOperationExecute},
		{http.MethodGet, "/v1/workflows/run-1/receipt", contracts.PermissionReceiptRead},
		{http.MethodPost, "/v1/workflows/run-1/receipt", contracts.PermissionReceiptWrite},
	}
	for _, item := range cases {
		r := httptest.NewRequest(item.method, item.path, nil)
		if got := permissionForRequest(r); got != item.want {
			t.Errorf("permissionForRequest(%s %s)=%q, want %q", item.method, item.path, got, item.want)
		}
	}
}

func TestToolRecoveryRequiresOperationExecutionPermission(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionOperationRead})
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("tool recovery route reached without operation:execute")
	})
	handler := withRequestMiddleware(srv.securityMiddleware(next), 1<<20)
	request := httptest.NewRequest(http.MethodPost, "/v1/tools/recovery?workspace_id="+workspaceID, strings.NewReader(`{"tool_run_id":"tool-run"}`))
	request.Header.Set("Authorization", "Bearer "+token)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("tool recovery authorization=%d body=%s, want forbidden", response.Code, response.Body.String())
	}
}

func TestSecurityMiddlewareRejectsLegacyGlobalRoutesWithoutDedicatedPermission(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionWorkspaceRead, contracts.PermissionIdentityAdmin})
	srv.legacyGlobalSurfaces = true
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Fatal("legacy route reached handler without dedicated permission")
	})
	handler := withRequestMiddleware(srv.securityMiddleware(next), 1<<20)
	for _, path := range []string{
		"/v1/federation/coord/import?workspace_id=" + workspaceID,
	} {
		request := httptest.NewRequest(http.MethodGet, path, nil)
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("X-Request-ID", "legacy-permission-deny-"+strings.ReplaceAll(path, "/", "-"))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusForbidden {
			t.Fatalf("legacy path %s response=%d body=%s", path, response.Code, response.Body.String())
		}
	}
}

func TestSecurityMiddlewareAllowsWorkspaceScopedCoordinationAndRouterRoutes(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionWorkspaceRead, contracts.PermissionWorkspaceWrite})
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := withRequestMiddleware(srv.securityMiddleware(next), 1<<20)
	cases := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/v1/coord/recent?workspace_id=" + workspaceID, ""},
		{http.MethodPost, "/v1/coord", `{"workspace_id":"` + workspaceID + `","sender":"a","recipient":"b","subject":"s"}`},
		{http.MethodGet, "/v1/router/recommend?workspace_id=" + workspaceID + "&category=review", ""},
		{http.MethodPost, "/v1/router/observation", `{"workspace_id":"` + workspaceID + `","task_category":"review","model_id":"fake","cost_usd":0.01,"latency_ms":10,"outcome":"success"}`},
	}
	for _, item := range cases {
		request := httptest.NewRequest(item.method, item.path, strings.NewReader(item.body))
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("X-Request-ID", "scoped-route-"+item.method+"-"+strings.ReplaceAll(item.path, "/", "-"))
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("scoped path %s %s response=%d body=%s", item.method, item.path, response.Code, response.Body.String())
		}
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

func TestSecurityMiddlewareAuthorizesGenericOperationSurface(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{
		contracts.PermissionOperationRead,
		contracts.PermissionOperationCreate,
		contracts.PermissionOperationExecute,
	})
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := withRequestMiddleware(srv.securityMiddleware(next), 1<<20)
	cases := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodPost, "/v1/operations", `{"workspace_id":"` + workspaceID + `"}`},
		{http.MethodGet, "/v1/operations/op-1?workspace_id=" + workspaceID, ""},
		{http.MethodPost, "/v1/operations/op-1/lease?workspace_id=" + workspaceID, `{}`},
		{http.MethodPost, "/v1/operations/op-1/transition?workspace_id=" + workspaceID, `{"to_status":"planned","idempotency_key":"operation-auth"}`},
		{http.MethodPost, "/v1/operations/op-1/replay?workspace_id=" + workspaceID, `{}`},
	}
	for _, item := range cases {
		request := httptest.NewRequest(item.method, item.path, strings.NewReader(item.body))
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("X-Request-ID", "operation-auth-"+item.method+"-"+item.path)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("authorized %s %s response=%d body=%s", item.method, item.path, response.Code, response.Body.String())
		}
	}
}

func TestSecurityMiddlewareDeniesGenericOperationWithoutExecutionCapability(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionOperationRead})
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { t.Fatal("denied operation mutation reached handler") })
	handler := withRequestMiddleware(srv.securityMiddleware(next), 1<<20)
	request := httptest.NewRequest(http.MethodPost, "/v1/operations/op-1/lease?workspace_id="+workspaceID, strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Request-ID", "operation-auth-deny")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("operation mutation response=%d body=%s", response.Code, response.Body.String())
	}
}

func TestSecurityMiddlewareAuthorizesPolicySurfaceAndSeparatesCapabilities(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{
		contracts.PermissionPolicyRead,
		contracts.PermissionPolicyCreate,
		contracts.PermissionPolicyActivate,
		contracts.PermissionPolicyRetire,
		contracts.PermissionPolicyResolve,
		contracts.PermissionPolicyCompare,
	})
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	handler := withRequestMiddleware(srv.securityMiddleware(next), 1<<20)
	cases := []struct {
		method string
		path   string
		body   string
	}{
		{http.MethodGet, "/v1/policies?workspace_id=" + workspaceID, ""},
		{http.MethodPost, "/v1/policies", `{"workspace_id":"` + workspaceID + `","idempotency_key":"policy-auth","pack":{"policy_id":"safe","version":"1"}}`},
		{http.MethodGet, "/v1/policies/safe/1?workspace_id=" + workspaceID, ""},
		{http.MethodPost, "/v1/policies/safe/1/activate", `{"workspace_id":"` + workspaceID + `"}`},
		{http.MethodPost, "/v1/policies/safe/1/default", `{"workspace_id":"` + workspaceID + `"}`},
		{http.MethodPost, "/v1/policies/safe/1/retire", `{"workspace_id":"` + workspaceID + `"}`},
		{http.MethodPost, "/v1/policies/resolve", `{"workspace_id":"` + workspaceID + `"}`},
		{http.MethodPost, "/v1/policies/dry-run-resolve", `{"workspace_id":"` + workspaceID + `"}`},
		{http.MethodPost, "/v1/policies/compare", `{"workspace_id":"` + workspaceID + `","left":{"workspace_id":"` + workspaceID + `","policy_id":"safe","version":"1"},"right":{"workspace_id":"` + workspaceID + `","policy_id":"safe","version":"1"}}`},
		{http.MethodGet, "/v1/policies/audit?workspace_id=" + workspaceID, ""},
	}
	for _, item := range cases {
		request := httptest.NewRequest(item.method, item.path, strings.NewReader(item.body))
		request.Header.Set("Authorization", "Bearer "+token)
		request.Header.Set("X-Request-ID", "policy-auth-"+item.method+"-"+item.path)
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("authorized %s %s response=%d body=%s", item.method, item.path, response.Code, response.Body.String())
		}
	}
}

func TestSecurityMiddlewareDeniesPolicyMutationWithoutPolicyCapability(t *testing.T) {
	srv, _, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionPolicyRead})
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { t.Fatal("denied policy mutation reached handler") })
	handler := withRequestMiddleware(srv.securityMiddleware(next), 1<<20)
	request := httptest.NewRequest(http.MethodPost, "/v1/policies", strings.NewReader(`{"workspace_id":"`+workspaceID+`"}`))
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("X-Request-ID", "policy-auth-deny")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("policy mutation response=%d body=%s", response.Code, response.Body.String())
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
