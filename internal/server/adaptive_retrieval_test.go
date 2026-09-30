package server

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func TestGateThresholdIsDeterministicAndBounded(t *testing.T) {
	if got := gateThreshold(10, 0.9, 3, false); !got.Satisfied || got.Reason != "deterministic_sufficient" {
		t.Fatalf("strong deterministic gate = %+v", got)
	}
	if got := gateThreshold(10, 0.01, 3, false); got.Satisfied || got.Reason != "insufficient_confidence" {
		t.Fatalf("weak deterministic gate = %+v", got)
	}
	if got := gateThreshold(1, 0, 0, false); got.Satisfied || got.Reason != "insufficient_candidates" {
		t.Fatalf("empty deterministic gate = %+v", got)
	}
	if got := gateThreshold(10, 0, 1, true); !got.Satisfied || got.Reason != "deterministic_sufficient" {
		t.Fatalf("exact deterministic gate = %+v", got)
	}
}

func TestQueryEmbeddingModesAreVersioned(t *testing.T) {
	for _, test := range []struct {
		input, fallback, want string
	}{
		{"adaptive", queryEmbeddingModeLegacy, queryEmbeddingModeAdaptive},
		{"required", queryEmbeddingModeLegacy, queryEmbeddingModeRequired},
		{"disabled", queryEmbeddingModeLegacy, queryEmbeddingModeDisabled},
		{"", queryEmbeddingModeAdaptive, queryEmbeddingModeAdaptive},
	} {
		if got := normalizeQueryEmbeddingMode(test.input, test.fallback); got != test.want {
			t.Fatalf("normalizeQueryEmbeddingMode(%q,%q)=%q want=%q", test.input, test.fallback, got, test.want)
		}
	}
	if got := normalizeQueryEmbeddingMode("unknown", queryEmbeddingModeAdaptive); got != "" {
		t.Fatalf("unknown mode=%q", got)
	}
}

func TestReferenceTimeIsBoundedAndReplayStable(t *testing.T) {
	reference := time.Date(2026, time.January, 2, 3, 4, 5, 123456789, time.FixedZone("test", 5*60*60))
	first, err := resolveReferenceTime(&reference)
	if err != nil {
		t.Fatal(err)
	}
	second, err := resolveReferenceTime(&first)
	if err != nil {
		t.Fatal(err)
	}
	if !first.Equal(second) || first.Location() != time.UTC || first.Nanosecond()%1000 != 0 {
		t.Fatalf("reference time was not canonicalized: first=%s second=%s", first, second)
	}
	if _, err := resolveReferenceTime(&time.Time{}); err == nil {
		t.Fatal("zero reference time was accepted")
	}
	future := time.Now().UTC().Add(maxReferenceTimeFuture + time.Second)
	if _, err := resolveReferenceTime(&future); err == nil {
		t.Fatal("far-future reference time was accepted")
	}
}

func TestLegacyRetrievalUsesExplicitReferenceTimeAndStableOrdering(t *testing.T) {
	srv, pool, workspaceID, token := newServerAuthTest(t, []contracts.Permission{contracts.PermissionRetrievalRead})
	t.Cleanup(func() {
		_, _ = pool.Exec(t.Context(), `DELETE FROM fornix.memos WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(t.Context(), `DELETE FROM fornix.chunks WHERE workspace_id=$1`, workspaceID)
	})
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO fornix.memos(workspace_id,title,content,type,tags,sha256,created_at)
		VALUES($1,'alpha-old','alpha old evidence','note','{}',$2,'2025-01-01T00:00:00Z'),
		      ($1,'alpha-new','alpha new evidence','note','{}',$3,'2026-01-01T00:00:00Z')`,
		workspaceID, strings.Repeat("1", 64), strings.Repeat("2", 64)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(t.Context(), `
		INSERT INTO fornix.chunks(workspace_id,source_path,source_range,content,content_sha256,metadata)
		VALUES($1,'src/wanted.go','1-2','alpha wanted',$2,'{"type":"wanted"}'),
		      ($1,'src/other.go','1-2','alpha other',$3,'{"type":"other"}')`,
		workspaceID, strings.Repeat("3", 64), strings.Repeat("4", 64)); err != nil {
		t.Fatal(err)
	}
	handler := withRequestMiddleware(srv.securityMiddleware(srv.routes()), 2<<20)
	reference := "2026-02-01T00:00:00Z"
	searchBody, err := json.Marshal(map[string]any{"query": "alpha", "top_k": 2, "mode": "tsvector", "reference_time": reference})
	if err != nil {
		t.Fatal(err)
	}
	first := performOperationRequest(handler, token, workspaceID, "POST", "/v1/memo/search", searchBody, nil)
	second := performOperationRequest(handler, token, workspaceID, "POST", "/v1/memo/search", searchBody, nil)
	if first.Code != 200 || second.Code != 200 {
		t.Fatalf("memo search status first=%d second=%d first_body=%s second_body=%s", first.Code, second.Code, first.Body.String(), second.Body.String())
	}
	firstJSON := decodeOperationJSON(t, first)
	secondJSON := decodeOperationJSON(t, second)
	firstResults, _ := json.Marshal(firstJSON["results"])
	secondResults, _ := json.Marshal(secondJSON["results"])
	if string(firstResults) != string(secondResults) || fmt.Sprint(firstJSON["reference_time"]) != reference {
		t.Fatalf("reference-time replay changed result: first=%s second=%s response_time=%v", firstResults, secondResults, firstJSON["reference_time"])
	}

	ragBody, err := json.Marshal(map[string]any{
		"q": "alpha", "top_k": 2, "embedding_mode": queryEmbeddingModeDisabled,
		"reference_time": reference, "weights": map[string]any{"cosine": 0, "tsvector": 1, "recency": 1},
		"filters": map[string]any{"type": "wanted", "source_paths": []string{"src/*.go"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	ragResponse := performOperationRequest(handler, token, workspaceID, "POST", "/v1/rag", ragBody, nil)
	if ragResponse.Code != 200 {
		t.Fatalf("rag search status=%d body=%s", ragResponse.Code, ragResponse.Body.String())
	}
	ragJSON := decodeOperationJSON(t, ragResponse)
	ragChunks, ok := ragJSON["chunks"].([]any)
	if !ok || len(ragChunks) != 1 {
		t.Fatalf("filtered RAG chunks=%v body=%s", ragJSON["chunks"], ragResponse.Body.String())
	}
	chunk, ok := ragChunks[0].(map[string]any)
	if !ok || chunk["source"] != "src/wanted.go:1-2" {
		t.Fatalf("filtered RAG chunk=%v", ragChunks[0])
	}
}

// TestWorkspaceScopedRetrievalRouteWithRuntimeRLS is invoked by the
// role-separated qualification script. It deliberately runs the real HTTP
// authentication and memo search handler as a non-owner NOBYPASSRLS role;
// owner-role integration tests cannot prove the transaction-context boundary.
func TestWorkspaceScopedRetrievalRouteWithRuntimeRLS(t *testing.T) {
	dsn := os.Getenv("FORNIX_RLS_TEST_DSN")
	token := os.Getenv("FORNIX_RLS_AUTH_TOKEN")
	if dsn == "" || token == "" {
		t.Skip("FORNIX_RLS_TEST_DSN and FORNIX_RLS_AUTH_TOKEN are required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	workspaceID := "qualification-auth-workspace"
	cleanup := func() {
		_ = store.WithWorkspaceTx(ctx, pool, workspaceID, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `DELETE FROM fornix.memos WHERE workspace_id=$1`, workspaceID)
			return err
		})
	}
	cleanup()
	t.Cleanup(cleanup)
	if err := store.WithWorkspaceTx(ctx, pool, workspaceID, func(tx pgx.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO fornix.memos(workspace_id,title,content,type,tags,sha256,created_at)
			VALUES($1,'runtime-scope','runtime scoped evidence','note','{}',$2,clock_timestamp())`, workspaceID, strings.Repeat("a", 64))
		return err
	}); err != nil {
		t.Fatalf("insert runtime-scoped memo: %v", err)
	}

	srv := &server{pool: pool, auth: store.NewAuthStore(pool), authMode: "workspace"}
	handler := withRequestMiddleware(srv.securityMiddleware(srv.routes()), 2<<20)
	body := []byte(`{"query":"runtime scoped","top_k":4,"mode":"tsvector","reference_time":"2026-02-01T00:00:00Z"}`)
	response := performOperationRequest(handler, token, workspaceID, "POST", "/v1/memo/search", body, nil)
	if response.Code != 200 {
		t.Fatalf("runtime-scoped search status=%d body=%s", response.Code, response.Body.String())
	}
	decoded := decodeOperationJSON(t, response)
	results, ok := decoded["results"].([]any)
	if !ok || len(results) != 1 {
		t.Fatalf("runtime-scoped results=%v body=%s", decoded["results"], response.Body.String())
	}
}

// TestWorkspaceScopedCoordinationRouterRoutesWithRuntimeRLS exercises both
// replacement authorities through the real authenticated HTTP path as a
// non-owner NOBYPASSRLS role. This prevents route-level authorization from
// masking a missing transaction-local workspace context or table grant.
func TestWorkspaceScopedCoordinationRouterRoutesWithRuntimeRLS(t *testing.T) {
	dsn := os.Getenv("FORNIX_RLS_TEST_DSN")
	token := os.Getenv("FORNIX_RLS_AUTH_TOKEN")
	if dsn == "" || token == "" {
		t.Skip("FORNIX_RLS_TEST_DSN and FORNIX_RLS_AUTH_TOKEN are required")
	}
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	workspaceID := "qualification-auth-workspace"
	cleanup := func() {
		_ = store.WithWorkspaceTx(ctx, pool, workspaceID, func(tx pgx.Tx) error {
			_, err := tx.Exec(ctx, `
				DELETE FROM fornix.workspace_router_observations WHERE workspace_id=$1;
				DELETE FROM fornix.workspace_coordination_messages WHERE workspace_id=$1`, workspaceID)
			return err
		})
	}
	cleanup()
	t.Cleanup(cleanup)

	events := store.NewEventStore(pool)
	srv := &server{
		pool:         pool,
		events:       events,
		auth:         store.NewAuthStore(pool),
		authMode:     "workspace",
		coordination: store.NewWorkspaceCoordinationStore(pool, events),
	}
	handler := withRequestMiddleware(srv.securityMiddleware(srv.routes()), 2<<20)

	coordBody := []byte(`{"sender":"runtime","recipient":"worker","subject":"qualification","body":"scoped"}`)
	coordHeaders := map[string]string{"Idempotency-Key": "qualification-coordination"}
	coordResponse := performOperationRequest(handler, token, workspaceID, "POST", "/v1/coord", coordBody, coordHeaders)
	if coordResponse.Code != 200 {
		t.Fatalf("coordination status=%d body=%s", coordResponse.Code, coordResponse.Body.String())
	}
	duplicateCoord := performOperationRequest(handler, token, workspaceID, "POST", "/v1/coord", coordBody, coordHeaders)
	if duplicateCoord.Code != 200 || !boolValue(t, decodeOperationJSON(t, duplicateCoord), "duplicate") {
		t.Fatalf("duplicate coordination status=%d body=%s", duplicateCoord.Code, duplicateCoord.Body.String())
	}
	recent := performOperationRequest(handler, token, workspaceID, "GET", "/v1/coord/recent?after_sequence=0&limit=10", nil, nil)
	if recent.Code != 200 || len(objectSliceValue(t, decodeOperationJSON(t, recent), "messages")) != 1 {
		t.Fatalf("coordination recent status=%d body=%s", recent.Code, recent.Body.String())
	}
	badCursor := performOperationRequest(handler, token, workspaceID, "GET", "/v1/coord/recent?after_sequence=not-a-sequence", nil, nil)
	if badCursor.Code != 400 {
		t.Fatalf("invalid coordination cursor status=%d body=%s", badCursor.Code, badCursor.Body.String())
	}

	routerBody := []byte(`{"task_category":"qualification","model_id":"fake","cost_usd":0.01,"latency_ms":10,"outcome":"success"}`)
	routerHeaders := map[string]string{"Idempotency-Key": "qualification-router"}
	routerResponse := performOperationRequest(handler, token, workspaceID, "POST", "/v1/router/observation", routerBody, routerHeaders)
	if routerResponse.Code != 200 {
		t.Fatalf("router observation status=%d body=%s", routerResponse.Code, routerResponse.Body.String())
	}
	duplicateRouter := performOperationRequest(handler, token, workspaceID, "POST", "/v1/router/observation", routerBody, routerHeaders)
	if duplicateRouter.Code != 200 || !boolValue(t, decodeOperationJSON(t, duplicateRouter), "duplicate") {
		t.Fatalf("duplicate router observation status=%d body=%s", duplicateRouter.Code, duplicateRouter.Body.String())
	}
	recommendation := performOperationRequest(handler, token, workspaceID, "GET", "/v1/router/recommend?category=qualification", nil, nil)
	if recommendation.Code != 200 || len(objectSliceValue(t, decodeOperationJSON(t, recommendation), "recommendations")) != 1 {
		t.Fatalf("router recommendation status=%d body=%s", recommendation.Code, recommendation.Body.String())
	}
}
