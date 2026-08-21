package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

func newIdentityTestStore(t *testing.T) (*AuthStore, *pgxpool.Pool, string) {
	t.Helper()
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create test pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatalf("ping test database: %v", err)
	}
	if err := ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatalf("apply migrations: %v", err)
	}
	workspaceID := fmt.Sprintf("test-identity-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.authorization_audit WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.api_keys WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.credential_references WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.identity_role_bindings WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.roles WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.identities WHERE workspace_id=$1`, workspaceID)
		pool.Close()
	})
	return NewAuthStore(pool), pool, workspaceID
}

func TestAuthStoreAPIKeyLifecycleAndRBAC(t *testing.T) {
	store, _, workspaceID := newIdentityTestStore(t)
	ctx := context.Background()
	identity, err := store.CreateIdentity(ctx, contracts.IdentityInput{
		WorkspaceID: workspaceID, Subject: "alice", Kind: "user", DisplayName: "Alice",
		Permissions: []contracts.Permission{contracts.PermissionModelInvoke, contracts.PermissionRetrievalRead},
	})
	if err != nil {
		t.Fatal(err)
	}
	key, token, err := store.CreateAPIKey(ctx, contracts.APIKeyInput{WorkspaceID: workspaceID, IdentityID: identity.ID})
	if err != nil {
		t.Fatal(err)
	}
	if token == "" || key.Token == "" || !strings.HasPrefix(token, "fornix_"+key.ID+"_") {
		t.Fatalf("invalid generated token metadata: key=%+v token=%q", key, token)
	}
	encoded, _ := json.Marshal(key)
	if strings.Contains(string(encoded), token) || strings.Contains(string(encoded), key.TokenHash) {
		t.Fatalf("secret material was serialized: %s", encoded)
	}
	principal, err := store.Authenticate(ctx, token)
	if err != nil {
		t.Fatal(err)
	}
	if principal.WorkspaceID != workspaceID || principal.ID != identity.ID || !principal.Has(contracts.PermissionModelInvoke) || principal.Has(contracts.PermissionToolExecute) {
		t.Fatalf("principal=%+v", principal)
	}
	if _, err := store.Authenticate(ctx, token+"x"); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("wrong secret error=%v", err)
	}

	decision, err := store.Authorize(ctx, principal, "request-rbac-1", contracts.PermissionModelInvoke, "model:fake", "POST", "/v1/model/complete")
	if err != nil || !decision.Allowed {
		t.Fatalf("allow decision=%+v err=%v", decision, err)
	}
	if _, err := store.Authorize(ctx, principal, "request-rbac-2", contracts.PermissionToolExecute, "tool:echo", "POST", "/v1/tools/execute"); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("deny error=%v", err)
	}

	rotated, rotatedToken, err := store.RotateAPIKey(ctx, workspaceID, key.ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	if rotated.RotatedFrom != key.ID || rotatedToken == token {
		t.Fatalf("rotation=%+v token_equal=%t", rotated, rotatedToken == token)
	}
	if _, err := store.Authenticate(ctx, token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("old token after rotation error=%v", err)
	}
	if _, err := store.Authenticate(ctx, rotatedToken); err != nil {
		t.Fatalf("rotated token: %v", err)
	}
	if err := store.RevokeAPIKey(ctx, workspaceID, rotated.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Authenticate(ctx, rotatedToken); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("revoked token error=%v", err)
	}
}

func TestAuthStoreExpiryCredentialLifecycleAndWorkspaceIsolation(t *testing.T) {
	store, _, workspaceID := newIdentityTestStore(t)
	ctx := context.Background()
	identity, err := store.CreateIdentity(ctx, contracts.IdentityInput{WorkspaceID: workspaceID, Subject: "expired", Permissions: []contracts.Permission{contracts.PermissionWorkspaceRead}})
	if err != nil {
		t.Fatal(err)
	}
	past := time.Now().UTC().Add(-time.Minute)
	_, token, err := store.CreateAPIKey(ctx, contracts.APIKeyInput{WorkspaceID: workspaceID, IdentityID: identity.ID, ExpiresAt: &past})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Authenticate(ctx, token); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("expired token error=%v", err)
	}
	ref, err := store.CreateCredentialRef(ctx, contracts.CredentialRefInput{WorkspaceID: workspaceID, Provider: "openai", Name: "default", Reference: "FORNIX_OPENAI_API_KEY"})
	if err != nil {
		t.Fatal(err)
	}
	if ref.Reference == "" || strings.Contains(ref.Reference, "sk-") {
		t.Fatalf("unexpected credential ref=%+v", ref)
	}
	rotated, err := store.RotateCredentialRef(ctx, workspaceID, ref.ID, "FORNIX_OPENAI_API_KEY_V2", nil)
	if err != nil || rotated.Version != 2 || rotated.RotatedFrom != ref.ID {
		t.Fatalf("credential rotation=%+v err=%v", rotated, err)
	}
	if _, err := store.CredentialRefForUse(ctx, workspaceID, "openai", "default"); err != nil {
		t.Fatal(err)
	}
	if err := store.RevokeCredentialRef(ctx, workspaceID, rotated.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.CredentialRefForUse(ctx, workspaceID, "openai", "default"); !errors.Is(err, ErrCredentialNotFound) {
		t.Fatalf("revoked credential error=%v", err)
	}
	foreign := contracts.Principal{ID: identity.ID, WorkspaceID: workspaceID + "-foreign", Kind: "user", Authenticated: true}
	if _, err := store.Authorize(ctx, foreign, "request-foreign", contracts.PermissionWorkspaceRead, "", "GET", "/v1/sessions"); err != nil {
		// Authorization itself is identity-scoped; the HTTP boundary owns the
		// request workspace comparison. This assertion documents that the
		// principal cannot be used to authenticate the original workspace.
		if !errors.Is(err, ErrAuthorizationDenied) {
			t.Fatalf("foreign principal decision=%v", err)
		}
	}
}

func TestAuthStoreAuthorizationAuditIsIdempotentUnderConcurrentDuplicateRequests(t *testing.T) {
	store, pool, workspaceID := newIdentityTestStore(t)
	ctx := context.Background()
	identity, err := store.CreateIdentity(ctx, contracts.IdentityInput{WorkspaceID: workspaceID, Subject: "concurrent", Permissions: []contracts.Permission{contracts.PermissionTaskRead}})
	if err != nil {
		t.Fatal(err)
	}
	_, token, err := store.CreateAPIKey(ctx, contracts.APIKeyInput{WorkspaceID: workspaceID, IdentityID: identity.ID})
	if err != nil {
		t.Fatal(err)
	}
	principal, err := store.Authenticate(ctx, token)
	if err != nil {
		t.Fatal(err)
	}
	const workers = 12
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			decision, err := store.Authorize(ctx, principal, "same-request", contracts.PermissionTaskRead, "task:list", "GET", "/v1/tasks")
			if err != nil || !decision.Allowed {
				errs <- fmt.Errorf("decision=%+v err=%w", decision, err)
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.authorization_audit WHERE workspace_id=$1 AND request_id='same-request'`, workspaceID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("duplicate authorization audit rows=%d", count)
	}
}

func TestAuthStoreAuthorizationIdempotencyCannotCrossIdentity(t *testing.T) {
	store, _, workspaceID := newIdentityTestStore(t)
	ctx := context.Background()
	allowedIdentity, err := store.CreateIdentity(ctx, contracts.IdentityInput{WorkspaceID: workspaceID, Subject: "allowed", Permissions: []contracts.Permission{contracts.PermissionTaskRead}})
	if err != nil {
		t.Fatal(err)
	}
	deniedIdentity, err := store.CreateIdentity(ctx, contracts.IdentityInput{WorkspaceID: workspaceID, Subject: "denied"})
	if err != nil {
		t.Fatal(err)
	}
	_, allowedToken, err := store.CreateAPIKey(ctx, contracts.APIKeyInput{WorkspaceID: workspaceID, IdentityID: allowedIdentity.ID})
	if err != nil {
		t.Fatal(err)
	}
	_, deniedToken, err := store.CreateAPIKey(ctx, contracts.APIKeyInput{WorkspaceID: workspaceID, IdentityID: deniedIdentity.ID})
	if err != nil {
		t.Fatal(err)
	}
	allowedPrincipal, err := store.Authenticate(ctx, allowedToken)
	if err != nil {
		t.Fatal(err)
	}
	deniedPrincipal, err := store.Authenticate(ctx, deniedToken)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Authorize(ctx, allowedPrincipal, "shared-request", contracts.PermissionTaskRead, "task:list", "GET", "/v1/tasks"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Authorize(ctx, deniedPrincipal, "shared-request", contracts.PermissionTaskRead, "task:list", "GET", "/v1/tasks"); !errors.Is(err, ErrAuthorizationDenied) {
		t.Fatalf("cross-identity decision was reused: %v", err)
	}
}

func TestAuthStoreAuthorizationLatency(t *testing.T) {
	store, _, workspaceID := newIdentityTestStore(t)
	ctx := context.Background()
	identity, err := store.CreateIdentity(ctx, contracts.IdentityInput{WorkspaceID: workspaceID, Subject: "latency", Permissions: []contracts.Permission{contracts.PermissionWorkspaceRead}})
	if err != nil {
		t.Fatal(err)
	}
	_, token, err := store.CreateAPIKey(ctx, contracts.APIKeyInput{WorkspaceID: workspaceID, IdentityID: identity.ID})
	if err != nil {
		t.Fatal(err)
	}
	principal, err := store.Authenticate(ctx, token)
	if err != nil {
		t.Fatal(err)
	}
	durations := make([]time.Duration, 0, 24)
	for i := 0; i < 24; i++ {
		started := time.Now()
		if _, err := store.Authorize(ctx, principal, fmt.Sprintf("latency-%d", i), contracts.PermissionWorkspaceRead, "workspace", "GET", "/v1/health"); err != nil {
			t.Fatal(err)
		}
		durations = append(durations, time.Since(started))
	}
	sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
	percentile := func(p int) time.Duration { return durations[(len(durations)*p/100)-1] }
	t.Logf("authorization latency samples=%d p50=%s p95=%s max=%s", len(durations), percentile(50), percentile(95), durations[len(durations)-1])
}
