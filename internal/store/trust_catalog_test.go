package store

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/adapters/fakeincident"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
)

func newTrustCatalogTestStore(t *testing.T) (*TrustCatalogStore, *pgxpool.Pool, string, ed25519.PrivateKey, connector.TrustPolicy, []contracts.CapabilityDefinition) {
	t.Helper()
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create test pool: %v", err)
	}
	if os.Getenv("FORNIX_SKIP_MIGRATIONS") == "" {
		if err := ApplyMigrations(ctx, pool); err != nil {
			pool.Close()
			t.Fatalf("apply migrations: %v", err)
		}
	}
	workspaceID := fmt.Sprintf("test-trust-catalog-%d", time.Now().UnixNano())
	adapter, err := fakeincident.NewConnector(workspaceID)
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	definitions := make([]contracts.CapabilityDefinition, 0, len(adapter.Capabilities()))
	for _, capability := range adapter.Capabilities() {
		definitions = append(definitions, capability.Definition())
	}
	policy, err := connector.NewTrustPolicy(workspaceID, "1", definitions)
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if err := policy.Sign("release-1", privateKey, time.Now().UTC().Add(-time.Minute), time.Now().UTC().Add(time.Hour)); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	store := NewTrustCatalogStore(pool)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.trust_policies WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.trust_signer_events WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.trust_signers WHERE workspace_id=$1`, workspaceID)
		pool.Close()
	})
	return store, pool, workspaceID, privateKey, policy, definitions
}

func trustTestActor(workspaceID string) contracts.AuditActor {
	return contracts.AuditActor{ID: "operator-1", WorkspaceID: workspaceID, Kind: "human", APIKeyID: "key-1"}
}

func TestTrustCatalogPublishesVerifiesAndRevokesSignedPolicy(t *testing.T) {
	store, _, workspaceID, privateKey, policy, _ := newTrustCatalogTestStore(t)
	ctx := context.Background()
	actor := trustTestActor(workspaceID)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	if err := store.RegisterSigner(ctx, workspaceID, policy.SignerID, publicKey, actor); err != nil {
		t.Fatalf("register signer: %v", err)
	}
	if err := store.PublishSignedPolicy(ctx, policy, time.Now().UTC(), actor); err != nil {
		t.Fatalf("publish policy: %v", err)
	}
	if err := store.PublishSignedPolicy(ctx, policy, time.Now().UTC(), actor); err != nil {
		t.Fatalf("duplicate policy should be idempotent: %v", err)
	}
	loaded, loadedKey, err := store.CurrentSignedPolicy(ctx, workspaceID, time.Now().UTC())
	if err != nil {
		t.Fatalf("load current policy: %v", err)
	}
	if loaded.PolicyHash != policy.PolicyHash || loaded.Revision != policy.Revision || !equalBytes(loadedKey, publicKey) {
		t.Fatalf("loaded policy differs: loaded=%+v", loaded)
	}
	registry := connector.NewRegistry()
	if err := store.InstallCurrent(ctx, registry, workspaceID, time.Now().UTC()); err != nil {
		t.Fatalf("install current policy: %v", err)
	}
	if err := store.InstallCurrent(ctx, registry, workspaceID, time.Now().UTC()); err != nil {
		t.Fatalf("idempotent policy install: %v", err)
	}
	if err := store.RevokeSigner(ctx, workspaceID, policy.SignerID, actor); err != nil {
		t.Fatalf("revoke signer: %v", err)
	}
	if _, _, err := store.CurrentSignedPolicy(ctx, workspaceID, time.Now().UTC()); !errors.Is(err, ErrTrustPolicyNotFound) {
		t.Fatalf("revoked signer policy read = %v, want not found", err)
	}
}

func TestTrustCatalogSerializesConcurrentRevisionPublication(t *testing.T) {
	store, pool, workspaceID, privateKey, policy, definitions := newTrustCatalogTestStore(t)
	ctx := context.Background()
	actor := trustTestActor(workspaceID)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	if err := store.RegisterSigner(ctx, workspaceID, policy.SignerID, publicKey, actor); err != nil {
		t.Fatal(err)
	}
	if err := store.PublishSignedPolicy(ctx, policy, time.Now().UTC(), actor); err != nil {
		t.Fatal(err)
	}
	second, err := connector.NewTrustPolicy(workspaceID, "2", definitions)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Sign(policy.SignerID, privateKey, time.Now().UTC().Add(-time.Minute), time.Now().UTC().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- store.PublishSignedPolicy(ctx, second, time.Now().UTC(), actor)
		}()
	}
	wg.Wait()
	close(results)
	var success, failure int
	for err := range results {
		switch {
		case err == nil:
			success++
		default:
			failure++
		}
	}
	if success != 2 || failure != 0 {
		t.Fatalf("concurrent publish outcomes success=%d failure=%d", success, failure)
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.trust_policies WHERE workspace_id=$1`, workspaceID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 2 {
		t.Fatalf("policy rows=%d, want initial plus one deduplicated revision", rows)
	}
}

func TestTrustCatalogPublishesAndInstallsSignedSchemaCatalog(t *testing.T) {
	store, _, workspaceID, privateKey, policy, definitions := newTrustCatalogTestStore(t)
	ctx := context.Background()
	actor := trustTestActor(workspaceID)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	if err := store.RegisterSigner(ctx, workspaceID, policy.SignerID, publicKey, actor); err != nil {
		t.Fatal(err)
	}
	catalog, err := connector.NewSchemaCatalog(workspaceID, "1", definitions)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := catalog.Sign(policy.SignerID, privateKey, now.Add(-time.Second), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := store.PublishSignedSchemaCatalog(ctx, catalog, now, actor); err != nil {
		t.Fatalf("publish schema catalog: %v", err)
	}
	if err := store.PublishSignedSchemaCatalog(ctx, catalog, now, actor); err != nil {
		t.Fatalf("duplicate schema catalog should be idempotent: %v", err)
	}
	loaded, loadedKey, err := store.CurrentSignedSchemaCatalog(ctx, workspaceID, now)
	if err != nil {
		t.Fatalf("load schema catalog: %v", err)
	}
	if loaded.CatalogHash != catalog.CatalogHash || loaded.Revision != catalog.Revision || !equalBytes(loadedKey, publicKey) {
		t.Fatalf("loaded schema catalog differs: loaded=%+v", loaded)
	}
	adapter, err := fakeincident.NewConnector(workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	registry := connector.NewRegistry()
	if err := registry.Register(adapter); err != nil {
		t.Fatal(err)
	}
	if err := store.InstallCurrentSchemaCatalog(ctx, registry, workspaceID, now); err != nil {
		t.Fatalf("install schema catalog: %v", err)
	}
	request := schemaCatalogTestRequest(adapter.Capabilities()[0].Definition())
	if _, err := registry.Admit(ctx, request, connector.AdmissionOptions{}); err != nil {
		t.Fatalf("installed schema catalog rejected matching request: %v", err)
	}
	if err := store.RevokeSigner(ctx, workspaceID, policy.SignerID, actor); err != nil {
		t.Fatal(err)
	}
	if _, _, err := store.CurrentSignedSchemaCatalog(ctx, workspaceID, now); !errors.Is(err, ErrSchemaCatalogNotFound) {
		t.Fatalf("revoked schema signer read=%v, want not found", err)
	}
}

func TestSignedSchemaCatalogBindsEffectAuthorityFacts(t *testing.T) {
	store, pool, workspaceID, privateKey, policy, definitions := newTrustCatalogTestStore(t)
	ctx := context.Background()
	actor := trustTestActor(workspaceID)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	if err := store.RegisterSigner(ctx, workspaceID, policy.SignerID, publicKey, actor); err != nil {
		t.Fatal(err)
	}
	catalog, err := connector.NewSchemaCatalog(workspaceID, "9", definitions)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := catalog.Sign(policy.SignerID, privateKey, now.Add(-time.Second), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := store.PublishSignedSchemaCatalog(ctx, catalog, now, actor); err != nil {
		t.Fatal(err)
	}
	tx, err := beginWorkspaceTx(ctx, pool, workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateAuthorityFactsTx(ctx, tx, workspaceID, catalog.CatalogHash, catalog.Revision, "", 0, 0, "", nil); err != nil {
		_ = tx.Rollback(ctx)
		t.Fatalf("validate signed catalog authority: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := store.RevokeSigner(ctx, workspaceID, policy.SignerID, actor); err != nil {
		t.Fatal(err)
	}
	tx, err = beginWorkspaceTx(ctx, pool, workspaceID)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if err := validateAuthorityFactsTx(ctx, tx, workspaceID, catalog.CatalogHash, catalog.Revision, "", 0, 0, "", nil); !errors.Is(err, ErrAuthorityLinkStale) {
		t.Fatalf("revoked catalog authority error=%v, want stale", err)
	}
}

func TestTrustCatalogSerializesConcurrentDuplicateSchemaPublication(t *testing.T) {
	store, pool, workspaceID, privateKey, policy, definitions := newTrustCatalogTestStore(t)
	ctx := context.Background()
	actor := trustTestActor(workspaceID)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	if err := store.RegisterSigner(ctx, workspaceID, policy.SignerID, publicKey, actor); err != nil {
		t.Fatal(err)
	}
	catalog, err := connector.NewSchemaCatalog(workspaceID, "1", definitions)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if err := catalog.Sign(policy.SignerID, privateKey, now.Add(-time.Second), now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results <- store.PublishSignedSchemaCatalog(ctx, catalog, now, actor)
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent duplicate schema publication failed: %v", err)
		}
	}
	var rows int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM fornix.trust_schema_catalogs WHERE workspace_id=$1`, workspaceID).Scan(&rows); err != nil {
		t.Fatal(err)
	}
	if rows != 1 {
		t.Fatalf("schema catalog rows=%d, want one deduplicated row", rows)
	}
}

func TestTrustCatalogRejectsCrossWorkspaceSchemaCatalog(t *testing.T) {
	store, _, workspaceID, privateKey, policy, definitions := newTrustCatalogTestStore(t)
	ctx := context.Background()
	actor := trustTestActor(workspaceID)
	publicKey := privateKey.Public().(ed25519.PublicKey)
	if err := store.RegisterSigner(ctx, workspaceID, policy.SignerID, publicKey, actor); err != nil {
		t.Fatal(err)
	}
	foreignWorkspace := "foreign-schema-workspace"
	foreignActor := trustTestActor(foreignWorkspace)
	catalog, err := connector.NewSchemaCatalog(foreignWorkspace, "1", definitions)
	if err == nil {
		t.Fatal("cross-workspace definitions unexpectedly produced a catalog")
	}
	if _, _, err := store.CurrentSignedSchemaCatalog(ctx, foreignWorkspace, time.Now().UTC()); !errors.Is(err, ErrSchemaCatalogNotFound) {
		t.Fatalf("foreign schema catalog read=%v, want not found", err)
	}
	_ = catalog
	_ = foreignActor
}

func schemaCatalogTestRequest(definition contracts.CapabilityDefinition) contracts.OperationRequest {
	return contracts.OperationRequest{
		ID: "schema-operation", RequestID: "schema-request", IdempotencyKey: "schema-idempotency", WorkspaceID: definition.WorkspaceID,
		Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: definition.WorkspaceID}, Capability: definition.Ref,
		Target:    contracts.ResourceRef{WorkspaceID: definition.WorkspaceID, System: contracts.SystemRef{WorkspaceID: definition.WorkspaceID, Type: "monitoring", ID: "monitor", Version: "1"}, Kind: definition.ResourceKinds[0], ID: "resource-1", Version: "1"},
		InputType: "incident.workflow.step", InputSchemaVersion: definition.InputSchemaVersion, InputSchemaHash: definition.InputSchemaHash, InputHash: contracts.HashStrings("schema-input"), Profile: definition.Profile,
	}
}
