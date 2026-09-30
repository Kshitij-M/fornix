package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/adapters/fakedomains"
	"github.com/omaveda/fornix/internal/adapters/fakeincident"
	"github.com/omaveda/fornix/internal/adapters/repository"
	"github.com/omaveda/fornix/internal/config"
	"github.com/omaveda/fornix/internal/connector"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/store"
)

func TestServerStartupReloadsSignedWorkspaceAuthority(t *testing.T) {
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := store.ApplyMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	workspaceID := fmt.Sprintf("test-authority-startup-%d", time.Now().UnixNano())
	signerID := "startup-release-" + fmt.Sprint(time.Now().UnixNano())
	operator := store.NewOperatorStore(pool, store.NewEventStore(pool))
	if _, err := operator.Bootstrap(ctx, contracts.WorkspaceBootstrapRequest{
		WorkspaceID: workspaceID, DisplayName: "authority startup", Subject: "startup-operator", IdentityKind: "human",
		RoleName: "owner", Permissions: []contracts.Permission{contracts.PermissionOperationExecute}, IdempotencyKey: "bootstrap-" + workspaceID,
	}); err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	trust := store.NewTrustCatalogStore(pool)
	// The CI smoke job reuses one Postgres database across its bounded
	// integration steps, so earlier steps may have left additional workspaces.
	// A strict production startup validates every durable workspace; install a
	// complete signed generation for the same built-in connector set on each
	// one instead of making the test depend on database cleanliness.
	workspaceIDs := map[string]struct{}{workspaceID: {}, contracts.DefaultWorkspaceID: {}}
	cursor := ""
	for {
		page, err := operator.ListWorkspaces(ctx, 100, cursor)
		if err != nil {
			t.Fatal(err)
		}
		for _, workspace := range page.Items {
			workspaceIDs[workspace.ID] = struct{}{}
		}
		if page.NextCursor == "" {
			break
		}
		cursor = page.NextCursor
	}
	orderedWorkspaceIDs := make([]string, 0, len(workspaceIDs))
	for id := range workspaceIDs {
		orderedWorkspaceIDs = append(orderedWorkspaceIDs, id)
	}
	sort.Strings(orderedWorkspaceIDs)
	issued := time.Now().UTC().Add(-time.Minute)
	var policy connector.TrustPolicy
	for _, id := range orderedWorkspaceIDs {
		actor := contracts.AuditActor{ID: "startup-operator", WorkspaceID: id, Kind: "human", APIKeyID: "startup-key"}
		workspacePolicy, publishErr := publishStartupAuthority(ctx, pool, trust, id, signerID, publicKey, privateKey, issued, actor)
		if publishErr != nil {
			t.Fatalf("publish signed startup authority for %s: %v", id, publishErr)
		}
		if id == workspaceID {
			policy = workspacePolicy
		}
	}
	serverConfig := config.Config{
		DSN: dsn, AuthMode: "workspace", Environment: "production", RequireSignedAuthority: true,
		Listen: ":0", OllamaURL: "http://127.0.0.1:11434", OpenAIBaseURL: "https://api.openai.com/v1",
		OpenAICredentialRef: "FORNIX_OPENAI_API_KEY", MaxBodyBytes: 4 << 20, ShutdownTimeout: time.Second,
		DBMaxConnections: 4, DBMinConnections: 1, WorkerEnabled: false,
	}
	srv, err := New(ctx, serverConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	status := srv.authorityStatus()
	if status["ready"] != true || status["required"] != true {
		t.Fatalf("signed authority was not ready after startup: %+v", status)
	}
	loaded, ok := srv.connectorRegistry.TrustPolicy(workspaceID)
	if !ok || loaded.PolicyHash != policy.PolicyHash || loaded.Revision != policy.Revision || loaded.Signature == "" {
		t.Fatalf("startup did not reload durable trust policy: %+v present=%v", loaded, ok)
	}
}

func publishStartupAuthority(ctx context.Context, pool *pgxpool.Pool, trust *store.TrustCatalogStore, workspaceID, signerID string, publicKey ed25519.PublicKey, privateKey ed25519.PrivateKey, issued time.Time, actor contracts.AuditActor) (connector.TrustPolicy, error) {
	repositoryAdapter, err := repository.NewConnector(workspaceID, repository.ConnectorVersion, nil)
	if err != nil {
		return connector.TrustPolicy{}, err
	}
	incidentAdapter, err := fakeincident.NewConnector(workspaceID)
	if err != nil {
		return connector.TrustPolicy{}, err
	}
	domainAdapter, err := fakedomains.NewConnector(workspaceID)
	if err != nil {
		return connector.TrustPolicy{}, err
	}
	definitions := make([]contracts.CapabilityDefinition, 0, len(repositoryAdapter.Capabilities())+len(incidentAdapter.Capabilities())+len(domainAdapter.Capabilities()))
	for _, capability := range append(append(repositoryAdapter.Capabilities(), incidentAdapter.Capabilities()...), domainAdapter.Capabilities()...) {
		definitions = append(definitions, capability.Definition())
	}
	var revision int64
	if err := pool.QueryRow(ctx, `SELECT GREATEST(COALESCE((SELECT MAX(revision) FROM fornix.trust_policies WHERE workspace_id=$1),0), COALESCE((SELECT MAX(revision) FROM fornix.trust_schema_catalogs WHERE workspace_id=$1),0))+1`, workspaceID).Scan(&revision); err != nil {
		return connector.TrustPolicy{}, err
	}
	policy, err := connector.NewTrustPolicy(workspaceID, fmt.Sprint(revision), definitions)
	if err != nil {
		return connector.TrustPolicy{}, err
	}
	catalog, err := connector.NewSchemaCatalog(workspaceID, fmt.Sprint(revision), definitions)
	if err != nil {
		return connector.TrustPolicy{}, err
	}
	if err := policy.Sign(signerID, privateKey, issued, issued.Add(time.Hour)); err != nil {
		return connector.TrustPolicy{}, err
	}
	if err := catalog.Sign(signerID, privateKey, issued, issued.Add(time.Hour)); err != nil {
		return connector.TrustPolicy{}, err
	}
	if err := trust.RegisterSigner(ctx, workspaceID, signerID, publicKey, actor); err != nil {
		return connector.TrustPolicy{}, err
	}
	if err := trust.PublishSignedPolicy(ctx, policy, time.Now().UTC(), actor); err != nil {
		return connector.TrustPolicy{}, err
	}
	if err := trust.PublishSignedSchemaCatalog(ctx, catalog, time.Now().UTC(), actor); err != nil {
		return connector.TrustPolicy{}, err
	}
	return policy, nil
}
