package server

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

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
	adapter, err := repository.NewConnector(workspaceID, repository.ConnectorVersion, nil)
	if err != nil {
		t.Fatal(err)
	}
	definitions := make([]contracts.CapabilityDefinition, 0, len(adapter.Capabilities()))
	for _, capability := range adapter.Capabilities() {
		definitions = append(definitions, capability.Definition())
	}
	policy, err := connector.NewTrustPolicy(workspaceID, "1", definitions)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := connector.NewSchemaCatalog(workspaceID, "1", definitions)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	issued := time.Now().UTC().Add(-time.Minute)
	if err := policy.Sign(signerID, privateKey, issued, issued.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Sign(signerID, privateKey, issued, issued.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	trust := store.NewTrustCatalogStore(pool)
	actor := contracts.AuditActor{ID: "startup-operator", WorkspaceID: workspaceID, Kind: "human", APIKeyID: "startup-key"}
	if err := trust.RegisterSigner(ctx, workspaceID, signerID, publicKey, actor); err != nil {
		t.Fatal(err)
	}
	if err := trust.PublishSignedPolicy(ctx, policy, time.Now().UTC(), actor); err != nil {
		t.Fatal(err)
	}
	if err := trust.PublishSignedSchemaCatalog(ctx, catalog, time.Now().UTC(), actor); err != nil {
		t.Fatal(err)
	}
	// The default workspace is a durable workspace whenever earlier operator
	// tests or a local installation have bootstrapped it. Strict startup must
	// qualify it too rather than silently retaining the built-in unsigned
	// snapshot.
	defaultRepository, err := repository.NewConnector(contracts.DefaultWorkspaceID, repository.ConnectorVersion, nil)
	if err != nil {
		t.Fatal(err)
	}
	defaultIncident, err := fakeincident.NewConnector(contracts.DefaultWorkspaceID)
	if err != nil {
		t.Fatal(err)
	}
	defaultDefinitions := make([]contracts.CapabilityDefinition, 0, len(defaultRepository.Capabilities())+len(defaultIncident.Capabilities()))
	for _, capability := range append(defaultRepository.Capabilities(), defaultIncident.Capabilities()...) {
		defaultDefinitions = append(defaultDefinitions, capability.Definition())
	}
	var defaultRevision int64
	if err := pool.QueryRow(ctx, `SELECT GREATEST(COALESCE((SELECT MAX(revision) FROM fornix.trust_policies WHERE workspace_id=$1),0), COALESCE((SELECT MAX(revision) FROM fornix.trust_schema_catalogs WHERE workspace_id=$1),0))+1`, contracts.DefaultWorkspaceID).Scan(&defaultRevision); err != nil {
		t.Fatal(err)
	}
	defaultPolicy, err := connector.NewTrustPolicy(contracts.DefaultWorkspaceID, fmt.Sprint(defaultRevision), defaultDefinitions)
	if err != nil {
		t.Fatal(err)
	}
	defaultCatalog, err := connector.NewSchemaCatalog(contracts.DefaultWorkspaceID, fmt.Sprint(defaultRevision), defaultDefinitions)
	if err != nil {
		t.Fatal(err)
	}
	if err := defaultPolicy.Sign(signerID, privateKey, issued, issued.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err := defaultCatalog.Sign(signerID, privateKey, issued, issued.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	defaultActor := contracts.AuditActor{ID: "startup-default-operator", WorkspaceID: contracts.DefaultWorkspaceID, Kind: "human", APIKeyID: "startup-default-key"}
	if err := trust.RegisterSigner(ctx, contracts.DefaultWorkspaceID, signerID, publicKey, defaultActor); err != nil {
		t.Fatal(err)
	}
	if err := trust.PublishSignedPolicy(ctx, defaultPolicy, time.Now().UTC(), defaultActor); err != nil {
		t.Fatal(err)
	}
	if err := trust.PublishSignedSchemaCatalog(ctx, defaultCatalog, time.Now().UTC(), defaultActor); err != nil {
		t.Fatal(err)
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
