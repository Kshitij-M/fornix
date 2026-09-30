package config

import (
	"strings"
	"testing"
	"time"
)

func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"FORNIX_KEY", "FORNIX_PG_DSN", "FORNIX_LISTEN", "FORNIX_OLLAMA_URL", "OLLAMA_URL",
		"FORNIX_AUTH_MODE",
		"FORNIX_WORKER_ENABLED",
		"FORNIX_ENABLE_LEGACY_GLOBAL_SURFACES",
		"FORNIX_REQUIRE_QUALIFICATION_TRUST", "FORNIX_QUALIFICATION_DEPLOYMENT_ID",
		"FORNIX_REQUIRE_QUALIFICATION_RELEASE", "FORNIX_QUALIFICATION_RELEASE_ID", "FORNIX_QUALIFICATION_RELEASE_ARTIFACT_KIND", "FORNIX_REQUIRE_RELEASE_ADMISSION_FOR_EFFECTS",
		"FORNIX_ENV", "FORNIX_MAX_BODY_BYTES",
		"FORNIX_SHUTDOWN_TIMEOUT_SECONDS", "FORNIX_DB_MAX_CONNS", "FORNIX_DB_MIN_CONNS",
		"FORNIX_OPENAI_ENABLED", "FORNIX_OPENAI_BASE_URL", "FORNIX_OPENAI_MODEL",
		"FORNIX_OPENAI_TIMEOUT_SECONDS", "FORNIX_OPENAI_CREDENTIAL_REF", "FORNIX_OPENAI_ALLOW_PRIVATE",
		"FORNIX_OPENAI_API_KEY",
		"FORNIX_FEDERATION_POLL_ENABLED", "FORNIX_CREDENTIAL_MANAGER_URL", "FORNIX_CREDENTIAL_MANAGER_TOKEN_REF",
		"FORNIX_CREDENTIAL_MANAGER_TOKEN", "FORNIX_CREDENTIAL_MANAGER_ALLOW_PRIVATE", "FORNIX_CREDENTIAL_MANAGER_TIMEOUT_SECONDS",
		"FORNIX_FEDERATION_RETENTION_ENABLED", "FORNIX_FEDERATION_RETENTION_INTERVAL_SECONDS", "FORNIX_FEDERATION_RETENTION_BATCH_SIZE", "FORNIX_FEDERATION_RETENTION_WORKSPACE_LIMIT",
		"FORNIX_OPERATION_WORKER_MAX_CONCURRENT", "FORNIX_OPERATION_WORKER_CLAIM_BATCH", "FORNIX_OPERATION_WORKER_MAX_ACTIVE", "FORNIX_OPERATION_WORKER_POLL_INTERVAL_SECONDS",
	} {
		t.Setenv(name, "")
	}
}

func TestLoadCanonicalDefaults(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FORNIX_KEY", strings.Repeat("k", 32))
	t.Setenv("FORNIX_AUTH_MODE", "development")
	t.Setenv("FORNIX_PG_DSN", "postgres://fornix:secret@localhost/fornix")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if c.Listen != defaultListen || c.OllamaURL != defaultOllamaURL {
		t.Fatalf("defaults = listen %q ollama %q", c.Listen, c.OllamaURL)
	}
	if c.MaxBodyBytes != defaultMaxBodyBytes || c.ShutdownTimeout != defaultShutdownWindow {
		t.Fatalf("limits = body %d shutdown %s", c.MaxBodyBytes, c.ShutdownTimeout)
	}
	if c.DBMaxConnections != 20 || c.DBMinConnections != 2 {
		t.Fatalf("pool defaults = min %d max %d", c.DBMinConnections, c.DBMaxConnections)
	}
	if !c.WorkerEnabled {
		t.Fatal("worker should be enabled by default")
	}
	if c.OperationWorkerMaxConcurrent != defaultOperationWorkerMaxConcurrent || c.OperationWorkerClaimBatch != defaultOperationWorkerClaimBatch || c.OperationWorkerMaxActive != defaultOperationWorkerMaxActive || c.OperationWorkerPollInterval != defaultOperationWorkerPollInterval {
		t.Fatalf("operation worker defaults = %+v", c)
	}
	if c.EnableLegacyGlobalSurfaces {
		t.Fatal("legacy global surfaces must be disabled by default")
	}
}

func TestLoadOptionalAliases(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("OLLAMA_URL", "http://ollama.internal/")
	t.Setenv("FORNIX_KEY", "optional-key")
	t.Setenv("FORNIX_AUTH_MODE", "development")
	t.Setenv("FORNIX_PG_DSN", "postgres://fornix@localhost/fornix")
	t.Setenv("FORNIX_ENV", "staging")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if c.APIKey != "optional-key" || c.DSN != "postgres://fornix@localhost/fornix" || c.Listen != defaultListen {
		t.Fatalf("canonical configuration not loaded: %+v", c)
	}
	if c.OllamaURL != "http://ollama.internal" || c.Environment != "staging" {
		t.Fatalf("optional configuration not loaded: %+v", c)
	}
}

func TestLoadProductionRejectsWeakKey(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FORNIX_KEY", "short")
	t.Setenv("FORNIX_AUTH_MODE", "development")
	t.Setenv("FORNIX_PG_DSN", "postgres://fornix@localhost/fornix")
	t.Setenv("FORNIX_ENV", "production")

	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "development") {
		t.Fatalf("Load() error = %v, want development-mode production rejection", err)
	}
}

func TestLoadParsesLimits(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FORNIX_KEY", "key")
	t.Setenv("FORNIX_AUTH_MODE", "development")
	t.Setenv("FORNIX_PG_DSN", "postgres://fornix@localhost/fornix")
	t.Setenv("FORNIX_MAX_BODY_BYTES", "1024")
	t.Setenv("FORNIX_SHUTDOWN_TIMEOUT_SECONDS", "7")
	t.Setenv("FORNIX_DB_MAX_CONNS", "8")
	t.Setenv("FORNIX_DB_MIN_CONNS", "3")

	c, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if c.MaxBodyBytes != 1024 || c.ShutdownTimeout != 7*time.Second || c.DBMaxConnections != 8 || c.DBMinConnections != 3 {
		t.Fatalf("parsed limits = %+v", c)
	}
}

func TestLoadOpenAIIsExplicitlyOptIn(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FORNIX_KEY", "key")
	t.Setenv("FORNIX_AUTH_MODE", "development")
	t.Setenv("FORNIX_PG_DSN", "postgres://fornix@localhost/fornix")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.OpenAIEnabled || c.OpenAIBaseURL != defaultOpenAIURL || c.OpenAIModel != defaultOpenAIModel {
		t.Fatalf("OpenAI is not disabled by default: %+v", c)
	}

	t.Setenv("FORNIX_OPENAI_ENABLED", "true")
	t.Setenv("FORNIX_OPENAI_API_KEY", "test-only-key")
	t.Setenv("FORNIX_OPENAI_TIMEOUT_SECONDS", "7")
	c, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if !c.OpenAIEnabled || c.OpenAITimeout != 7*time.Second || c.OpenAICredentialRef != "FORNIX_OPENAI_API_KEY" {
		t.Fatalf("OpenAI opt-in config = %+v", c)
	}
}

func TestLoadProductionOpenAIUsesManagedCredentialReference(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FORNIX_PG_DSN", "postgres://fornix@localhost/fornix")
	t.Setenv("FORNIX_ENV", "production")
	t.Setenv("FORNIX_AUTH_MODE", "workspace")
	t.Setenv("FORNIX_OPENAI_ENABLED", "true")
	t.Setenv("FORNIX_QUALIFICATION_DEPLOYMENT_ID", "deployment-test")
	t.Setenv("FORNIX_QUALIFICATION_RELEASE_ID", "release-test")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "namespaced logical credential reference") {
		t.Fatalf("Load() error = %v, want explicit managed credential reference requirement", err)
	}
	t.Setenv("FORNIX_OPENAI_CREDENTIAL_REF", "openai/production-key")
	t.Setenv("FORNIX_CREDENTIAL_MANAGER_URL", "https://credentials.example.test/v1/resolve")
	loaded, err := Load()
	if err != nil {
		t.Fatalf("Load() with managed OpenAI reference: %v", err)
	}
	if !loaded.OpenAIEnabled || loaded.OpenAICredentialRef != "openai/production-key" || loaded.CredentialManagerURL == "" {
		t.Fatalf("production managed OpenAI configuration = %+v", loaded)
	}
	for _, test := range []struct {
		name       string
		managerURL string
		openAIURL  string
	}{
		{name: "credential manager plaintext", managerURL: "http://credentials.example.test/v1/resolve", openAIURL: "https://api.openai.com/v1"},
		{name: "provider plaintext", managerURL: "https://credentials.example.test/v1/resolve", openAIURL: "http://api.example.test/v1"},
		{name: "credential manager without host", managerURL: "https:///resolve", openAIURL: "https://api.openai.com/v1"},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("FORNIX_CREDENTIAL_MANAGER_URL", test.managerURL)
			t.Setenv("FORNIX_OPENAI_BASE_URL", test.openAIURL)
			if _, err := Load(); err == nil || !strings.Contains(err.Error(), "HTTPS") {
				t.Fatalf("Load() error = %v, want production HTTPS rejection", err)
			}
		})
	}
}

func TestLoadQualificationTrustRequiresDeploymentIdentity(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FORNIX_KEY", "key")
	t.Setenv("FORNIX_AUTH_MODE", "development")
	t.Setenv("FORNIX_PG_DSN", "postgres://fornix@localhost/fornix")
	t.Setenv("FORNIX_REQUIRE_QUALIFICATION_TRUST", "true")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "QUALIFICATION_DEPLOYMENT_ID") {
		t.Fatalf("Load() error = %v, want deployment identity requirement", err)
	}
	t.Setenv("FORNIX_QUALIFICATION_DEPLOYMENT_ID", "deployment-local")
	loaded, err := Load()
	if err != nil {
		t.Fatalf("Load() with qualification deployment identity: %v", err)
	}
	if !loaded.RequireQualificationTrust || loaded.QualificationDeploymentID != "deployment-local" {
		t.Fatalf("qualification trust config = %+v", loaded)
	}
}

func TestLoadQualificationReleaseRequiresExplicitIdentity(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FORNIX_KEY", "key")
	t.Setenv("FORNIX_AUTH_MODE", "development")
	t.Setenv("FORNIX_PG_DSN", "postgres://fornix@localhost/fornix")
	t.Setenv("FORNIX_REQUIRE_QUALIFICATION_RELEASE", "true")
	t.Setenv("FORNIX_QUALIFICATION_DEPLOYMENT_ID", "deployment-local")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "QUALIFICATION_RELEASE_ID") {
		t.Fatalf("Load() error = %v, want release identity requirement", err)
	}
	t.Setenv("FORNIX_QUALIFICATION_RELEASE_ID", "release-local")
	loaded, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.RequireQualificationRelease || loaded.QualificationReleaseID != "release-local" || loaded.QualificationReleaseArtifactKind != "release" {
		t.Fatalf("qualification release config = %+v", loaded)
	}
}

func TestLoadReleaseAdmissionForEffectsIsExplicitOutsideProduction(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FORNIX_KEY", "key")
	t.Setenv("FORNIX_AUTH_MODE", "development")
	t.Setenv("FORNIX_PG_DSN", "postgres://fornix@localhost/fornix")
	loaded, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded.RequireReleaseAdmissionForEffects {
		t.Fatal("release admission for effects should be opt-in outside production")
	}
	t.Setenv("FORNIX_REQUIRE_RELEASE_ADMISSION_FOR_EFFECTS", "true")
	loaded, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if !loaded.RequireReleaseAdmissionForEffects {
		t.Fatal("explicit release admission for effects was ignored")
	}
}

func TestLoadWorkerCanBeDisabledExplicitly(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FORNIX_AUTH_MODE", "development")
	t.Setenv("FORNIX_KEY", "key")
	t.Setenv("FORNIX_PG_DSN", "postgres://fornix@localhost/fornix")
	t.Setenv("FORNIX_WORKER_ENABLED", "false")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.WorkerEnabled {
		t.Fatal("worker disable flag was ignored")
	}
}

func TestLoadOperationWorkerBoundsAndOverrides(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FORNIX_AUTH_MODE", "development")
	t.Setenv("FORNIX_KEY", "key")
	t.Setenv("FORNIX_PG_DSN", "postgres://fornix@localhost/fornix")
	t.Setenv("FORNIX_OPERATION_WORKER_MAX_CONCURRENT", "7")
	t.Setenv("FORNIX_OPERATION_WORKER_CLAIM_BATCH", "11")
	t.Setenv("FORNIX_OPERATION_WORKER_MAX_ACTIVE", "13")
	t.Setenv("FORNIX_OPERATION_WORKER_POLL_INTERVAL_SECONDS", "9")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.OperationWorkerMaxConcurrent != 7 || c.OperationWorkerClaimBatch != 11 || c.OperationWorkerMaxActive != 13 || c.OperationWorkerPollInterval != 9*time.Second {
		t.Fatalf("operation worker overrides = %+v", c)
	}
	t.Setenv("FORNIX_OPERATION_WORKER_MAX_CONCURRENT", "65")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "OPERATION_WORKER_MAX_CONCURRENT") {
		t.Fatalf("unbounded operation worker concurrency error=%v", err)
	}
}

func TestOptionalBoundedIntRejectsHugeValuesBeforeConversion(t *testing.T) {
	t.Setenv("FORNIX_TEST_BOUNDED_INT", "9223372036854775807")
	if _, _, err := optionalBoundedInt("FORNIX_TEST_BOUNDED_INT", 1, 1000); err == nil {
		t.Fatal("expected signed maximum to be rejected by the configured bound")
	}
	t.Setenv("FORNIX_TEST_BOUNDED_INT", "1001")
	if _, _, err := optionalBoundedInt("FORNIX_TEST_BOUNDED_INT", 1, 1000); err == nil {
		t.Fatal("expected out-of-range value to be rejected")
	}
	t.Setenv("FORNIX_TEST_BOUNDED_INT", "12")
	if got, present, err := optionalBoundedInt("FORNIX_TEST_BOUNDED_INT", 1, 1000); err != nil || !present || got != 12 {
		t.Fatalf("bounded value = %d, present=%t, error=%v", got, present, err)
	}
}

func TestLoadLegacyGlobalSurfacesRequireExplicitNonProductionOptIn(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FORNIX_AUTH_MODE", "development")
	t.Setenv("FORNIX_KEY", "key")
	t.Setenv("FORNIX_PG_DSN", "postgres://fornix@localhost/fornix")
	t.Setenv("FORNIX_ENABLE_LEGACY_GLOBAL_SURFACES", "true")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !c.EnableLegacyGlobalSurfaces {
		t.Fatal("explicit legacy global surface opt-in was ignored")
	}

	t.Setenv("FORNIX_ENV", "production")
	t.Setenv("FORNIX_AUTH_MODE", "workspace")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "LEGACY_GLOBAL_SURFACES") {
		t.Fatalf("production legacy-surface configuration error = %v", err)
	}
}

func TestLoadCredentialManagerConfigurationIsNonSecretAndBounded(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FORNIX_AUTH_MODE", "development")
	t.Setenv("FORNIX_KEY", "key")
	t.Setenv("FORNIX_PG_DSN", "postgres://fornix@localhost/fornix")
	t.Setenv("FORNIX_FEDERATION_POLL_ENABLED", "true")
	t.Setenv("FORNIX_CREDENTIAL_MANAGER_URL", "https://secrets.example.test/v1/resolve")
	t.Setenv("FORNIX_CREDENTIAL_MANAGER_TOKEN_REF", "FORNIX_MANAGER_TOKEN")
	t.Setenv("FORNIX_CREDENTIAL_MANAGER_TIMEOUT_SECONDS", "17")

	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !c.FederationPollEnabled || c.CredentialManagerURL == "" || c.CredentialManagerTokenRef != "FORNIX_MANAGER_TOKEN" || c.CredentialManagerTimeout != 17*time.Second {
		t.Fatalf("manager configuration = %+v", c)
	}
}

func TestLoadRejectsUnboundedCredentialManagerTimeout(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FORNIX_AUTH_MODE", "development")
	t.Setenv("FORNIX_KEY", "key")
	t.Setenv("FORNIX_PG_DSN", "postgres://fornix@localhost/fornix")
	t.Setenv("FORNIX_CREDENTIAL_MANAGER_TIMEOUT_SECONDS", "121")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "CREDENTIAL_MANAGER_TIMEOUT_SECONDS") {
		t.Fatalf("Load() error = %v, want bounded manager timeout rejection", err)
	}
}

func TestLoadFederationRetentionOwnerConfigurationIsBounded(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FORNIX_AUTH_MODE", "development")
	t.Setenv("FORNIX_KEY", "key")
	t.Setenv("FORNIX_PG_DSN", "postgres://fornix@localhost/fornix")
	t.Setenv("FORNIX_FEDERATION_RETENTION_ENABLED", "true")
	t.Setenv("FORNIX_FEDERATION_RETENTION_INTERVAL_SECONDS", "17")
	t.Setenv("FORNIX_FEDERATION_RETENTION_BATCH_SIZE", "12")
	t.Setenv("FORNIX_FEDERATION_RETENTION_WORKSPACE_LIMIT", "4")
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !c.FederationRetentionEnabled || c.FederationRetentionInterval != 17*time.Second || c.FederationRetentionBatchSize != 12 || c.FederationRetentionWorkspaceLimit != 4 {
		t.Fatalf("retention configuration = %+v", c)
	}

	t.Setenv("FORNIX_FEDERATION_RETENTION_INTERVAL_SECONDS", "86401")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "FEDERATION_RETENTION_INTERVAL_SECONDS") {
		t.Fatalf("unbounded retention interval error = %v", err)
	}
}
