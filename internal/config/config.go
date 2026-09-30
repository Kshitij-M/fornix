package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/profile"
)

const (
	defaultListen         = ":8201"
	defaultOllamaURL      = "http://localhost:11434"
	defaultOpenAIURL      = "https://api.openai.com/v1"
	defaultOpenAIModel    = "gpt-4o-mini"
	defaultMaxBodyBytes   = int64(4 << 20)
	defaultShutdownWindow = 10 * time.Second
)

type Config struct {
	APIKey              string
	BootstrapKey        string
	AuthMode            string
	DSN                 string
	Listen              string
	OllamaURL           string
	OpenAIEnabled       bool
	OpenAIBaseURL       string
	OpenAIModel         string
	OpenAITimeout       time.Duration
	OpenAICredentialRef string
	OpenAIAllowPrivate  bool
	// FederationPollEnabled is an explicit admission switch. The server will
	// refuse startup when it is enabled without an injected credential
	// authority; it never falls back to a raw bearer token.
	FederationPollEnabled             bool
	FederationRetentionEnabled        bool
	FederationRetentionInterval       time.Duration
	FederationRetentionBatchSize      int
	FederationRetentionWorkspaceLimit int
	CredentialManagerURL              string
	CredentialManagerTokenRef         string
	CredentialManagerAllowPrivate     bool
	CredentialManagerTimeout          time.Duration
	Environment                       string
	MaxBodyBytes                      int64
	ShutdownTimeout                   time.Duration
	DBMaxConnections                  int32
	DBMinConnections                  int32
	WorkerEnabled                     bool
	// OperationWorkerMaxConcurrent bounds the number of workspace turns that
	// the server-owned read/observation worker may run at once.
	OperationWorkerMaxConcurrent int
	// OperationWorkerClaimBatch bounds the number of eligible operations
	// claimed for one workspace turn.
	OperationWorkerClaimBatch int
	// OperationWorkerMaxActive is the durable per-workspace active-operation
	// cap supplied to the Postgres queue claim.
	OperationWorkerMaxActive    int
	OperationWorkerPollInterval time.Duration
	// RequireSignedAuthority enables durable signed trust/schema catalog
	// loading at startup and before admission. Production always enables it;
	// development may opt in explicitly for qualification.
	RequireSignedAuthority bool
	// EnableLegacyGlobalSurfaces is a migration-only opt-in for the historical
	// federation and router APIs whose original tables predate workspace
	// isolation. It must remain disabled in production.
	EnableLegacyGlobalSurfaces bool
	// RequireQualificationTrust makes qualification imports and readiness
	// depend on a current, durable deployment trust snapshot. Production
	// configuration enables it automatically.
	RequireQualificationTrust bool
	// QualificationDeploymentID identifies the deployment trust snapshot to
	// load. It is metadata only; private signing material never enters config.
	QualificationDeploymentID string
	// RequireQualificationRelease makes readiness require a verified release
	// admission decision in addition to the trust snapshot. Production enables
	// this automatically and must name the release explicitly.
	RequireQualificationRelease bool
	// QualificationReleaseID identifies the immutable release to admit.
	QualificationReleaseID string
	// QualificationReleaseArtifactKind selects the hash-only verification kind.
	QualificationReleaseArtifactKind string
	// RequireReleaseAdmissionForEffects makes every newly reserved generic
	// effect carry a current hash-only deployment admission reference. It is
	// enabled automatically in production and is opt-in for development.
	RequireReleaseAdmissionForEffects bool
	// RequireExternalBoundaryAuthority makes every network communication
	// effect carry an exact redacted egress/destination/network envelope. It
	// is automatic in production and opt-in for development.
	RequireExternalBoundaryAuthority bool
}

func Load() (Config, error) {
	c := Config{
		APIKey:                            firstEnv("FORNIX_KEY"),
		BootstrapKey:                      firstEnv("FORNIX_BOOTSTRAP_KEY"),
		AuthMode:                          strings.ToLower(firstEnvOr("FORNIX_AUTH_MODE", "", "workspace")),
		DSN:                               firstEnv("FORNIX_PG_DSN"),
		Listen:                            firstEnvOr("FORNIX_LISTEN", "", defaultListen),
		OllamaURL:                         strings.TrimRight(firstEnvOr("FORNIX_OLLAMA_URL", "OLLAMA_URL", defaultOllamaURL), "/"),
		OpenAIBaseURL:                     strings.TrimRight(firstEnvOr("FORNIX_OPENAI_BASE_URL", "", defaultOpenAIURL), "/"),
		OpenAIModel:                       firstEnvOr("FORNIX_OPENAI_MODEL", "", defaultOpenAIModel),
		OpenAITimeout:                     defaultModelTimeout,
		OpenAICredentialRef:               firstEnvOr("FORNIX_OPENAI_CREDENTIAL_REF", "", "FORNIX_OPENAI_API_KEY"),
		CredentialManagerURL:              strings.TrimSpace(firstEnvOr("FORNIX_CREDENTIAL_MANAGER_URL", "", "")),
		CredentialManagerTokenRef:         firstEnvOr("FORNIX_CREDENTIAL_MANAGER_TOKEN_REF", "", "FORNIX_CREDENTIAL_MANAGER_TOKEN"),
		CredentialManagerTimeout:          defaultCredentialManagerTimeout,
		FederationRetentionInterval:       defaultFederationRetentionInterval,
		FederationRetentionBatchSize:      defaultFederationRetentionBatchSize,
		FederationRetentionWorkspaceLimit: defaultFederationRetentionWorkspaceLimit,
		Environment:                       strings.ToLower(firstEnvOr("FORNIX_ENV", "", "development")),
		MaxBodyBytes:                      defaultMaxBodyBytes,
		ShutdownTimeout:                   defaultShutdownWindow,
		DBMaxConnections:                  20,
		DBMinConnections:                  2,
		WorkerEnabled:                     true,
		OperationWorkerMaxConcurrent:      defaultOperationWorkerMaxConcurrent,
		OperationWorkerClaimBatch:         defaultOperationWorkerClaimBatch,
		OperationWorkerMaxActive:          defaultOperationWorkerMaxActive,
		OperationWorkerPollInterval:       defaultOperationWorkerPollInterval,
	}
	if c.DSN == "" {
		return Config{}, fmt.Errorf("FORNIX_PG_DSN is required")
	}
	if c.AuthMode != "workspace" && c.AuthMode != "development" {
		return Config{}, fmt.Errorf("FORNIX_AUTH_MODE must be workspace or development")
	}
	if c.AuthMode == "development" && c.APIKey == "" {
		return Config{}, fmt.Errorf("FORNIX_KEY is required when FORNIX_AUTH_MODE=development")
	}
	if c.Environment == "production" && c.AuthMode == "development" {
		return Config{}, fmt.Errorf("FORNIX_AUTH_MODE=development is not allowed in production")
	}
	c.OpenAIEnabled = parseBoolEnv("FORNIX_OPENAI_ENABLED")
	c.OpenAIAllowPrivate = parseBoolEnv("FORNIX_OPENAI_ALLOW_PRIVATE")
	c.FederationPollEnabled = parseBoolEnv("FORNIX_FEDERATION_POLL_ENABLED")
	c.FederationRetentionEnabled = parseBoolEnv("FORNIX_FEDERATION_RETENTION_ENABLED")
	c.CredentialManagerAllowPrivate = parseBoolEnv("FORNIX_CREDENTIAL_MANAGER_ALLOW_PRIVATE")
	c.EnableLegacyGlobalSurfaces = parseBoolEnv("FORNIX_ENABLE_LEGACY_GLOBAL_SURFACES")
	c.RequireSignedAuthority = c.Environment == "production" || parseBoolEnv("FORNIX_REQUIRE_SIGNED_AUTHORITY")
	c.RequireQualificationTrust = c.Environment == "production" || parseBoolEnv("FORNIX_REQUIRE_QUALIFICATION_TRUST")
	c.QualificationDeploymentID = strings.TrimSpace(firstEnvOr("FORNIX_QUALIFICATION_DEPLOYMENT_ID", "", ""))
	c.RequireQualificationRelease = c.Environment == "production" || parseBoolEnv("FORNIX_REQUIRE_QUALIFICATION_RELEASE")
	c.QualificationReleaseID = strings.TrimSpace(firstEnvOr("FORNIX_QUALIFICATION_RELEASE_ID", "", ""))
	c.QualificationReleaseArtifactKind = strings.ToLower(strings.TrimSpace(firstEnvOr("FORNIX_QUALIFICATION_RELEASE_ARTIFACT_KIND", "", "release")))
	c.RequireReleaseAdmissionForEffects = c.Environment == "production" || parseBoolEnv("FORNIX_REQUIRE_RELEASE_ADMISSION_FOR_EFFECTS")
	c.RequireExternalBoundaryAuthority = c.Environment == "production" || parseBoolEnv("FORNIX_REQUIRE_EXTERNAL_BOUNDARY_AUTHORITY")
	if c.Environment == "production" && c.EnableLegacyGlobalSurfaces {
		return Config{}, fmt.Errorf("FORNIX_ENABLE_LEGACY_GLOBAL_SURFACES is not allowed in production")
	}
	if c.OpenAIEnabled {
		explicitCredentialRef := strings.TrimSpace(os.Getenv("FORNIX_OPENAI_CREDENTIAL_REF"))
		managedReference := explicitCredentialRef != "" && strings.Contains(explicitCredentialRef, "/")
		if c.Environment == "production" {
			if !isHTTPSURL(c.OpenAIBaseURL) {
				return Config{}, fmt.Errorf("FORNIX_OPENAI_BASE_URL must use HTTPS in production")
			}
			if !managedReference || profile.ValidateReference(explicitCredentialRef) != nil {
				return Config{}, fmt.Errorf("FORNIX_OPENAI_CREDENTIAL_REF must be an explicit namespaced logical credential reference in production")
			}
			if !isHTTPSURL(c.CredentialManagerURL) {
				return Config{}, fmt.Errorf("FORNIX_CREDENTIAL_MANAGER_URL must be an HTTPS URL for production OpenAI credentials")
			}
			c.OpenAICredentialRef = explicitCredentialRef
		} else if managedReference {
			if profile.ValidateReference(explicitCredentialRef) != nil {
				return Config{}, fmt.Errorf("FORNIX_OPENAI_CREDENTIAL_REF is invalid")
			}
			if c.CredentialManagerURL == "" {
				return Config{}, fmt.Errorf("FORNIX_CREDENTIAL_MANAGER_URL is required for a managed OpenAI credential reference")
			}
			c.OpenAICredentialRef = explicitCredentialRef
		} else if strings.TrimSpace(os.Getenv(c.OpenAICredentialRef)) == "" {
			return Config{}, fmt.Errorf("%s is required when FORNIX_OPENAI_ENABLED is true", c.OpenAICredentialRef)
		}
	}
	if c.RequireQualificationTrust && c.QualificationDeploymentID == "" {
		return Config{}, fmt.Errorf("FORNIX_QUALIFICATION_DEPLOYMENT_ID is required when qualification trust is required")
	}
	if c.RequireQualificationRelease && c.QualificationDeploymentID == "" {
		return Config{}, fmt.Errorf("FORNIX_QUALIFICATION_DEPLOYMENT_ID is required when release admission is required")
	}
	if c.QualificationReleaseArtifactKind != "release" && c.QualificationReleaseArtifactKind != "image" && c.QualificationReleaseArtifactKind != "binary" && c.QualificationReleaseArtifactKind != "manifest" {
		return Config{}, fmt.Errorf("FORNIX_QUALIFICATION_RELEASE_ARTIFACT_KIND must be release, image, binary, or manifest")
	}
	if c.RequireQualificationRelease && c.QualificationReleaseID == "" {
		return Config{}, fmt.Errorf("FORNIX_QUALIFICATION_RELEASE_ID is required when release admission is required")
	}
	if seconds, present, err := optionalInt("FORNIX_CREDENTIAL_MANAGER_TIMEOUT_SECONDS"); err != nil {
		return Config{}, err
	} else if present {
		c.CredentialManagerTimeout = time.Duration(seconds) * time.Second
	}
	if c.CredentialManagerTimeout <= 0 || c.CredentialManagerTimeout > maxCredentialManagerTimeout {
		return Config{}, fmt.Errorf("FORNIX_CREDENTIAL_MANAGER_TIMEOUT_SECONDS must be between 1 and %d", int(maxCredentialManagerTimeout/time.Second))
	}
	if seconds, present, err := optionalInt("FORNIX_FEDERATION_RETENTION_INTERVAL_SECONDS"); err != nil {
		return Config{}, err
	} else if present {
		c.FederationRetentionInterval = time.Duration(seconds) * time.Second
	}
	if c.FederationRetentionInterval < minFederationRetentionInterval || c.FederationRetentionInterval > maxFederationRetentionInterval {
		return Config{}, fmt.Errorf("FORNIX_FEDERATION_RETENTION_INTERVAL_SECONDS must be between 1 and %d", int(maxFederationRetentionInterval/time.Second))
	}
	if value, present, err := optionalBoundedInt("FORNIX_FEDERATION_RETENTION_BATCH_SIZE", 1, 500); err != nil {
		return Config{}, err
	} else if present {
		c.FederationRetentionBatchSize = value
	}
	if c.FederationRetentionBatchSize < 1 || c.FederationRetentionBatchSize > 500 {
		return Config{}, fmt.Errorf("FORNIX_FEDERATION_RETENTION_BATCH_SIZE must be between 1 and 500")
	}
	if value, present, err := optionalBoundedInt("FORNIX_FEDERATION_RETENTION_WORKSPACE_LIMIT", 1, 1000); err != nil {
		return Config{}, err
	} else if present {
		c.FederationRetentionWorkspaceLimit = value
	}
	if c.FederationRetentionWorkspaceLimit < 1 || c.FederationRetentionWorkspaceLimit > 1000 {
		return Config{}, fmt.Errorf("FORNIX_FEDERATION_RETENTION_WORKSPACE_LIMIT must be between 1 and 1000")
	}
	if seconds, present, err := optionalInt("FORNIX_OPENAI_TIMEOUT_SECONDS"); err != nil {
		return Config{}, err
	} else if present {
		c.OpenAITimeout = time.Duration(seconds) * time.Second
	}
	if c.OpenAITimeout <= 0 || c.OpenAITimeout > maxModelTimeout {
		return Config{}, fmt.Errorf("FORNIX_OPENAI_TIMEOUT_SECONDS must be between 1 and %d", int(maxModelTimeout/time.Second))
	}

	var err error
	if c.MaxBodyBytes, err = positiveInt64("FORNIX_MAX_BODY_BYTES", c.MaxBodyBytes); err != nil {
		return Config{}, err
	}
	if seconds, present, err := optionalInt("FORNIX_SHUTDOWN_TIMEOUT_SECONDS"); err != nil {
		return Config{}, err
	} else if present {
		c.ShutdownTimeout = time.Duration(seconds) * time.Second
	}
	if c.DBMaxConnections, err = positiveInt32("FORNIX_DB_MAX_CONNS", c.DBMaxConnections); err != nil {
		return Config{}, err
	}
	if c.DBMinConnections, err = nonNegativeInt32("FORNIX_DB_MIN_CONNS", c.DBMinConnections); err != nil {
		return Config{}, err
	}
	if c.DBMinConnections > c.DBMaxConnections {
		return Config{}, fmt.Errorf("FORNIX_DB_MIN_CONNS cannot exceed FORNIX_DB_MAX_CONNS")
	}
	if value, present, err := optionalBoundedInt("FORNIX_OPERATION_WORKER_MAX_CONCURRENT", 1, maxOperationWorkerMaxConcurrent); err != nil {
		return Config{}, err
	} else if present {
		c.OperationWorkerMaxConcurrent = value
	}
	if c.OperationWorkerMaxConcurrent < 1 || c.OperationWorkerMaxConcurrent > maxOperationWorkerMaxConcurrent {
		return Config{}, fmt.Errorf("FORNIX_OPERATION_WORKER_MAX_CONCURRENT must be between 1 and %d", maxOperationWorkerMaxConcurrent)
	}
	if value, present, err := optionalBoundedInt("FORNIX_OPERATION_WORKER_CLAIM_BATCH", 1, maxOperationWorkerClaimBatch); err != nil {
		return Config{}, err
	} else if present {
		c.OperationWorkerClaimBatch = value
	}
	if c.OperationWorkerClaimBatch < 1 || c.OperationWorkerClaimBatch > maxOperationWorkerClaimBatch {
		return Config{}, fmt.Errorf("FORNIX_OPERATION_WORKER_CLAIM_BATCH must be between 1 and %d", maxOperationWorkerClaimBatch)
	}
	if value, present, err := optionalBoundedInt("FORNIX_OPERATION_WORKER_MAX_ACTIVE", 1, maxOperationWorkerMaxActive); err != nil {
		return Config{}, err
	} else if present {
		c.OperationWorkerMaxActive = value
	}
	if c.OperationWorkerMaxActive < 1 || c.OperationWorkerMaxActive > maxOperationWorkerMaxActive {
		return Config{}, fmt.Errorf("FORNIX_OPERATION_WORKER_MAX_ACTIVE must be between 1 and %d", maxOperationWorkerMaxActive)
	}
	if seconds, present, err := optionalInt("FORNIX_OPERATION_WORKER_POLL_INTERVAL_SECONDS"); err != nil {
		return Config{}, err
	} else if present {
		c.OperationWorkerPollInterval = time.Duration(seconds) * time.Second
	}
	if c.OperationWorkerPollInterval < minOperationWorkerPollInterval || c.OperationWorkerPollInterval > maxOperationWorkerPollInterval {
		return Config{}, fmt.Errorf("FORNIX_OPERATION_WORKER_POLL_INTERVAL_SECONDS must be between 1 and %d", int(maxOperationWorkerPollInterval/time.Second))
	}
	if raw := strings.TrimSpace(os.Getenv("FORNIX_WORKER_ENABLED")); raw != "" {
		switch strings.ToLower(raw) {
		case "1", "true", "yes", "on":
			c.WorkerEnabled = true
		case "0", "false", "no", "off":
			c.WorkerEnabled = false
		default:
			return Config{}, fmt.Errorf("FORNIX_WORKER_ENABLED must be a boolean")
		}
	}
	return c, nil
}

func isHTTPSURL(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	return err == nil && strings.EqualFold(parsed.Scheme, "https") && parsed.Hostname() != "" && parsed.User == nil
}

const (
	defaultModelTimeout                      = 30 * time.Second
	maxModelTimeout                          = 10 * time.Minute
	defaultCredentialManagerTimeout          = 10 * time.Second
	maxCredentialManagerTimeout              = 2 * time.Minute
	defaultFederationRetentionInterval       = time.Hour
	minFederationRetentionInterval           = time.Second
	maxFederationRetentionInterval           = 24 * time.Hour
	defaultFederationRetentionBatchSize      = 100
	defaultFederationRetentionWorkspaceLimit = 100
	defaultOperationWorkerMaxConcurrent      = 4
	maxOperationWorkerMaxConcurrent          = 64
	defaultOperationWorkerClaimBatch         = 8
	maxOperationWorkerClaimBatch             = 64
	defaultOperationWorkerMaxActive          = 4
	maxOperationWorkerMaxActive              = 4096
	defaultOperationWorkerPollInterval       = 2 * time.Second
	minOperationWorkerPollInterval           = time.Second
	maxOperationWorkerPollInterval           = 5 * time.Minute
)

func parseBoolEnv(name string) bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv(name))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func firstEnv(names ...string) string {
	for _, name := range names {
		if value := strings.TrimSpace(os.Getenv(name)); value != "" {
			return value
		}
	}
	return ""
}

func firstEnvOr(first, second, fallback string) string {
	if value := firstEnv(first, second); value != "" {
		return value
	}
	return fallback
}

func positiveInt64(name string, fallback int64) (int64, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", name)
	}
	return n, nil
}

func optionalInt(name string) (int64, bool, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return 0, false, nil
	}
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil || n <= 0 {
		return 0, true, fmt.Errorf("%s must be a positive integer", name)
	}
	return n, true, nil
}

func optionalBoundedInt(name string, minimum, maximum int) (int, bool, error) {
	value, present, err := optionalInt(name)
	if err != nil || !present {
		return 0, present, err
	}
	if value < int64(minimum) || value > int64(maximum) {
		return 0, false, fmt.Errorf("%s must be between %d and %d", name, minimum, maximum)
	}
	return int(value), true, nil
}

func positiveInt32(name string, fallback int32) (int32, error) {
	n, err := positiveInt64(name, int64(fallback))
	if err != nil || n > int64(^uint32(0)>>1) {
		if err != nil {
			return 0, err
		}
		return 0, fmt.Errorf("%s is too large", name)
	}
	return int32(n), nil
}

func nonNegativeInt32(name string, fallback int32) (int32, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	n, err := strconv.ParseInt(value, 10, 32)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("%s must be a non-negative integer", name)
	}
	return int32(n), nil
}
