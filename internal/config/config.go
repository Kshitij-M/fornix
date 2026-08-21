package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
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
	Environment         string
	MaxBodyBytes        int64
	ShutdownTimeout     time.Duration
	DBMaxConnections    int32
	DBMinConnections    int32
	WorkerEnabled       bool
}

func Load() (Config, error) {
	c := Config{
		APIKey:              firstEnv("FORNIX_KEY"),
		BootstrapKey:        firstEnv("FORNIX_BOOTSTRAP_KEY"),
		AuthMode:            strings.ToLower(firstEnvOr("FORNIX_AUTH_MODE", "", "workspace")),
		DSN:                 firstEnv("FORNIX_PG_DSN"),
		Listen:              firstEnvOr("FORNIX_LISTEN", "", defaultListen),
		OllamaURL:           strings.TrimRight(firstEnvOr("FORNIX_OLLAMA_URL", "OLLAMA_URL", defaultOllamaURL), "/"),
		OpenAIBaseURL:       strings.TrimRight(firstEnvOr("FORNIX_OPENAI_BASE_URL", "", defaultOpenAIURL), "/"),
		OpenAIModel:         firstEnvOr("FORNIX_OPENAI_MODEL", "", defaultOpenAIModel),
		OpenAITimeout:       defaultModelTimeout,
		OpenAICredentialRef: firstEnvOr("FORNIX_OPENAI_CREDENTIAL_REF", "", "FORNIX_OPENAI_API_KEY"),
		Environment:         strings.ToLower(firstEnvOr("FORNIX_ENV", "", "development")),
		MaxBodyBytes:        defaultMaxBodyBytes,
		ShutdownTimeout:     defaultShutdownWindow,
		DBMaxConnections:    20,
		DBMinConnections:    2,
		WorkerEnabled:       true,
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
	if c.OpenAIEnabled && strings.TrimSpace(os.Getenv(c.OpenAICredentialRef)) == "" {
		return Config{}, fmt.Errorf("%s is required when FORNIX_OPENAI_ENABLED is true", c.OpenAICredentialRef)
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

const (
	defaultModelTimeout = 30 * time.Second
	maxModelTimeout     = 10 * time.Minute
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
