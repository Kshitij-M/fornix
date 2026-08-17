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
	defaultMaxBodyBytes   = int64(4 << 20)
	defaultShutdownWindow = 10 * time.Second
)

type Config struct {
	APIKey           string
	DSN              string
	Listen           string
	OllamaURL        string
	Environment      string
	MaxBodyBytes     int64
	ShutdownTimeout  time.Duration
	DBMaxConnections int32
	DBMinConnections int32
}

func Load() (Config, error) {
	c := Config{
		APIKey:           firstEnv("FORNIX_KEY"),
		DSN:              firstEnv("FORNIX_PG_DSN"),
		Listen:           firstEnvOr("FORNIX_LISTEN", "", defaultListen),
		OllamaURL:        strings.TrimRight(firstEnvOr("FORNIX_OLLAMA_URL", "OLLAMA_URL", defaultOllamaURL), "/"),
		Environment:      strings.ToLower(firstEnvOr("FORNIX_ENV", "", "development")),
		MaxBodyBytes:     defaultMaxBodyBytes,
		ShutdownTimeout:  defaultShutdownWindow,
		DBMaxConnections: 20,
		DBMinConnections: 2,
	}
	if c.APIKey == "" {
		return Config{}, fmt.Errorf("FORNIX_KEY is required")
	}
	if c.DSN == "" {
		return Config{}, fmt.Errorf("FORNIX_PG_DSN is required")
	}
	if c.Environment == "production" && (len(c.APIKey) < 32 || strings.Contains(c.APIKey, "dev-only")) {
		return Config{}, fmt.Errorf("production requires a non-default API key of at least 32 characters")
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
	return c, nil
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
