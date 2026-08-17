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
		"FORNIX_ENV", "FORNIX_MAX_BODY_BYTES",
		"FORNIX_SHUTDOWN_TIMEOUT_SECONDS", "FORNIX_DB_MAX_CONNS", "FORNIX_DB_MIN_CONNS",
	} {
		t.Setenv(name, "")
	}
}

func TestLoadCanonicalDefaults(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FORNIX_KEY", strings.Repeat("k", 32))
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
}

func TestLoadOptionalAliases(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("OLLAMA_URL", "http://ollama.internal/")
	t.Setenv("FORNIX_KEY", "optional-key")
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
	t.Setenv("FORNIX_PG_DSN", "postgres://fornix@localhost/fornix")
	t.Setenv("FORNIX_ENV", "production")

	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "production requires") {
		t.Fatalf("Load() error = %v, want production key validation", err)
	}
}

func TestLoadParsesLimits(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("FORNIX_KEY", "key")
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
