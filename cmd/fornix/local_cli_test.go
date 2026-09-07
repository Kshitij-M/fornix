package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/omaveda/fornix/internal/credentials"
	"github.com/omaveda/fornix/internal/profile"
	"github.com/omaveda/fornix/internal/version"
)

func TestParseLocalOptionsKeepsPromptAndValidatesBudgets(t *testing.T) {
	opts, err := parseLocalOptions([]string{
		"run", "--repo", ".", "--provider", "fake", "--max-cost", "0.25",
		"--max-time", "2s", "--max-turns", "4", "--max-output-tokens=128",
		"--max-context-bytes", "4096", "--max-context-tokens", "256",
		"--port", "18281",
		"Review", "the", "repository",
	})
	if err != nil {
		t.Fatal(err)
	}
	if opts.repository != "." || opts.provider != "fake" || opts.maxCost != 0.25 || opts.maxTurns != 4 || opts.maxOutput != 128 || opts.maxContextB != 4096 || opts.maxContextTok != 256 || opts.port != 18281 {
		t.Fatalf("parsed options = %+v", opts)
	}
	if opts.prompt != "Review the repository" {
		t.Fatalf("prompt = %q", opts.prompt)
	}
	for _, test := range []struct {
		name string
		args []string
	}{
		{name: "zero turns", args: []string{"run", "--max-turns", "0", "prompt"}},
		{name: "bad cost", args: []string{"run", "--max-cost", "NaN", "prompt"}},
		{name: "bad provider", args: []string{"run", "--provider", "unknown", "prompt"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := parseLocalOptions(test.args); err == nil {
				t.Fatal("parse unexpectedly succeeded")
			}
		})
	}
}

func TestParseLocalOptionsDefaultsModelForDurableProviderIdentity(t *testing.T) {
	t.Setenv("FORNIX_OPENAI_MODEL", "")
	options, err := parseLocalOptions([]string{"run", "offline smoke"})
	if err != nil {
		t.Fatal(err)
	}
	if options.provider != "fake" || options.model != "fake-model" {
		t.Fatalf("defaults = provider %q model %q", options.provider, options.model)
	}
	options, err = parseLocalOptions([]string{"run", "--provider", "openai", "--model", "gpt-test", "remote smoke"})
	if err != nil {
		t.Fatal(err)
	}
	if options.model != "gpt-test" {
		t.Fatalf("explicit model = %q", options.model)
	}
}

func TestParseLocalOptionsAcceptsLifecycleCompatibilityFlags(t *testing.T) {
	start, err := parseLocalOptions([]string{"start", "--detach", "--pull", "--repo", "."})
	if err != nil {
		t.Fatal(err)
	}
	if !start.detach || !start.pull || start.repository != "." {
		t.Fatalf("start options = %+v", start)
	}
	stop, err := parseLocalOptions([]string{"stop", "--keep-data"})
	if err != nil {
		t.Fatal(err)
	}
	if !stop.keepData {
		t.Fatalf("stop options = %+v", stop)
	}
}

func TestResolvedLocalPortUsesExplicitEnvironmentProfileAndDefaultPrecedence(t *testing.T) {
	t.Setenv("FORNIX_PORT", "19001")
	metadata := profile.Metadata{Port: 18001}
	if got, err := resolvedLocalPort(localOptions{}, metadata); err != nil || got != 19001 {
		t.Fatalf("environment port = %d, %v", got, err)
	}
	if got, err := resolvedLocalPort(localOptions{port: 20001}, metadata); err != nil || got != 20001 {
		t.Fatalf("explicit port = %d, %v", got, err)
	}
	t.Setenv("FORNIX_PORT", "")
	if got, err := resolvedLocalPort(localOptions{}, metadata); err != nil || got != 18001 {
		t.Fatalf("profile port = %d, %v", got, err)
	}
	if got, err := resolvedLocalPort(localOptions{}, profile.Metadata{}); err != nil || got != 8201 {
		t.Fatalf("default port = %d, %v", got, err)
	}
}

func TestOpenLocalSessionBootstrapsPrivateProfileAndCredentialReferences(t *testing.T) {
	home := filepath.Join(t.TempDir(), "fornix")
	session, err := openLocalSession(localOptions{home: home, workspace: "workspace-1"}, true)
	if err != nil {
		t.Fatal(err)
	}
	if session.profile.WorkspaceID != "workspace-1" || session.profile.CredentialRef != "local/api" {
		t.Fatalf("profile = %+v", session.profile)
	}
	metadata, err := os.ReadFile(filepath.Join(home, "profile.json"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(metadata), "fornix_db_") || strings.Contains(string(metadata), "fornix_bootstrap_") {
		t.Fatal("generated credential material entered profile metadata")
	}
	if info, err := os.Stat(home); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("profile directory permissions: info=%v err=%v", info, err)
	}
	for _, reference := range []string{localDatabaseRef, localBootstrapRef} {
		ref, err := credentials.ParseRef(reference)
		if err != nil {
			t.Fatal(err)
		}
		secret, err := session.credentials.Read(ref)
		if err != nil {
			t.Fatalf("read %s: %v", reference, err)
		}
		if secret.Len() == 0 {
			t.Fatalf("empty %s", reference)
		}
		secret.Clear()
	}
	loaded, err := session.store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded != session.profile {
		t.Fatalf("loaded profile = %+v, session profile = %+v", loaded, session.profile)
	}
}

func TestLocalRuntimeEnvironmentOverridesHostSecretsInMemoryOnly(t *testing.T) {
	t.Setenv("FORNIX_OPENAI_API_KEY", "provider-secret")
	t.Setenv("FORNIX_WORKER_DISABLED", "true")
	environment := localRuntimeEnvironment([]byte("db-secret"), []byte("bootstrap-secret"), false)
	joined := strings.Join(environment, "\n")
	for _, expected := range []string{
		"FORNIX_DATABASE_PASSWORD=db-secret",
		"FORNIX_BOOTSTRAP_KEY=bootstrap-secret",
		"FORNIX_WORKER_ENABLED=false",
		"FORNIX_OPENAI_ENABLED=false",
		"FORNIX_OPENAI_API_KEY=",
	} {
		if !strings.Contains(joined, expected) {
			t.Errorf("environment missing %q", expected)
		}
	}
	if strings.Contains(joined, "FORNIX_OPENAI_API_KEY=provider-secret") {
		t.Fatal("disabled provider key was passed to runtime")
	}
}

func TestPrintVersionJSONIsMachineReadableWithoutSecrets(t *testing.T) {
	info := version.Current()
	data, err := json.Marshal(info)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "secret") || strings.Contains(string(data), "password") {
		t.Fatal("version output contains credential-like data")
	}
}
