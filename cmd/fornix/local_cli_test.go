package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
	support, err := parseLocalOptions([]string{"support", "bundle", "--output", "/tmp/fornix-support.json"})
	if err != nil || support.output != "/tmp/fornix-support.json" {
		t.Fatalf("support bundle options = %+v, err=%v", support, err)
	}
	upgrade, err := parseLocalOptions([]string{"upgrade", "--version", "v1.2.3", "--dry-run"})
	if err != nil || upgrade.runtimeVersion != "1.2.3" || !upgrade.dryRun {
		t.Fatalf("upgrade options = %+v, err=%v", upgrade, err)
	}
}

func TestLocalSupportBundleIsBoundedAndExcludesPrivateProfileFields(t *testing.T) {
	metadata := profile.Metadata{
		SchemaVersion:   profile.SchemaVersion,
		Name:            "support-test",
		ServerURL:       "https://private-host.invalid/private-route",
		WorkspaceID:     "workspace-secret-sentinel",
		WorkspaceName:   "workspace-name-sentinel",
		ActorID:         "actor-secret-sentinel",
		CredentialRef:   "credential-reference-sentinel",
		RuntimeVersion:  "runtime-version-sentinel",
		RuntimeProject:  "runtime-project-sentinel",
		DataVolume:      "data-volume-sentinel",
		RepositoryMount: "/private/repository/sentinel",
		Migration:       123,
	}
	bundle := newLocalSupportBundle(metadata, "initialized", time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC))
	data, err := marshalLocalSupportBundle(bundle)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) > maxLocalSupportBundleBytes {
		t.Fatalf("bundle is %d bytes, limit is %d", len(data), maxLocalSupportBundleBytes)
	}
	for _, secret := range []string{
		"private-host.invalid", "private-route", "workspace-secret-sentinel", "workspace-name-sentinel",
		"actor-secret-sentinel", "credential-reference-sentinel", "runtime-version-sentinel",
		"runtime-project-sentinel", "data-volume-sentinel", "/private/repository/sentinel",
	} {
		if strings.Contains(string(data), secret) {
			t.Errorf("support bundle contains private sentinel %q: %s", secret, data)
		}
	}
	var decoded localSupportBundle
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.SchemaVersion != localSupportBundleSchemaVersion || !decoded.Redacted || decoded.ProfileState != "initialized" || decoded.ProfileSchema != profile.SchemaVersion || decoded.Migration != 123 {
		t.Fatalf("support bundle metadata = %+v", decoded)
	}
	if !decoded.ServerConfigured || !decoded.RuntimeConfigured {
		t.Fatalf("safe configuration indicators were not retained: %+v", decoded)
	}
}

func TestLocalSupportBundleMissingOrInvalidProfileContainsOnlyCoarseState(t *testing.T) {
	for _, state := range []string{"missing", "invalid"} {
		t.Run(state, func(t *testing.T) {
			bundle := newLocalSupportBundle(profile.Metadata{
				SchemaVersion:   profile.SchemaVersion,
				WorkspaceID:     "private-workspace-sentinel",
				ServerURL:       "https://private.example/path",
				CredentialRef:   "private-credential-reference",
				RepositoryMount: "/private/repo",
			}, state, time.Unix(1, 0))
			data, err := marshalLocalSupportBundle(bundle)
			if err != nil {
				t.Fatal(err)
			}
			if bundle.ProfileState != state || bundle.ProfileSchema != 0 || bundle.ServerConfigured || bundle.RuntimeConfigured || bundle.Migration != 0 {
				t.Fatalf("non-initialized state disclosed profile facts: %+v", bundle)
			}
			for _, secret := range []string{"private-workspace-sentinel", "private.example", "private-credential-reference", "/private/repo"} {
				if strings.Contains(string(data), secret) {
					t.Errorf("%s profile bundle contains private sentinel %q", state, secret)
				}
			}
		})
	}
}

func TestWriteLocalSupportBundleUsesPrivateExclusiveCreation(t *testing.T) {
	t.Run("creates mode 0600", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "support.json")
		if err := writeLocalSupportBundle(path, []byte("{}\n")); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("support bundle permissions = %04o, want 0600", got)
		}
	})
	t.Run("does not alter existing file", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "existing.json")
		original := []byte("do not overwrite")
		if err := os.WriteFile(path, original, 0o644); err != nil {
			t.Fatal(err)
		}
		if err := writeLocalSupportBundle(path, []byte("replacement")); err == nil {
			t.Fatal("write unexpectedly replaced an existing destination")
		}
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		if string(got) != string(original) {
			t.Fatalf("existing file content changed: %q", got)
		}
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o644 {
			t.Fatalf("existing file permissions changed to %04o", got)
		}
	})
	t.Run("rejects oversized output", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "oversized.json")
		if err := writeLocalSupportBundle(path, make([]byte, maxLocalSupportBundleBytes+1)); err == nil {
			t.Fatal("oversized support bundle was accepted")
		}
		if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("oversized output created a file: %v", err)
		}
	})
}

func TestLocalSupportBundleDoesNotWriteAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := filepath.Join(t.TempDir(), "support.json")
	err := runLocalSupportBundle(ctx, localOptions{output: path})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled support command error = %v", err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canceled support command created a file: %v", err)
	}
}

func TestRunLocalSupportBundleDoesNotMutateProfileRoot(t *testing.T) {
	t.Run("missing root remains missing", func(t *testing.T) {
		base := t.TempDir()
		home := filepath.Join(base, "missing-profile")
		output := filepath.Join(base, "support.json")
		if err := runLocalSupportBundle(context.Background(), localOptions{home: home, output: output, json: true}); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(home); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("support command created missing profile root: %v", err)
		}
		data, err := os.ReadFile(output)
		if err != nil {
			t.Fatal(err)
		}
		var bundle localSupportBundle
		if err := json.Unmarshal(data, &bundle); err != nil {
			t.Fatal(err)
		}
		if bundle.ProfileState != "missing" {
			t.Fatalf("missing profile status = %q", bundle.ProfileState)
		}
	})

	t.Run("insecure root is not chmodded", func(t *testing.T) {
		base := t.TempDir()
		home := filepath.Join(base, "insecure-profile")
		if err := os.Mkdir(home, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(home, 0o755); err != nil {
			t.Fatal(err)
		}
		output := filepath.Join(base, "support.json")
		if err := runLocalSupportBundle(context.Background(), localOptions{home: home, output: output, json: true}); err != nil {
			t.Fatal(err)
		}
		info, err := os.Stat(home)
		if err != nil {
			t.Fatal(err)
		}
		if got := info.Mode().Perm(); got != 0o755 {
			t.Fatalf("support command changed profile root mode to %04o", got)
		}
		data, err := os.ReadFile(output)
		if err != nil {
			t.Fatal(err)
		}
		var bundle localSupportBundle
		if err := json.Unmarshal(data, &bundle); err != nil {
			t.Fatal(err)
		}
		if bundle.ProfileState != "invalid" {
			t.Fatalf("insecure profile status = %q", bundle.ProfileState)
		}
	})
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

func TestCompletionScriptsAreDeterministicAndSecretFree(t *testing.T) {
	for _, shell := range []string{"bash", "zsh", "fish"} {
		first, err := completionScript(shell)
		if err != nil {
			t.Fatal(err)
		}
		second, err := completionScript(shell)
		if err != nil {
			t.Fatal(err)
		}
		if first != second || !strings.Contains(first, "fornix") || strings.Contains(first, "OPENAI_API_KEY") {
			t.Fatalf("completion output for %s is unstable or contains a secret-like value", shell)
		}
	}
	if _, err := completionScript("powershell"); err == nil {
		t.Fatal("unsupported completion shell was accepted")
	}
}
