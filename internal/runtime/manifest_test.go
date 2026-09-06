package runtime

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRenderManifestHasSafeDeterministicDefaults(t *testing.T) {
	config, err := DefaultManifestConfig("v0.10.1")
	if err != nil {
		t.Fatalf("DefaultManifestConfig: %v", err)
	}
	first, err := RenderManifest(config)
	if err != nil {
		t.Fatalf("RenderManifest first: %v", err)
	}
	second, err := RenderManifest(config)
	if err != nil {
		t.Fatalf("RenderManifest second: %v", err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("manifest rendering is not deterministic")
	}

	manifest := string(first)
	required := []string{
		`image: "ghcr.io/kshitij-m/fornix:v0.10.1"`,
		`image: "pgvector/pgvector@sha256:cf134a767f474095eeba57e0117be8e568e011a63f33fbf252f14c9b760f8e6f"`,
		`FORNIX_AUTH_MODE: "workspace"`,
		`"127.0.0.1:8201:8201"`,
		`fornix-postgres-data:/var/lib/postgresql/data`,
		`condition: service_healthy`,
		`pg_isready -U fornix -d fornix`,
		`/usr/local/bin/fornix`,
		`driver: bridge`,
	}
	for _, value := range required {
		if !strings.Contains(manifest, value) {
			t.Errorf("manifest does not contain %q", value)
		}
	}
	forbidden := []string{
		"5432:5432",
		"0.0.0.0:8201",
		"FORNIX_AUTH_MODE: \"development\"",
		":latest",
		"{{",
	}
	for _, value := range forbidden {
		if strings.Contains(manifest, value) {
			t.Errorf("manifest unexpectedly contains %q", value)
		}
	}
}

func TestManifestConfigRejectsMutableOrInjectedValues(t *testing.T) {
	tests := []ManifestConfig{
		{FornixImage: "fornix:latest", PostgresImage: "postgres:17", AppPort: 8201},
		{FornixImage: "fornix:1", PostgresImage: "postgres", AppPort: 8201},
		{FornixImage: "${IMAGE}", PostgresImage: "postgres:17", AppPort: 8201},
		{FornixImage: "fornix:1\nprivileged: true", PostgresImage: "postgres:17", AppPort: 8201},
		{FornixImage: "fornix:1", PostgresImage: "postgres:17", AppPort: 80},
	}
	for index, config := range tests {
		if err := config.Validate(); !errors.Is(err, ErrInvalidManifestConfig) {
			t.Fatalf("case %d error = %v, want ErrInvalidManifestConfig", index, err)
		}
	}
}

func TestRenderManifestAddsOnlyReadOnlyRepositoryMount(t *testing.T) {
	repository := filepath.Join(t.TempDir(), "repository")
	if err := os.Mkdir(repository, 0o700); err != nil {
		t.Fatal(err)
	}
	config, err := DefaultManifestConfig("v0.10.1")
	if err != nil {
		t.Fatal(err)
	}
	config.RepositoryPath = repository
	manifest, err := RenderManifest(config)
	if err != nil {
		t.Fatal(err)
	}
	text := string(manifest)
	for _, expected := range []string{
		"volumes:",
		`target: /workspace/repository`,
		"read_only: true",
	} {
		if !strings.Contains(text, expected) {
			t.Errorf("manifest missing %q", expected)
		}
	}
	if strings.Contains(text, "REPOSITORY_VOLUME") {
		t.Fatal("repository template token was not resolved")
	}
}

func TestManifestRejectsRepositoryPathTraversalAndMissingDirectory(t *testing.T) {
	config, err := DefaultManifestConfig("v0.10.1")
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{
		"relative",
		filepath.Join(t.TempDir(), "missing"),
		filepath.Join(t.TempDir(), "..", "outside"),
	} {
		config.RepositoryPath = path
		if err := config.Validate(); !errors.Is(err, ErrInvalidManifestConfig) {
			t.Errorf("repository path %q error = %v, want ErrInvalidManifestConfig", path, err)
		}
	}
}
