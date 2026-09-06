package runtime

import (
	_ "embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

var (
	// ErrInvalidManifestConfig reports unsafe or incomplete image and network
	// settings for the embedded local runtime.
	ErrInvalidManifestConfig = errors.New("invalid runtime manifest configuration")

	//go:embed assets/compose.v1.yaml
	composeTemplate string

	// defaultPostgresImage is pinned by manifest digest so a fresh local start
	// does not silently change database bits when a registry tag moves. The
	// image is a multi-platform pgvector/pg17 manifest.
	defaultPostgresImage = "pgvector/pgvector@sha256:cf134a767f474095eeba57e0117be8e568e011a63f33fbf252f14c9b760f8e6f"

	imageReferencePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._/@:+-]*$`)
)

// ManifestConfig selects immutable container artifacts and the loopback port
// used by the local runtime. Credentials are deliberately absent: the manifest
// references required environment variables without persisting their values.
type ManifestConfig struct {
	FornixImage   string
	PostgresImage string
	AppPort       uint16
	// RepositoryPath is an optional, resolved host directory mounted read-only
	// at /workspace/repository.
	RepositoryPath string
}

// DefaultManifestConfig returns safe, versioned local-runtime defaults for a
// Fornix release tag. releaseVersion must be a concrete tag, never "latest".
func DefaultManifestConfig(releaseVersion string) (ManifestConfig, error) {
	releaseVersion = strings.TrimSpace(releaseVersion)
	if releaseVersion == "" || strings.ContainsAny(releaseVersion, "\t\r\n /@:") || strings.EqualFold(releaseVersion, "latest") {
		return ManifestConfig{}, fmt.Errorf("%w: release version must be a concrete image tag", ErrInvalidManifestConfig)
	}
	config := ManifestConfig{
		FornixImage:   "ghcr.io/kshitij-m/fornix:" + releaseVersion,
		PostgresImage: defaultPostgresImage,
		AppPort:       8201,
	}
	return config, config.Validate()
}

// Validate rejects mutable image tags, image references containing Compose
// interpolation, privileged ports, and zero-valued settings.
func (c ManifestConfig) Validate() error {
	if err := validateImageReference("Fornix", c.FornixImage); err != nil {
		return err
	}
	if err := validateImageReference("PostgreSQL", c.PostgresImage); err != nil {
		return err
	}
	if c.AppPort < 1024 {
		return fmt.Errorf("%w: application port must be between 1024 and 65535", ErrInvalidManifestConfig)
	}
	if c.RepositoryPath != "" {
		if err := validateRepositoryPath(c.RepositoryPath); err != nil {
			return err
		}
	}
	return nil
}

// RenderManifest renders the embedded manifest deterministically. The result
// contains no credential values and always binds the application to loopback.
func RenderManifest(config ManifestConfig) ([]byte, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	repositoryPath := config.RepositoryPath
	if repositoryPath != "" {
		resolved, err := filepath.EvalSymlinks(repositoryPath)
		if err != nil {
			return nil, fmt.Errorf("%w: resolve repository path: %v", ErrInvalidManifestConfig, err)
		}
		repositoryPath = resolved
	}
	replacements := map[string]string{
		"{{FORNIX_IMAGE}}":      config.FornixImage,
		"{{POSTGRES_IMAGE}}":    config.PostgresImage,
		"{{APP_PORT}}":          strconv.FormatUint(uint64(config.AppPort), 10),
		"{{REPOSITORY_VOLUME}}": repositoryVolume(repositoryPath),
	}
	result := composeTemplate
	for _, token := range []string{"{{FORNIX_IMAGE}}", "{{POSTGRES_IMAGE}}", "{{APP_PORT}}", "{{REPOSITORY_VOLUME}}"} {
		result = strings.ReplaceAll(result, token, replacements[token])
	}
	if strings.Contains(result, "{{") || strings.Contains(result, "}}") {
		return nil, fmt.Errorf("%w: embedded manifest contains unresolved tokens", ErrInvalidManifestConfig)
	}
	return []byte(result), nil
}

func repositoryVolume(path string) string {
	if path == "" {
		return ""
	}
	return "    volumes:\n      - type: bind\n        source: " + strconv.Quote(path) + "\n        target: /workspace/repository\n        read_only: true\n"
}

func validateRepositoryPath(path string) error {
	if path == "" || strings.IndexByte(path, 0) >= 0 || !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fmt.Errorf("%w: repository path must be an absolute normalized directory", ErrInvalidManifestConfig)
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("%w: resolve repository path: %v", ErrInvalidManifestConfig, err)
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("%w: repository path must be a directory", ErrInvalidManifestConfig)
	}
	if resolved == string(filepath.Separator) {
		return fmt.Errorf("%w: repository path may not be filesystem root", ErrInvalidManifestConfig)
	}
	return nil
}

func validateImageReference(label, value string) error {
	if !imageReferencePattern.MatchString(value) || strings.Contains(value, "${") {
		return fmt.Errorf("%w: %s image reference is malformed", ErrInvalidManifestConfig, label)
	}
	lower := strings.ToLower(value)
	if lower == "latest" || strings.HasSuffix(lower, ":latest") {
		return fmt.Errorf("%w: %s image must not use the latest tag", ErrInvalidManifestConfig, label)
	}
	lastSlash := strings.LastIndexByte(value, '/')
	lastColon := strings.LastIndexByte(value, ':')
	if !strings.Contains(value, "@sha256:") && lastColon <= lastSlash {
		return fmt.Errorf("%w: %s image must include a tag or digest", ErrInvalidManifestConfig, label)
	}
	return nil
}
