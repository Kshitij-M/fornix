package profile

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	// SchemaVersion is the on-disk profile metadata schema understood by this
	// package. Unknown versions fail closed rather than being partially loaded.
	SchemaVersion = 1
	// DirectoryMode is the required permission mode for the profile root and
	// its private subdirectories on permission-aware operating systems.
	DirectoryMode os.FileMode = 0o700
	// FileMode is the required permission mode for profile metadata and lock
	// files on permission-aware operating systems.
	FileMode os.FileMode = 0o600
	// MaxMetadataBytes bounds local metadata reads and writes.
	MaxMetadataBytes = 64 << 10

	metadataFilename = "profile.json"
)

var (
	// ErrInvalidRoot identifies an empty, relative, filesystem-root, or
	// otherwise unsupported profile root.
	ErrInvalidRoot = errors.New("invalid profile root")
	// ErrUnsafePath identifies a symlink or non-directory at a private
	// directory boundary, or a non-regular metadata file.
	ErrUnsafePath = errors.New("unsafe profile path")
	// ErrInvalidMetadata identifies metadata that does not satisfy the stable
	// profile schema and identifier rules.
	ErrInvalidMetadata = errors.New("invalid profile metadata")
	// ErrNotFound identifies a profile that has not been saved yet.
	ErrNotFound = errors.New("profile not found")
	// ErrInsecurePermissions identifies existing state that is readable or
	// writable by users other than the owner.
	ErrInsecurePermissions = errors.New("insecure profile permissions")
)

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9](?:[A-Za-z0-9._-]{0,126}[A-Za-z0-9])?$`)

// Metadata is the non-secret local profile document. CredentialRef identifies
// a value in a credential provider; it must never contain the credential
// itself. Empty optional fields support an atomic, staged first-run bootstrap.
type Metadata struct {
	SchemaVersion   int    `json:"schema_version"`
	Name            string `json:"name"`
	ServerURL       string `json:"server_url,omitempty"`
	Port            int    `json:"port,omitempty"`
	WorkspaceID     string `json:"workspace_id,omitempty"`
	WorkspaceName   string `json:"workspace_name,omitempty"`
	ActorID         string `json:"actor_id,omitempty"`
	CredentialRef   string `json:"credential_ref,omitempty"`
	RuntimeVersion  string `json:"runtime_version,omitempty"`
	RuntimeProject  string `json:"runtime_project,omitempty"`
	DataVolume      string `json:"data_volume,omitempty"`
	RepositoryMount string `json:"repository_mount,omitempty"`
	Migration       int    `json:"migration,omitempty"`
}

// NewMetadata returns the smallest valid metadata document for a named local
// profile. Callers may fill optional bootstrap fields before Save.
func NewMetadata(name string) Metadata {
	return Metadata{SchemaVersion: SchemaVersion, Name: name}
}

// Validate checks the stable on-disk schema without reading external state.
// Validation errors never include arbitrary field contents.
func (m Metadata) Validate() error {
	if m.SchemaVersion != SchemaVersion {
		return fmt.Errorf("%w: unsupported schema version", ErrInvalidMetadata)
	}
	if err := ValidateIdentifier(m.Name); err != nil {
		return fmt.Errorf("%w: invalid name", ErrInvalidMetadata)
	}
	identifiers := []struct {
		label string
		value string
	}{
		{label: "workspace_id", value: m.WorkspaceID},
		{label: "workspace_name", value: m.WorkspaceName},
		{label: "actor_id", value: m.ActorID},
		{label: "runtime_project", value: m.RuntimeProject},
		{label: "data_volume", value: m.DataVolume},
	}
	for _, identifier := range identifiers {
		if identifier.value != "" {
			if err := ValidateIdentifier(identifier.value); err != nil {
				return fmt.Errorf("%w: invalid %s", ErrInvalidMetadata, identifier.label)
			}
		}
	}
	if m.CredentialRef != "" {
		if err := ValidateReference(m.CredentialRef); err != nil {
			return fmt.Errorf("%w: invalid credential_ref", ErrInvalidMetadata)
		}
	}
	if m.RuntimeVersion != "" && !isBoundedPrintable(m.RuntimeVersion, 128) {
		return fmt.Errorf("%w: invalid runtime_version", ErrInvalidMetadata)
	}
	if m.Migration < 0 {
		return fmt.Errorf("%w: invalid migration", ErrInvalidMetadata)
	}
	if m.RepositoryMount != "" {
		if strings.IndexByte(m.RepositoryMount, 0) >= 0 || !filepath.IsAbs(m.RepositoryMount) || filepath.Clean(m.RepositoryMount) != m.RepositoryMount {
			return fmt.Errorf("%w: invalid repository_mount", ErrInvalidMetadata)
		}
	}
	if m.ServerURL != "" {
		parsed, err := url.Parse(m.ServerURL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" {
			return fmt.Errorf("%w: invalid server_url", ErrInvalidMetadata)
		}
	}
	if m.Port != 0 && (m.Port < 1024 || m.Port > 65535) {
		return fmt.Errorf("%w: invalid port", ErrInvalidMetadata)
	}
	return nil
}

// ValidateIdentifier accepts bounded, portable names used in profile
// metadata. Path separators, traversal segments, whitespace, and control
// characters are rejected.
func ValidateIdentifier(value string) error {
	if len(value) == 0 || len(value) > 128 || !identifierPattern.MatchString(value) || value == "." || value == ".." {
		return fmt.Errorf("%w: identifier", ErrInvalidMetadata)
	}
	return nil
}

// ValidateReference accepts slash-separated, bounded reference segments. It
// validates a logical identifier only; it never converts the reference into a
// filesystem path.
func ValidateReference(value string) error {
	if len(value) == 0 || len(value) > 256 || strings.TrimSpace(value) != value {
		return fmt.Errorf("%w: reference", ErrInvalidMetadata)
	}
	parts := strings.Split(value, "/")
	if len(parts) > 8 {
		return fmt.Errorf("%w: reference", ErrInvalidMetadata)
	}
	for _, part := range parts {
		if err := ValidateIdentifier(part); err != nil {
			return fmt.Errorf("%w: reference", ErrInvalidMetadata)
		}
	}
	return nil
}

// Store reads and atomically replaces one non-secret profile document below
// an explicit private root.
type Store struct {
	root string
}

// New validates root and returns a profile store without creating files.
// root is normally the caller-resolved value of FORNIX_HOME.
func New(root string) (*Store, error) {
	clean, err := ValidateRoot(root)
	if err != nil {
		return nil, err
	}
	return &Store{root: clean}, nil
}

// ValidateRoot canonicalizes an explicit absolute profile root. The
// filesystem root and an existing symlink at the final component fail closed.
func ValidateRoot(root string) (string, error) {
	if root == "" || strings.TrimSpace(root) != root || strings.IndexByte(root, 0) >= 0 || !filepath.IsAbs(root) {
		return "", ErrInvalidRoot
	}
	clean := filepath.Clean(root)
	volume := filepath.VolumeName(clean)
	if clean == string(filepath.Separator) || clean == volume+string(filepath.Separator) {
		return "", ErrInvalidRoot
	}
	info, err := os.Lstat(clean)
	if err == nil {
		if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
			return "", ErrUnsafePath
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("validate profile root: %w", err)
	}
	return clean, nil
}

// Root returns the canonical profile root. It contains no credential values.
func (s *Store) Root() string {
	if s == nil {
		return ""
	}
	return s.root
}

// Save validates and atomically replaces profile metadata while holding the
// profile-wide lock. No previous complete document is exposed partially.
func (s *Store) Save(m Metadata) error {
	if s == nil {
		return ErrInvalidRoot
	}
	if err := m.Validate(); err != nil {
		return err
	}
	encoded, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return fmt.Errorf("encode profile metadata: %w", err)
	}
	encoded = append(encoded, '\n')
	if len(encoded) > MaxMetadataBytes {
		return fmt.Errorf("%w: document exceeds size limit", ErrInvalidMetadata)
	}
	lock, err := Acquire(s.root)
	if err != nil {
		return err
	}
	defer lock.Release()
	return writeAtomic(s.root, metadataFilename, encoded)
}

// Load returns the last complete profile document. It refuses symlinks,
// non-regular files, excessive data, insecure permissions, unknown fields,
// trailing JSON, and unsupported schema versions.
func (s *Store) Load() (Metadata, error) {
	if s == nil {
		return Metadata{}, ErrInvalidRoot
	}
	if _, err := ensurePrivateDirectory(s.root); err != nil {
		return Metadata{}, err
	}
	path := filepath.Join(s.root, metadataFilename)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return Metadata{}, ErrNotFound
	}
	if err != nil {
		return Metadata{}, fmt.Errorf("inspect profile metadata: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return Metadata{}, ErrUnsafePath
	}
	if info.Mode().Perm() != FileMode {
		return Metadata{}, ErrInsecurePermissions
	}
	file, err := os.Open(path)
	if err != nil {
		return Metadata{}, fmt.Errorf("open profile metadata: %w", err)
	}
	defer file.Close()
	limited := io.LimitReader(file, MaxMetadataBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return Metadata{}, fmt.Errorf("read profile metadata: %w", err)
	}
	if len(data) > MaxMetadataBytes {
		return Metadata{}, fmt.Errorf("%w: document exceeds size limit", ErrInvalidMetadata)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var m Metadata
	if err := decoder.Decode(&m); err != nil {
		return Metadata{}, fmt.Errorf("%w: decode document", ErrInvalidMetadata)
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return Metadata{}, fmt.Errorf("%w: trailing document", ErrInvalidMetadata)
	}
	if err := m.Validate(); err != nil {
		return Metadata{}, err
	}
	return m, nil
}

func isBoundedPrintable(value string, limit int) bool {
	if len(value) == 0 || len(value) > limit || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if r < 0x20 || r == 0x7f {
			return false
		}
	}
	return true
}
