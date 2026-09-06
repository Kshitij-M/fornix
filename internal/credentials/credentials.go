package credentials

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/omaveda/fornix/internal/profile"
)

const (
	// MaxSecretBytes bounds one local credential and prevents accidental use of
	// this narrow token store for arbitrary artifacts.
	MaxSecretBytes = 64 << 10
	credentialDir  = "credentials"
)

var (
	// ErrInvalidReference identifies an invalid logical credential reference.
	ErrInvalidReference = errors.New("invalid credential reference")
	// ErrInvalidSecret identifies an empty or oversized secret.
	ErrInvalidSecret = errors.New("invalid credential secret")
	// ErrNotFound identifies a credential reference with no stored token.
	ErrNotFound = errors.New("credential not found")
	// ErrSecretSerialization is returned whenever code attempts to marshal a
	// Secret. Secret material must cross only explicit read/write APIs.
	ErrSecretSerialization = errors.New("credential secrets cannot be serialized")
)

// Ref is a validated logical credential reference such as "local/api" or
// "provider/openai". Its string value is safe to store in profile metadata;
// it never contains secret material.
type Ref struct {
	value string
}

// ParseRef validates a logical reference without interpreting it as a path.
func ParseRef(value string) (Ref, error) {
	if err := profile.ValidateReference(value); err != nil {
		return Ref{}, ErrInvalidReference
	}
	return Ref{value: value}, nil
}

// String returns the non-secret logical reference.
func (r Ref) String() string { return r.value }

// MarshalText serializes only the non-secret reference.
func (r Ref) MarshalText() ([]byte, error) {
	if _, err := ParseRef(r.value); err != nil {
		return nil, err
	}
	return []byte(r.value), nil
}

// UnmarshalText validates and replaces a logical reference.
func (r *Ref) UnmarshalText(value []byte) error {
	if r == nil {
		return ErrInvalidReference
	}
	parsed, err := ParseRef(string(value))
	if err != nil {
		return err
	}
	*r = parsed
	return nil
}

// Secret owns a private copy of opaque credential bytes. Its formatting and
// serialization methods never disclose the value. Bytes returns a copy only
// for the immediate operation boundary that needs the credential.
type Secret struct {
	value []byte
}

// NewSecret validates and copies secret bytes.
func NewSecret(value []byte) (Secret, error) {
	if len(value) == 0 || len(value) > MaxSecretBytes {
		return Secret{}, ErrInvalidSecret
	}
	return Secret{value: append([]byte(nil), value...)}, nil
}

// Bytes returns a copy of the secret. Callers should keep the copy scoped to
// the provider or authentication operation and clear it when practical.
func (s Secret) Bytes() []byte { return append([]byte(nil), s.value...) }

// Len reports the secret length without disclosing its contents.
func (s Secret) Len() int { return len(s.value) }

// Equal compares two secrets in constant time when their lengths match.
func (s Secret) Equal(other Secret) bool {
	if len(s.value) != len(other.value) {
		return false
	}
	return subtle.ConstantTimeCompare(s.value, other.value) == 1
}

// Clear overwrites this Secret's owned bytes. Copies previously returned by
// Bytes or made by value assignment remain the caller's responsibility.
func (s *Secret) Clear() {
	if s == nil {
		return
	}
	for i := range s.value {
		s.value[i] = 0
	}
	s.value = nil
}

// String implements fmt.Stringer without exposing secret material.
func (s Secret) String() string { return Redacted }

// GoString implements fmt.GoStringer without exposing secret material.
func (s Secret) GoString() string { return Redacted }

// Format prevents all fmt verbs, including hexadecimal and quoted forms,
// from reflecting the secret's internal byte slice.
func (s Secret) Format(state fmt.State, _ rune) {
	_, _ = io.WriteString(state, Redacted)
}

// MarshalJSON rejects attempts to place secret values in JSON documents.
func (s Secret) MarshalJSON() ([]byte, error) { return nil, ErrSecretSerialization }

// MarshalText rejects attempts to place secret values in text documents.
func (s Secret) MarshalText() ([]byte, error) { return nil, ErrSecretSerialization }

// Store is the owner-only local-file credential fallback below an explicit
// profile root. File names are hashes of validated references, never
// caller-controlled path components.
type Store struct {
	root string
}

// New validates a caller-supplied profile root without reading FORNIX_HOME or
// any other ambient configuration.
func New(root string) (*Store, error) {
	profiles, err := profile.New(root)
	if err != nil {
		return nil, err
	}
	return &Store{root: profiles.Root()}, nil
}

// Write atomically creates or rotates the token for ref while holding the
// profile-wide process lock. Errors contain no token bytes.
func (s *Store) Write(ref Ref, secret Secret) error {
	if s == nil {
		return profile.ErrInvalidRoot
	}
	if _, err := ParseRef(ref.value); err != nil {
		return err
	}
	if len(secret.value) == 0 || len(secret.value) > MaxSecretBytes {
		return ErrInvalidSecret
	}
	lock, err := profile.Acquire(s.root)
	if err != nil {
		return err
	}
	defer lock.Release()
	directory, err := s.ensureDirectory()
	if err != nil {
		return err
	}
	return writeAtomic(directory, filename(ref), secret.value)
}

// Read resolves ref to a Secret while holding the profile-wide lock. It fails
// closed on symlinks, non-regular files, insecure permissions, and oversized
// values.
func (s *Store) Read(ref Ref) (Secret, error) {
	if s == nil {
		return Secret{}, profile.ErrInvalidRoot
	}
	if _, err := ParseRef(ref.value); err != nil {
		return Secret{}, err
	}
	lock, err := profile.Acquire(s.root)
	if err != nil {
		return Secret{}, err
	}
	defer lock.Release()
	directory, err := s.ensureDirectory()
	if err != nil {
		return Secret{}, err
	}
	path := filepath.Join(directory, filename(ref))
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return Secret{}, ErrNotFound
	}
	if err != nil {
		return Secret{}, fmt.Errorf("inspect credential: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return Secret{}, profile.ErrUnsafePath
	}
	if info.Mode().Perm() != profile.FileMode {
		return Secret{}, profile.ErrInsecurePermissions
	}
	file, err := os.Open(path)
	if err != nil {
		return Secret{}, fmt.Errorf("open credential: %w", err)
	}
	defer file.Close()
	value, err := io.ReadAll(io.LimitReader(file, MaxSecretBytes+1))
	if err != nil {
		return Secret{}, fmt.Errorf("read credential: %w", err)
	}
	if len(value) == 0 || len(value) > MaxSecretBytes {
		clearBytes(value)
		return Secret{}, ErrInvalidSecret
	}
	secret, err := NewSecret(value)
	clearBytes(value)
	return secret, err
}

// Delete removes the token for ref and durably syncs its directory. Missing
// references return ErrNotFound; deletion never changes profile metadata.
func (s *Store) Delete(ref Ref) error {
	if s == nil {
		return profile.ErrInvalidRoot
	}
	if _, err := ParseRef(ref.value); err != nil {
		return err
	}
	lock, err := profile.Acquire(s.root)
	if err != nil {
		return err
	}
	defer lock.Release()
	directory, err := s.ensureDirectory()
	if err != nil {
		return err
	}
	path := filepath.Join(directory, filename(ref))
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("inspect credential for deletion: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return profile.ErrUnsafePath
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("delete credential: %w", err)
	}
	return syncDirectory(directory)
}

func (s *Store) ensureDirectory() (string, error) {
	directory := filepath.Join(s.root, credentialDir)
	info, err := os.Lstat(directory)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(directory, profile.DirectoryMode); err != nil && !errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("create credential directory: %w", err)
		}
		info, err = os.Lstat(directory)
	}
	if err != nil {
		return "", fmt.Errorf("inspect credential directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", profile.ErrUnsafePath
	}
	if err := os.Chmod(directory, profile.DirectoryMode); err != nil {
		return "", fmt.Errorf("restrict credential directory: %w", err)
	}
	return directory, nil
}

func filename(ref Ref) string {
	hash := sha256.Sum256([]byte(ref.value))
	return hex.EncodeToString(hash[:]) + ".token"
}

func writeAtomic(directory, name string, value []byte) (resultErr error) {
	temporary, err := os.CreateTemp(directory, ".credential-write-*")
	if err != nil {
		return fmt.Errorf("create credential temporary file: %w", err)
	}
	temporaryName := temporary.Name()
	defer func() {
		_ = temporary.Close()
		if resultErr != nil {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(profile.FileMode); err != nil {
		return fmt.Errorf("restrict credential temporary file: %w", err)
	}
	if _, err := temporary.Write(value); err != nil {
		return fmt.Errorf("write credential temporary file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		return fmt.Errorf("sync credential temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close credential temporary file: %w", err)
	}
	if err := os.Rename(temporaryName, filepath.Join(directory, name)); err != nil {
		return fmt.Errorf("publish credential: %w", err)
	}
	return syncDirectory(directory)
}

func syncDirectory(directory string) error {
	dir, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open credential directory for sync: %w", err)
	}
	defer dir.Close()
	if err := dir.Sync(); err != nil {
		return fmt.Errorf("sync credential directory: %w", err)
	}
	return nil
}

func clearBytes(value []byte) {
	for i := range value {
		value[i] = 0
	}
}
