package runtime

import (
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
)

const (
	// ManifestVersion identifies the embedded local-runtime contract. Changing
	// this value requires an explicit compatibility and migration decision.
	ManifestVersion = "v1"

	manifestFilename = "compose." + ManifestVersion + ".yaml"
)

var (
	// ErrInvalidProfile reports a profile name, project name, or filesystem path
	// that cannot safely identify a managed Fornix runtime.
	ErrInvalidProfile = errors.New("invalid runtime profile")

	profileNamePattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,47}$`)
)

// Profile is the immutable identity and filesystem boundary for one local
// runtime. ProjectName and ManifestPath are derived from Name and RootDir so
// callers cannot redirect Compose to an unrelated project or file.
type Profile struct {
	name        string
	projectName string
	rootDir     string
	manifest    string
}

// NewProfile validates name and rootDir and returns a deterministic profile.
// rootDir must be an absolute, already-clean path below a filesystem root.
func NewProfile(rootDir, name string) (Profile, error) {
	if !profileNamePattern.MatchString(name) {
		return Profile{}, fmt.Errorf("%w: name must match %s", ErrInvalidProfile, profileNamePattern.String())
	}
	if rootDir == "" || strings.IndexByte(rootDir, 0) >= 0 || !filepath.IsAbs(rootDir) {
		return Profile{}, fmt.Errorf("%w: root directory must be an absolute path", ErrInvalidProfile)
	}
	cleanRoot := filepath.Clean(rootDir)
	if cleanRoot != rootDir {
		return Profile{}, fmt.Errorf("%w: root directory must be normalized", ErrInvalidProfile)
	}
	volume := filepath.VolumeName(cleanRoot)
	if cleanRoot == string(filepath.Separator) || (volume != "" && cleanRoot == volume+string(filepath.Separator)) {
		return Profile{}, fmt.Errorf("%w: filesystem root is not an allowed profile directory", ErrInvalidProfile)
	}

	projectName := "fornix-" + name
	runtimeDir := filepath.Join(cleanRoot, "runtime")
	manifest := filepath.Join(runtimeDir, manifestFilename)
	if !pathWithin(cleanRoot, manifest) {
		return Profile{}, fmt.Errorf("%w: manifest path escapes profile root", ErrInvalidProfile)
	}

	return Profile{
		name:        name,
		projectName: projectName,
		rootDir:     cleanRoot,
		manifest:    manifest,
	}, nil
}

// Name returns the stable local profile name.
func (p Profile) Name() string { return p.name }

// ProjectName returns the deterministic Compose project name.
func (p Profile) ProjectName() string { return p.projectName }

// RootDir returns the private root directory owned by this profile.
func (p Profile) RootDir() string { return p.rootDir }

// RuntimeDir returns the directory containing generated runtime assets.
func (p Profile) RuntimeDir() string { return filepath.Dir(p.manifest) }

// ManifestPath returns the deterministic path of the rendered Compose file.
func (p Profile) ManifestPath() string { return p.manifest }

// Validate rechecks all derived profile invariants. Managers call Validate at
// every external boundary so a zero value or manually assembled value fails
// closed.
func (p Profile) Validate() error {
	expected, err := NewProfile(p.rootDir, p.name)
	if err != nil {
		return err
	}
	if p != expected {
		return fmt.Errorf("%w: derived project or manifest path does not match", ErrInvalidProfile)
	}
	return nil
}

func pathWithin(root, candidate string) bool {
	rel, err := filepath.Rel(root, candidate)
	if err != nil {
		return false
	}
	return rel != ".." && rel != "." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)) && !filepath.IsAbs(rel)
}
