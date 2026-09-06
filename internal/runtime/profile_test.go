package runtime

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestNewProfileDerivesStablePaths(t *testing.T) {
	root := filepath.Join(t.TempDir(), "profiles", "local")
	profile, err := NewProfile(root, "local")
	if err != nil {
		t.Fatalf("NewProfile: %v", err)
	}
	if profile.Name() != "local" || profile.ProjectName() != "fornix-local" {
		t.Fatalf("unexpected identity: name=%q project=%q", profile.Name(), profile.ProjectName())
	}
	wantManifest := filepath.Join(root, "runtime", "compose.v1.yaml")
	if profile.ManifestPath() != wantManifest {
		t.Fatalf("manifest path = %q, want %q", profile.ManifestPath(), wantManifest)
	}
	if err := profile.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestNewProfileRejectsInvalidInputs(t *testing.T) {
	uncleanRoot := t.TempDir() + string(filepath.Separator) + "a" + string(filepath.Separator) + ".." + string(filepath.Separator) + "b"
	tests := []struct {
		name    string
		root    string
		profile string
	}{
		{name: "relative root", root: "relative", profile: "local"},
		{name: "unclean root", root: uncleanRoot, profile: "local"},
		{name: "filesystem root", root: string(filepath.Separator), profile: "local"},
		{name: "uppercase", root: t.TempDir(), profile: "Local"},
		{name: "traversal", root: t.TempDir(), profile: "../local"},
		{name: "separator", root: t.TempDir(), profile: "team/local"},
		{name: "too long", root: t.TempDir(), profile: "a123456789012345678901234567890123456789012345678"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewProfile(test.root, test.profile)
			if !errors.Is(err, ErrInvalidProfile) {
				t.Fatalf("error = %v, want ErrInvalidProfile", err)
			}
		})
	}
}

func TestZeroProfileFailsClosed(t *testing.T) {
	if err := (Profile{}).Validate(); !errors.Is(err, ErrInvalidProfile) {
		t.Fatalf("error = %v, want ErrInvalidProfile", err)
	}
}
