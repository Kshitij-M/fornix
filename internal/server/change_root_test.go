package server

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestResolveChangeRootEnforcesCanonicalWorkspaceMount(t *testing.T) {
	parent := t.TempDir()
	mount := filepath.Join(parent, "workspace")
	inside := filepath.Join(mount, "repo")
	outside := filepath.Join(parent, "outside")
	for _, path := range []string{inside, outside} {
		if err := os.MkdirAll(path, 0o700); err != nil {
			t.Fatalf("create test directory: %v", err)
		}
	}

	resolved, err := resolveChangeRoot(mount, inside)
	if err != nil {
		t.Fatalf("resolve in-mount repository: %v", err)
	}
	canonical, err := filepath.EvalSymlinks(inside)
	if err != nil {
		t.Fatalf("canonicalize test repository: %v", err)
	}
	if resolved != canonical {
		t.Fatalf("resolved root = %q, want canonical %q", resolved, canonical)
	}

	escape := filepath.Join(mount, "escape")
	if err := os.Symlink(outside, escape); err != nil {
		t.Skipf("create symlink for containment test: %v", err)
	}
	if _, err := resolveChangeRoot(mount, escape); err == nil || !strings.Contains(err.Error(), "outside workspace mount") {
		t.Fatalf("symlink escape error = %v, want outside-mount rejection", err)
	}
	if _, err := resolveChangeRoot(mount, outside); err == nil || !strings.Contains(err.Error(), "outside workspace mount") {
		t.Fatalf("outside root error = %v, want outside-mount rejection", err)
	}
}

func TestResolveChangeRootRejectsRelativeRequestedRoot(t *testing.T) {
	mount := t.TempDir()
	if _, err := resolveChangeRoot(mount, "repo"); err == nil {
		t.Fatal("expected relative repository root to be rejected")
	}
}
