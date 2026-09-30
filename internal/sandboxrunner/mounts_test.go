package sandboxrunner

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func mountRef(workspaceID, suffix string) contracts.WorkspaceMountRef {
	return contracts.WorkspaceMountRef{ID: "wsmount_" + strings.Repeat(suffix, 32), WorkspaceID: workspaceID}
}

func canonicalTempDir(t *testing.T) string {
	t.Helper()
	path, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func TestMountCatalogResolvesOnlyWorkspaceBoundRelativeWorkdirs(t *testing.T) {
	root := canonicalTempDir(t)
	if err := os.Mkdir(filepath.Join(root, "src"), 0o700); err != nil {
		t.Fatal(err)
	}
	catalog, err := NewMountCatalog([]MountRegistration{{Reference: mountRef("workspace-a", "a"), HostPath: root}})
	if err != nil {
		t.Fatal(err)
	}

	reference := mountRef("workspace-a", "a")
	mount, err := catalog.Resolve(reference)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mount.Close() })
	if got := mount.Reference(); got != reference {
		t.Fatalf("reference = %#v, want %#v", got, reference)
	}
	if !mount.ReadOnly() {
		t.Fatal("resolved workspace mounts must be read-only")
	}
	if got, err := mount.ContainerWorkdir("src"); err != nil || got != "/workspace/src" {
		t.Fatalf("ContainerWorkdir(src) = %q, %v", got, err)
	}
	if got, err := mount.ContainerWorkdir(""); err != nil || got != "/workspace" {
		t.Fatalf("ContainerWorkdir(empty) = %q, %v", got, err)
	}
	wantRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	if got, err := mount.HostPath(); err != nil || got != wantRoot {
		t.Fatalf("HostPath() = %q, %v; want canonical root %q", got, err, wantRoot)
	}

	if _, err := catalog.Resolve(mountRef("workspace-b", "a")); !errors.Is(err, ErrMountUnavailable) {
		t.Fatalf("cross-workspace reference error = %v, want unavailable", err)
	}
}

func TestZeroValueMountCatalogCanRegisterSafely(t *testing.T) {
	root := canonicalTempDir(t)
	var catalog MountCatalog
	reference := mountRef("workspace-zero", "0")
	if err := catalog.Register(reference, root); err != nil {
		t.Fatalf("register in zero-value catalog: %v", err)
	}
	resolved, err := catalog.Resolve(reference)
	if err != nil {
		t.Fatalf("resolve from zero-value catalog: %v", err)
	}
	if err := resolved.Close(); err != nil {
		t.Fatalf("close zero-value catalog resolution: %v", err)
	}
}

func TestMountCatalogRejectsTraversalAndSymlinkEscape(t *testing.T) {
	root := canonicalTempDir(t)
	outside := canonicalTempDir(t)
	if err := os.Symlink(outside, filepath.Join(root, "escape")); err != nil {
		t.Fatal(err)
	}
	catalog, err := NewMountCatalog([]MountRegistration{{Reference: mountRef("workspace-a", "b"), HostPath: root}})
	if err != nil {
		t.Fatal(err)
	}
	mount, err := catalog.Resolve(mountRef("workspace-a", "b"))
	if err != nil {
		t.Fatal(err)
	}
	defer mount.Close()

	for _, relative := range []string{"../outside", "escape", "/etc", `src\\..\\outside`, "./src", "src/../outside"} {
		if got, err := mount.ContainerWorkdir(relative); err == nil {
			t.Errorf("ContainerWorkdir(%q) = %q, want rejection", relative, got)
		}
	}
}

func TestMountCatalogRejectsInvalidRootsAndReferences(t *testing.T) {
	root := canonicalTempDir(t)
	cases := []struct {
		name string
		ref  contracts.WorkspaceMountRef
		path string
	}{
		{name: "missing root", ref: mountRef("workspace-a", "c"), path: filepath.Join(root, "missing")},
		{name: "file root", ref: mountRef("workspace-a", "d"), path: filepath.Join(root, "file")},
		{name: "relative root", ref: mountRef("workspace-a", "e"), path: "relative"},
		{name: "invalid reference", ref: contracts.WorkspaceMountRef{ID: "not-a-reference", WorkspaceID: "workspace-a"}, path: root},
		{name: "missing workspace", ref: contracts.WorkspaceMountRef{ID: "wsmount_" + strings.Repeat("f", 32)}, path: root},
	}
	if err := os.WriteFile(filepath.Join(root, "file"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			catalog := &MountCatalog{byID: make(map[string]mountRecord)}
			if err := catalog.Register(test.ref, test.path); !errors.Is(err, ErrInvalidMount) {
				t.Fatalf("Register() error = %v, want invalid mount", err)
			}
		})
	}
}

func TestMountCatalogRejectsSymlinkedRoot(t *testing.T) {
	parent := canonicalTempDir(t)
	actual := filepath.Join(parent, "actual")
	alias := filepath.Join(parent, "alias")
	if err := os.Mkdir(actual, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(actual, alias); err != nil {
		t.Fatal(err)
	}
	catalog := &MountCatalog{byID: make(map[string]mountRecord)}
	if err := catalog.Register(mountRef("workspace-a", "f"), alias); !errors.Is(err, ErrInvalidMount) {
		t.Fatalf("symlinked root registration error = %v, want invalid mount", err)
	}
}

func TestMountCatalogRejectsCrossWorkspaceOverlapAndReferenceRebinding(t *testing.T) {
	root := canonicalTempDir(t)
	child := filepath.Join(root, "child")
	if err := os.Mkdir(child, 0o700); err != nil {
		t.Fatal(err)
	}
	catalog, err := NewMountCatalog([]MountRegistration{{Reference: mountRef("workspace-a", "1"), HostPath: root}})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Register(mountRef("workspace-b", "2"), child); !errors.Is(err, ErrMountConflict) {
		t.Fatalf("nested cross-workspace registration error = %v, want conflict", err)
	}
	if err := catalog.Register(mountRef("workspace-a", "1"), child); !errors.Is(err, ErrMountConflict) {
		t.Fatalf("reference rebind error = %v, want conflict", err)
	}
	if err := catalog.Register(mountRef("workspace-a", "1"), root); err != nil {
		t.Fatalf("identical registration should be idempotent: %v", err)
	}
}

func TestMountCatalogRejectsCaseVariantAliasOnMacOS(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("case-variant path identity is specific to macOS filesystem qualification")
	}
	root := canonicalTempDir(t)
	alias := strings.ToUpper(root)
	rootInfo, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	aliasInfo, err := os.Stat(alias)
	if err != nil || !os.SameFile(rootInfo, aliasInfo) {
		t.Skip("the active macOS filesystem is case-sensitive")
	}
	catalog, err := NewMountCatalog([]MountRegistration{{Reference: mountRef("workspace-a", "7"), HostPath: root}})
	if err != nil {
		t.Fatal(err)
	}
	if err := catalog.Register(mountRef("workspace-b", "8"), alias); !errors.Is(err, ErrMountConflict) {
		t.Fatalf("case-variant alias registration error = %v, want cross-workspace conflict", err)
	}
}

func TestMountOverlapUsesDirectoryIdentityAsWellAsPath(t *testing.T) {
	root := canonicalTempDir(t)
	info, err := os.Stat(root)
	if err != nil {
		t.Fatal(err)
	}
	left := mountRecord{reference: mountRef("workspace-a", "9"), hostPath: root, rootInfo: info}
	right := mountRecord{
		reference: mountRef("workspace-b", "a"),
		hostPath:  filepath.Join(filepath.Dir(root), "alias-"+filepath.Base(root)),
		rootInfo:  info,
	}
	if !mountsOverlap(left, right) {
		t.Fatal("different paths with the same directory identity were not treated as overlapping")
	}
}

func TestResolvedMountDetectsReplacementAtRegisteredPath(t *testing.T) {
	parent := canonicalTempDir(t)
	root := filepath.Join(parent, "workspace")
	if err := os.Mkdir(root, 0o700); err != nil {
		t.Fatal(err)
	}
	outside := canonicalTempDir(t)
	catalog, err := NewMountCatalog([]MountRegistration{{Reference: mountRef("workspace-a", "3"), HostPath: root}})
	if err != nil {
		t.Fatal(err)
	}
	mount, err := catalog.Resolve(mountRef("workspace-a", "3"))
	if err != nil {
		t.Fatal(err)
	}
	defer mount.Close()
	if err := os.Rename(root, root+".old"); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, root); err != nil {
		t.Fatal(err)
	}
	if _, err := mount.HostPath(); !errors.Is(err, ErrMountUnavailable) {
		t.Fatalf("HostPath after replacement error = %v, want unavailable", err)
	}
}

func TestMountCatalogConcurrentResolveAndRegister(t *testing.T) {
	root := canonicalTempDir(t)
	catalog := &MountCatalog{byID: make(map[string]mountRecord)}
	first := mountRef("workspace-a", "4")
	if err := catalog.Register(first, root); err != nil {
		t.Fatal(err)
	}

	var wait sync.WaitGroup
	errorsFound := make(chan error, 8*50+50)
	for i := 0; i < 8; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for j := 0; j < 50; j++ {
				mount, err := catalog.Resolve(first)
				if err != nil {
					errorsFound <- fmt.Errorf("resolve: %w", err)
					continue
				}
				if err := mount.Close(); err != nil {
					errorsFound <- fmt.Errorf("close: %w", err)
				}
			}
		}()
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		for i := 0; i < 50; i++ {
			if err := catalog.Register(mountRef("workspace-a", "5"), root); err != nil {
				errorsFound <- fmt.Errorf("register: %w", err)
			}
		}
	}()
	wait.Wait()
	close(errorsFound)
	for err := range errorsFound {
		t.Error(err)
	}
}

func TestResolvedMountUseAndCloseAreRaceSafe(t *testing.T) {
	root := canonicalTempDir(t)
	if err := os.Mkdir(filepath.Join(root, "src"), 0o700); err != nil {
		t.Fatal(err)
	}
	catalog, err := NewMountCatalog([]MountRegistration{{Reference: mountRef("workspace-a", "6"), HostPath: root}})
	if err != nil {
		t.Fatal(err)
	}
	mount, err := catalog.Resolve(mountRef("workspace-a", "6"))
	if err != nil {
		t.Fatal(err)
	}

	var wait sync.WaitGroup
	for i := 0; i < 8; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for j := 0; j < 50; j++ {
				_, _ = mount.HostPath()
				_, _ = mount.ContainerWorkdir("src")
			}
		}()
	}
	wait.Add(1)
	go func() {
		defer wait.Done()
		_ = mount.Close()
	}()
	wait.Wait()
	if _, err := mount.HostPath(); !errors.Is(err, ErrMountUnavailable) {
		t.Fatalf("closed mount HostPath error = %v, want unavailable", err)
	}
}
