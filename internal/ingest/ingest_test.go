package ingest

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestDiscoverIsDeterministicAndManifestIncludesMetadata(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, ".git"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "z.go"), []byte("package z\nfunc Z() {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "a.py"), []byte("class A:\n    pass\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, ".git", "ignored"), []byte("ignored"), 0o644); err != nil {
		t.Fatal(err)
	}
	source := contracts.RepositorySource{Repository: "repo", SourceRoot: root, MountRoot: root, ExtractSymbols: true}
	first, err := Discover(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Discover(context.Background(), source)
	if err != nil {
		t.Fatal(err)
	}
	if first.ManifestHash != second.ManifestHash {
		t.Fatalf("manifest changed: %s != %s", first.ManifestHash, second.ManifestHash)
	}
	if len(first.Files) != 2 || first.Files[0].File.Path != "a.py" || first.Files[1].File.Path != "z.go" {
		t.Fatalf("unexpected files: %#v", first.Files)
	}
	if first.Files[0].File.Mode != 0o600 {
		t.Fatalf("mode missing from manifest: %#o", first.Files[0].File.Mode)
	}
}

func TestDiscoverRejectsSymlinkAndEscapingRoot(t *testing.T) {
	parent := t.TempDir()
	root := filepath.Join(parent, "repo")
	outside := filepath.Join(parent, "outside")
	if err := os.Mkdir(root, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(outside, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(root, "secret.txt")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	_, err := Discover(context.Background(), contracts.RepositorySource{Repository: "repo", SourceRoot: root, MountRoot: parent})
	if err == nil {
		t.Fatal("expected symlink rejection")
	}
	_, err = Discover(context.Background(), contracts.RepositorySource{Repository: "repo", SourceRoot: filepath.Join(parent, ".."), MountRoot: root})
	if err == nil {
		t.Fatal("expected mount escape rejection")
	}
}

func TestChunksAndSymbolsHaveStableOrdering(t *testing.T) {
	data := []byte("package demo\n\nfunc Alpha() {}\nfunc Beta() {}\n")
	first, err := Chunk(data, 24, 4)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Chunk(data, 24, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != len(second) {
		t.Fatal("chunk count changed")
	}
	for i := range first {
		if first[i].Text != second[i].Text || first[i].RuneStart != second[i].RuneStart {
			t.Fatalf("chunk %d changed", i)
		}
	}
	symbols := Symbols("demo.go", data)
	if len(symbols) != 2 || symbols[0].SymbolName != "Alpha" || symbols[1].SymbolName != "Beta" {
		t.Fatalf("unexpected symbols: %#v", symbols)
	}
}

func TestReadAndVerifyDetectsSourceMutation(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "README.md")
	data := []byte("stable")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	discovered, err := Discover(context.Background(), contracts.RepositorySource{Repository: "repo", SourceRoot: root, MountRoot: root})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("changed"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadAndVerify(root, discovered.Files[0].File); err == nil {
		t.Fatal("expected mutation rejection")
	}
}
