package profile

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestStoreRoundTripAndPermissions(t *testing.T) {
	root := filepath.Join(t.TempDir(), "fornix-home")
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	metadata := NewMetadata("local")
	metadata.ServerURL = "http://127.0.0.1:8201"
	metadata.Port = 18281
	metadata.WorkspaceID = "workspace-1"
	metadata.ActorID = "actor-1"
	metadata.CredentialRef = "local/api"
	metadata.RuntimeVersion = "v1.2.3"
	metadata.RuntimeProject = "fornix-local"
	metadata.DataVolume = "fornix-data"
	metadata.Migration = 32
	if err := store.Save(metadata); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if loaded != metadata {
		t.Fatalf("loaded metadata = %+v, want %+v", loaded, metadata)
	}
	assertMode(t, root, DirectoryMode)
	assertMode(t, filepath.Join(root, metadataFilename), FileMode)
	assertMode(t, filepath.Join(root, lockFilename), FileMode)
}

func TestConcurrentSavesRemainComplete(t *testing.T) {
	root := filepath.Join(t.TempDir(), "fornix-home")
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	const writers = 32
	start := make(chan struct{})
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			metadata := NewMetadata(fmt.Sprintf("profile-%02d", i))
			metadata.WorkspaceID = fmt.Sprintf("workspace-%02d", i)
			errs <- store.Save(metadata)
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatal(err)
	}
	if err := loaded.Validate(); err != nil {
		t.Fatalf("concurrent save exposed invalid document: %v", err)
	}
}

func TestAcquireContextSerializesAndCancels(t *testing.T) {
	root := filepath.Join(t.TempDir(), "fornix-home")
	first, err := Acquire(root)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, err = AcquireContext(ctx, root)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("AcquireContext error = %v, want deadline", err)
	}
	if time.Since(started) < 60*time.Millisecond {
		t.Fatal("contending lock returned before context deadline")
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	second, err := Acquire(root)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}
	if err := second.Release(); err != nil {
		t.Fatalf("idempotent release: %v", err)
	}
}

func TestValidationIsStableAndPathSafe(t *testing.T) {
	tests := []struct {
		name string
		root string
	}{
		{name: "empty", root: ""},
		{name: "relative", root: "relative/home"},
		{name: "traversal relative", root: "../home"},
		{name: "filesystem root", root: string(filepath.Separator)},
		{name: "whitespace", root: " " + filepath.Join(t.TempDir(), "home")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ValidateRoot(test.root); !errors.Is(err, ErrInvalidRoot) {
				t.Fatalf("ValidateRoot(%q) = %v", test.root, err)
			}
		})
	}
	for _, value := range []string{"../escape", "a/../../b", "a\\b", "a b", ".", "..", ""} {
		if err := ValidateReference(value); !errors.Is(err, ErrInvalidMetadata) {
			t.Fatalf("ValidateReference(%q) = %v", value, err)
		}
	}
	if err := ValidateReference("provider/openai"); err != nil {
		t.Fatal(err)
	}
}

func TestRejectsSymlinkBoundariesAndInsecureMetadata(t *testing.T) {
	base := t.TempDir()
	realRoot := filepath.Join(base, "real")
	if err := os.Mkdir(realRoot, DirectoryMode); err != nil {
		t.Fatal(err)
	}
	symlinkRoot := filepath.Join(base, "linked")
	if err := os.Symlink(realRoot, symlinkRoot); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	if _, err := New(symlinkRoot); !errors.Is(err, ErrUnsafePath) {
		t.Fatalf("New(symlink root) = %v", err)
	}

	store, err := New(realRoot)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(realRoot, metadataFilename), []byte(`{"schema_version":1,"name":"local"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Load(); !errors.Is(err, ErrInsecurePermissions) {
		t.Fatalf("Load insecure metadata = %v", err)
	}
}

func TestLoadRejectsUnknownAndTrailingJSON(t *testing.T) {
	for name, document := range map[string]string{
		"unknown":  `{"schema_version":1,"name":"local","secret":"must-not-load"}`,
		"trailing": `{"schema_version":1,"name":"local"} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			root := filepath.Join(t.TempDir(), "fornix-home")
			if err := os.Mkdir(root, DirectoryMode); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(root, metadataFilename), []byte(document), FileMode); err != nil {
				t.Fatal(err)
			}
			store, err := New(root)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.Load(); !errors.Is(err, ErrInvalidMetadata) {
				t.Fatalf("Load() = %v", err)
			}
		})
	}
}

func assertMode(t *testing.T, path string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != want {
		t.Fatalf("%s mode = %o, want %o", path, got, want)
	}
}
