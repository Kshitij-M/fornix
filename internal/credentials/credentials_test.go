package credentials

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/omaveda/fornix/internal/profile"
)

func TestStoreWriteReadDeleteAndPermissions(t *testing.T) {
	root := filepath.Join(t.TempDir(), "fornix-home")
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	ref := mustRef(t, "provider/openai")
	written := mustSecret(t, "test-secret-value")
	defer written.Clear()
	if err := store.Write(ref, written); err != nil {
		t.Fatal(err)
	}
	read, err := store.Read(ref)
	if err != nil {
		t.Fatal(err)
	}
	defer read.Clear()
	if !read.Equal(written) {
		t.Fatal("read credential differs from written credential")
	}
	directory := filepath.Join(root, credentialDir)
	assertMode(t, directory, profile.DirectoryMode)
	path := filepath.Join(directory, filename(ref))
	assertMode(t, path, profile.FileMode)
	if filepath.Base(path) == ref.String() || strings.Contains(path, ref.String()) {
		t.Fatal("credential reference was used as a storage path")
	}
	if err := store.Delete(ref); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Read(ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Read after Delete = %v", err)
	}
	if err := store.Delete(ref); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Delete = %v", err)
	}
}

func TestSecretNeverFormatsOrMarshalsItsValue(t *testing.T) {
	const token = "should-never-appear"
	secret := mustSecret(t, token)
	defer secret.Clear()
	for _, rendered := range []string{
		fmt.Sprint(secret),
		fmt.Sprintf("%v", secret),
		fmt.Sprintf("%+v", secret),
		fmt.Sprintf("%#v", secret),
		fmt.Sprintf("%s", secret),
		fmt.Sprintf("%q", secret),
		fmt.Sprintf("%x", secret),
	} {
		if strings.Contains(rendered, token) || rendered != Redacted {
			t.Fatalf("unsafe secret formatting: %q", rendered)
		}
	}
	if _, err := json.Marshal(secret); !errors.Is(err, ErrSecretSerialization) {
		t.Fatalf("MarshalJSON error = %v", err)
	}
	if _, err := secret.MarshalText(); !errors.Is(err, ErrSecretSerialization) {
		t.Fatalf("MarshalText error = %v", err)
	}
	copy := secret.Bytes()
	if string(copy) != token {
		t.Fatal("Bytes did not return the secret")
	}
	copy[0] = 'X'
	if string(secret.Bytes()) != token {
		t.Fatal("Bytes exposed mutable internal storage")
	}
}

func TestReferenceValidationRejectsPathInputs(t *testing.T) {
	for _, value := range []string{"", "../openai", "provider/../../escape", "/absolute", "provider\\openai", "provider//openai", "provider/open ai"} {
		if _, err := ParseRef(value); !errors.Is(err, ErrInvalidReference) {
			t.Fatalf("ParseRef(%q) = %v", value, err)
		}
	}
	ref := mustRef(t, "local/api")
	encoded, err := ref.MarshalText()
	if err != nil || string(encoded) != "local/api" {
		t.Fatalf("MarshalText = %q, %v", encoded, err)
	}
}

func TestConcurrentCredentialRotationIsAtomic(t *testing.T) {
	root := filepath.Join(t.TempDir(), "fornix-home")
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	ref := mustRef(t, "local/api")
	const writers = 32
	start := make(chan struct{})
	errs := make(chan error, writers)
	wants := make(map[string]struct{}, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		value := fmt.Sprintf("credential-%02d-with-complete-content", i)
		wants[value] = struct{}{}
		wg.Add(1)
		go func(value string) {
			defer wg.Done()
			<-start
			secret := mustSecret(t, value)
			defer secret.Clear()
			errs <- store.Write(ref, secret)
		}(value)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	got, err := store.Read(ref)
	if err != nil {
		t.Fatal(err)
	}
	defer got.Clear()
	if _, ok := wants[string(got.Bytes())]; !ok {
		t.Fatal("concurrent write exposed a partial or unknown credential")
	}
	entries, err := os.ReadDir(filepath.Join(root, credentialDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filename(ref) {
		t.Fatalf("credential directory contains unexpected files: %+v", entries)
	}
}

func TestCredentialDirectorySymlinkFailsClosed(t *testing.T) {
	root := filepath.Join(t.TempDir(), "fornix-home")
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.MkdirAll(root, profile.DirectoryMode); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(outside, profile.DirectoryMode); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(root, credentialDir)); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	store, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	secret := mustSecret(t, "does-not-escape")
	defer secret.Clear()
	if err := store.Write(mustRef(t, "local/api"), secret); !errors.Is(err, profile.ErrUnsafePath) {
		t.Fatalf("Write through symlink = %v", err)
	}
	entries, err := os.ReadDir(outside)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatal("credential write escaped the profile root")
	}
}

func TestRedactionHelpersAreStable(t *testing.T) {
	secret := mustSecret(t, "opaque-token")
	defer secret.Clear()
	input := "Authorization: Bearer abc.def and opaque-token"
	got := RedactText(input, secret)
	if got != "Authorization: Bearer [REDACTED] and [REDACTED]" {
		t.Fatalf("RedactText = %q", got)
	}
	if RedactValue("") != "" || RedactValue("present") != Redacted {
		t.Fatal("RedactValue contract changed")
	}
	original := map[string]string{"endpoint": "localhost", "api_key": "value", "providerSecret": "value-2"}
	redacted := RedactMap(original)
	if redacted["endpoint"] != "localhost" || redacted["api_key"] != Redacted || redacted["providerSecret"] != Redacted {
		t.Fatalf("RedactMap = %#v", redacted)
	}
	if original["api_key"] != "value" {
		t.Fatal("RedactMap mutated input")
	}
}

func mustRef(t *testing.T, value string) Ref {
	t.Helper()
	ref, err := ParseRef(value)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

func mustSecret(t *testing.T, value string) Secret {
	t.Helper()
	secret, err := NewSecret([]byte(value))
	if err != nil {
		t.Fatal(err)
	}
	return secret
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
