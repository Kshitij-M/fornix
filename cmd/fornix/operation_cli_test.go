package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadBoundedJSONFileRejectsInvalidAndOversizedInput(t *testing.T) {
	directory := t.TempDir()
	validPath := filepath.Join(directory, "request.json")
	if err := os.WriteFile(validPath, []byte(`{"workspace_id":"demo","input_hash":"abc"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	data, err := readBoundedJSONFile(validPath, 128)
	if err != nil {
		t.Fatalf("read valid JSON: %v", err)
	}
	if !strings.Contains(string(data), `"workspace_id":"demo"`) {
		t.Fatalf("unexpected JSON=%s", data)
	}

	invalidPath := filepath.Join(directory, "invalid.json")
	if err := os.WriteFile(invalidPath, []byte(`{"broken"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readBoundedJSONFile(invalidPath, 128); err == nil {
		t.Fatal("invalid JSON was accepted")
	}

	largePath := filepath.Join(directory, "large.json")
	if err := os.WriteFile(largePath, []byte(`{"value":"`+strings.Repeat("x", 256)+`"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readBoundedJSONFile(largePath, 64); err == nil {
		t.Fatal("oversized JSON was accepted")
	}
}

func TestOperationCLICompletionIncludesGenericSurface(t *testing.T) {
	for _, shell := range []string{"bash", "zsh", "fish"} {
		script, err := completionScript(shell)
		if err != nil {
			t.Fatalf("completion %s: %v", shell, err)
		}
		if !strings.Contains(script, "operation") {
			t.Fatalf("completion %s omitted operation command", shell)
		}
	}
}

func TestOperationCreateUsesRequestIdempotencyByDefault(t *testing.T) {
	key, err := operationCreateIdempotencyKey([]byte(`{"idempotency_key":"request-key"}`), nil, "workspace-a")
	if err != nil || key != "request-key" {
		t.Fatalf("request idempotency key=%q err=%v", key, err)
	}
	key, err = operationCreateIdempotencyKey([]byte(`{"idempotency_key":"request-key"}`), []string{"--idempotency", "explicit-key"}, "workspace-a")
	if err != nil || key != "explicit-key" {
		t.Fatalf("explicit idempotency key=%q err=%v", key, err)
	}
	key, err = operationCreateIdempotencyKey([]byte(`{"request_id":"request-only"}`), nil, "workspace-a")
	if err != nil || !strings.HasPrefix(key, "operation:create:workspace-a:") {
		t.Fatalf("generated idempotency key=%q err=%v", key, err)
	}
}
