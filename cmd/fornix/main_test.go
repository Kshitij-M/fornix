package main

import (
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/config"
)

func TestBuildServerDependenciesCreatesModelOnlyCredentialManager(t *testing.T) {
	dependencies, err := buildServerDependencies(config.Config{
		OpenAIEnabled:            true,
		OpenAICredentialRef:      "openai/production-key",
		CredentialManagerURL:     "https://credentials.example.test/v1/resolve",
		CredentialManagerTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if dependencies.OpenAISecretManager == nil {
		t.Fatal("model-only runtime did not receive the configured credential manager")
	}
	if dependencies.FederationSecretManager != nil {
		t.Fatal("model-only runtime unexpectedly enabled federation credentials")
	}
}

func TestBuildServerDependenciesKeepsEnvironmentOpenAIOutOfManagedPath(t *testing.T) {
	dependencies, err := buildServerDependencies(config.Config{
		OpenAIEnabled:            true,
		OpenAICredentialRef:      "FORNIX_OPENAI_API_KEY",
		CredentialManagerURL:     "https://credentials.example.test/v1/resolve",
		CredentialManagerTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if dependencies.OpenAISecretManager != nil {
		t.Fatal("development environment-key reference was silently redirected to the credential manager")
	}
}
