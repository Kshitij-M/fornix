package credentials

import (
	"context"
	"strings"
	"testing"
)

func TestEnvTokenSourceReadsOnlyConfiguredVariable(t *testing.T) {
	t.Setenv("FORNIX_TEST_MANAGER_TOKEN", "manager-secret")
	source := EnvTokenSource{Name: "FORNIX_TEST_MANAGER_TOKEN"}
	secret, err := source.Token(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(secret.Bytes()) != "manager-secret" {
		t.Fatalf("unexpected token value")
	}
	secret.Clear()
	for _, name := range []string{"", "1BAD", "FORNIX-TOKEN", "FORNIX TOKEN", "FORNIX=TOKEN"} {
		if _, err := (EnvTokenSource{Name: name}).Token(context.Background()); err == nil {
			t.Fatalf("token source accepted unsafe variable name %q", name)
		}
	}
}

func TestEnvTokenSourceDoesNotPutTokenInErrors(t *testing.T) {
	const token = "manager-secret-never-in-error"
	t.Setenv("FORNIX_TEST_MANAGER_TOKEN", token)
	secret, err := (EnvTokenSource{Name: "FORNIX_MISSING_MANAGER_TOKEN"}).Token(context.Background())
	if err == nil || strings.Contains(err.Error(), token) || secret.Len() != 0 {
		t.Fatalf("missing token error leaked secret or returned bytes: err=%v len=%d", err, secret.Len())
	}
}
