package credentials

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestAuthorityProbeIsRedactedBoundedAndStable(t *testing.T) {
	authority := NewTestDeploymentAuthority()
	now := time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC)
	authority.SetNow(func() time.Time { return now })
	if err := authority.SetToken([]byte("manager-token-never-report")); err != nil {
		t.Fatal(err)
	}
	ref, err := ParseRef("provider/openai")
	if err != nil {
		t.Fatal(err)
	}
	if err := authority.Put("workspace-a", "openai", ref, "model:openai", []byte("secret-never-report"), time.Minute); err != nil {
		t.Fatal(err)
	}
	probe := &AuthorityProbe{Manager: authority, Token: authority, Timeout: time.Second, Now: func() time.Time { return now }}
	request := AuthorityProbeRequest{WorkspaceID: "workspace-a", Provider: "openai", Reference: ref, Purpose: "model:openai", CheckToken: true}
	first, err := probe.Run(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := probe.Run(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if first.ReportHash != second.ReportHash || first.Outcome != "healthy" || first.SourceVersion == "" || !first.SecretPresent || !first.TokenPresent {
		t.Fatalf("unstable or incomplete probe: first=%+v second=%+v", first, second)
	}
	encoded, err := json.Marshal(first)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"secret-never-report", "manager-token-never-report"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("probe report leaked secret material: %s", encoded)
		}
	}
}

func TestAuthorityProbeFailsClosedForWorkspaceAndExpiry(t *testing.T) {
	ref, err := ParseRef("provider/openai")
	if err != nil {
		t.Fatal(err)
	}
	manager := SecretManagerFunc(func(_ context.Context, request ResolveRequest) (ResolvedSecret, error) {
		if request.WorkspaceID != "workspace-a" {
			return ResolvedSecret{}, ErrManagedDenied
		}
		secret, secretErr := NewSecret([]byte("secret-never-error"))
		if secretErr != nil {
			return ResolvedSecret{}, secretErr
		}
		return ResolvedSecret{Secret: secret, Version: "v1", ExpiresAt: time.Now().UTC().Add(-time.Second)}, nil
	})
	probe := &AuthorityProbe{Manager: manager, Now: func() time.Time { return time.Now().UTC() }}
	_, err = probe.Run(context.Background(), AuthorityProbeRequest{WorkspaceID: "workspace-a", Provider: "openai", Reference: ref, Purpose: "model:openai"})
	if !errors.Is(err, ErrAuthorityQualification) {
		t.Fatalf("expired authority error=%v, want qualification failure", err)
	}
	if strings.Contains(err.Error(), "secret-never-error") {
		t.Fatalf("authority secret leaked in error: %v", err)
	}
	_, err = probe.Run(context.Background(), AuthorityProbeRequest{WorkspaceID: "workspace-b", Provider: "openai", Reference: ref, Purpose: "model:openai"})
	if !errors.Is(err, ErrAuthorityQualification) {
		t.Fatalf("cross-workspace authority error=%v, want qualification failure", err)
	}
}

func TestAuthorityProbeRejectsUnsafeBounds(t *testing.T) {
	ref, err := ParseRef("provider/openai")
	if err != nil {
		t.Fatal(err)
	}
	probe := &AuthorityProbe{Manager: SecretManagerFunc(func(context.Context, ResolveRequest) (ResolvedSecret, error) {
		return ResolvedSecret{}, ErrManagedUnavailable
	}), Timeout: maxAuthorityQualificationTimeout + time.Nanosecond}
	if _, err := probe.Run(context.Background(), AuthorityProbeRequest{WorkspaceID: "workspace-a", Provider: "openai", Reference: ref, Purpose: "model:openai"}); !errors.Is(err, ErrAuthorityQualification) {
		t.Fatalf("oversized timeout error=%v, want qualification failure", err)
	}
}
