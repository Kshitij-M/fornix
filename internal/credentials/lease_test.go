package credentials

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestLeaseValidationIsScopedExpiringAndRedacted(t *testing.T) {
	ref, err := ParseRef("provider/api")
	if err != nil {
		t.Fatal(err)
	}
	secret, err := NewSecret([]byte("do-not-persist"))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(100, 0).UTC()
	lease := Lease{Reference: ref, WorkspaceID: "workspace-a", LeaseID: "lease-1", Purpose: "http:billing", ExpiresAt: now.Add(time.Minute), Secret: secret}
	if err := lease.Validate("workspace-a", "provider/api", "http:billing", now); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(lease.Validate("workspace-b", "provider/api", "http:billing", now), ErrLeaseScope) {
		t.Fatal("cross-workspace lease was accepted")
	}
	if !errors.Is(lease.Validate("workspace-a", "provider/api", "other", now), ErrLeaseScope) {
		t.Fatal("cross-purpose lease was accepted")
	}
	if !errors.Is(lease.Validate("workspace-a", "provider/api", "http:billing", now.Add(time.Minute)), ErrLeaseExpired) {
		t.Fatal("expired lease was accepted")
	}
	raw, marshalErr := json.Marshal(lease)
	if marshalErr != nil {
		t.Fatal(marshalErr)
	}
	if strings.Contains(string(raw), "do-not-persist") || strings.Contains(lease.Secret.String(), "do-not-persist") {
		t.Fatal("secret appeared in lease disclosure")
	}
}

func TestLeaseResolverBoundsTTLAndValidatesReturnedLease(t *testing.T) {
	var observed time.Duration
	resolver := LeaseResolverFunc{AcquireFunc: func(_ context.Context, workspace, reference, purpose string, ttl time.Duration) (Lease, error) {
		observed = ttl
		ref, _ := ParseRef(reference)
		secret, _ := NewSecret([]byte("secret"))
		return Lease{Reference: ref, WorkspaceID: workspace, LeaseID: "lease", Purpose: purpose, ExpiresAt: time.Now().UTC().Add(time.Minute), Secret: secret}, nil
	}}
	lease, err := resolver.Acquire(context.Background(), "workspace-a", "provider/api", "model", 2*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if observed != MaxLeaseTTL || lease.LeaseID != "lease" {
		t.Fatalf("lease=%+v observed ttl=%s", lease, observed)
	}
	bad := LeaseResolverFunc{AcquireFunc: func(_ context.Context, workspace, reference, purpose string, _ time.Duration) (Lease, error) {
		ref, _ := ParseRef(reference)
		secret, _ := NewSecret([]byte("secret"))
		return Lease{Reference: ref, WorkspaceID: workspace + "-other", LeaseID: "lease", Purpose: purpose, ExpiresAt: time.Now().UTC().Add(time.Minute), Secret: secret}, nil
	}}
	if _, err := bad.Acquire(context.Background(), "workspace-a", "provider/api", "model", time.Minute); !errors.Is(err, ErrLeaseScope) {
		t.Fatalf("invalid returned lease error=%v", err)
	}
}
