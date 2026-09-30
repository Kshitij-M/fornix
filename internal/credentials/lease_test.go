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
	lease := Lease{Reference: ref, WorkspaceID: "workspace-a", LeaseID: "lease-1", Purpose: "http:billing", Fence: 1, RevocationEpoch: 1, ExpiresAt: now.Add(time.Minute), Secret: secret}
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
	for _, invalid := range []Lease{
		{Reference: ref, WorkspaceID: "workspace-a", LeaseID: "lease-zero-fence", Purpose: "http:billing", RevocationEpoch: 1, ExpiresAt: now.Add(time.Minute), Secret: secret},
		{Reference: ref, WorkspaceID: "workspace-a", LeaseID: "lease-zero-epoch", Purpose: "http:billing", Fence: 1, ExpiresAt: now.Add(time.Minute), Secret: secret},
	} {
		if err := invalid.Validate("workspace-a", "provider/api", "http:billing", now); !errors.Is(err, ErrLeaseInvalid) {
			t.Fatalf("lease without positive fence and epoch was accepted: %v", err)
		}
	}
}

func TestLeaseResolverBoundsTTLAndValidatesReturnedLease(t *testing.T) {
	var observed time.Duration
	resolver := LeaseResolverFunc{AcquireFunc: func(_ context.Context, workspace, reference, purpose string, ttl time.Duration) (Lease, error) {
		observed = ttl
		ref, _ := ParseRef(reference)
		secret, _ := NewSecret([]byte("secret"))
		return Lease{Reference: ref, WorkspaceID: workspace, LeaseID: "lease", Purpose: purpose, Fence: 1, RevocationEpoch: 1, ExpiresAt: time.Now().UTC().Add(time.Minute), Secret: secret}, nil
	}, ValidateLeaseFunc: func(context.Context, Lease) error { return nil }}
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
		return Lease{Reference: ref, WorkspaceID: workspace + "-other", LeaseID: "lease", Purpose: purpose, Fence: 1, RevocationEpoch: 1, ExpiresAt: time.Now().UTC().Add(time.Minute), Secret: secret}, nil
	}, ValidateLeaseFunc: func(context.Context, Lease) error { return nil }}
	if _, err := bad.Acquire(context.Background(), "workspace-a", "provider/api", "model", time.Minute); !errors.Is(err, ErrLeaseScope) {
		t.Fatalf("invalid returned lease error=%v", err)
	}
}

func TestLeaseResolverFailsClosedWithoutAuthorityValidation(t *testing.T) {
	ref, _ := ParseRef("provider/api")
	secret, _ := NewSecret([]byte("secret"))
	released := false
	resolver := LeaseResolverFunc{
		AcquireFunc: func(_ context.Context, workspace, reference, purpose string, _ time.Duration) (Lease, error) {
			return Lease{Reference: ref, WorkspaceID: workspace, LeaseID: "lease", Purpose: purpose, Fence: 1, RevocationEpoch: 1, ExpiresAt: time.Now().UTC().Add(time.Minute), Secret: secret}, nil
		},
		ReleaseFunc: func(context.Context, Lease) error { released = true; return nil },
	}
	lease, err := resolver.Acquire(context.Background(), "workspace-a", "provider/api", "model", time.Minute)
	if !errors.Is(err, ErrLeaseInvalid) || lease.Secret.Len() != 0 || !released {
		t.Fatalf("unverifiable lease was not cleared/released: lease=%+v released=%v err=%v", lease, released, err)
	}
}

func TestReleaseLeaseBoundedUsesLiveDeadlineAndClearsSecret(t *testing.T) {
	ref, _ := ParseRef("provider/api")
	secret, _ := NewSecret([]byte("release-secret"))
	lease := Lease{Reference: ref, WorkspaceID: "workspace-a", LeaseID: "lease", Purpose: "model:provider", Fence: 1, RevocationEpoch: 1, ExpiresAt: time.Now().Add(time.Minute), Secret: secret}
	called := false
	resolver := LeaseResolverFunc{ReleaseFunc: func(ctx context.Context, got Lease) error {
		called = true
		if _, ok := ctx.Deadline(); !ok {
			t.Fatal("release context has no bounded deadline")
		}
		if ctx.Err() != nil {
			t.Fatalf("release started with cancelled context: %v", ctx.Err())
		}
		if got.LeaseID != lease.LeaseID {
			t.Fatalf("released lease identity %q changed", got.LeaseID)
		}
		return nil
	}}
	if err := ReleaseLeaseBounded(resolver, lease); err != nil {
		t.Fatalf("bounded release: %v", err)
	}
	if !called {
		t.Fatal("release was not called")
	}
	for _, value := range lease.Secret.Bytes() {
		if value != 0 {
			t.Fatal("released lease secret was not cleared")
		}
	}
}
