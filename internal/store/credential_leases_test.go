package store

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/credentials"
)

type testSecretResolver struct {
	secret credentials.Secret
}

func (r testSecretResolver) Resolve(context.Context, string, credentials.Ref, string) (credentials.Secret, error) {
	return r.secret, nil
}

type versionedTestResolver struct {
	secret    credentials.Secret
	version   string
	expiresAt time.Time
}

type partialSecretResolver struct{ secret credentials.Secret }

func (r partialSecretResolver) Resolve(context.Context, string, credentials.Ref, string) (credentials.Secret, error) {
	return r.secret, errors.New("resolver failed")
}

type partialVersionedSecretResolver struct{ secret credentials.Secret }

func (r partialVersionedSecretResolver) Resolve(context.Context, string, credentials.Ref, string) (credentials.Secret, error) {
	return r.secret, errors.New("resolver failed")
}

func (r partialVersionedSecretResolver) ResolveVersioned(context.Context, string, credentials.Ref, string) (credentials.ResolvedSecret, error) {
	return credentials.ResolvedSecret{Secret: r.secret, Version: "version-1"}, errors.New("resolver failed")
}

func TestCredentialLeaseResolutionClearsPartialSecretsOnError(t *testing.T) {
	ref, err := credentials.ParseRef("provider/openai")
	if err != nil {
		t.Fatal(err)
	}
	for name, makeResolver := range map[string]func(credentials.Secret) credentials.SecretResolver{
		"unversioned": func(secret credentials.Secret) credentials.SecretResolver {
			return partialSecretResolver{secret: secret}
		},
		"versioned": func(secret credentials.Secret) credentials.SecretResolver {
			return partialVersionedSecretResolver{secret: secret}
		},
	} {
		t.Run(name, func(t *testing.T) {
			secret, secretErr := credentials.NewSecret([]byte("partial-secret"))
			if secretErr != nil {
				t.Fatal(secretErr)
			}
			leaseStore := NewCredentialLeaseStore(nil, makeResolver(secret))
			resolved, resolveErr := leaseStore.resolveSecret(context.Background(), "workspace-a", ref, "model:openai")
			for _, value := range secret.Bytes() {
				if value != 0 {
					t.Fatalf("partial secret bytes were not zeroed: err=%v", resolveErr)
				}
			}
			if !errors.Is(resolveErr, ErrCredentialLeaseResolve) || resolved.Secret.Len() != 0 {
				t.Fatalf("partial secret was not cleared: returned=%d err=%v", resolved.Secret.Len(), resolveErr)
			}
		})
	}
}

func (r versionedTestResolver) Resolve(_ context.Context, _ string, _ credentials.Ref, _ string) (credentials.Secret, error) {
	return r.secret, nil
}

func (r versionedTestResolver) ResolveVersioned(_ context.Context, _ string, _ credentials.Ref, _ string) (credentials.ResolvedSecret, error) {
	copySecret, err := credentials.NewSecret(r.secret.Bytes())
	if err != nil {
		return credentials.ResolvedSecret{}, err
	}
	return credentials.ResolvedSecret{Secret: copySecret, Version: r.version, ExpiresAt: r.expiresAt}, nil
}

func newCredentialLeaseTestStore(t *testing.T) (*CredentialLeaseStore, *AuthStore, *pgxpool.Pool, string, string) {
	secret, err := credentials.NewSecret([]byte("lease-test-secret"))
	if err != nil {
		t.Fatal(err)
	}
	return newCredentialLeaseTestStoreWithResolver(t, testSecretResolver{secret: secret})
}

func newCredentialLeaseTestStoreWithResolver(t *testing.T, resolver credentials.SecretResolver) (*CredentialLeaseStore, *AuthStore, *pgxpool.Pool, string, string) {
	t.Helper()
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("create test pool: %v", err)
	}
	if os.Getenv("FORNIX_SKIP_MIGRATIONS") == "" {
		if err := ApplyMigrations(ctx, pool); err != nil {
			pool.Close()
			t.Fatalf("apply migrations: %v", err)
		}
	}
	workspaceID := fmt.Sprintf("test-credential-lease-%d", time.Now().UnixNano())
	refID := ""
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.credential_lease_events WHERE workspace_id=$1`, workspaceID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.credential_leases WHERE workspace_id=$1`, workspaceID)
		if refID != "" {
			_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.credential_references WHERE workspace_id=$1 AND id=$2`, workspaceID, refID)
		}
		pool.Close()
	})
	leaseStore := NewCredentialLeaseStore(pool, resolver)
	auth := NewAuthStore(pool)
	ref, err := auth.CreateCredentialRef(ctx, contracts.CredentialRefInput{WorkspaceID: workspaceID, Provider: "openai", Name: "default", Reference: "provider/openai"})
	if err != nil {
		pool.Close()
		t.Fatalf("create credential reference: %v", err)
	}
	refID = ref.ID
	return leaseStore, auth, pool, workspaceID, ref.Reference
}

func TestCredentialLeaseStoreFencesTakeoverAndRevocation(t *testing.T) {
	leaseStore, auth, _, workspaceID, reference := newCredentialLeaseTestStore(t)
	ctx := context.Background()
	first, err := leaseStore.Acquire(ctx, workspaceID, reference, "model:openai", time.Minute)
	if err != nil {
		t.Fatalf("acquire first lease: %v", err)
	}
	if first.Fence != 1 || first.RevocationEpoch != 1 {
		t.Fatalf("unexpected first fence: %+v", first)
	}
	if err := leaseStore.ValidateLease(ctx, first); err != nil {
		t.Fatalf("validate first lease: %v", err)
	}
	second, err := leaseStore.Acquire(ctx, workspaceID, reference, "model:openai", time.Minute)
	if err != nil {
		t.Fatalf("acquire takeover lease: %v", err)
	}
	if second.Fence != 2 {
		t.Fatalf("takeover fence = %d, want 2", second.Fence)
	}
	if err := leaseStore.ValidateLease(ctx, first); !errors.Is(err, credentials.ErrLeaseRevoked) {
		t.Fatalf("stale lease validation = %v, want revoked", err)
	}
	if err := leaseStore.ValidateLease(ctx, second); err != nil {
		t.Fatalf("validate takeover lease: %v", err)
	}
	ref, err := auth.CredentialRefForUse(ctx, workspaceID, "openai", "default")
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.RevokeCredentialRef(ctx, workspaceID, ref.ID); err != nil {
		t.Fatalf("revoke credential reference: %v", err)
	}
	if err := leaseStore.ValidateLease(ctx, second); !errors.Is(err, credentials.ErrLeaseRevoked) {
		t.Fatalf("revoked lease validation = %v, want revoked", err)
	}
}

func TestCredentialLeaseStoreRenewAndReleaseAreFenced(t *testing.T) {
	leaseStore, _, _, workspaceID, reference := newCredentialLeaseTestStore(t)
	ctx := context.Background()
	lease, err := leaseStore.Acquire(ctx, workspaceID, reference, "model:openai", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	renewed, err := leaseStore.Renew(ctx, lease, 2*time.Minute)
	if err != nil {
		t.Fatalf("renew lease: %v", err)
	}
	if !renewed.ExpiresAt.After(lease.ExpiresAt) {
		t.Fatalf("renewed expiry %v is not after %v", renewed.ExpiresAt, lease.ExpiresAt)
	}
	if err := leaseStore.Release(ctx, renewed); err != nil {
		t.Fatalf("release lease: %v", err)
	}
	if err := leaseStore.ValidateLease(ctx, renewed); !errors.Is(err, credentials.ErrLeaseRevoked) {
		t.Fatalf("released lease validation = %v, want revoked", err)
	}
	if err := leaseStore.Release(ctx, renewed); !errors.Is(err, credentials.ErrLeaseRevoked) {
		t.Fatalf("duplicate release = %v, want revoked", err)
	}
}

func TestCredentialLeaseBindsManagedSourceVersionAndExpiry(t *testing.T) {
	secret, err := credentials.NewSecret([]byte("managed-lease-secret"))
	if err != nil {
		t.Fatal(err)
	}
	resolver := versionedTestResolver{secret: secret, version: "vault-v1", expiresAt: time.Now().UTC().Add(500 * time.Millisecond)}
	leaseStore, _, _, workspaceID, reference := newCredentialLeaseTestStoreWithResolver(t, resolver)
	lease, err := leaseStore.Acquire(context.Background(), workspaceID, reference, "model:openai", time.Minute)
	if err != nil {
		t.Fatalf("acquire managed lease: %v", err)
	}
	if lease.SourceVersion != "vault-v1" {
		t.Fatalf("source version=%q, want vault-v1", lease.SourceVersion)
	}
	if !lease.ExpiresAt.Before(time.Now().UTC().Add(time.Second)) {
		t.Fatalf("lease expiry was not bounded by managed source expiry: %v", lease.ExpiresAt)
	}
	if _, err := leaseStore.Renew(context.Background(), lease, time.Minute); err != nil {
		t.Fatalf("renew managed lease: %v", err)
	}
	if err := leaseStore.ValidateLease(context.Background(), lease); err != nil && !errors.Is(err, credentials.ErrLeaseRevoked) {
		// The short source expiry may have elapsed by the time the validation
		// runs; any other result is a lease-authority error.
		t.Fatalf("validate managed lease: %v", err)
	}
}

func TestCredentialLeaseResolvesTheExactAdmittedFence(t *testing.T) {
	secret, err := credentials.NewSecret([]byte("exact-lease-secret"))
	if err != nil {
		t.Fatal(err)
	}
	resolver := versionedTestResolver{secret: secret, version: "vault-exact-v1", expiresAt: time.Now().UTC().Add(time.Minute)}
	leaseStore, _, _, workspaceID, reference := newCredentialLeaseTestStoreWithResolver(t, resolver)
	lease, err := leaseStore.Acquire(context.Background(), workspaceID, reference, "http:openai", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	exact, err := leaseStore.ResolveExisting(context.Background(), credentials.ExistingLeaseRequest{WorkspaceID: workspaceID, LeaseID: lease.LeaseID, Reference: reference, Purpose: lease.Purpose, Fence: lease.Fence, RevocationEpoch: lease.RevocationEpoch, SourceVersion: lease.SourceVersion, SourceExpiresAt: lease.SourceExpiresAt})
	if err != nil {
		t.Fatalf("resolve exact lease: %v", err)
	}
	if exact.LeaseID != lease.LeaseID || exact.Fence != lease.Fence || exact.SourceVersion != lease.SourceVersion || exact.Secret.Len() == 0 {
		t.Fatalf("exact lease metadata or secret changed: %+v", exact)
	}
	exact.Secret.Clear()
	if _, err := leaseStore.ResolveExisting(context.Background(), credentials.ExistingLeaseRequest{WorkspaceID: workspaceID, LeaseID: lease.LeaseID, Reference: reference, Purpose: lease.Purpose, Fence: lease.Fence + 1, RevocationEpoch: lease.RevocationEpoch, SourceVersion: lease.SourceVersion, SourceExpiresAt: lease.SourceExpiresAt}); !errors.Is(err, credentials.ErrLeaseRevoked) {
		t.Fatalf("stale exact lease error=%v, want revoked", err)
	}
}
