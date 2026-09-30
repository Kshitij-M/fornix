package credentials

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

var (
	ErrLeaseInvalid = errors.New("credential lease is invalid")
	ErrLeaseExpired = errors.New("credential lease is expired")
	ErrLeaseScope   = errors.New("credential lease is outside the requested scope")
	// ErrLeaseRevoked identifies a lease whose credential authority has been
	// revoked or rotated. Providers must fail closed before sending bytes.
	ErrLeaseRevoked = errors.New("credential lease is revoked")
)

const (
	MaxLeaseIDLength       = 128
	MaxPurposeLength       = 128
	MaxSourceVersionLength = 128
	// LocalSourceVersion identifies leases created through the explicit
	// owner-only development profile. It is intentionally not presented as a
	// managed secret-manager version.
	LocalSourceVersion         = "local-unversioned"
	DefaultLeaseTTL            = 5 * time.Minute
	MaxLeaseTTL                = 1 * time.Hour
	DefaultLeaseReleaseTimeout = 2 * time.Second
)

// Lease is the redacted result of one credential acquisition. Secret is
// available only to the immediate adapter boundary and is excluded from all
// text/JSON serialization. External secret-manager implementations should
// return a short expiry and revoke or invalidate the lease on release.
type Lease struct {
	Reference   Ref    `json:"reference"`
	WorkspaceID string `json:"workspace_id"`
	LeaseID     string `json:"lease_id"`
	Purpose     string `json:"purpose"`
	// Fence is monotonically increased for every acquisition of a logical
	// credential reference. It prevents an older holder from being mistaken
	// for the current holder after expiry or rotation.
	Fence uint64 `json:"fence"`
	// RevocationEpoch changes whenever the referenced credential is revoked or
	// rotated. A validator can reject a lease without inspecting its secret.
	RevocationEpoch uint64 `json:"revocation_epoch"`
	// SourceVersion is an opaque, non-secret version returned by the managed
	// secret authority. It binds this in-memory lease to the source snapshot
	// that supplied Secret.
	SourceVersion   string     `json:"source_version"`
	SourceExpiresAt *time.Time `json:"source_expires_at,omitempty"`
	ExpiresAt       time.Time  `json:"expires_at"`
	Secret          Secret     `json:"-"`
}

// Normalize validates the non-secret lease envelope and applies no clock
// decision. Validate performs the time and scope checks for a specific use.
func (l *Lease) Normalize() error {
	if l == nil {
		return ErrLeaseInvalid
	}
	ref, err := ParseRef(l.Reference.String())
	if err != nil {
		return ErrLeaseInvalid
	}
	l.Reference = ref
	l.WorkspaceID = strings.TrimSpace(l.WorkspaceID)
	l.LeaseID = strings.TrimSpace(l.LeaseID)
	l.Purpose = strings.TrimSpace(l.Purpose)
	l.SourceVersion = strings.TrimSpace(l.SourceVersion)
	if l.SourceVersion == "" {
		l.SourceVersion = LocalSourceVersion
	}
	if l.SourceExpiresAt != nil {
		expiry := l.SourceExpiresAt.UTC()
		l.SourceExpiresAt = &expiry
		if !expiry.After(time.Time{}) && l.SourceVersion != LocalSourceVersion {
			return ErrLeaseInvalid
		}
	}
	if l.WorkspaceID == "" || l.LeaseID == "" || len(l.LeaseID) > MaxLeaseIDLength || l.Purpose == "" || len(l.Purpose) > MaxPurposeLength || l.SourceVersion == "" || len(l.SourceVersion) > MaxSourceVersionLength || l.Fence == 0 || l.RevocationEpoch == 0 || l.ExpiresAt.IsZero() || l.Secret.Len() == 0 {
		return ErrLeaseInvalid
	}
	return nil
}

// Validate proves that this lease is usable for exactly one workspace,
// logical reference, and purpose at now. Expiry is checked with UTC-safe
// time comparison and no secret bytes are inspected or returned.
func (l Lease) Validate(workspaceID, reference, purpose string, now time.Time) error {
	if err := l.Normalize(); err != nil {
		return err
	}
	parsed, err := ParseRef(reference)
	if err != nil || parsed.String() != l.Reference.String() || strings.TrimSpace(workspaceID) != l.WorkspaceID || strings.TrimSpace(purpose) != l.Purpose {
		return ErrLeaseScope
	}
	if !now.Before(l.ExpiresAt) {
		return ErrLeaseExpired
	}
	if l.SourceExpiresAt != nil && !now.Before(l.SourceExpiresAt.UTC()) {
		return ErrLeaseExpired
	}
	return nil
}

// LeaseResolver is the integration seam for an external secret manager. It
// owns secret retrieval, rotation, revocation, and durable lease validation.
// Every egress caller validates the exact lease immediately before use; a
// resolver that cannot re-check its authority must fail closed.
type LeaseResolver interface {
	Acquire(context.Context, string, string, string, time.Duration) (Lease, error)
	Release(context.Context, Lease) error
	LeaseValidator
}

// ReleaseLeaseBounded performs best-effort cleanup with a fresh bounded
// context. Release may run after request cancellation or as deferred cleanup;
// a context-aware resolver must honor this deadline. The wrapper cannot
// forcibly interrupt a resolver that violates the context contract. Expired
// leases remain safe if cleanup cannot complete.
func ReleaseLeaseBounded(resolver LeaseResolver, lease Lease) error {
	if resolver == nil {
		lease.Secret.Clear()
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), DefaultLeaseReleaseTimeout)
	defer cancel()
	defer lease.Secret.Clear()
	return resolver.Release(ctx, lease)
}

// LeaseValidator re-checks lease expiry, fencing, and revocation against its
// owning authority. LeaseResolver embeds this contract, so provider callers
// cannot accidentally treat authority validation as an optional enhancement.
type LeaseValidator interface {
	ValidateLease(context.Context, Lease) error
}

// ExistingLeaseRequest asks a durable credential authority to materialize the
// secret for one already-admitted lease. It prevents an effectful adapter from
// silently acquiring a newer fence after admission.
type ExistingLeaseRequest struct {
	WorkspaceID     string
	LeaseID         string
	Reference       string
	Purpose         string
	Fence           uint64
	RevocationEpoch uint64
	SourceVersion   string
	SourceExpiresAt *time.Time
}

// ExistingLeaseResolver is the strict adapter boundary for task-bound or
// effectful calls. Implementations must return the exact requested lease or
// fail closed; they must not substitute a replacement lease.
type ExistingLeaseResolver interface {
	ResolveExisting(context.Context, ExistingLeaseRequest) (Lease, error)
}

// SecretResolver resolves a validated logical reference at the final provider
// boundary. WorkspaceID is mandatory for managed implementations; it prevents
// a resolver from accidentally treating a workspace-scoped reference as a
// process-global secret. Implementations must never return the value through
// logs, JSON, events, or durable database state.
type SecretResolver interface {
	Resolve(context.Context, string, Ref, string) (Secret, error)
}

// VersionedSecretResolver is implemented by managed authorities that can
// return an opaque source version and source expiry in addition to bytes. The
// lease store persists only those non-secret facts.
type VersionedSecretResolver interface {
	ResolveVersioned(context.Context, string, Ref, string) (ResolvedSecret, error)
}

// ResolvedSecret is the in-memory result of one managed lookup. Secret must be
// cleared by the caller when the provider boundary is finished.
type ResolvedSecret struct {
	Secret    Secret
	Version   string
	ExpiresAt time.Time
}

func (s *ResolvedSecret) Clear() {
	if s == nil {
		return
	}
	s.Secret.Clear()
	s.Version = ""
	s.ExpiresAt = time.Time{}
}

// LocalStoreResolver adapts the owner-only local credential store to the
// provider-neutral resolver seam. It is bound to one explicit local workspace
// so even development compatibility mode cannot silently become a
// process-global cross-workspace resolver. A hosted deployment should inject
// a KMS/secret-manager implementation.
type LocalStoreResolver struct {
	Store       *Store
	WorkspaceID string
}

func (r LocalStoreResolver) Resolve(_ context.Context, workspaceID string, ref Ref, _ string) (Secret, error) {
	if r.Store == nil || strings.TrimSpace(r.WorkspaceID) == "" || strings.TrimSpace(workspaceID) != strings.TrimSpace(r.WorkspaceID) {
		return Secret{}, ErrLeaseScope
	}
	return r.Store.Read(ref)
}

// LeaseResolverFunc adapts functions without forcing a secret-manager
// dependency into the universal connector package.
type LeaseResolverFunc struct {
	AcquireFunc       func(context.Context, string, string, string, time.Duration) (Lease, error)
	ReleaseFunc       func(context.Context, Lease) error
	ValidateLeaseFunc func(context.Context, Lease) error
}

func (r LeaseResolverFunc) Acquire(ctx context.Context, workspaceID, reference, purpose string, ttl time.Duration) (Lease, error) {
	if r.AcquireFunc == nil {
		return Lease{}, fmt.Errorf("credential lease acquire is not configured")
	}
	lease, err := r.AcquireFunc(ctx, workspaceID, reference, purpose, boundedTTL(ttl))
	if err != nil {
		lease.Secret.Clear()
		_ = ReleaseLeaseBounded(r, lease)
		return Lease{}, err
	}
	if err := lease.Validate(workspaceID, reference, purpose, time.Now().UTC()); err != nil {
		lease.Secret.Clear()
		_ = ReleaseLeaseBounded(r, lease)
		return Lease{}, err
	}
	if err := r.ValidateLease(ctx, lease); err != nil {
		lease.Secret.Clear()
		_ = ReleaseLeaseBounded(r, lease)
		return Lease{}, err
	}
	return lease, nil
}

func (r LeaseResolverFunc) Release(ctx context.Context, lease Lease) error {
	if r.ReleaseFunc == nil {
		return nil
	}
	return r.ReleaseFunc(ctx, lease)
}

// ValidateLease re-checks current lease authority. The function adapter has no
// implicit local-success behavior: callers that omit ValidateLeaseFunc fail
// closed before egress.
func (r LeaseResolverFunc) ValidateLease(ctx context.Context, lease Lease) error {
	if r.ValidateLeaseFunc == nil {
		return ErrLeaseInvalid
	}
	return r.ValidateLeaseFunc(ctx, lease)
}

func boundedTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return DefaultLeaseTTL
	}
	if ttl > MaxLeaseTTL {
		return MaxLeaseTTL
	}
	return ttl
}
