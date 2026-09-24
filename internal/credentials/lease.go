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
)

const (
	MaxLeaseIDLength = 128
	MaxPurposeLength = 128
	DefaultLeaseTTL  = 5 * time.Minute
	MaxLeaseTTL      = 1 * time.Hour
)

// Lease is the redacted result of one credential acquisition. Secret is
// available only to the immediate adapter boundary and is excluded from all
// text/JSON serialization. External secret-manager implementations should
// return a short expiry and revoke or invalidate the lease on release.
type Lease struct {
	Reference   Ref       `json:"reference"`
	WorkspaceID string    `json:"workspace_id"`
	LeaseID     string    `json:"lease_id"`
	Purpose     string    `json:"purpose"`
	ExpiresAt   time.Time `json:"expires_at"`
	Secret      Secret    `json:"-"`
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
	if l.WorkspaceID == "" || l.LeaseID == "" || len(l.LeaseID) > MaxLeaseIDLength || l.Purpose == "" || len(l.Purpose) > MaxPurposeLength || l.ExpiresAt.IsZero() || l.Secret.Len() == 0 {
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
	return nil
}

// LeaseResolver is the integration seam for an external secret manager. The
// resolver owns secret retrieval, rotation, revocation, and any durable lease
// state; callers must not cache a lease beyond ExpiresAt.
type LeaseResolver interface {
	Acquire(context.Context, string, string, string, time.Duration) (Lease, error)
	Release(context.Context, Lease) error
}

// LeaseResolverFunc adapts functions without forcing a secret-manager
// dependency into the universal connector package.
type LeaseResolverFunc struct {
	AcquireFunc func(context.Context, string, string, string, time.Duration) (Lease, error)
	ReleaseFunc func(context.Context, Lease) error
}

func (r LeaseResolverFunc) Acquire(ctx context.Context, workspaceID, reference, purpose string, ttl time.Duration) (Lease, error) {
	if r.AcquireFunc == nil {
		return Lease{}, fmt.Errorf("credential lease acquire is not configured")
	}
	lease, err := r.AcquireFunc(ctx, workspaceID, reference, purpose, boundedTTL(ttl))
	if err != nil {
		return Lease{}, err
	}
	if err := lease.Validate(workspaceID, reference, purpose, time.Now().UTC()); err != nil {
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

func boundedTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return DefaultLeaseTTL
	}
	if ttl > MaxLeaseTTL {
		return MaxLeaseTTL
	}
	return ttl
}
