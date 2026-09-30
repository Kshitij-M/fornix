package credentials

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

// TestDeploymentAuthority is a bounded in-memory authority used to qualify
// deployment composition without contacting a cloud secret manager. It
// implements the same SecretManager and TokenSource seams as a hosted
// workload-identity or mTLS authority. Secret bytes never leave the immediate
// in-memory return boundary and are not included in observations.
type TestDeploymentAuthority struct {
	mu            sync.Mutex
	now           func() time.Time
	items         map[string]*deploymentSecret
	token         Secret
	observations  []contracts.DeploymentAuthorityObservation
	rotationLimit int
}

type deploymentSecret struct {
	secret              Secret
	version             string
	expiresAt           time.Time
	revokedAt           time.Time
	epoch               uint64
	rotatedAt           time.Time
	lastObservedVersion string
}

// NewTestDeploymentAuthority creates a secret-free-by-default test authority.
func NewTestDeploymentAuthority() *TestDeploymentAuthority {
	return &TestDeploymentAuthority{now: func() time.Time { return time.Now().UTC() }, items: make(map[string]*deploymentSecret), rotationLimit: 1024}
}

func (a *TestDeploymentAuthority) clock() time.Time {
	if a != nil && a.now != nil {
		return a.now().UTC()
	}
	return time.Now().UTC()
}

// SetNow makes expiry and rotation-lag tests deterministic.
func (a *TestDeploymentAuthority) SetNow(now func() time.Time) {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.now = now
	a.mu.Unlock()
}

// Put publishes a new secret version for one workspace-scoped request. It is
// intentionally an explicit test-only API; production authorities own this
// lifecycle outside the Fornix process.
func (a *TestDeploymentAuthority) Put(workspaceID, provider string, ref Ref, purpose string, value []byte, ttl time.Duration) error {
	if a == nil {
		return ErrManagedUnavailable
	}
	request := ResolveRequest{WorkspaceID: workspaceID, Provider: provider, Reference: ref, Purpose: purpose}
	if err := request.Normalize(); err != nil {
		return err
	}
	secret, err := NewSecret(value)
	if err != nil {
		return err
	}
	if ttl <= 0 || ttl > MaxLeaseTTL {
		return ErrManagedExpired
	}
	now := a.clock()
	a.mu.Lock()
	defer a.mu.Unlock()
	key := deploymentKey(request)
	item := a.items[key]
	if item == nil {
		item = &deploymentSecret{epoch: 1}
		a.items[key] = item
	} else {
		item.secret.Clear()
		item.epoch++
	}
	item.secret = secret
	item.version = fmt.Sprintf("test-v-%d", item.epoch)
	item.expiresAt = now.Add(ttl)
	item.revokedAt = time.Time{}
	item.rotatedAt = now
	item.lastObservedVersion = ""
	return nil
}

// Rotate replaces an existing version and increments its revocation epoch.
func (a *TestDeploymentAuthority) Rotate(workspaceID, provider string, ref Ref, purpose string, value []byte, ttl time.Duration) error {
	if a == nil {
		return ErrManagedUnavailable
	}
	request := ResolveRequest{WorkspaceID: workspaceID, Provider: provider, Reference: ref, Purpose: purpose}
	if err := request.Normalize(); err != nil {
		return err
	}
	secret, err := NewSecret(value)
	if err != nil {
		return err
	}
	if ttl <= 0 || ttl > MaxLeaseTTL {
		return ErrManagedExpired
	}
	now := a.clock()
	a.mu.Lock()
	defer a.mu.Unlock()
	item := a.items[deploymentKey(request)]
	if item == nil {
		secret.Clear()
		return ErrNotFound
	}
	previous := item.version
	item.secret.Clear()
	item.secret = secret
	item.epoch++
	item.version = fmt.Sprintf("test-v-%d", item.epoch)
	item.expiresAt = now.Add(ttl)
	item.revokedAt = time.Time{}
	item.rotatedAt = now
	a.recordObservationLocked(request, item, previous, now, true, false)
	return nil
}

// Revoke fences all existing leases for one request. Resolve fails closed
// until Put or Rotate publishes a new version.
func (a *TestDeploymentAuthority) Revoke(workspaceID, provider string, ref Ref, purpose string) error {
	if a == nil {
		return ErrManagedUnavailable
	}
	request := ResolveRequest{WorkspaceID: workspaceID, Provider: provider, Reference: ref, Purpose: purpose}
	if err := request.Normalize(); err != nil {
		return err
	}
	now := a.clock()
	a.mu.Lock()
	defer a.mu.Unlock()
	item := a.items[deploymentKey(request)]
	if item == nil {
		return ErrNotFound
	}
	item.epoch++
	item.revokedAt = now
	a.recordObservationLocked(request, item, item.lastObservedVersion, now, false, true)
	return nil
}

// Resolve implements SecretManager.
func (a *TestDeploymentAuthority) Resolve(ctx context.Context, request ResolveRequest) (ResolvedSecret, error) {
	if err := ctx.Err(); err != nil {
		return ResolvedSecret{}, err
	}
	if err := request.Normalize(); err != nil {
		return ResolvedSecret{}, err
	}
	if a == nil {
		return ResolvedSecret{}, ErrManagedUnavailable
	}
	now := a.clock()
	a.mu.Lock()
	defer a.mu.Unlock()
	item := a.items[deploymentKey(request)]
	if item == nil {
		return ResolvedSecret{}, ErrNotFound
	}
	if !item.revokedAt.IsZero() || !now.Before(item.expiresAt) || item.secret.Len() == 0 {
		if !item.revokedAt.IsZero() {
			a.recordObservationLocked(request, item, item.lastObservedVersion, now, false, true)
		}
		return ResolvedSecret{}, ErrManagedDenied
	}
	copySecret, err := NewSecret(item.secret.Bytes())
	if err != nil {
		return ResolvedSecret{}, err
	}
	previous := item.lastObservedVersion
	rotationObserved := previous != "" && previous != item.version
	item.lastObservedVersion = item.version
	a.recordObservationLocked(request, item, previous, now, rotationObserved, false)
	return ResolvedSecret{Secret: copySecret, Version: item.version, ExpiresAt: item.expiresAt}, nil
}

// Token implements TokenSource for the bounded manager protocol.
func (a *TestDeploymentAuthority) Token(ctx context.Context) (Secret, error) {
	if err := ctx.Err(); err != nil {
		return Secret{}, err
	}
	if a == nil {
		return Secret{}, ErrManagedUnavailable
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.token.Len() == 0 {
		return Secret{}, ErrManagedDenied
	}
	return NewSecret(a.token.Bytes())
}

// SetToken configures a test-only manager token and copies it into memory.
func (a *TestDeploymentAuthority) SetToken(value []byte) error {
	if a == nil {
		return ErrManagedUnavailable
	}
	secret, err := NewSecret(value)
	if err != nil {
		return err
	}
	a.mu.Lock()
	a.token.Clear()
	a.token = secret
	a.mu.Unlock()
	return nil
}

// Observations returns redacted authority facts for rotation/revocation
// qualification. The result is bounded and safe to include in a report.
func (a *TestDeploymentAuthority) Observations() []contracts.DeploymentAuthorityObservation {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]contracts.DeploymentAuthorityObservation(nil), a.observations...)
}

func (a *TestDeploymentAuthority) recordObservationLocked(request ResolveRequest, item *deploymentSecret, previous string, now time.Time, rotated, revoked bool) {
	if len(a.observations) >= a.rotationLimit {
		a.observations = a.observations[1:]
	}
	rotationLag := int64(0)
	revocationLag := int64(0)
	if rotated && !item.rotatedAt.IsZero() {
		rotationLag = maxInt64(0, now.Sub(item.rotatedAt).Milliseconds())
	}
	if revoked && !item.revokedAt.IsZero() {
		revocationLag = maxInt64(0, now.Sub(item.revokedAt).Milliseconds())
	}
	observation := contracts.DeploymentAuthorityObservation{
		SchemaVersion: contracts.DeploymentCertificateSchemaVersion,
		WorkspaceID:   request.WorkspaceID, Provider: request.Provider,
		SourceVersion: item.version, PreviousVersion: previous,
		RevocationEpoch: item.epoch, ObservedAt: now,
		RotatedAt: item.rotatedAt, RevokedAt: item.revokedAt,
		RotationLagMillis: rotationLag, RevocationLagMS: revocationLag,
		ObservationHash: contracts.DeploymentAuthorityHash(request.WorkspaceID, request.Provider, item.version, item.epoch),
	}
	a.observations = append(a.observations, observation)
}

func deploymentKey(request ResolveRequest) string {
	return request.WorkspaceID + "\x00" + request.Provider + "\x00" + request.Reference.String() + "\x00" + request.Purpose
}

func maxInt64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}
