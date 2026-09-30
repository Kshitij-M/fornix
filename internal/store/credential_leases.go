package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/credentials"
)

var (
	// ErrCredentialLeaseNotFound is deliberately indistinguishable from a
	// cross-workspace lookup to callers. A missing or foreign lease must fail
	// closed without becoming an isolation oracle.
	ErrCredentialLeaseNotFound = errors.New("credential lease not found")
	ErrCredentialLeaseConflict = errors.New("credential lease authority conflict")
	ErrCredentialLeaseResolve  = errors.New("credential lease secret resolution failed")
)

// CredentialLeaseStore is the durable lease authority for provider-neutral
// secret use. It stores only the logical reference, fencing metadata, and
// lifecycle audit; SecretResolver owns the secret bytes.
type CredentialLeaseStore struct {
	pool    *pgxpool.Pool
	resolve credentials.SecretResolver
}

// NewCredentialLeaseStore constructs a durable lease store. A nil resolver is
// allowed for inspection-only deployments, but Acquire will fail closed.
func NewCredentialLeaseStore(pool *pgxpool.Pool, resolve credentials.SecretResolver) *CredentialLeaseStore {
	return &CredentialLeaseStore{pool: pool, resolve: resolve}
}

// Acquire resolves a current workspace credential, then records a new fenced
// lease. Secret bytes are held only by the returned in-memory Lease. Existing
// active holders are expired transactionally before the new fence is issued.
func (s *CredentialLeaseStore) Acquire(ctx context.Context, workspaceID, reference, purpose string, ttl time.Duration) (credentials.Lease, error) {
	if s == nil || s.pool == nil || s.resolve == nil {
		return credentials.Lease{}, fmt.Errorf("credential lease authority is not configured")
	}
	workspaceID, reference, purpose = strings.TrimSpace(workspaceID), strings.TrimSpace(reference), strings.TrimSpace(purpose)
	if workspaceID == "" || reference == "" || purpose == "" {
		return credentials.Lease{}, credentials.ErrLeaseInvalid
	}
	ref, err := credentials.ParseRef(reference)
	if err != nil {
		return credentials.Lease{}, err
	}
	provider, err := providerForCredentialPurpose(purpose)
	if err != nil {
		return credentials.Lease{}, err
	}

	// Resolve outside the transaction so an external secret manager cannot
	// hold a Postgres lock while doing network I/O. The identity/version is
	// checked again before commit below.
	current, err := s.readCredentialReference(ctx, workspaceID, provider, reference)
	if err != nil {
		return credentials.Lease{}, err
	}
	resolved, resolveErr := s.resolveSecret(ctx, workspaceID, ref, purpose)
	if resolveErr != nil {
		return credentials.Lease{}, resolveErr
	}
	secret := resolved.Secret
	resolved.Secret = credentials.Secret{}
	sourceVersion := strings.TrimSpace(resolved.Version)
	sourceExpiresAt := resolved.ExpiresAt.UTC()
	if sourceVersion == "" || len(sourceVersion) > credentials.MaxSourceVersionLength {
		secret.Clear()
		return credentials.Lease{}, credentials.ErrLeaseInvalid
	}
	now := time.Now().UTC()
	if !sourceExpiresAt.IsZero() && !now.Before(sourceExpiresAt) {
		secret.Clear()
		return credentials.Lease{}, credentials.ErrLeaseExpired
	}
	if secret.Len() == 0 {
		secret.Clear()
		return credentials.Lease{}, credentials.ErrInvalidSecret
	}

	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		secret.Clear()
		return credentials.Lease{}, fmt.Errorf("begin credential lease acquire: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var refID, refProvider, refValue, refStatus string
	var version int
	var expiresAt *time.Time
	if err := tx.QueryRow(ctx, `
		SELECT id,provider,reference,version,status,expires_at
		FROM fornix.credential_references
		WHERE workspace_id=$1 AND id=$2
		FOR UPDATE`, workspaceID, current.ID).Scan(&refID, &refProvider, &refValue, &version, &refStatus, &expiresAt); err != nil {
		secret.Clear()
		if errors.Is(err, pgx.ErrNoRows) {
			return credentials.Lease{}, ErrCredentialLeaseNotFound
		}
		return credentials.Lease{}, fmt.Errorf("lock credential reference: %w", err)
	}
	if refProvider != provider || refValue != reference || refStatus != contracts.CredentialActive || version != current.Version || (expiresAt != nil && !time.Now().UTC().Before(*expiresAt)) {
		secret.Clear()
		return credentials.Lease{}, credentials.ErrLeaseRevoked
	}

	// The reference row lock serializes fencing-token allocation for one
	// logical credential. Marking prior holders expired makes the latest
	// acquisition the only valid owner; ValidateLease also checks the exact
	// fence, so stale callers fail closed even if they race this transaction.
	if _, err := tx.Exec(ctx, `
		WITH expired AS (
			UPDATE fornix.credential_leases
			SET status='expired', renewed_at=clock_timestamp()
			WHERE workspace_id=$1 AND credential_ref_id=$2 AND status='active'
			RETURNING id,credential_ref_id,fence,revocation_epoch
		)
		INSERT INTO fornix.credential_lease_events(workspace_id,lease_id,credential_ref_id,event,fence,revocation_epoch)
		SELECT $1,id,credential_ref_id,'expired',fence,revocation_epoch FROM expired`, workspaceID, refID); err != nil {
		secret.Clear()
		return credentials.Lease{}, fmt.Errorf("expire previous credential leases: %w", err)
	}
	var fence int64
	if err := tx.QueryRow(ctx, `
		SELECT COALESCE(MAX(fence),0)+1
		FROM fornix.credential_leases
		WHERE workspace_id=$1 AND credential_ref_id=$2`, workspaceID, refID).Scan(&fence); err != nil {
		secret.Clear()
		return credentials.Lease{}, fmt.Errorf("allocate credential lease fence: %w", err)
	}
	leaseID := contracts.NewID("credlease")
	leaseTTL := boundedCredentialLeaseTTL(ttl)
	leaseExpires := now.Add(leaseTTL)
	if !sourceExpiresAt.IsZero() && sourceExpiresAt.Before(leaseExpires) {
		leaseExpires = sourceExpiresAt
	}
	if !now.Before(leaseExpires) {
		secret.Clear()
		return credentials.Lease{}, credentials.ErrLeaseExpired
	}
	var sourceExpiryArg *time.Time
	if !sourceExpiresAt.IsZero() {
		sourceExpiry := sourceExpiresAt
		sourceExpiryArg = &sourceExpiry
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.credential_leases(
			id,workspace_id,credential_ref_id,provider,purpose,fence,revocation_epoch,source_version,source_expires_at,status,expires_at
		) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,'active',$10)`, leaseID, workspaceID, refID, provider, purpose, fence, version, sourceVersion, sourceExpiryArg, leaseExpires); err != nil {
		secret.Clear()
		return credentials.Lease{}, fmt.Errorf("insert credential lease: %w", err)
	}
	if err := appendCredentialLeaseEventTx(ctx, tx, workspaceID, leaseID, refID, "acquired", fence, int64(version), map[string]string{"source_version": sourceVersion}); err != nil {
		secret.Clear()
		return credentials.Lease{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		secret.Clear()
		return credentials.Lease{}, fmt.Errorf("commit credential lease: %w", err)
	}
	return credentials.Lease{Reference: ref, WorkspaceID: workspaceID, LeaseID: leaseID, Purpose: purpose, Fence: uint64(fence), RevocationEpoch: uint64(version), SourceVersion: sourceVersion, SourceExpiresAt: optionalTimeCopy(sourceExpiresAt), ExpiresAt: leaseExpires, Secret: secret}, nil
}

// ValidateLease checks the current database fence and credential lifecycle at
// the external side-effect boundary. It does not read or persist the secret.
func (s *CredentialLeaseStore) ValidateLease(ctx context.Context, lease credentials.Lease) error {
	if s == nil || s.pool == nil {
		return credentials.ErrLeaseInvalid
	}
	if err := lease.Normalize(); err != nil {
		return err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, lease.WorkspaceID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status, reference, refStatus, sourceVersion string
	var fence, epoch, refVersion int64
	var expiresAt, sourceExpiresAt, refExpiresAt *time.Time
	err = tx.QueryRow(ctx, `
		SELECT l.status,l.fence,l.revocation_epoch,l.source_version,l.expires_at,l.source_expires_at,
		       r.reference,r.status,r.version,r.expires_at
		FROM fornix.credential_leases l
		JOIN fornix.credential_references r
		  ON r.workspace_id=l.workspace_id AND r.id=l.credential_ref_id
		WHERE l.workspace_id=$1 AND l.id=$2
		FOR UPDATE`, lease.WorkspaceID, lease.LeaseID).Scan(&status, &fence, &epoch, &sourceVersion, &expiresAt, &sourceExpiresAt, &reference, &refStatus, &refVersion, &refExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrCredentialLeaseNotFound
	}
	if err != nil {
		return fmt.Errorf("validate credential lease: %w", err)
	}
	now := time.Now().UTC()
	if uint64(fence) != lease.Fence || uint64(epoch) != lease.RevocationEpoch || sourceVersion != lease.SourceVersion || !sameOptionalTime(sourceExpiresAt, lease.SourceExpiresAt) || reference != lease.Reference.String() || refStatus != contracts.CredentialActive || status != "active" {
		return credentials.ErrLeaseRevoked
	}
	if expiresAt == nil || !now.Before(expiresAt.UTC()) || (sourceExpiresAt != nil && !now.Before(sourceExpiresAt.UTC())) || (refExpiresAt != nil && !now.Before(refExpiresAt.UTC())) {
		if _, updateErr := tx.Exec(ctx, `UPDATE fornix.credential_leases SET status='expired',renewed_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2 AND status='active'`, lease.WorkspaceID, lease.LeaseID); updateErr != nil {
			return fmt.Errorf("mark expired credential lease: %w", updateErr)
		}
		if eventErr := appendCredentialLeaseEventTx(ctx, tx, lease.WorkspaceID, lease.LeaseID, "", "expired", fence, epoch, nil); eventErr != nil {
			return eventErr
		}
		if commitErr := tx.Commit(ctx); commitErr != nil {
			return commitErr
		}
		return credentials.ErrLeaseExpired
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

// ResolveExisting materializes secret bytes for an already-issued lease. It
// is intentionally separate from Acquire: an effectful adapter must not
// replace the admitted fence while dispatching. The source authority is
// queried outside the database lock, then every non-secret fact is compared
// against the locked lease before the secret is returned.
func (s *CredentialLeaseStore) ResolveExisting(ctx context.Context, request credentials.ExistingLeaseRequest) (credentials.Lease, error) {
	if s == nil || s.pool == nil || s.resolve == nil || strings.TrimSpace(request.WorkspaceID) == "" || strings.TrimSpace(request.LeaseID) == "" {
		return credentials.Lease{}, credentials.ErrLeaseInvalid
	}
	ref, err := credentials.ParseRef(request.Reference)
	if err != nil {
		return credentials.Lease{}, err
	}
	if request.Fence == 0 || request.RevocationEpoch == 0 || strings.TrimSpace(request.SourceVersion) == "" {
		return credentials.Lease{}, credentials.ErrLeaseInvalid
	}
	resolved, err := s.resolveSecret(ctx, request.WorkspaceID, ref, request.Purpose)
	if err != nil {
		return credentials.Lease{}, err
	}
	defer resolved.Clear()
	if resolved.Secret.Len() == 0 || strings.TrimSpace(resolved.Version) != request.SourceVersion || !sameOptionalTime(optionalTimeCopy(resolved.ExpiresAt), request.SourceExpiresAt) {
		resolved.Clear()
		return credentials.Lease{}, credentials.ErrLeaseRevoked
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, request.WorkspaceID)
	if err != nil {
		resolved.Clear()
		return credentials.Lease{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var status, storedReference, sourceVersion string
	var refID string
	var refVersion int
	var fence, epoch int64
	var expiresAt, sourceExpiresAt *time.Time
	if err := tx.QueryRow(ctx, `SELECT l.status,l.credential_ref_id,l.fence,l.revocation_epoch,l.source_version,l.expires_at,l.source_expires_at,r.reference,r.version FROM fornix.credential_leases l JOIN fornix.credential_references r ON r.workspace_id=l.workspace_id AND r.id=l.credential_ref_id WHERE l.workspace_id=$1 AND l.id=$2 FOR SHARE`, request.WorkspaceID, request.LeaseID).Scan(&status, &refID, &fence, &epoch, &sourceVersion, &expiresAt, &sourceExpiresAt, &storedReference, &refVersion); err != nil {
		resolved.Clear()
		if errors.Is(err, pgx.ErrNoRows) {
			return credentials.Lease{}, ErrCredentialLeaseNotFound
		}
		return credentials.Lease{}, err
	}
	now := time.Now().UTC()
	if status != "active" || storedReference != request.Reference || fence <= 0 || epoch <= 0 || uint64(fence) != request.Fence || uint64(epoch) != request.RevocationEpoch || sourceVersion != request.SourceVersion || !sameOptionalTime(sourceExpiresAt, request.SourceExpiresAt) || expiresAt == nil || !expiresAt.After(now) || sourceExpiresAt != nil && !sourceExpiresAt.After(now) || refVersion != int(epoch) {
		resolved.Clear()
		return credentials.Lease{}, credentials.ErrLeaseRevoked
	}
	secretBytes := resolved.Secret.Bytes()
	secret, secretErr := credentials.NewSecret(secretBytes)
	for i := range secretBytes {
		secretBytes[i] = 0
	}
	if secretErr != nil {
		return credentials.Lease{}, secretErr
	}
	lease := credentials.Lease{Reference: ref, WorkspaceID: request.WorkspaceID, LeaseID: request.LeaseID, Purpose: request.Purpose, Fence: uint64(fence), RevocationEpoch: uint64(epoch), SourceVersion: sourceVersion, SourceExpiresAt: optionalTimeCopyPtr(sourceExpiresAt), ExpiresAt: expiresAt.UTC(), Secret: secret}
	if err := tx.Commit(ctx); err != nil {
		lease.Secret.Clear()
		return credentials.Lease{}, err
	}
	return lease, nil
}

// resolveSecret moves all resolver implementations through one cleanup point.
// A buggy resolver returning material and an error cannot leave the secret
// buffer live in the caller or expose arbitrary error text.
func (s *CredentialLeaseStore) resolveSecret(ctx context.Context, workspaceID string, ref credentials.Ref, purpose string) (credentials.ResolvedSecret, error) {
	if versioned, ok := s.resolve.(credentials.VersionedSecretResolver); ok {
		resolved, err := versioned.ResolveVersioned(ctx, workspaceID, ref, purpose)
		if err != nil {
			resolved.Clear()
			return credentials.ResolvedSecret{}, ErrCredentialLeaseResolve
		}
		return resolved, nil
	}
	secret, err := s.resolve.Resolve(ctx, workspaceID, ref, purpose)
	if err != nil {
		secret.Clear()
		return credentials.ResolvedSecret{}, ErrCredentialLeaseResolve
	}
	return credentials.ResolvedSecret{Secret: secret, Version: credentials.LocalSourceVersion}, nil
}

// Renew extends exactly one current fence. It never changes a fencing token;
// takeover always gets a strictly higher one through Acquire.
func (s *CredentialLeaseStore) Renew(ctx context.Context, lease credentials.Lease, ttl time.Duration) (credentials.Lease, error) {
	if err := lease.Normalize(); err != nil {
		return credentials.Lease{}, err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, lease.WorkspaceID)
	if err != nil {
		return credentials.Lease{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	now := time.Now().UTC()
	newExpiry := now.Add(boundedCredentialLeaseTTL(ttl))
	var status, sourceVersion string
	var refID string
	var epoch int64
	var sourceExpiresAt *time.Time
	if err := tx.QueryRow(ctx, `
		UPDATE fornix.credential_leases
		SET expires_at=LEAST($1,COALESCE(source_expires_at,$1)),renewed_at=clock_timestamp()
		WHERE workspace_id=$2 AND id=$3 AND fence=$4 AND source_version=$5 AND status='active' AND expires_at>clock_timestamp() AND (source_expires_at IS NULL OR source_expires_at>clock_timestamp())
		RETURNING status,credential_ref_id,revocation_epoch,source_version,source_expires_at,expires_at`, newExpiry, lease.WorkspaceID, lease.LeaseID, lease.Fence, lease.SourceVersion).Scan(&status, &refID, &epoch, &sourceVersion, &sourceExpiresAt, &newExpiry); errors.Is(err, pgx.ErrNoRows) {
		return credentials.Lease{}, credentials.ErrLeaseRevoked
	} else if err != nil {
		return credentials.Lease{}, fmt.Errorf("renew credential lease: %w", err)
	}
	if err := appendCredentialLeaseEventTx(ctx, tx, lease.WorkspaceID, lease.LeaseID, refID, "renewed", int64(lease.Fence), epoch, map[string]string{"source_version": sourceVersion}); err != nil {
		return credentials.Lease{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return credentials.Lease{}, err
	}
	lease.ExpiresAt = newExpiry
	lease.RevocationEpoch = uint64(epoch)
	lease.SourceVersion = sourceVersion
	lease.SourceExpiresAt = optionalTimeCopyPtr(sourceExpiresAt)
	return lease, nil
}

func optionalTimeCopy(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	copyValue := value.UTC()
	return &copyValue
}

func optionalTimeCopyPtr(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copyValue := value.UTC()
	return &copyValue
}

func sameOptionalTime(left, right *time.Time) bool {
	if left == nil || right == nil {
		return left == nil && right == nil
	}
	return left.UTC().Equal(right.UTC())
}

// Release is idempotent for the exact current fence. A stale holder cannot
// release a replacement lease.
func (s *CredentialLeaseStore) Release(ctx context.Context, lease credentials.Lease) error {
	if err := lease.Normalize(); err != nil {
		return err
	}
	tx, err := beginWorkspaceTx(ctx, s.pool, lease.WorkspaceID)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var refID string
	var epoch int64
	err = tx.QueryRow(ctx, `
		UPDATE fornix.credential_leases
		SET status='released',released_at=clock_timestamp(),renewed_at=clock_timestamp()
		WHERE workspace_id=$1 AND id=$2 AND fence=$3 AND status='active'
		RETURNING credential_ref_id,revocation_epoch`, lease.WorkspaceID, lease.LeaseID, lease.Fence).Scan(&refID, &epoch)
	if errors.Is(err, pgx.ErrNoRows) {
		return credentials.ErrLeaseRevoked
	}
	if err != nil {
		return fmt.Errorf("release credential lease: %w", err)
	}
	if err := appendCredentialLeaseEventTx(ctx, tx, lease.WorkspaceID, lease.LeaseID, refID, "released", int64(lease.Fence), epoch, nil); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	return nil
}

// revokeCredentialLeasesTx fences every active holder of a reference in the
// same transaction as credential rotation/revocation. The event insert keeps
// the lifecycle auditable without persisting secret material.
func revokeCredentialLeasesTx(ctx context.Context, tx pgx.Tx, workspaceID, refID string) error {
	_, err := tx.Exec(ctx, `
		WITH revoked AS (
			UPDATE fornix.credential_leases
			SET status='revoked',revoked_at=clock_timestamp(),renewed_at=clock_timestamp()
			WHERE workspace_id=$1 AND credential_ref_id=$2 AND status='active'
			RETURNING id,credential_ref_id,fence,revocation_epoch
		)
		INSERT INTO fornix.credential_lease_events(workspace_id,lease_id,credential_ref_id,event,fence,revocation_epoch)
		SELECT $1,id,credential_ref_id,'revoked',fence,revocation_epoch FROM revoked`, workspaceID, refID)
	if err != nil {
		return fmt.Errorf("revoke credential leases: %w", err)
	}
	return nil
}

func (s *CredentialLeaseStore) readCredentialReference(ctx context.Context, workspaceID, provider, reference string) (contracts.CredentialRef, error) {
	tx, err := beginWorkspaceTx(ctx, s.pool, workspaceID)
	if err != nil {
		return contracts.CredentialRef{}, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var ref contracts.CredentialRef
	err = tx.QueryRow(ctx, `
		SELECT id,workspace_id,provider,name,reference,version,status,COALESCE(rotated_from,''),expires_at,revoked_at,created_at,updated_at
		FROM fornix.credential_references
		WHERE workspace_id=$1 AND provider=$2 AND reference=$3 AND status='active'
		  AND (expires_at IS NULL OR expires_at>clock_timestamp())
		ORDER BY version DESC LIMIT 1`, workspaceID, provider, reference).Scan(&ref.ID, &ref.WorkspaceID, &ref.Provider, &ref.Name, &ref.Reference, &ref.Version, &ref.Status, &ref.RotatedFrom, &ref.ExpiresAt, &ref.RevokedAt, &ref.CreatedAt, &ref.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.CredentialRef{}, ErrCredentialLeaseNotFound
	}
	if err != nil {
		return contracts.CredentialRef{}, fmt.Errorf("read credential reference: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.CredentialRef{}, err
	}
	return ref, nil
}

func appendCredentialLeaseEventTx(ctx context.Context, tx pgx.Tx, workspaceID, leaseID, refID, event string, fence, epoch int64, metadata map[string]string) error {
	if refID == "" {
		if err := tx.QueryRow(ctx, `SELECT credential_ref_id FROM fornix.credential_leases WHERE workspace_id=$1 AND id=$2`, workspaceID, leaseID).Scan(&refID); err != nil {
			return fmt.Errorf("read credential lease reference: %w", err)
		}
	}
	if metadata == nil {
		metadata = map[string]string{}
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.credential_lease_events(workspace_id,lease_id,credential_ref_id,event,fence,revocation_epoch,metadata)
		VALUES($1,$2,$3,$4,$5,$6,$7::jsonb)`, workspaceID, leaseID, refID, event, fence, epoch, encoded); err != nil {
		return fmt.Errorf("append credential lease event: %w", err)
	}
	return nil
}

func providerForCredentialPurpose(purpose string) (string, error) {
	parts := strings.SplitN(strings.TrimSpace(purpose), ":", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[1]) == "" {
		return "", fmt.Errorf("credential lease purpose must include provider")
	}
	return strings.TrimSpace(parts[1]), nil
}

func boundedCredentialLeaseTTL(ttl time.Duration) time.Duration {
	if ttl <= 0 {
		return credentials.DefaultLeaseTTL
	}
	if ttl > credentials.MaxLeaseTTL {
		return credentials.MaxLeaseTTL
	}
	return ttl
}
