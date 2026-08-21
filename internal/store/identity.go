package store

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrUnauthenticated     = errors.New("unauthenticated")
	ErrAuthorizationDenied = errors.New("authorization denied")
	ErrWorkspaceViolation  = errors.New("workspace access denied")
	ErrIdentityExists      = errors.New("identity already exists")
	ErrIdentityNotFound    = errors.New("identity not found")
	ErrAPIKeyNotFound      = errors.New("api key not found")
	ErrCredentialNotFound  = errors.New("credential reference not found")
	ErrCredentialRevoked   = errors.New("credential reference is revoked")
)

// AuthStore is the Postgres authority for workspace identities, RBAC bindings,
// API-key authentication, and provider credential references. Secrets are
// accepted only at creation/rotation and are never reconstructed from storage.
type AuthStore struct {
	pool *pgxpool.Pool
}

// NewAuthStore constructs an identity and authorization store over pool.
func NewAuthStore(pool *pgxpool.Pool) *AuthStore { return &AuthStore{pool: pool} }

// CreateIdentity creates an active or explicitly disabled/revoked identity in a
// workspace and optionally binds its normalized permissions through a default
// role. The workspace is part of every database lookup.
func (s *AuthStore) CreateIdentity(ctx context.Context, input contracts.IdentityInput) (contracts.Identity, error) {
	if s == nil || s.pool == nil {
		return contracts.Identity{}, fmt.Errorf("identity store is not configured")
	}
	workspaceID := strings.TrimSpace(input.WorkspaceID)
	subject := strings.TrimSpace(input.Subject)
	if workspaceID == "" || subject == "" {
		return contracts.Identity{}, fmt.Errorf("workspace_id and subject are required")
	}
	kind := strings.TrimSpace(input.Kind)
	if kind == "" {
		kind = "user"
	}
	status := strings.TrimSpace(input.Status)
	if status == "" {
		status = contracts.IdentityActive
	}
	if status != contracts.IdentityActive && status != contracts.IdentityRevoked && status != contracts.IdentityDisabled {
		return contracts.Identity{}, fmt.Errorf("invalid identity status %q", status)
	}
	permissions, err := contracts.NormalizePermissions(input.Permissions)
	if err != nil {
		return contracts.Identity{}, err
	}
	id := contracts.NewID("identity")
	now := time.Now().UTC()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.Identity{}, fmt.Errorf("begin identity create: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	identity := contracts.Identity{SchemaVersion: contracts.IdentitySchemaVersion, ID: id, WorkspaceID: workspaceID, Subject: subject, Kind: kind, DisplayName: strings.TrimSpace(input.DisplayName), Status: status, CreatedAt: now, UpdatedAt: now}
	if err := tx.QueryRow(ctx, `
		INSERT INTO fornix.identities(id, workspace_id, subject, kind, display_name, status)
		VALUES($1,$2,$3,$4,$5,$6)
		RETURNING created_at, updated_at`, id, workspaceID, subject, kind, identity.DisplayName, status).Scan(&identity.CreatedAt, &identity.UpdatedAt); err != nil {
		if isUniqueViolation(err) {
			return contracts.Identity{}, ErrIdentityExists
		}
		return contracts.Identity{}, fmt.Errorf("insert identity: %w", err)
	}
	if len(permissions) > 0 {
		roleID := contracts.NewID("role")
		roleName := "default-" + id
		permissionJSON, _ := json.Marshal(permissions)
		if _, err := tx.Exec(ctx, `
			INSERT INTO fornix.roles(id, workspace_id, name, permissions)
			VALUES($1,$2,$3,$4::jsonb)`, roleID, workspaceID, roleName, permissionJSON); err != nil {
			return contracts.Identity{}, fmt.Errorf("insert default role: %w", err)
		}
		if _, err := tx.Exec(ctx, `
			INSERT INTO fornix.identity_role_bindings(workspace_id, identity_id, role_id)
			VALUES($1,$2,$3)`, workspaceID, id, roleID); err != nil {
			return contracts.Identity{}, fmt.Errorf("bind default role: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.Identity{}, fmt.Errorf("commit identity create: %w", err)
	}
	return identity, nil
}

// GrantRole upserts a workspace-scoped role and binds it to an identity after
// verifying that both records belong to the requested workspace.
func (s *AuthStore) GrantRole(ctx context.Context, workspaceID, identityID, name string, permissions []contracts.Permission, grantedBy string) (contracts.Role, error) {
	workspaceID, identityID, name = strings.TrimSpace(workspaceID), strings.TrimSpace(identityID), strings.TrimSpace(name)
	if workspaceID == "" || identityID == "" || name == "" {
		return contracts.Role{}, fmt.Errorf("workspace_id, identity_id, and role name are required")
	}
	normalized, err := contracts.NormalizePermissions(permissions)
	if err != nil {
		return contracts.Role{}, err
	}
	permissionJSON, _ := json.Marshal(normalized)
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.Role{}, fmt.Errorf("begin role grant: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var identityWorkspace string
	if err := tx.QueryRow(ctx, `SELECT workspace_id FROM fornix.identities WHERE id=$1 FOR SHARE`, identityID).Scan(&identityWorkspace); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.Role{}, ErrIdentityNotFound
		}
		return contracts.Role{}, fmt.Errorf("read identity for role: %w", err)
	}
	if identityWorkspace != workspaceID {
		return contracts.Role{}, ErrWorkspaceViolation
	}
	role := contracts.Role{SchemaVersion: contracts.IdentitySchemaVersion, ID: contracts.NewID("role"), WorkspaceID: workspaceID, Name: name, Permissions: normalized}
	if err := tx.QueryRow(ctx, `
		INSERT INTO fornix.roles(id, workspace_id, name, permissions)
		VALUES($1,$2,$3,$4::jsonb)
		ON CONFLICT (workspace_id,name) DO UPDATE SET permissions=EXCLUDED.permissions, updated_at=clock_timestamp()
		RETURNING id, created_at, updated_at`, role.ID, workspaceID, name, permissionJSON).Scan(&role.ID, &role.CreatedAt, &role.UpdatedAt); err != nil {
		return contracts.Role{}, fmt.Errorf("upsert role: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO fornix.identity_role_bindings(workspace_id, identity_id, role_id, granted_by)
		VALUES($1,$2,$3,$4)
		ON CONFLICT (workspace_id,identity_id,role_id) DO UPDATE SET granted_by=EXCLUDED.granted_by, expires_at=NULL`, workspaceID, identityID, role.ID, strings.TrimSpace(grantedBy)); err != nil {
		return contracts.Role{}, fmt.Errorf("bind role: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.Role{}, fmt.Errorf("commit role grant: %w", err)
	}
	return role, nil
}

// CreateAPIKey creates a hashed API key and returns its bearer token exactly
// once. Callers must deliver the returned token securely; it is not recoverable
// from the database.
func (s *AuthStore) CreateAPIKey(ctx context.Context, input contracts.APIKeyInput) (contracts.APIKey, string, error) {
	workspaceID, identityID := strings.TrimSpace(input.WorkspaceID), strings.TrimSpace(input.IdentityID)
	if workspaceID == "" || identityID == "" {
		return contracts.APIKey{}, "", fmt.Errorf("workspace_id and identity_id are required")
	}
	secretBytes := make([]byte, 32)
	if _, err := rand.Read(secretBytes); err != nil {
		return contracts.APIKey{}, "", fmt.Errorf("generate api key secret: %w", err)
	}
	keyID := contracts.NewID("key")
	secret := hex.EncodeToString(secretBytes)
	token := "fornix_" + keyID + "_" + secret
	prefix := "fornix_" + keyID + "_"
	digest := sha256.Sum256([]byte(secret))
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.APIKey{}, "", fmt.Errorf("begin api key create: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var identityWorkspace, identityStatus string
	if err := tx.QueryRow(ctx, `SELECT workspace_id,status FROM fornix.identities WHERE id=$1 FOR SHARE`, identityID).Scan(&identityWorkspace, &identityStatus); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.APIKey{}, "", ErrIdentityNotFound
		}
		return contracts.APIKey{}, "", fmt.Errorf("read api key identity: %w", err)
	}
	if identityWorkspace != workspaceID {
		return contracts.APIKey{}, "", ErrWorkspaceViolation
	}
	if identityStatus != contracts.IdentityActive {
		return contracts.APIKey{}, "", fmt.Errorf("identity is not active")
	}
	key := contracts.APIKey{SchemaVersion: contracts.IdentitySchemaVersion, ID: keyID, WorkspaceID: workspaceID, IdentityID: identityID, Prefix: prefix, Status: contracts.APIKeyActive, ExpiresAt: input.ExpiresAt, Token: token, TokenHash: hex.EncodeToString(digest[:])}
	if err := tx.QueryRow(ctx, `
		INSERT INTO fornix.api_keys(id,workspace_id,identity_id,prefix,token_hash,status,expires_at)
		VALUES($1,$2,$3,$4,$5,$6,$7)
		RETURNING created_at`, keyID, workspaceID, identityID, prefix, digest[:], key.Status, input.ExpiresAt).Scan(&key.CreatedAt); err != nil {
		return contracts.APIKey{}, "", fmt.Errorf("insert api key: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.APIKey{}, "", fmt.Errorf("commit api key: %w", err)
	}
	return key, token, nil
}

// RotateAPIKey revokes an active key and atomically issues its replacement.
// Authentication of the old token stops when the transaction commits.
func (s *AuthStore) RotateAPIKey(ctx context.Context, workspaceID, keyID string, expiresAt *time.Time) (contracts.APIKey, string, error) {
	workspaceID, keyID = strings.TrimSpace(workspaceID), strings.TrimSpace(keyID)
	if workspaceID == "" || keyID == "" {
		return contracts.APIKey{}, "", fmt.Errorf("workspace_id and key_id are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.APIKey{}, "", fmt.Errorf("begin api key rotation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var identityID, status string
	if err := tx.QueryRow(ctx, `SELECT identity_id,status FROM fornix.api_keys WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, workspaceID, keyID).Scan(&identityID, &status); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.APIKey{}, "", ErrAPIKeyNotFound
		}
		return contracts.APIKey{}, "", fmt.Errorf("read api key for rotation: %w", err)
	}
	if status != contracts.APIKeyActive {
		return contracts.APIKey{}, "", ErrAPIKeyNotFound
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.api_keys SET status='revoked', revoked_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2`, workspaceID, keyID); err != nil {
		return contracts.APIKey{}, "", fmt.Errorf("revoke old api key: %w", err)
	}
	secretBytes := make([]byte, 32)
	if _, err := rand.Read(secretBytes); err != nil {
		return contracts.APIKey{}, "", fmt.Errorf("generate rotated api key secret: %w", err)
	}
	newID := contracts.NewID("key")
	secret := hex.EncodeToString(secretBytes)
	token := "fornix_" + newID + "_" + secret
	prefix := "fornix_" + newID + "_"
	digest := sha256.Sum256([]byte(secret))
	key := contracts.APIKey{SchemaVersion: contracts.IdentitySchemaVersion, ID: newID, WorkspaceID: workspaceID, IdentityID: identityID, Prefix: prefix, Status: contracts.APIKeyActive, ExpiresAt: expiresAt, RotatedFrom: keyID, Token: token, TokenHash: hex.EncodeToString(digest[:])}
	if err := tx.QueryRow(ctx, `
		INSERT INTO fornix.api_keys(id,workspace_id,identity_id,prefix,token_hash,status,expires_at,rotated_from)
		VALUES($1,$2,$3,$4,$5,'active',$6,$7)
		RETURNING created_at`, newID, workspaceID, identityID, prefix, digest[:], expiresAt, keyID).Scan(&key.CreatedAt); err != nil {
		return contracts.APIKey{}, "", fmt.Errorf("insert rotated api key: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.APIKey{}, "", fmt.Errorf("commit api key rotation: %w", err)
	}
	return key, token, nil
}

// RevokeAPIKey invalidates an active workspace API key.
func (s *AuthStore) RevokeAPIKey(ctx context.Context, workspaceID, keyID string) error {
	result, err := s.pool.Exec(ctx, `UPDATE fornix.api_keys SET status='revoked', revoked_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2 AND status='active'`, strings.TrimSpace(workspaceID), strings.TrimSpace(keyID))
	if err != nil {
		return fmt.Errorf("revoke api key: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrAPIKeyNotFound
	}
	return nil
}

func parseAPIKey(token string) (string, string, bool) {
	token = strings.TrimSpace(token)
	if !strings.HasPrefix(token, "fornix_") {
		return "", "", false
	}
	value := strings.TrimPrefix(token, "fornix_")
	separator := strings.LastIndexByte(value, '_')
	if separator <= 0 || separator >= len(value)-1 {
		return "", "", false
	}
	keyID, secret := value[:separator], value[separator+1:]
	if strings.TrimSpace(keyID) == "" || strings.TrimSpace(secret) == "" {
		return "", "", false
	}
	return keyID, secret, true
}

// Authenticate verifies an API key in constant time and returns its current
// workspace-scoped principal and permissions. Revoked, expired, and disabled
// credentials fail closed.
func (s *AuthStore) Authenticate(ctx context.Context, token string) (contracts.Principal, error) {
	keyID, secret, ok := parseAPIKey(token)
	if !ok || s == nil || s.pool == nil {
		return contracts.Principal{}, ErrUnauthenticated
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.Principal{}, fmt.Errorf("begin authentication: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var (
		workspaceID, identityID, keyPrefix, keyStatus              string
		identitySubject, identityKind, displayName, identityStatus string
		storedHash                                                 []byte
		expiresAt                                                  *time.Time
	)
	if err := tx.QueryRow(ctx, `
		SELECT k.workspace_id,k.identity_id,k.prefix,k.token_hash,k.status,k.expires_at,
		       i.subject,i.kind,i.display_name,i.status
		FROM fornix.api_keys k
		JOIN fornix.identities i ON i.id=k.identity_id AND i.workspace_id=k.workspace_id
		WHERE k.id=$1 AND k.status='active' AND i.status='active'
		  AND (k.expires_at IS NULL OR k.expires_at > clock_timestamp())
		FOR UPDATE`, keyID).Scan(&workspaceID, &identityID, &keyPrefix, &storedHash, &keyStatus, &expiresAt, &identitySubject, &identityKind, &displayName, &identityStatus); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.Principal{}, ErrUnauthenticated
		}
		return contracts.Principal{}, fmt.Errorf("read api key: %w", err)
	}
	digest := sha256.Sum256([]byte(secret))
	if len(storedHash) != len(digest) || subtle.ConstantTimeCompare(storedHash, digest[:]) != 1 {
		return contracts.Principal{}, ErrUnauthenticated
	}
	permissions := make([]contracts.Permission, 0, 8)
	rows, err := tx.Query(ctx, `
		SELECT r.permissions
		FROM fornix.identity_role_bindings b
		JOIN fornix.roles r ON r.id=b.role_id AND r.workspace_id=b.workspace_id
		WHERE b.workspace_id=$1 AND b.identity_id=$2
		  AND (b.expires_at IS NULL OR b.expires_at > clock_timestamp())
		ORDER BY r.id`, workspaceID, identityID)
	if err != nil {
		return contracts.Principal{}, fmt.Errorf("read role permissions: %w", err)
	}
	for rows.Next() {
		var raw []byte
		if err := rows.Scan(&raw); err != nil {
			rows.Close()
			return contracts.Principal{}, fmt.Errorf("scan role permissions: %w", err)
		}
		var rolePermissions []contracts.Permission
		if err := json.Unmarshal(raw, &rolePermissions); err != nil {
			rows.Close()
			return contracts.Principal{}, fmt.Errorf("decode role permissions: %w", err)
		}
		permissions = append(permissions, rolePermissions...)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return contracts.Principal{}, fmt.Errorf("read role permissions: %w", err)
	}
	rows.Close()
	permissions, err = contracts.NormalizePermissions(permissions)
	if err != nil {
		return contracts.Principal{}, fmt.Errorf("normalize role permissions: %w", err)
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.api_keys SET last_used_at=clock_timestamp() WHERE id=$1`, keyID); err != nil {
		return contracts.Principal{}, fmt.Errorf("record api key use: %w", err)
	}
	principal := contracts.Principal{ID: identityID, WorkspaceID: workspaceID, Subject: identitySubject, Kind: identityKind, DisplayName: displayName, APIKeyID: keyID, Permissions: permissions, Authenticated: true}
	if err := tx.Commit(ctx); err != nil {
		return contracts.Principal{}, fmt.Errorf("commit authentication: %w", err)
	}
	_ = keyStatus
	_ = identityStatus
	return principal, nil
}

// Authorize deterministically evaluates one capability and records the decision
// in the workspace audit trail. A denied decision is returned with
// ErrAuthorizationDenied so callers cannot accidentally continue.
func (s *AuthStore) Authorize(ctx context.Context, principal contracts.Principal, requestID string, permission contracts.Permission, resource, method, path string) (contracts.AuthorizationDecision, error) {
	if s == nil || s.pool == nil {
		return contracts.AuthorizationDecision{}, fmt.Errorf("authorization store is not configured")
	}
	if !principal.Authenticated {
		return contracts.AuthorizationDecision{}, ErrUnauthenticated
	}
	principal, err := principal.Normalize()
	if err != nil {
		return contracts.AuthorizationDecision{}, err
	}
	requestID = strings.TrimSpace(requestID)
	permission = contracts.Permission(strings.ToLower(strings.TrimSpace(string(permission))))
	if requestID == "" || permission == "" {
		return contracts.AuthorizationDecision{}, fmt.Errorf("request_id and permission are required")
	}
	allowed := principal.Has(permission)
	reason := "permission_granted"
	if !allowed {
		reason = "permission_denied"
	}
	decision := contracts.AuthorizationDecision{SchemaVersion: contracts.IdentitySchemaVersion, RequestID: requestID, WorkspaceID: principal.WorkspaceID, Actor: contracts.AuditActor{ID: principal.ID, WorkspaceID: principal.WorkspaceID, Kind: principal.Kind, APIKeyID: principal.APIKeyID}, Permission: permission, Resource: strings.TrimSpace(resource), Allowed: allowed, Reason: reason, DecidedAt: time.Now().UTC()}
	decision.DecisionHash = decision.Hash()
	actorJSON, _ := json.Marshal(decision.Actor)
	_, err = s.pool.Exec(ctx, `
		INSERT INTO fornix.authorization_audit(workspace_id,request_id,identity_id,api_key_id,actor,permission,resource,decision,reason,method,path,decision_hash)
		VALUES($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10,$11,$12)
		ON CONFLICT (workspace_id,request_id,identity_id,api_key_id,permission,resource) DO NOTHING`, decision.WorkspaceID, decision.RequestID, principal.ID, principal.APIKeyID, actorJSON, decision.Permission, decision.Resource, decision.Allowed, decision.Reason, strings.TrimSpace(method), strings.TrimSpace(path), decision.DecisionHash)
	if err != nil {
		return contracts.AuthorizationDecision{}, fmt.Errorf("write authorization audit: %w", err)
	}
	var existing contracts.AuthorizationDecision
	var existingActor []byte
	err = s.pool.QueryRow(ctx, `
		SELECT request_id,workspace_id,actor,permission,resource,decision,reason,decision_hash,created_at
		FROM fornix.authorization_audit
		WHERE workspace_id=$1 AND request_id=$2 AND identity_id=$3 AND api_key_id=$4 AND permission=$5 AND resource=$6`, decision.WorkspaceID, decision.RequestID, principal.ID, principal.APIKeyID, decision.Permission, decision.Resource).Scan(&existing.RequestID, &existing.WorkspaceID, &existingActor, &existing.Permission, &existing.Resource, &existing.Allowed, &existing.Reason, &existing.DecisionHash, &existing.DecidedAt)
	if err != nil {
		return contracts.AuthorizationDecision{}, fmt.Errorf("read authorization audit: %w", err)
	}
	existing.SchemaVersion = contracts.IdentitySchemaVersion
	if err := json.Unmarshal(existingActor, &existing.Actor); err != nil {
		return contracts.AuthorizationDecision{}, fmt.Errorf("decode authorization actor: %w", err)
	}
	if !existing.Allowed {
		return existing, ErrAuthorizationDenied
	}
	return existing, nil
}

// CreateCredentialRef stores a workspace-scoped indirection to a provider
// secret. The referenced secret material remains outside Fornix's durable
// records.
func (s *AuthStore) CreateCredentialRef(ctx context.Context, input contracts.CredentialRefInput) (contracts.CredentialRef, error) {
	workspaceID, provider, name, reference := strings.TrimSpace(input.WorkspaceID), strings.TrimSpace(input.Provider), strings.TrimSpace(input.Name), strings.TrimSpace(input.Reference)
	if workspaceID == "" || provider == "" || name == "" || reference == "" {
		return contracts.CredentialRef{}, fmt.Errorf("workspace_id, provider, name, and reference are required")
	}
	ref := contracts.CredentialRef{SchemaVersion: contracts.IdentitySchemaVersion, ID: contracts.NewID("credref"), WorkspaceID: workspaceID, Provider: provider, Name: name, Reference: reference, Version: 1, Status: contracts.CredentialActive}
	if err := s.pool.QueryRow(ctx, `
		INSERT INTO fornix.credential_references(id,workspace_id,provider,name,reference,version,status,expires_at)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8)
		RETURNING created_at,updated_at`, ref.ID, ref.WorkspaceID, ref.Provider, ref.Name, ref.Reference, ref.Version, ref.Status, input.ExpiresAt).Scan(&ref.CreatedAt, &ref.UpdatedAt); err != nil {
		return contracts.CredentialRef{}, fmt.Errorf("insert credential reference: %w", err)
	}
	ref.ExpiresAt = input.ExpiresAt
	return ref, nil
}

// RotateCredentialRef supersedes a credential reference and advances its
// version without overwriting the prior audit history.
func (s *AuthStore) RotateCredentialRef(ctx context.Context, workspaceID, id, reference string, expiresAt *time.Time) (contracts.CredentialRef, error) {
	workspaceID, id, reference = strings.TrimSpace(workspaceID), strings.TrimSpace(id), strings.TrimSpace(reference)
	if workspaceID == "" || id == "" || reference == "" {
		return contracts.CredentialRef{}, fmt.Errorf("workspace_id, id, and reference are required")
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return contracts.CredentialRef{}, fmt.Errorf("begin credential rotation: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var provider, name, status string
	var version int
	if err := tx.QueryRow(ctx, `SELECT provider,name,status,version FROM fornix.credential_references WHERE workspace_id=$1 AND id=$2 FOR UPDATE`, workspaceID, id).Scan(&provider, &name, &status, &version); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.CredentialRef{}, ErrCredentialNotFound
		}
		return contracts.CredentialRef{}, fmt.Errorf("read credential reference: %w", err)
	}
	if status != contracts.CredentialActive {
		return contracts.CredentialRef{}, ErrCredentialNotFound
	}
	if _, err := tx.Exec(ctx, `UPDATE fornix.credential_references SET status='rotated',updated_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2`, workspaceID, id); err != nil {
		return contracts.CredentialRef{}, fmt.Errorf("rotate old credential reference: %w", err)
	}
	ref := contracts.CredentialRef{SchemaVersion: contracts.IdentitySchemaVersion, ID: contracts.NewID("credref"), WorkspaceID: workspaceID, Provider: provider, Name: name, Reference: reference, Version: version + 1, Status: contracts.CredentialActive, RotatedFrom: id, ExpiresAt: expiresAt}
	if err := tx.QueryRow(ctx, `
		INSERT INTO fornix.credential_references(id,workspace_id,provider,name,reference,version,status,expires_at,rotated_from)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
		RETURNING created_at,updated_at`, ref.ID, ref.WorkspaceID, ref.Provider, ref.Name, ref.Reference, ref.Version, ref.Status, ref.ExpiresAt, ref.RotatedFrom).Scan(&ref.CreatedAt, &ref.UpdatedAt); err != nil {
		return contracts.CredentialRef{}, fmt.Errorf("insert rotated credential reference: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.CredentialRef{}, fmt.Errorf("commit credential rotation: %w", err)
	}
	return ref, nil
}

// CredentialRefForUse returns the newest active, unexpired reference for a
// workspace/provider/name tuple.
func (s *AuthStore) CredentialRefForUse(ctx context.Context, workspaceID, provider, name string) (contracts.CredentialRef, error) {
	var ref contracts.CredentialRef
	err := s.pool.QueryRow(ctx, `
		SELECT id,workspace_id,provider,name,reference,version,status,COALESCE(rotated_from,''),expires_at,revoked_at,created_at,updated_at
		FROM fornix.credential_references
		WHERE workspace_id=$1 AND provider=$2 AND name=$3 AND status='active'
		  AND (expires_at IS NULL OR expires_at > clock_timestamp())
		ORDER BY version DESC LIMIT 1`, strings.TrimSpace(workspaceID), strings.TrimSpace(provider), strings.TrimSpace(name)).Scan(&ref.ID, &ref.WorkspaceID, &ref.Provider, &ref.Name, &ref.Reference, &ref.Version, &ref.Status, &ref.RotatedFrom, &ref.ExpiresAt, &ref.RevokedAt, &ref.CreatedAt, &ref.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.CredentialRef{}, ErrCredentialNotFound
	}
	if err != nil {
		return contracts.CredentialRef{}, fmt.Errorf("read credential reference: %w", err)
	}
	return ref, nil
}

// RevokeCredentialRef marks an active credential reference unusable while
// retaining its metadata for audit and rotation lineage.
func (s *AuthStore) RevokeCredentialRef(ctx context.Context, workspaceID, id string) error {
	result, err := s.pool.Exec(ctx, `UPDATE fornix.credential_references SET status='revoked',revoked_at=clock_timestamp(),updated_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2 AND status='active'`, strings.TrimSpace(workspaceID), strings.TrimSpace(id))
	if err != nil {
		return fmt.Errorf("revoke credential reference: %w", err)
	}
	if result.RowsAffected() == 0 {
		return ErrCredentialNotFound
	}
	return nil
}

// GetIdentity reads one identity only when it belongs to workspaceID.
func (s *AuthStore) GetIdentity(ctx context.Context, workspaceID, id string) (contracts.Identity, error) {
	var identity contracts.Identity
	err := s.pool.QueryRow(ctx, `SELECT 1,id,workspace_id,subject,kind,display_name,status,created_at,updated_at FROM fornix.identities WHERE workspace_id=$1 AND id=$2`, strings.TrimSpace(workspaceID), strings.TrimSpace(id)).Scan(&identity.SchemaVersion, &identity.ID, &identity.WorkspaceID, &identity.Subject, &identity.Kind, &identity.DisplayName, &identity.Status, &identity.CreatedAt, &identity.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return contracts.Identity{}, ErrIdentityNotFound
	}
	return identity, err
}

// ListIdentities returns a bounded, ID-ordered page of workspace identities.
func (s *AuthStore) ListIdentities(ctx context.Context, workspaceID string, limit int, cursor string) (IdentityPage, error) {
	limit = boundedOperatorLimit(limit)
	rows, err := s.pool.Query(ctx, `SELECT 1,id,workspace_id,subject,kind,display_name,status,created_at,updated_at FROM fornix.identities WHERE workspace_id=$1 AND id>$2 ORDER BY id LIMIT $3`, strings.TrimSpace(workspaceID), strings.TrimSpace(cursor), limit+1)
	if err != nil {
		return IdentityPage{}, err
	}
	defer rows.Close()
	page := IdentityPage{Items: make([]contracts.Identity, 0, limit)}
	for rows.Next() {
		var identity contracts.Identity
		if err := rows.Scan(&identity.SchemaVersion, &identity.ID, &identity.WorkspaceID, &identity.Subject, &identity.Kind, &identity.DisplayName, &identity.Status, &identity.CreatedAt, &identity.UpdatedAt); err != nil {
			return IdentityPage{}, err
		}
		page.Items = append(page.Items, identity)
	}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, rows.Err()
}

// DisableIdentity prevents an identity and its authentication path from being
// used for subsequent requests without deleting its audit history.
func (s *AuthStore) DisableIdentity(ctx context.Context, workspaceID, id string) error {
	result, err := s.pool.Exec(ctx, `UPDATE fornix.identities SET status='disabled',updated_at=clock_timestamp() WHERE workspace_id=$1 AND id=$2 AND status='active'`, strings.TrimSpace(workspaceID), strings.TrimSpace(id))
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return ErrIdentityNotFound
	}
	return nil
}

// ListRoles returns a bounded, ID-ordered page of roles in one workspace.
func (s *AuthStore) ListRoles(ctx context.Context, workspaceID string, limit int, cursor string) (RolePage, error) {
	limit = boundedOperatorLimit(limit)
	rows, err := s.pool.Query(ctx, `SELECT 1,id,workspace_id,name,permissions,created_at,updated_at FROM fornix.roles WHERE workspace_id=$1 AND id>$2 ORDER BY id LIMIT $3`, strings.TrimSpace(workspaceID), strings.TrimSpace(cursor), limit+1)
	if err != nil {
		return RolePage{}, err
	}
	defer rows.Close()
	page := RolePage{Items: make([]contracts.Role, 0, limit)}
	for rows.Next() {
		var role contracts.Role
		var raw []byte
		if err := rows.Scan(&role.SchemaVersion, &role.ID, &role.WorkspaceID, &role.Name, &raw, &role.CreatedAt, &role.UpdatedAt); err != nil {
			return RolePage{}, err
		}
		if err := json.Unmarshal(raw, &role.Permissions); err != nil {
			return RolePage{}, err
		}
		page.Items = append(page.Items, role)
	}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, rows.Err()
}

// UnbindRole removes one workspace-scoped identity/role binding while leaving
// the identity and role records available for audit.
func (s *AuthStore) UnbindRole(ctx context.Context, workspaceID, identityID, roleID string) error {
	result, err := s.pool.Exec(ctx, `DELETE FROM fornix.identity_role_bindings WHERE workspace_id=$1 AND identity_id=$2 AND role_id=$3`, strings.TrimSpace(workspaceID), strings.TrimSpace(identityID), strings.TrimSpace(roleID))
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return ErrIdentityNotFound
	}
	return nil
}

// ListAPIKeys returns bounded key metadata without returning token hashes or
// bearer secrets.
func (s *AuthStore) ListAPIKeys(ctx context.Context, workspaceID string, limit int, cursor string) (APIKeyPage, error) {
	limit = boundedOperatorLimit(limit)
	rows, err := s.pool.Query(ctx, `SELECT 1,id,workspace_id,identity_id,prefix,status,expires_at,revoked_at,created_at,last_used_at FROM fornix.api_keys WHERE workspace_id=$1 AND id>$2 ORDER BY id LIMIT $3`, strings.TrimSpace(workspaceID), strings.TrimSpace(cursor), limit+1)
	if err != nil {
		return APIKeyPage{}, err
	}
	defer rows.Close()
	page := APIKeyPage{Items: make([]contracts.APIKey, 0, limit)}
	for rows.Next() {
		var key contracts.APIKey
		if err := rows.Scan(&key.SchemaVersion, &key.ID, &key.WorkspaceID, &key.IdentityID, &key.Prefix, &key.Status, &key.ExpiresAt, &key.RevokedAt, &key.CreatedAt, &key.LastUsedAt); err != nil {
			return APIKeyPage{}, err
		}
		page.Items = append(page.Items, key)
	}
	if len(page.Items) > limit {
		page.NextCursor = page.Items[limit-1].ID
		page.Items = page.Items[:limit]
	}
	return page, rows.Err()
}

func isUniqueViolation(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "duplicate key value violates unique constraint")
}
