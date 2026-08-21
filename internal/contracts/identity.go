package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"
)

// IdentitySchemaVersion is the wire/durable schema version for identity and
// authorization contracts.
const IdentitySchemaVersion = 1

const (
	IdentityActive   = "active"
	IdentityRevoked  = "revoked"
	IdentityDisabled = "disabled"

	APIKeyActive  = "active"
	APIKeyRevoked = "revoked"
	APIKeyExpired = "expired"

	CredentialActive  = "active"
	CredentialRevoked = "revoked"
	CredentialRotated = "rotated"
)

// Permission is a stable capability name. Authorization code must compare
// these values, not human-readable role names.
type Permission string

const (
	PermissionModelInvoke     Permission = "model:invoke"
	PermissionToolExecute     Permission = "tool:execute"
	PermissionToolApprove     Permission = "tool:approve"
	PermissionTaskRead        Permission = "task:read"
	PermissionTaskMutate      Permission = "task:mutate"
	PermissionTaskExecute     Permission = "task:execute"
	PermissionRetrievalRead   Permission = "retrieval:read"
	PermissionRetrievalWrite  Permission = "retrieval:write"
	PermissionEvidenceRead    Permission = "evidence:read"
	PermissionEvidenceWrite   Permission = "evidence:write"
	PermissionAgentRun        Permission = "agent:run"
	PermissionAgentRead       Permission = "agent:read"
	PermissionSchedulerRun    Permission = "scheduler:run"
	PermissionWorkspaceRead   Permission = "workspace:read"
	PermissionWorkspaceWrite  Permission = "workspace:write"
	PermissionIdentityAdmin   Permission = "identity:admin"
	PermissionCredentialUse   Permission = "credential:use"
	PermissionEvaluationRead  Permission = "evaluation:read"
	PermissionEvaluationRun   Permission = "evaluation:run"
	PermissionEvaluationWrite Permission = "evaluation:write"
)

// AdminWildcard is written with no whitespace on the wire. The named
// constant above remains readable in source while this value is canonical.
const AdminWildcard Permission = "admin:*"

var knownPermissions = map[Permission]struct{}{
	PermissionModelInvoke: {}, PermissionToolExecute: {}, PermissionToolApprove: {},
	PermissionTaskRead: {}, PermissionTaskMutate: {}, PermissionTaskExecute: {},
	PermissionRetrievalRead: {}, PermissionRetrievalWrite: {},
	PermissionEvidenceRead: {}, PermissionEvidenceWrite: {},
	PermissionAgentRun: {}, PermissionAgentRead: {}, PermissionSchedulerRun: {},
	PermissionWorkspaceRead: {}, PermissionWorkspaceWrite: {},
	PermissionIdentityAdmin: {}, PermissionCredentialUse: {}, PermissionEvaluationRead: {}, PermissionEvaluationRun: {}, PermissionEvaluationWrite: {}, AdminWildcard: {},
}

// Principal is the authenticated caller presented to authorization checks.
// Its workspace and permissions are derived from durable identity state, not
// from caller-supplied resource fields.
type Principal struct {
	ID            string       `json:"id"`
	WorkspaceID   string       `json:"workspace_id"`
	Subject       string       `json:"subject,omitempty"`
	Kind          string       `json:"kind,omitempty"`
	DisplayName   string       `json:"display_name,omitempty"`
	APIKeyID      string       `json:"api_key_id,omitempty"`
	Permissions   []Permission `json:"permissions,omitempty"`
	Authenticated bool         `json:"authenticated"`
	Development   bool         `json:"development,omitempty"`
}

// Normalize validates the principal identity and canonicalizes permissions.
func (p Principal) Normalize() (Principal, error) {
	p.ID = strings.TrimSpace(p.ID)
	p.WorkspaceID = strings.TrimSpace(p.WorkspaceID)
	p.Subject = strings.TrimSpace(p.Subject)
	p.Kind = strings.TrimSpace(p.Kind)
	p.DisplayName = strings.TrimSpace(p.DisplayName)
	p.APIKeyID = strings.TrimSpace(p.APIKeyID)
	if p.ID == "" || p.WorkspaceID == "" {
		return Principal{}, fmt.Errorf("principal id and workspace_id are required")
	}
	permissions, err := NormalizePermissions(p.Permissions)
	if err != nil {
		return Principal{}, err
	}
	p.Permissions = permissions
	return p, nil
}

// Actor converts the principal into the redacted actor reference propagated
// through events, task transitions, and audit records.
func (p Principal) Actor() ActorRef {
	return ActorRef{ID: p.ID, Kind: p.Kind, Name: p.DisplayName, WorkspaceID: p.WorkspaceID}
}

// Has reports whether the principal has a capability, including the explicit
// administrator wildcard.
func (p Principal) Has(permission Permission) bool {
	permission = Permission(strings.ToLower(strings.TrimSpace(string(permission))))
	for _, candidate := range p.Permissions {
		if candidate == AdminWildcard || candidate == permission {
			return true
		}
	}
	return false
}

// Identity is a workspace-scoped durable actor record. It contains no secret
// credential material.
type Identity struct {
	SchemaVersion int       `json:"schema_version"`
	ID            string    `json:"id"`
	WorkspaceID   string    `json:"workspace_id"`
	Subject       string    `json:"subject"`
	Kind          string    `json:"kind"`
	DisplayName   string    `json:"display_name,omitempty"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// Role binds a stable set of permissions to an identity within one workspace.
type Role struct {
	SchemaVersion int          `json:"schema_version"`
	ID            string       `json:"id"`
	WorkspaceID   string       `json:"workspace_id"`
	Name          string       `json:"name"`
	Permissions   []Permission `json:"permissions"`
	CreatedAt     time.Time    `json:"created_at"`
	UpdatedAt     time.Time    `json:"updated_at"`
}

// APIKey is the non-secret representation of an API credential. Token and
// TokenHash are transient fields and must never cross an output boundary.
type APIKey struct {
	SchemaVersion int        `json:"schema_version"`
	ID            string     `json:"id"`
	WorkspaceID   string     `json:"workspace_id"`
	IdentityID    string     `json:"identity_id"`
	Prefix        string     `json:"prefix"`
	Status        string     `json:"status"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
	RevokedAt     *time.Time `json:"revoked_at,omitempty"`
	RotatedFrom   string     `json:"rotated_from,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	LastUsedAt    *time.Time `json:"last_used_at,omitempty"`
	// Token and TokenHash are transient/internal values and must never be
	// serialized, logged, placed in events, or returned by an HTTP handler.
	Token     string `json:"-"`
	TokenHash string `json:"-"`
}

// CredentialRef names provider credential material without storing the secret
// itself. References are rotated and revoked as durable workspace state.
type CredentialRef struct {
	SchemaVersion int        `json:"schema_version"`
	ID            string     `json:"id"`
	WorkspaceID   string     `json:"workspace_id"`
	Provider      string     `json:"provider"`
	Name          string     `json:"name"`
	Reference     string     `json:"reference"`
	Version       int        `json:"version"`
	Status        string     `json:"status"`
	RotatedFrom   string     `json:"rotated_from,omitempty"`
	ExpiresAt     *time.Time `json:"expires_at,omitempty"`
	RevokedAt     *time.Time `json:"revoked_at,omitempty"`
	CreatedAt     time.Time  `json:"created_at"`
	UpdatedAt     time.Time  `json:"updated_at"`
}

// AuthorizationDecision is the auditable result of one deterministic
// permission check.
type AuthorizationDecision struct {
	SchemaVersion int        `json:"schema_version"`
	RequestID     string     `json:"request_id"`
	WorkspaceID   string     `json:"workspace_id"`
	Actor         AuditActor `json:"actor"`
	Permission    Permission `json:"permission"`
	Resource      string     `json:"resource,omitempty"`
	Allowed       bool       `json:"allowed"`
	Reason        string     `json:"reason"`
	DecisionHash  string     `json:"decision_hash"`
	DecidedAt     time.Time  `json:"decided_at"`
}

// AuditActor is the minimal non-secret actor identity stored with an audit
// decision.
type AuditActor struct {
	ID          string `json:"id"`
	WorkspaceID string `json:"workspace_id"`
	Kind        string `json:"kind,omitempty"`
	APIKeyID    string `json:"api_key_id,omitempty"`
}

// IdentityInput contains the bounded fields needed to create an identity.
type IdentityInput struct {
	WorkspaceID string
	Subject     string
	Kind        string
	DisplayName string
	Status      string
	Permissions []Permission
}

// APIKeyInput requests a new workspace-scoped API key for an identity.
type APIKeyInput struct {
	WorkspaceID string
	IdentityID  string
	ExpiresAt   *time.Time
}

// CredentialRefInput requests a provider credential reference without the
// credential value itself.
type CredentialRefInput struct {
	WorkspaceID string
	Provider    string
	Name        string
	Reference   string
	ExpiresAt   *time.Time
}

// NormalizePermissions validates, deduplicates, and sorts capabilities so
// authorization decisions remain deterministic.
func NormalizePermissions(values []Permission) ([]Permission, error) {
	seen := make(map[Permission]struct{}, len(values))
	out := make([]Permission, 0, len(values))
	for _, raw := range values {
		permission := Permission(strings.ToLower(strings.TrimSpace(string(raw))))
		if permission == "admin: *" {
			permission = AdminWildcard
		}
		if permission == "" {
			continue
		}
		if _, ok := knownPermissions[permission]; !ok {
			return nil, fmt.Errorf("unknown permission %q", permission)
		}
		if _, ok := seen[permission]; ok {
			continue
		}
		seen[permission] = struct{}{}
		out = append(out, permission)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// Hash returns the stable identity of the decision, excluding timestamps so a
// retry of the same authorization input compares equal.
func (d AuthorizationDecision) Hash() string {
	value := fmt.Sprintf("%d|%s|%s|%s|%s|%s|%s|%t|%s", d.SchemaVersion, d.RequestID, d.WorkspaceID, d.Actor.ID, d.Actor.Kind, d.Actor.APIKeyID, d.Permission, d.Allowed, d.Resource)
	digest := sha256.Sum256([]byte(value))
	return hex.EncodeToString(digest[:])
}

// CredentialRefFingerprint provides a non-reversible comparison value for a
// credential reference; it is not a replacement for secret storage.
func CredentialRefFingerprint(reference string) string {
	digest := sha256.Sum256([]byte(strings.TrimSpace(reference)))
	return hex.EncodeToString(digest[:])
}
