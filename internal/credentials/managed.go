package credentials

import (
	"context"
	"errors"
	"strings"
	"time"
)

var (
	ErrManagedScope       = errors.New("managed credential scope is invalid")
	ErrManagedUnavailable = errors.New("managed credential authority is unavailable")
	ErrManagedDenied      = errors.New("managed credential authority denied the request")
	ErrManagedResponse    = errors.New("managed credential authority returned an invalid response")
	ErrManagedExpired     = errors.New("managed credential authority returned an expired secret")
	ErrManagedVersion     = errors.New("managed credential authority returned an invalid version")
)

const MaxProviderNameLength = 128

// ResolveRequest is the secret-free input to a managed secret authority.
// Implementations must treat Reference and Purpose as opaque validated
// metadata, not as filesystem paths or executable input.
type ResolveRequest struct {
	WorkspaceID string
	Provider    string
	Reference   Ref
	Purpose     string
}

func (r *ResolveRequest) Normalize() error {
	if r == nil {
		return ErrManagedScope
	}
	r.WorkspaceID = strings.TrimSpace(r.WorkspaceID)
	r.Provider = strings.TrimSpace(r.Provider)
	r.Purpose = strings.TrimSpace(r.Purpose)
	parsed, err := ParseRef(r.Reference.String())
	if err != nil {
		return ErrManagedScope
	}
	r.Reference = parsed
	if r.WorkspaceID == "" || r.Provider == "" || len(r.Provider) > MaxProviderNameLength || r.Purpose == "" || len(r.Purpose) > MaxPurposeLength {
		return ErrManagedScope
	}
	parts := strings.SplitN(r.Purpose, ":", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[1]) != r.Provider {
		return ErrManagedScope
	}
	return nil
}

// SecretManager is the provider-neutral external authority seam. The
// implementation may be backed by Vault, OpenBao, a cloud secret manager, or
// a KMS service. It must return secret bytes only in memory.
type SecretManager interface {
	Resolve(context.Context, ResolveRequest) (ResolvedSecret, error)
}

// SecretManagerFunc adapts a function for tests and deployment adapters.
type SecretManagerFunc func(context.Context, ResolveRequest) (ResolvedSecret, error)

func (f SecretManagerFunc) Resolve(ctx context.Context, request ResolveRequest) (ResolvedSecret, error) {
	if f == nil {
		return ResolvedSecret{}, ErrManagedUnavailable
	}
	return f(ctx, request)
}

// ManagedSecretResolver binds an external manager to the workspace-scoped
// SecretResolver contract. It validates the returned envelope before any
// caller can use the secret and never places the source response in an error.
type ManagedSecretResolver struct {
	Manager SecretManager
	Now     func() time.Time
}

func NewManagedSecretResolver(manager SecretManager) *ManagedSecretResolver {
	return &ManagedSecretResolver{Manager: manager, Now: func() time.Time { return time.Now().UTC() }}
}

func (r *ManagedSecretResolver) Resolve(ctx context.Context, workspaceID string, ref Ref, purpose string) (Secret, error) {
	resolved, err := r.ResolveVersioned(ctx, workspaceID, ref, purpose)
	if err != nil {
		return Secret{}, err
	}
	return resolved.Secret, nil
}

func (r *ManagedSecretResolver) ResolveVersioned(ctx context.Context, workspaceID string, ref Ref, purpose string) (ResolvedSecret, error) {
	if r == nil || r.Manager == nil {
		return ResolvedSecret{}, ErrManagedUnavailable
	}
	provider, err := providerFromPurpose(purpose)
	if err != nil {
		return ResolvedSecret{}, ErrManagedScope
	}
	request := ResolveRequest{WorkspaceID: workspaceID, Provider: provider, Reference: ref, Purpose: purpose}
	if err := request.Normalize(); err != nil {
		return ResolvedSecret{}, err
	}
	resolved, err := r.Manager.Resolve(ctx, request)
	if err != nil {
		// A manager may return a partial secret together with an error. Do not
		// leave those bytes reachable while classifying the failure.
		resolved.Clear()
		// Do not wrap arbitrary manager errors: a misbehaving adapter must not
		// be able to place a secret or response body in durable/error output.
		return ResolvedSecret{}, classifyManagerError(err)
	}
	resolved.Version = strings.TrimSpace(resolved.Version)
	if resolved.Secret.Len() == 0 || resolved.Version == "" || len(resolved.Version) > MaxSourceVersionLength || strings.ContainsAny(resolved.Version, "\x00\r\n\t") {
		resolved.Clear()
		return ResolvedSecret{}, ErrManagedVersion
	}
	now := time.Now().UTC()
	if r.Now != nil {
		now = r.Now().UTC()
	}
	if !resolved.ExpiresAt.IsZero() && !now.Before(resolved.ExpiresAt.UTC()) {
		resolved.Clear()
		return ResolvedSecret{}, ErrManagedExpired
	}
	return resolved, nil
}

func providerFromPurpose(purpose string) (string, error) {
	parts := strings.SplitN(strings.TrimSpace(purpose), ":", 2)
	if len(parts) != 2 || strings.TrimSpace(parts[1]) == "" {
		return "", ErrManagedScope
	}
	return strings.TrimSpace(parts[1]), nil
}

func classifyManagerError(err error) error {
	if errors.Is(err, ErrManagedDenied) {
		return ErrManagedDenied
	}
	if errors.Is(err, ErrManagedResponse) || errors.Is(err, ErrManagedExpired) || errors.Is(err, ErrManagedVersion) {
		return ErrManagedResponse
	}
	return ErrManagedUnavailable
}
