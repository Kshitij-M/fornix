package credentials

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

var ErrAuthorityQualification = errors.New("deployment authority qualification failed")

const (
	defaultAuthorityQualificationTimeout = 10 * time.Second
	maxAuthorityQualificationTimeout     = 2 * time.Minute
)

// AuthorityProbeRequest contains only validated metadata. It is safe to pass
// through a deployment qualification command; it never contains a secret or
// a token value.
type AuthorityProbeRequest struct {
	WorkspaceID string
	Provider    string
	Reference   Ref
	Purpose     string
	CheckToken  bool
}

func (r *AuthorityProbeRequest) Normalize() error {
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
	request := ResolveRequest{WorkspaceID: r.WorkspaceID, Provider: r.Provider, Reference: r.Reference, Purpose: r.Purpose}
	return request.Normalize()
}

// AuthorityProbeResult is a redacted deployment qualification result. Source
// versions and timings are operational metadata; secret and token contents are
// intentionally not representable in this type.
type AuthorityProbeResult struct {
	SchemaVersion int       `json:"schema_version"`
	WorkspaceID   string    `json:"workspace_id"`
	Provider      string    `json:"provider"`
	Reference     string    `json:"reference"`
	Purpose       string    `json:"purpose"`
	Outcome       string    `json:"outcome"`
	SourceVersion string    `json:"source_version,omitempty"`
	ExpiresAt     time.Time `json:"expires_at,omitempty"`
	SecretPresent bool      `json:"secret_present"`
	TokenPresent  bool      `json:"token_present"`
	ResolveMillis int64     `json:"resolve_ms"`
	TokenMillis   int64     `json:"token_ms,omitempty"`
	ObservedAt    time.Time `json:"observed_at"`
	ReportHash    string    `json:"report_hash"`
}

// AuthorityProbe runs one bounded provider-neutral authority check. The
// caller may inject a live SecretManager and optional TokenSource, or use the
// test authority offline. The probe does not retry: repeating an external
// authority read is a deployment decision and must not be confused with an
// exactly-once guarantee.
type AuthorityProbe struct {
	Manager SecretManager
	Token   TokenSource
	Timeout time.Duration
	Now     func() time.Time
}

func (p *AuthorityProbe) Run(ctx context.Context, request AuthorityProbeRequest) (AuthorityProbeResult, error) {
	if p == nil || p.Manager == nil {
		return AuthorityProbeResult{}, ErrManagedUnavailable
	}
	if err := request.Normalize(); err != nil {
		return AuthorityProbeResult{}, err
	}
	timeout := p.Timeout
	if timeout <= 0 {
		timeout = defaultAuthorityQualificationTimeout
	}
	if timeout > maxAuthorityQualificationTimeout {
		return AuthorityProbeResult{}, ErrAuthorityQualification
	}
	now := time.Now().UTC()
	if p.Now != nil {
		now = p.Now().UTC()
	}
	result := AuthorityProbeResult{
		SchemaVersion: contracts.DeploymentCertificateSchemaVersion,
		WorkspaceID:   request.WorkspaceID,
		Provider:      request.Provider,
		Reference:     request.Reference.String(),
		Purpose:       request.Purpose,
		ObservedAt:    now,
		Outcome:       "started",
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resolver := NewManagedSecretResolver(p.Manager)
	resolver.Now = func() time.Time {
		if p.Now != nil {
			return p.Now().UTC()
		}
		return time.Now().UTC()
	}
	started := time.Now()
	resolved, err := resolver.ResolveVersioned(callCtx, request.WorkspaceID, request.Reference, request.Purpose)
	result.ResolveMillis = elapsedMillis(started)
	if err != nil {
		result.Outcome = authorityErrorOutcome(err)
		result.ReportHash = authorityReportHash(result)
		return result, fmt.Errorf("%w: %s", ErrAuthorityQualification, result.Outcome)
	}
	result.SourceVersion = resolved.Version
	result.ExpiresAt = resolved.ExpiresAt.UTC()
	result.SecretPresent = resolved.Secret.Len() > 0
	resolved.Clear()

	if request.CheckToken {
		if p.Token == nil {
			result.Outcome = "token_source_missing"
			result.ReportHash = authorityReportHash(result)
			return result, fmt.Errorf("%w: token_source_missing", ErrAuthorityQualification)
		}
		tokenStarted := time.Now()
		token, tokenErr := p.Token.Token(callCtx)
		result.TokenMillis = elapsedMillis(tokenStarted)
		result.TokenPresent = token.Len() > 0
		token.Clear()
		if tokenErr != nil || !result.TokenPresent {
			result.Outcome = "token_denied"
			result.ReportHash = authorityReportHash(result)
			return result, fmt.Errorf("%w: token_denied", ErrAuthorityQualification)
		}
	}
	result.Outcome = "healthy"
	result.ReportHash = authorityReportHash(result)
	return result, nil
}

func authorityErrorOutcome(err error) string {
	switch {
	case errors.Is(err, ErrManagedDenied):
		return "denied"
	case errors.Is(err, ErrManagedExpired):
		return "expired"
	case errors.Is(err, ErrManagedVersion), errors.Is(err, ErrManagedResponse):
		return "invalid_response"
	case errors.Is(err, context.DeadlineExceeded):
		return "timeout"
	default:
		return "unavailable"
	}
}

func authorityReportHash(result AuthorityProbeResult) string {
	return contracts.HashStrings(
		"authority-probe",
		result.WorkspaceID,
		result.Provider,
		result.Reference,
		result.Purpose,
		result.Outcome,
		result.SourceVersion,
		result.ExpiresAt.UTC().Format(time.RFC3339Nano),
		fmt.Sprint(result.SecretPresent),
		fmt.Sprint(result.TokenPresent),
	)
}

func elapsedMillis(start time.Time) int64 {
	ms := time.Since(start).Milliseconds()
	if ms < 0 {
		return 0
	}
	return ms
}
