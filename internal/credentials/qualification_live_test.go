package credentials

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

// TestLiveAuthorityQualification is intentionally skipped unless an operator
// supplies an explicit deployment endpoint and metadata through the process
// environment. It never accepts a token value as a test argument and never
// prints the token or the resolved secret.
func TestLiveAuthorityQualification(t *testing.T) {
	endpoint := strings.TrimSpace(os.Getenv("FORNIX_LIVE_AUTHORITY_URL"))
	if endpoint == "" {
		t.Skip("FORNIX_LIVE_AUTHORITY_URL is not set")
	}
	workspaceID := strings.TrimSpace(os.Getenv("FORNIX_LIVE_AUTHORITY_WORKSPACE"))
	provider := strings.TrimSpace(os.Getenv("FORNIX_LIVE_AUTHORITY_PROVIDER"))
	referenceText := strings.TrimSpace(os.Getenv("FORNIX_LIVE_AUTHORITY_REFERENCE"))
	purpose := strings.TrimSpace(os.Getenv("FORNIX_LIVE_AUTHORITY_PURPOSE"))
	if workspaceID == "" || provider == "" || referenceText == "" || purpose == "" {
		t.Fatal("FORNIX_LIVE_AUTHORITY_WORKSPACE, _PROVIDER, _REFERENCE, and _PURPOSE are required when live authority qualification is enabled")
	}
	ref, err := ParseRef(referenceText)
	if err != nil {
		t.Fatalf("live authority reference is invalid: %v", err)
	}
	tokenEnv := strings.TrimSpace(os.Getenv("FORNIX_LIVE_AUTHORITY_TOKEN_ENV"))
	if tokenEnv == "" {
		tokenEnv = "FORNIX_CREDENTIAL_MANAGER_TOKEN"
	}
	allowPrivate, _ := strconv.ParseBool(strings.TrimSpace(os.Getenv("FORNIX_LIVE_AUTHORITY_ALLOW_PRIVATE")))
	manager, err := NewHTTPSecretManager(HTTPSecretManagerConfig{
		Endpoint:             endpoint,
		Token:                EnvTokenSource{Name: tokenEnv},
		AllowPrivateNetworks: allowPrivate,
		Timeout:              10 * time.Second,
	})
	if err != nil {
		t.Fatalf("construct live authority adapter: %v", err)
	}
	result, err := (&AuthorityProbe{Manager: manager, Timeout: 10 * time.Second}).Run(context.Background(), AuthorityProbeRequest{
		WorkspaceID: workspaceID,
		Provider:    provider,
		Reference:   ref,
		Purpose:     purpose,
		CheckToken:  false,
	})
	if err != nil {
		t.Fatalf("live authority qualification failed: %v (outcome=%s report_hash=%s)", err, result.Outcome, result.ReportHash)
	}
	t.Logf("live authority qualified: workspace=%s provider=%s outcome=%s resolve_ms=%d report_hash=%s", workspaceID, provider, result.Outcome, result.ResolveMillis, result.ReportHash)
}
