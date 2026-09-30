package qualification

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestPostgresEffectAuthorityQualificationRequiresExplicitOptIn(t *testing.T) {
	if _, err := RunPostgresEffectAuthorityProbe(context.Background(), PostgresEffectAuthorityProbeOptions{WorkspaceID: "qualification-workspace", RunID: "qualification-run"}); err == nil {
		t.Fatal("probe without an explicit DSN unexpectedly succeeded")
	}
}

// TestPostgresEffectAuthorityQualification is opt-in because it requires a
// disposable PostgreSQL database. The Make/script entry point additionally
// requires FORNIX_DISPOSABLE_DATABASE_CONFIRM=1; the default test suite never
// contacts a database through this probe.
func TestPostgresEffectAuthorityQualification(t *testing.T) {
	if os.Getenv("FORNIX_RUN_EFFECT_AUTHORITY_PROBE") != "1" || os.Getenv("FORNIX_DISPOSABLE_DATABASE_CONFIRM") != "1" {
		t.Skip("set FORNIX_RUN_EFFECT_AUTHORITY_PROBE=1 and FORNIX_DISPOSABLE_DATABASE_CONFIRM=1 to run against disposable PostgreSQL")
	}
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	workspace := fmt.Sprintf("qualification-effect-%d", time.Now().UnixNano())
	observation, err := RunPostgresEffectAuthorityProbe(context.Background(), PostgresEffectAuthorityProbeOptions{
		DSN: dsn, WorkspaceID: workspace, RunID: "effect-authority-run", Timeout: 45 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := observation.Normalize(); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(observation)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > 16<<10 {
		t.Fatalf("observation exceeded bound: %d", len(encoded))
	}
	if observation.StableHash() == "" || observation.InvocationCount != 1 || !observation.SuccessReconciled || !observation.ReceiptLinkVerified {
		t.Fatalf("incomplete authority observation: %+v", observation)
	}
}
