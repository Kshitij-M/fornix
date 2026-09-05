package store_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/contracts"
	policyruntime "github.com/omaveda/fornix/internal/policy"
	"github.com/omaveda/fornix/internal/store"
)

type policyIntegration struct {
	pool      *pgxpool.Pool
	store     *store.PolicyStore
	workspace string
	actor     contracts.ActorRef
}

func newPolicyIntegration(t *testing.T) *policyIntegration {
	t.Helper()
	dsn := os.Getenv("FORNIX_TEST_PG_DSN")
	if dsn == "" {
		t.Skip("FORNIX_TEST_PG_DSN is not set")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	if err := store.ApplyMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	workspace := fmt.Sprintf("test-policy-%d", time.Now().UnixNano())
	resolver := &policyruntime.Resolver{Lookup: func(ref contracts.ValidatorRef) bool {
		return ref.Version == "1" && len(ref.ID) > 0 && ref.ID != "unknown.validator"
	}}
	result := &policyIntegration{pool: pool, store: store.NewPolicyStore(pool, store.NewEventStore(pool), resolver), workspace: workspace, actor: contracts.ActorRef{ID: "policy-test", Kind: "test", WorkspaceID: workspace}}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		for _, query := range []string{
			`DELETE FROM fornix.validation_policy_defaults WHERE workspace_id=$1`,
			`DELETE FROM fornix.validation_policy_idempotency WHERE workspace_id=$1`,
			`DELETE FROM fornix.validation_policy_audit WHERE workspace_id=$1`,
			`DELETE FROM fornix.validation_policy_transitions WHERE workspace_id=$1`,
			`DELETE FROM fornix.validation_policy_rules WHERE workspace_id=$1`,
			`DELETE FROM fornix.validation_policy_versions WHERE workspace_id=$1`,
			`DELETE FROM fornix.validation_policies WHERE workspace_id=$1`,
			`DELETE FROM fornix.control_events WHERE workspace_id=$1`,
			`DELETE FROM fornix.evidence_records WHERE workspace_id=$1`,
		} {
			_, _ = pool.Exec(cleanupCtx, query, workspace)
		}
		pool.Close()
	})
	return result
}

func policyPack(workspace, version string) contracts.ValidationPolicyPack {
	return contracts.ValidationPolicyPack{
		WorkspaceID: workspace, PolicyID: "repository-safety", Version: version,
		Rules:    []contracts.ValidationPolicyRule{{Validator: contracts.ValidatorRef{ID: contracts.ValidationValidatorFiles, Version: "1"}, Required: true}},
		Approval: contracts.PolicyApprovalConfig{Mode: contracts.PolicyApprovalRequired},
		Budget:   contracts.PolicyBudget{Change: contracts.ChangeBudgets{MaxOperations: 2, MaxFileBytes: 1024, MaxTotalBytes: 2048}, Validation: contracts.ValidationBudget{MaxValidators: 16, MaxFiles: 100, MaxBytes: 1 << 20, MaxOutputBytes: 1 << 20, MaxWallTimeMS: 30000, MaxSQLQueries: 100, MaxRetries: 1, MaxReportBytes: 1 << 16}},
	}
}

func createPolicy(t *testing.T, integration *policyIntegration, version string) contracts.ValidationPolicyVersion {
	t.Helper()
	value, created, err := integration.store.Create(context.Background(), contracts.PolicyCreateRequest{
		WorkspaceID: integration.workspace, RequestID: "create-" + version, IdempotencyKey: "create-" + version,
		Actor: integration.actor, Pack: policyPack(integration.workspace, version),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !created || value.Status != contracts.PolicyDraft || value.PolicyHash == "" {
		t.Fatalf("policy create result = created %v value %+v", created, value)
	}
	return value
}

func TestPolicyLifecycleIsIdempotentAuditableAndWorkspaceScoped(t *testing.T) {
	integration := newPolicyIntegration(t)
	created := createPolicy(t, integration, "1")
	duplicate, duplicateCreated, err := integration.store.Create(context.Background(), contracts.PolicyCreateRequest{
		WorkspaceID: integration.workspace, RequestID: "different-request", IdempotencyKey: "create-1",
		Actor: integration.actor, Pack: policyPack(integration.workspace, "1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if duplicateCreated || duplicate.PolicyHash != created.PolicyHash {
		t.Fatalf("duplicate create was not a no-op: created=%v duplicate=%+v", duplicateCreated, duplicate)
	}
	activated, deduped, err := integration.store.Activate(context.Background(), contracts.PolicyLifecycleRequest{WorkspaceID: integration.workspace, Policy: created.Pack.Ref(), Actor: integration.actor, RequestID: "activate-1", IdempotencyKey: "activate-1"})
	if err != nil {
		t.Fatal(err)
	}
	if deduped || activated.Status != contracts.PolicyActive {
		t.Fatalf("activation result = deduped %v value %+v", deduped, activated)
	}
	duplicateActivation, duplicateDeduped, err := integration.store.Activate(context.Background(), contracts.PolicyLifecycleRequest{WorkspaceID: integration.workspace, Policy: created.Pack.Ref(), Actor: integration.actor, RequestID: "different-request", IdempotencyKey: "activate-1"})
	if err != nil || !duplicateDeduped || duplicateActivation.PolicyHash != activated.PolicyHash {
		t.Fatalf("duplicate activation was not reported as a no-op: deduped=%v value=%+v err=%v", duplicateDeduped, duplicateActivation, err)
	}
	if _, _, err := integration.store.SetDefault(context.Background(), contracts.PolicyLifecycleRequest{WorkspaceID: integration.workspace, Policy: activated.Pack.Ref(), Actor: integration.actor, RequestID: "default-1", IdempotencyKey: "default-1"}); err != nil {
		t.Fatal(err)
	}
	resolution, err := integration.store.Resolve(context.Background(), contracts.PolicyEvaluationRequest{WorkspaceID: integration.workspace})
	if err != nil {
		t.Fatal(err)
	}
	if !resolution.Selected || resolution.Ref == nil || resolution.Ref.PolicyHash != created.PolicyHash {
		t.Fatalf("default resolution did not pin policy: %+v", resolution)
	}
	var auditsBefore, eventsBefore int
	if err := integration.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.validation_policy_audit WHERE workspace_id=$1`, integration.workspace).Scan(&auditsBefore); err != nil {
		t.Fatal(err)
	}
	if err := integration.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1`, integration.workspace).Scan(&eventsBefore); err != nil {
		t.Fatal(err)
	}
	dryRun, err := integration.store.DryRunResolve(context.Background(), contracts.PolicyEvaluationRequest{WorkspaceID: integration.workspace})
	if err != nil {
		t.Fatal(err)
	}
	if dryRun.ResolutionHash != resolution.ResolutionHash || dryRun.Ref == nil || dryRun.Ref.PolicyHash != resolution.Ref.PolicyHash {
		t.Fatalf("dry-run resolution differs from durable resolution: dry=%+v durable=%+v", dryRun, resolution)
	}
	var auditsAfter, eventsAfter int
	if err := integration.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.validation_policy_audit WHERE workspace_id=$1`, integration.workspace).Scan(&auditsAfter); err != nil {
		t.Fatal(err)
	}
	if err := integration.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1`, integration.workspace).Scan(&eventsAfter); err != nil {
		t.Fatal(err)
	}
	if auditsBefore != auditsAfter || eventsBefore != eventsAfter {
		t.Fatalf("dry-run mutated durable history: audits %d -> %d, events %d -> %d", auditsBefore, auditsAfter, eventsBefore, eventsAfter)
	}
	if _, deduped, err := integration.store.Retire(context.Background(), contracts.PolicyLifecycleRequest{
		WorkspaceID: integration.workspace, Policy: created.Pack.Ref(), Actor: integration.actor,
		RequestID: "retire-1", IdempotencyKey: "retire-1", Reason: "policy lifecycle test",
	}); err != nil || deduped {
		t.Fatalf("policy retirement = deduped %v error %v", deduped, err)
	}
	retiredRef := created.Pack.Ref()
	if _, err := integration.store.Resolve(context.Background(), contracts.PolicyEvaluationRequest{
		WorkspaceID: integration.workspace, Policy: &retiredRef,
	}); !errors.Is(err, store.ErrPolicyRetired) {
		t.Fatalf("retired policy resolution error = %v", err)
	}
	if _, err := integration.store.Default(context.Background(), integration.workspace); !errors.Is(err, store.ErrPolicyDefaultMissing) {
		t.Fatalf("retired default lookup error = %v", err)
	}
	audit, err := integration.store.Audit(context.Background(), integration.workspace, "repository-safety", "", 100, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(audit.Items) < 3 {
		t.Fatalf("audit history is incomplete: %+v", audit)
	}
	if _, err := integration.store.Get(context.Background(), "other-workspace", created.PolicyID, created.Version); !errors.Is(err, store.ErrPolicyNotFound) {
		t.Fatalf("cross-workspace policy read error = %v", err)
	}
	if _, err := integration.store.Resolve(context.Background(), contracts.PolicyEvaluationRequest{WorkspaceID: "other-workspace", Policy: &contracts.ValidationPolicyRef{WorkspaceID: integration.workspace, PolicyID: created.PolicyID, Version: created.Version}}); err == nil {
		t.Fatal("cross-workspace policy resolution unexpectedly succeeded")
	}
}

func TestPolicyConcurrentCreateAndTransitionHaveOneDurableEffect(t *testing.T) {
	integration := newPolicyIntegration(t)
	const workers = 8
	results := make(chan contracts.ValidationPolicyVersion, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			value, _, err := integration.store.Create(context.Background(), contracts.PolicyCreateRequest{WorkspaceID: integration.workspace, RequestID: "concurrent-create", IdempotencyKey: "concurrent-create", Actor: integration.actor, Pack: policyPack(integration.workspace, "1")})
			if err != nil {
				errs <- err
				return
			}
			results <- value
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var first contracts.ValidationPolicyVersion
	for value := range results {
		if first.PolicyHash == "" {
			first = value
		}
		if value.PolicyHash != first.PolicyHash {
			t.Fatalf("concurrent create changed hash: %s != %s", value.PolicyHash, first.PolicyHash)
		}
	}
	var count int
	if err := integration.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.validation_policy_versions WHERE workspace_id=$1`, integration.workspace).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("concurrent create inserted %d versions", count)
	}
}

func TestPolicyConcurrentActivationLeavesOneActiveVersion(t *testing.T) {
	integration := newPolicyIntegration(t)
	first := createPolicy(t, integration, "1")
	second := createPolicy(t, integration, "2")
	refs := []contracts.ValidationPolicyRef{first.Pack.Ref(), second.Pack.Ref()}
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _, err := integration.store.Activate(context.Background(), contracts.PolicyLifecycleRequest{
				WorkspaceID:    integration.workspace,
				Policy:         refs[worker%len(refs)],
				RequestID:      fmt.Sprintf("concurrent-activate-%d", worker),
				IdempotencyKey: fmt.Sprintf("concurrent-activate-%d", worker),
				Actor:          integration.actor,
			})
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var active int
	if err := integration.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.validation_policy_versions WHERE workspace_id=$1 AND policy_id=$2 AND status='active'`, integration.workspace, first.PolicyID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 1 {
		t.Fatalf("concurrent activation left %d active versions", active)
	}
}

func TestPolicyCreateCrashRollsBackBodyHistoryAndEvent(t *testing.T) {
	integration := newPolicyIntegration(t)
	integration.store.SetFailureHook(func(stage string) error {
		if stage == "policy_created" {
			return errors.New("simulated policy create crash")
		}
		return nil
	})
	_, _, err := integration.store.Create(context.Background(), contracts.PolicyCreateRequest{
		WorkspaceID: integration.workspace, RequestID: "policy-crash", IdempotencyKey: "policy-crash",
		Actor: integration.actor, Pack: policyPack(integration.workspace, "1"),
	})
	if err == nil {
		t.Fatal("policy create crash unexpectedly succeeded")
	}
	integration.store.SetFailureHook(nil)
	for name, query := range map[string]string{
		"version":  `SELECT count(*) FROM fornix.validation_policy_versions WHERE workspace_id=$1`,
		"audit":    `SELECT count(*) FROM fornix.validation_policy_audit WHERE workspace_id=$1`,
		"event":    `SELECT count(*) FROM fornix.control_events WHERE workspace_id=$1 AND event_type='policy.created'`,
		"evidence": `SELECT count(*) FROM fornix.evidence_records WHERE workspace_id=$1 AND kind='event'`,
	} {
		var count int
		if err := integration.pool.QueryRow(context.Background(), query, integration.workspace).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 0 {
			t.Fatalf("policy create crash left %s rows: %d", name, count)
		}
	}
}

func TestPolicyVersionBodyIsImmutableAtDatabaseBoundary(t *testing.T) {
	integration := newPolicyIntegration(t)
	created := createPolicy(t, integration, "1")
	_, err := integration.pool.Exec(context.Background(), `UPDATE fornix.validation_policy_versions SET pack=jsonb_set(pack,'{policy_id}','"changed"') WHERE workspace_id=$1 AND policy_id=$2 AND version=$3`, integration.workspace, created.PolicyID, created.Version)
	if err == nil {
		t.Fatal("direct policy body mutation unexpectedly succeeded")
	}
	if _, err := integration.store.Get(context.Background(), integration.workspace, created.PolicyID, created.Version); err != nil {
		t.Fatal(err)
	}
}

func TestPolicyResolutionRejectsCrossWorkspaceAndHashMismatch(t *testing.T) {
	integration := newPolicyIntegration(t)
	created := createPolicy(t, integration, "1")
	if _, _, err := integration.store.Activate(context.Background(), contracts.PolicyLifecycleRequest{
		WorkspaceID: integration.workspace, Policy: created.Pack.Ref(), RequestID: "resolve-activate",
		IdempotencyKey: "resolve-activate", Actor: integration.actor,
	}); err != nil {
		t.Fatal(err)
	}

	wrongHash := created.Pack.Ref()
	wrongHash.PolicyHash = fmt.Sprintf("%064d", 7)
	if _, err := integration.store.Resolve(context.Background(), contracts.PolicyEvaluationRequest{
		WorkspaceID: integration.workspace, Policy: &wrongHash,
	}); !errors.Is(err, store.ErrPolicyConflict) {
		t.Fatalf("wrong policy hash error = %v", err)
	}

	foreign := created.Pack.Ref()
	foreign.WorkspaceID = "foreign-workspace"
	if _, err := integration.store.Resolve(context.Background(), contracts.PolicyEvaluationRequest{
		WorkspaceID: integration.workspace, Policy: &foreign,
	}); err == nil {
		t.Fatal("cross-workspace policy resolution unexpectedly succeeded")
	}
}
