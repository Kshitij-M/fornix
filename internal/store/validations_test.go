package store_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/omaveda/fornix/internal/change"
	"github.com/omaveda/fornix/internal/contracts"
	"github.com/omaveda/fornix/internal/ingest"
	policyruntime "github.com/omaveda/fornix/internal/policy"
	"github.com/omaveda/fornix/internal/store"
	validationruntime "github.com/omaveda/fornix/internal/validation"
)

type validationIntegration struct {
	pool        *pgxpool.Pool
	workspace   string
	root        string
	actor       contracts.ActorRef
	changes     *change.Service
	validations *store.ValidationStore
	runtime     *validationruntime.Service
	policies    *store.PolicyStore
}

func newValidationIntegration(t *testing.T) *validationIntegration {
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
	workspace := fmt.Sprintf("test-validation-%d", time.Now().UnixNano())
	root := t.TempDir()
	actor := contracts.ActorRef{ID: "validation-operator", Kind: "test", WorkspaceID: workspace}
	artifacts := store.NewArtifactStore(pool)
	events := store.NewEventStore(pool)
	receipts := store.NewWorkReceiptStore(pool)
	registry, err := validationruntime.NewDefaultRegistry()
	if err != nil {
		pool.Close()
		t.Fatal(err)
	}
	policies := store.NewPolicyStore(pool, events, &policyruntime.Resolver{Lookup: func(ref contracts.ValidatorRef) bool {
		_, ok := registry.Lookup(ref)
		return ok
	}})
	changeStore := store.NewRepositoryChangeStore(pool, events, artifacts)
	changeStore.SetPolicyStore(policies)
	changes := change.NewService(changeStore, artifacts)
	changes.SetReceiptStore(receipts)
	validations := store.NewValidationStore(pool, events, store.NewEvidenceStore(pool), artifacts, store.NewObservabilityStore(pool))
	validations.SetReceiptStore(receipts)
	validations.SetPolicyStore(policies)
	runtime := &validationruntime.Service{Registry: registry, Runs: validations, Discovery: ingest.Discover}
	integration := &validationIntegration{pool: pool, workspace: workspace, root: root, actor: actor, changes: changes, validations: validations, runtime: runtime, policies: policies}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		// The test workspace is unique. Cleanup remains explicit so this test can
		// run against a shared development database without broad deletion.
		for _, query := range []string{
			`DELETE FROM fornix.work_receipt_references WHERE workspace_id=$1`,
			`DELETE FROM fornix.work_receipt_steps WHERE workspace_id=$1`,
			`DELETE FROM fornix.work_receipts WHERE workspace_id=$1`,
			`DELETE FROM fornix.validation_artifact_links WHERE workspace_id=$1`,
			`DELETE FROM fornix.reindex_handoffs WHERE workspace_id=$1`,
			`DELETE FROM fornix.validation_check_results WHERE workspace_id=$1`,
			`DELETE FROM fornix.validation_transitions WHERE workspace_id=$1`,
			`DELETE FROM fornix.validation_runs WHERE workspace_id=$1`,
			`DELETE FROM fornix.change_artifact_links WHERE workspace_id=$1`,
			`DELETE FROM fornix.change_transitions WHERE workspace_id=$1`,
			`DELETE FROM fornix.change_approvals WHERE workspace_id=$1`,
			`DELETE FROM fornix.change_applications WHERE workspace_id=$1`,
			`DELETE FROM fornix.change_operations WHERE workspace_id=$1`,
			`DELETE FROM fornix.change_proposals WHERE workspace_id=$1`,
			`DELETE FROM fornix.evidence_records WHERE workspace_id=$1`,
			`DELETE FROM fornix.artifact_provenance WHERE workspace_id=$1`,
			`DELETE FROM fornix.artifact_refs WHERE workspace_id=$1`,
			`DELETE FROM fornix.artifact_chunks WHERE workspace_id=$1`,
			`DELETE FROM fornix.artifacts WHERE workspace_id=$1`,
			`DELETE FROM fornix.control_events WHERE workspace_id=$1`,
			`DELETE FROM fornix.idempotency_records WHERE workspace_id=$1`,
			`DELETE FROM fornix.validation_policy_defaults WHERE workspace_id=$1`,
			`DELETE FROM fornix.validation_policy_idempotency WHERE workspace_id=$1`,
			`DELETE FROM fornix.validation_policy_audit WHERE workspace_id=$1`,
			`DELETE FROM fornix.validation_policy_transitions WHERE workspace_id=$1`,
			`DELETE FROM fornix.validation_policy_rules WHERE workspace_id=$1`,
			`DELETE FROM fornix.validation_policy_versions WHERE workspace_id=$1`,
			`DELETE FROM fornix.validation_policies WHERE workspace_id=$1`,
		} {
			_, _ = pool.Exec(cleanupCtx, query, workspace)
		}
		pool.Close()
	})
	return integration
}

func (i *validationIntegration) appliedChange(t *testing.T, key string) (contracts.ChangeApplication, contracts.ChangeProposal) {
	t.Helper()
	request := contracts.ChangeProposalRequest{
		WorkspaceID: i.workspace, Repository: "validation-repository", IdempotencyKey: key,
		Actor: i.actor, ApprovalMode: contracts.ChangeApprovalAutomatic,
		Source:     contracts.ChangeSourceSnapshot{WorkspaceID: i.workspace, Repository: "validation-repository", SourceRoot: i.root},
		Operations: []contracts.ChangeOperationInput{{ID: "op-1", Type: contracts.ChangeOpCreate, Path: "validated.txt", Content: []byte("validated content\n")}},
	}
	proposal, _, _, err := i.changes.Propose(context.Background(), change.PlanInput{Request: request, Root: i.root})
	if err != nil {
		t.Fatal(err)
	}
	application, appliedProposal, err := i.changes.Apply(context.Background(), contracts.ChangeApplicationRequest{
		WorkspaceID: i.workspace, ProposalID: proposal.ID, PacketHash: proposal.PacketHash,
		IdempotencyKey: "application:" + key, Actor: i.actor,
	}, i.root)
	if err != nil {
		t.Fatal(err)
	}
	if application.Status != contracts.ChangeApplied || appliedProposal.Status != contracts.ChangeApplied {
		t.Fatalf("change was not applied: application=%+v proposal=%+v", application, appliedProposal)
	}
	return application, appliedProposal
}

func validationRequest(i *validationIntegration, application contracts.ChangeApplication, proposal contracts.ChangeProposal, key string) contracts.ValidationRequest {
	return contracts.ValidationRequest{
		ID: "validation-" + key, RequestID: "validation-request-" + key, IdempotencyKey: key,
		WorkspaceID: i.workspace, Actor: i.actor, ChangeApplicationID: application.ID, ProposalID: proposal.ID,
		PacketHash: application.PacketHash, ExpectedTreeHash: application.ExpectedTreeHash,
		Repository: proposal.Repository, SourceManifestHash: proposal.Source.ManifestHash,
		Source: contracts.RepositorySource{Repository: proposal.Repository, SourceRoot: i.root, MountRoot: i.root},
	}
}

func TestValidationEndToEndIsIdempotentReplayableAndReceiptBacked(t *testing.T) {
	integration := newValidationIntegration(t)
	application, proposal := integration.appliedChange(t, "end-to-end")
	request := validationRequest(integration, application, proposal, "validation-end-to-end")
	first, err := integration.runtime.Validate(context.Background(), request, integration.root, integration.root)
	if err != nil {
		t.Fatal(err)
	}
	if first.Run.Status != contracts.ValidationPassed || first.Handoff == nil || first.Handoff.Status != contracts.ReindexHandoffPending {
		t.Fatalf("validation result = %+v", first)
	}
	if first.Run.Report == nil || first.Run.Report.ResultCount != len(contracts.DefaultValidatorRefs()) || first.Run.ReplayHash == "" {
		t.Fatalf("validation report is incomplete: %+v", first.Run)
	}

	duplicate, err := integration.runtime.Validate(context.Background(), request, integration.root, integration.root)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Run.ID != first.Run.ID || duplicate.Run.ReportHash != first.Run.ReportHash || duplicate.Run.ReplayHash != first.Run.ReplayHash {
		t.Fatalf("duplicate validation changed durable identity: first=%+v duplicate=%+v", first.Run, duplicate.Run)
	}

	var runs, results, receipts, evidence, handoffCount, links int
	queries := []struct {
		query string
		out   *int
	}{
		{`SELECT count(*) FROM fornix.validation_runs WHERE workspace_id=$1 AND id=$2`, &runs},
		{`SELECT count(*) FROM fornix.validation_check_results WHERE workspace_id=$1 AND validation_run_id=$2`, &results},
		{`SELECT count(*) FROM fornix.work_receipts WHERE workspace_id=$1 AND work_kind='validation' AND work_id=$2`, &receipts},
		{`SELECT count(*) FROM fornix.evidence_records WHERE workspace_id=$1 AND kind='validation_result'`, &evidence},
		{`SELECT count(*) FROM fornix.reindex_handoffs WHERE workspace_id=$1 AND validation_run_id=$2`, &handoffCount},
		{`SELECT count(*) FROM fornix.validation_artifact_links WHERE workspace_id=$1 AND validation_run_id=$2`, &links},
	}
	for _, query := range queries {
		args := []any{integration.workspace, first.Run.ID}
		if query.out == &evidence {
			args = []any{integration.workspace}
		}
		if err := integration.pool.QueryRow(context.Background(), query.query, args...).Scan(query.out); err != nil {
			t.Fatal(err)
		}
	}
	if runs != 1 || results != len(contracts.DefaultValidatorRefs()) || receipts != 1 || evidence != len(contracts.DefaultValidatorRefs()) || handoffCount != 1 || links != 0 {
		// The default report is intentionally inline. Artifact links become
		// non-zero when a caller requests an oversized report budget boundary.
		t.Fatalf("durable validation counts runs=%d results=%d receipts=%d evidence=%d handoffs=%d links=%d", runs, results, receipts, evidence, handoffCount, links)
	}

	replay, err := integration.validations.Replay(context.Background(), contracts.ValidationReplayRequest{WorkspaceID: integration.workspace, ValidationRunID: first.Run.ID, Limit: 32})
	if err != nil {
		t.Fatal(err)
	}
	if replay.ReplayHash != first.Run.ReplayHash || len(replay.Results) != len(contracts.DefaultValidatorRefs()) || len(replay.Events) != 2 {
		t.Fatalf("replay is not complete or stable: %+v", replay)
	}
	for _, event := range replay.Events {
		var payload map[string]any
		if err := json.Unmarshal(event.Payload, &payload); err != nil {
			t.Fatal(err)
		}
		if payload["run_id"] != first.Run.ID {
			t.Fatalf("replay event is missing validation run identity: %+v", event.Payload)
		}
	}

	disclosure, err := integration.validations.Disclose(context.Background(), contracts.ValidationDisclosureRequest{WorkspaceID: integration.workspace, ValidationRunID: first.Run.ID, Level: string(contracts.DisclosureDetail), MaxBytes: 32 << 10, MaxItems: 32})
	if err != nil {
		t.Fatal(err)
	}
	secondDisclosure, err := integration.validations.Disclose(context.Background(), contracts.ValidationDisclosureRequest{WorkspaceID: integration.workspace, ValidationRunID: first.Run.ID, Level: string(contracts.DisclosureDetail), MaxBytes: 32 << 10, MaxItems: 32})
	if err != nil {
		t.Fatal(err)
	}
	if disclosure.ContentViewHash == "" || disclosure.ContentViewHash != secondDisclosure.ContentViewHash || disclosure.ReportHash != first.Run.ReportHash {
		t.Fatalf("disclosure hash is not stable: first=%+v second=%+v", disclosure, secondDisclosure)
	}
	encodedDisclosure, err := json.Marshal(disclosure)
	if err != nil {
		t.Fatal(err)
	}
	if len(encodedDisclosure) > 32<<10 || disclosure.TotalBytes != len(encodedDisclosure) {
		t.Fatalf("disclosure exceeded its declared byte budget: bytes=%d declared=%d", len(encodedDisclosure), disclosure.TotalBytes)
	}
	if _, err := integration.validations.Get(context.Background(), integration.workspace+"-foreign", first.Run.ID); !errors.Is(err, store.ErrValidationNotFound) {
		t.Fatalf("cross-workspace validation read error = %v", err)
	}
}

func TestValidationPinsWorkspaceDefaultPolicyAndReplaysItsReference(t *testing.T) {
	integration := newValidationIntegration(t)
	pack := policyPack(integration.workspace, "1")
	pack.PolicyID = "validation-admission"
	pack.Approval.Mode = contracts.PolicyApprovalAutomatic
	policy, created, err := integration.policies.Create(context.Background(), contracts.PolicyCreateRequest{WorkspaceID: integration.workspace, RequestID: "policy-create-validation", IdempotencyKey: "policy-create-validation", Actor: integration.actor, Pack: pack})
	if err != nil || !created {
		t.Fatalf("policy create = %+v, created=%v, err=%v", policy, created, err)
	}
	if _, _, err := integration.policies.Activate(context.Background(), contracts.PolicyLifecycleRequest{WorkspaceID: integration.workspace, Policy: policy.Pack.Ref(), RequestID: "policy-activate-validation", IdempotencyKey: "policy-activate-validation", Actor: integration.actor}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := integration.policies.SetDefault(context.Background(), contracts.PolicyLifecycleRequest{WorkspaceID: integration.workspace, Policy: policy.Pack.Ref(), RequestID: "policy-default-validation", IdempotencyKey: "policy-default-validation", Actor: integration.actor}); err != nil {
		t.Fatal(err)
	}
	application, proposal := integration.appliedChange(t, "default-policy")
	result, err := integration.runtime.Validate(context.Background(), validationRequest(integration, application, proposal, "default-policy-validation"), integration.root, integration.root)
	if err != nil {
		t.Fatal(err)
	}
	if result.Run.Policy == nil || result.Run.Policy.PolicyHash != policy.PolicyHash || result.Run.Report == nil || result.Run.Report.Policy == nil || result.Run.Report.Policy.PolicyHash != policy.PolicyHash {
		t.Fatalf("validation policy reference was not preserved: run=%+v report=%+v", result.Run, result.Run.Report)
	}
	if result.Handoff != nil {
		t.Fatalf("policy with require_reindex=false unexpectedly created a handoff: %+v", result.Handoff)
	}
	var handoffCount int
	if err := integration.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.reindex_handoffs WHERE workspace_id=$1 AND validation_run_id=$2`, integration.workspace, result.Run.ID).Scan(&handoffCount); err != nil {
		t.Fatal(err)
	}
	if handoffCount != 0 {
		t.Fatalf("policy with require_reindex=false persisted %d handoffs", handoffCount)
	}
	replay, err := integration.validations.Replay(context.Background(), contracts.ValidationReplayRequest{WorkspaceID: integration.workspace, ValidationRunID: result.Run.ID, Limit: 32})
	if err != nil {
		t.Fatal(err)
	}
	if replay.ReplayHash != result.Run.ReplayHash || len(replay.Events) != 2 {
		t.Fatalf("policy validation replay changed: %+v", replay)
	}
	for _, event := range replay.Events {
		if event.Policy == nil || event.Policy.PolicyHash != policy.PolicyHash {
			t.Fatalf("replayed event lost policy reference: %+v", event)
		}
	}

	// A later immutable version can tighten the workflow by requiring the
	// re-index handoff. Activation and default binding are separate so the
	// test also verifies that only the selected workspace default controls new
	// admissions.
	reindexPack := policyPack(integration.workspace, "2")
	reindexPack.PolicyID = policy.PolicyID
	reindexPack.RequireReindex = true
	reindexPack.Approval.Mode = contracts.PolicyApprovalAutomatic
	reindexPolicy, created, err := integration.policies.Create(context.Background(), contracts.PolicyCreateRequest{
		WorkspaceID: integration.workspace, RequestID: "policy-create-reindex", IdempotencyKey: "policy-create-reindex", Actor: integration.actor, Pack: reindexPack,
	})
	if err != nil || !created {
		t.Fatalf("reindex policy create = %+v, created=%v, err=%v", reindexPolicy, created, err)
	}
	if _, _, err := integration.policies.Activate(context.Background(), contracts.PolicyLifecycleRequest{WorkspaceID: integration.workspace, Policy: reindexPolicy.Pack.Ref(), RequestID: "policy-activate-reindex", IdempotencyKey: "policy-activate-reindex", Actor: integration.actor}); err != nil {
		t.Fatal(err)
	}
	if _, _, err := integration.policies.SetDefault(context.Background(), contracts.PolicyLifecycleRequest{WorkspaceID: integration.workspace, Policy: reindexPolicy.Pack.Ref(), RequestID: "policy-default-reindex", IdempotencyKey: "policy-default-reindex", Actor: integration.actor}); err != nil {
		t.Fatal(err)
	}
	integration.root = t.TempDir()
	reindexApplication, reindexProposal := integration.appliedChange(t, "default-policy-reindex")
	reindexed, err := integration.runtime.Validate(context.Background(), validationRequest(integration, reindexApplication, reindexProposal, "default-policy-reindex-validation"), integration.root, integration.root)
	if err != nil {
		t.Fatal(err)
	}
	if reindexed.Handoff == nil || reindexed.Handoff.Policy == nil || reindexed.Handoff.Policy.PolicyHash != reindexPolicy.PolicyHash {
		t.Fatalf("policy with require_reindex=true did not create a pinned handoff: %+v", reindexed.Handoff)
	}
}

func TestValidationCrashBeforeCommitResumesWithoutDuplicateHistory(t *testing.T) {
	integration := newValidationIntegration(t)
	application, proposal := integration.appliedChange(t, "crash-recovery")
	request := validationRequest(integration, application, proposal, "validation-crash-recovery")
	integration.validations.SetFailureHook(func(stage string) error {
		if stage == "validation_before_commit" {
			return errors.New("simulated validation crash")
		}
		return nil
	})
	if _, err := integration.runtime.Validate(context.Background(), request, integration.root, integration.root); err == nil {
		t.Fatal("validation crash unexpectedly succeeded")
	}
	integration.validations.SetFailureHook(nil)
	run, err := integration.validations.Get(context.Background(), integration.workspace, request.ID)
	if err != nil {
		t.Fatal(err)
	}
	if run.Status != contracts.ValidationPending || run.ResultCount != 0 {
		t.Fatalf("crash left partial authoritative state: %+v", run)
	}
	resumed, err := integration.runtime.Resume(context.Background(), integration.workspace, run.ID, integration.root, integration.root)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Run.Status != contracts.ValidationPassed {
		t.Fatalf("resumed validation status = %+v", resumed.Run)
	}
	var resultCount int
	if err := integration.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.validation_check_results WHERE workspace_id=$1 AND validation_run_id=$2`, integration.workspace, run.ID).Scan(&resultCount); err != nil {
		t.Fatal(err)
	}
	if resultCount != len(contracts.DefaultValidatorRefs()) {
		t.Fatalf("resumed validation result count = %d", resultCount)
	}
}

func TestValidationDryRunAndOversizedReportAreBoundedAndAtomic(t *testing.T) {
	integration := newValidationIntegration(t)
	application, proposal := integration.appliedChange(t, "report-artifact")
	dryRequest := validationRequest(integration, application, proposal, "validation-dry-run")
	dryRequest.DryRun = true
	dry, err := integration.runtime.Validate(context.Background(), dryRequest, integration.root, integration.root)
	if err != nil {
		t.Fatal(err)
	}
	if !dry.DryRun || dry.Run.Status != contracts.ValidationAbstained || dry.Run.Report == nil || dry.Run.Report.Outcome != contracts.ValidationOutcomeSkipped {
		t.Fatalf("dry-run result = %+v", dry)
	}
	var dryRows int
	if err := integration.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.validation_runs WHERE workspace_id=$1 AND idempotency_key=$2`, integration.workspace, dryRequest.IdempotencyKey).Scan(&dryRows); err != nil {
		t.Fatal(err)
	}
	if dryRows != 0 {
		t.Fatalf("dry-run created %d durable validation rows", dryRows)
	}

	artifactRequest := validationRequest(integration, application, proposal, "validation-report-artifact")
	artifactRequest.Budget.MaxReportBytes = 1
	result, err := integration.runtime.Validate(context.Background(), artifactRequest, integration.root, integration.root)
	if err != nil {
		t.Fatal(err)
	}
	if result.Run.ReportArtifact == nil || result.Run.Report == nil || len(result.Run.Report.Results) != 0 {
		t.Fatalf("oversized report was not reduced to an artifact-backed inline view: %+v", result.Run)
	}
	var links int
	if err := integration.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.validation_artifact_links WHERE workspace_id=$1 AND validation_run_id=$2`, integration.workspace, result.Run.ID).Scan(&links); err != nil {
		t.Fatal(err)
	}
	receipt, err := store.NewWorkReceiptStore(integration.pool).Get(context.Background(), integration.workspace, "validation-receipt-"+result.Run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if links != 1 || len(receipt.Artifacts) != 1 {
		t.Fatalf("report artifact links are not durable: validation_links=%d receipt_artifacts=%d", links, len(receipt.Artifacts))
	}
	if _, err := integration.validations.Disclose(context.Background(), contracts.ValidationDisclosureRequest{WorkspaceID: integration.workspace, ValidationRunID: result.Run.ID, Level: string(contracts.DisclosureDetail), MaxBytes: 1, MaxItems: 1}); !errors.Is(err, store.ErrValidationDisclosureBudget) {
		t.Fatalf("too-small disclosure error = %v", err)
	}
}

func TestValidationConcurrentDuplicateDeliveryHasOneEffect(t *testing.T) {
	integration := newValidationIntegration(t)
	application, proposal := integration.appliedChange(t, "concurrent")
	request := validationRequest(integration, application, proposal, "validation-concurrent")
	const workers = 8
	results := make(chan validationruntime.Result, workers)
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for n := 0; n < workers; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			result, err := integration.runtime.Validate(context.Background(), request, integration.root, integration.root)
			if err != nil {
				errs <- err
				return
			}
			results <- result
		}()
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var first validationruntime.Result
	for result := range results {
		if first.Run.ID == "" {
			first = result
			continue
		}
		if result.Run.ID != first.Run.ID || result.Run.ReplayHash != first.Run.ReplayHash {
			t.Fatalf("concurrent validation identities differ: first=%+v result=%+v", first.Run, result.Run)
		}
	}
	if first.Run.ID == "" {
		t.Fatal("concurrent validation produced no result")
	}
	var runCount, resultCount, receiptCount int
	for query, out := range map[string]*int{
		`SELECT count(*) FROM fornix.validation_runs WHERE workspace_id=$1 AND idempotency_key=$2`:                  &runCount,
		`SELECT count(*) FROM fornix.validation_check_results WHERE workspace_id=$1 AND validation_run_id=$2`:       &resultCount,
		`SELECT count(*) FROM fornix.work_receipts WHERE workspace_id=$1 AND work_kind='validation' AND work_id=$2`: &receiptCount,
	} {
		args := []any{integration.workspace, request.IdempotencyKey}
		if out != &runCount {
			args = []any{integration.workspace, first.Run.ID}
		}
		if err := integration.pool.QueryRow(context.Background(), query, args...).Scan(out); err != nil {
			t.Fatal(err)
		}
	}
	if runCount != 1 || resultCount != len(contracts.DefaultValidatorRefs()) || receiptCount != 1 {
		t.Fatalf("concurrent validation counts runs=%d results=%d receipts=%d", runCount, resultCount, receiptCount)
	}
}

func TestValidationStaleTaskFenceFailsClosed(t *testing.T) {
	integration := newValidationIntegration(t)
	application, proposal := integration.appliedChange(t, "stale-fence")
	var taskID int64
	if err := integration.pool.QueryRow(context.Background(), `INSERT INTO fornix.tasks(workspace_id,title,brief,created_by,status,execution_fence,max_attempts) VALUES($1,'validation task','validation fence test','test','claimed',4,1) RETURNING id`, integration.workspace).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := integration.pool.Exec(context.Background(), `INSERT INTO fornix.task_execution_leases(workspace_id,task_id,owner_id,fence,lease_until) VALUES($1,$2,'current-worker',4,clock_timestamp()+interval '1 minute')`, integration.workspace, taskID); err != nil {
		t.Fatal(err)
	}
	request := validationRequest(integration, application, proposal, "validation-stale-fence")
	request.Task = &contracts.EntityRef{ID: fmt.Sprint(taskID), Kind: "task", WorkspaceID: integration.workspace}
	request.TaskOwnerID, request.TaskFence = "current-worker", 4
	plan, err := request.Plan()
	if err != nil {
		t.Fatal(err)
	}
	started, _, err := integration.validations.Start(context.Background(), store.StartValidationInput{Request: request, Plan: plan})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := integration.pool.Exec(context.Background(), `UPDATE fornix.task_execution_leases SET released_at=clock_timestamp() WHERE workspace_id=$1 AND task_id=$2`, integration.workspace, taskID); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := integration.validations.Commit(context.Background(), store.ValidationCommitInput{WorkspaceID: integration.workspace, RunID: started.ID, Actor: integration.actor, TaskOwnerID: request.TaskOwnerID, TaskFence: request.TaskFence, Results: []contracts.ValidationResult{{Ordinal: 0, Validator: plan.Validators[0], InputHash: plan.RequestHash, Status: contracts.ValidationPassed, Outcome: contracts.ValidationOutcomePassed, Summary: "test"}}, Report: contracts.ValidationReport{RunID: started.ID}}); !errors.Is(err, store.ErrValidationStaleFence) {
		t.Fatalf("stale validation commit error = %v", err)
	}
}

func TestValidationCommitRejectsPartialPlan(t *testing.T) {
	integration := newValidationIntegration(t)
	application, proposal := integration.appliedChange(t, "partial-plan")
	request := validationRequest(integration, application, proposal, "validation-partial-plan")
	plan, err := request.Plan()
	if err != nil {
		t.Fatal(err)
	}
	started, _, err := integration.validations.Start(context.Background(), store.StartValidationInput{Request: request, Plan: plan})
	if err != nil {
		t.Fatal(err)
	}
	_, _, _, err = integration.validations.Commit(context.Background(), store.ValidationCommitInput{
		WorkspaceID: integration.workspace,
		RunID:       started.ID,
		Actor:       integration.actor,
		Results: []contracts.ValidationResult{{
			Ordinal: 0, Validator: plan.Validators[0], InputHash: plan.RequestHash,
			Status: contracts.ValidationPassed, Outcome: contracts.ValidationOutcomePassed,
		}},
		Report: contracts.ValidationReport{RunID: started.ID},
	})
	if !errors.Is(err, store.ErrValidationResultCount) {
		t.Fatalf("partial validation commit error = %v", err)
	}
	current, err := integration.validations.Get(context.Background(), integration.workspace, started.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != contracts.ValidationPending || current.ResultCount != 0 {
		t.Fatalf("partial commit changed durable run: %+v", current)
	}
}

func TestValidationCommitRejectsUnresolvedEvidence(t *testing.T) {
	integration := newValidationIntegration(t)
	application, proposal := integration.appliedChange(t, "evidence-binding")
	request := validationRequest(integration, application, proposal, "validation-evidence-binding")
	plan, err := request.Plan()
	if err != nil {
		t.Fatal(err)
	}
	started, _, err := integration.validations.Start(context.Background(), store.StartValidationInput{Request: request, Plan: plan})
	if err != nil {
		t.Fatal(err)
	}
	results := make([]contracts.ValidationResult, len(plan.Validators))
	for ordinal, validator := range plan.Validators {
		results[ordinal] = contracts.ValidationResult{
			Ordinal: ordinal, Validator: validator, InputHash: plan.RequestHash,
			Status: contracts.ValidationPassed, Outcome: contracts.ValidationOutcomePassed,
		}
	}
	results[0].Evidence = []contracts.ValidationEvidence{{
		Kind: "evidence", SourceReference: "999999999", Hash: fmt.Sprintf("%064d", 0), Role: "unresolved",
	}}
	_, _, _, err = integration.validations.Commit(context.Background(), store.ValidationCommitInput{
		WorkspaceID: integration.workspace, RunID: started.ID, Actor: integration.actor,
		Results: results, Report: contracts.ValidationReport{RunID: started.ID},
	})
	if !errors.Is(err, store.ErrValidationEvidence) {
		t.Fatalf("unresolved evidence commit error = %v", err)
	}
	current, err := integration.validations.Get(context.Background(), integration.workspace, started.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != contracts.ValidationPending || current.ResultCount != 0 {
		t.Fatalf("unresolved evidence changed durable run: %+v", current)
	}
}

func TestValidationBindsAuthorityMetadataAndHandoffManifest(t *testing.T) {
	integration := newValidationIntegration(t)
	application, proposal := integration.appliedChange(t, "authority-binding")
	request := validationRequest(integration, application, proposal, "validation-authority-binding")
	request.Repository = "different-repository"
	request.Source.Repository = request.Repository
	request.Source.SourceRoot = filepath.Join(integration.root, "different-root")
	plan, err := request.Plan()
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := integration.validations.Start(context.Background(), store.StartValidationInput{Request: request, Plan: plan}); !errors.Is(err, store.ErrValidationAuthority) {
		t.Fatalf("authority mismatch error = %v", err)
	}

	validRequest := validationRequest(integration, application, proposal, "validation-handoff-binding")
	validated, err := integration.runtime.Validate(context.Background(), validRequest, integration.root, integration.root)
	if err != nil {
		t.Fatal(err)
	}
	if validated.Handoff == nil {
		t.Fatal("validation did not create a handoff")
	}
	badJob := contracts.IngestJob{ID: "ingest-wrong-manifest", WorkspaceID: integration.workspace, Repository: proposal.Repository, ManifestHash: fmt.Sprintf("%064d", 0)}
	if _, err := integration.validations.MarkHandoffSubmitted(context.Background(), validated.Handoff.ID, integration.workspace, badJob); !errors.Is(err, store.ErrHandoffConflict) {
		t.Fatalf("handoff manifest mismatch error = %v", err)
	}
	goodJob := contracts.IngestJob{ID: "ingest-matching-manifest", WorkspaceID: integration.workspace, Repository: proposal.Repository, ManifestHash: validated.Handoff.ManifestHash}
	submitted, err := integration.validations.MarkHandoffSubmitted(context.Background(), validated.Handoff.ID, integration.workspace, goodJob)
	if err != nil {
		t.Fatal(err)
	}
	if submitted.Status != contracts.ReindexHandoffSubmitted {
		t.Fatalf("handoff status = %s", submitted.Status)
	}
	unchanged, err := integration.validations.MarkHandoffFailed(context.Background(), integration.workspace, validated.Handoff.ID, contracts.ValidationFailure{Code: contracts.ValidationFailureRecovery, Message: "late failure"})
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.Status != contracts.ReindexHandoffSubmitted {
		t.Fatalf("late failure regressed submitted handoff: %+v", unchanged)
	}
}
