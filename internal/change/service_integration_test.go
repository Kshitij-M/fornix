package change_test

import (
	"context"
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
	"github.com/omaveda/fornix/internal/store"
)

type changeIntegration struct {
	service *change.Service
	store   *store.RepositoryChangeStore
	pool    *pgxpool.Pool
	root    string
	work    string
}

func newChangeIntegration(t *testing.T) *changeIntegration {
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
	workspace := fmt.Sprintf("test-change-%d", time.Now().UnixNano())
	root := t.TempDir()
	artifacts := store.NewArtifactStore(pool)
	events := store.NewEventStore(pool)
	changeStore := store.NewRepositoryChangeStore(pool, events, artifacts)
	service := change.NewService(changeStore, artifacts)
	service.SetReceiptStore(store.NewWorkReceiptStore(pool))
	integration := &changeIntegration{service: service, store: changeStore, pool: pool, root: root, work: workspace}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.work_receipt_references WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.work_receipt_steps WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.work_receipts WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.change_artifact_links WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.change_transitions WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.change_approvals WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.change_applications WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.change_operations WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.change_proposals WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.artifact_refs WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.artifact_chunks WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.artifacts WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.control_events WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.idempotency_records WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.task_execution_leases WHERE workspace_id=$1`, workspace)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM fornix.tasks WHERE workspace_id=$1`, workspace)
		pool.Close()
	})
	return integration
}

func changeRequest(workspace, key, root string, content []byte) contracts.ChangeProposalRequest {
	return contracts.ChangeProposalRequest{
		WorkspaceID: workspace, Repository: "repo", IdempotencyKey: key,
		Actor:      contracts.ActorRef{ID: "operator", Kind: "user", WorkspaceID: workspace},
		Source:     contracts.ChangeSourceSnapshot{WorkspaceID: workspace, Repository: "repo", SourceRoot: root},
		Operations: []contracts.ChangeOperationInput{{ID: "op-1", Type: contracts.ChangeOpCreate, Path: "result.txt", Content: content}},
	}
}

func TestRepositoryChangeConcurrentProposalIsIdempotentAndArtifactBacked(t *testing.T) {
	integration := newChangeIntegration(t)
	request := changeRequest(integration.work, "proposal-concurrent", integration.root, []byte("durable content"))
	const writers = 10
	proposals := make(chan contracts.ChangeProposal, writers)
	errs := make(chan error, writers)
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			proposal, _, _, err := integration.service.Propose(context.Background(), change.PlanInput{Request: request, Root: integration.root})
			if err != nil {
				errs <- err
				return
			}
			proposals <- proposal
		}()
	}
	wg.Wait()
	close(proposals)
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	var first contracts.ChangeProposal
	for proposal := range proposals {
		if first.ID == "" {
			first = proposal
			continue
		}
		if proposal.ID != first.ID || proposal.PacketHash != first.PacketHash {
			t.Fatalf("concurrent proposal identities differ: %#v and %#v", first, proposal)
		}
	}
	if first.ID == "" || first.Status != contracts.ChangeAwaitingApproval || first.DiffArtifact == nil || len(first.Operations) != 1 || first.Operations[0].NewContentArtifact == nil {
		t.Fatalf("proposal is not artifact-backed: %#v", first)
	}
	var proposalCount, artifactCount, operationCount int
	if err := integration.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.change_proposals WHERE workspace_id=$1 AND idempotency_key=$2`, integration.work, request.IdempotencyKey).Scan(&proposalCount); err != nil {
		t.Fatal(err)
	}
	if err := integration.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.artifact_refs WHERE workspace_id=$1 AND source_kind='change_proposal' AND source_id=$2`, integration.work, first.ID).Scan(&artifactCount); err != nil {
		t.Fatal(err)
	}
	if err := integration.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.change_operations WHERE workspace_id=$1 AND proposal_id=$2`, integration.work, first.ID).Scan(&operationCount); err != nil {
		t.Fatal(err)
	}
	if proposalCount != 1 || artifactCount != 2 || operationCount != 1 {
		t.Fatalf("durable counts proposal=%d artifacts=%d operations=%d", proposalCount, artifactCount, operationCount)
	}
}

func TestRepositoryChangeApprovalApplyDuplicateAndDryRun(t *testing.T) {
	integration := newChangeIntegration(t)
	request := changeRequest(integration.work, "proposal-apply", integration.root, []byte("applied once"))
	proposal, _, _, err := integration.service.Propose(context.Background(), change.PlanInput{Request: request, Root: integration.root})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := integration.service.Approve(context.Background(), contracts.ChangeApprovalRequest{WorkspaceID: integration.work, ProposalID: proposal.ID, PacketHash: proposal.PacketHash, Decision: "approved", IdempotencyKey: "approval-1", Actor: request.Actor}); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := integration.service.Approve(context.Background(), contracts.ChangeApprovalRequest{WorkspaceID: integration.work, ProposalID: proposal.ID, PacketHash: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", Decision: "approved", IdempotencyKey: "approval-1", Actor: request.Actor}); !errors.Is(err, store.ErrIdempotencyConflict) {
		t.Fatalf("approval packet mismatch error = %v", err)
	}
	applicationRequest := contracts.ChangeApplicationRequest{WorkspaceID: integration.work, ProposalID: proposal.ID, PacketHash: proposal.PacketHash, IdempotencyKey: "application-1", Actor: request.Actor}
	dryRequest := applicationRequest
	dryRequest.IdempotencyKey = "dry-run-application"
	dryRequest.DryRun = true
	dry, _, err := integration.service.Apply(context.Background(), dryRequest, integration.root)
	if err != nil || dry.ResultTreeHash != proposal.ExpectedTreeHash {
		t.Fatalf("dry-run result = %#v, err = %v", dry, err)
	}
	var applications int
	if err := integration.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.change_applications WHERE workspace_id=$1 AND proposal_id=$2`, integration.work, proposal.ID).Scan(&applications); err != nil {
		t.Fatal(err)
	}
	if applications != 0 {
		t.Fatalf("dry-run mutated application count to %d", applications)
	}
	application, appliedProposal, err := integration.service.Apply(context.Background(), applicationRequest, integration.root)
	if err != nil {
		t.Fatal(err)
	}
	if application.Status != contracts.ChangeApplied || appliedProposal.Status != contracts.ChangeApplied {
		t.Fatalf("apply result = %#v / %#v", application, appliedProposal)
	}
	content, err := os.ReadFile(filepath.Join(integration.root, "result.txt"))
	if err != nil || string(content) != "applied once" {
		t.Fatalf("applied file = %q, err = %v", content, err)
	}
	duplicate, _, err := integration.service.Apply(context.Background(), applicationRequest, integration.root)
	if err != nil || duplicate.ID != application.ID || duplicate.Status != contracts.ChangeApplied {
		t.Fatalf("duplicate apply = %#v, err = %v", duplicate, err)
	}
	if err := integration.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.change_applications WHERE workspace_id=$1 AND proposal_id=$2`, integration.work, proposal.ID).Scan(&applications); err != nil {
		t.Fatal(err)
	}
	if applications != 1 {
		t.Fatalf("duplicate apply created %d application rows", applications)
	}
}

func TestRepositoryChangeProposalRollbackLeavesNoAuthoritativeRows(t *testing.T) {
	integration := newChangeIntegration(t)
	integration.store.SetFailureHook(func(stage string) error {
		if stage == "proposal_ready" {
			return errors.New("simulated crash before proposal commit")
		}
		return nil
	})
	request := changeRequest(integration.work, "proposal-crash", integration.root, []byte("should rollback"))
	if _, _, _, err := integration.service.Propose(context.Background(), change.PlanInput{Request: request, Root: integration.root}); err == nil {
		t.Fatal("simulated crash unexpectedly succeeded")
	}
	integration.store.SetFailureHook(nil)
	var proposals, operations, artifacts int
	for query, destination := range map[string]*int{
		`SELECT count(*) FROM fornix.change_proposals WHERE workspace_id=$1 AND idempotency_key=$2`: &proposals,
		`SELECT count(*) FROM fornix.change_operations WHERE workspace_id=$1`:                       &operations,
		`SELECT count(*) FROM fornix.artifact_refs WHERE workspace_id=$1`:                           &artifacts,
	} {
		args := []any{integration.work}
		if destination == &proposals {
			args = append(args, request.IdempotencyKey)
		}
		if err := integration.pool.QueryRow(context.Background(), query, args...).Scan(destination); err != nil {
			t.Fatal(err)
		}
	}
	if proposals != 0 || operations != 0 || artifacts != 0 {
		t.Fatalf("rollback left rows proposals=%d operations=%d artifacts=%d", proposals, operations, artifacts)
	}
}

func TestRepositoryChangeApplicationRejectsStaleTaskFence(t *testing.T) {
	integration := newChangeIntegration(t)
	var taskID int64
	if err := integration.pool.QueryRow(context.Background(), `
		INSERT INTO fornix.tasks(workspace_id,title,brief,created_by,status,execution_fence,max_attempts)
		VALUES($1,'fenced change','fenced repository change','test','claimed',2,1)
		RETURNING id`, integration.work).Scan(&taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := integration.pool.Exec(context.Background(), `
		INSERT INTO fornix.task_execution_leases(workspace_id,task_id,owner_id,fence,lease_until)
		VALUES($1,$2,'current-worker',2,clock_timestamp()+interval '1 minute')`, integration.work, taskID); err != nil {
		t.Fatal(err)
	}
	request := changeRequest(integration.work, "fenced-proposal", integration.root, []byte("fenced content"))
	request.Task = &contracts.EntityRef{ID: fmt.Sprint(taskID), Kind: "task", WorkspaceID: integration.work}
	request.TaskOwnerID, request.TaskFence = "current-worker", 2
	proposal, _, _, err := integration.service.Propose(context.Background(), change.PlanInput{Request: request, Root: integration.root})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := integration.service.Approve(context.Background(), contracts.ChangeApprovalRequest{
		WorkspaceID: integration.work, ProposalID: proposal.ID, PacketHash: proposal.PacketHash,
		Decision: "approved", IdempotencyKey: "fenced-approval", Actor: request.Actor,
	}); err != nil {
		t.Fatal(err)
	}
	stale := contracts.ChangeApplicationRequest{
		WorkspaceID: integration.work, ProposalID: proposal.ID, PacketHash: proposal.PacketHash,
		IdempotencyKey: "fenced-application", Actor: request.Actor,
		TaskOwnerID: "old-worker", TaskFence: 1,
	}
	if _, _, err := integration.service.Apply(context.Background(), stale, integration.root); !errors.Is(err, store.ErrChangeStaleFence) {
		t.Fatalf("stale change application error = %v", err)
	}
	var applications int
	if err := integration.pool.QueryRow(context.Background(), `SELECT count(*) FROM fornix.change_applications WHERE workspace_id=$1 AND proposal_id=$2`, integration.work, proposal.ID).Scan(&applications); err != nil {
		t.Fatal(err)
	}
	if applications != 0 {
		t.Fatalf("stale worker created %d application rows", applications)
	}
}
