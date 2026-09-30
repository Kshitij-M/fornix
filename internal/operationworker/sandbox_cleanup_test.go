package operationworker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

type cleanupStoreFixture struct {
	mu         sync.Mutex
	jobs       []contracts.SandboxCleanupJob
	claimErr   error
	renewErr   error
	renews     int
	completed  []contracts.SandboxCleanupObservation
	retried    []contracts.SandboxCleanupObservation
	completion contracts.SandboxCleanupJob
}

func (s *cleanupStoreFixture) Claim(_ context.Context, workspaceID, ownerID string, _ int, _ time.Duration) ([]contracts.SandboxCleanupJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.claimErr != nil {
		return nil, s.claimErr
	}
	jobs := append([]contracts.SandboxCleanupJob(nil), s.jobs...)
	for i := range jobs {
		jobs[i].Intent.WorkspaceID = workspaceID
		jobs[i].OwnerID = ownerID
	}
	return jobs, nil
}

func (s *cleanupStoreFixture) Renew(_ context.Context, _ contracts.SandboxCleanupLease, _ time.Duration) (contracts.SandboxCleanupLease, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.renews++
	return contracts.SandboxCleanupLease{}, s.renewErr
}

func (s *cleanupStoreFixture) Complete(_ context.Context, lease contracts.SandboxCleanupLease, observation contracts.SandboxCleanupObservation) (contracts.SandboxCleanupJob, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completed = append(s.completed, observation)
	return contracts.SandboxCleanupJob{ID: lease.JobID, Status: contracts.SandboxCleanupCompleted}, false, nil
}

func (s *cleanupStoreFixture) Retry(_ context.Context, _ contracts.SandboxCleanupLease, observation contracts.SandboxCleanupObservation) (contracts.SandboxCleanupJob, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retried = append(s.retried, observation)
	return contracts.SandboxCleanupJob{ID: observation.JobID, Status: contracts.SandboxCleanupRetryWait}, nil
}

type cleanupRunnerFixture struct {
	mu      sync.Mutex
	started chan struct{}
	release chan struct{}
	command contracts.SandboxCleanupCommand
	result  contracts.SandboxCleanupObservation
	err     error
}

func (r *cleanupRunnerFixture) CleanupAttempt(ctx context.Context, command contracts.SandboxCleanupCommand) (contracts.SandboxCleanupObservation, error) {
	r.mu.Lock()
	r.command = command
	started, release, result, err := r.started, r.release, r.result, r.err
	r.mu.Unlock()
	if started != nil {
		close(started)
	}
	if release != nil {
		select {
		case <-release:
		case <-ctx.Done():
			return contracts.SandboxCleanupObservation{}, ctx.Err()
		}
	}
	return result, err
}

func cleanupJobFixture(t *testing.T, owner string) contracts.SandboxCleanupJob {
	t.Helper()
	workspace := "cleanup-workspace"
	identity := contracts.SandboxExecutionIdentity{
		SchemaVersion: contracts.SandboxExecutionIdentitySchemaVersion,
		WorkspaceID:   workspace, ToolRunID: "cleanup-tool-run", ToolAttempt: 1, Backend: contracts.SandboxBackendOCI,
		OperationID: "cleanup-operation", OperationOwnerID: "operation-owner", OperationFence: 2,
		AttemptID: "cleanup-attempt", EffectID: "cleanup-effect",
		ToolRequestHash: contracts.HashStrings("tool request"), OperationRequestHash: contracts.HashStrings("operation request"),
		EffectReservationHash: contracts.HashStrings("reservation"), ToolDefinitionHash: contracts.HashStrings("definition"),
		SandboxProfileHash: contracts.HashStrings("profile"), QualificationHash: contracts.HashStrings("qualification"),
	}
	intent := contracts.SandboxCleanupIntent{
		WorkspaceID: workspace, ToolRunID: identity.ToolRunID, ToolAttempt: identity.ToolAttempt,
		DomainLinkID: "cleanup-domain-link", Identity: identity, ResultHash: contracts.HashStrings("result"),
		Actor: contracts.ActorRef{ID: "cleanup-actor", Kind: "service", WorkspaceID: workspace},
	}
	if err := intent.Normalize(); err != nil {
		t.Fatal(err)
	}
	return contracts.SandboxCleanupJob{
		ID: "cleanup-job", Intent: intent, IntentHash: intent.StableHash(), Status: contracts.SandboxCleanupLeased,
		OwnerID: owner, Fence: 4, LeaseUntil: ptrTime(time.Now().UTC().Add(time.Minute)),
	}
}

func cleanupObservationFor(job contracts.SandboxCleanupJob, state string) contracts.SandboxCleanupObservation {
	return contracts.SandboxCleanupObservation{
		WorkspaceID: job.Intent.WorkspaceID, JobID: job.ID, OwnerID: job.OwnerID, Fence: job.Fence,
		IdentityHash: job.Intent.Identity.StableHash(), RequestHash: job.Intent.Identity.ToolRequestHash, State: state,
	}
}

func TestSandboxCleanupWorkerCompletesExactObservation(t *testing.T) {
	job := cleanupJobFixture(t, "cleanup-worker")
	store := &cleanupStoreFixture{jobs: []contracts.SandboxCleanupJob{job}}
	runner := &cleanupRunnerFixture{result: cleanupObservationFor(job, contracts.SandboxCleanupRemoved)}
	worker := &SandboxCleanupWorker{Store: store, Runner: runner, OwnerID: "cleanup-worker", Limit: 1}

	result, err := worker.RunOnce(context.Background(), job.Intent.WorkspaceID)
	if err != nil || result.Claims != 1 || result.Completed != 1 || result.Retried != 0 || result.Failed != 0 {
		t.Fatalf("cleanup batch=%+v err=%v", result, err)
	}
	if len(store.completed) != 1 || len(store.retried) != 0 {
		t.Fatalf("completed=%d retried=%d", len(store.completed), len(store.retried))
	}
	if runner.command.IntentHash != job.IntentHash || runner.command.Intent.Identity.StableHash() != job.Intent.Identity.StableHash() || runner.command.Fence != job.Fence {
		t.Fatalf("runner command did not preserve exact identity/fence: %+v", runner.command)
	}
}

func TestSandboxCleanupWorkerConvertsRunnerFailureToRedactedRetry(t *testing.T) {
	job := cleanupJobFixture(t, "cleanup-worker")
	store := &cleanupStoreFixture{jobs: []contracts.SandboxCleanupJob{job}}
	runner := &cleanupRunnerFixture{err: errors.New("/host/private/path bearer=secret")}
	worker := &SandboxCleanupWorker{Store: store, Runner: runner, OwnerID: "cleanup-worker", Limit: 1}

	result, err := worker.RunOnce(context.Background(), job.Intent.WorkspaceID)
	if err != nil || result.Retried != 1 || result.Completed != 0 || len(store.retried) != 1 {
		t.Fatalf("cleanup batch=%+v err=%v retry=%+v", result, err, store.retried)
	}
	observation := store.retried[0]
	if observation.State != contracts.SandboxCleanupUnknown || observation.FailureCode != "runner_unavailable" ||
		result.Outcomes[0].Err != nil || result.Outcomes[0].State != contracts.SandboxCleanupUnknown {
		t.Fatalf("runner failure was not reduced to stable redacted evidence: %+v", observation)
	}
}

func TestSandboxCleanupWorkerRejectsMismatchedObservationBeforeCompletion(t *testing.T) {
	job := cleanupJobFixture(t, "cleanup-worker")
	bad := cleanupObservationFor(job, contracts.SandboxCleanupAlreadyAbsent)
	bad.IdentityHash = contracts.HashStrings("wrong identity")
	store := &cleanupStoreFixture{jobs: []contracts.SandboxCleanupJob{job}}
	runner := &cleanupRunnerFixture{result: bad}
	worker := &SandboxCleanupWorker{Store: store, Runner: runner, OwnerID: "cleanup-worker", Limit: 1}

	result, err := worker.RunOnce(context.Background(), job.Intent.WorkspaceID)
	if err != nil || result.Retried != 1 || result.Completed != 0 || len(store.completed) != 0 || len(store.retried) != 1 {
		t.Fatalf("mismatched observation result=%+v err=%v", result, err)
	}
	if store.retried[0].State != contracts.SandboxCleanupIdentityMismatch || store.retried[0].FailureCode != "invalid_observation" {
		t.Fatalf("mismatched observation was not safely classified: %+v", store.retried[0])
	}
}

func TestSandboxCleanupWorkerRenewsWhileRunnerCallIsActive(t *testing.T) {
	job := cleanupJobFixture(t, "cleanup-worker")
	store := &cleanupStoreFixture{jobs: []contracts.SandboxCleanupJob{job}}
	runner := &cleanupRunnerFixture{
		started: make(chan struct{}), release: make(chan struct{}), result: cleanupObservationFor(job, contracts.SandboxCleanupAlreadyAbsent),
	}
	worker := &SandboxCleanupWorker{
		Store: store, Runner: runner, OwnerID: "cleanup-worker", Limit: 1,
		LeaseTTL: 300 * time.Millisecond, HeartbeatInterval: 20 * time.Millisecond,
	}
	done := make(chan error, 1)
	go func() {
		result, err := worker.RunOnce(context.Background(), job.Intent.WorkspaceID)
		if err == nil && result.Completed != 1 {
			err = errors.New("cleanup was not completed")
		}
		done <- err
	}()
	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("cleanup runner did not start")
	}
	time.Sleep(70 * time.Millisecond)
	close(runner.release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("cleanup worker did not finish")
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.renews < 1 {
		t.Fatalf("cleanup lease renewals=%d, want at least one", store.renews)
	}
}

func TestSandboxCleanupWorkerStopsWritesAfterLeaseLoss(t *testing.T) {
	job := cleanupJobFixture(t, "cleanup-worker")
	store := &cleanupStoreFixture{jobs: []contracts.SandboxCleanupJob{job}, renewErr: errors.New("stale fence")}
	runner := &cleanupRunnerFixture{started: make(chan struct{}), release: make(chan struct{})}
	worker := &SandboxCleanupWorker{
		Store: store, Runner: runner, OwnerID: "cleanup-worker", Limit: 1,
		LeaseTTL: 300 * time.Millisecond, HeartbeatInterval: 10 * time.Millisecond,
	}
	done := make(chan error, 1)
	go func() {
		_, err := worker.RunOnce(context.Background(), job.Intent.WorkspaceID)
		done <- err
	}()
	select {
	case <-runner.started:
	case <-time.After(time.Second):
		t.Fatal("cleanup runner did not start")
	}
	select {
	case err := <-done:
		if !errors.Is(err, ErrSandboxCleanupLeaseLost) {
			t.Fatalf("lease-loss error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("lease loss did not cancel runner")
	}
	if len(store.completed) != 0 || len(store.retried) != 0 {
		t.Fatalf("stale worker wrote completion=%d retries=%d", len(store.completed), len(store.retried))
	}
}

func ptrTime(value time.Time) *time.Time { return &value }
