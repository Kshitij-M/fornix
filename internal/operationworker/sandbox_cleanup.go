package operationworker

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

var (
	ErrSandboxCleanupWorkerNotConfigured = errors.New("sandbox cleanup worker is not configured")
	ErrSandboxCleanupLeaseLost           = errors.New("sandbox cleanup worker lost its lease")
	ErrSandboxCleanupRunnerUnavailable   = errors.New("sandbox cleanup runner is unavailable")
)

const (
	DefaultSandboxCleanupLeaseTTL          = 90 * time.Second
	DefaultSandboxCleanupHeartbeatInterval = 30 * time.Second
	DefaultSandboxCleanupPollInterval      = 5 * time.Second
	DefaultSandboxCleanupAttemptTimeout    = 30 * time.Second
	MaxSandboxCleanupClaimBatch            = 32
	MaxSandboxCleanupAttemptTimeout        = 2 * time.Minute
	MaxSandboxCleanupPollInterval          = 5 * time.Minute
)

// SandboxCleanupStore is the fenced durable queue consumed by this worker.
// Implementations must validate owner, fence, and expiry inside every write.
type SandboxCleanupStore interface {
	Claim(context.Context, string, string, int, time.Duration) ([]contracts.SandboxCleanupJob, error)
	Renew(context.Context, contracts.SandboxCleanupLease, time.Duration) (contracts.SandboxCleanupLease, error)
	Complete(context.Context, contracts.SandboxCleanupLease, contracts.SandboxCleanupObservation) (contracts.SandboxCleanupJob, bool, error)
	Retry(context.Context, contracts.SandboxCleanupLease, contracts.SandboxCleanupObservation) (contracts.SandboxCleanupJob, error)
}

// SandboxCleanupRunner is the authenticated host-runtime boundary. Errors are
// deliberately treated as uncertain and never exposed in durable events.
type SandboxCleanupRunner interface {
	CleanupAttempt(context.Context, contracts.SandboxCleanupCommand) (contracts.SandboxCleanupObservation, error)
}

type SandboxCleanupOutcome struct {
	WorkspaceID string
	JobID       string
	Fence       uint64
	State       string
	Completed   bool
	Retried     bool
	Err         error
}

type SandboxCleanupBatchResult struct {
	Claims    int
	Completed int
	Retried   int
	Failed    int
	Outcomes  []SandboxCleanupOutcome
}

// SandboxCleanupWorker processes only the workspace explicitly supplied by
// its caller. The Postgres queue remains responsible for ownership and for
// refusing stale completion or retry writes.
type SandboxCleanupWorker struct {
	Store             SandboxCleanupStore
	Runner            SandboxCleanupRunner
	OwnerID           string
	Limit             int
	LeaseTTL          time.Duration
	HeartbeatInterval time.Duration
	PollInterval      time.Duration
	AttemptTimeout    time.Duration
}

func (w *SandboxCleanupWorker) configured() bool {
	return w != nil && w.Store != nil && w.Runner != nil && strings.TrimSpace(w.OwnerID) != ""
}

func (w *SandboxCleanupWorker) leaseTTL() time.Duration {
	value := w.LeaseTTL
	if value <= 0 || value > 10*time.Minute {
		return DefaultSandboxCleanupLeaseTTL
	}
	return value
}

func (w *SandboxCleanupWorker) heartbeatInterval(ttl time.Duration) time.Duration {
	value := w.HeartbeatInterval
	if value <= 0 || value >= ttl {
		value = ttl / 3
	}
	if value < time.Millisecond {
		return time.Millisecond
	}
	return value
}

func (w *SandboxCleanupWorker) limit() int {
	value := w.Limit
	if value <= 0 || value > MaxSandboxCleanupClaimBatch {
		return MaxSandboxCleanupClaimBatch
	}
	return value
}

func (w *SandboxCleanupWorker) attemptTimeout() time.Duration {
	value := w.AttemptTimeout
	if value <= 0 || value > MaxSandboxCleanupAttemptTimeout {
		return DefaultSandboxCleanupAttemptTimeout
	}
	return value
}

// RunOnce claims and handles one bounded batch for one workspace. Provider
// failures become stable retry observations; raw runner errors are discarded.
func (w *SandboxCleanupWorker) RunOnce(ctx context.Context, workspaceID string) (SandboxCleanupBatchResult, error) {
	if !w.configured() || ctx == nil {
		return SandboxCleanupBatchResult{}, ErrSandboxCleanupWorkerNotConfigured
	}
	workspaceID = strings.TrimSpace(workspaceID)
	if workspaceID == "" {
		return SandboxCleanupBatchResult{}, fmt.Errorf("%w: workspace_id is required", ErrSandboxCleanupWorkerNotConfigured)
	}
	jobs, err := w.Store.Claim(ctx, workspaceID, strings.TrimSpace(w.OwnerID), w.limit(), w.leaseTTL())
	if err != nil {
		return SandboxCleanupBatchResult{}, fmt.Errorf("claim sandbox cleanup: %w", err)
	}
	result := SandboxCleanupBatchResult{Claims: len(jobs), Outcomes: make([]SandboxCleanupOutcome, 0, len(jobs))}
	var firstErr error
	for _, job := range jobs {
		outcome := w.process(ctx, job)
		result.Outcomes = append(result.Outcomes, outcome)
		if outcome.Completed {
			result.Completed++
		}
		if outcome.Retried {
			result.Retried++
		}
		if outcome.Err != nil {
			result.Failed++
			if firstErr == nil {
				firstErr = outcome.Err
			}
		}
	}
	return result, firstErr
}

func (w *SandboxCleanupWorker) process(parent context.Context, job contracts.SandboxCleanupJob) SandboxCleanupOutcome {
	outcome := SandboxCleanupOutcome{WorkspaceID: job.Intent.WorkspaceID, JobID: job.ID, Fence: job.Fence}
	if job.Status != contracts.SandboxCleanupLeased || job.OwnerID != strings.TrimSpace(w.OwnerID) || job.Fence == 0 || job.LeaseUntil == nil {
		outcome.Err = ErrSandboxCleanupLeaseLost
		return outcome
	}
	lease := contracts.SandboxCleanupLease{WorkspaceID: job.Intent.WorkspaceID, JobID: job.ID, OwnerID: job.OwnerID, Fence: job.Fence, LeaseUntil: *job.LeaseUntil}
	command := contracts.SandboxCleanupCommand{
		SchemaVersion: contracts.SandboxCleanupProtocolVersion, JobID: job.ID, OwnerID: job.OwnerID,
		Fence: job.Fence, IntentHash: job.IntentHash, Intent: job.Intent,
	}
	if command.Normalize() != nil {
		outcome.Err = ErrSandboxCleanupWorkerNotConfigured
		return outcome
	}

	runCtx, cancel := context.WithCancel(parent)
	defer cancel()
	stopHeartbeat := make(chan struct{})
	heartbeatErr := make(chan error, 1)
	var heartbeatWG sync.WaitGroup
	heartbeatWG.Add(1)
	go func() {
		defer heartbeatWG.Done()
		ticker := time.NewTicker(w.heartbeatInterval(w.leaseTTL()))
		defer ticker.Stop()
		for {
			select {
			case <-stopHeartbeat:
				return
			case <-runCtx.Done():
				return
			case <-ticker.C:
				if _, err := w.Store.Renew(runCtx, lease, w.leaseTTL()); err != nil {
					select {
					case heartbeatErr <- err:
					default:
					}
					cancel()
					return
				}
			}
		}
	}()

	callCtx, cancelCall := context.WithTimeout(runCtx, w.attemptTimeout())
	observation, runnerErr := w.Runner.CleanupAttempt(callCtx, command)
	cancelCall()
	close(stopHeartbeat)
	heartbeatWG.Wait()
	select {
	case <-parent.Done():
		outcome.Err = parent.Err()
		return outcome
	default:
	}
	select {
	case err := <-heartbeatErr:
		_ = err // Never publish diagnostics from a possibly secret-bearing adapter.
		outcome.Err = ErrSandboxCleanupLeaseLost
		return outcome
	default:
	}
	if runnerErr != nil {
		observation = cleanupFailureObservation(command, contracts.SandboxCleanupUnknown, "runner_unavailable")
	} else if observation.NormalizeForCommand(command) != nil {
		observation = cleanupFailureObservation(command, contracts.SandboxCleanupIdentityMismatch, "invalid_observation")
	}
	outcome.State = observation.State
	if observation.State == contracts.SandboxCleanupRemoved || observation.State == contracts.SandboxCleanupAlreadyAbsent {
		if _, _, err := w.Store.Complete(parent, lease, observation); err != nil {
			outcome.Err = fmt.Errorf("complete sandbox cleanup: %w", err)
			return outcome
		}
		outcome.Completed = true
		return outcome
	}
	if _, err := w.Store.Retry(parent, lease, observation); err != nil {
		outcome.Err = fmt.Errorf("retry sandbox cleanup: %w", err)
		return outcome
	}
	outcome.Retried = true
	return outcome
}

func cleanupFailureObservation(command contracts.SandboxCleanupCommand, state, failureCode string) contracts.SandboxCleanupObservation {
	return contracts.SandboxCleanupObservation{
		WorkspaceID: command.Intent.WorkspaceID, JobID: command.JobID, OwnerID: command.OwnerID, Fence: command.Fence,
		IdentityHash: command.Intent.Identity.StableHash(), RequestHash: command.Intent.Identity.ToolRequestHash,
		State: state, FailureCode: failureCode,
	}
}

// Run repeatedly scans the explicit workspace with a delay after every pass.
func (w *SandboxCleanupWorker) Run(ctx context.Context, workspaceID string) error {
	if !w.configured() || ctx == nil || strings.TrimSpace(workspaceID) == "" {
		return ErrSandboxCleanupWorkerNotConfigured
	}
	interval := w.PollInterval
	if interval <= 0 || interval > MaxSandboxCleanupPollInterval {
		interval = DefaultSandboxCleanupPollInterval
	}
	for {
		_, err := w.RunOnce(ctx, workspaceID)
		if err != nil && ctx.Err() != nil {
			return ctx.Err()
		}
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
