// Package operationsupervisor coordinates bounded operation workers across an
// explicit workspace set. It is process-level scheduling policy only: Postgres
// remains the authority for claims, leases, fences, quotas, and recovery.
package operationsupervisor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/omaveda/fornix/internal/operationworker"
)

var (
	ErrNotConfigured = errors.New("operation supervisor is not configured")
	ErrWorkspaceSet  = errors.New("operation supervisor workspace set is invalid")
	ErrConcurrency   = errors.New("operation supervisor concurrency is invalid")
	ErrPollInterval  = errors.New("operation supervisor poll interval is invalid")
)

const (
	DefaultMaxConcurrent = 1
	MaxConcurrent        = 64
	MaxWorkspaces        = 4096
	DefaultPollInterval  = 2 * time.Second
	MaxPollInterval      = 5 * time.Minute
)

// Runner is intentionally smaller than operationworker.Worker. It keeps the
// supervisor testable and allows an adapter host to wrap a worker with its own
// policy without giving this package authority over operation state.
type Runner interface {
	RunOnce(context.Context, string) (operationworker.BatchResult, error)
}

// RunnerFactory creates the runner assigned to one workspace. The factory is
// called at each scheduling step so a host can rotate a worker or refresh
// bounded configuration. It must not return a runner shared across workspace
// identities unless that runner is explicitly safe for concurrent use.
type RunnerFactory func(workspaceID string) (Runner, error)

// WorkspaceResult is one deterministic scheduling outcome. Worker failures
// are retained here for supervision/telemetry; they do not stop unrelated
// workspaces from receiving a turn.
type WorkspaceResult struct {
	WorkspaceID string
	Batch       operationworker.BatchResult
	Err         error
}

// StepResult contains outcomes in the selected round-robin order. Results are
// not durable; operation claims, transitions, and leases are the durable
// records.
type StepResult struct {
	Results []WorkspaceResult
}

// Supervisor schedules an explicit, bounded workspace set. It never scans a
// global workspace catalog, which prevents accidental cross-tenant work and
// makes fairness auditable from configuration.
type Supervisor struct {
	Workspaces    []string
	Factory       RunnerFactory
	MaxConcurrent int
	PollInterval  time.Duration
	OnWorkerError func(WorkspaceResult)

	mu   sync.Mutex
	next int
}

// New validates and de-duplicates the explicit workspace set while preserving
// its configured order. The order is the initial fairness order; later rounds
// rotate from the last selected position.
func New(workspaces []string, factory RunnerFactory) (*Supervisor, error) {
	if factory == nil {
		return nil, ErrNotConfigured
	}
	normalized, err := normalizeWorkspaces(workspaces, false)
	if err != nil {
		return nil, err
	}
	return &Supervisor{Workspaces: normalized, Factory: factory, MaxConcurrent: DefaultMaxConcurrent, PollInterval: DefaultPollInterval}, nil
}

// ReplaceWorkspaces atomically replaces the explicit workspace inventory used
// by subsequent scheduling turns. An empty inventory is allowed so a server
// can stop claiming work while no active workspace is available; Step still
// fails closed if called with that inventory.
func (s *Supervisor) ReplaceWorkspaces(workspaces []string) error {
	if s == nil || s.Factory == nil {
		return ErrNotConfigured
	}
	normalized, err := normalizeWorkspaces(workspaces, true)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.Workspaces = normalized
	if len(normalized) == 0 {
		s.next = 0
	} else {
		s.next %= len(normalized)
	}
	s.mu.Unlock()
	return nil
}

func normalizeWorkspaces(workspaces []string, allowEmpty bool) ([]string, error) {
	seen := make(map[string]struct{}, len(workspaces))
	normalized := make([]string, 0, len(workspaces))
	for _, workspaceID := range workspaces {
		workspaceID = strings.TrimSpace(workspaceID)
		if workspaceID == "" {
			return nil, fmt.Errorf("%w: workspace id is empty", ErrWorkspaceSet)
		}
		if _, exists := seen[workspaceID]; exists {
			continue
		}
		seen[workspaceID] = struct{}{}
		normalized = append(normalized, workspaceID)
	}
	minimum := 1
	if allowEmpty {
		minimum = 0
	}
	if (!allowEmpty && len(normalized) == 0) || len(normalized) > MaxWorkspaces {
		return nil, fmt.Errorf("%w: workspace count must be between %d and %d", ErrWorkspaceSet, minimum, MaxWorkspaces)
	}
	return normalized, nil
}

func (s *Supervisor) concurrency() (int, error) {
	if s == nil || s.Factory == nil {
		return 0, ErrNotConfigured
	}
	value := s.MaxConcurrent
	if value == 0 {
		value = DefaultMaxConcurrent
	}
	if value < 1 || value > MaxConcurrent {
		return 0, fmt.Errorf("%w: got %d, want 1..%d", ErrConcurrency, value, MaxConcurrent)
	}
	return value, nil
}

func (s *Supervisor) selected() ([]string, error) {
	if s == nil {
		return nil, ErrNotConfigured
	}
	concurrency, err := s.concurrency()
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.Workspaces) == 0 || len(s.Workspaces) > MaxWorkspaces {
		return nil, ErrWorkspaceSet
	}
	if concurrency > len(s.Workspaces) {
		concurrency = len(s.Workspaces)
	}
	start := s.next % len(s.Workspaces)
	selected := make([]string, concurrency)
	for index := range selected {
		selected[index] = s.Workspaces[(start+index)%len(s.Workspaces)]
	}
	s.next = (start + concurrency) % len(s.Workspaces)
	return selected, nil
}

// Step runs one fair scheduling round. Adapter/worker errors are returned in
// WorkspaceResult and do not prevent other selected workspaces from running.
// Configuration and context errors are returned directly.
func (s *Supervisor) Step(ctx context.Context) (StepResult, error) {
	if ctx == nil {
		return StepResult{}, ErrNotConfigured
	}
	selected, err := s.selected()
	if err != nil {
		return StepResult{}, err
	}
	results := make([]WorkspaceResult, len(selected))
	var wait sync.WaitGroup
	wait.Add(len(selected))
	for index, workspaceID := range selected {
		index, workspaceID := index, workspaceID
		go func() {
			defer wait.Done()
			result := WorkspaceResult{WorkspaceID: workspaceID}
			runner, factoryErr := s.Factory(workspaceID)
			if factoryErr != nil {
				result.Err = factoryErr
				results[index] = result
				return
			}
			if runner == nil {
				result.Err = fmt.Errorf("workspace %s: %w", workspaceID, ErrNotConfigured)
				results[index] = result
				return
			}
			result.Batch, result.Err = runner.RunOnce(ctx, workspaceID)
			results[index] = result
		}()
	}
	wait.Wait()
	step := StepResult{Results: results}
	if s.OnWorkerError != nil {
		for _, result := range results {
			if result.Err != nil && !errors.Is(result.Err, context.Canceled) && !errors.Is(result.Err, context.DeadlineExceeded) {
				s.OnWorkerError(result)
			}
		}
	}
	if ctx.Err() != nil {
		return step, ctx.Err()
	}
	return step, nil
}

// Run continues fair polling until cancellation. Individual worker failures
// are reported to OnWorkerError and do not starve other workspaces; callers
// should make that callback observable and bounded.
func (s *Supervisor) Run(ctx context.Context) error {
	if _, err := s.concurrency(); err != nil {
		return err
	}
	if len(s.Workspaces) == 0 {
		return ErrWorkspaceSet
	}
	interval := s.PollInterval
	if interval <= 0 {
		interval = DefaultPollInterval
	}
	if interval > MaxPollInterval {
		return fmt.Errorf("%w: poll interval exceeds %s", ErrPollInterval, MaxPollInterval)
	}
	for {
		if _, err := s.Step(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
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
