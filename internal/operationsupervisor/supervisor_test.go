package operationsupervisor

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/operationworker"
)

type fakeRunner struct {
	mu      *sync.Mutex
	seen    *[]string
	active  *atomic.Int32
	maxSeen *atomic.Int32
	delay   time.Duration
	err     error
}

func (r *fakeRunner) RunOnce(ctx context.Context, workspaceID string) (operationworker.BatchResult, error) {
	current := r.active.Add(1)
	for {
		previous := r.maxSeen.Load()
		if current <= previous || r.maxSeen.CompareAndSwap(previous, current) {
			break
		}
	}
	defer r.active.Add(-1)
	r.mu.Lock()
	*r.seen = append(*r.seen, workspaceID)
	r.mu.Unlock()
	timer := time.NewTimer(r.delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return operationworker.BatchResult{}, ctx.Err()
	case <-timer.C:
	}
	return operationworker.BatchResult{Claims: 1}, r.err
}

func TestSupervisorRoundRobinIsExplicitAndDeterministic(t *testing.T) {
	var seen []string
	var mu sync.Mutex
	var active, maxSeen atomic.Int32
	supervisor, err := New([]string{"workspace-a", "workspace-b", "workspace-a", "workspace-c"}, func(workspaceID string) (Runner, error) {
		return &fakeRunner{mu: &mu, seen: &seen, active: &active, maxSeen: &maxSeen}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for index := 0; index < 4; index++ {
		if _, err := supervisor.Step(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if want := []string{"workspace-a", "workspace-b", "workspace-c", "workspace-a"}; !equalStrings(seen, want) {
		t.Fatalf("round-robin sequence=%v, want %v", seen, want)
	}
	if maxSeen.Load() != 1 {
		t.Fatalf("default concurrency=%d, want one", maxSeen.Load())
	}
}

func TestSupervisorBoundsConcurrentWorkspaceTurnsAndRetainsWorkerErrors(t *testing.T) {
	var seen []string
	var mu sync.Mutex
	var active, maxSeen atomic.Int32
	var reported atomic.Int32
	supervisor, err := New([]string{"a", "b", "c", "d"}, func(workspaceID string) (Runner, error) {
		if workspaceID == "c" {
			return &fakeRunner{mu: &mu, seen: &seen, active: &active, maxSeen: &maxSeen, delay: 10 * time.Millisecond, err: errors.New("adapter unavailable")}, nil
		}
		return &fakeRunner{mu: &mu, seen: &seen, active: &active, maxSeen: &maxSeen, delay: 10 * time.Millisecond}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	supervisor.MaxConcurrent = 2
	supervisor.OnWorkerError = func(result WorkspaceResult) {
		if result.WorkspaceID == "c" && result.Err != nil {
			reported.Add(1)
		}
	}
	result, err := supervisor.Step(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Results) != 2 || result.Results[0].WorkspaceID != "a" || result.Results[1].WorkspaceID != "b" {
		t.Fatalf("first concurrent round=%+v", result.Results)
	}
	result, err = supervisor.Step(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Results) != 2 || result.Results[0].WorkspaceID != "c" || result.Results[1].WorkspaceID != "d" {
		t.Fatalf("second concurrent round=%+v", result.Results)
	}
	if result.Results[0].Err == nil || reported.Load() != 1 {
		t.Fatalf("worker error=%v reported=%d", result.Results[0].Err, reported.Load())
	}
	if maxSeen.Load() > 2 {
		t.Fatalf("concurrency=%d exceeded two", maxSeen.Load())
	}
}

func TestSupervisorCancellationStopsAllTurns(t *testing.T) {
	var seen []string
	var mu sync.Mutex
	var active, maxSeen atomic.Int32
	supervisor, err := New([]string{"a", "b"}, func(string) (Runner, error) {
		return &fakeRunner{mu: &mu, seen: &seen, active: &active, maxSeen: &maxSeen, delay: time.Second}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	supervisor.MaxConcurrent = 2
	ctx, cancel := context.WithCancel(context.Background())
	started := make(chan struct{})
	go func() {
		for {
			mu.Lock()
			if len(seen) == 2 {
				close(started)
				mu.Unlock()
				return
			}
			mu.Unlock()
			time.Sleep(time.Millisecond)
		}
	}()
	resultCh := make(chan error, 1)
	go func() {
		_, runErr := supervisor.Step(ctx)
		resultCh <- runErr
	}()
	select {
	case <-started:
		cancel()
	case <-time.After(time.Second):
		cancel()
		t.Fatal("supervisor did not start both workspace turns")
	}
	if err := <-resultCh; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error=%v, want context canceled", err)
	}
}

func TestSupervisorRejectsUnboundedConfiguration(t *testing.T) {
	if _, err := New([]string{""}, func(string) (Runner, error) { return nil, nil }); !errors.Is(err, ErrWorkspaceSet) {
		t.Fatalf("empty workspace error=%v", err)
	}
	supervisor, err := New([]string{"workspace"}, func(string) (Runner, error) { return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	supervisor.MaxConcurrent = MaxConcurrent + 1
	if _, err := supervisor.Step(context.Background()); !errors.Is(err, ErrConcurrency) {
		t.Fatalf("unbounded concurrency error=%v", err)
	}
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for index := range left {
		if left[index] != right[index] {
			return false
		}
	}
	return true
}
