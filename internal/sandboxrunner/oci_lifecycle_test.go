package sandboxrunner

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

type fakeOCILifecycleEngine struct {
	mu sync.Mutex

	image            LocalImageInspection
	imageErr         error
	imageInspections int

	object             *OCIContainerInspection
	createCount        int
	createAckLoss      bool
	createError        error
	startCount         int
	startError         error
	waitCount          int
	waitError          error
	waitUntilCancel    bool
	lateExitOnWait     bool
	cancelParent       context.CancelFunc
	cancelOnWait       bool
	stopCount          int
	removeCount        int
	removeOptions      []OCIContainerRemoveOptions
	removeError        error
	capture            OCIOutputCapture
	captureError       error
	captureSinkStopped bool
	inspectError       error

	startTime  time.Time
	finishTime time.Time
	exitCode   int
}

func newFakeOCILifecycleEngine() *fakeOCILifecycleEngine {
	return &fakeOCILifecycleEngine{
		image: LocalImageInspection{
			ImageID:  "sha256:" + strings.Repeat("a", 64),
			Platform: contracts.SandboxImagePlatform{OS: "linux", Architecture: "amd64"},
		},
		capture:    OCIOutputCapture{Stdout: []byte("ok"), Complete: true},
		startTime:  time.Date(2026, time.January, 2, 3, 4, 5, 0, time.UTC),
		finishTime: time.Date(2026, time.January, 2, 3, 4, 6, 0, time.UTC),
	}
}

func (e *fakeOCILifecycleEngine) InspectLocalImage(_ context.Context, imageID string) (LocalImageInspection, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.imageInspections++
	if imageID != e.image.ImageID {
		return LocalImageInspection{}, errors.New("untrusted image diagnostic")
	}
	return e.image, e.imageErr
}

func (e *fakeOCILifecycleEngine) InspectContainer(_ context.Context, name string) (OCIContainerInspection, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.inspectError != nil {
		return OCIContainerInspection{}, e.inspectError
	}
	if e.object == nil {
		return OCIContainerInspection{}, nil
	}
	if e.object.Name != name {
		return OCIContainerInspection{}, errors.New("inspection name mismatch")
	}
	return cloneOCIInspection(*e.object), nil
}

func (e *fakeOCILifecycleEngine) CreateContainer(_ context.Context, spec OCIContainerSpec) (OCIEngineObject, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.createCount++
	if e.object != nil {
		return OCIEngineObject{}, errors.New("name already exists")
	}
	e.object = &OCIContainerInspection{
		Exists: true, ID: "runtime-1", Name: spec.Name, State: ociContainerCreated,
		Policy: cloneOCIPolicy(spec.Policy),
	}
	ref := OCIEngineObject{ID: e.object.ID, Name: e.object.Name}
	if e.createAckLoss {
		return OCIEngineObject{}, errors.New("daemon acknowledgement contains sensitive endpoint")
	}
	if e.createError != nil {
		return OCIEngineObject{}, e.createError
	}
	return ref, nil
}

func (e *fakeOCILifecycleEngine) StartContainer(_ context.Context, id string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.startCount++
	if e.object == nil || e.object.ID != id {
		return errors.New("start ID mismatch")
	}
	e.object.State = ociContainerRunning
	e.object.StartedAt = e.startTime
	return e.startError
}

func (e *fakeOCILifecycleEngine) WaitContainer(ctx context.Context, id string) (OCIWaitResult, error) {
	e.mu.Lock()
	e.waitCount++
	if e.object == nil || e.object.ID != id {
		e.mu.Unlock()
		return OCIWaitResult{}, errors.New("wait ID mismatch")
	}
	if e.cancelOnWait && e.cancelParent != nil {
		cancel := e.cancelParent
		e.mu.Unlock()
		cancel()
		return OCIWaitResult{}, ctx.Err()
	}
	if e.waitUntilCancel {
		e.mu.Unlock()
		<-ctx.Done()
		if e.lateExitOnWait {
			deadline, _ := ctx.Deadline()
			e.mu.Lock()
			if e.object != nil && e.object.ID == id {
				e.object.State = ociContainerExited
				e.object.ExitCode = 0
				e.object.FinishedAt = deadline.Add(time.Millisecond)
			}
			e.mu.Unlock()
		}
		return OCIWaitResult{}, ctx.Err()
	}
	if e.waitError != nil {
		err := e.waitError
		e.mu.Unlock()
		return OCIWaitResult{}, err
	}
	e.object.State = ociContainerExited
	e.object.ExitCode = e.exitCode
	e.object.FinishedAt = e.finishTime
	e.mu.Unlock()
	return OCIWaitResult{State: ociContainerExited}, nil
}

func (e *fakeOCILifecycleEngine) CaptureOutput(_ context.Context, id string, sink OCIOutputChunkSink) error {
	e.mu.Lock()
	if e.object == nil || e.object.ID != id || (e.object.State != ociContainerExited && e.object.State != ociContainerCreated) {
		e.mu.Unlock()
		return errors.New("capture object is unavailable")
	}
	capture := e.capture
	capture.Stdout = append([]byte(nil), capture.Stdout...)
	capture.Stderr = append([]byte(nil), capture.Stderr...)
	captureError := e.captureError
	e.mu.Unlock()
	for _, output := range []struct {
		stream OCIOutputStream
		bytes  []byte
	}{
		{stream: OCIOutputStdout, bytes: capture.Stdout},
		{stream: OCIOutputStderr, bytes: capture.Stderr},
	} {
		if len(output.bytes) == 0 {
			continue
		}
		if err := sink(output.stream, output.bytes); err != nil {
			e.mu.Lock()
			e.captureSinkStopped = true
			e.mu.Unlock()
			return err
		}
	}
	if !capture.Complete {
		return errors.New("capture stream incomplete")
	}
	return captureError
}

func (e *fakeOCILifecycleEngine) StopContainer(_ context.Context, id string, grace time.Duration) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.stopCount++
	if grace <= 0 || e.object == nil || e.object.ID != id || e.object.State != ociContainerRunning {
		return errors.New("stop target mismatch")
	}
	e.object.State = ociContainerExited
	e.object.ExitCode = 137
	e.object.FinishedAt = e.finishTime
	return nil
}

func (e *fakeOCILifecycleEngine) RemoveContainer(_ context.Context, id string, options OCIContainerRemoveOptions) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.removeCount++
	e.removeOptions = append(e.removeOptions, options)
	if e.object == nil || e.object.ID != id || (e.object.State != ociContainerExited && e.object.State != ociContainerCreated) {
		return errors.New("remove target mismatch")
	}
	if e.removeError != nil {
		return e.removeError
	}
	e.object = nil
	return nil
}

func cloneOCIInspection(source OCIContainerInspection) OCIContainerInspection {
	source.Policy = cloneOCIPolicy(source.Policy)
	return source
}

func lifecycleFixture(t *testing.T) (*OCILifecycleRuntime, *fakeOCILifecycleEngine, contracts.SandboxRunnerRequest) {
	t.Helper()
	mountCatalog, _, reference, _ := catalogTestMount(t, "workspace-oci")
	definition := catalogTestDefinition(t)
	tools, err := NewToolCatalog([]contracts.ToolDefinition{definition})
	if err != nil {
		t.Fatal(err)
	}
	request := catalogTestRequest(t, definition, reference)
	engine := newFakeOCILifecycleEngine()
	runtime, err := NewOCILifecycleRuntime(engine, tools, mountCatalog)
	if err != nil {
		t.Fatal(err)
	}
	// The production constructor uses BuildOCIContainerPlan and therefore
	// refuses a root runner. Keep these engine-decision unit tests independent
	// of the host account while leaving that production check intact.
	runtime.buildPlan = func(request contracts.SandboxRunnerRequest, catalog *ToolCatalog, mount *ResolvedMount) (OCIContainerPlan, error) {
		return buildOCIContainerPlanForIdentity(request, catalog, mount, 501, 20)
	}
	return runtime, engine, request
}

func lifecyclePlan(t *testing.T, runtime *OCILifecycleRuntime, request contracts.SandboxRunnerRequest) OCIContainerPlan {
	t.Helper()
	mount, err := runtime.mounts.Resolve(request.WorkspaceMount)
	if err != nil {
		t.Fatal(err)
	}
	defer mount.Close()
	plan, err := runtime.buildPlan(request, runtime.tools, mount)
	if err != nil {
		t.Fatal(err)
	}
	return plan
}

func seedLifecycleObject(t *testing.T, runtime *OCILifecycleRuntime, engine *fakeOCILifecycleEngine, request contracts.SandboxRunnerRequest, state OCIContainerState) {
	t.Helper()
	plan := lifecyclePlan(t, runtime, request)
	engine.mu.Lock()
	engine.object = &OCIContainerInspection{
		Exists: true, ID: "runtime-seeded", Name: plan.name, State: state,
		Policy: policyFromPlan(plan),
	}
	if state == ociContainerRunning || state == ociContainerExited {
		engine.object.StartedAt = engine.startTime
	}
	if state == ociContainerExited {
		engine.object.ExitCode = engine.exitCode
		engine.object.FinishedAt = engine.finishTime
	}
	engine.mu.Unlock()
}

func TestOCILifecycleRunIsIdempotentAndStartsCanonicalObjectOnce(t *testing.T) {
	runtime, engine, request := lifecycleFixture(t)
	first, err := runtime.RunAttempt(context.Background(), request)
	if err != nil {
		t.Fatalf("first run: %v", err)
	}
	second, err := runtime.RunAttempt(context.Background(), request)
	if err != nil {
		t.Fatalf("recovered duplicate run: %v", err)
	}
	if !reflect.DeepEqual(first, second) {
		t.Fatalf("duplicate result differs:\nfirst=%#v\nsecond=%#v", first, second)
	}
	if first.Outcome != contracts.SandboxRunnerOutcomeCompleted || string(first.Stdout) != "ok" {
		t.Fatalf("unexpected result: %#v", first)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.createCount != 1 || engine.startCount != 1 || engine.waitCount != 1 || engine.imageInspections != 1 {
		t.Fatalf("create/start/wait/image inspections = %d/%d/%d/%d; want 1/1/1/1", engine.createCount, engine.startCount, engine.waitCount, engine.imageInspections)
	}
}

func TestOCILifecycleTransportRequestIDDoesNotDuplicateSameFencedEffect(t *testing.T) {
	runtime, engine, request := lifecycleFixture(t)
	if _, err := runtime.RunAttempt(context.Background(), request); err != nil {
		t.Fatalf("initial run: %v", err)
	}
	conflict := cloneSandboxRunnerRequest(request)
	conflict.RequestID = "different-request-id"
	response, err := runtime.RunAttempt(context.Background(), conflict)
	if err != nil || response.Outcome != contracts.SandboxRunnerOutcomeCompleted || response.RequestID != conflict.RequestID {
		t.Fatalf("same effect with a new transport request ID was not idempotent: response=%#v err=%v", response, err)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.createCount != 1 || engine.startCount != 1 {
		t.Fatalf("conflicting request caused a new effect: create=%d start=%d", engine.createCount, engine.startCount)
	}
}

func TestOCILifecycleMissingLocalImageNeverCreates(t *testing.T) {
	runtime, engine, request := lifecycleFixture(t)
	engine.imageErr = errors.New("daemon endpoint and image pull details")
	_, err := runtime.RunAttempt(context.Background(), request)
	if !errors.Is(err, ErrSandboxImageUnavailable) || strings.Contains(err.Error(), "endpoint") {
		t.Fatalf("missing local image error=%v", err)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.imageInspections != 1 || engine.createCount != 0 || engine.startCount != 0 {
		t.Fatalf("missing image reached mutation: inspect/create/start=%d/%d/%d", engine.imageInspections, engine.createCount, engine.startCount)
	}
}

func TestOCILifecycleConcurrentDuplicateDeliveryStartsAtMostOnce(t *testing.T) {
	runtime, engine, request := lifecycleFixture(t)
	const deliveries = 8
	var group sync.WaitGroup
	for index := 0; index < deliveries; index++ {
		group.Add(1)
		go func() {
			defer group.Done()
			_, _ = runtime.RunAttempt(context.Background(), request)
		}()
	}
	group.Wait()
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.createCount > deliveries || engine.startCount != 1 {
		t.Fatalf("concurrent duplicate calls created=%d started=%d; want exactly one start", engine.createCount, engine.startCount)
	}
	if engine.object == nil || engine.object.State != ociContainerExited {
		t.Fatalf("concurrent deliveries did not converge to one exited object: %#v", engine.object)
	}
}

func TestOCILifecycleRejectsExistingPolicyDriftBeforeStart(t *testing.T) {
	runtime, engine, request := lifecycleFixture(t)
	seedLifecycleObject(t, runtime, engine, request, ociContainerCreated)
	engine.mu.Lock()
	engine.object.Policy.ReadOnlyRootFS = false
	engine.mu.Unlock()
	_, err := runtime.RunAttempt(context.Background(), request)
	if !errors.Is(err, ErrOCIPolicyMismatch) {
		t.Fatalf("policy drift error=%v, want ErrOCIPolicyMismatch", err)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.startCount != 0 || engine.createCount != 0 {
		t.Fatalf("policy mismatch mutated Engine: create=%d start=%d", engine.createCount, engine.startCount)
	}
}

func TestOCILifecycleRejectsIdentityAndUnexpectedTypedPolicy(t *testing.T) {
	tests := []struct {
		name   string
		change func(*OCIContainerInspection)
		want   error
	}{
		{name: "identity label", change: func(s *OCIContainerInspection) {
			s.Policy.Labels[ociLabelPrefix+"execution-hash"] = strings.Repeat("0", 64)
		}, want: ErrOCIIdentityMismatch},
		{name: "extra mount", change: func(s *OCIContainerInspection) {
			s.Policy.Mounts = append(s.Policy.Mounts, OCIContainerMount{Kind: "bind", Source: "/outside", Target: "/etc", ReadOnly: true})
		}, want: ErrOCIPolicyMismatch},
		{name: "unexpected engine control", change: func(s *OCIContainerInspection) {
			s.Policy.UnexpectedControls = []OCIControlSetting{{Name: "host_pid_namespace", Value: "true"}}
		}, want: ErrOCIPolicyMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, engine, request := lifecycleFixture(t)
			seedLifecycleObject(t, runtime, engine, request, ociContainerCreated)
			engine.mu.Lock()
			test.change(engine.object)
			engine.mu.Unlock()
			_, err := runtime.RunAttempt(context.Background(), request)
			if !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want %v", err, test.want)
			}
			engine.mu.Lock()
			defer engine.mu.Unlock()
			if engine.startCount != 0 {
				t.Fatalf("mismatched object was started %d times", engine.startCount)
			}
		})
	}
}

func TestOCILifecycleAmbiguousCreateRemovesUnstartedObjectBeforeRetry(t *testing.T) {
	runtime, engine, request := lifecycleFixture(t)
	engine.createAckLoss = true
	if _, err := runtime.RunAttempt(context.Background(), request); !errors.Is(err, ErrOCIStateUnknown) {
		t.Fatalf("ambiguous create error=%v, want ErrOCIStateUnknown", err)
	}
	engine.mu.Lock()
	if engine.createCount != 1 || engine.startCount != 0 || engine.removeCount != 1 || engine.object != nil {
		engine.mu.Unlock()
		t.Fatalf("ambiguous never-started object was not removed: create=%d start=%d remove=%d object=%#v", engine.createCount, engine.startCount, engine.removeCount, engine.object)
	}
	engine.createAckLoss = false
	engine.mu.Unlock()
	response, err := runtime.RunAttempt(context.Background(), request)
	if err != nil || response.Outcome != contracts.SandboxRunnerOutcomeCompleted {
		t.Fatalf("retry after safely removing unstarted object response=%#v err=%v", response, err)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.createCount != 2 || engine.startCount != 1 {
		t.Fatalf("retry repeated a tool start: create=%d start=%d", engine.createCount, engine.startCount)
	}
}

func TestOCILifecycleAmbiguousStartNeverStartsAgain(t *testing.T) {
	runtime, engine, request := lifecycleFixture(t)
	engine.startError = errors.New("secret daemon endpoint and argv")
	_, err := runtime.RunAttempt(context.Background(), request)
	if !errors.Is(err, ErrOCIStateUnknown) || strings.Contains(err.Error(), "secret") {
		t.Fatalf("ambiguous start error=%v", err)
	}
	if _, err := runtime.RunAttempt(context.Background(), request); !errors.Is(err, ErrOCIStateUnknown) {
		t.Fatalf("running-object replay error=%v, want unknown", err)
	}
	engine.mu.Lock()
	if engine.startCount != 1 || engine.object == nil || engine.object.State != ociContainerRunning {
		engine.mu.Unlock()
		t.Fatalf("start was retried: count=%d", engine.startCount)
	}
	engine.object.State = ociContainerExited
	engine.object.ExitCode = 0
	engine.object.FinishedAt = engine.finishTime
	engine.startError = nil
	engine.mu.Unlock()
	if response, err := runtime.RunAttempt(context.Background(), request); err != nil || response.Outcome != contracts.SandboxRunnerOutcomeCompleted {
		t.Fatalf("exited recovery response=%#v err=%v", response, err)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.startCount != 1 {
		t.Fatalf("recovered result triggered another start: %d", engine.startCount)
	}
}

func TestOCILifecycleNeverStartsPreexistingCreatedOrRunningObject(t *testing.T) {
	for _, state := range []OCIContainerState{ociContainerCreated, ociContainerRunning} {
		t.Run(string(state), func(t *testing.T) {
			runtime, engine, request := lifecycleFixture(t)
			seedLifecycleObject(t, runtime, engine, request, state)
			_, err := runtime.RunAttempt(context.Background(), request)
			if !errors.Is(err, ErrOCIStateUnknown) {
				t.Fatalf("preexisting %s error=%v, want unknown", state, err)
			}
			engine.mu.Lock()
			defer engine.mu.Unlock()
			if engine.startCount != 0 || engine.createCount != 0 {
				t.Fatalf("preexisting %s object was started or recreated: create=%d start=%d", state, engine.createCount, engine.startCount)
			}
			if state == ociContainerCreated && (engine.removeCount != 1 || engine.object != nil) {
				t.Fatalf("never-started object was not removed: remove=%d object=%#v", engine.removeCount, engine.object)
			}
			if state == ociContainerRunning && (engine.removeCount != 0 || engine.object == nil) {
				t.Fatalf("running object was mutated: remove=%d object=%#v", engine.removeCount, engine.object)
			}
		})
	}
}

func TestOCILifecycleReconcileNeverInventsCompletedResult(t *testing.T) {
	runtime, engine, request := lifecycleFixture(t)
	seedLifecycleObject(t, runtime, engine, request, ociContainerExited)
	observation, err := runtime.ReconcileAttempt(context.Background(), request.Execution)
	if err != nil {
		t.Fatalf("reconcile exited object: %v", err)
	}
	if observation.State != contracts.SandboxAttemptStopped || observation.Result != nil || observation.ResultHash != "" {
		t.Fatalf("identity-only reconcile invented a result: %#v", observation)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.startCount != 0 || engine.removeCount != 0 {
		t.Fatalf("reconcile mutated Engine: start=%d remove=%d", engine.startCount, engine.removeCount)
	}
}

func TestOCILifecycleBoundsOutputAndRejectsIncompleteCapture(t *testing.T) {
	tests := []struct {
		name            string
		capture         OCIOutputCapture
		wantErr         error
		outcome         string
		wantOutputLimit bool
	}{
		{name: "bytes beyond limit", capture: OCIOutputCapture{Stdout: []byte(strings.Repeat("x", 1025)), Complete: true}, outcome: contracts.SandboxRunnerOutcomeFailed, wantOutputLimit: true},
		{name: "incomplete stream", capture: OCIOutputCapture{Stdout: []byte("partial"), Complete: false}, wantErr: ErrOCIOutputIncomplete},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, engine, request := lifecycleFixture(t)
			engine.capture = test.capture
			response, err := runtime.RunAttempt(context.Background(), request)
			if test.wantErr != nil {
				if !errors.Is(err, test.wantErr) {
					t.Fatalf("RunAttempt error=%v, want %v", err, test.wantErr)
				}
				return
			}
			if err != nil || response.Outcome != test.outcome || response.FailureCode != contracts.ToolFailureOutputLimit {
				t.Fatalf("truncated response=%#v err=%v", response, err)
			}
			if test.wantOutputLimit {
				if len(response.Stdout) != request.Profile.MaxStdoutBytes || len(response.Stdout) > contracts.MaxToolOutputBytes {
					t.Fatalf("oversized stream was not capped: bytes=%d limit=%d", len(response.Stdout), request.Profile.MaxStdoutBytes)
				}
				engine.mu.Lock()
				defer engine.mu.Unlock()
				if !engine.captureSinkStopped {
					t.Fatal("engine stream continued after the sink rejected the over-budget chunk")
				}
			}
		})
	}
}

func TestOCILifecycleCancellationStopsOnlyTheVerifiedObject(t *testing.T) {
	runtime, engine, request := lifecycleFixture(t)
	ctx, cancel := context.WithCancel(context.Background())
	engine.cancelParent = cancel
	engine.cancelOnWait = true
	response, err := runtime.RunAttempt(ctx, request)
	cancel()
	if err != nil {
		t.Fatalf("cancelled attempt did not resolve its stop: %v", err)
	}
	if response.Outcome != contracts.SandboxRunnerOutcomeCancelled || response.FailureCode != contracts.ToolFailureCancelled {
		t.Fatalf("cancellation response=%#v", response)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.startCount != 1 || engine.stopCount != 1 || engine.object == nil || engine.object.State != ociContainerExited {
		t.Fatalf("cancellation lifecycle start/stop/state=%d/%d/%v", engine.startCount, engine.stopCount, engine.object)
	}
}

func TestOCILifecycleDeadlineIsBoundedAndStopsAttempt(t *testing.T) {
	runtime, engine, request := lifecycleFixture(t)
	runtime.buildPlan = func(request contracts.SandboxRunnerRequest, catalog *ToolCatalog, mount *ResolvedMount) (OCIContainerPlan, error) {
		plan, err := buildOCIContainerPlanForIdentity(request, catalog, mount, 501, 20)
		if err != nil {
			return OCIContainerPlan{}, err
		}
		plan.timeout = 20 * time.Millisecond
		plan.planHash = plan.calculatedHash()
		return plan, nil
	}
	engine.waitUntilCancel = true
	response, err := runtime.RunAttempt(context.Background(), request)
	if err != nil {
		t.Fatalf("timed out attempt was not stopped: %v", err)
	}
	if response.Outcome != contracts.SandboxRunnerOutcomeTimedOut || response.FailureCode != contracts.ToolFailureTimeout {
		t.Fatalf("deadline response=%#v", response)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.startCount != 1 || engine.stopCount != 1 || engine.object == nil || engine.object.State != ociContainerExited {
		t.Fatalf("deadline lifecycle start/stop/state=%d/%d/%#v", engine.startCount, engine.stopCount, engine.object)
	}
}

func TestOCILifecycleNaturalExitAfterDeadlineIsTimedOut(t *testing.T) {
	runtime, engine, request := lifecycleFixture(t)
	runtime.buildPlan = func(request contracts.SandboxRunnerRequest, catalog *ToolCatalog, mount *ResolvedMount) (OCIContainerPlan, error) {
		plan, err := buildOCIContainerPlanForIdentity(request, catalog, mount, 501, 20)
		if err != nil {
			return OCIContainerPlan{}, err
		}
		plan.timeout = 20 * time.Millisecond
		plan.planHash = plan.calculatedHash()
		return plan, nil
	}
	engine.startTime = time.Now()
	engine.waitUntilCancel = true
	engine.lateExitOnWait = true
	response, err := runtime.RunAttempt(context.Background(), request)
	if err != nil {
		t.Fatalf("late natural exit did not resolve to a bounded timeout: %v", err)
	}
	if response.Outcome != contracts.SandboxRunnerOutcomeTimedOut || response.FailureCode != contracts.ToolFailureTimeout {
		t.Fatalf("post-deadline natural exit was not timed out: %#v", response)
	}
}

func TestOCILifecycleCleanupIsIdentityBoundNonForcedAndIdempotent(t *testing.T) {
	runtime, engine, request := lifecycleFixture(t)
	if _, err := runtime.RunAttempt(context.Background(), request); err != nil {
		t.Fatalf("create exited object: %v", err)
	}
	command := lifecycleCleanupCommand(request)
	first, err := runtime.CleanupAttempt(context.Background(), command)
	if err != nil || first.State != contracts.SandboxCleanupRemoved || first.FailureCode != "" {
		t.Fatalf("first cleanup=%#v err=%v", first, err)
	}
	second, err := runtime.CleanupAttempt(context.Background(), command)
	if err != nil || second.State != contracts.SandboxCleanupAlreadyAbsent {
		t.Fatalf("idempotent cleanup=%#v err=%v", second, err)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.removeCount != 1 || len(engine.removeOptions) != 1 || engine.removeOptions[0].Force || engine.removeOptions[0].RemoveVolumes {
		t.Fatalf("remove call/options violate safe cleanup: count=%d options=%#v", engine.removeCount, engine.removeOptions)
	}
}

func TestOCILifecycleCleanupRemovesVerifiedNeverStartedObject(t *testing.T) {
	runtime, engine, request := lifecycleFixture(t)
	seedLifecycleObject(t, runtime, engine, request, ociContainerCreated)
	observation, err := runtime.CleanupAttempt(context.Background(), lifecycleCleanupCommand(request))
	if err != nil || observation.State != contracts.SandboxCleanupRemoved {
		t.Fatalf("cleanup of verified unstarted object=%#v err=%v", observation, err)
	}
	engine.mu.Lock()
	defer engine.mu.Unlock()
	if engine.removeCount != 1 || engine.object != nil || engine.removeOptions[0].Force || engine.removeOptions[0].RemoveVolumes {
		t.Fatalf("unstarted cleanup options/effect are unsafe: remove=%d object=%#v options=%#v", engine.removeCount, engine.object, engine.removeOptions)
	}
}

func TestOCILifecycleCleanupRetainsMismatchedOrRunningObjects(t *testing.T) {
	tests := []struct {
		name   string
		state  OCIContainerState
		change func(*OCIContainerInspection)
		want   string
	}{
		{name: "mismatched labels", state: ociContainerExited, change: func(s *OCIContainerInspection) {
			s.Policy.Labels[ociLabelPrefix+"tool-request-hash"] = strings.Repeat("0", 64)
		}, want: contracts.SandboxCleanupIdentityMismatch},
		{name: "running object", state: ociContainerRunning, change: func(*OCIContainerInspection) {}, want: contracts.SandboxCleanupUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			runtime, engine, request := lifecycleFixture(t)
			seedLifecycleObject(t, runtime, engine, request, test.state)
			engine.mu.Lock()
			test.change(engine.object)
			engine.mu.Unlock()
			observation, err := runtime.CleanupAttempt(context.Background(), lifecycleCleanupCommand(request))
			if err != nil || observation.State != test.want {
				t.Fatalf("cleanup observation=%#v err=%v", observation, err)
			}
			engine.mu.Lock()
			defer engine.mu.Unlock()
			if engine.removeCount != 0 || engine.object == nil {
				t.Fatalf("unsafe object was removed: remove=%d object=%#v", engine.removeCount, engine.object)
			}
		})
	}
}

func TestOCILifecycleNeverLeaksRawEngineDiagnostics(t *testing.T) {
	runtime, engine, request := lifecycleFixture(t)
	engine.inspectError = errors.New("unix:///private/path argv secret")
	_, err := runtime.RunAttempt(context.Background(), request)
	if err == nil || strings.Contains(err.Error(), "/private/path") || strings.Contains(err.Error(), "argv secret") {
		t.Fatalf("raw Engine diagnostic escaped: %v", err)
	}
}

func TestOCILifecycleSensitiveEngineValuesRedactGenericDiagnostics(t *testing.T) {
	policy := OCIContainerPolicy{
		Executable: "/private/tool", Arguments: []string{"--token", "credential-value"},
		Mounts: []OCIContainerMount{{Kind: "bind", Source: "/private/workspace", Target: "/workspace"}},
	}
	for _, rendered := range []string{fmt.Sprintf("%v", policy), fmt.Sprintf("%#v", policy)} {
		if strings.Contains(rendered, "/private/") || strings.Contains(rendered, "credential-value") {
			t.Fatalf("policy diagnostic leaked sensitive value: %s", rendered)
		}
	}
	if _, err := json.Marshal(policy); !errors.Is(err, ErrOCISensitiveValue) {
		t.Fatalf("policy serialization error=%v, want redaction sentinel", err)
	}
	if _, err := json.Marshal(OCIOutputCapture{Stdout: []byte("credential-value")}); !errors.Is(err, ErrOCISensitiveValue) {
		t.Fatalf("output serialization error=%v, want redaction sentinel", err)
	}
	var logBuffer bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logBuffer, nil))
	logger.Info("engine values", slog.Any("policy", policy), slog.Any("output", OCIOutputCapture{Stdout: []byte("credential-value")}))
	if strings.Contains(logBuffer.String(), "/private/") || strings.Contains(logBuffer.String(), "credential-value") {
		t.Fatalf("structured log leaked sensitive Engine value: %s", logBuffer.String())
	}
}

func lifecycleCleanupCommand(request contracts.SandboxRunnerRequest) contracts.SandboxCleanupCommand {
	intent := contracts.SandboxCleanupIntent{
		WorkspaceID: request.WorkspaceID, ToolRunID: request.Execution.ToolRunID,
		ToolAttempt: request.Execution.ToolAttempt, DomainLinkID: "sandbox-link",
		Identity: request.Execution, ResultHash: contracts.HashStrings("cleanup-result"),
		Actor: contracts.ActorRef{ID: "operator", Kind: "human", WorkspaceID: request.WorkspaceID},
	}
	return contracts.SandboxCleanupCommand{
		SchemaVersion: contracts.SandboxCleanupProtocolVersion, JobID: "cleanup-job", OwnerID: "cleanup-owner",
		Fence: 1, IntentHash: intent.StableHash(), Intent: intent,
	}
}
