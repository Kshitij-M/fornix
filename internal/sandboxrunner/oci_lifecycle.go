package sandboxrunner

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

const (
	// OCIContainerStateCreated is an inspected object that has not started.
	OCIContainerStateCreated OCIContainerState = "created"
	// OCIContainerStateRunning is an object whose init process is running.
	OCIContainerStateRunning OCIContainerState = "running"
	// OCIContainerStateExited is an object with a terminal process state.
	OCIContainerStateExited OCIContainerState = "exited"

	ociContainerCreated = OCIContainerStateCreated
	ociContainerRunning = OCIContainerStateRunning
	ociContainerExited  = OCIContainerStateExited

	ociReconcileTimeout = 10 * time.Second
	ociCleanupTimeout   = 10 * time.Second
	ociStopTimeout      = 5 * time.Second
	ociStopGrace        = 2 * time.Second
	ociMaxPolicyEntries = 128
	ociMaxPolicyText    = 4096
	ociAttemptLockCount = 32
)

// OCIContainerState is the finite lifecycle state returned by the Engine.
// Values outside the coordinator's recognized set fail closed.
type OCIContainerState string

var (
	// ErrOCILifecycleRequest reports an invalid request or unavailable trusted
	// catalog entry without disclosing arguments, workspace paths, or secrets.
	ErrOCILifecycleRequest = errors.New("sandbox OCI lifecycle request is invalid")
	// ErrOCIIdentityMismatch reports that the canonical object name, runtime ID,
	// or identity labels do not belong to this exact attempt.
	ErrOCIIdentityMismatch = errors.New("sandbox OCI runtime identity mismatch")
	// ErrOCIPolicyMismatch reports any difference between the inspected typed
	// OCI policy and the policy sealed into the request's plan.
	ErrOCIPolicyMismatch = errors.New("sandbox OCI runtime policy mismatch")
	// ErrOCIStateUnknown reports an ambiguous create, start, wait, or inspect
	// outcome that cannot safely be treated as absence or completion.
	ErrOCIStateUnknown = errors.New("sandbox OCI attempt state is unknown")
	// ErrOCIOutputIncomplete reports output that exceeded its bound or could not
	// be proven completely captured for the exact exited object.
	ErrOCIOutputIncomplete = errors.New("sandbox OCI output capture is incomplete")
	errOCIOutputLimit      = errors.New("sandbox OCI output exceeded its configured limit")
	// ErrOCISensitiveValue reports an attempt to serialize data-bearing local
	// Engine values that may contain paths, argv, labels, or captured output.
	ErrOCISensitiveValue = errors.New("sensitive sandbox OCI data cannot be serialized")
)

// OCIEngine is the narrow local Engine seam used by the lifecycle coordinator.
// It intentionally has no pull, build, tag-resolution, or arbitrary daemon
// option method. CreateContainer must use Policy.ImageID as a local immutable
// ID and fail if unavailable; it must never pull or resolve a tag.
// InspectContainer must return the effective typed policy, not a caller-
// supplied hash; adapters must map unrepresented effective controls into
// OCIContainerPolicy.UnexpectedControls so the coordinator fails closed.
// CaptureOutput must stream chunks to the sink in order, stop when the sink
// returns an error, and must not buffer the full output first. The coordinator
// owns the per-stream retained-byte limits. Every method must honor its context
// and discard, rather than log or return,
// raw paths, argv, output, credentials, endpoints, and SDK diagnostics.
type OCIEngine interface {
	LocalImageInspector
	InspectContainer(context.Context, string) (OCIContainerInspection, error)
	CreateContainer(context.Context, OCIContainerSpec) (OCIEngineObject, error)
	StartContainer(context.Context, string) error
	WaitContainer(context.Context, string) (OCIWaitResult, error)
	CaptureOutput(context.Context, string, OCIOutputChunkSink) error
	StopContainer(context.Context, string, time.Duration) error
	RemoveContainer(context.Context, string, OCIContainerRemoveOptions) error
}

// OCIContainerSpec is the sole create input accepted by OCIEngine. ImageID is
// always a verified full local image ID, and Policy is derived from the sealed
// plan; callers cannot supply additional daemon options.
type OCIContainerSpec struct {
	Name   string
	Policy OCIContainerPolicy
}

// OCIContainerMount is one fully typed mount in the inspected effective
// container policy. Source is an operator-approved host path and must never be
// logged or returned by an Engine adapter.
type OCIContainerMount struct {
	Kind        string
	Source      string
	Target      string
	ReadOnly    bool
	Recursive   bool
	SizeBytes   int64
	Options     []string
	Propagation string
	Consistency string
	CopyData    bool
}

// String returns a redacted diagnostic label for the mount.
func (OCIContainerMount) String() string { return "OCIContainerMount{redacted}" }

// GoString returns a redacted Go-syntax representation of the mount.
func (OCIContainerMount) GoString() string { return "OCIContainerMount{redacted}" }

// LogValue returns only a redaction marker for structured logging.
func (OCIContainerMount) LogValue() slog.Value {
	return ociRedactedLogValue("OCIContainerMount")
}

// MarshalJSON rejects serialization because the mount contains a host path.
func (OCIContainerMount) MarshalJSON() ([]byte, error) { return nil, ErrOCISensitiveValue }

// OCIContainerDevice describes a device mapping reported by an Engine. The
// Fornix offline profile permits none.
type OCIContainerDevice struct {
	HostPath      string
	ContainerPath string
	Permissions   string
}

// String returns a redacted diagnostic label for the device mapping.
func (OCIContainerDevice) String() string { return "OCIContainerDevice{redacted}" }

// GoString returns a redacted Go-syntax representation of the device mapping.
func (OCIContainerDevice) GoString() string { return "OCIContainerDevice{redacted}" }

// LogValue returns only a redaction marker for structured logging.
func (OCIContainerDevice) LogValue() slog.Value {
	return ociRedactedLogValue("OCIContainerDevice")
}

// MarshalJSON rejects serialization because the device mapping contains paths.
func (OCIContainerDevice) MarshalJSON() ([]byte, error) { return nil, ErrOCISensitiveValue }

// OCIControlSetting records an effective OCI/Engine control that is outside
// the explicitly modeled profile. Any non-empty value is rejected. Adapters
// must not silently omit an effective setting merely because this coordinator
// does not use it.
type OCIControlSetting struct {
	Name  string
	Value string
}

// String returns a redacted diagnostic label for the Engine control.
func (OCIControlSetting) String() string { return "OCIControlSetting{redacted}" }

// GoString returns a redacted Go-syntax representation of the Engine control.
func (OCIControlSetting) GoString() string { return "OCIControlSetting{redacted}" }

// LogValue returns only a redaction marker for structured logging.
func (OCIControlSetting) LogValue() slog.Value {
	return ociRedactedLogValue("OCIControlSetting")
}

// MarshalJSON rejects serialization of adapter-specific control details.
func (OCIControlSetting) MarshalJSON() ([]byte, error) { return nil, ErrOCISensitiveValue }

// OCIContainerPolicy is the complete typed policy view used for both create
// requests and live inspection. An Engine adapter must populate all fields
// from effective container configuration and host configuration; unsupported
// non-default options belong in UnexpectedControls and cause rejection.
// Host paths and argv may occur here and must not be logged or serialized.
type OCIContainerPolicy struct {
	ImageID                string
	ImagePlatform          contracts.SandboxImagePlatform
	Executable             string
	Arguments              []string
	Environment            []string
	User                   string
	WorkingDirectory       string
	Labels                 map[string]string
	NetworkMode            string
	Privileged             bool
	ReadOnlyRootFS         bool
	Mounts                 []OCIContainerMount
	DroppedCapabilities    []string
	AddedCapabilities      []string
	SecurityOptions        []string
	NanoCPUs               int64
	MemoryBytes            int64
	MemorySwapBytes        int64
	MemoryReservationBytes int64
	CPUPeriod              int64
	CPUQuota               int64
	CPUSetCPUs             string
	CPUSetMems             string
	PIDsLimit              int64
	OomKillDisable         bool
	OomScoreAdj            int
	CgroupParent           string
	CgroupnsMode           string
	Runtime                string
	PIDMode                string
	IPCMode                string
	UTSMode                string
	UserNamespaceMode      string
	Hostname               string
	Domainname             string
	AutoRemove             bool
	RestartPolicy          string
	Devices                []OCIContainerDevice
	PortBindings           map[string][]string
	ExtraHosts             []string
	DNS                    []string
	Links                  []string
	VolumesFrom            []string
	NetworkAliases         []string
	ShmSizeBytes           int64
	Init                   bool
	InitPath               string
	UnexpectedControls     []OCIControlSetting
}

// String returns a redacted diagnostic label for the effective policy.
func (OCIContainerPolicy) String() string { return "OCIContainerPolicy{redacted}" }

// GoString returns a redacted Go-syntax representation of the effective policy.
func (OCIContainerPolicy) GoString() string { return "OCIContainerPolicy{redacted}" }

// LogValue returns only a redaction marker for structured logging.
func (OCIContainerPolicy) LogValue() slog.Value {
	return ociRedactedLogValue("OCIContainerPolicy")
}

// MarshalJSON rejects serialization because policy may contain argv and paths.
func (OCIContainerPolicy) MarshalJSON() ([]byte, error) { return nil, ErrOCISensitiveValue }

// OCIContainerInspection is one Engine observation for an exact canonical
// name. Exists=false must be a clean absence; Exists=true requires a stable
// opaque ID, exact name, state, labels, and effective policy.
type OCIContainerInspection struct {
	Exists     bool
	ID         string
	Name       string
	State      OCIContainerState
	Policy     OCIContainerPolicy
	ExitCode   int
	StartedAt  time.Time
	FinishedAt time.Time
}

// String returns a redacted diagnostic label for the inspection.
func (OCIContainerInspection) String() string { return "OCIContainerInspection{redacted}" }

// GoString returns a redacted Go-syntax representation of the inspection.
func (OCIContainerInspection) GoString() string { return "OCIContainerInspection{redacted}" }

// LogValue returns only a redaction marker for structured logging.
func (OCIContainerInspection) LogValue() slog.Value {
	return ociRedactedLogValue("OCIContainerInspection")
}

// MarshalJSON rejects serialization of the inspected runtime policy.
func (OCIContainerInspection) MarshalJSON() ([]byte, error) { return nil, ErrOCISensitiveValue }

// OCIEngineObject is the acknowledgement returned by CreateContainer. The
// coordinator still inspects the canonical name and does not trust this value
// as policy evidence.
type OCIEngineObject struct {
	ID   string
	Name string
}

// OCIWaitResult is only a wait notification. The coordinator always inspects
// the object afterward and does not accept this result as completion proof.
type OCIWaitResult struct {
	State OCIContainerState
}

// OCIOutputStream identifies one output channel. Engine adapters must call the
// sink serially and preserve byte order within each channel.
type OCIOutputStream string

const (
	OCIOutputStdout OCIOutputStream = "stdout"
	OCIOutputStderr OCIOutputStream = "stderr"
)

// OCIOutputChunkSink accepts one stream chunk. It returns errOCIOutputLimit
// immediately after retaining the configured per-stream prefix.
type OCIOutputChunkSink func(OCIOutputStream, []byte) error

// OCIOutputCapture contains only coordinator-retained output. Complete means
// the Engine drained both streams to their terminal state. On known overflow,
// the capture contains only the bounded prefix and a truncation flag; it is
// returned as a failed tool result, never success.
type OCIOutputCapture struct {
	Stdout          []byte
	Stderr          []byte
	StdoutTruncated bool
	StderrTruncated bool
	Complete        bool
}

// String returns a redacted diagnostic label for captured process output.
func (OCIOutputCapture) String() string { return "OCIOutputCapture{redacted}" }

// GoString returns a redacted Go-syntax representation of captured output.
func (OCIOutputCapture) GoString() string { return "OCIOutputCapture{redacted}" }

// LogValue returns only a redaction marker for structured logging.
func (OCIOutputCapture) LogValue() slog.Value {
	return ociRedactedLogValue("OCIOutputCapture")
}

// MarshalJSON rejects serialization of raw process output.
func (OCIOutputCapture) MarshalJSON() ([]byte, error) { return nil, ErrOCISensitiveValue }

// OCIContainerRemoveOptions makes the cleanup safety contract explicit. The
// coordinator always sets both Force and RemoveVolumes to false.
type OCIContainerRemoveOptions struct {
	Force         bool
	RemoveVolumes bool
}

// OCILifecycleRuntime coordinates one canonical OCI object per durable Fornix
// attempt. It owns deterministic Engine decisions only; it does not own
// authorization, durable fences, result persistence, or OS isolation proof.
// Its dependencies are immutable catalogs and one local Engine adapter.
type OCILifecycleRuntime struct {
	engine    OCIEngine
	tools     *ToolCatalog
	mounts    *MountCatalog
	buildPlan func(contracts.SandboxRunnerRequest, *ToolCatalog, *ResolvedMount) (OCIContainerPlan, error)
	locks     [ociAttemptLockCount]chan struct{}
}

var _ Runtime = (*OCILifecycleRuntime)(nil)

// NewOCILifecycleRuntime constructs a coordinator over an injected Engine and
// the runner's operator-owned tool and workspace catalogs. It holds only
// bounded in-process lock stripes; the Engine is not permitted to pull images
// or accept caller-defined policy.
func NewOCILifecycleRuntime(engine OCIEngine, tools *ToolCatalog, mounts *MountCatalog) (*OCILifecycleRuntime, error) {
	if engine == nil || tools == nil || mounts == nil {
		return nil, ErrOCILifecycleRequest
	}
	runtime := &OCILifecycleRuntime{
		engine: engine, tools: tools, mounts: mounts, buildPlan: BuildOCIContainerPlan,
	}
	for index := range runtime.locks {
		runtime.locks[index] = make(chan struct{}, 1)
		runtime.locks[index] <- struct{}{}
	}
	return runtime, nil
}

// RunAttempt creates and starts at most one exact object for request. Existing
// objects are never started; they are inspected and, only when exited with
// complete bounded output, may be recovered as a result. An ambiguous mutation
// is inspected and never blindly repeated.
func (r *OCILifecycleRuntime) RunAttempt(ctx context.Context, request contracts.SandboxRunnerRequest) (contracts.SandboxRunnerResponse, error) {
	if ctx == nil || r == nil || r.engine == nil || r.tools == nil || r.mounts == nil || r.buildPlan == nil {
		return contracts.SandboxRunnerResponse{}, ErrOCILifecycleRequest
	}
	request = cloneSandboxRunnerRequest(request)
	if err := request.Normalize(); err != nil {
		return contracts.SandboxRunnerResponse{}, ErrOCILifecycleRequest
	}
	unlock, err := r.lockAttempt(ctx, request.Execution)
	if err != nil {
		return contracts.SandboxRunnerResponse{}, err
	}
	defer unlock()
	mount, err := r.mounts.Resolve(request.WorkspaceMount)
	if err != nil {
		return contracts.SandboxRunnerResponse{}, ErrOCILifecycleRequest
	}
	defer mount.Close()
	plan, err := r.buildPlan(request, r.tools, mount)
	if err != nil {
		return contracts.SandboxRunnerResponse{}, ErrOCILifecycleRequest
	}
	planSnapshot, err := plan.engineSnapshot()
	if err != nil {
		return contracts.SandboxRunnerResponse{}, ErrOCILifecycleRequest
	}
	runCtx, cancel := context.WithTimeout(ctx, planSnapshot.timeout)
	defer cancel()

	initial, err := r.inspect(runCtx, planSnapshot.name)
	if err != nil {
		if runCtx.Err() != nil {
			return contracts.SandboxRunnerResponse{}, runCtx.Err()
		}
		return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
	}
	if initial.Exists {
		if err := verifyOCIInspection(planSnapshot, initial); err != nil {
			return contracts.SandboxRunnerResponse{}, err
		}
		if initial.State == ociContainerExited {
			return r.recoverExited(runCtx, request, planSnapshot, initial)
		}
		if initial.State == ociContainerCreated {
			// A verified never-started object has no tool effects to recover. Remove
			// it without force or volume deletion so a later durable retry is not
			// permanently blocked by a lost create acknowledgement.
			if err := r.removeCreatedContainer(runCtx, planSnapshot, initial); err != nil {
				return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
			}
			return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
		}
		// A running object may have been left by an interrupted call. It is never
		// restarted, and this identity-only path does not wait on it.
		return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
	}
	if err := planSnapshot.VerifyLocalImage(runCtx, r.engine); err != nil {
		if runCtx.Err() != nil {
			return contracts.SandboxRunnerResponse{}, runCtx.Err()
		}
		return contracts.SandboxRunnerResponse{}, err
	}
	if err := runCtx.Err(); err != nil {
		return contracts.SandboxRunnerResponse{}, err
	}
	if err := planSnapshot.Validate(); err != nil || mount.VerifyCurrent() != nil {
		return contracts.SandboxRunnerResponse{}, ErrOCILifecycleRequest
	}
	spec := OCIContainerSpec{Name: planSnapshot.name, Policy: policyFromPlan(planSnapshot)}
	created, createErr := r.engine.CreateContainer(runCtx, cloneOCISpec(spec))
	if createErr != nil {
		return r.resolveAmbiguousMutation(request, planSnapshot, runCtx, false)
	}
	if !validEngineID(created.ID) || created.Name != planSnapshot.name {
		return r.resolveAmbiguousMutation(request, planSnapshot, runCtx, false)
	}
	if runCtx.Err() != nil {
		return r.resolveAmbiguousMutation(request, planSnapshot, runCtx, false)
	}
	createdInspection, err := r.inspect(runCtx, planSnapshot.name)
	if err != nil {
		if runCtx.Err() != nil {
			return contracts.SandboxRunnerResponse{}, runCtx.Err()
		}
		return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
	}
	if err := verifyOCIInspection(planSnapshot, createdInspection); err != nil {
		return contracts.SandboxRunnerResponse{}, err
	}
	if createdInspection.ID != created.ID || createdInspection.Name != created.Name {
		return contracts.SandboxRunnerResponse{}, ErrOCIIdentityMismatch
	}
	if createdInspection.State == ociContainerExited {
		return r.recoverExited(runCtx, request, planSnapshot, createdInspection)
	}
	if createdInspection.State != ociContainerCreated {
		return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
	}
	if err := runCtx.Err(); err != nil {
		return contracts.SandboxRunnerResponse{}, err
	}
	if err := planSnapshot.Validate(); err != nil {
		return contracts.SandboxRunnerResponse{}, ErrOCILifecycleRequest
	}
	startErr := r.engine.StartContainer(runCtx, createdInspection.ID)
	if startErr != nil {
		if runCtx.Err() != nil {
			deadline, _ := runCtx.Deadline()
			return r.finishCancellation(request, planSnapshot, runCtx.Err(), deadline)
		}
		return r.resolveAmbiguousMutation(request, planSnapshot, runCtx, true)
	}
	_, waitErr := r.engine.WaitContainer(runCtx, createdInspection.ID)
	if waitErr != nil {
		if runCtx.Err() != nil {
			deadline, _ := runCtx.Deadline()
			return r.finishCancellation(request, planSnapshot, runCtx.Err(), deadline)
		}
		return r.resolveAmbiguousMutation(request, planSnapshot, runCtx, true)
	}
	if runCtx.Err() != nil {
		deadline, _ := runCtx.Deadline()
		return r.finishCancellation(request, planSnapshot, runCtx.Err(), deadline)
	}
	finished, err := r.inspect(runCtx, planSnapshot.name)
	if err != nil {
		if runCtx.Err() != nil {
			deadline, _ := runCtx.Deadline()
			return r.finishCancellation(request, planSnapshot, runCtx.Err(), deadline)
		}
		return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
	}
	if err := verifyOCIInspection(planSnapshot, finished); err != nil {
		return contracts.SandboxRunnerResponse{}, err
	}
	if finished.ID != createdInspection.ID || finished.State != ociContainerExited {
		return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
	}
	return r.recoverExited(runCtx, request, planSnapshot, finished)
}

// ReconcileAttempt performs inspection only. Since the protocol supplies no
// original request or output budget, an exited object is reported as stopped,
// never as a fabricated completed ToolResult.
func (r *OCILifecycleRuntime) ReconcileAttempt(ctx context.Context, identity contracts.SandboxExecutionIdentity) (contracts.SandboxAttemptObservation, error) {
	if ctx == nil || r == nil || r.engine == nil {
		return contracts.SandboxAttemptObservation{}, ErrOCILifecycleRequest
	}
	if err := identity.Normalize(); err != nil || identity.Backend == contracts.SandboxBackendLocalProcess {
		return contracts.SandboxAttemptObservation{}, ErrOCILifecycleRequest
	}
	observeCtx, cancel := context.WithTimeout(ctx, ociReconcileTimeout)
	defer cancel()
	unlock, err := r.lockAttempt(observeCtx, identity)
	if err != nil {
		return contracts.SandboxAttemptObservation{}, err
	}
	defer unlock()
	snapshot, err := r.inspect(observeCtx, identity.RuntimeName())
	if err != nil {
		if observeCtx.Err() != nil {
			return contracts.SandboxAttemptObservation{}, observeCtx.Err()
		}
		return contracts.SandboxAttemptObservation{}, ErrOCIStateUnknown
	}
	observation := contracts.SandboxAttemptObservation{IdentityHash: identity.StableHash(), State: contracts.SandboxAttemptUnknown}
	if !snapshot.Exists {
		observation.State = contracts.SandboxAttemptAbsent
		return observation, observation.Normalize(identity)
	}
	if !validEngineID(snapshot.ID) || snapshot.Name != identity.RuntimeName() || !identityLabelsMatch(snapshot.Policy.Labels, identity) {
		return observation, observation.Normalize(identity)
	}
	observation.RuntimeID = snapshot.ID
	switch snapshot.State {
	case ociContainerRunning:
		observation.State = contracts.SandboxAttemptRunning
	case ociContainerExited:
		observation.State = contracts.SandboxAttemptStopped
	default:
		observation.State = contracts.SandboxAttemptUnknown
	}
	if err := observation.Normalize(identity); err != nil {
		return contracts.SandboxAttemptObservation{}, ErrOCIStateUnknown
	}
	return observation, nil
}

// CleanupAttempt removes only a never-started created object or an exited
// object whose canonical name and full identity-label set match the durable
// cleanup intent. Removal is never forced and never removes anonymous volumes;
// repeated removal of an absent object is idempotent success.
func (r *OCILifecycleRuntime) CleanupAttempt(ctx context.Context, command contracts.SandboxCleanupCommand) (contracts.SandboxCleanupObservation, error) {
	if ctx == nil || r == nil || r.engine == nil {
		return contracts.SandboxCleanupObservation{}, ErrOCILifecycleRequest
	}
	if err := command.Normalize(); err != nil {
		return contracts.SandboxCleanupObservation{}, ErrOCILifecycleRequest
	}
	cleanupCtx, cancel := context.WithTimeout(ctx, ociCleanupTimeout)
	defer cancel()
	identity := command.Intent.Identity
	unlock, err := r.lockAttempt(cleanupCtx, identity)
	if err != nil {
		return contracts.SandboxCleanupObservation{}, err
	}
	defer unlock()
	base := contracts.SandboxCleanupObservation{
		WorkspaceID: command.Intent.WorkspaceID, JobID: command.JobID, OwnerID: command.OwnerID,
		Fence: command.Fence, IdentityHash: identity.StableHash(), RequestHash: identity.ToolRequestHash,
	}
	unknown := func(code string) contracts.SandboxCleanupObservation {
		result := base
		result.State, result.FailureCode = contracts.SandboxCleanupUnknown, code
		return result
	}
	snapshot, err := r.inspect(cleanupCtx, identity.RuntimeName())
	if err != nil {
		if cleanupCtx.Err() != nil {
			return unknown("engine_timeout"), nil
		}
		return unknown("engine_unavailable"), nil
	}
	if !snapshot.Exists {
		base.State = contracts.SandboxCleanupAlreadyAbsent
		return base, nil
	}
	if !validEngineID(snapshot.ID) || snapshot.Name != identity.RuntimeName() || !identityLabelsMatch(snapshot.Policy.Labels, identity) {
		mismatch := base
		mismatch.State, mismatch.FailureCode = contracts.SandboxCleanupIdentityMismatch, "identity_mismatch"
		return mismatch, nil
	}
	if snapshot.State != ociContainerExited && snapshot.State != ociContainerCreated {
		return unknown("not_stopped"), nil
	}
	removeOptions := OCIContainerRemoveOptions{Force: false, RemoveVolumes: false}
	removeErr := r.engine.RemoveContainer(cleanupCtx, snapshot.ID, removeOptions)
	if cleanupCtx.Err() != nil {
		return unknown("engine_timeout"), nil
	}
	// A remove acknowledgement can be lost. Inspect the same canonical name;
	// absence proves the idempotent cleanup outcome, while anything else stays
	// unknown or mismatched and is never removed a second time here.
	after, inspectErr := r.inspect(cleanupCtx, identity.RuntimeName())
	if inspectErr != nil {
		if removeErr != nil {
			return unknown("engine_unavailable"), nil
		}
		return unknown("verification_failed"), nil
	}
	if !after.Exists {
		base.State = contracts.SandboxCleanupRemoved
		return base, nil
	}
	if !validEngineID(after.ID) || after.Name != identity.RuntimeName() || after.ID != snapshot.ID || !identityLabelsMatch(after.Policy.Labels, identity) {
		mismatch := base
		mismatch.State, mismatch.FailureCode = contracts.SandboxCleanupIdentityMismatch, "identity_mismatch"
		return mismatch, nil
	}
	if removeErr != nil {
		return unknown("remove_unconfirmed"), nil
	}
	return unknown("remove_unconfirmed"), nil
}

func (r *OCILifecycleRuntime) inspect(ctx context.Context, name string) (OCIContainerInspection, error) {
	if ctx == nil || name == "" {
		return OCIContainerInspection{}, ErrOCIStateUnknown
	}
	snapshot, err := r.engine.InspectContainer(ctx, name)
	if ctx.Err() != nil {
		return OCIContainerInspection{}, ctx.Err()
	}
	if err != nil {
		return OCIContainerInspection{}, ErrOCIStateUnknown
	}
	if !snapshot.Exists {
		policy, ok := canonicalOCIPolicy(snapshot.Policy)
		if snapshot.ID != "" || snapshot.Name != "" || snapshot.State != "" || !ok || !reflect.DeepEqual(policy, OCIContainerPolicy{}) || snapshot.ExitCode != 0 || !snapshot.StartedAt.IsZero() || !snapshot.FinishedAt.IsZero() {
			return OCIContainerInspection{}, ErrOCIStateUnknown
		}
		return snapshot, nil
	}
	return snapshot, nil
}

func (r *OCILifecycleRuntime) lockAttempt(ctx context.Context, identity contracts.SandboxExecutionIdentity) (func(), error) {
	if ctx == nil || r == nil || identity.Normalize() != nil {
		return nil, ErrOCILifecycleRequest
	}
	hash := identity.StableHash()
	if len(hash) < 2 {
		return nil, ErrOCILifecycleRequest
	}
	index := ((int(hash[0]) << 8) | int(hash[1])) % len(r.locks)
	lock := r.locks[index]
	if lock == nil {
		return nil, ErrOCILifecycleRequest
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-lock:
		return func() { lock <- struct{}{} }, nil
	}
}

func (r *OCILifecycleRuntime) resolveAmbiguousMutation(request contracts.SandboxRunnerRequest, plan OCIContainerPlan, operationCtx context.Context, started bool) (contracts.SandboxRunnerResponse, error) {
	recoveryCtx, cancel := context.WithTimeout(context.Background(), ociReconcileTimeout)
	defer cancel()
	snapshot, err := r.inspect(recoveryCtx, plan.name)
	if err != nil || !snapshot.Exists {
		return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
	}
	if err := verifyOCIInspection(plan, snapshot); err != nil {
		return contracts.SandboxRunnerResponse{}, err
	}
	if snapshot.State == ociContainerCreated {
		if err := r.removeCreatedContainer(recoveryCtx, plan, snapshot); err != nil {
			return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
		}
		return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
	}
	if snapshot.State == ociContainerExited {
		if operationCtx != nil && operationCtx.Err() != nil {
			deadline, _ := operationCtx.Deadline()
			return r.finishCancellation(request, plan, operationCtx.Err(), deadline)
		}
		return r.recoverExited(recoveryCtx, request, plan, snapshot)
	}
	if started && operationCtx != nil && operationCtx.Err() != nil && snapshot.State == ociContainerRunning {
		deadline, _ := operationCtx.Deadline()
		return r.finishCancellation(request, plan, operationCtx.Err(), deadline)
	}
	return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
}

func (r *OCILifecycleRuntime) removeCreatedContainer(ctx context.Context, plan OCIContainerPlan, snapshot OCIContainerInspection) error {
	if ctx == nil || snapshot.State != ociContainerCreated || verifyOCIInspection(plan, snapshot) != nil {
		return ErrOCIStateUnknown
	}
	removeErr := r.engine.RemoveContainer(ctx, snapshot.ID, OCIContainerRemoveOptions{Force: false, RemoveVolumes: false})
	if ctx.Err() != nil {
		return ErrOCIStateUnknown
	}
	after, inspectErr := r.inspect(ctx, plan.name)
	if inspectErr != nil {
		return ErrOCIStateUnknown
	}
	if !after.Exists {
		// Absence is the idempotent proof even if the remove acknowledgement was
		// lost. A remaining object is never removed a second time in this call.
		return nil
	}
	if after.ID != snapshot.ID || after.State != ociContainerCreated || verifyOCIInspection(plan, after) != nil {
		return ErrOCIIdentityMismatch
	}
	if removeErr != nil {
		return ErrOCIStateUnknown
	}
	return ErrOCIStateUnknown
}

func (r *OCILifecycleRuntime) captureOutput(ctx context.Context, id string, stdoutLimit, stderrLimit int) (OCIOutputCapture, error) {
	if ctx == nil || r == nil || r.engine == nil || id == "" ||
		stdoutLimit < 1 || stdoutLimit > contracts.MaxToolOutputBytes ||
		stderrLimit < 1 || stderrLimit > contracts.MaxToolOutputBytes {
		return OCIOutputCapture{}, ErrOCIOutputIncomplete
	}
	var stdout, stderr bytes.Buffer
	stdoutTruncated, stderrTruncated := false, false
	var callbackErr error
	sink := func(stream OCIOutputStream, chunk []byte) error {
		if callbackErr != nil {
			return callbackErr
		}
		var target *bytes.Buffer
		var limit int
		var truncated *bool
		switch stream {
		case OCIOutputStdout:
			target, limit, truncated = &stdout, stdoutLimit, &stdoutTruncated
		case OCIOutputStderr:
			target, limit, truncated = &stderr, stderrLimit, &stderrTruncated
		default:
			callbackErr = ErrOCIOutputIncomplete
			return callbackErr
		}
		if len(chunk) == 0 {
			return nil
		}
		remaining := limit - target.Len()
		if remaining < 0 {
			callbackErr = ErrOCIOutputIncomplete
			return callbackErr
		}
		if len(chunk) > remaining {
			_, _ = target.Write(chunk[:remaining])
			*truncated = true
			callbackErr = errOCIOutputLimit
			return callbackErr
		}
		_, _ = target.Write(chunk)
		return nil
	}
	streamErr := r.engine.CaptureOutput(ctx, id, sink)
	capture := OCIOutputCapture{
		Stdout: stdout.Bytes(), Stderr: stderr.Bytes(),
		StdoutTruncated: stdoutTruncated, StderrTruncated: stderrTruncated,
	}
	if stdoutTruncated || stderrTruncated {
		return capture, errOCIOutputLimit
	}
	if callbackErr != nil || streamErr != nil {
		return OCIOutputCapture{}, ErrOCIOutputIncomplete
	}
	capture.Complete = true
	return capture, nil
}

func (r *OCILifecycleRuntime) recoverExited(ctx context.Context, request contracts.SandboxRunnerRequest, plan OCIContainerPlan, before OCIContainerInspection) (contracts.SandboxRunnerResponse, error) {
	if err := verifyOCIInspection(plan, before); err != nil {
		return contracts.SandboxRunnerResponse{}, err
	}
	if before.State != ociContainerExited || !validExitFacts(plan, before) {
		return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
	}
	capture, err := r.captureOutput(ctx, before.ID, plan.maxStdoutBytes, plan.maxStderrBytes)
	if ctx.Err() != nil {
		return contracts.SandboxRunnerResponse{}, ctx.Err()
	}
	outputLimit := errors.Is(err, errOCIOutputLimit)
	if (err != nil && !outputLimit) || (!capture.Complete && !outputLimit) {
		return contracts.SandboxRunnerResponse{}, ErrOCIOutputIncomplete
	}
	after, err := r.inspect(ctx, plan.name)
	if err != nil {
		return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
	}
	if err := verifyOCIInspection(plan, after); err != nil {
		return contracts.SandboxRunnerResponse{}, err
	}
	if after.ID != before.ID || after.State != ociContainerExited || after.ExitCode != before.ExitCode || !after.StartedAt.Equal(before.StartedAt) || !after.FinishedAt.Equal(before.FinishedAt) {
		return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
	}
	response := responseForExited(request, after, capture)
	if err := response.NormalizeFor(request); err != nil {
		return contracts.SandboxRunnerResponse{}, ErrOCIOutputIncomplete
	}
	return response, nil
}

func (r *OCILifecycleRuntime) finishCancellation(request contracts.SandboxRunnerRequest, plan OCIContainerPlan, cause error, deadline time.Time) (contracts.SandboxRunnerResponse, error) {
	cleanupCtx, cancel := context.WithTimeout(context.Background(), ociStopTimeout)
	defer cancel()
	snapshot, err := r.inspect(cleanupCtx, plan.name)
	if err != nil || !snapshot.Exists {
		return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
	}
	if err := verifyOCIInspection(plan, snapshot); err != nil {
		return contracts.SandboxRunnerResponse{}, err
	}
	if snapshot.State == ociContainerExited {
		// An exit observed after the actual context deadline is a timeout, not a
		// successful recovery. An earlier natural exit can be recovered even if
		// the waiter observed cancellation a little later.
		if !errors.Is(cause, context.DeadlineExceeded) || deadline.IsZero() || !snapshot.FinishedAt.After(deadline) {
			return r.recoverExited(cleanupCtx, request, plan, snapshot)
		}
	} else if snapshot.State == ociContainerRunning {
		if err := r.engine.StopContainer(cleanupCtx, snapshot.ID, ociStopGrace); err != nil && cleanupCtx.Err() != nil {
			return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
		}
	} else {
		return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
	}
	after := snapshot
	if snapshot.State == ociContainerRunning {
		var err error
		after, err = r.inspect(cleanupCtx, plan.name)
		if err != nil || !after.Exists || verifyOCIInspection(plan, after) != nil || after.ID != snapshot.ID || after.State != ociContainerExited {
			return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
		}
	}
	if !validExitFacts(plan, after) {
		return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
	}
	// Capture remains bounded after stop. Cancellation/timeout is the outcome;
	// partial or truncated output is omitted instead of represented as complete.
	capture, captureErr := r.captureOutput(cleanupCtx, after.ID, plan.maxStdoutBytes, plan.maxStderrBytes)
	if captureErr != nil || cleanupCtx.Err() != nil || !capture.Complete || capture.StdoutTruncated || capture.StderrTruncated || len(capture.Stdout) > plan.maxStdoutBytes || len(capture.Stderr) > plan.maxStderrBytes {
		capture = OCIOutputCapture{Complete: true}
	}
	outcome, failure := contracts.SandboxRunnerOutcomeCancelled, contracts.ToolFailureCancelled
	if errors.Is(cause, context.DeadlineExceeded) {
		outcome, failure = contracts.SandboxRunnerOutcomeTimedOut, contracts.ToolFailureTimeout
	}
	response := contracts.SandboxRunnerResponse{
		SchemaVersion: contracts.SandboxRunnerProtocolVersion,
		RequestID:     request.RequestID, ExecutionHash: request.Execution.StableHash(), RequestHash: request.StableHash(),
		Outcome: outcome, ExitCode: after.ExitCode, Stdout: capture.Stdout, Stderr: capture.Stderr,
		FailureCode: failure, StartedAt: after.StartedAt.UTC(), FinishedAt: after.FinishedAt.UTC(),
	}
	if err := response.NormalizeFor(request); err != nil {
		return contracts.SandboxRunnerResponse{}, ErrOCIStateUnknown
	}
	return response, nil
}

func responseForExited(request contracts.SandboxRunnerRequest, inspection OCIContainerInspection, capture OCIOutputCapture) contracts.SandboxRunnerResponse {
	response := contracts.SandboxRunnerResponse{
		SchemaVersion: contracts.SandboxRunnerProtocolVersion,
		RequestID:     request.RequestID, ExecutionHash: request.Execution.StableHash(), RequestHash: request.StableHash(),
		Outcome:  contracts.SandboxRunnerOutcomeCompleted,
		ExitCode: inspection.ExitCode,
		Stdout:   capture.Stdout, Stderr: capture.Stderr,
		StartedAt: inspection.StartedAt.UTC(), FinishedAt: inspection.FinishedAt.UTC(),
	}
	if capture.StdoutTruncated || capture.StderrTruncated {
		response.Outcome = contracts.SandboxRunnerOutcomeFailed
		response.FailureCode = contracts.ToolFailureOutputLimit
	} else if inspection.ExitCode != 0 {
		response.Outcome = contracts.SandboxRunnerOutcomeFailed
		response.FailureCode = contracts.ToolFailureExecution
	}
	return response
}

func validExitFacts(plan OCIContainerPlan, inspection OCIContainerInspection) bool {
	if inspection.ExitCode < 0 || inspection.ExitCode > 255 || inspection.StartedAt.IsZero() || inspection.FinishedAt.IsZero() || inspection.FinishedAt.Before(inspection.StartedAt) {
		return false
	}
	return inspection.FinishedAt.Sub(inspection.StartedAt) <= plan.timeout+30*time.Second
}

func verifyOCIInspection(plan OCIContainerPlan, snapshot OCIContainerInspection) error {
	if !snapshot.Exists || !validEngineID(snapshot.ID) || snapshot.Name != plan.name || !reflect.DeepEqual(snapshot.Policy.Labels, plan.expectedLabels) {
		return ErrOCIIdentityMismatch
	}
	if !ociPolicyEqual(policyFromPlan(plan), snapshot.Policy) {
		return ErrOCIPolicyMismatch
	}
	switch snapshot.State {
	case ociContainerCreated, ociContainerRunning, ociContainerExited:
		return nil
	default:
		return ErrOCIStateUnknown
	}
}
func policyFromPlan(plan OCIContainerPlan) OCIContainerPolicy {
	return OCIContainerPolicy{
		ImageID: plan.imageDigest, ImagePlatform: plan.imagePlatform,
		Executable: plan.executable, Arguments: append([]string(nil), plan.arguments...),
		Environment: append([]string(nil), plan.environment...), User: plan.containerUser,
		WorkingDirectory: plan.workingDirectory, Labels: cloneStringMap(plan.labels),
		NetworkMode: plan.networkMode, Privileged: plan.privileged, ReadOnlyRootFS: plan.readOnlyRootFS,
		Mounts: []OCIContainerMount{
			{Kind: "bind", Source: plan.workspaceSource, Target: plan.workspaceTarget, ReadOnly: plan.workspaceReadOnly, Recursive: plan.workspaceRecursive},
			{Kind: "tmpfs", Target: plan.scratchTarget, SizeBytes: plan.scratchSizeBytes, Options: []string{plan.scratchOptions}},
		},
		DroppedCapabilities: append([]string(nil), plan.droppedCapabilities...),
		AddedCapabilities:   nil, SecurityOptions: append([]string(nil), plan.securityOptions...),
		NanoCPUs: plan.nanoCPUs, MemoryBytes: plan.memoryBytes, PIDsLimit: plan.pidsLimit,
	}
}

func identityLabelsMatch(labels map[string]string, identity contracts.SandboxExecutionIdentity) bool {
	if identity.StableHash() == "" || len(labels) != 7 {
		return false
	}
	expected := map[string]string{
		ociLabelPrefix + "schema":            "1",
		ociLabelPrefix + "execution-hash":    identity.StableHash(),
		ociLabelPrefix + "tool-request-hash": identity.ToolRequestHash,
		ociLabelPrefix + "tool-definition":   identity.ToolDefinitionHash,
		ociLabelPrefix + "sandbox-profile":   identity.SandboxProfileHash,
		ociLabelPrefix + "qualification":     identity.QualificationHash,
		ociLabelPrefix + "workspace-hash":    contracts.HashStrings("fornix.sandbox.workspace.v1", identity.WorkspaceID),
	}
	for key, value := range expected {
		if labels[key] != value {
			return false
		}
	}
	return true
}

func ociPolicyEqual(expected, actual OCIContainerPolicy) bool {
	expectedCopy, expectedOK := canonicalOCIPolicy(expected)
	actualCopy, actualOK := canonicalOCIPolicy(actual)
	return expectedOK && actualOK && reflect.DeepEqual(expectedCopy, actualCopy)
}

func canonicalOCIPolicy(policy OCIContainerPolicy) (OCIContainerPolicy, bool) {
	if len(policy.Arguments) > 4096 || len(policy.Environment) > 4096 ||
		len(policy.Labels) > ociMaxPolicyEntries || len(policy.Mounts) > ociMaxPolicyEntries ||
		len(policy.DroppedCapabilities) > ociMaxPolicyEntries || len(policy.AddedCapabilities) > ociMaxPolicyEntries || len(policy.SecurityOptions) > ociMaxPolicyEntries ||
		len(policy.Devices) > ociMaxPolicyEntries || len(policy.PortBindings) > ociMaxPolicyEntries || len(policy.ExtraHosts) > ociMaxPolicyEntries ||
		len(policy.DNS) > ociMaxPolicyEntries || len(policy.Links) > ociMaxPolicyEntries || len(policy.VolumesFrom) > ociMaxPolicyEntries ||
		len(policy.NetworkAliases) > ociMaxPolicyEntries || len(policy.UnexpectedControls) > ociMaxPolicyEntries {
		return OCIContainerPolicy{}, false
	}
	for _, value := range []string{
		policy.ImageID, policy.ImagePlatform.OS, policy.ImagePlatform.Architecture, policy.ImagePlatform.Variant,
		policy.Executable, policy.User, policy.WorkingDirectory, policy.NetworkMode,
		policy.CPUSetCPUs, policy.CPUSetMems, policy.CgroupParent, policy.CgroupnsMode, policy.Runtime,
		policy.PIDMode, policy.IPCMode, policy.UTSMode, policy.UserNamespaceMode,
		policy.Hostname, policy.Domainname, policy.RestartPolicy, policy.InitPath,
	} {
		if len(value) > ociMaxPolicyText {
			return OCIContainerPolicy{}, false
		}
	}
	for key, value := range policy.Labels {
		if len(key) > ociMaxPolicyText || len(value) > ociMaxPolicyText {
			return OCIContainerPolicy{}, false
		}
	}
	for _, values := range [][]string{policy.Arguments, policy.Environment, policy.DroppedCapabilities,
		policy.AddedCapabilities, policy.SecurityOptions, policy.ExtraHosts, policy.DNS,
		policy.Links, policy.VolumesFrom, policy.NetworkAliases} {
		for _, value := range values {
			if len(value) > ociMaxPolicyText {
				return OCIContainerPolicy{}, false
			}
		}
	}
	for _, device := range policy.Devices {
		if len(device.HostPath) > ociMaxPolicyText || len(device.ContainerPath) > ociMaxPolicyText || len(device.Permissions) > ociMaxPolicyText {
			return OCIContainerPolicy{}, false
		}
	}
	for key, values := range policy.PortBindings {
		if len(key) > ociMaxPolicyText || len(values) > ociMaxPolicyEntries {
			return OCIContainerPolicy{}, false
		}
		for _, value := range values {
			if len(value) > ociMaxPolicyText {
				return OCIContainerPolicy{}, false
			}
		}
	}
	for _, control := range policy.UnexpectedControls {
		if len(control.Name) > ociMaxPolicyText || len(control.Value) > ociMaxPolicyText {
			return OCIContainerPolicy{}, false
		}
	}
	for _, mount := range policy.Mounts {
		if len(mount.Kind) > ociMaxPolicyText || len(mount.Source) > ociMaxPolicyText || len(mount.Target) > ociMaxPolicyText ||
			len(mount.Propagation) > ociMaxPolicyText || len(mount.Consistency) > ociMaxPolicyText || len(mount.Options) > ociMaxPolicyEntries {
			return OCIContainerPolicy{}, false
		}
		for _, value := range mount.Options {
			if len(value) > ociMaxPolicyText {
				return OCIContainerPolicy{}, false
			}
		}
	}
	policy = cloneOCIPolicy(policy)
	if len(policy.Arguments) == 0 {
		policy.Arguments = nil
	}
	if len(policy.Environment) == 0 {
		policy.Environment = nil
	}
	if len(policy.Mounts) == 0 {
		policy.Mounts = nil
	}
	if len(policy.DroppedCapabilities) == 0 {
		policy.DroppedCapabilities = nil
	}
	if len(policy.AddedCapabilities) == 0 {
		policy.AddedCapabilities = nil
	}
	if len(policy.SecurityOptions) == 0 {
		policy.SecurityOptions = nil
	}
	if len(policy.Devices) == 0 {
		policy.Devices = nil
	}
	if len(policy.PortBindings) == 0 {
		policy.PortBindings = nil
	}
	if len(policy.ExtraHosts) == 0 {
		policy.ExtraHosts = nil
	}
	if len(policy.DNS) == 0 {
		policy.DNS = nil
	}
	if len(policy.Links) == 0 {
		policy.Links = nil
	}
	if len(policy.VolumesFrom) == 0 {
		policy.VolumesFrom = nil
	}
	if len(policy.NetworkAliases) == 0 {
		policy.NetworkAliases = nil
	}
	if len(policy.UnexpectedControls) == 0 {
		policy.UnexpectedControls = nil
	}
	if len(policy.Labels) == 0 {
		policy.Labels = nil
	}
	sort.Strings(policy.DroppedCapabilities)
	sort.Strings(policy.AddedCapabilities)
	sort.Strings(policy.SecurityOptions)
	sort.Strings(policy.ExtraHosts)
	sort.Strings(policy.DNS)
	sort.Strings(policy.Links)
	sort.Strings(policy.VolumesFrom)
	sort.Strings(policy.NetworkAliases)
	for index := range policy.Mounts {
		if len(policy.Mounts[index].Options) == 0 {
			policy.Mounts[index].Options = nil
		}
		sort.Strings(policy.Mounts[index].Options)
	}
	sort.Slice(policy.Mounts, func(i, j int) bool {
		left, right := policy.Mounts[i], policy.Mounts[j]
		if left.Target != right.Target {
			return left.Target < right.Target
		}
		if left.Kind != right.Kind {
			return left.Kind < right.Kind
		}
		return left.Source < right.Source
	})
	sort.Slice(policy.Devices, func(i, j int) bool {
		left, right := policy.Devices[i], policy.Devices[j]
		if left.ContainerPath != right.ContainerPath {
			return left.ContainerPath < right.ContainerPath
		}
		return left.HostPath < right.HostPath
	})
	for key, values := range policy.PortBindings {
		sort.Strings(values)
		policy.PortBindings[key] = values
	}
	sort.Slice(policy.UnexpectedControls, func(i, j int) bool {
		if policy.UnexpectedControls[i].Name != policy.UnexpectedControls[j].Name {
			return policy.UnexpectedControls[i].Name < policy.UnexpectedControls[j].Name
		}
		return policy.UnexpectedControls[i].Value < policy.UnexpectedControls[j].Value
	})
	return policy, true
}

func cloneOCIPolicy(policy OCIContainerPolicy) OCIContainerPolicy {
	policy.Arguments = append([]string(nil), policy.Arguments...)
	policy.Environment = append([]string(nil), policy.Environment...)
	policy.Labels = cloneStringMap(policy.Labels)
	policy.Mounts = append([]OCIContainerMount(nil), policy.Mounts...)
	for index := range policy.Mounts {
		policy.Mounts[index].Options = append([]string(nil), policy.Mounts[index].Options...)
	}
	policy.DroppedCapabilities = append([]string(nil), policy.DroppedCapabilities...)
	policy.AddedCapabilities = append([]string(nil), policy.AddedCapabilities...)
	policy.SecurityOptions = append([]string(nil), policy.SecurityOptions...)
	policy.Devices = append([]OCIContainerDevice(nil), policy.Devices...)
	if policy.PortBindings != nil {
		bindings := make(map[string][]string, len(policy.PortBindings))
		for key, values := range policy.PortBindings {
			bindings[key] = append([]string(nil), values...)
		}
		policy.PortBindings = bindings
	}
	policy.ExtraHosts = append([]string(nil), policy.ExtraHosts...)
	policy.DNS = append([]string(nil), policy.DNS...)
	policy.Links = append([]string(nil), policy.Links...)
	policy.VolumesFrom = append([]string(nil), policy.VolumesFrom...)
	policy.NetworkAliases = append([]string(nil), policy.NetworkAliases...)
	policy.UnexpectedControls = append([]OCIControlSetting(nil), policy.UnexpectedControls...)
	return policy
}

func cloneOCISpec(spec OCIContainerSpec) OCIContainerSpec {
	spec.Policy = cloneOCIPolicy(spec.Policy)
	return spec
}

func cloneSandboxRunnerRequest(request contracts.SandboxRunnerRequest) contracts.SandboxRunnerRequest {
	request.Argv = append([]string(nil), request.Argv...)
	if request.Environment != nil {
		environment := make(map[string]string, len(request.Environment))
		for key, value := range request.Environment {
			environment[key] = value
		}
		request.Environment = environment
	}
	request.Profile.RequiredCapabilities = append([]contracts.SandboxCapability(nil), request.Profile.RequiredCapabilities...)
	return request
}

func validEngineID(id string) bool {
	if id == "" || len(id) > 256 || strings.TrimSpace(id) != id {
		return false
	}
	for _, char := range id {
		if !(char >= 'a' && char <= 'z' || char >= 'A' && char <= 'Z' || char >= '0' && char <= '9' || char == '-' || char == '_' || char == '.') {
			return false
		}
	}
	return true
}

func ociRedactedLogValue(kind string) slog.Value {
	return slog.GroupValue(slog.String("type", kind), slog.Bool("redacted", true))
}
