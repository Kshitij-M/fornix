package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"
	"time"
)

const (
	SandboxRunnerProtocolVersion  = 2
	MaxSandboxRunnerRequestBytes  = 1 << 20
	MaxSandboxRunnerOutputBytes   = 2 * MaxToolOutputBytes
	MaxSandboxRunnerResponseBytes = 16 << 20
)

const (
	SandboxRunnerOutcomeCompleted = "completed"
	SandboxRunnerOutcomeFailed    = "failed"
	SandboxRunnerOutcomeTimedOut  = "timed_out"
	SandboxRunnerOutcomeCancelled = "cancelled"
)

var (
	errInvalidSandboxRunnerRequest  = errors.New("invalid sandbox runner request")
	errInvalidSandboxRunnerResponse = errors.New("invalid sandbox runner response")
	workspaceMountRefPattern        = regexp.MustCompile(`^wsmount_[a-f0-9]{32,64}$`)
)

// WorkspaceMountRef is an opaque identifier resolved only by a separately
// trusted runner's local mount catalog. It is not a path or a bearer secret.
type WorkspaceMountRef struct {
	ID          string `json:"id"`
	WorkspaceID string `json:"workspace_id"`
}

// SandboxRunnerProfile is the wire-safe subset of a sandbox profile. It omits
// AllowedWorkdirRoot because the runner maps a workspace mount reference, not
// an absolute caller-supplied path, to the container's fixed mount point.
type SandboxRunnerProfile struct {
	Backend              string               `json:"backend"`
	TimeoutMS            int                  `json:"timeout_ms"`
	MaxStdoutBytes       int                  `json:"max_stdout_bytes"`
	MaxStderrBytes       int                  `json:"max_stderr_bytes"`
	MaxArgCount          int                  `json:"max_arg_count"`
	MaxArgBytes          int                  `json:"max_arg_bytes"`
	MaxEnvEntries        int                  `json:"max_env_entries"`
	MaxEnvBytes          int                  `json:"max_env_bytes"`
	CPUQuotaMilli        int                  `json:"cpu_quota_milli,omitempty"`
	MemoryBytes          int64                `json:"memory_bytes,omitempty"`
	PIDsLimit            int                  `json:"pids_limit,omitempty"`
	ScratchBytes         int64                `json:"scratch_bytes,omitempty"`
	ImageDigest          string               `json:"image_digest"`
	ImagePlatform        SandboxImagePlatform `json:"image_platform"`
	AllowNetwork         bool                 `json:"allow_network"`
	InheritEnvironment   bool                 `json:"inherit_environment"`
	ReadOnlyWorkdir      bool                 `json:"read_only_workdir"`
	ReadOnlyRootFS       bool                 `json:"read_only_rootfs"`
	RequiredCapabilities []SandboxCapability  `json:"required_capabilities,omitempty"`
}

// NewSandboxRunnerProfile strips caller path roots and copies only normalized
// runtime controls into the runner protocol.
func NewSandboxRunnerProfile(profile SandboxProfile) (SandboxRunnerProfile, error) {
	if err := profile.Normalize(); err != nil {
		return SandboxRunnerProfile{}, fmt.Errorf("normalize source sandbox profile: %w", err)
	}
	if profile.Backend != string(SandboxBackendOCI) {
		return SandboxRunnerProfile{}, fmt.Errorf("sandbox runner profile requires OCI backend")
	}
	runnerProfile := SandboxRunnerProfile{
		Backend: profile.Backend, TimeoutMS: profile.TimeoutMS,
		MaxStdoutBytes: profile.MaxStdoutBytes, MaxStderrBytes: profile.MaxStderrBytes,
		MaxArgCount: profile.MaxArgCount, MaxArgBytes: profile.MaxArgBytes,
		MaxEnvEntries: profile.MaxEnvEntries, MaxEnvBytes: profile.MaxEnvBytes,
		CPUQuotaMilli: profile.CPUQuotaMilli, MemoryBytes: profile.MemoryBytes,
		PIDsLimit: profile.PIDsLimit, ScratchBytes: profile.ScratchBytes,
		ImageDigest: profile.ImageDigest, ImagePlatform: profile.ImagePlatform, AllowNetwork: profile.AllowNetwork,
		InheritEnvironment: profile.InheritEnvironment,
		ReadOnlyWorkdir:    profile.ReadOnlyWorkdir, ReadOnlyRootFS: profile.ReadOnlyRootFS,
		RequiredCapabilities: append([]SandboxCapability(nil), profile.RequiredCapabilities...),
	}
	return runnerProfile, runnerProfile.Normalize()
}

// Normalize validates and canonicalizes the controls that cross the runner
// boundary without accepting any filesystem path.
func (p *SandboxRunnerProfile) Normalize() error {
	if p == nil {
		return fmt.Errorf("sandbox runner profile is nil")
	}
	full := SandboxProfile{
		Backend: p.Backend, TimeoutMS: p.TimeoutMS,
		MaxStdoutBytes: p.MaxStdoutBytes, MaxStderrBytes: p.MaxStderrBytes,
		MaxArgCount: p.MaxArgCount, MaxArgBytes: p.MaxArgBytes,
		MaxEnvEntries: p.MaxEnvEntries, MaxEnvBytes: p.MaxEnvBytes,
		CPUQuotaMilli: p.CPUQuotaMilli, MemoryBytes: p.MemoryBytes,
		PIDsLimit: p.PIDsLimit, ScratchBytes: p.ScratchBytes,
		ImageDigest: p.ImageDigest, ImagePlatform: p.ImagePlatform, AllowNetwork: p.AllowNetwork,
		InheritEnvironment: p.InheritEnvironment,
		ReadOnlyWorkdir:    p.ReadOnlyWorkdir, ReadOnlyRootFS: p.ReadOnlyRootFS,
		RequiredCapabilities: append([]SandboxCapability(nil), p.RequiredCapabilities...),
	}
	if err := full.Normalize(); err != nil {
		return err
	}
	if full.Backend != string(SandboxBackendOCI) || full.AllowNetwork || full.InheritEnvironment || !full.ReadOnlyRootFS || !full.ReadOnlyWorkdir {
		return fmt.Errorf("sandbox runner requires an offline OCI profile with no inherited environment and read-only root and workspace")
	}
	p.Backend, p.TimeoutMS = full.Backend, full.TimeoutMS
	p.MaxStdoutBytes, p.MaxStderrBytes = full.MaxStdoutBytes, full.MaxStderrBytes
	p.MaxArgCount, p.MaxArgBytes = full.MaxArgCount, full.MaxArgBytes
	p.MaxEnvEntries, p.MaxEnvBytes = full.MaxEnvEntries, full.MaxEnvBytes
	p.CPUQuotaMilli, p.MemoryBytes = full.CPUQuotaMilli, full.MemoryBytes
	p.PIDsLimit, p.ScratchBytes = full.PIDsLimit, full.ScratchBytes
	p.ImageDigest, p.AllowNetwork = full.ImageDigest, full.AllowNetwork
	p.ImagePlatform = full.ImagePlatform
	p.InheritEnvironment, p.ReadOnlyWorkdir = full.InheritEnvironment, full.ReadOnlyWorkdir
	p.ReadOnlyRootFS, p.RequiredCapabilities = full.ReadOnlyRootFS, full.RequiredCapabilities
	return nil
}

// Hash returns the stable identity of the path-free runner policy.
func (p SandboxRunnerProfile) Hash() string {
	if err := p.Normalize(); err != nil {
		return ""
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// Normalize checks the reference's shape and workspace binding. The runner
// must still resolve ID from its own trusted catalog; this method does not
// authorize or create a mount.
func (r *WorkspaceMountRef) Normalize() error {
	if r == nil {
		return fmt.Errorf("%w: workspace mount reference is required", errInvalidSandboxRunnerRequest)
	}
	r.ID = strings.TrimSpace(r.ID)
	if !workspaceMountRefPattern.MatchString(r.ID) {
		return fmt.Errorf("%w: workspace mount reference is malformed", errInvalidSandboxRunnerRequest)
	}
	workspace, err := normalizeDomainWorkspace(r.WorkspaceID)
	if err != nil {
		return fmt.Errorf("%w: workspace mount reference has invalid workspace", errInvalidSandboxRunnerRequest)
	}
	r.WorkspaceID = workspace
	return nil
}

// SandboxRunnerRequest is the narrow, host-path-free capability request sent
// to a separately authenticated runner. It intentionally has no Docker/OCI
// options, host paths, devices, ports, namespace selectors, or mount list.
// Argv and Environment may contain user data and must never be logged.
type SandboxRunnerRequest struct {
	SchemaVersion      int                      `json:"schema_version"`
	RequestID          string                   `json:"request_id"`
	WorkspaceID        string                   `json:"workspace_id"`
	WorkspaceMount     WorkspaceMountRef        `json:"workspace_mount"`
	Execution          SandboxExecutionIdentity `json:"execution"`
	ToolID             string                   `json:"tool_id"`
	ToolDefinitionHash string                   `json:"tool_definition_hash"`
	SandboxProfileHash string                   `json:"sandbox_profile_hash"`
	RunnerProfileHash  string                   `json:"runner_profile_hash"`
	// Argv contains only arguments after the executable. The runner resolves the
	// executable from its own trusted tool catalog keyed by ToolID and hash.
	Argv             []string             `json:"argv,omitempty"`
	Environment      map[string]string    `json:"environment,omitempty"`
	WorkingDirectory string               `json:"working_directory,omitempty"`
	Profile          SandboxRunnerProfile `json:"profile"`
}

// Normalize validates that the request is a single OCI capability invocation
// bound to the existing durable execution identity. It never resolves a mount
// reference or proves that a runtime enforces the requested controls.
func (r *SandboxRunnerRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("%w: request is nil", errInvalidSandboxRunnerRequest)
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = SandboxRunnerProtocolVersion
	}
	if r.SchemaVersion != SandboxRunnerProtocolVersion {
		return fmt.Errorf("%w: unsupported protocol version", errInvalidSandboxRunnerRequest)
	}
	var err error
	if r.RequestID, err = normalizeDomainIdentifier(r.RequestID, "sandbox runner request_id", MaxIdempotencyLength, true); err != nil {
		return fmt.Errorf("%w: request_id is invalid", errInvalidSandboxRunnerRequest)
	}
	if r.WorkspaceID, err = normalizeDomainWorkspace(r.WorkspaceID); err != nil {
		return fmt.Errorf("%w: workspace is invalid", errInvalidSandboxRunnerRequest)
	}
	if err := r.WorkspaceMount.Normalize(); err != nil {
		return err
	}
	if r.WorkspaceMount.WorkspaceID != r.WorkspaceID {
		return fmt.Errorf("%w: mount reference is outside request workspace", errInvalidSandboxRunnerRequest)
	}
	if err := r.Execution.Normalize(); err != nil {
		return fmt.Errorf("%w: execution identity is invalid", errInvalidSandboxRunnerRequest)
	}
	if r.Execution.WorkspaceID != r.WorkspaceID || r.Execution.Backend != SandboxBackendOCI {
		return fmt.Errorf("%w: execution identity scope or backend does not match", errInvalidSandboxRunnerRequest)
	}
	r.ToolID = strings.ToLower(strings.TrimSpace(r.ToolID))
	if r.ToolID == "" || len(r.ToolID) > MaxDomainIDLength {
		return fmt.Errorf("%w: tool_id is invalid", errInvalidSandboxRunnerRequest)
	}
	if r.ToolDefinitionHash, err = normalizeDomainHash(r.ToolDefinitionHash, "sandbox runner tool definition hash", true); err != nil {
		return fmt.Errorf("%w: tool definition identity is invalid", errInvalidSandboxRunnerRequest)
	}
	if r.SandboxProfileHash, err = normalizeDomainHash(r.SandboxProfileHash, "sandbox runner profile hash", true); err != nil {
		return fmt.Errorf("%w: profile identity is invalid", errInvalidSandboxRunnerRequest)
	}
	if r.Execution.ToolDefinitionHash != r.ToolDefinitionHash || r.Execution.SandboxProfileHash != r.SandboxProfileHash {
		return fmt.Errorf("%w: request hashes differ from durable execution identity", errInvalidSandboxRunnerRequest)
	}
	if err := r.Profile.Normalize(); err != nil {
		return fmt.Errorf("%w: sandbox profile is invalid", errInvalidSandboxRunnerRequest)
	}
	r.RunnerProfileHash, err = normalizeDomainHash(r.RunnerProfileHash, "sandbox runner profile hash", true)
	if err != nil || r.RunnerProfileHash != r.Profile.Hash() {
		return fmt.Errorf("%w: runner profile hash does not match its normalized controls", errInvalidSandboxRunnerRequest)
	}
	if len(r.Argv)+1 > r.Profile.MaxArgCount {
		return fmt.Errorf("%w: argument count exceeds profile", errInvalidSandboxRunnerRequest)
	}
	argBytes := 0
	for _, arg := range r.Argv {
		if strings.IndexByte(arg, 0) >= 0 || len(arg) > r.Profile.MaxArgBytes {
			return fmt.Errorf("%w: argument is invalid or exceeds profile", errInvalidSandboxRunnerRequest)
		}
		argBytes += len(arg)
	}
	if argBytes > MaxSandboxRunnerRequestBytes {
		return fmt.Errorf("%w: aggregate command exceeds protocol budget", errInvalidSandboxRunnerRequest)
	}
	// User-supplied environment values cannot be reliably redacted after a
	// runner restart because durable tool evidence intentionally stores only
	// redacted values. Keep the first offline OCI profile credential-free until
	// managed credential leases and restart-safe redaction are implemented.
	if len(r.Environment) != 0 {
		return fmt.Errorf("%w: request environment is unavailable for the offline runner profile", errInvalidSandboxRunnerRequest)
	}
	r.WorkingDirectory = strings.TrimSpace(r.WorkingDirectory)
	if r.WorkingDirectory == "" {
		r.WorkingDirectory = "."
	}
	if strings.Contains(r.WorkingDirectory, `\`) || strings.IndexByte(r.WorkingDirectory, 0) >= 0 || path.IsAbs(r.WorkingDirectory) || path.Clean(r.WorkingDirectory) != r.WorkingDirectory || r.WorkingDirectory == ".." || strings.HasPrefix(r.WorkingDirectory, "../") {
		return fmt.Errorf("%w: working directory must be normalized within the workspace mount", errInvalidSandboxRunnerRequest)
	}
	raw, err := json.Marshal(r)
	if err != nil || len(raw) > MaxSandboxRunnerRequestBytes {
		return fmt.Errorf("%w: serialized request exceeds protocol budget", errInvalidSandboxRunnerRequest)
	}
	return nil
}

// StableHash returns the deterministic hash of the normalized request. It may
// include user-supplied argv and environment data; callers must not log it
// alongside those values or treat it as redacted evidence.
func (r SandboxRunnerRequest) StableHash() string {
	if err := r.Normalize(); err != nil {
		return ""
	}
	raw, err := json.Marshal(r)
	if err != nil || len(raw) > MaxSandboxRunnerRequestBytes {
		return ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

// MarshalBounded returns canonical, size-bounded JSON suitable for a future
// authenticated IPC transport. It does not authenticate or transmit requests.
func (r SandboxRunnerRequest) MarshalBounded() ([]byte, error) {
	if err := r.Normalize(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("marshal sandbox runner request: %w", err)
	}
	if len(raw) > MaxSandboxRunnerRequestBytes {
		return nil, fmt.Errorf("%w: serialized request exceeds protocol budget", errInvalidSandboxRunnerRequest)
	}
	return raw, nil
}

// SandboxRunnerResponse is one bounded result for an exact execution request.
// Output is represented as bytes so arbitrary control characters cannot
// inflate JSON encoding without bound. FailureCode is a stable code only;
// runner diagnostics and raw error messages are intentionally excluded.
type SandboxRunnerResponse struct {
	SchemaVersion int       `json:"schema_version"`
	RequestID     string    `json:"request_id"`
	ExecutionHash string    `json:"execution_hash"`
	RequestHash   string    `json:"request_hash"`
	Outcome       string    `json:"outcome"`
	ExitCode      int       `json:"exit_code,omitempty"`
	Stdout        []byte    `json:"stdout,omitempty"`
	Stderr        []byte    `json:"stderr,omitempty"`
	FailureCode   string    `json:"failure_code,omitempty"`
	StartedAt     time.Time `json:"started_at"`
	FinishedAt    time.Time `json:"finished_at"`
}

// NormalizeFor validates response identity, outcome, timestamps, and output
// against the original bounded request before a caller converts it to a
// durable ToolResult.
func (r *SandboxRunnerResponse) NormalizeFor(request SandboxRunnerRequest) error {
	if r == nil {
		return fmt.Errorf("%w: response is nil", errInvalidSandboxRunnerResponse)
	}
	if err := request.Normalize(); err != nil {
		return fmt.Errorf("%w: source request is invalid", errInvalidSandboxRunnerResponse)
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = SandboxRunnerProtocolVersion
	}
	if r.SchemaVersion != SandboxRunnerProtocolVersion || r.RequestID != request.RequestID || r.ExecutionHash != request.Execution.StableHash() || r.RequestHash != request.StableHash() {
		return fmt.Errorf("%w: request or execution identity mismatch", errInvalidSandboxRunnerResponse)
	}
	switch r.Outcome {
	case SandboxRunnerOutcomeCompleted:
		if r.FailureCode != "" {
			return fmt.Errorf("%w: completed response cannot contain a failure code", errInvalidSandboxRunnerResponse)
		}
	case SandboxRunnerOutcomeFailed:
		if !knownSandboxRunnerFailureCode(r.FailureCode) || r.FailureCode == ToolFailureTimeout || r.FailureCode == ToolFailureCancelled {
			return fmt.Errorf("%w: failure code is missing or unsupported", errInvalidSandboxRunnerResponse)
		}
	case SandboxRunnerOutcomeTimedOut:
		if r.FailureCode != ToolFailureTimeout {
			return fmt.Errorf("%w: timed-out response requires the timeout failure code", errInvalidSandboxRunnerResponse)
		}
	case SandboxRunnerOutcomeCancelled:
		if r.FailureCode != ToolFailureCancelled {
			return fmt.Errorf("%w: cancelled response requires the cancellation failure code", errInvalidSandboxRunnerResponse)
		}
	default:
		return fmt.Errorf("%w: outcome is unsupported", errInvalidSandboxRunnerResponse)
	}
	if r.ExitCode < 0 || r.ExitCode > 255 {
		return fmt.Errorf("%w: exit code is outside the runtime range", errInvalidSandboxRunnerResponse)
	}
	if len(r.Stdout) > request.Profile.MaxStdoutBytes || len(r.Stderr) > request.Profile.MaxStderrBytes || len(r.Stdout)+len(r.Stderr) > MaxSandboxRunnerOutputBytes {
		return fmt.Errorf("%w: output exceeds request budget", errInvalidSandboxRunnerResponse)
	}
	if r.StartedAt.IsZero() || r.FinishedAt.IsZero() || r.FinishedAt.Before(r.StartedAt) || r.FinishedAt.Sub(r.StartedAt) > time.Duration(request.Profile.TimeoutMS)*time.Millisecond+30*time.Second {
		return fmt.Errorf("%w: timestamps are invalid or exceed bounded cleanup allowance", errInvalidSandboxRunnerResponse)
	}
	return nil
}

// MarshalBoundedFor returns a validated, size-bounded response for the exact
// request. A transport must also cap bytes read before JSON decoding so an
// oversized untrusted body is rejected before allocation.
func (r SandboxRunnerResponse) MarshalBoundedFor(request SandboxRunnerRequest) ([]byte, error) {
	if err := r.NormalizeFor(request); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(r)
	if err != nil {
		return nil, fmt.Errorf("marshal sandbox runner response: %w", err)
	}
	if len(raw) > MaxSandboxRunnerResponseBytes {
		return nil, fmt.Errorf("%w: serialized response exceeds protocol budget", errInvalidSandboxRunnerResponse)
	}
	return raw, nil
}

// ToolResult converts a validated response to the established durable result
// shape. It does not persist the result or complete its external effect.
func (r SandboxRunnerResponse) ToolResult(request SandboxRunnerRequest) (ToolResult, error) {
	if err := request.Normalize(); err != nil {
		return ToolResult{}, fmt.Errorf("%w: source request is invalid", errInvalidSandboxRunnerResponse)
	}
	if err := r.NormalizeFor(request); err != nil {
		return ToolResult{}, err
	}
	result := ToolResult{
		RequestID:  request.RequestID,
		RunID:      request.Execution.ToolRunID,
		ToolID:     request.ToolID,
		Status:     ToolRunSucceeded,
		ExitCode:   r.ExitCode,
		Stdout:     string(r.Stdout),
		Stderr:     string(r.Stderr),
		StartedAt:  r.StartedAt.UTC(),
		FinishedAt: r.FinishedAt.UTC(),
		DurationMS: r.FinishedAt.Sub(r.StartedAt).Milliseconds(),
	}
	if r.Outcome != SandboxRunnerOutcomeCompleted || r.ExitCode != 0 {
		result.Status = ToolRunFailed
		failureCode := r.FailureCode
		if failureCode == "" {
			failureCode = ToolFailureExecution
		}
		result.Failure = &ToolFailure{
			Code: failureCode, Message: "sandbox runner reported a bounded execution failure",
			Retryable: failureCode == ToolFailureTimeout,
		}
	}
	if r.Outcome == SandboxRunnerOutcomeCancelled {
		result.Status = ToolRunCancelled
	}
	result.ContentHash = result.Hash()
	return result, nil
}

func knownSandboxRunnerFailureCode(code string) bool {
	switch code {
	case ToolFailureTimeout, ToolFailureOutputLimit, ToolFailureExecution,
		ToolFailureCancelled,
		ToolFailureSandboxUnavailable, ToolFailureSandboxCapability,
		ToolFailureEnvironmentLimit:
		return true
	default:
		return false
	}
}
