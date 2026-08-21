package contracts

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"time"
)

const (
	ToolSchemaVersion       = 1
	DefaultToolTimeout      = 30 * time.Second
	MaxToolTimeout          = 10 * time.Minute
	DefaultToolOutputBytes  = 256 << 10
	MaxToolOutputBytes      = 4 << 20
	DefaultToolArgCount     = 64
	MaxToolArgCount         = 256
	DefaultToolArgBytes     = 16 << 10
	MaxToolArgBytes         = 64 << 10
	DefaultToolEnvCount     = 64
	MaxToolEnvCount         = 128
	DefaultToolEnvBytes     = 32 << 10
	MaxToolEnvBytes         = 128 << 10
	MaxToolEvidenceBytes    = 64 << 10
	MaxToolWorkdirLength    = 4096
	MaxToolCapabilityLength = 128
)

const (
	ToolRunPending          = "pending"
	ToolRunAwaitingApproval = "awaiting_approval"
	ToolRunRunning          = "running"
	ToolRunSucceeded        = "succeeded"
	ToolRunFailed           = "failed"
	ToolRunDenied           = "denied"
	ToolRunCancelled        = "cancelled"
)

const (
	ToolModeAutomatic   = "automatic"
	ToolModePreApproved = "pre_approved"
	ToolModeInteractive = "interactive"
	ToolModeDenied      = "denied"
)

const (
	ToolFailureInvalidRequest   = "invalid_request"
	ToolFailureUnknownTool      = "unknown_tool"
	ToolFailureUnauthorized     = "unauthorized"
	ToolFailureApprovalRequired = "approval_required"
	ToolFailureApprovalDenied   = "approval_denied"
	ToolFailureApprovalExpired  = "approval_expired"
	ToolFailureTimeout          = "timeout"
	ToolFailureOutputLimit      = "output_limit"
	ToolFailureArgumentLimit    = "argument_limit"
	ToolFailureEnvironmentLimit = "environment_limit"
	ToolFailureWorkdirDenied    = "workdir_denied"
	ToolFailureExecution        = "execution"
	ToolFailureTransport        = "transport"
	ToolFailureStaleFence       = "stale_fence"
	ToolFailureInProgress       = "in_progress"
	ToolFailureBudget           = "budget"
	ToolFailureCancelled        = "cancelled"
	ToolFailureConflict         = "conflict"
)

const (
	ApprovalPending  = "pending"
	ApprovalApproved = "approved"
	ApprovalDenied   = "denied"
	ApprovalExpired  = "expired"
)

const (
	ToolEventRequested         = "tool.requested"
	ToolEventApprovalRequested = "tool.approval_requested"
	ToolEventApprovalDecided   = "tool.approval_decided"
	ToolEventStarted           = "tool.started"
	ToolEventSucceeded         = "tool.succeeded"
	ToolEventFailed            = "tool.failed"
	ToolEventDenied            = "tool.denied"
)

var envKeyPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// SandboxProfile is the local executor's bounded, per-call process policy.
// It intentionally describes limits, not an unsupported claim of kernel
// isolation. Network and filesystem enforcement are explicit capabilities of
// a future provider, not silently implied by this profile.
type SandboxProfile struct {
	Backend            string `json:"backend"`
	TimeoutMS          int    `json:"timeout_ms"`
	MaxStdoutBytes     int    `json:"max_stdout_bytes"`
	MaxStderrBytes     int    `json:"max_stderr_bytes"`
	MaxArgCount        int    `json:"max_arg_count"`
	MaxArgBytes        int    `json:"max_arg_bytes"`
	MaxEnvEntries      int    `json:"max_env_entries"`
	MaxEnvBytes        int    `json:"max_env_bytes"`
	AllowedWorkdirRoot string `json:"allowed_workdir_root,omitempty"`
	AllowNetwork       bool   `json:"allow_network"`
	InheritEnvironment bool   `json:"inherit_environment"`
	ReadOnlyWorkdir    bool   `json:"read_only_workdir"`
}

// DefaultSandboxProfile returns the bounded local-process defaults. It does
// not claim kernel isolation or network control.
func DefaultSandboxProfile() SandboxProfile {
	return SandboxProfile{
		Backend: "local-process", TimeoutMS: int(DefaultToolTimeout / time.Millisecond),
		MaxStdoutBytes: DefaultToolOutputBytes, MaxStderrBytes: DefaultToolOutputBytes,
		MaxArgCount: DefaultToolArgCount, MaxArgBytes: DefaultToolArgBytes,
		MaxEnvEntries: DefaultToolEnvCount, MaxEnvBytes: DefaultToolEnvBytes,
	}
}

// Normalize validates process limits and rejects settings the local executor
// cannot enforce safely.
func (p *SandboxProfile) Normalize() error {
	if p.Backend == "" {
		p.Backend = "local-process"
	}
	if p.TimeoutMS == 0 {
		p.TimeoutMS = int(DefaultToolTimeout / time.Millisecond)
	}
	if p.MaxStdoutBytes == 0 {
		p.MaxStdoutBytes = DefaultToolOutputBytes
	}
	if p.MaxStderrBytes == 0 {
		p.MaxStderrBytes = DefaultToolOutputBytes
	}
	if p.MaxArgCount == 0 {
		p.MaxArgCount = DefaultToolArgCount
	}
	if p.MaxArgBytes == 0 {
		p.MaxArgBytes = DefaultToolArgBytes
	}
	if p.MaxEnvEntries == 0 {
		p.MaxEnvEntries = DefaultToolEnvCount
	}
	if p.MaxEnvBytes == 0 {
		p.MaxEnvBytes = DefaultToolEnvBytes
	}
	if p.TimeoutMS < 1 || time.Duration(p.TimeoutMS)*time.Millisecond > MaxToolTimeout {
		return fmt.Errorf("tool timeout_ms must be between 1 and %d", MaxToolTimeout/time.Millisecond)
	}
	if p.MaxStdoutBytes < 1 || p.MaxStdoutBytes > MaxToolOutputBytes || p.MaxStderrBytes < 1 || p.MaxStderrBytes > MaxToolOutputBytes {
		return fmt.Errorf("tool output budget exceeds bounds")
	}
	if p.MaxArgCount < 1 || p.MaxArgCount > MaxToolArgCount || p.MaxArgBytes < 1 || p.MaxArgBytes > MaxToolArgBytes {
		return fmt.Errorf("tool argument budget exceeds bounds")
	}
	if p.MaxEnvEntries < 0 || p.MaxEnvEntries > MaxToolEnvCount || p.MaxEnvBytes < 0 || p.MaxEnvBytes > MaxToolEnvBytes {
		return fmt.Errorf("tool environment budget exceeds bounds")
	}
	p.AllowedWorkdirRoot = strings.TrimSpace(p.AllowedWorkdirRoot)
	if p.AllowedWorkdirRoot != "" && !filepath.IsAbs(p.AllowedWorkdirRoot) {
		return fmt.Errorf("allowed_workdir_root must be absolute")
	}
	if p.AllowNetwork {
		return fmt.Errorf("local-process profile cannot claim network isolation")
	}
	if p.InheritEnvironment {
		return fmt.Errorf("inherited environment is not permitted")
	}
	return nil
}

// ToolDefinition is an explicitly registered executable capability. Requests
// cannot choose a different executable than the definition names.
type ToolDefinition struct {
	ID              string         `json:"id"`
	Version         string         `json:"version"`
	Name            string         `json:"name"`
	Capability      string         `json:"capability"`
	Description     string         `json:"description,omitempty"`
	Executable      string         `json:"executable"`
	ArgvPrefix      []string       `json:"argv_prefix,omitempty"`
	PathArgvIndexes []int          `json:"path_argv_indexes,omitempty"`
	AllowedEnvKeys  []string       `json:"allowed_env_keys,omitempty"`
	WorkdirRoot     string         `json:"workdir_root,omitempty"`
	Sandbox         SandboxProfile `json:"sandbox"`
	Enabled         bool           `json:"enabled"`
}

// Normalize validates the registered executable and canonicalizes its
// capability metadata before it is exposed to requests.
func (d *ToolDefinition) Normalize() error {
	if d == nil {
		return fmt.Errorf("tool definition is nil")
	}
	d.ID, d.Name, d.Version, d.Capability = strings.TrimSpace(d.ID), strings.TrimSpace(d.Name), strings.TrimSpace(d.Version), strings.TrimSpace(d.Capability)
	if d.ID == "" || d.Name == "" || d.Version == "" || d.Capability == "" {
		return fmt.Errorf("tool id, name, version, and capability are required")
	}
	d.ID, d.Capability = strings.ToLower(d.ID), strings.ToLower(d.Capability)
	d.Executable = strings.TrimSpace(d.Executable)
	if !filepath.IsAbs(d.Executable) {
		return fmt.Errorf("tool executable must be absolute")
	}
	if isShellExecutable(d.Executable) {
		return fmt.Errorf("shell executables are not permitted")
	}
	if len(d.ArgvPrefix) > MaxToolArgCount {
		return fmt.Errorf("tool argv prefix is too large")
	}
	if len(d.PathArgvIndexes) > MaxToolArgCount {
		return fmt.Errorf("tool path argv indexes are too large")
	}
	for _, index := range d.PathArgvIndexes {
		if index < 0 || index >= MaxToolArgCount {
			return fmt.Errorf("tool path argv index is out of bounds")
		}
	}
	for i := range d.ArgvPrefix {
		if len(d.ArgvPrefix[i]) > MaxToolArgBytes {
			return fmt.Errorf("tool argv prefix argument is too large")
		}
	}
	d.WorkdirRoot = strings.TrimSpace(d.WorkdirRoot)
	if d.WorkdirRoot != "" && !filepath.IsAbs(d.WorkdirRoot) {
		return fmt.Errorf("tool workdir_root must be absolute")
	}
	for i := range d.AllowedEnvKeys {
		d.AllowedEnvKeys[i] = strings.TrimSpace(d.AllowedEnvKeys[i])
		if !envKeyPattern.MatchString(d.AllowedEnvKeys[i]) {
			return fmt.Errorf("invalid allowed environment key")
		}
	}
	d.AllowedEnvKeys = uniqueSorted(d.AllowedEnvKeys)
	if err := d.Sandbox.Normalize(); err != nil {
		return err
	}
	if d.Sandbox.AllowedWorkdirRoot == "" {
		d.Sandbox.AllowedWorkdirRoot = d.WorkdirRoot
	}
	if !d.Enabled {
		return fmt.Errorf("tool %q is disabled", d.ID)
	}
	return nil
}

func isShellExecutable(path string) bool {
	base := strings.ToLower(filepath.Base(path))
	switch base {
	case "sh", "bash", "zsh", "fish", "dash", "ksh", "cmd", "powershell", "pwsh":
		return true
	}
	return false
}

// ToolPolicyRule is an ordered, deny-by-default policy rule. Empty scope
// fields are wildcards; callers should use explicit workspace rules for
// production. A rule with a matching scope is still bounded by definition.
type ToolPolicyRule struct {
	ID             string         `json:"id"`
	Priority       int            `json:"priority"`
	WorkspaceID    string         `json:"workspace_id"`
	ActorID        string         `json:"actor_id,omitempty"`
	TaskID         string         `json:"task_id,omitempty"`
	SessionID      string         `json:"session_id,omitempty"`
	ToolID         string         `json:"tool_id"`
	Capability     string         `json:"capability,omitempty"`
	Mode           string         `json:"mode"`
	ArgvPrefix     []string       `json:"argv_prefix,omitempty"`
	AllowedEnvKeys []string       `json:"allowed_env_keys,omitempty"`
	WorkdirRoot    string         `json:"workdir_root,omitempty"`
	Sandbox        SandboxProfile `json:"sandbox"`
	Enabled        bool           `json:"enabled"`
}

// Normalize validates one ordered deny-by-default policy rule.
func (r *ToolPolicyRule) Normalize() error {
	if r == nil {
		return fmt.Errorf("tool policy rule is nil")
	}
	r.ID, r.WorkspaceID, r.ActorID, r.TaskID, r.SessionID, r.ToolID, r.Capability = strings.TrimSpace(r.ID), strings.TrimSpace(r.WorkspaceID), strings.TrimSpace(r.ActorID), strings.TrimSpace(r.TaskID), strings.TrimSpace(r.SessionID), strings.ToLower(strings.TrimSpace(r.ToolID)), strings.ToLower(strings.TrimSpace(r.Capability))
	r.Mode = strings.ToLower(strings.TrimSpace(r.Mode))
	if r.ID == "" || r.WorkspaceID == "" || r.ToolID == "" {
		return fmt.Errorf("policy id, workspace_id, and tool_id are required")
	}
	switch r.Mode {
	case ToolModeAutomatic, ToolModePreApproved, ToolModeInteractive, ToolModeDenied:
	default:
		return fmt.Errorf("unsupported tool policy mode %q", r.Mode)
	}
	if r.Priority < 0 {
		return fmt.Errorf("policy priority cannot be negative")
	}
	for i := range r.ArgvPrefix {
		if len(r.ArgvPrefix[i]) > MaxToolArgBytes {
			return fmt.Errorf("policy argv prefix argument is too large")
		}
	}
	for i := range r.AllowedEnvKeys {
		r.AllowedEnvKeys[i] = strings.TrimSpace(r.AllowedEnvKeys[i])
		if !envKeyPattern.MatchString(r.AllowedEnvKeys[i]) {
			return fmt.Errorf("invalid policy environment key")
		}
	}
	r.AllowedEnvKeys = uniqueSorted(r.AllowedEnvKeys)
	r.WorkdirRoot = strings.TrimSpace(r.WorkdirRoot)
	if r.WorkdirRoot != "" && !filepath.IsAbs(r.WorkdirRoot) {
		return fmt.Errorf("policy workdir_root must be absolute")
	}
	return r.Sandbox.Normalize()
}

// ToolRequest is the structured, authenticated request to run one registered
// capability. Argv is passed directly to the executable; no shell is implied.
type ToolRequest struct {
	SchemaVersion  int               `json:"schema_version"`
	RequestID      string            `json:"request_id"`
	IdempotencyKey string            `json:"idempotency_key"`
	CausationID    string            `json:"causation_id,omitempty"`
	CorrelationID  string            `json:"correlation_id,omitempty"`
	WorkspaceID    string            `json:"workspace_id"`
	Actor          ActorRef          `json:"actor,omitempty"`
	Task           *EntityRef        `json:"task,omitempty"`
	Session        *EntityRef        `json:"session,omitempty"`
	TaskOwnerID    string            `json:"task_owner_id,omitempty"`
	TaskFence      uint64            `json:"task_fence,omitempty"`
	ToolID         string            `json:"tool_id"`
	Capability     string            `json:"capability,omitempty"`
	Argv           []string          `json:"argv"`
	Environment    map[string]string `json:"environment,omitempty"`
	Workdir        string            `json:"workdir,omitempty"`
	Mode           string            `json:"mode,omitempty"`
	ApprovalID     string            `json:"approval_id,omitempty"`
	Budget         SandboxProfile    `json:"budget"`
	Metadata       map[string]string `json:"metadata,omitempty"`
}

// Normalize validates workspace/entity scope, argv, environment, and all
// execution budgets before policy evaluation.
func (r *ToolRequest) Normalize() error {
	if r == nil {
		return fmt.Errorf("tool request is nil")
	}
	if r.SchemaVersion == 0 {
		r.SchemaVersion = ToolSchemaVersion
	}
	if r.SchemaVersion != ToolSchemaVersion {
		return fmt.Errorf("unsupported tool schema_version %d", r.SchemaVersion)
	}
	r.RequestID, r.IdempotencyKey, r.CausationID, r.CorrelationID = strings.TrimSpace(r.RequestID), strings.TrimSpace(r.IdempotencyKey), strings.TrimSpace(r.CausationID), strings.TrimSpace(r.CorrelationID)
	if r.RequestID == "" {
		r.RequestID = NewID("toolreq")
	}
	if r.IdempotencyKey == "" {
		return fmt.Errorf("idempotency_key is required")
	}
	if len(r.IdempotencyKey) > MaxIdempotencyLength {
		return fmt.Errorf("idempotency_key is too large")
	}
	r.WorkspaceID, r.ToolID, r.Capability, r.TaskOwnerID = strings.TrimSpace(r.WorkspaceID), strings.ToLower(strings.TrimSpace(r.ToolID)), strings.ToLower(strings.TrimSpace(r.Capability)), strings.TrimSpace(r.TaskOwnerID)
	if r.WorkspaceID == "" || r.ToolID == "" {
		return fmt.Errorf("workspace_id and tool_id are required")
	}
	if r.Task != nil {
		if err := validateModelEntityRef(r.Task, "task", r.WorkspaceID); err != nil {
			return err
		}
	}
	if r.Session != nil {
		if err := validateModelEntityRef(r.Session, "session", r.WorkspaceID); err != nil {
			return err
		}
	}
	if len(r.Argv) == 0 {
		return fmt.Errorf("argv is required")
	}
	if len(r.Argv) > MaxToolArgCount {
		return fmt.Errorf("argv exceeds %d arguments", MaxToolArgCount)
	}
	for _, arg := range r.Argv {
		if len(arg) > MaxToolArgBytes {
			return fmt.Errorf("argv argument exceeds %d bytes", MaxToolArgBytes)
		}
	}
	if !filepath.IsAbs(r.Argv[0]) || isShellExecutable(r.Argv[0]) {
		return fmt.Errorf("argv executable must be absolute and not a shell")
	}
	r.Workdir = strings.TrimSpace(r.Workdir)
	if len(r.Workdir) > MaxToolWorkdirLength || (r.Workdir != "" && !filepath.IsAbs(r.Workdir)) {
		return fmt.Errorf("workdir must be an absolute path within its policy")
	}
	if r.Mode == "" {
		r.Mode = ToolModeAutomatic
	}
	r.Mode = strings.ToLower(strings.TrimSpace(r.Mode))
	switch r.Mode {
	case ToolModeAutomatic, ToolModePreApproved, ToolModeInteractive, ToolModeDenied:
	default:
		return fmt.Errorf("unsupported tool mode %q", r.Mode)
	}
	if err := r.Budget.Normalize(); err != nil {
		return err
	}
	if len(r.Environment) > r.Budget.MaxEnvEntries {
		return fmt.Errorf("environment exceeds entry budget")
	}
	envBytes := 0
	for key, value := range r.Environment {
		if !envKeyPattern.MatchString(key) || len(key)+len(value) > r.Budget.MaxEnvBytes {
			return fmt.Errorf("environment entry is invalid or too large")
		}
		envBytes += len(key) + len(value)
	}
	if envBytes > r.Budget.MaxEnvBytes {
		return fmt.Errorf("environment exceeds byte budget")
	}
	if len(r.Metadata) > 64 {
		return fmt.Errorf("metadata cannot contain more than 64 entries")
	}
	return nil
}

// RequestHash identifies logical tool input while excluding retry, approval,
// and transport identities.
func (r ToolRequest) RequestHash() (string, error) {
	clone := r
	clone.RequestID = ""
	clone.IdempotencyKey = ""
	clone.CausationID = ""
	clone.CorrelationID = ""
	clone.Mode = ""
	clone.ApprovalID = ""
	raw, err := json.Marshal(clone)
	if err != nil {
		return "", fmt.Errorf("marshal tool request hash: %w", err)
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:]), nil
}

// RedactedEvidence returns bounded request evidence with environment values
// replaced before the request enters the durable ledger.
func (r ToolRequest) RedactedEvidence() ([]byte, error) {
	clone := r
	clone.Environment = make(map[string]string, len(r.Environment))
	for key := range r.Environment {
		clone.Environment[key] = "[REDACTED]"
	}
	return json.Marshal(clone)
}

// ToolFailure is the stable, redacted failure shape for a tool run.
type ToolFailure struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
	Detail    string `json:"detail,omitempty"`
}

// ToolResult is the normalized bounded result of a tool attempt. Large output
// is linked to artifacts by the store rather than expanded indefinitely.
type ToolResult struct {
	RequestID   string              `json:"request_id"`
	RunID       string              `json:"run_id,omitempty"`
	ToolID      string              `json:"tool_id"`
	Status      string              `json:"status"`
	ExitCode    int                 `json:"exit_code,omitempty"`
	Stdout      string              `json:"stdout,omitempty"`
	Stderr      string              `json:"stderr,omitempty"`
	StartedAt   time.Time           `json:"started_at,omitempty"`
	FinishedAt  time.Time           `json:"finished_at,omitempty"`
	DurationMS  int64               `json:"duration_ms,omitempty"`
	ContentHash string              `json:"content_hash,omitempty"`
	Artifacts   []ArtifactReference `json:"artifacts,omitempty"`
	Failure     *ToolFailure        `json:"failure,omitempty"`
}

// ApprovalRequest is the durable authorization gate for interactive tool
// execution.
type ApprovalRequest struct {
	ID          string     `json:"id"`
	WorkspaceID string     `json:"workspace_id"`
	RequestID   string     `json:"request_id"`
	RunID       string     `json:"run_id"`
	RequestHash string     `json:"request_hash"`
	ToolID      string     `json:"tool_id"`
	Actor       ActorRef   `json:"actor"`
	Task        *EntityRef `json:"task,omitempty"`
	Session     *EntityRef `json:"session,omitempty"`
	Status      string     `json:"status"`
	Reason      string     `json:"reason,omitempty"`
	ExpiresAt   time.Time  `json:"expires_at"`
	CreatedAt   time.Time  `json:"created_at"`
	DecidedAt   *time.Time `json:"decided_at,omitempty"`
}

// ApprovalDecision records an authorized, auditable approval outcome.
type ApprovalDecision struct {
	ApprovalID  string     `json:"approval_id"`
	WorkspaceID string     `json:"workspace_id"`
	Decision    string     `json:"decision"`
	Actor       ActorRef   `json:"actor"`
	Reason      string     `json:"reason,omitempty"`
	ExpiresAt   *time.Time `json:"expires_at,omitempty"`
}

// ToolRun is the authoritative idempotent lifecycle record for one tool
// request. Task-bound runs require the worker's current fence.
type ToolRun struct {
	ID               string       `json:"id"`
	WorkspaceID      string       `json:"workspace_id"`
	RequestID        string       `json:"request_id"`
	IdempotencyKey   string       `json:"idempotency_key"`
	RequestHash      string       `json:"request_hash"`
	SchemaVersion    int          `json:"schema_version"`
	CausationID      string       `json:"causation_id,omitempty"`
	CorrelationID    string       `json:"correlation_id,omitempty"`
	ToolID           string       `json:"tool_id"`
	Capability       string       `json:"capability"`
	Actor            ActorRef     `json:"actor"`
	Task             *EntityRef   `json:"task,omitempty"`
	Session          *EntityRef   `json:"session,omitempty"`
	TaskOwnerID      string       `json:"task_owner_id,omitempty"`
	TaskFence        uint64       `json:"task_fence,omitempty"`
	Mode             string       `json:"mode"`
	Status           string       `json:"status"`
	ApprovalID       string       `json:"approval_id,omitempty"`
	Attempt          int          `json:"attempt"`
	Result           *ToolResult  `json:"result,omitempty"`
	Failure          *ToolFailure `json:"failure,omitempty"`
	RequestEvidence  []byte       `json:"request_evidence,omitempty"`
	ResponseEvidence []byte       `json:"response_evidence,omitempty"`
	StdoutArtifact   *ArtifactRef `json:"stdout_artifact,omitempty"`
	StderrArtifact   *ArtifactRef `json:"stderr_artifact,omitempty"`
	ResultArtifact   *ArtifactRef `json:"result_artifact,omitempty"`
	CreatedAt        time.Time    `json:"created_at"`
	StartedAt        *time.Time   `json:"started_at,omitempty"`
	FinishedAt       *time.Time   `json:"finished_at,omitempty"`
	DurationMS       int64        `json:"duration_ms,omitempty"`
}

// Hash returns the stable result identity used to compare duplicate delivery.
func (r ToolResult) Hash() string {
	raw, _ := json.Marshal(struct {
		Status   string       `json:"status"`
		ExitCode int          `json:"exit_code"`
		Stdout   string       `json:"stdout"`
		Stderr   string       `json:"stderr"`
		Failure  *ToolFailure `json:"failure,omitempty"`
	}{r.Status, r.ExitCode, r.Stdout, r.Stderr, r.Failure})
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func uniqueSorted(values []string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		if value = strings.TrimSpace(value); value != "" {
			seen[value] = struct{}{}
		}
	}
	out := make([]string, 0, len(seen))
	for value := range seen {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

// IsToolTerminal reports whether a tool run accepts no further execution
// result.
func IsToolTerminal(status string) bool {
	switch strings.TrimSpace(status) {
	case ToolRunSucceeded, ToolRunFailed, ToolRunDenied, ToolRunCancelled:
		return true
	default:
		return false
	}
}
