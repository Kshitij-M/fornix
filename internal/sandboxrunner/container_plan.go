package sandboxrunner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

var ErrContainerPlanInvalid = errors.New("sandbox container plan is invalid")

const (
	ociWorkspaceTarget = "/workspace"
	ociScratchTarget   = "/tmp"
	ociLabelPrefix     = "dev.fornix.sandbox."
)

// OCIContainerPlan is runner-local input to a future Engine adapter. It
// intentionally contains a host path and argv, so it must never be serialized,
// emitted as evidence, or logged. Validate it immediately before each Engine
// mutation. It is a policy plan, not proof that an Engine enforces the policy.
type OCIContainerPlan struct {
	name                string
	imageDigest         string
	imagePlatform       contracts.SandboxImagePlatform
	executable          string
	arguments           []string
	environment         []string
	containerUser       string
	workingDirectory    string
	labels              map[string]string
	networkMode         string
	privileged          bool
	readOnlyRootFS      bool
	workspaceSource     string
	workspaceTarget     string
	workspaceReadOnly   bool
	workspaceRecursive  bool
	droppedCapabilities []string
	securityOptions     []string
	nanoCPUs            int64
	memoryBytes         int64
	pidsLimit           int64
	scratchTarget       string
	scratchSizeBytes    int64
	scratchOptions      string
	timeout             time.Duration
	maxStdoutBytes      int
	maxStderrBytes      int

	executionHash  string
	requestHash    string
	planHash       string
	expectedUser   string
	maxArgCount    int
	maxArgBytes    int
	expectedLabels map[string]string
}

// BuildOCIContainerPlan derives the complete fixed OCI policy from the
// normalized request, trusted tool catalog, and exact resolved workspace
// mount. The process's own non-root identity is used inside the container;
// root-runner deployments fail closed.
func BuildOCIContainerPlan(request contracts.SandboxRunnerRequest, catalog *ToolCatalog, mount *ResolvedMount) (OCIContainerPlan, error) {
	return buildOCIContainerPlanForIdentity(request, catalog, mount, os.Geteuid(), os.Getegid())
}

func buildOCIContainerPlanForIdentity(request contracts.SandboxRunnerRequest, catalog *ToolCatalog, mount *ResolvedMount, uid, gid int) (OCIContainerPlan, error) {
	if uid <= 0 || gid <= 0 || uid > 2_147_483_647 || gid > 2_147_483_647 {
		return OCIContainerPlan{}, ErrContainerPlanInvalid
	}
	if err := request.Normalize(); err != nil || catalog == nil || mount == nil {
		return OCIContainerPlan{}, ErrContainerPlanInvalid
	}
	invocation, err := catalog.Resolve(request, mount)
	if err != nil {
		return OCIContainerPlan{}, ErrContainerPlanInvalid
	}
	if err := mount.VerifyCurrent(); err != nil {
		return OCIContainerPlan{}, ErrContainerPlanInvalid
	}
	workspaceSource, err := mount.HostPath()
	if err != nil || workspaceSource == "" || !filepath.IsAbs(workspaceSource) || filepath.Clean(workspaceSource) != workspaceSource {
		return OCIContainerPlan{}, ErrContainerPlanInvalid
	}
	executionHash := request.Execution.StableHash()
	requestHash := request.StableHash()
	if len(executionHash) != 64 || len(requestHash) != 64 {
		return OCIContainerPlan{}, ErrContainerPlanInvalid
	}
	user := strconv.Itoa(uid) + ":" + strconv.Itoa(gid)
	plan := OCIContainerPlan{
		name: request.Execution.RuntimeName(), imageDigest: request.Profile.ImageDigest,
		imagePlatform: request.Profile.ImagePlatform,
		executable:    invocation.Executable, arguments: append([]string(nil), invocation.Argv[1:]...),
		containerUser: user, workingDirectory: invocation.WorkingDirectory,
		networkMode: "none", readOnlyRootFS: true,
		workspaceSource: workspaceSource, workspaceTarget: ociWorkspaceTarget,
		workspaceReadOnly: true, workspaceRecursive: false,
		droppedCapabilities: []string{"ALL"},
		securityOptions:     []string{"no-new-privileges:true"},
		nanoCPUs:            int64(request.Profile.CPUQuotaMilli) * 1_000_000,
		memoryBytes:         request.Profile.MemoryBytes,
		pidsLimit:           int64(request.Profile.PIDsLimit),
		scratchTarget:       ociScratchTarget,
		scratchSizeBytes:    request.Profile.ScratchBytes,
		scratchOptions:      fmt.Sprintf("rw,nosuid,nodev,size=%d,mode=1777", request.Profile.ScratchBytes),
		timeout:             time.Duration(request.Profile.TimeoutMS) * time.Millisecond,
		maxStdoutBytes:      request.Profile.MaxStdoutBytes,
		maxStderrBytes:      request.Profile.MaxStderrBytes,
		executionHash:       executionHash,
		requestHash:         requestHash,
	}
	plan.labels = map[string]string{
		ociLabelPrefix + "schema":            "1",
		ociLabelPrefix + "execution-hash":    executionHash,
		ociLabelPrefix + "tool-request-hash": request.Execution.ToolRequestHash,
		ociLabelPrefix + "tool-definition":   request.ToolDefinitionHash,
		ociLabelPrefix + "sandbox-profile":   request.SandboxProfileHash,
		ociLabelPrefix + "qualification":     request.Execution.QualificationHash,
		ociLabelPrefix + "workspace-hash":    contracts.HashStrings("fornix.sandbox.workspace.v1", request.WorkspaceID),
	}
	plan.expectedUser = user
	plan.maxArgCount = request.Profile.MaxArgCount
	plan.maxArgBytes = request.Profile.MaxArgBytes
	plan.expectedLabels = cloneStringMap(plan.labels)
	plan.planHash = plan.calculatedHash()
	if err := plan.Validate(); err != nil {
		return OCIContainerPlan{}, err
	}
	return plan, nil
}

// Validate rejects any plan that relaxes a fixed security control or is no
// longer bound to the exact request hashes that produced its runtime name.
func (p OCIContainerPlan) Validate() error {
	platform := p.imagePlatform
	if platform.Normalize() != nil || platform != p.imagePlatform {
		return ErrContainerPlanInvalid
	}
	if len(p.executionHash) != 64 || len(p.requestHash) != 64 ||
		p.name != contracts.SandboxRuntimeNameFromHash(p.executionHash) ||
		!validSHA256Digest(p.imageDigest) ||
		!filepath.IsAbs(p.executable) || filepath.Clean(p.executable) != p.executable ||
		isShellCommand(p.executable) || len(p.arguments)+1 > p.maxArgCount ||
		len(p.executable) > p.maxArgBytes || p.containerUser != p.expectedUser ||
		p.networkMode != "none" || p.privileged || !p.readOnlyRootFS ||
		p.workspaceTarget != ociWorkspaceTarget || !p.workspaceReadOnly || p.workspaceRecursive ||
		!filepath.IsAbs(p.workspaceSource) || filepath.Clean(p.workspaceSource) != p.workspaceSource ||
		p.scratchTarget != ociScratchTarget || p.nanoCPUs <= 0 || p.nanoCPUs > int64(contracts.MaxToolCPUQuotaMilli)*1_000_000 ||
		p.memoryBytes <= 0 || p.memoryBytes > contracts.MaxToolMemoryBytes || p.pidsLimit <= 0 || p.pidsLimit > contracts.MaxToolProcessCount ||
		p.scratchSizeBytes <= 0 || p.scratchSizeBytes > contracts.MaxToolScratchBytes || p.timeout <= 0 || p.timeout > contracts.MaxToolTimeout ||
		p.maxStdoutBytes <= 0 || p.maxStdoutBytes > contracts.MaxToolOutputBytes || p.maxStderrBytes <= 0 || p.maxStderrBytes > contracts.MaxToolOutputBytes ||
		len(p.environment) != 0 || p.containerUser == "" ||
		!equalStrings(p.droppedCapabilities, []string{"ALL"}) ||
		!equalStrings(p.securityOptions, []string{"no-new-privileges:true"}) ||
		p.scratchOptions != fmt.Sprintf("rw,nosuid,nodev,size=%d,mode=1777", p.scratchSizeBytes) {
		return ErrContainerPlanInvalid
	}
	uidGID := strings.Split(p.containerUser, ":")
	if len(uidGID) != 2 {
		return ErrContainerPlanInvalid
	}
	uid, uidErr := strconv.Atoi(uidGID[0])
	gid, gidErr := strconv.Atoi(uidGID[1])
	if uidErr != nil || gidErr != nil || uid <= 0 || gid <= 0 || uid > 2_147_483_647 || gid > 2_147_483_647 ||
		p.containerUser != strconv.Itoa(uid)+":"+strconv.Itoa(gid) {
		return ErrContainerPlanInvalid
	}
	if len(p.labels) != len(p.expectedLabels) || len(p.expectedLabels) != 7 {
		return ErrContainerPlanInvalid
	}
	for key, value := range p.expectedLabels {
		if p.labels[key] != value {
			return ErrContainerPlanInvalid
		}
	}
	for key, value := range p.labels {
		if !strings.HasPrefix(key, ociLabelPrefix) || strings.ContainsAny(value, "\r\n\x00") || len(value) > 128 {
			return ErrContainerPlanInvalid
		}
	}
	for _, arg := range p.arguments {
		if strings.IndexByte(arg, 0) >= 0 || len(arg) > p.maxArgBytes {
			return ErrContainerPlanInvalid
		}
	}
	if p.planHash == "" || p.planHash != p.calculatedHash() {
		return ErrContainerPlanInvalid
	}
	return nil
}

func (p OCIContainerPlan) calculatedHash() string {
	type fingerprint struct {
		Name                string
		ImageDigest         string
		ImagePlatform       contracts.SandboxImagePlatform
		Executable          string
		Arguments           []string
		Environment         []string
		ContainerUser       string
		WorkingDirectory    string
		Labels              map[string]string
		NetworkMode         string
		Privileged          bool
		ReadOnlyRootFS      bool
		WorkspaceSource     string
		WorkspaceTarget     string
		WorkspaceReadOnly   bool
		WorkspaceRecursive  bool
		DroppedCapabilities []string
		SecurityOptions     []string
		NanoCPUs            int64
		MemoryBytes         int64
		PIDsLimit           int64
		ScratchTarget       string
		ScratchSizeBytes    int64
		ScratchOptions      string
		Timeout             time.Duration
		MaxStdoutBytes      int
		MaxStderrBytes      int
		ExecutionHash       string
		RequestHash         string
		ExpectedUser        string
		MaxArgCount         int
		MaxArgBytes         int
		ExpectedLabels      map[string]string
	}
	canonical := fingerprint{
		Name: p.name, ImageDigest: p.imageDigest, ImagePlatform: p.imagePlatform, Executable: p.executable,
		Arguments: p.arguments, Environment: p.environment, ContainerUser: p.containerUser,
		WorkingDirectory: p.workingDirectory, Labels: p.labels, NetworkMode: p.networkMode,
		Privileged: p.privileged, ReadOnlyRootFS: p.readOnlyRootFS,
		WorkspaceSource: p.workspaceSource, WorkspaceTarget: p.workspaceTarget,
		WorkspaceReadOnly: p.workspaceReadOnly, WorkspaceRecursive: p.workspaceRecursive,
		DroppedCapabilities: p.droppedCapabilities, SecurityOptions: p.securityOptions,
		NanoCPUs: p.nanoCPUs, MemoryBytes: p.memoryBytes, PIDsLimit: p.pidsLimit,
		ScratchTarget: p.scratchTarget, ScratchSizeBytes: p.scratchSizeBytes,
		ScratchOptions: p.scratchOptions, Timeout: p.timeout,
		MaxStdoutBytes: p.maxStdoutBytes, MaxStderrBytes: p.maxStderrBytes,
		ExecutionHash: p.executionHash, RequestHash: p.requestHash,
		ExpectedUser: p.expectedUser, MaxArgCount: p.maxArgCount,
		MaxArgBytes: p.maxArgBytes, ExpectedLabels: p.expectedLabels,
	}
	raw, err := json.Marshal(canonical)
	if err != nil {
		return ""
	}
	digest := sha256.Sum256(raw)
	return hex.EncodeToString(digest[:])
}

func validSHA256Digest(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, char := range value[len("sha256:"):] {
		if !(char >= '0' && char <= '9' || char >= 'a' && char <= 'f') {
			return false
		}
	}
	return true
}

func isShellCommand(executable string) bool {
	switch strings.ToLower(filepath.Base(executable)) {
	case "sh", "bash", "zsh", "fish", "dash", "ksh", "cmd", "powershell", "pwsh":
		return true
	default:
		return false
	}
}

func cloneStringMap(source map[string]string) map[string]string {
	clone := make(map[string]string, len(source))
	for key, value := range source {
		clone[key] = value
	}
	return clone
}

// String prevents accidental diagnostic logging from exposing argv or the
// host workspace path.
func (p OCIContainerPlan) String() string {
	return fmt.Sprintf("OCIContainerPlan{name:%q, image_digest:%q, execution_hash:%q}", p.name, p.imageDigest, p.executionHash)
}

// GoString keeps Go's diagnostic representation from exposing argv or the
// host workspace path.
func (p OCIContainerPlan) GoString() string { return p.String() }

// LogValue keeps structured logs from reflecting argv, environment, or a host
// path from the exported plan fields.
func (p OCIContainerPlan) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("name", p.name),
		slog.String("image_digest", p.imageDigest),
		slog.String("execution_hash", p.executionHash),
	)
}

// MarshalJSON rejects accidental serialization of host paths and argv. The
// Engine adapter maps the typed fields directly to its SDK request instead.
func (OCIContainerPlan) MarshalJSON() ([]byte, error) {
	return nil, ErrContainerPlanInvalid
}

// engineSnapshot verifies and deep-copies the opaque plan. The Engine adapter
// must use only this detached snapshot for one atomic create request.
func (p OCIContainerPlan) engineSnapshot() (OCIContainerPlan, error) {
	if err := p.Validate(); err != nil {
		return OCIContainerPlan{}, err
	}
	p.arguments = append([]string(nil), p.arguments...)
	p.environment = append([]string(nil), p.environment...)
	p.labels = cloneStringMap(p.labels)
	p.droppedCapabilities = append([]string(nil), p.droppedCapabilities...)
	p.securityOptions = append([]string(nil), p.securityOptions...)
	p.expectedLabels = cloneStringMap(p.expectedLabels)
	return p, nil
}

func equalStrings(left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}
