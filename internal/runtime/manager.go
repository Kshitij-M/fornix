package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"
)

var (
	// ErrUnsafeRuntimePath reports a symlink or resolved path that escapes the
	// validated profile directory.
	ErrUnsafeRuntimePath = errors.New("unsafe runtime path")
	// ErrInvalidLogsOptions reports an unbounded tail or unknown service.
	ErrInvalidLogsOptions = errors.New("invalid runtime logs options")
)

// Limits bounds Docker lifecycle duration and diagnostic output. Values apply
// per invocation and are validated by NewManager.
type Limits struct {
	LifecycleTimeout time.Duration
	LogsTimeout      time.Duration
	MaxStdoutBytes   int
	MaxStderrBytes   int
}

// DefaultLimits returns conservative bounds suitable for local image pulls,
// health checks, and diagnostics.
func DefaultLimits() Limits {
	return Limits{
		LifecycleTimeout: 5 * time.Minute,
		LogsTimeout:      30 * time.Second,
		MaxStdoutBytes:   1 << 20,
		MaxStderrBytes:   1 << 20,
	}
}

// LogsOptions bounds local runtime log disclosure. Service may be empty for
// all services, "fornix", or "db". Tail must be between 1 and 10,000 lines.
type LogsOptions struct {
	Service string
	Tail    int
	Follow  bool
}

// Manager owns manifest materialization and idempotent Docker Compose
// lifecycle commands for one validated profile. It does not own Docker daemon
// installation, application bootstrap, or authoritative application state.
type Manager struct {
	mu          sync.Mutex
	profile     Profile
	manifest    ManifestConfig
	executor    Executor
	dockerPath  string
	limits      Limits
	environment []string
}

// NewManager constructs a manager using dockerPath (normally "docker") and a
// replaceable Executor. No subprocess or filesystem mutation occurs here.
func NewManager(profile Profile, manifest ManifestConfig, executor Executor, dockerPath string, limits Limits) (*Manager, error) {
	return NewManagerWithEnvironment(profile, manifest, executor, dockerPath, limits, nil)
}

// NewManagerWithEnvironment is NewManager with an explicit process
// environment for Compose interpolation and container startup. The manager
// copies the slice and never includes environment values in errors. Passing
// nil preserves the host environment for backwards-compatible callers.
func NewManagerWithEnvironment(profile Profile, manifest ManifestConfig, executor Executor, dockerPath string, limits Limits, environment []string) (*Manager, error) {
	if err := profile.Validate(); err != nil {
		return nil, err
	}
	if err := manifest.Validate(); err != nil {
		return nil, err
	}
	if executor == nil {
		return nil, errors.New("runtime executor is required")
	}
	probe := Command{
		Executable:     dockerPath,
		Timeout:        limits.LifecycleTimeout,
		MaxStdoutBytes: limits.MaxStdoutBytes,
		MaxStderrBytes: limits.MaxStderrBytes,
	}
	if err := probe.validate(); err != nil {
		return nil, err
	}
	if limits.LogsTimeout <= 0 || limits.LogsTimeout > maxCommandTimeout {
		return nil, fmt.Errorf("runtime logs timeout must be between 1 nanosecond and %s", maxCommandTimeout)
	}
	return &Manager{profile: profile, manifest: manifest, executor: executor, dockerPath: dockerPath, limits: limits, environment: append([]string(nil), environment...)}, nil
}

// Start creates or verifies the profile manifest and idempotently converges
// the local services to healthy running state.
func (m *Manager) Start(ctx context.Context) (Result, error) {
	return m.lifecycle(ctx, []string{"up", "--detach", "--wait", "--remove-orphans"}, m.limits.LifecycleTimeout)
}

// Stop idempotently stops services while preserving containers and named data
// volumes for a subsequent Start.
func (m *Manager) Stop(ctx context.Context) (Result, error) {
	return m.lifecycle(ctx, []string{"stop"}, m.limits.LifecycleTimeout)
}

// Restart idempotently recreates services from the current embedded manifest
// and waits for their health checks without deleting named volumes.
func (m *Manager) Restart(ctx context.Context) (Result, error) {
	return m.lifecycle(ctx, []string{"up", "--detach", "--wait", "--remove-orphans", "--force-recreate"}, m.limits.LifecycleTimeout)
}

// Pull downloads the configured images without changing running services.
// An upgrade can therefore fail before the currently running containers are
// disturbed.
func (m *Manager) Pull(ctx context.Context) (Result, error) {
	return m.lifecycle(ctx, []string{"pull"}, m.limits.LifecycleTimeout)
}

// Down removes this profile's containers and network. Named volumes remain
// unless removeVolumes is explicitly requested by a destructive uninstall.
func (m *Manager) Down(ctx context.Context, removeVolumes bool) (Result, error) {
	operation := []string{"down"}
	if removeVolumes {
		operation = append(operation, "--volumes")
	}
	return m.lifecycle(ctx, operation, m.limits.LifecycleTimeout)
}

// CheckDocker verifies that the configured Docker executable can reach a
// daemon. It is read-only and deliberately does not disclose the host
// environment or command arguments.
func (m *Manager) CheckDocker(ctx context.Context) (Result, error) {
	if m == nil {
		return Result{}, ErrInvalidCommand
	}
	return m.executor.Execute(ctx, Command{
		Executable:     m.dockerPath,
		Args:           []string{"version", "--format", "{{.Server.Version}}"},
		Timeout:        m.limits.LifecycleTimeout,
		MaxStdoutBytes: m.limits.MaxStdoutBytes,
		MaxStderrBytes: m.limits.MaxStderrBytes,
		Environment:    append([]string(nil), m.environment...),
	})
}

// ServerURL returns the loopback endpoint exposed by the selected manifest.
func (m *Manager) ServerURL() string {
	if m == nil {
		return ""
	}
	return "http://127.0.0.1:" + strconv.FormatUint(uint64(m.manifest.AppPort), 10)
}

// Status returns bounded machine-readable Compose state for all services. It
// does not mutate application or container state.
func (m *Manager) Status(ctx context.Context) (Result, error) {
	return m.lifecycle(ctx, []string{"ps", "--all", "--format", "json"}, m.limits.LifecycleTimeout)
}

// Logs returns bounded, color-free logs. Follow remains bounded by LogsTimeout
// and the configured stdout/stderr limits.
func (m *Manager) Logs(ctx context.Context, options LogsOptions) (Result, error) {
	if options.Tail < 1 || options.Tail > 10_000 {
		return Result{}, fmt.Errorf("%w: tail must be between 1 and 10000", ErrInvalidLogsOptions)
	}
	if options.Service != "" && options.Service != "fornix" && options.Service != "db" {
		return Result{}, fmt.Errorf("%w: service must be fornix or db", ErrInvalidLogsOptions)
	}
	args := []string{"logs", "--no-color", "--timestamps", "--tail", strconv.Itoa(options.Tail)}
	if options.Follow {
		args = append(args, "--follow")
	}
	if options.Service != "" {
		args = append(args, options.Service)
	}
	return m.lifecycle(ctx, args, m.limits.LogsTimeout)
}

func (m *Manager) lifecycle(ctx context.Context, operation []string, timeout time.Duration) (Result, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.profile.Validate(); err != nil {
		return Result{}, err
	}
	if err := m.ensureManifest(); err != nil {
		return Result{}, err
	}
	args := []string{
		"compose",
		"--ansi", "never",
		"--project-name", m.profile.ProjectName(),
		"--file", m.profile.ManifestPath(),
	}
	args = append(args, operation...)
	return m.executor.Execute(ctx, Command{
		Executable:     m.dockerPath,
		Args:           args,
		Dir:            m.profile.RuntimeDir(),
		Timeout:        timeout,
		MaxStdoutBytes: m.limits.MaxStdoutBytes,
		MaxStderrBytes: m.limits.MaxStderrBytes,
		Environment:    append([]string(nil), m.environment...),
	})
}

func (m *Manager) ensureManifest() error {
	content, err := RenderManifest(m.manifest)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(m.profile.RuntimeDir(), 0o700); err != nil {
		return fmt.Errorf("create runtime directory: %w", err)
	}
	if err := m.validateResolvedRuntimeDir(); err != nil {
		return err
	}
	if err := os.Chmod(m.profile.RuntimeDir(), 0o700); err != nil {
		return fmt.Errorf("secure runtime directory: %w", err)
	}
	if info, err := os.Lstat(m.profile.ManifestPath()); err == nil && info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("%w: runtime manifest must not be a symlink", ErrUnsafeRuntimePath)
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect runtime manifest: %w", err)
	}
	if existing, err := os.ReadFile(m.profile.ManifestPath()); err == nil && string(existing) == string(content) {
		if err := os.Chmod(m.profile.ManifestPath(), 0o600); err != nil {
			return fmt.Errorf("secure runtime manifest: %w", err)
		}
		return nil
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read runtime manifest: %w", err)
	}

	temporary, err := os.CreateTemp(m.profile.RuntimeDir(), ".compose-*.tmp")
	if err != nil {
		return fmt.Errorf("create runtime manifest temporary file: %w", err)
	}
	temporaryName := temporary.Name()
	removeTemporary := true
	defer func() {
		if removeTemporary {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("secure runtime manifest: %w", err)
	}
	if _, err := temporary.Write(content); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write runtime manifest: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync runtime manifest: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close runtime manifest: %w", err)
	}
	if err := os.Rename(temporaryName, m.profile.ManifestPath()); err != nil {
		return fmt.Errorf("replace runtime manifest: %w", err)
	}
	removeTemporary = false
	return nil
}

func (m *Manager) validateResolvedRuntimeDir() error {
	resolvedRoot, err := filepath.EvalSymlinks(m.profile.RootDir())
	if err != nil {
		return fmt.Errorf("%w: resolve profile root: %v", ErrUnsafeRuntimePath, err)
	}
	resolvedRuntime, err := filepath.EvalSymlinks(m.profile.RuntimeDir())
	if err != nil {
		return fmt.Errorf("%w: resolve runtime directory: %v", ErrUnsafeRuntimePath, err)
	}
	if !pathWithin(resolvedRoot, filepath.Join(resolvedRuntime, manifestFilename)) {
		return fmt.Errorf("%w: runtime directory escapes profile root", ErrUnsafeRuntimePath)
	}
	return nil
}
