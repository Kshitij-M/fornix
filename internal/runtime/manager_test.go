package runtime

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
)

type recordingExecutor struct {
	mu       sync.Mutex
	commands []Command
	result   Result
	err      error
}

func (e *recordingExecutor) Execute(_ context.Context, command Command) (Result, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	command.Args = append([]string(nil), command.Args...)
	e.commands = append(e.commands, command)
	return e.result, e.err
}

func (e *recordingExecutor) snapshot() []Command {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]Command(nil), e.commands...)
}

func TestManagerLifecycleUsesDeterministicComposeArguments(t *testing.T) {
	manager, executor, profile := testManager(t)
	ctx := context.Background()
	operations := []struct {
		name string
		run  func() error
		args []string
	}{
		{name: "start", run: func() error { _, err := manager.Start(ctx); return err }, args: []string{"up", "--detach", "--wait", "--remove-orphans"}},
		{name: "start again", run: func() error { _, err := manager.Start(ctx); return err }, args: []string{"up", "--detach", "--wait", "--remove-orphans"}},
		{name: "stop", run: func() error { _, err := manager.Stop(ctx); return err }, args: []string{"stop"}},
		{name: "restart", run: func() error { _, err := manager.Restart(ctx); return err }, args: []string{"up", "--detach", "--wait", "--remove-orphans", "--force-recreate"}},
		{name: "status", run: func() error { _, err := manager.Status(ctx); return err }, args: []string{"ps", "--all", "--format", "json"}},
		{name: "logs", run: func() error { _, err := manager.Logs(ctx, LogsOptions{Service: "fornix", Tail: 50}); return err }, args: []string{"logs", "--no-color", "--timestamps", "--tail", "50", "fornix"}},
	}
	for _, operation := range operations {
		t.Run(operation.name, func(t *testing.T) {
			if err := operation.run(); err != nil {
				t.Fatalf("operation: %v", err)
			}
		})
	}

	commands := executor.snapshot()
	if len(commands) != len(operations) {
		t.Fatalf("commands = %d, want %d", len(commands), len(operations))
	}
	prefix := []string{"compose", "--ansi", "never", "--project-name", "fornix-local", "--file", profile.ManifestPath()}
	for index, operation := range operations {
		want := append(append([]string(nil), prefix...), operation.args...)
		if !reflect.DeepEqual(commands[index].Args, want) {
			t.Errorf("command %d args = %q, want %q", index, commands[index].Args, want)
		}
		if commands[index].Executable != "docker" || commands[index].Dir != profile.RuntimeDir() {
			t.Errorf("command %d has unexpected boundary: %+v", index, commands[index])
		}
	}

	manifest, err := os.ReadFile(profile.ManifestPath())
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	info, err := os.Stat(profile.ManifestPath())
	if err != nil {
		t.Fatalf("stat manifest: %v", err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Errorf("manifest permissions = %o, want 600", info.Mode().Perm())
	}
	if len(manifest) == 0 {
		t.Fatal("manifest is empty")
	}
}

func TestManagerPropagatesBoundedExecutorFailure(t *testing.T) {
	manager, executor, _ := testManager(t)
	executor.result = Result{Stderr: "bounded", Truncated: true}
	executor.err = ErrCommandOutputLimit
	result, err := manager.Start(context.Background())
	if !errors.Is(err, ErrCommandOutputLimit) || !result.Truncated {
		t.Fatalf("result=%+v error=%v, want bounded output failure", result, err)
	}
}

func TestManagerRejectsUnboundedOrUnknownLogs(t *testing.T) {
	manager, executor, _ := testManager(t)
	for _, options := range []LogsOptions{
		{Tail: 0},
		{Tail: 10_001},
		{Tail: 10, Service: "unknown"},
	} {
		if _, err := manager.Logs(context.Background(), options); !errors.Is(err, ErrInvalidLogsOptions) {
			t.Fatalf("options=%+v error=%v, want ErrInvalidLogsOptions", options, err)
		}
	}
	if got := len(executor.snapshot()); got != 0 {
		t.Fatalf("executor called %d times for invalid logs", got)
	}
}

func TestManagerRejectsRuntimeDirectorySymlinkEscape(t *testing.T) {
	root := filepath.Join(t.TempDir(), "profile")
	outside := t.TempDir()
	if err := os.MkdirAll(root, 0o700); err != nil {
		t.Fatalf("mkdir root: %v", err)
	}
	if err := os.Symlink(outside, filepath.Join(root, "runtime")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	profile, err := NewProfile(root, "local")
	if err != nil {
		t.Fatalf("NewProfile: %v", err)
	}
	config, err := DefaultManifestConfig("v0.10.1")
	if err != nil {
		t.Fatalf("DefaultManifestConfig: %v", err)
	}
	manager, err := NewManager(profile, config, &recordingExecutor{}, "docker", DefaultLimits())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	if _, err := manager.Start(context.Background()); !errors.Is(err, ErrUnsafeRuntimePath) {
		t.Fatalf("error = %v, want ErrUnsafeRuntimePath", err)
	}
}

func TestManagerRejectsManifestSymlinkAndRepairsPermissions(t *testing.T) {
	t.Run("manifest symlink", func(t *testing.T) {
		manager, _, profile := testManager(t)
		if err := os.MkdirAll(profile.RuntimeDir(), 0o700); err != nil {
			t.Fatalf("mkdir runtime: %v", err)
		}
		target := filepath.Join(t.TempDir(), "outside.yaml")
		if err := os.WriteFile(target, []byte("outside"), 0o600); err != nil {
			t.Fatalf("write target: %v", err)
		}
		if err := os.Symlink(target, profile.ManifestPath()); err != nil {
			t.Skipf("symlinks unavailable: %v", err)
		}
		if _, err := manager.Start(context.Background()); !errors.Is(err, ErrUnsafeRuntimePath) {
			t.Fatalf("error = %v, want ErrUnsafeRuntimePath", err)
		}
	})

	t.Run("permissions", func(t *testing.T) {
		manager, _, profile := testManager(t)
		if _, err := manager.Start(context.Background()); err != nil {
			t.Fatalf("Start first: %v", err)
		}
		if err := os.Chmod(profile.ManifestPath(), 0o644); err != nil {
			t.Fatalf("chmod manifest: %v", err)
		}
		if err := os.Chmod(profile.RuntimeDir(), 0o755); err != nil {
			t.Fatalf("chmod runtime: %v", err)
		}
		if _, err := manager.Start(context.Background()); err != nil {
			t.Fatalf("Start second: %v", err)
		}
		manifestInfo, err := os.Stat(profile.ManifestPath())
		if err != nil {
			t.Fatalf("stat manifest: %v", err)
		}
		runtimeInfo, err := os.Stat(profile.RuntimeDir())
		if err != nil {
			t.Fatalf("stat runtime: %v", err)
		}
		if manifestInfo.Mode().Perm() != 0o600 || runtimeInfo.Mode().Perm() != 0o700 {
			t.Fatalf("permissions = manifest %o runtime %o, want 600 and 700", manifestInfo.Mode().Perm(), runtimeInfo.Mode().Perm())
		}
	})
}

func TestManagerLogsFollowIsExplicitAndBounded(t *testing.T) {
	manager, executor, _ := testManager(t)
	if _, err := manager.Logs(context.Background(), LogsOptions{Service: "db", Tail: 1, Follow: true}); err != nil {
		t.Fatalf("Logs: %v", err)
	}
	command := executor.snapshot()[0]
	wantSuffix := []string{"logs", "--no-color", "--timestamps", "--tail", "1", "--follow", "db"}
	if !reflect.DeepEqual(command.Args[len(command.Args)-len(wantSuffix):], wantSuffix) {
		t.Fatalf("args = %q, want suffix %q", command.Args, wantSuffix)
	}
	if command.Timeout != DefaultLimits().LogsTimeout {
		t.Fatalf("timeout = %v, want %v", command.Timeout, DefaultLimits().LogsTimeout)
	}
}

func TestManagerSupportsPullDownAndDockerCheckWithExplicitEnvironment(t *testing.T) {
	manager, executor, _ := testManager(t)
	config, err := DefaultManifestConfig("v0.10.1")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	runtimeProfile, err := NewProfile(filepath.Join(root, "local"), "local")
	if err != nil {
		t.Fatal(err)
	}
	manager, err = NewManagerWithEnvironment(runtimeProfile, config, executor, "docker", DefaultLimits(), []string{"FORNIX_BOOTSTRAP_KEY=redacted-test-value"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.CheckDocker(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Pull(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Down(context.Background(), true); err != nil {
		t.Fatal(err)
	}
	commands := executor.snapshot()
	if len(commands) != 3 {
		t.Fatalf("command count = %d, want 3", len(commands))
	}
	if commands[0].Args[0] != "version" || commands[1].Args[len(commands[1].Args)-1] != "pull" {
		t.Fatalf("unexpected check/pull commands: %+v", commands)
	}
	if got := commands[2].Args[len(commands[2].Args)-1]; got != "--volumes" {
		t.Fatalf("down command suffix = %q, want --volumes", got)
	}
	if len(commands[0].Environment) == 0 || commands[0].Environment[0] != "FORNIX_BOOTSTRAP_KEY=redacted-test-value" {
		t.Fatalf("explicit environment was not preserved: %+v", commands[0].Environment)
	}
}

func testManager(t *testing.T) (*Manager, *recordingExecutor, Profile) {
	t.Helper()
	root := filepath.Join(t.TempDir(), "profiles", "local")
	profile, err := NewProfile(root, "local")
	if err != nil {
		t.Fatalf("NewProfile: %v", err)
	}
	config, err := DefaultManifestConfig("v0.10.1")
	if err != nil {
		t.Fatalf("DefaultManifestConfig: %v", err)
	}
	executor := &recordingExecutor{}
	manager, err := NewManager(profile, config, executor, "docker", DefaultLimits())
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}
	return manager, executor, profile
}
