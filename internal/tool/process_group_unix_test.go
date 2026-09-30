//go:build unix

package tool

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/omaveda/fornix/internal/contracts"
)

func TestProcessGroupChildHelper(t *testing.T) {
	if os.Getenv("FORNIX_PROCESS_GROUP_HELPER") != "1" {
		return
	}
	child := exec.Command("/bin/sleep", "30")
	child.Stdin, child.Stdout, child.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := child.Start(); err != nil {
		os.Exit(71)
	}
	pidFile := os.Getenv("FORNIX_PROCESS_GROUP_PID_FILE")
	if err := os.WriteFile(pidFile, []byte(strconv.Itoa(child.Process.Pid)), 0o600); err != nil {
		os.Exit(72)
	}
	time.Sleep(30 * time.Second)
}

func TestProcessGroupOutputHelper(t *testing.T) {
	if os.Getenv("FORNIX_PROCESS_GROUP_OUTPUT_HELPER") != "1" {
		return
	}
	buffer := make([]byte, 4096)
	for {
		if _, err := os.Stdout.Write(buffer); err != nil {
			return
		}
	}
}

func TestLocalExecutorTimeoutTerminatesProcessGroup(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	definition := testDefinition(executable)
	definition.ArgvPrefix = []string{"-test.run=^TestProcessGroupChildHelper$"}
	definition.AllowedEnvKeys = []string{"FORNIX_PROCESS_GROUP_HELPER", "FORNIX_PROCESS_GROUP_PID_FILE"}
	definition.Sandbox.TimeoutMS = 1000
	request := testRequest()
	request.Argv = append([]string{executable}, definition.ArgvPrefix...)
	request.Environment = map[string]string{
		"FORNIX_PROCESS_GROUP_HELPER":   "1",
		"FORNIX_PROCESS_GROUP_PID_FILE": pidFile,
	}
	request.Budget = definition.Sandbox
	result, err := (LocalExecutor{}).Run(context.Background(), definition, request)
	if err != nil || result.Failure == nil || result.Failure.Code != contracts.ToolFailureTimeout {
		t.Fatalf("process group execution did not time out: result=%#v err=%v", result, err)
	}
	rawPID, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatalf("helper did not start its child: %v", err)
	}
	childPID, err := strconv.Atoi(string(rawPID))
	if err != nil {
		t.Fatalf("invalid child pid %q: %v", rawPID, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		err = syscall.Kill(childPID, 0)
		if errors.Is(err, syscall.ESRCH) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("descendant process %d remained after the process-group deadline", childPID)
}

func TestLocalExecutorParentCancellationTerminatesProcessGroup(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	pidFile := filepath.Join(t.TempDir(), "child.pid")
	definition := testDefinition(executable)
	definition.ArgvPrefix = []string{"-test.run=^TestProcessGroupChildHelper$"}
	definition.AllowedEnvKeys = []string{"FORNIX_PROCESS_GROUP_HELPER", "FORNIX_PROCESS_GROUP_PID_FILE"}
	definition.Sandbox.TimeoutMS = 5000
	request := testRequest()
	request.Argv = append([]string{executable}, definition.ArgvPrefix...)
	request.Environment = map[string]string{
		"FORNIX_PROCESS_GROUP_HELPER":   "1",
		"FORNIX_PROCESS_GROUP_PID_FILE": pidFile,
	}
	request.Budget = definition.Sandbox
	ctx, cancel := context.WithCancel(context.Background())
	type execution struct {
		result contracts.ToolResult
		err    error
	}
	finished := make(chan execution, 1)
	go func() {
		result, runErr := (LocalExecutor{}).Run(ctx, definition, request)
		finished <- execution{result: result, err: runErr}
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(pidFile); err == nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := os.Stat(pidFile); err != nil {
		cancel()
		t.Fatalf("helper did not start its child: %v", err)
	}
	cancel()
	var completed execution
	select {
	case completed = <-finished:
	case <-time.After(4 * time.Second):
		t.Fatal("parent cancellation did not stop the process group promptly")
	}
	if completed.err != nil || completed.result.Failure == nil || completed.result.Failure.Code != contracts.ToolFailureCancelled {
		t.Fatalf("parent cancellation returned the wrong outcome: result=%#v err=%v", completed.result, completed.err)
	}
	rawPID, err := os.ReadFile(pidFile)
	if err != nil {
		t.Fatal(err)
	}
	childPID, err := strconv.Atoi(string(rawPID))
	if err != nil {
		t.Fatalf("invalid child pid %q: %v", rawPID, err)
	}
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if errors.Is(syscall.Kill(childPID, 0), syscall.ESRCH) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("descendant process %d remained after parent cancellation", childPID)
}

func TestLocalExecutorOutputLimitTerminatesProcessGroup(t *testing.T) {
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	definition := testDefinition(executable)
	definition.ArgvPrefix = []string{"-test.run=^TestProcessGroupOutputHelper$"}
	definition.AllowedEnvKeys = []string{"FORNIX_PROCESS_GROUP_OUTPUT_HELPER"}
	definition.Sandbox.MaxStdoutBytes = 64
	definition.Sandbox.MaxStderrBytes = 64
	request := testRequest()
	request.Argv = append([]string{executable}, definition.ArgvPrefix...)
	request.Environment = map[string]string{"FORNIX_PROCESS_GROUP_OUTPUT_HELPER": "1"}
	request.Budget = definition.Sandbox
	result, err := (LocalExecutor{}).Run(context.Background(), definition, request)
	if err != nil || result.Failure == nil || result.Failure.Code != contracts.ToolFailureOutputLimit {
		t.Fatalf("output overflow did not stop the process group: result=%#v err=%v", result, err)
	}
}
