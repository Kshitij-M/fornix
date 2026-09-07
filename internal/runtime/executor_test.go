package runtime

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"
)

func TestOSExecutorCapturesStructuredOutputAndExit(t *testing.T) {
	command := helperCommand("output", "hello", "warning", "7")
	result, err := (OSExecutor{}).Execute(context.Background(), command)
	var commandErr *CommandError
	if !errors.As(err, &commandErr) {
		t.Fatalf("error = %v, want CommandError", err)
	}
	if result.Stdout != "hello" || result.Stderr != "warning" || result.ExitCode != 7 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if strings.Contains(err.Error(), "hello") || strings.Contains(err.Error(), "warning") {
		t.Fatalf("command error leaked captured output: %v", err)
	}
}

func TestOSExecutorEnforcesOutputLimit(t *testing.T) {
	command := helperCommand("repeat", "x", "128")
	command.MaxStdoutBytes = 16
	result, err := (OSExecutor{}).Execute(context.Background(), command)
	if !errors.Is(err, ErrCommandOutputLimit) {
		t.Fatalf("error = %v, want ErrCommandOutputLimit", err)
	}
	if !result.Truncated || len(result.Stdout) != 16 {
		t.Fatalf("unexpected bounded result: %+v", result)
	}
}

func TestOSExecutorEnforcesTimeoutAndCancellation(t *testing.T) {
	command := helperCommand("sleep", "500")
	command.Timeout = 20 * time.Millisecond
	if _, err := (OSExecutor{}).Execute(context.Background(), command); !errors.Is(err, ErrCommandTimeout) {
		t.Fatalf("timeout error = %v, want ErrCommandTimeout", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	command.Timeout = time.Second
	if _, err := (OSExecutor{}).Execute(ctx, command); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation error = %v, want context.Canceled", err)
	}
}

func TestOSExecutorRejectsUnboundedCommand(t *testing.T) {
	_, err := (OSExecutor{}).Execute(context.Background(), Command{Executable: "docker"})
	if !errors.Is(err, ErrInvalidCommand) {
		t.Fatalf("error = %v, want ErrInvalidCommand", err)
	}
}

func helperCommand(arguments ...string) Command {
	args := []string{"-test.run=TestRuntimeExecutorHelperProcess", "--"}
	args = append(args, arguments...)
	return Command{
		Executable:     os.Args[0],
		Args:           args,
		Environment:    append(os.Environ(), "GO_WANT_RUNTIME_HELPER_PROCESS=1"),
		Timeout:        time.Second,
		MaxStdoutBytes: 1024,
		MaxStderrBytes: 1024,
	}
}

func TestRuntimeExecutorHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_RUNTIME_HELPER_PROCESS") != "1" {
		return
	}
	separator := 0
	for index, arg := range os.Args {
		if arg == "--" {
			separator = index + 1
			break
		}
	}
	args := os.Args[separator:]
	switch args[0] {
	case "output":
		_, _ = fmt.Fprint(os.Stdout, args[1])
		_, _ = fmt.Fprint(os.Stderr, args[2])
		var code int
		_, _ = fmt.Sscanf(args[3], "%d", &code)
		os.Exit(code)
	case "repeat":
		var count int
		_, _ = fmt.Sscanf(args[2], "%d", &count)
		_, _ = fmt.Fprint(os.Stdout, strings.Repeat(args[1], count))
		os.Exit(0)
	case "sleep":
		var milliseconds int
		_, _ = fmt.Sscanf(args[1], "%d", &milliseconds)
		time.Sleep(time.Duration(milliseconds) * time.Millisecond)
		os.Exit(0)
	default:
		os.Exit(2)
	}
}
