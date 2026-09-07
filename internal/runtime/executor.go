package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

const (
	maxCommandOutputBytes = 16 << 20
	maxCommandTimeout     = 30 * time.Minute
)

var (
	// ErrInvalidCommand reports an executable request that violates the
	// structured-command or resource-bound contract.
	ErrInvalidCommand = errors.New("invalid runtime command")
	// ErrCommandOutputLimit reports that stdout or stderr exceeded its bound.
	ErrCommandOutputLimit = errors.New("runtime command output limit exceeded")
	// ErrCommandTimeout reports that a runtime command exceeded its deadline.
	ErrCommandTimeout = errors.New("runtime command timed out")
)

// Command describes one subprocess without shell interpretation. Args are
// passed directly to the executable. Environment is nil to inherit the caller's
// environment or an explicit complete environment when non-nil.
type Command struct {
	Executable     string
	Args           []string
	Dir            string
	Environment    []string
	Timeout        time.Duration
	MaxStdoutBytes int
	MaxStderrBytes int
}

// Result is bounded diagnostic output for one command attempt. Output may be
// truncated only when Truncated is true and ErrCommandOutputLimit is returned.
type Result struct {
	Stdout    string
	Stderr    string
	ExitCode  int
	Duration  time.Duration
	Truncated bool
}

// Executor is the process boundary used by Manager. Implementations must not
// invoke an implicit shell and must preserve Command's timeout and output
// limits. Tests can provide a recorder without requiring Docker.
type Executor interface {
	Execute(ctx context.Context, command Command) (Result, error)
}

// CommandError reports a non-zero process exit without including arguments,
// environment values, or unbounded process output in the error string.
type CommandError struct {
	Executable string
	ExitCode   int
}

// Error implements error while keeping potentially sensitive command details
// out of logs.
func (e *CommandError) Error() string {
	return fmt.Sprintf("runtime executable %q failed with exit code %d", e.Executable, e.ExitCode)
}

// OSExecutor runs structured commands on the host operating system.
type OSExecutor struct{}

// Execute runs command directly, captures bounded stdout and stderr, and
// returns stable timeout, output-limit, cancellation, and exit errors.
func (OSExecutor) Execute(parent context.Context, command Command) (Result, error) {
	if err := command.validate(); err != nil {
		return Result{}, err
	}
	started := time.Now()
	ctx, cancel := context.WithTimeout(parent, command.Timeout)
	defer cancel()

	stdout := newBoundedBuffer(command.MaxStdoutBytes, cancel)
	stderr := newBoundedBuffer(command.MaxStderrBytes, cancel)
	cmd := exec.CommandContext(ctx, command.Executable, command.Args...)
	cmd.Dir = command.Dir
	if command.Environment != nil {
		cmd.Env = append([]string(nil), command.Environment...)
	} else {
		cmd.Env = os.Environ()
	}
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	err := cmd.Run()

	result := Result{
		Stdout:    stdout.String(),
		Stderr:    stderr.String(),
		ExitCode:  0,
		Duration:  time.Since(started),
		Truncated: stdout.Overflow() || stderr.Overflow(),
	}
	if result.Truncated {
		return result, ErrCommandOutputLimit
	}
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return result, ErrCommandTimeout
	}
	if parent.Err() != nil {
		return result, parent.Err()
	}
	if err == nil {
		return result, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		result.ExitCode = exitErr.ExitCode()
		return result, &CommandError{Executable: command.Executable, ExitCode: result.ExitCode}
	}
	return result, fmt.Errorf("execute runtime command: %w", err)
}

func (c Command) validate() error {
	if strings.TrimSpace(c.Executable) == "" || strings.IndexByte(c.Executable, 0) >= 0 {
		return fmt.Errorf("%w: executable is required", ErrInvalidCommand)
	}
	for _, arg := range c.Args {
		if strings.IndexByte(arg, 0) >= 0 {
			return fmt.Errorf("%w: argument contains a NUL byte", ErrInvalidCommand)
		}
	}
	if c.Timeout <= 0 || c.Timeout > maxCommandTimeout {
		return fmt.Errorf("%w: timeout must be between 1 nanosecond and %s", ErrInvalidCommand, maxCommandTimeout)
	}
	if c.MaxStdoutBytes <= 0 || c.MaxStdoutBytes > maxCommandOutputBytes || c.MaxStderrBytes <= 0 || c.MaxStderrBytes > maxCommandOutputBytes {
		return fmt.Errorf("%w: output limits must be between 1 and %d bytes", ErrInvalidCommand, maxCommandOutputBytes)
	}
	return nil
}

type boundedBuffer struct {
	mu       sync.Mutex
	buffer   bytes.Buffer
	limit    int
	overflow bool
	cancel   context.CancelFunc
}

func newBoundedBuffer(limit int, cancel context.CancelFunc) *boundedBuffer {
	return &boundedBuffer{limit: limit, cancel: cancel}
}

func (b *boundedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	remaining := b.limit - b.buffer.Len()
	if remaining > 0 {
		writeLen := len(data)
		if writeLen > remaining {
			writeLen = remaining
		}
		_, _ = b.buffer.Write(data[:writeLen])
	}
	if len(data) > remaining {
		b.overflow = true
		b.cancel()
	}
	return len(data), nil
}

func (b *boundedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buffer.String()
}

func (b *boundedBuffer) Overflow() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.overflow
}
