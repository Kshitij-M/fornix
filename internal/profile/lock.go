package profile

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const lockFilename = ".lock"

var errLockBusy = errors.New("profile lock busy")

// Lock is an acquired, profile-wide advisory lock. It coordinates independent
// Fornix CLI processes; Release is idempotent. The lock file is retained to
// avoid inode replacement races and contains no profile or credential data.
type Lock struct {
	file *os.File
	once sync.Once
	err  error
}

// Acquire waits until the profile-wide lock is available. The supplied root
// must be an explicit absolute path and is created with private permissions.
func Acquire(root string) (*Lock, error) {
	return AcquireContext(context.Background(), root)
}

// AcquireContext is Acquire with caller-controlled cancellation and timeout.
func AcquireContext(ctx context.Context, root string) (*Lock, error) {
	if ctx == nil {
		return nil, errors.New("acquire profile lock: nil context")
	}
	root, err := ensurePrivateDirectory(root)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(root, lockFilename)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, FileMode)
	if err != nil {
		return nil, fmt.Errorf("open profile lock: %w", err)
	}
	if err := file.Chmod(FileMode); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("restrict profile lock: %w", err)
	}
	for {
		err = tryFileLock(file)
		if err == nil {
			return &Lock{file: file}, nil
		}
		if !errors.Is(err, errLockBusy) {
			_ = file.Close()
			return nil, fmt.Errorf("acquire profile lock: %w", err)
		}
		timer := time.NewTimer(20 * time.Millisecond)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			_ = file.Close()
			return nil, fmt.Errorf("acquire profile lock: %w", ctx.Err())
		case <-timer.C:
		}
	}
}

// Release unlocks and closes the underlying file. Calls after the first return
// the same result and do not unlock a later owner's file descriptor.
func (l *Lock) Release() error {
	if l == nil {
		return nil
	}
	l.once.Do(func() {
		if l.file == nil {
			return
		}
		if err := unlockFile(l.file); err != nil {
			l.err = fmt.Errorf("release profile lock: %w", err)
		}
		if err := l.file.Close(); err != nil && l.err == nil {
			l.err = fmt.Errorf("close profile lock: %w", err)
		}
	})
	return l.err
}
