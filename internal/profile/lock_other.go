//go:build !darwin && !linux && !windows

package profile

import (
	"errors"
	"os"
)

func tryFileLock(_ *os.File) error {
	return errors.New("profile locking is unsupported on this platform")
}

func unlockFile(_ *os.File) error { return nil }
