//go:build darwin || linux

package sandboxrunner

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// validateSocketDirectory protects the socket path from replacement by a
// different local account. The leaf directory must belong to this process and
// be private; ancestors must be root- or process-owned and not writable by
// other users. A root-owned sticky temporary directory is the sole exception.
func validateSocketDirectory(path string) error {
	absolute, err := filepath.Abs(path)
	if err != nil || absolute != path {
		return fmt.Errorf("sandbox runner socket directory must be absolute")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("sandbox runner socket directory chain is unsafe")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint32(stat.Uid) != uint32(os.Geteuid()) || info.Mode().Perm()&0o077 != 0 || info.Mode().Perm()&0o700 != 0o700 {
		return fmt.Errorf("sandbox runner socket directory must be private and owned by the current user")
	}
	return validateSocketAncestors(filepath.Dir(path))
}

func ensureSocketDirectory(path string) error {
	absolute, err := filepath.Abs(path)
	if err != nil || absolute != path {
		return fmt.Errorf("sandbox runner socket directory must be absolute")
	}
	current := string(filepath.Separator)
	for _, component := range strings.Split(strings.TrimPrefix(path, current), current) {
		if component == "" {
			continue
		}
		if err := validateSocketAncestors(current); err != nil {
			return err
		}
		next := filepath.Join(current, component)
		info, statErr := os.Lstat(next)
		if errors.Is(statErr, os.ErrNotExist) {
			mkdirErr := os.Mkdir(next, 0o700)
			if mkdirErr != nil && !errors.Is(mkdirErr, os.ErrExist) {
				return fmt.Errorf("create private sandbox runner socket directory")
			}
			info, statErr = os.Lstat(next)
		}
		if statErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("sandbox runner socket directory chain is unsafe")
		}
		current = next
	}
	return nil
}

func validateSocketAncestors(path string) error {
	absolute, err := filepath.Abs(path)
	if err != nil || absolute != path {
		return fmt.Errorf("sandbox runner socket directory must be absolute")
	}
	euid := uint32(os.Geteuid())
	for current := path; ; current = filepath.Dir(current) {
		info, statErr := os.Lstat(current)
		if statErr != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("sandbox runner socket directory chain is unsafe")
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok {
			return fmt.Errorf("sandbox runner socket directory ownership is unavailable")
		}
		owner := uint32(stat.Uid)
		if owner != 0 && owner != euid {
			return fmt.Errorf("sandbox runner socket directory chain has an untrusted owner")
		}
		permissions := info.Mode().Perm()
		if permissions&0o022 != 0 {
			stickyRootTemp := owner == 0 && info.Mode()&os.ModeSticky != 0
			if !stickyRootTemp {
				return fmt.Errorf("sandbox runner socket directory has a replaceable ancestor")
			}
		}
		if current == string(filepath.Separator) {
			break
		}
		parent := filepath.Dir(current)
		if parent == current {
			return fmt.Errorf("sandbox runner socket directory chain is invalid")
		}
	}
	return nil
}
