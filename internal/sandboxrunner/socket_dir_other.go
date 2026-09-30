//go:build !darwin && !linux

package sandboxrunner

import "fmt"

func validateSocketDirectory(string) error {
	return fmt.Errorf("sandbox runner Unix socket ownership checks are unsupported on this platform")
}

func ensureSocketDirectory(string) error {
	return fmt.Errorf("sandbox runner Unix socket ownership checks are unsupported on this platform")
}
