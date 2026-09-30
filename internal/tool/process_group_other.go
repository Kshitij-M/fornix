//go:build !unix

package tool

import (
	"os/exec"
	"time"
)

func supportsProcessGroupTermination() bool { return false }

func configureProcessGroup(cmd *exec.Cmd) {
	cmd.WaitDelay = 2 * time.Second
}

func terminateProcessGroup(*exec.Cmd) {}
