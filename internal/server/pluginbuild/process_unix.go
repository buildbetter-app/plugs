//go:build linux || darwin || freebsd

package pluginbuild

import (
	"errors"
	"os"
	"os/exec"
	"syscall"
	"time"
)

func configureBuildProcess(command *exec.Cmd) {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Cancel = func() error {
		// Stop the compiler/linker children as well as the Go command.
		err := syscall.Kill(-command.Process.Pid, syscall.SIGKILL)
		if errors.Is(err, syscall.ESRCH) {
			return os.ErrProcessDone
		}
		return err
	}
	command.WaitDelay = time.Second
}
