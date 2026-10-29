//go:build linux || darwin || freebsd || openbsd || netbsd

package sqlite

import (
	"errors"
	"os"
	"syscall"
)

func tryFileLock(path string, share bool) (*os.File, bool, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, false, err
	}
	op := syscall.LOCK_EX
	if share {
		op = syscall.LOCK_SH
	}
	err = syscall.Flock(int(f.Fd()), op|syscall.LOCK_NB)
	if err == nil {
		return f, true, nil
	}
	_ = f.Close()
	if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
		return nil, false, nil
	}
	return nil, false, err
}

// Closing the descriptor also unlocks it, including when a process exits.
func unlockFile(f *os.File) {
	if f != nil {
		_ = f.Close()
	}
}
