//go:build linux

package sqlite

import (
	"syscall"
)

// guardHandle keeps one stripe's flock alive without retaining a descriptor.
// Linux keeps the file reference associated with this mapping after close,
// preserving interprocess exclusion. Lock acquires at most guardStripeCount
// handles, bounding both descriptors and VMAs independently of the key count.
type guardHandle interface {
	release()
}

type mappedGuard struct {
	mapping []byte
}

func (g *mappedGuard) release() {
	if g == nil || g.mapping == nil {
		return
	}
	_ = syscall.Munmap(g.mapping)
	g.mapping = nil
}

func tryGuardLock(path string, share bool) (guardHandle, bool, error) {
	f, ok, err := tryFileLock(path, share)
	if err != nil || !ok {
		return nil, ok, err
	}
	mapping, err := syscall.Mmap(int(f.Fd()), 0, 1, syscall.PROT_NONE, syscall.MAP_SHARED)
	if err != nil {
		unlockFile(f)
		return nil, false, err
	}
	if err := f.Close(); err != nil {
		_ = syscall.Munmap(mapping)
		return nil, false, err
	}
	return &mappedGuard{mapping: mapping}, true, nil
}

func releaseGuards(guards []guardHandle) {
	for _, guard := range guards {
		if guard != nil {
			guard.release()
		}
	}
}
