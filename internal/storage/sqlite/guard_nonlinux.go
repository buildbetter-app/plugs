//go:build !linux

package sqlite

import "os"

type guardHandle interface {
	release()
}

type fileGuard struct {
	file *os.File
}

func (g *fileGuard) release() {
	if g == nil || g.file == nil {
		return
	}
	unlockFile(g.file)
	g.file = nil
}

func tryGuardLock(path string, share bool) (guardHandle, bool, error) {
	f, ok, err := tryFileLock(path, share)
	if err != nil || !ok {
		return nil, ok, err
	}
	return &fileGuard{file: f}, true, nil
}

func releaseGuards(guards []guardHandle) {
	for _, guard := range guards {
		if guard != nil {
			guard.release()
		}
	}
}
