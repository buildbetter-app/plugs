//go:build !linux && !darwin && !freebsd && !openbsd && !netbsd

package sqlite

import (
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"os"
)

func tryFileLock(string, bool) (*os.File, bool, error) { return nil, false, driver.ErrUnsupported }
func validateDatabaseFile(*os.File) error              { return driver.ErrUnsupported }
func unlockFile(f *os.File) {
	if f != nil {
		_ = f.Close()
	}
}
