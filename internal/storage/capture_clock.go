package storage

import (
	"time"

	"github.com/steveyegge/beads/internal/captureclock"
)

// CaptureNow preserves time.Now for ordinary commands. The explicit disposable
// capture opt-in controls only the native wall-time seams that call it.
func CaptureNow() time.Time { return captureclock.Now() }
