// Package acceptance provides deterministic inputs and byte-level traces for
// scenarios that exercise the real application through its in-process transport.
package acceptance

import (
	"fmt"
	"sync"
	"time"
)

// Inputs must be installed in every reached application clock and ID seam.
// Exhausting the supplied IDs is a fixture error, never a random fallback.
type Inputs struct {
	mu   sync.Mutex
	now  time.Time
	ids  []string
	next int
}

func NewInputs(now time.Time, ids ...string) (*Inputs, error) {
	if now.IsZero() {
		return nil, fmt.Errorf("acceptance clock must be explicit")
	}
	seen := make(map[string]bool, len(ids))
	for _, id := range ids {
		if id == "" || seen[id] {
			return nil, fmt.Errorf("acceptance IDs must be nonempty and unique: %q", id)
		}
		seen[id] = true
	}
	return &Inputs{now: now.UTC(), ids: append([]string(nil), ids...)}, nil
}

func (in *Inputs) Now() time.Time {
	in.mu.Lock()
	defer in.mu.Unlock()
	return in.now
}

func (in *Inputs) Advance(d time.Duration) error {
	in.mu.Lock()
	defer in.mu.Unlock()
	if d < 0 {
		return fmt.Errorf("acceptance clock cannot move backward")
	}
	in.now = in.now.Add(d)
	return nil
}

func (in *Inputs) NextID() (string, error) {
	in.mu.Lock()
	defer in.mu.Unlock()
	if in.next == len(in.ids) {
		return "", fmt.Errorf("acceptance ID input exhausted after %d IDs", in.next)
	}
	id := in.ids[in.next]
	in.next++
	return id, nil
}
