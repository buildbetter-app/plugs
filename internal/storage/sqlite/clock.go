package sqlite

import "time"

func (s *Store) now() time.Time {
	if s.clock != nil {
		return s.clock()
	}
	return time.Now()
}

// leaseTimes keeps the existing single time.Now sample on the production
// path. Only the durable timestamp changes with an injected clock; the real
// timer still starts at this sample and retains the unchanged TTL budget.
func (s *Store) leaseTimes(ttl time.Duration) (stamp, deadline time.Time) {
	started := time.Now()
	stamp = started
	if s.clock != nil {
		stamp = s.clock()
	}
	return stamp, started.Add(ttl)
}
