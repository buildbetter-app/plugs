package memory

import (
	"context"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type activeLease struct {
	store   *Store
	epoch   uint64
	ttl     time.Duration
	expires time.Time
	lost    chan struct{}
	ended   bool
	timer   *time.Timer
}

func canonicalKey(key string) bool {
	return key != "" && strings.TrimSpace(key) == key && !strings.ContainsRune(key, '\x00')
}

func (s *Store) Lease(ctx context.Context, holder string, ttl time.Duration) (driver.ActiveLease, error) {
	if !canonicalKey(holder) || ttl <= 0 {
		return nil, fmt.Errorf("memory: invalid lease holder or TTL")
	}
	for {
		s.mu.Lock()
		if err := s.check(ctx); err != nil {
			s.mu.Unlock()
			return nil, err
		}
		// expireLease takes leaseMu after Store.mu, preserving the single lock
		// order used by all data publication paths.
		s.expireLease()
		s.leaseMu.Lock()
		if s.active == nil {
			if s.epoch == math.MaxUint64 {
				s.leaseMu.Unlock()
				s.mu.Unlock()
				return nil, fmt.Errorf("memory: lease epoch exhausted")
			}
			s.epoch++
			l := &activeLease{store: s, epoch: s.epoch, ttl: ttl, expires: s.now().Add(ttl), lost: make(chan struct{})}
			s.active = l
			l.timer = time.AfterFunc(ttl, func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				s.expireLease()
			})
			s.leaseMu.Unlock()
			s.mu.Unlock()
			return l, nil
		}
		changed := s.notify
		s.leaseMu.Unlock()
		s.mu.Unlock()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		}
	}
}

func (l *activeLease) Epoch() uint64         { return l.epoch }
func (l *activeLease) Lost() <-chan struct{} { return l.lost }

// loseLocked is called only while Store.leaseMu is held.
func (l *activeLease) loseLocked() {
	if !l.ended {
		l.ended = true
		if l.timer != nil {
			l.timer.Stop()
		}
		close(l.lost)
	}
}

// Called with Store.mu held, including from commit. A timer is only
// notification; the deadline itself is checked at every fenced publication.
func (s *Store) expireLease() {
	s.leaseMu.Lock()
	ended := s.expireLeaseLocked()
	s.leaseMu.Unlock()
	if ended != nil {
		s.finishLeaseLocked(ended)
	}
}

func (s *Store) expireLeaseLocked() *activeLease {
	if s.active == nil || s.now().Before(s.active.expires) {
		return nil
	}
	ended := s.active
	ended.loseLocked()
	s.active = nil
	return ended
}

// endLease is called with Store.mu held. It marks the active lease while
// holding leaseMu, then releases only transactions owned by that lease.
func (s *Store) endLease() {
	s.leaseMu.Lock()
	ended := s.active
	if ended != nil {
		ended.loseLocked()
		s.active = nil
	}
	s.leaseMu.Unlock()
	if ended != nil {
		s.finishLeaseLocked(ended)
	}
}

// finishLeaseLocked is called with Store.mu held. Transactions are tied to
// their lease so a renewal-side expiry cannot strand the old head or tear down
// a successor transaction acquired concurrently.
func (s *Store) finishLeaseLocked(ended *activeLease) {
	for tx := range s.transactions {
		if tx.lease == ended {
			tx.finish()
		}
	}
	s.signal()
}

// Called with Store.mu held. Data paths take this lock before leaseMu.
func (s *Store) checkLease(value driver.ActiveLease) error {
	s.expireLease()
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	return s.checkLeaseLocked(value)
}

func (s *Store) checkLeaseLocked(value driver.ActiveLease) error {
	l, ok := value.(*activeLease)
	if !ok || l == nil || l.store != s || l != s.active || l.ended {
		var expected uint64
		if ok && l != nil {
			expected = l.epoch
		}
		return driver.ErrFenced{ExpectedEpoch: expected, ObservedEpoch: s.epoch}
	}
	return nil
}

// checkLeaseAtPublicationLocked is the final linearization fence. Store.mu is
// already held by the caller; holding leaseMu through the state swap prevents
// a takeover or expiry from interleaving with the check.
func (s *Store) checkLeaseAtPublicationLocked(value driver.ActiveLease) error {
	l, ok := value.(*activeLease)
	if !ok || l == nil || l.store != s || l != s.active || l.ended {
		var expected uint64
		if ok && l != nil {
			expected = l.epoch
		}
		return driver.ErrFenced{ExpectedEpoch: expected, ObservedEpoch: s.epoch}
	}
	if s.now().Before(l.expires) {
		return nil
	}
	ended := s.active
	ended.loseLocked()
	s.active = nil
	s.finishLeaseLocked(ended)
	return driver.ErrFenced{ExpectedEpoch: l.epoch, ObservedEpoch: s.epoch}
}

// withLeasePublication keeps the epoch/expiry check adjacent to the atomic
// in-memory publication. Store.mu must be held by the caller.
func (s *Store) withLeasePublication(value driver.ActiveLease, publish func()) error {
	s.leaseMu.Lock()
	defer s.leaseMu.Unlock()
	if err := s.checkLeaseAtPublicationLocked(value); err != nil {
		return err
	}
	publish()
	return nil
}

// Renew intentionally takes only leaseMu. It never waits for a data commit.
// If a fake or wall clock has crossed the deadline, cleanup is performed after
// releasing leaseMu and is scoped to this expired lease.
func (l *activeLease) Renew(ctx context.Context) error {
	s := l.store
	if err := ctx.Err(); err != nil {
		return err
	}
	s.leaseMu.Lock()
	if err := ctx.Err(); err != nil {
		s.leaseMu.Unlock()
		return err
	}
	if l.store != s || l != s.active || l.ended {
		err := driver.ErrFenced{ExpectedEpoch: l.epoch, ObservedEpoch: s.epoch}
		s.leaseMu.Unlock()
		return err
	}
	now := s.now()
	if !now.Before(l.expires) {
		l.loseLocked()
		s.active = nil
		observed := s.epoch
		s.leaseMu.Unlock()
		s.mu.Lock()
		s.finishLeaseLocked(l)
		s.mu.Unlock()
		return driver.ErrFenced{ExpectedEpoch: l.epoch, ObservedEpoch: observed}
	}
	l.expires = now.Add(l.ttl)
	l.timer.Reset(l.ttl)
	s.leaseMu.Unlock()
	return nil
}

func (l *activeLease) Release(ctx context.Context) error {
	s := l.store
	s.mu.Lock()
	defer s.mu.Unlock()
	s.leaseMu.Lock()
	ended := l.ended
	s.leaseMu.Unlock()
	if ended {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.checkLease(l); err != nil {
		return err
	}
	s.endLease()
	return nil
}
