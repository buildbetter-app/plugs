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
		s.expireLease()
		if s.active == nil {
			if s.epoch == math.MaxUint64 {
				s.mu.Unlock()
				return nil, fmt.Errorf("memory: lease epoch exhausted")
			}
			s.epoch++
			l := &activeLease{store: s, epoch: s.epoch, ttl: ttl, expires: s.now().Add(ttl), lost: make(chan struct{})}
			s.active = l
			l.timer = time.AfterFunc(ttl, func() { s.mu.Lock(); defer s.mu.Unlock(); s.expireLease() })
			s.mu.Unlock()
			return l, nil
		}
		changed := s.notify
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
func (l *activeLease) lose() {
	if !l.ended {
		l.ended = true
		if l.timer != nil {
			l.timer.Stop()
		}
		close(l.lost)
	}
}

// Called under Store.mu, including from commit. A timer is only notification;
// the deadline itself is checked at every fenced publication.
func (s *Store) expireLease() {
	if s.active != nil && !s.now().Before(s.active.expires) {
		s.endLease()
	}
}

func (s *Store) endLease() {
	s.active.lose()
	s.active = nil
	for tx := range s.transactions {
		tx.finish()
	}
	s.signal()
}

func (s *Store) checkLease(value driver.ActiveLease) error {
	s.expireLease()
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

func (l *activeLease) Renew(ctx context.Context) error {
	s := l.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.check(ctx); err != nil {
		return err
	}
	if err := s.checkLease(l); err != nil {
		return err
	}
	l.expires = s.now().Add(l.ttl)
	l.timer.Reset(l.ttl)
	return nil
}

func (l *activeLease) Release(ctx context.Context) error {
	s := l.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if l.ended && s.active == nil && s.epoch == l.epoch {
		return nil
	}
	if err := s.checkLease(l); err != nil {
		return err
	}
	s.endLease()
	return nil
}
