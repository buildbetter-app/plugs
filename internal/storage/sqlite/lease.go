package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type leaseRow struct {
	epoch         uint64
	token, holder string
	expires       int64
}

func readLease(ctx context.Context, q queryer) (leaseRow, error) {
	var r leaseRow
	var epoch string
	err := q.QueryRowContext(ctx, `SELECT epoch,token,holder,expires FROM active_lease WHERE singleton=1`).Scan(&epoch, &r.token, &r.holder, &r.expires)
	if err == nil {
		r.epoch, err = strconv.ParseUint(epoch, 10, 64)
	}
	return r, err
}

type activeLease struct {
	store    *Store
	epoch    uint64
	token    string
	ttl      time.Duration
	renewMu  sync.Mutex
	mu       sync.Mutex
	deadline time.Time
	timer    *time.Timer
	lost     chan struct{}
	lostFlag bool
	released bool
}

func (l *activeLease) Epoch() uint64         { return l.epoch }
func (l *activeLease) Lost() <-chan struct{} { return l.lost }
func (l *activeLease) signalLost() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.loseLocked()
}
func (l *activeLease) loseLocked() {
	if !l.lostFlag {
		l.lostFlag = true
		close(l.lost)
	}
	if l.timer != nil {
		l.timer.Stop()
	}
}
func (l *activeLease) expire() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lostFlag {
		return
	}
	remaining := time.Until(l.deadline)
	if remaining <= 0 {
		l.loseLocked()
		return
	}
	l.timer = time.AfterFunc(remaining, l.expire)
}
func (s *Store) fence(ctx context.Context, q queryer, l *activeLease) error {
	r, err := readLease(ctx, q)
	if err != nil {
		return err
	}
	if l == nil {
		return driver.ErrFenced{ObservedEpoch: r.epoch}
	}
	select {
	case <-l.lost:
		return driver.ErrFenced{ExpectedEpoch: l.epoch, ObservedEpoch: r.epoch}
	default:
	}
	if l.store != s || r.epoch != l.epoch || r.token != l.token || time.Now().UnixNano() >= r.expires {
		l.signalLost()
		return driver.ErrFenced{ExpectedEpoch: l.epoch, ObservedEpoch: r.epoch}
	}
	return nil
}
func (s *Store) leaseFor(ctx context.Context, value driver.ActiveLease) (*activeLease, error) {
	if err := s.check(ctx); err != nil {
		return nil, err
	}
	l, ok := value.(*activeLease)
	if !ok || l == nil || l.store != s {
		r, err := readLease(ctx, s.db)
		if err != nil {
			return nil, err
		}
		var expected uint64
		if value != nil {
			expected = value.Epoch()
		}
		return nil, driver.ErrFenced{ExpectedEpoch: expected, ObservedEpoch: r.epoch}
	}
	if err := s.fence(ctx, s.db, l); err != nil {
		return nil, err
	}
	return l, nil
}
func (s *Store) Lease(ctx context.Context, holder string, ttl time.Duration) (driver.ActiveLease, error) {
	if holder == "" || strings.TrimSpace(holder) != holder || strings.ContainsRune(holder, '\x00') || ttl <= 0 || ttl > driver.LeaseTTL {
		return nil, fmt.Errorf("sqlite: canonical holder and TTL in (0,10s] required")
	}
	for {
		if err := s.check(ctx); err != nil {
			return nil, err
		}
		var acquired *activeLease
		err := s.immediate(ctx, nil, func(c *sql.Conn) error {
			r, err := readLease(ctx, c)
			if err != nil {
				return err
			}
			now := time.Now()
			if r.token != "" && now.UnixNano() < r.expires {
				return nil
			}
			if r.epoch == math.MaxUint64 {
				return fmt.Errorf("sqlite: lease epoch exhausted")
			}
			l := &activeLease{store: s, epoch: r.epoch + 1, token: randomID(), ttl: ttl, deadline: now.Add(ttl), lost: make(chan struct{})}
			_, err = c.ExecContext(ctx, `UPDATE active_lease SET epoch=?,token=?,holder=?,expires=? WHERE singleton=1`, revKey(l.epoch), l.token, holder, l.deadline.UnixNano())
			if err == nil {
				acquired = l
			}
			return err
		})
		if err != nil {
			return nil, err
		}
		if acquired != nil {
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				_ = acquired.Release(context.Background())
				return nil, fmt.Errorf("sqlite: store closed")
			}
			s.leases[acquired] = struct{}{}
			s.mu.Unlock()
			acquired.mu.Lock()
			acquired.timer = time.AfterFunc(time.Until(acquired.deadline), acquired.expire)
			acquired.mu.Unlock()
			return acquired, nil
		}
		timer := time.NewTimer(2 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}
func (l *activeLease) Renew(ctx context.Context) error {
	l.renewMu.Lock()
	defer l.renewMu.Unlock()
	if err := l.store.check(ctx); err != nil {
		return err
	}
	// Do not hold l.mu while fencing: the fence may signal Lost.
	updated := false
	err := l.store.immediate(ctx, l, func(c *sql.Conn) error {
		deadline := time.Now().Add(l.ttl)
		_, err := c.ExecContext(ctx, `UPDATE active_lease SET expires=? WHERE singleton=1`, deadline.UnixNano())
		if err != nil {
			return err
		}
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.lostFlag {
			return driver.ErrFenced{ExpectedEpoch: l.epoch, ObservedEpoch: l.epoch}
		}
		// Publish the new timer while still inside the SQL publication lock.
		// Failed SQL publication closes Lost instead of trusting this deadline.
		l.deadline = deadline
		if l.timer != nil {
			l.timer.Stop()
		}
		l.timer = time.AfterFunc(time.Until(deadline), l.expire)
		updated = true
		return nil
	})
	if err != nil {
		if updated {
			l.signalLost()
		}
		return err
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	// An expiry callback that won the race is irrevocable. The server has
	// already stopped admission and must acquire another lease.
	if l.lostFlag {
		return driver.ErrFenced{ExpectedEpoch: l.epoch, ObservedEpoch: l.epoch}
	}
	return nil
}
func (l *activeLease) Release(ctx context.Context) error {
	l.signalLost()
	l.mu.Lock()
	released := l.released
	l.mu.Unlock()
	if released {
		return nil
	}
	err := l.store.immediate(ctx, nil, func(c *sql.Conn) error {
		_, err := c.ExecContext(ctx, `UPDATE active_lease SET token='',holder='',expires=0 WHERE singleton=1 AND epoch=? AND token=?`, revKey(l.epoch), l.token)
		return err
	})
	if err != nil {
		return err
	}
	l.mu.Lock()
	l.released = true
	l.mu.Unlock()
	l.store.mu.Lock()
	delete(l.store.leases, l)
	l.store.mu.Unlock()
	return nil
}

var _ driver.ActiveLease = (*activeLease)(nil)
