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
	store *Store
	epoch uint64
	token string
	ttl   time.Duration
	mu    sync.Mutex
	// expires is either durable, or protected by this exact lease's registered
	// publication. In the latter case the owner holds BOTH BEGIN IMMEDIATE and
	// the cross-process publication gate when registration begins. The gate
	// stays held until every accepted heartbeat is durable or Lost is closed.
	expires    int64
	renewal    uint64
	publishing bool
	// handoff stops accepting private heartbeats after the graph COMMIT. New
	// Renew callers wait for the publication gate instead of extending the
	// heartbeat while its final durable copy is being committed.
	handoff  bool
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

// liveLocked checks time explicitly, not only the timer callback: a process
// paused past its monotonic deadline must not renew or commit when it resumes.
func (l *activeLease) liveLocked(now, expires int64) bool {
	if l.lostFlag || l.released || now >= expires || !time.Now().Before(l.deadline) {
		l.loseLocked()
		return false
	}
	return true
}

func (l *activeLease) fenceLocked(s *Store, r leaseRow, now, expires int64) error {
	if l.store != s || r.epoch != l.epoch || r.token != l.token || !l.liveLocked(now, expires) {
		l.loseLocked()
		return driver.ErrFenced{ExpectedEpoch: l.epoch, ObservedEpoch: r.epoch}
	}
	return nil
}

func (s *Store) fence(ctx context.Context, q queryer, l *activeLease) error {
	r, err := readLease(ctx, q)
	if err != nil {
		return err
	}
	if l == nil {
		return driver.ErrFenced{ObservedEpoch: r.epoch}
	}
	// Clock is an external callback and may itself signal lease loss. Sample it
	// before taking lease/store mutexes, then recheck Lost and real time inside.
	now := s.now().UnixNano()
	l.mu.Lock()
	defer l.mu.Unlock()
	// A WAL reader may have sampled the old durable row just before a heartbeat
	// was persisted and unregistered. The synchronized effective expiry remains
	// safe even then: it cannot escape the publication gate while live unless
	// it was persisted, and it never overrides a changed epoch/token.
	return l.fenceLocked(s, r, now, max(r.expires, l.expires))
}

// renewal identifies an accepted heartbeat even when an injected wall clock
// returns the same timestamp twice. It is local bookkeeping, not a lease epoch.
// A single publication cannot span 2^64 successful Renew calls.
type leaseHeartbeat struct {
	expires int64
	renewal uint64
}

func (l *activeLease) heartbeatLocked() leaseHeartbeat {
	return leaseHeartbeat{expires: l.expires, renewal: l.renewal}
}

func (s *Store) beginLeasePublication(ctx context.Context, c *sql.Conn, l *activeLease) (leaseHeartbeat, error) {
	r, err := readLease(ctx, c)
	if err != nil {
		return leaseHeartbeat{}, err
	}
	now := s.now().UnixNano()
	l.mu.Lock()
	defer l.mu.Unlock()
	// The gate and native writer are already held. Require the fresh DURABLE
	// expiry at entry; only this registered interval can bridge its renewals.
	if err := l.fenceLocked(s, r, now, r.expires); err != nil {
		return leaseHeartbeat{}, err
	}
	l.publishing = true
	return leaseHeartbeat{expires: r.expires, renewal: l.renewal}, nil
}

// renewPublication acknowledges a heartbeat without SQL only while takeover is
// excluded by this lease's publication gate. The store lock orders it against
// Close; l.mu orders it against expiry, release, registration and handoff.
func (l *activeLease) renewPublication(ctx context.Context) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	now, deadline := l.store.leaseTimes(l.ttl)
	l.store.mu.Lock()
	defer l.store.mu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if l.store.closed {
		l.loseLocked()
		return false, fmt.Errorf("sqlite: store closed")
	}
	// The callback and mutex acquisition may have taken time: check the old
	// monotonic deadline NOW, even when its expiry callback has not run yet.
	if !l.liveLocked(now.UnixNano(), l.expires) {
		return false, driver.ErrFenced{ExpectedEpoch: l.epoch, ObservedEpoch: l.epoch}
	}
	if !l.publishing || l.handoff {
		// At the handoff cutoff the last accepted heartbeat is being flushed.
		// Queue this call on the gate without acknowledging a new extension.
		// It must still pass both expiry checks when it can renew normally.
		return false, nil
	}
	if !deadline.After(l.deadline) {
		// A concurrent Renew sampled later but acquired the mutex first. Its
		// extension already covers this call; never move either deadline back.
		return true, nil
	}
	l.expires = now.Add(l.ttl).UnixNano()
	l.renewal++
	l.deadline = deadline
	if l.timer != nil {
		l.timer.Stop()
	}
	l.timer = time.AfterFunc(time.Until(deadline), l.expire)
	return true, nil
}

// finishPublication closes the bridge if the last accepted heartbeat is
// durable (or Lost is already closed). Otherwise it atomically cuts off private
// heartbeat admission: later Renew calls wait on the gate while ONE lease-only
// transaction flushes the fixed heartbeat. No mutex is held across SQL/fsync.
// Even if the lease died during fsync, a committed graph is still successful.
func (l *activeLease) finishPublication(durable leaseHeartbeat) bool {
	now := l.store.now().UnixNano()
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.liveLocked(now, l.expires) || l.heartbeatLocked() == durable {
		l.publishing = false
		l.handoff = false
		return true
	}
	l.handoff = true
	return false
}

func (l *activeLease) abortPublication(durable leaseHeartbeat) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.publishing || l.handoff {
		// Rollback/cancellation/uncertain COMMIT cannot leave an acknowledged
		// private heartbeat live after relinquishing cross-process exclusion.
		if l.heartbeatLocked() != durable {
			l.loseLocked()
		}
		l.publishing = false
		l.handoff = false
	}
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
			now, deadline := s.leaseTimes(ttl)
			if r.token != "" && now.UnixNano() < r.expires {
				return nil
			}
			if r.epoch == math.MaxUint64 {
				return fmt.Errorf("sqlite: lease epoch exhausted")
			}
			l := &activeLease{store: s, epoch: r.epoch + 1, token: randomID(), ttl: ttl, expires: now.Add(ttl).UnixNano(), deadline: deadline, lost: make(chan struct{})}
			_, err = c.ExecContext(ctx, `UPDATE active_lease SET epoch=?,token=?,holder=?,expires=? WHERE singleton=1`, revKey(l.epoch), l.token, holder, l.expires)
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
	// Serialize deadline updates with l.mu, not a mutex spanning SQL: another
	// timely Renew must also bridge a slow lease-only native COMMIT.
	if err := l.store.check(ctx); err != nil {
		return err
	}
	gate, err := l.store.fileLockUntil(ctx, "publication", false, l.Lost(), func() (bool, error) {
		return l.renewPublication(ctx)
	})
	if err != nil || gate == nil {
		return err
	}
	defer unlockFile(gate)
	// No publication belonged to this lease while we waited. Use the normal
	// immediate boundary, which registers a bridge before advancing the timer.
	if err := l.store.immediateLocked(ctx, l, func(*sql.Conn) error {
		_, err := l.renewPublication(ctx)
		return err
	}); err != nil {
		return err
	}
	now := l.store.now().UnixNano()
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.liveLocked(now, l.expires) {
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
