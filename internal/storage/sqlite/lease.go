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
	// expires is durable in SQLite or the identity-bound heartbeat journal.
	// Registration owns BOTH BEGIN IMMEDIATE and the publication flock. Renew
	// persists independently of SQLite until that single transaction returns.
	expires     int64
	renewal     uint64
	publishing  bool
	deadline    time.Time
	timer       *time.Timer
	commitWatch *commitWatch
	lost        chan struct{}
	lossReason  string
	released    bool
}

func (l *activeLease) Epoch() uint64         { return l.epoch }
func (l *activeLease) Lost() <-chan struct{} { return l.lost }

// WaitForCommit lets recovery wait for an admitted COMMIT to return before
// retiring its runtime. A watchdog that has decided to exit keeps it blocked.
// Callers must observe Lost first, after which no new COMMIT can be admitted.
func (l *activeLease) WaitForCommit(ctx context.Context) error {
	l.mu.Lock()
	watch := l.commitWatch
	l.mu.Unlock()
	if watch != nil {
		select {
		case <-watch.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (l *activeLease) LossReason() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.lossReason
}
func (l *activeLease) signalLost(reason string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.loseLocked(reason)
}
func (l *activeLease) loseLocked(reason string) {
	if l.lossReason == "" {
		l.lossReason = reason
		close(l.lost)
	}
	if l.timer != nil {
		l.timer.Stop()
	}
}
func (l *activeLease) expire() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lossReason != "" {
		return
	}
	remaining := time.Until(l.deadline)
	if remaining <= 0 {
		l.loseLocked("lease renewal deadline elapsed")
		return
	}
	l.timer = time.AfterFunc(remaining, l.expire)
}

// liveLocked checks time explicitly, not only the timer callback: a process
// paused past its monotonic deadline must not renew or commit when it resumes.
func (l *activeLease) liveLocked(now, expires int64) bool {
	if l.lossReason != "" {
		return false
	}
	if l.released || now >= expires || !time.Now().Before(l.deadline) {
		l.loseLocked("lease renewal deadline elapsed")
		return false
	}
	return true
}

func (l *activeLease) fenceLocked(s *Store, r leaseRow, now, expires int64) error {
	if l.store != s || r.epoch != l.epoch || r.token != l.token {
		l.loseLocked("durable lease owner changed")
		return driver.ErrFenced{ExpectedEpoch: l.epoch, ObservedEpoch: r.epoch}
	}
	if !l.liveLocked(now, expires) {
		return driver.ErrFenced{ExpectedEpoch: l.epoch, ObservedEpoch: r.epoch}
	}
	return nil
}

func (s *Store) fence(ctx context.Context, q queryer, l *activeLease) error {
	r, err := s.readLease(ctx, q)
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
	// A WAL/journal reader may have sampled just before an accepted heartbeat.
	// The synchronized expiry is safe too: acceptance already persisted it to
	// the identity-bound journal, and it never overrides a changed epoch/token.
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
	r, err := s.readLease(ctx, c)
	if err != nil {
		return leaseHeartbeat{}, err
	}
	now := s.now().UnixNano()
	l.mu.Lock()
	defer l.mu.Unlock()
	// The gate and native writer are already held. Require the fresh durable
	// SQLite/journal expiry at entry; neither a stale row nor local time alone
	// can admit a successor's or genuinely expired holder's publication.
	if err := l.fenceLocked(s, r, now, r.expires); err != nil {
		return leaseHeartbeat{}, err
	}
	l.publishing = true
	return leaseHeartbeat{expires: r.expires, renewal: l.renewal}, nil
}

// renewPublication persists a heartbeat without SQL only while takeover is
// excluded by this lease's publication gate. The store lock orders it against
// Close; l.mu orders journal replacement against expiry, release and registration.
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
		l.loseLocked("store closed during renewal")
		return false, fmt.Errorf("sqlite: store closed")
	}
	// The callback and mutex acquisition may have taken time: check the old
	// monotonic deadline NOW, even when its expiry callback has not run yet.
	if !l.liveLocked(now.UnixNano(), l.expires) {
		return false, driver.ErrFenced{ExpectedEpoch: l.epoch, ObservedEpoch: l.epoch}
	}
	if !l.publishing {
		return false, nil
	}
	if !deadline.After(l.deadline) {
		// A concurrent Renew sampled later but acquired the mutex first. Its
		// extension already covers this call; never move either deadline back.
		return true, nil
	}
	expires := now.Add(l.ttl).UnixNano()
	if err := l.persistHeartbeat(expires); err != nil {
		l.loseLocked("heartbeat journal update failed")
		return false, err
	}
	// Journal I/O is not a grace period. In particular a process stopped inside
	// fsync must not acknowledge or publish after its OLD real deadline. Clock
	// callbacks remain outside both mutexes; this elapsed check is monotonic.
	if !time.Now().Before(l.deadline) {
		l.loseLocked("heartbeat journal update exceeded lease deadline")
		return false, driver.ErrFenced{ExpectedEpoch: l.epoch, ObservedEpoch: l.epoch}
	}
	if err := ctx.Err(); err != nil {
		l.loseLocked("heartbeat journal update exceeded renewal attempt budget")
		return false, err
	}
	l.expires = expires
	l.renewal++
	l.deadline = deadline
	if l.commitWatch != nil {
		l.commitWatch.leaseDeadline.Store(&deadline)
	}
	if l.timer != nil {
		l.timer.Stop()
	}
	l.timer = time.AfterFunc(time.Until(deadline), l.expire)
	return true, nil
}

// finishPublication unregisters admission after the ONE native COMMIT. Every
// accepted heartbeat is already durable independently, including one racing
// COMMIT's return. There is no cutoff, follow-up transaction or moving target.
// Expiry after a confirmed COMMIT closes Lost, never changes its success result.
func (l *activeLease) finishPublication() {
	now := l.store.now().UnixNano()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.liveLocked(now, l.expires)
	l.publishing = false
	l.commitWatch = nil
}

func (l *activeLease) abortPublication(durable leaseHeartbeat, rolledBack bool, commitAttempted bool) (renewed bool) {
	now := l.store.now().UnixNano()
	l.mu.Lock()
	defer l.mu.Unlock()
	// A successful native rollback leaves the identity-bound journal as the
	// durable heartbeat, independent of the SQL changes it just reverted.
	// Never reopen Lost or extend the deadline here.
	if rolledBack {
		l.liveLocked(now, l.expires)
	} else if commitAttempted {
		l.loseLocked("native publication outcome was uncertain")
	} else {
		l.loseLocked("native publication rollback was unconfirmed")
	}
	l.publishing = false
	l.commitWatch = nil
	return l.renewal != durable.renewal
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
			r, err := s.readLease(ctx, c)
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
	// No publication belonged to this lease while we waited. Use the normal
	// immediate boundary, which registers a bridge before advancing the timer.
	if err := l.store.immediateLocked(driver.WithPublicationMethod(ctx, "lease.renew"), gate, l, false, func(*sql.Conn) error {
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
	l.signalLost("lease released")
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
