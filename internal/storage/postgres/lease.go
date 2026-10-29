package postgres

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type activeLease struct {
	store         *Store
	owner         string
	epoch         uint64
	ttl           time.Duration
	mu            sync.Mutex
	lost          chan struct{}
	timer         *time.Timer
	timerDeadline time.Time
	released      bool
}

func (l *activeLease) Epoch() uint64         { return l.epoch }
func (l *activeLease) Lost() <-chan struct{} { return l.lost }
func (l *activeLease) ended() bool {
	select {
	case <-l.lost:
		return true
	default:
		return false
	}
}
func (l *activeLease) signal() {
	select {
	case <-l.lost:
	default:
		close(l.lost)
	}
}
func (l *activeLease) deadline(at time.Time) {
	if l.timer != nil {
		l.timer.Stop()
	}
	// Each callback checks its own deadline under the same mutex as renewal;
	// an already-running old callback cannot terminate a renewed lease.
	l.timer = time.AfterFunc(time.Until(at), func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.timerDeadline.Equal(at) {
			l.signal()
		}
	})
	l.timerDeadline = at
}

func (s *Store) Lease(ctx context.Context, holder string, ttl time.Duration) (driver.ActiveLease, error) {
	if holder == "" || strings.TrimSpace(holder) != holder || strings.ContainsRune(holder, '\x00') || ttl < time.Microsecond || ttl%time.Microsecond != 0 {
		return nil, fmt.Errorf("postgres: lease requires canonical holder and positive microsecond TTL")
	}
	owner, err := identity()
	if err != nil {
		return nil, err
	}
	for {
		if err = s.check(ctx); err != nil {
			return nil, err
		}
		tx, e := s.db.BeginTx(ctx, nil)
		if e != nil {
			return nil, e
		}
		var epoch string
		var available bool
		e = tx.QueryRowContext(ctx, `SELECT epoch::text,owner='' OR expires<=clock_timestamp() FROM plug_postgres.lease WHERE singleton FOR UPDATE`).Scan(&epoch, &available)
		if e != nil {
			_ = tx.Rollback()
			return nil, e
		}
		n, e := parseCounter(epoch)
		if e != nil {
			_ = tx.Rollback()
			return nil, e
		}
		if !available {
			_ = tx.Rollback()
			timer := time.NewTimer(5 * time.Millisecond)
			select {
			case <-ctx.Done():
				timer.Stop()
				return nil, ctx.Err()
			case <-timer.C:
				continue
			}
		}
		if n == math.MaxUint64 {
			_ = tx.Rollback()
			return nil, fmt.Errorf("%w: postgres lease epoch exhausted", driver.ErrUnsupported)
		}
		started := time.Now()
		_, e = tx.ExecContext(ctx, `UPDATE plug_postgres.lease SET epoch=$1,owner=$2,holder=$3,expires=clock_timestamp()+($4::bigint*interval '1 microsecond') WHERE singleton`, decimal(n+1), owner, holder, ttl.Microseconds())
		if e == nil {
			e = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
		if e != nil {
			return nil, e
		}
		l := &activeLease{store: s, owner: owner, epoch: n + 1, ttl: ttl, lost: make(chan struct{})}
		l.mu.Lock()
		l.deadline(started.Add(ttl))
		l.mu.Unlock()
		s.mu.Lock()
		closed := s.closed
		if !closed {
			s.leases[l] = struct{}{}
		}
		s.mu.Unlock()
		if closed {
			_ = l.Release(context.Background())
			return nil, driver.ErrTxClosed
		}
		return l, nil
	}
}

// checkLease locks the epoch row for publications. The row lock remains held
// through SQL COMMIT, so Release, Renew and takeover cannot interleave with it.
func (s *Store) checkLease(ctx context.Context, q querier, lease driver.ActiveLease, lock bool) (*activeLease, error) {
	var epoch, owner string
	var expires time.Time
	suffix := ""
	if lock {
		suffix = " FOR SHARE"
	}
	err := q.QueryRowContext(ctx, `SELECT epoch::text,owner,expires FROM plug_postgres.lease WHERE singleton`+suffix).Scan(&epoch, &owner, &expires)
	if err != nil {
		return nil, err
	}
	observed, err := parseCounter(epoch)
	if err != nil {
		return nil, err
	}
	var expected uint64
	if lease != nil {
		expected = lease.Epoch()
	}
	l, ok := lease.(*activeLease)
	fenced := driver.ErrFenced{ExpectedEpoch: expected, ObservedEpoch: observed}
	if !ok || l.store != s || l.owner != owner || l.epoch != observed || l.ended() {
		return nil, fenced
	}
	var now time.Time
	if err = q.QueryRowContext(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return nil, err
	}
	if !now.Before(expires) {
		l.mu.Lock()
		l.signal()
		l.mu.Unlock()
		return nil, fenced
	}
	return l, nil
}

func (l *activeLease) Renew(ctx context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.ended() || !time.Now().Before(l.timerDeadline) {
		l.signal()
		return driver.ErrFenced{ExpectedEpoch: l.epoch, ObservedEpoch: l.epoch}
	}
	if err := l.store.check(ctx); err != nil {
		return err
	}
	started := time.Now()
	var epoch string
	err := l.store.db.QueryRowContext(ctx, `UPDATE plug_postgres.lease SET expires=clock_timestamp()+($3::bigint*interval '1 microsecond') WHERE singleton AND epoch=$1 AND owner=$2 AND expires>clock_timestamp() RETURNING epoch::text`, decimal(l.epoch), l.owner, l.ttl.Microseconds()).Scan(&epoch)
	if errors.Is(err, sql.ErrNoRows) {
		l.signal()
		return driver.ErrFenced{ExpectedEpoch: l.epoch}
	}
	if err != nil {
		return err
	}
	if !time.Now().Before(started.Add(l.ttl)) {
		l.signal()
		return driver.ErrFenced{ExpectedEpoch: l.epoch, ObservedEpoch: l.epoch}
	}
	l.deadline(started.Add(l.ttl))
	return nil
}
func (l *activeLease) Release(ctx context.Context) error {
	l.mu.Lock()
	if l.released {
		l.mu.Unlock()
		return nil
	}
	l.released = true
	l.signal()
	if l.timer != nil {
		l.timer.Stop()
	}
	l.mu.Unlock()
	_, err := l.store.db.ExecContext(ctx, `UPDATE plug_postgres.lease SET owner='',holder='',expires='epoch' WHERE singleton AND epoch=$1 AND owner=$2`, decimal(l.epoch), l.owner)
	l.store.mu.Lock()
	delete(l.store.leases, l)
	l.store.mu.Unlock()
	return err
}

var _ driver.ActiveLease = (*activeLease)(nil)
