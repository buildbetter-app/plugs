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
	renewing      chan struct{}
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
		// A contender must not queue a stronger tuple lock ahead of the current
		// holder's compatible expiry update while publication protects ownership.
		e = tx.QueryRowContext(ctx, `SELECT epoch::text,owner='' OR expires<=clock_timestamp() FROM plug_postgres.lease WHERE singleton FOR UPDATE NOWAIT`).Scan(&epoch, &available)
		if sqlState(e) == "55P03" {
			_ = tx.Rollback()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(5 * time.Millisecond):
				continue
			}
		}
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
		l := &activeLease{store: s, owner: owner, epoch: n + 1, ttl: ttl, renewing: make(chan struct{}, 1), lost: make(chan struct{})}
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

// checkLease protects ownership through SQL COMMIT. The lease_ownership unique
// index makes owner/epoch changes conflict with KEY SHARE, while expiry-only
// renewal remains independent on its reserved connection. A final fresh check
// still rejects local loss, expired SQL time, Release, or a replaced epoch.
func (s *Store) checkLease(ctx context.Context, q querier, lease driver.ActiveLease, lock bool) (*activeLease, error) {
	var expected uint64
	if lease != nil {
		expected = lease.Epoch()
	}
	l, ok := lease.(*activeLease)
	checkExpiry := ok && l.store == s && !l.ended()
	var epoch, owner string
	var live bool
	suffix := ""
	if lock {
		suffix = " FOR KEY SHARE"
	}
	err := q.QueryRowContext(ctx, `SELECT epoch::text,owner,CASE WHEN $1::boolean THEN expires>clock_timestamp() ELSE false END FROM plug_postgres.lease WHERE singleton`+suffix, checkExpiry).Scan(&epoch, &owner, &live)
	if err != nil {
		return nil, err
	}
	observed, err := parseCounter(epoch)
	if err != nil {
		return nil, err
	}
	fenced := driver.ErrFenced{ExpectedEpoch: expected, ObservedEpoch: observed}
	if !ok || l.store != s || l.owner != owner || l.epoch != observed || l.ended() {
		return nil, fenced
	}
	// Observe SQL expiry with its row version in the same statement, only for
	// a locally live holder. Terminal holders never consult the SQL clock. A
	// compatible heartbeat can refresh expiry between round trips; comparing
	// a cached expiry to a later clock would falsely terminate that holder.
	if !live {
		l.mu.Lock()
		l.signal()
		l.mu.Unlock()
		return nil, fenced
	}
	return l, nil
}

func (l *activeLease) Renew(ctx context.Context) error {
	fenced := driver.ErrFenced{ExpectedEpoch: l.epoch, ObservedEpoch: l.epoch}
	// Serialize heartbeats independently of the state mutex. A queued caller
	// can stop waiting, and expiry/publication never wait for a SQL heartbeat.
	select {
	case l.renewing <- struct{}{}:
		defer func() { <-l.renewing }()
	case <-ctx.Done():
		return ctx.Err()
	case <-l.Lost():
		return fenced
	}
	l.mu.Lock()
	if l.ended() || !time.Now().Before(l.timerDeadline) {
		l.signal()
		l.mu.Unlock()
		return fenced
	}
	deadline := l.timerDeadline
	l.mu.Unlock()
	if err := l.store.check(ctx); err != nil {
		return err
	}
	// A blocked UPDATE cannot outlive this lease. Loss also cancels it on
	// Release, while the caller's own cancellation remains independently valid.
	renewCtx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	go func() {
		select {
		case <-l.Lost():
			cancel()
		case <-renewCtx.Done():
		}
	}()
	started := time.Now()
	var epoch string
	l.store.renewMu.Lock()
	renewConn, err := l.store.renewConnLocked(renewCtx)
	if err == nil {
		// This connection is reserved at Store.Open and is never leased to a
		// request transaction. Keep the lock through the query so Store.Close
		// cannot close the connection underneath an in-flight heartbeat.
		err = renewConn.QueryRowContext(renewCtx, `UPDATE plug_postgres.lease SET expires=clock_timestamp()+($3::bigint*interval '1 microsecond') WHERE singleton AND epoch=$1 AND owner=$2 AND expires>clock_timestamp() RETURNING epoch::text`, decimal(l.epoch), l.owner, l.ttl.Microseconds()).Scan(&epoch)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			// A cancelled or failed pgx query may leave the reserved handle
			// unusable. Never retain it for the next heartbeat.
			l.store.discardRenewConnLocked(renewConn)
		}
	}
	l.store.renewMu.Unlock()
	l.mu.Lock()
	defer l.mu.Unlock()
	// A successful SQL response arriving after local expiry cannot resurrect
	// the holder or reset its timer. The old timer callback uses this same lock.
	if l.ended() || !time.Now().Before(deadline) {
		l.signal()
		return fenced
	}
	if errors.Is(err, sql.ErrNoRows) {
		l.signal()
		return driver.ErrFenced{ExpectedEpoch: l.epoch}
	}
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return err
	}
	if !time.Now().Before(started.Add(l.ttl)) {
		l.signal()
		return fenced
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
