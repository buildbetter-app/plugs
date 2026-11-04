package postgres

import (
	"context"
	"fmt"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/readyindex"
)

// FreshReadyOrderExists uses one READ COMMITTED statement after Head. Current
// metadata and all Ready completeness/eligibility rows share that statement's
// snapshot. Indexed lateral point reads take no additional locks and exclude
// every staged overlay and every earlier pinned reader observation.
func (t *transaction) FreshReadyOrderExists(ctx context.Context, p driver.ReadyOrderPlan) (bool, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return false, err
	}
	if !t.hasHead {
		return false, fmt.Errorf("%w: fresh Ready requires Head", driver.ErrUnsupported)
	}
	proof, ok := any(t.reader).(interface{ readyOrderPhysicalCertificateSQL() string })
	if !ok || proof.readyOrderPhysicalCertificateSQL() == "" {
		return false, fmt.Errorf("%w: fresh Ready physical source certificate unavailable", driver.ErrUnsupported)
	}
	keys, err := readyindex.ExistenceKeys(p)
	if err != nil {
		return false, err
	}
	ids, states := make([][]byte, len(keys)), make([]driver.LockState, len(keys))
	indexes := make(map[string]int, len(keys))
	for i, key := range keys {
		id, err := keyID(key)
		if err != nil {
			return false, err
		}
		ids[i], indexes[id] = []byte(id), i
		states[i] = driver.LockState{Key: copyLockKey(key), Revision: model.Natural64(0)}
	}
	rows, err := t.sql.QueryContext(ctx, `SELECT m.incarnation,m.head::text,(`+proof.readyOrderPhysicalCertificateSQL()+`),w.key,h.value
        FROM plug_postgres.meta m CROSS JOIN unnest($1::bytea[]) w(key)
        LEFT JOIN LATERAL (SELECT s.value FROM plug_postgres.states s
        WHERE s.key=w.key AND s.revision<=m.head ORDER BY s.revision DESC LIMIT 1) h ON true
        WHERE m.singleton`, ids)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	seen := make(map[string]bool, len(keys))
	for rows.Next() {
		var incarnation, head string
		var certified bool
		var id, data []byte
		if err := rows.Scan(&incarnation, &head, &certified, &id, &data); err != nil {
			return false, err
		}
		if incarnation != t.meta.Incarnation || head != decimal(t.at) {
			return false, driver.ErrConflict
		}
		if !certified {
			return false, fmt.Errorf("%w: fresh Ready physical source certificate unavailable", driver.ErrUnsupported)
		}
		i, found := indexes[string(id)]
		if !found || seen[string(id)] {
			return false, fmt.Errorf("postgres: invalid fresh Ready point observation")
		}
		seen[string(id)] = true
		if data != nil {
			if err := decode(data, &states[i]); err != nil {
				return false, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if len(seen) != len(keys) {
		return false, fmt.Errorf("postgres: incomplete fresh Ready point observation")
	}
	at := model.Natural64(t.at)
	return readyindex.ExistsCommitted(ctx, driver.View{Incarnation: t.meta.Incarnation, Branch: model.Natural64(0), At: &at}, p, states)
}

var _ driver.FreshReadyOrderExistsReader = (*transaction)(nil)

func (t *transaction) FreshReadyAuthority(ctx context.Context) (driver.ReadyAuthoritySnapshot, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return nil, err
	}
	if !t.hasHead {
		return nil, fmt.Errorf("%w: committed Ready authority requires Head", driver.ErrUnsupported)
	}
	return postgresReadyAuthority{t}, nil
}

type postgresReadyAuthority struct{ tx *transaction }

func (a postgresReadyAuthority) Snapshot() driver.View { return a.tx.Snapshot() }
func (a postgresReadyAuthority) Get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
	a.tx.mu.Lock()
	defer a.tx.mu.Unlock()
	if err := a.tx.check(ctx); err != nil {
		return nil, err
	}
	r := &reader{store: a.tx.store, sql: a.tx.sql, meta: a.tx.meta, at: a.tx.at}
	return r.Get(ctx, refs)
}
func (a postgresReadyAuthority) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	a.tx.mu.Lock()
	defer a.tx.mu.Unlock()
	if err := a.tx.check(ctx); err != nil {
		return nil, err
	}
	r := &reader{store: a.tx.store, sql: a.tx.sql, meta: a.tx.meta, at: a.tx.at}
	return r.Observe(ctx, keys)
}

var _ driver.FreshReadyAuthorityReader = (*transaction)(nil)
