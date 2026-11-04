package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/readyindex"
)

// FreshReadyOrderExists requires Head's repinned WAL view and obtains Head,
// coverage, ontology and all existence metadata in one nonlocking statement.
// Committed key_history point reads bypass all staged transaction overlays.
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
	args, values := make([]any, len(keys)), make([]string, len(keys))
	indexes := make(map[string]int, len(keys))
	states := make([]driver.LockState, len(keys))
	for i, key := range keys {
		id := keyID(key)
		args[i], values[i], indexes[id] = id, "(?)", i
		states[i] = driver.LockState{Key: copyKey(key), Revision: model.Natural64(0)}
	}
	rows, err := t.tx.QueryContext(ctx, `WITH wanted(key) AS (VALUES `+strings.Join(values, ",")+`)
	 SELECT m.incarnation,m.head,(`+proof.readyOrderPhysicalCertificateSQL()+`),w.key,
        (SELECT h.data FROM key_history h WHERE h.key=w.key AND h.rev<=m.head ORDER BY h.rev DESC LIMIT 1)
        FROM meta m CROSS JOIN wanted w WHERE m.singleton=1`, args...)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	seen := make(map[string]bool, len(keys))
	for rows.Next() {
		var incarnation, head, id string
		var certified bool
		var data []byte
		if err := rows.Scan(&incarnation, &head, &certified, &id, &data); err != nil {
			return false, err
		}
		if incarnation != t.meta.Incarnation || head != revKey(t.at) {
			return false, driver.ErrConflict
		}
		if !certified {
			return false, fmt.Errorf("%w: fresh Ready physical source certificate unavailable", driver.ErrUnsupported)
		}
		i, found := indexes[id]
		if !found || seen[id] {
			return false, fmt.Errorf("sqlite: invalid fresh Ready point observation")
		}
		seen[id] = true
		if data != nil {
			if err := json.Unmarshal(data, &states[i]); err != nil {
				return false, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if len(seen) != len(keys) {
		return false, fmt.Errorf("sqlite: incomplete fresh Ready point observation")
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
	return sqliteReadyAuthority{t}, nil
}

type sqliteReadyAuthority struct{ tx *transaction }

func (a sqliteReadyAuthority) Snapshot() driver.View { return a.tx.Snapshot() }
func (a sqliteReadyAuthority) Get(ctx context.Context, refs []driver.Ref) ([]driver.Record, error) {
	a.tx.mu.Lock()
	defer a.tx.mu.Unlock()
	if err := a.tx.check(ctx); err != nil {
		return nil, err
	}
	r := &reader{store: a.tx.store, tx: a.tx.tx, meta: a.tx.meta, at: a.tx.at}
	return r.Get(ctx, refs)
}
func (a sqliteReadyAuthority) Observe(ctx context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	a.tx.mu.Lock()
	defer a.tx.mu.Unlock()
	if err := a.tx.check(ctx); err != nil {
		return nil, err
	}
	r := &reader{store: a.tx.store, tx: a.tx.tx, meta: a.tx.meta, at: a.tx.at}
	return r.Observe(ctx, keys)
}

var _ driver.FreshReadyAuthorityReader = (*transaction)(nil)
