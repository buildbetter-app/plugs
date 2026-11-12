package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func (t *transaction) PrepareOfflineHistory(ctx context.Context, p driver.RestoreImportProfile, h driver.OfflineHistory, when time.Time) (driver.Meta, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.check(ctx); err != nil {
		return driver.Meta{}, err
	}
	binding, err := driver.PrepareOfflineHistoryBinding(t.meta, p, h, when)
	if err != nil {
		return driver.Meta{}, err
	}
	if _, err = t.prepareRestore(ctx, p.SourceStoreID); err != nil {
		return driver.Meta{}, err
	}
	m := binding.Meta()
	t.restore = &m
	t.offlineHistory = binding
	return m, nil
}
func (t *transaction) CommitOfflineHistory(ctx context.Context, h driver.ChangesetHeader) (model.Changeset, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	zero := model.Changeset{}
	if err := t.check(ctx); err != nil {
		return zero, err
	}
	if !t.hasHead || t.offlineHistory == nil {
		return zero, fmt.Errorf("history commit requires prepared offline capability and Head")
	}
	if err := t.validateChanges(ctx, h.Changes); err != nil {
		return zero, err
	}
	if err := t.validatePredicates(ctx); err != nil {
		return zero, err
	}
	tail := []driver.Record{}
	for _, r := range t.records {
		if r == nil {
			return zero, fmt.Errorf("history final image cannot stage deletion")
		}
		tail = append(tail, *r)
	}
	bumps := []driver.LockKey{}
	for _, k := range t.bumps {
		bumps = append(bumps, k)
	}

	if t.sourcePlaneImport != nil {
		if err := t.sourcePlaneImport.Validate(h, t.checkpointWrites, t.records); err != nil {
			return zero, err
		}
	} else if len(t.checkpointWrites)+len(t.checkpointGuards) != 0 {
		return zero, driver.ErrUnsupported
	}
	plan, err := t.offlineHistory.Finalize(h, tail, t.acquired, bumps)
	if err != nil {
		return zero, err
	}
	entry := plan.Entries[len(plan.Entries)-1]
	err = t.store.immediate(ctx, t.lease, func(c *sql.Conn) error {
		if err := t.checkCheckpointGuards(ctx, c); err != nil {
			return err
		}
		if err := restoreEmpty(ctx, c); err != nil {
			return err
		}
		if err := t.publishAllocatorJournal(ctx, c, plan.Meta, true); err != nil {
			return err
		}
		for i, row := range plan.Rows {
			r := row.Record
			id, _ := recordKey(r.Ref)
			rev := revKey(mustUint(r.Revision))
			raw, e := json.Marshal(r)
			if e != nil {
				return e
			}
			if row.Deleted {
				_, e = c.ExecContext(ctx, `DELETE FROM records WHERE key=?`, id)
			} else {
				_, e = c.ExecContext(ctx, `INSERT INTO records(key,data) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET data=excluded.data`, id, raw)
			}
			if e != nil {
				return e
			}
			if _, e = c.ExecContext(ctx, `INSERT OR IGNORE INTO objects(key,kind) VALUES(?,?)`, id, r.Kind); e != nil {
				return e
			}
			v := plan.Versions[i]
			raw, e = json.Marshal(v)
			if e != nil {
				return e
			}
			if _, e = c.ExecContext(ctx, `INSERT INTO versions(key,rev,base,data) VALUES(?,?,?,?)`, id, rev, v.Base, raw); e != nil {
				return e
			}
			if e = populateHistoricalQueryFields(ctx, c, id, rev, raw); e != nil {
				return e
			}
			state := driver.LockState{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &r.Ref}, Revision: r.Revision, Present: !row.Deleted}
			if e = publishKey(ctx, c, state); e != nil {
				return e
			}
		}
		if err := rebuildQueryOrder(ctx, c); err != nil {
			return err
		}
		for _, state := range t.derived {
			state.Revision = plan.Meta.Head
			if e := publishKey(ctx, c, state); e != nil {
				return e
			}
		}
		for _, key := range t.bumps {
			if e := publishKey(ctx, c, driver.LockState{Key: key, Revision: plan.Meta.Head, Present: true}); e != nil {
				return e
			}
		}
		for prefix, a := range t.allocators {
			raw, e := json.Marshal(a)
			if e != nil {
				return e
			}
			if _, e = c.ExecContext(ctx, `INSERT INTO allocator_states(prefix,data) VALUES(?,?)`, prefix, raw); e != nil {
				return e
			}
		}
		for hash, data := range t.blobs {
			if data == nil {
				data = []byte{}
			}
			if _, e := c.ExecContext(ctx, `INSERT INTO blobs(hash,rev,data) VALUES(?,?,?)`, string(hash), revKey(mustUint(plan.Meta.Head)), data); e != nil {
				return e
			}
		}
		var final []byte
		for _, e := range plan.Entries {
			raw, err := json.Marshal(e)
			if err != nil {
				return err
			}
			if _, err = c.ExecContext(ctx, `INSERT INTO changesets(rev,data) VALUES(?,?)`, revKey(mustUint(e.Revision)), raw); err != nil {
				return err
			}
			final = raw
		}
		if _, e := c.ExecContext(ctx, `UPDATE meta SET store_id=?,incarnation=?,head=?,history_floor=? WHERE singleton=1`, string(plan.Meta.StoreID), plan.Meta.Incarnation, revKey(mustUint(plan.Meta.Head)), revKey(mustUint(plan.Meta.HistoryFloor))); e != nil {
			return e
		}
		if err := t.publishCheckpoints(ctx, c); err != nil {
			return err
		}
		return t.store.saveReplay(ctx, c, h, plan.Meta.Head, final)
	})
	if err == nil {
		t.finish()
		return entry, nil
	}
	var uncertain commitError
	if errors.As(err, &uncertain) {
		t.finish()
		return zero, driver.ErrIndeterminate{IdempotencyKey: h.IdempotencyKey}
	}
	return zero, err
}

var _ driver.OfflineHistoryTx = (*transaction)(nil)
