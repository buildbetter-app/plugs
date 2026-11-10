package postgres

import (
	"context"
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
	b, err := driver.PrepareOfflineHistoryBinding(t.meta, p, h, when)
	if err != nil {
		return driver.Meta{}, err
	}
	if _, err = t.prepareRestore(ctx, p.SourceStoreID); err != nil {
		return driver.Meta{}, err
	}
	m := b.Meta()
	t.restore = &m
	t.offlineHistory = b
	return m, nil
}
func (t *transaction) CommitOfflineHistory(ctx context.Context, h driver.ChangesetHeader) (model.Changeset, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	zero := model.Changeset{}
	if err := t.check(ctx); err != nil {
		return zero, t.abortFenced(err)
	}
	if !t.hasHead || t.offlineHistory == nil {
		return zero, fmt.Errorf("history commit requires prepared offline capability and Head")
	}
	if err := t.restoreEmpty(ctx); err != nil {
		return zero, err
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
	// All validation precedes engine writes. A failure below closes and rolls back
	// the SQL transaction, including every history and current-image row.
	fail := func(err error) (model.Changeset, error) { _ = t.finish(); return zero, err }
	if err = t.fence(ctx); err != nil {
		return zero, err
	}
	if err := t.checkCheckpointGuards(ctx); err != nil {
		return fail(err)
	}
	if err := t.publishAllocatorJournal(ctx, t.sql, plan.Meta, true); err != nil {
		return fail(err)
	}
	counts := map[string]uint64{}
	for i, row := range plan.Rows {
		r := row.Record
		id, _ := recordKey(r.Ref)
		counts[id]++
		ref, err := encode(r.Ref)
		if err != nil {
			return fail(err)
		}
		if _, err = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.records VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(key) DO UPDATE SET revision=EXCLUDED.revision,body=EXCLUDED.body,deleted=EXCLUDED.deleted,version_count=EXCLUDED.version_count`, []byte(id), ref, r.Kind, r.Revision.String(), r.Body, row.Deleted, decimal(counts[id])); err != nil {
			return fail(err)
		}
		v := plan.Versions[i]
		raw, err := encode(v)
		if err != nil {
			return fail(err)
		}
		if _, err = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.versions VALUES($1,$2,$3,$4)`, []byte(id), r.Revision.String(), v.Base, raw); err != nil {
			return fail(err)
		}
		if err = t.writeState(ctx, id, driver.LockState{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &r.Ref}, Revision: r.Revision, Present: !row.Deleted}); err != nil {
			return fail(err)
		}
	}
	for id, v := range t.derived {
		v.Revision = plan.Meta.Head
		if err = t.writeState(ctx, id, v); err != nil {
			return fail(err)
		}
	}
	for id, k := range t.bumps {
		if err = t.writeState(ctx, id, driver.LockState{Key: k, Revision: plan.Meta.Head, Present: true}); err != nil {
			return fail(err)
		}
	}
	for prefix, a := range t.allocators {
		raw, err := encode(a)
		if err != nil {
			return fail(err)
		}
		if _, err = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.allocator_states(prefix,data) VALUES($1,$2)`, prefix, raw); err != nil {
			return fail(err)
		}
	}
	for hash, data := range t.blobs {
		if _, err = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.blobs VALUES($1,$2,$3)`, string(hash), plan.Meta.Head.String(), data); err != nil {
			return fail(err)
		}
	}
	for _, entry := range plan.Entries {
		raw, err := encode(entry)
		if err != nil {
			return fail(err)
		}
		if _, err = t.sql.ExecContext(ctx, `INSERT INTO plug_postgres.changesets VALUES($1,$2)`, entry.Revision.String(), raw); err != nil {
			return fail(err)
		}
	}
	if _, err = t.sql.ExecContext(ctx, `UPDATE plug_postgres.meta SET store_id=$1,incarnation=$2,head=$3,history_floor=$4 WHERE singleton`, string(plan.Meta.StoreID), plan.Meta.Incarnation, plan.Meta.Head.String(), plan.Meta.HistoryFloor.String()); err != nil {
		return fail(err)
	}
	response, err := responseAt(h.Response, plan.Meta.Head)
	if err != nil {
		return fail(err)
	}
	principal, err := driver.PrincipalKey(h.Principal)
	if err != nil {
		return fail(err)
	}
	scope, err := scopeKey(principal, h.AgentLabel, h.Method, h.IdempotencyKey)
	if err != nil {
		return fail(err)
	}
	entry := plan.Entries[len(plan.Entries)-1]
	if err = t.saveReceipt(ctx, scope, h, response, entry); err != nil {
		return fail(err)
	}
	if err = t.publishCheckpoints(ctx); err != nil {
		return fail(err)
	}
	if err = t.publish(ctx, h.IdempotencyKey); err != nil {
		return zero, err
	}
	return entry, nil
}

var _ driver.OfflineHistoryTx = (*transaction)(nil)
