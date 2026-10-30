package conformance

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type auditStaged struct {
	tx     driver.WriteTx
	header driver.ChangesetHeader
	hash   driver.Hash
	keys   []driver.LockKey
}

func stageAudit(t *testing.T, f auditFixture) auditStaged {
	t.Helper()
	sentinel, err := driver.NewBeadSentinel("pl-blocker")
	if err != nil {
		t.Fatal(err)
	}
	fact, err := driver.NewReadFact("audit/guard")
	if err != nil {
		t.Fatal(err)
	}
	requests := update(recordLock("pl-blocker"), driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel}, driver.LockKey{Kind: driver.LockReadFact, Fact: fact}, driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "audit-rank"}})
	tx := begin(t, f.ctx, f.store, f.lease, requests)
	states, err := tx.Observe(f.ctx, lockKeys(requests))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put(f.ctx, driver.Record{Ref: f.changes[0].Ref, Kind: "bead", Body: f.changes[0].Body}); err != nil {
		t.Fatal(err)
	}
	hash, err := tx.PutBlob(f.ctx, []byte("audit-payload"))
	if err != nil {
		t.Fatal(err)
	}
	if err := tx.PutDerived(f.ctx, driver.DerivedSortKey, nil, []driver.DerivedRow{{Key: "audit-rank", Value: []byte("audit-value")}}); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(f.ctx, []driver.Sentinel{sentinel}); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpReadFacts(f.ctx, []driver.ReadFact{fact}); err != nil {
		t.Fatal(err)
	}
	h := f.header
	h.Events = model.CloneEventDescriptors(h.Events)
	h.LockKeys = lockKeys(requests)
	h.Changes = []model.ChangeRecord{{Ref: f.changes[0].Ref, Op: model.OpPatch, FromRevision: states[0].Revision, ChangedPaths: []string{"/status"}}}
	return auditStaged{tx: tx, header: h, hash: hash, keys: h.LockKeys}
}

func auditAborts(t *testing.T, factory Factory) {
	for _, mode := range []string{"invalid_plan", "rollback", "cancel", "fence"} {
		t.Run(mode, func(t *testing.T) {
			f := newAuditFixture(t, factory, 2)
			before := read(t, f.ctx, f.store, f.meta, nil)
			staged := stageAudit(t, f)
			ctx := f.ctx
			switch mode {
			case "invalid_plan":
				staged.header.Events[1].Index = "9"
			case "rollback":
				if err := staged.tx.Rollback(ctx); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "fence":
				if err := f.lease.Release(ctx); err != nil {
					t.Fatal(err)
				}
				next, err := f.store.Lease(ctx, "audit-takeover", driver.LeaseTTL)
				if err != nil {
					t.Fatal(err)
				}
				defer next.Release(context.Background())
			}
			_, err := staged.tx.Commit(ctx, staged.header)
			switch mode {
			case "invalid_plan":
				if err == nil {
					t.Fatal("invalid plan committed other staged state")
				}
			case "rollback":
				if !errors.Is(err, driver.ErrTxClosed) {
					t.Fatalf("rolled-back commit=%v", err)
				}
			case "cancel":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled commit=%v", err)
				}
			case "fence":
				requireFenced(t, err)
			}
			_ = staged.tx.Close()
			auditUnchanged(t, f, before)
			auditNoExtraPublication(t, f, before, staged)
		})
	}
}

func auditNoExtraPublication(t *testing.T, f auditFixture, before driver.Reader, staged auditStaged) {
	t.Helper()
	r := read(t, f.ctx, f.store, f.meta, nil)
	if _, err := r.Blob(f.ctx, staged.hash); !errors.Is(err, driver.ErrNotFound) {
		t.Fatalf("aborted audit published blob: %v", err)
	}
	old, err := before.Observe(f.ctx, staged.keys)
	if err != nil {
		t.Fatal(err)
	}
	current, err := r.Observe(f.ctx, staged.keys)
	if err != nil || !reflect.DeepEqual(current, old) {
		t.Fatalf("aborted audit published derived/predicate state: %+v %v", current, err)
	}
}

func auditTakeoverRace(t *testing.T, factory Factory) {
	for i := 0; i < 8; i++ {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			f := newAuditFixture(t, factory, 2)
			before := read(t, f.ctx, f.store, f.meta, nil)
			staged := stageAudit(t, f)
			start := make(chan struct{})
			type outcome struct {
				entry model.Changeset
				err   error
			}
			committed := make(chan outcome, 1)
			taken := make(chan error, 1)
			go func() {
				<-start
				entry, err := staged.tx.Commit(f.ctx, staged.header)
				committed <- outcome{entry, err}
			}()
			go func() { <-start; taken <- f.lease.Release(f.ctx) }()
			close(start)
			result := <-committed
			if err := <-taken; err != nil {
				t.Fatal(err)
			}
			next, err := f.store.Lease(f.ctx, "audit-takeover", driver.LeaseTTL)
			if err != nil {
				t.Fatal(err)
			}
			defer next.Release(context.Background())
			if result.err != nil {
				requireFenced(t, result.err)
				auditUnchanged(t, f, before)
				auditNoExtraPublication(t, f, before, staged)
			} else {
				r := read(t, f.ctx, f.store, f.meta, nil)
				cached, err := r.IdempotencyByKey(f.ctx, "system:conformance", "", f.header.Method, f.header.IdempotencyKey)
				if err != nil || cached == nil || !reflect.DeepEqual(cached.Changeset, result.entry) || !reflect.DeepEqual(result.entry.Events, f.header.Events) || r.Snapshot().At.String() != "2" {
					t.Fatalf("racing audit receipt/plan/head split: %+v %v", cached, err)
				}
				data, err := r.Blob(f.ctx, staged.hash)
				if err != nil || string(data) != "audit-payload" {
					t.Fatalf("racing audit blob missing: %q %v", data, err)
				}
				states, err := r.Observe(f.ctx, staged.keys)
				if err != nil {
					t.Fatal(err)
				}
				for _, state := range states {
					if !state.Present || state.Revision.String() != "2" {
						t.Fatalf("racing audit graph/predicate split: %+v", state)
					}
				}
			}
			// Loss must release both row and head locks for the new holder.
			if _, err := Seed(f.ctx, f.store, next, driver.ChangesetHeader{}, f.changes); err != nil {
				t.Fatalf("takeover retained stale locks: %v", err)
			}
		})
	}
}
