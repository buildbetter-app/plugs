package s1b21b24

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/memory"
	"github.com/buildbetter-app/plugs/internal/storage/postgres"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

// TestS1B22CloseRefusesStaleExpectedRevision is the B22 line item carried
// from B08: a close that names an out-of-date expected revision is refused
// with the typed CAS outcome and publishes nothing. The concurrent close/close
// and update/close forms are TestS1B22LifecycleCASRacesBindWinnerEventsAndReopen;
// this case pins the sequential form on both public close surfaces, reuse of
// the refused key for the corrected close, and the card edge that the
// expectation is checked before an otherwise repeated close.
// TestS1B22RacedCloseRefusesAtCommitRecheck forces the raced form.
func TestS1B22CloseRefusesStaleExpectedRevision(t *testing.T) {
	for _, engine := range transactionEngines() {
		for _, method := range []string{"bead.close", "tx.apply"} {
			t.Run(engine+"/"+method, func(t *testing.T) {
				f := newFixture(t, engine)
				target, created := f.create(t, "b22-close-stale-seed", "close guard")
				moved := commit(t, f.call("bead.update", "b22-close-stale-move", map[string]any{
					"id": target, "fields": map[string]any{"description": "moved past the caller's revision"},
				}))
				if created.Revision.String() != "1" || moved.Revision.String() != "2" {
					t.Fatalf("B22 stale-close seed revisions = %s, %s; want 1, 2", created.Revision, moved.Revision)
				}
				params := func(expected model.Natural, reason string) map[string]any {
					expect := model.PreconditionByRevision(target, expected)
					if method == "tx.apply" {
						return batchParams(closeOp(target, reason, expect))
					}
					return map[string]any{"id": target, "reason": reason, "expect": []model.Precondition{expect}}
				}

				before := f.state(t)
				if doc := closeDocument(t, before, target); doc.Fields["status"] != "todo" || doc.ClosedAt != nil || len(doc.Comments) != 0 {
					t.Fatalf("B22 stale-close target is not open before the refusal: %+v", doc)
				}
				for _, reopen := range []bool{false, true} {
					if reopen {
						f.reopen(t)
					}
					stale := f.call(method, "b22-close-stale", params(created.Revision, "stale close"))
					b22CASRefusal(t, stale, target, "revision", "2")
					sameState(t, before, f.state(t))
					if f.casReceipt(t, method, "b22-close-stale") != nil {
						t.Fatalf("B22 stale close stored a receipt (reopen=%v)", reopen)
					}
				}

				// The refused key was never consumed: it commits the corrected close.
				current := f.call(method, "b22-close-stale", params(moved.Revision, "current close"))
				closed := commit(t, current)
				if current.Meta.Replayed || closed.Revision.String() != "3" {
					t.Fatalf("B22 current-revision close did not commit revision 3: %+v", closed)
				}
				after := f.state(t)
				doc := closeDocument(t, after, target)
				if doc.Fields["status"] != "done" || doc.ClosedAt == nil || len(doc.Comments) != 1 || doc.Comments[0].Body != "current close" {
					t.Fatalf("B22 current-revision close image differs: %+v", doc)
				}
				if len(after.Changesets) != 3 || len(after.Versions[target.ID]) != 3 || f.casReceipt(t, method, "b22-close-stale") == nil {
					t.Fatal("B22 current-revision close did not publish exactly one changeset, version and receipt")
				}

				repeat := f.call(method, "b22-close-repeat", params(moved.Revision, "repeat close"))
				b22CASRefusal(t, repeat, target, "revision", "3")
				sameState(t, after, f.state(t))
				if f.casReceipt(t, method, "b22-close-repeat") != nil {
					t.Fatal("B22 stale repeat close stored a receipt")
				}

				f.reopen(t)
				replay := f.call(method, "b22-close-stale", params(moved.Revision, "current close"))
				if replay.Error != nil || !replay.Meta.Replayed || !bytes.Equal(replay.Result, current.Result) {
					t.Fatalf("B22 current-revision close replay differs after reopen: %+v", replay)
				}
				sameState(t, after, f.state(t))
			})
		}
	}
}

// b22WriteRace runs one armed hook at the next Store.Write, i.e. after the
// Writer has prepared and authorized its attempt and before it commits.
type b22WriteRace struct {
	hook  atomic.Pointer[func()]
	fired atomic.Int32
}

func (r *b22WriteRace) arm(hook func()) { r.hook.Store(&hook) }

func (r *b22WriteRace) beforeWrite() {
	if hook := r.hook.Swap(nil); hook != nil {
		r.fired.Add(1)
		(*hook)()
	}
}

// The wrappers embed each concrete real store so every optional driver
// capability is promoted unchanged; only Write is intercepted.
type b22MemoryRace struct {
	*memory.Store
	race *b22WriteRace
}

func (s *b22MemoryRace) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.race.beforeWrite()
	return s.Store.Write(ctx, lease)
}

type b22SQLiteRace struct {
	*sqlite.Store
	race *b22WriteRace
}

func (s *b22SQLiteRace) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.race.beforeWrite()
	return s.Store.Write(ctx, lease)
}

type b22PostgresRace struct {
	*postgres.Store
	race *b22WriteRace
}

func (s *b22PostgresRace) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	s.race.beforeWrite()
	return s.Store.Write(ctx, lease)
}

// installWriteRace rebinds the fixture's Writer and service over a racing
// wrapper of its open real store.
func (f *fixture) installWriteRace(t *testing.T) *b22WriteRace {
	t.Helper()
	race := &b22WriteRace{}
	if err := f.lease.Release(f.ctx); err != nil {
		t.Fatal(err)
	}
	switch real := f.store.(type) {
	case *memory.Store:
		f.store = &b22MemoryRace{Store: real, race: race}
	case *sqlite.Store:
		f.store = &b22SQLiteRace{Store: real, race: race}
	case *postgres.Store:
		f.store = &b22PostgresRace{Store: real, race: race}
	default:
		t.Fatalf("raced close needs a real store, got %T", f.store)
	}
	f.bind(t)
	return race
}

// TestS1B22RacedCloseRefusesAtCommitRecheck forces the B08 raced stale close.
// The close names the then-current revision, so preparation and the CAS pass;
// a competing update commits at the close's Store.Write; the commit guard
// sends the close back through preparation, where its expectation is now
// stale. Covers bead.close (changeSet expect) and the tx.apply ordered close
// (operation expect).
func TestS1B22RacedCloseRefusesAtCommitRecheck(t *testing.T) {
	for _, engine := range transactionEngines() {
		for _, method := range []string{"bead.close", "tx.apply"} {
			t.Run(engine+"/"+method, func(t *testing.T) {
				f := newFixture(t, engine)
				target, created := f.create(t, "b22-raced-close-seed", "raced close guard")
				race := f.installWriteRace(t)
				params := func(expected model.Natural, reason string) map[string]any {
					expect := model.PreconditionByRevision(target, expected)
					if method == "tx.apply" {
						return batchParams(closeOp(target, reason, expect))
					}
					return map[string]any{"id": target, "reason": reason, "expect": []model.Precondition{expect}}
				}
				var moved reply
				race.arm(func() {
					moved = f.call("bead.update", "b22-raced-close-move", map[string]any{
						"id": target, "fields": map[string]any{"description": "committed inside the close's write window"},
					})
				})
				before := f.state(t)
				raced := f.call(method, "b22-raced-close", params(created.Revision, "raced close"))
				if race.fired.Load() != 1 {
					t.Fatalf("B22 raced close hook fired %d times; want exactly 1", race.fired.Load())
				}
				if c := commit(t, moved); c.Revision.String() != "2" {
					t.Fatalf("B22 competing update committed revision %s; want 2", c.Revision)
				}
				b22CASRefusal(t, raced, target, "revision", "2")
				after := f.state(t)
				if len(after.Changesets) != len(before.Changesets)+1 || len(after.Versions[target.ID]) != 2 {
					t.Fatal("B22 raced close published more than the competing update")
				}
				if doc := closeDocument(t, after, target); doc.Fields["status"] != "todo" || doc.ClosedAt != nil || len(doc.Comments) != 0 {
					t.Fatalf("B22 raced close partially applied: %+v", doc)
				}
				if f.casReceipt(t, method, "b22-raced-close") != nil {
					t.Fatal("B22 raced close stored a receipt")
				}
				closed := commit(t, f.call(method, "b22-raced-close", params(model.Natural64(2), "corrected close")))
				if closed.Revision.String() != "3" || race.fired.Load() != 1 {
					t.Fatalf("B22 corrected close after the race did not commit revision 3 once: %+v", closed)
				}
			})
		}
	}
}
