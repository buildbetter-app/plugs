package memory

import (
	"context"
	"errors"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestBeadMutationRequiresExactPredicate(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease, err := s.Lease(ctx, "test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	meta, _ := s.Meta(ctx)
	ref := model.RefByID("pl-target", model.Natural64(0))
	wrong, _ := driver.NewBeadSentinel("pl-other")
	keys := []driver.LockKey{{Kind: driver.LockRecord, Ref: &ref}, {Kind: driver.LockSentinel, Sentinel: wrong}}
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Lock(ctx, []driver.LockRequest{{Key: keys[0], Mode: driver.LockUpdate}, {Key: keys[1], Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Put(ctx, driver.Record{Ref: ref, Kind: "bead", Body: []byte(`{}`)}); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{wrong}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "test"}, LockKeys: keys, Changes: []model.ChangeRecord{{Ref: ref, Op: model.OpCreate}}}); err == nil {
		t.Fatal("wrong bead scope accepted")
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	r, _ := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	defer r.Close()
	records, err := r.Get(ctx, []driver.Ref{ref})
	if err != nil || len(records) != 0 {
		t.Fatal("failed predicate validation published")
	}
	states, err := r.Observe(ctx, keys)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range states {
		if value.Present || !value.Revision.IsZero() {
			t.Fatal("failed predicate validation bumped revisions")
		}
	}
}

func TestHistoricalBoundsAndIsolation(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease, err := s.Lease(ctx, "test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	ref := model.RefByID("pl-history", model.Natural64(0))
	change := conformance.FixtureChange{Ref: ref, Kind: "test", Op: model.OpCreate, Body: []byte(`{"value":1}`)}
	if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{change}); err != nil {
		t.Fatal(err)
	}
	change.Op = model.OpPatch
	change.Body = []byte(`{"value":2}`)
	if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{change}); err != nil {
		t.Fatal(err)
	}
	meta, _ := s.Meta(ctx)
	r, _ := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	defer r.Close()
	view := r.Snapshot()
	*view.At = model.Natural64(0)
	if r.Snapshot().At.String() != "2" {
		t.Fatal("Snapshot returned shared revision pointer")
	}
	versions, err := r.Versions(ctx, ref, driver.PageReq{})
	if err != nil {
		t.Fatal(err)
	}
	versions.Items[0].Body[0] = '!'
	versions.Items[1].Patch[0] = '!'
	at := model.Natural64(1)
	past, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	if err != nil {
		t.Fatal(err)
	}
	defer past.Close()
	records, err := past.Get(ctx, []driver.Ref{ref})
	if err != nil || string(records[0].Body) != `{"value":1}` {
		t.Fatal("version byte alias leaked")
	}
	// History-floor plumbing is tested directly; restore/compaction APIs are
	// outside this driver's task and are not simulated as public operations.
	s.mu.Lock()
	next := s.state.fork()
	next.meta.HistoryFloor = model.Natural64(2)
	s.state = next
	s.mu.Unlock()
	_, err = s.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
	var truncated driver.ErrTruncated
	if !errors.As(err, &truncated) || truncated.Floor.String() != "2" {
		t.Fatalf("history floor=%v", err)
	}
	latest, _ := s.Read(ctx, driver.View{Incarnation: meta.Incarnation})
	defer latest.Close()
	if _, err := latest.Changesets(ctx, &driver.Cursor{Incarnation: meta.Incarnation, Revision: at}, 100); !errors.As(err, &truncated) {
		t.Fatalf("log floor=%v", err)
	}
	tooLarge := model.Natural("18446744073709551616")
	if _, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &tooLarge}); err == nil {
		t.Fatal("revision silently narrowed")
	}
	future := model.Natural64(3)
	if _, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &future}); err == nil {
		t.Fatal("future snapshot accepted")
	}
	branchRef := model.RefByID("other", model.Natural64(1))
	if _, err := r.Observe(ctx, []driver.LockKey{{Kind: driver.LockRecord, Ref: &branchRef}}); err == nil {
		t.Fatal("unsupported branch observation accepted")
	}
}

func TestRestoreHistoryCapabilityRefusesBeforeMutation(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()

	before, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := driver.RequireRestoreCapability(s, driver.RestoreModeCurrentImage); err != nil {
		t.Fatalf("current-image restore was refused: %v", err)
	}
	if err := driver.RequireRestoreCapability(s, driver.RestoreModeFullHistory); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("full-history restore error=%v", err)
	}
	if _, err := driver.RestoreModeForHistory("partial"); err == nil {
		t.Fatal("malformed history was accepted")
	}
	after, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if before != after {
		t.Fatalf("restore capability validation mutated memory metadata: before=%+v after=%+v", before, after)
	}
}
