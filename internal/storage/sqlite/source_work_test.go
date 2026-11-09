package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

type sourceWorkObserver struct {
	total  map[driver.ReadWorkKind]int64
	scoped map[driver.ReadWorkKind]int64
}

type totalOnlySourceWorkObserver struct{ total map[driver.ReadWorkKind]int64 }

func (o *totalOnlySourceWorkObserver) ObserveReadWork(kind driver.ReadWorkKind, amount int64) {
	o.total[kind] += amount
}

func (o *sourceWorkObserver) ObserveReadWork(kind driver.ReadWorkKind, amount int64) {
	o.total[kind] += amount
}

func (o *sourceWorkObserver) ObserveScopedReadWork(scope driver.ReadWorkScope, kind driver.ReadWorkKind, amount int64) {
	if scope == driver.ReadWorkOntologyValidation {
		o.scoped[kind] += amount
	}
}

func TestGraphSourceCountsActualLookaheadMaterializations(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "source-work.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	parent, child := testRef("work-parent"), testRef("work-child")
	body := `{"id":"work-child","title":"read"}`
	tx, h := beginSourceTest(t, s, lease, child)
	if err := tx.PutGraphRecord(ctx, sourceRecord(child.ID, body), &parent, nil); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, h)
	// A hierarchy read is allowed only after a real committed graph startup
	// proves both Parent and Links coverage. Keep that proof separate from the
	// lookahead read so the observer measures materialization rather than an
	// unavailable-coverage refusal.
	if err := s.BuildGraphSourceIndexes(ctx, lease, func(driver.Record) (driver.GraphSourceMetadata, error) {
		return driver.GraphSourceMetadata{Parent: &parent}, nil
	}); err != nil {
		t.Fatal(err)
	}

	r := sourceReader(t, s, nil)
	o := &sourceWorkObserver{total: map[driver.ReadWorkKind]int64{}, scoped: map[driver.ReadWorkKind]int64{}}
	measured := driver.WithReadWorkScope(driver.WithReadWorkObserver(ctx, o), driver.ReadWorkOntologyValidation)
	rows, more, err := r.HierarchyChildren(measured, []driver.Ref{parent}, 0)
	if err != nil || len(rows) != 0 || !more {
		t.Fatalf("zero-limit lookahead: rows=%+v more=%v err=%v", rows, more, err)
	}
	if o.total[driver.ReadSourceHeaders] < 1 || o.total[driver.ReadSourceQueries] < 2 || o.total[driver.ReadSourceImages] != 1 {
		t.Fatalf("source work missed lookahead: %+v", o.total)
	}
	// Stored SQL Scan allocates the body once and fetchImages clones it once.
	// Both are real copies although the API emitted zero records.
	if o.total[driver.ReadPayloadRecords] != 2 || o.total[driver.ReadPayloadBytes] != int64(2*len(body)) {
		t.Fatalf("payload copies missed lookahead: %+v", o.total)
	}
	for kind, total := range o.total {
		if o.scoped[kind] != total {
			t.Fatalf("scope %s=%d total=%d", kind, o.scoped[kind], total)
		}
	}
	// The optional scoped callback cannot replace or duplicate the total
	// callback. A caller with the old observer interface sees the same work.
	legacy := &totalOnlySourceWorkObserver{total: map[driver.ReadWorkKind]int64{}}
	legacyCtx := driver.WithReadWorkScope(driver.WithReadWorkObserver(ctx, legacy), driver.ReadWorkOntologyValidation)
	if _, _, err := r.HierarchyChildren(legacyCtx, []driver.Ref{parent}, 0); err != nil {
		t.Fatal(err)
	}
	for kind, total := range o.total {
		if legacy.total[kind] != total {
			t.Fatalf("legacy total %s=%d scoped-compatible total=%d", kind, legacy.total[kind], total)
		}
	}
	// Scope lives only on its derived context, and a nil observer is inert.
	if _, _, err := r.HierarchyChildren(driver.WithReadWorkObserver(ctx, nil), []driver.Ref{parent}, 0); err != nil {
		t.Fatal(err)
	}
	beforeTotal := cloneSourceWorkCounts(o.total)
	beforeScoped := cloneSourceWorkCounts(o.scoped)
	if _, _, err := r.HierarchyChildren(driver.WithReadWorkObserver(ctx, o), []driver.Ref{parent}, 0); err != nil {
		t.Fatal(err)
	}
	if o.total[driver.ReadSourceImages] != beforeTotal[driver.ReadSourceImages]+1 || !sameSourceWorkCounts(o.scoped, beforeScoped) {
		t.Fatalf("unscoped work missing or scope leaked: total=%+v before=%+v scoped=%+v before=%+v", o.total, beforeTotal, o.scoped, beforeScoped)
	}
}

func cloneSourceWorkCounts(in map[driver.ReadWorkKind]int64) map[driver.ReadWorkKind]int64 {
	out := make(map[driver.ReadWorkKind]int64, len(in))
	for kind, value := range in {
		out[kind] = value
	}
	return out
}

func sameSourceWorkCounts(a, b map[driver.ReadWorkKind]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for kind, value := range a {
		if b[kind] != value {
			return false
		}
	}
	return true
}

func TestGraphSourceCountsStagedLookaheadClone(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "staged-source-work.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	// Publish one independent typed graph image so the source family has
	// real committed coverage before the staged lookahead is requested.
	seedParent, seed := testRef("staged-work-seed-parent"), testRef("staged-work-seed")
	seedTx, seedHeader := beginSourceTest(t, s, lease, seed)
	if err := seedTx.PutGraphRecord(ctx, sourceRecord(seed.ID, `{"id":"staged-work-seed"}`), &seedParent, nil); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, seedTx, seedHeader)
	if err := s.BuildGraphSourceIndexes(ctx, lease, func(driver.Record) (driver.GraphSourceMetadata, error) {
		return driver.GraphSourceMetadata{Parent: &seedParent}, nil
	}); err != nil {
		t.Fatal(err)
	}
	parent, child := testRef("staged-work-parent"), testRef("staged-work-child")
	body := `{"id":"staged-work-child"}`
	tx, _ := beginSourceTest(t, s, lease, child)
	if err := tx.PutGraphRecord(ctx, sourceRecord(child.ID, body), &parent, nil); err != nil {
		t.Fatal(err)
	}
	o := &sourceWorkObserver{total: map[driver.ReadWorkKind]int64{}, scoped: map[driver.ReadWorkKind]int64{}}
	measured := driver.WithReadWorkObserver(ctx, o)
	rows, more, err := tx.HierarchyChildren(measured, []driver.Ref{parent}, 0)
	if err != nil || len(rows) != 0 || !more {
		t.Fatalf("staged zero-limit lookahead: rows=%+v more=%v err=%v", rows, more, err)
	}
	if o.total[driver.ReadSourceHeaders] != 1 || o.total[driver.ReadSourceImages] != 1 || o.total[driver.ReadPayloadRecords] != 1 || o.total[driver.ReadPayloadBytes] != int64(len(body)) {
		t.Fatalf("staged alias must charge only final body clone: %+v", o.total)
	}
	if len(o.scoped) != 0 {
		t.Fatalf("unscoped read acquired scope: %+v", o.scoped)
	}
}
