package sqlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sourceindex"
)

func sourceRecord(id, body string) driver.Record {
	return driver.Record{Ref: testRef(id), Kind: "bead", Body: []byte(body)}
}

func beginSourceTest(t *testing.T, s *Store, lease driver.ActiveLease, refs ...model.Ref) (*transaction, driver.ChangesetHeader) {
	t.Helper()
	return beginSourceTestWithLocks(t, s, lease, nil, true, refs...)
}

func beginSourceTestWithLocks(t *testing.T, s *Store, lease driver.ActiveLease, extra []driver.LockKey, bump bool, refs ...model.Ref) (*transaction, driver.ChangesetHeader) {
	t.Helper()
	ctx := context.Background()
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Close() })
	keys := append([]driver.LockKey{driver.GraphBlockingSentinelLock}, extra...)
	sentinels := []driver.Sentinel{driver.SentinelGraphBlocking}
	for _, ref := range refs {
		ref := ref
		keys = append(keys, driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		bead, err := driver.NewBeadSentinel(ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead})
		sentinels = append(sentinels, bead)
	}
	sort.Slice(keys, func(i, j int) bool { return keyID(keys[i]) < keyID(keys[j]) })
	requests := make(driver.LockSet, len(keys))
	for i, key := range keys {
		requests[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	if _, err = tx.Lock(ctx, requests); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if bump {
		if err = tx.BumpSentinels(ctx, sentinels); err != nil {
			t.Fatal(err)
		}
	}
	return tx.(*transaction), driver.ChangesetHeader{Incarnation: getMeta(t, ctx, s).Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "source-index"}, LockKeys: keys}
}

// Native source fixtures declare an explicitly empty allocator inventory, bind
// it before Lock/Head, and stage it before any bead or predicate publication.
func beginNativeSourceTest(t *testing.T, s *Store, lease driver.ActiveLease, profile driver.NativeRestoreProfile, refs ...model.Ref) (*transaction, driver.ChangesetHeader, driver.Meta, error) {
	t.Helper()
	ctx := context.Background()
	snapshot, err := driver.NewAllocatorSnapshot(driver.Meta{StoreID: profile.StoreID, Incarnation: "native-source-incarnation"}, model.Natural64(1), model.Natural64(0), driver.AllocatorInventory{})
	if err != nil {
		t.Fatal(err)
	}
	profile.AllocatorBinding, err = snapshot.Binding()
	if err != nil {
		t.Fatal(err)
	}
	fence, err := driver.NativeAllocatorRestoreLock()
	if err != nil {
		t.Fatal(err)
	}
	tx, h := beginSourceTestWithLocks(t, s, lease, []driver.LockKey{fence}, false, refs...)
	meta, err := tx.PrepareNativeRestore(ctx, profile)
	if err == nil {
		err = tx.RestoreNativeAllocators(ctx, snapshot)
	}
	return tx, h, meta, err
}

func commitSourceTest(t *testing.T, tx *transaction, h driver.ChangesetHeader) model.Changeset {
	t.Helper()
	keys := make([]string, 0, len(tx.records))
	for id := range tx.records {
		keys = append(keys, id)
	}
	sort.Strings(keys)
	for _, id := range keys {
		ref := tx.recordRef(id)
		previous, err := tx.observe(context.Background(), driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		if err != nil {
			t.Fatal(err)
		}
		op := model.OpPatch
		if previous.Revision.IsZero() {
			op = model.OpCreate
		}
		if tx.records[id] == nil {
			op = model.OpDelete
		}
		h.Changes = append(h.Changes, model.ChangeRecord{Ref: ref, Op: op, FromRevision: previous.Revision})
	}
	entry, err := tx.Commit(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func sourceReader(t *testing.T, s *Store, at *model.Revision) *reader {
	t.Helper()
	return getReader(t, context.Background(), s, getMeta(t, context.Background(), s), at).(*reader)
}

func requireSources(t *testing.T, rows []driver.Record, more bool, err error, wantMore bool, ids ...string) {
	t.Helper()
	got := make([]string, 0, len(rows))
	for _, row := range rows {
		got = append(got, row.Ref.ID)
	}
	if err != nil || more != wantMore || !reflect.DeepEqual(got, ids) && !(len(got) == 0 && len(ids) == 0) {
		t.Fatalf("sources=%q more=%v err=%v want=%q more=%v", got, more, err, ids, wantMore)
	}
}

func TestGraphSourcePublicationKeepsExactHistoryAndReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "sources.db")
	s := openStore(t, ctx, path)
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	p, q := testRef("pl-p"), testRef("pl-q")
	a, z := testRef("pl-a"), testRef("pl-z")
	tx, h := beginSourceTest(t, s, lease, a, z)
	want := []byte(" \n{\"opaque\": [ 1, 2 ]} \t")
	if err := tx.PutGraphRecord(ctx, driver.Record{Ref: a, Kind: "bead", Body: want}, &p, []driver.SourceLink{{Kind: "waits-for", To: q}, {Kind: "custom\x00 ", To: p}}); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutGraphRecord(ctx, sourceRecord(z.ID, `{}`), &p, []driver.SourceLink{{Kind: "blocks", To: q}}); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, h)
	pinned := sourceReader(t, s, nil)
	rows, more, err := pinned.HierarchyChildren(ctx, []model.Ref{p}, 1)
	requireSources(t, rows, more, err, true, a.ID)
	if !bytes.Equal(rows[0].Body, want) {
		t.Fatal("current source bytes changed")
	}
	rows[0].Body[0] = '!'
	zero := model.Natural64(0)
	rows, more, err = sourceReader(t, s, &zero).HierarchyChildren(ctx, []model.Ref{p}, 0)
	requireSources(t, rows, more, err, false)
	tx, h = beginSourceTest(t, s, lease, a)
	if err = tx.PutGraphRecord(ctx, sourceRecord(a.ID, `{"moved":true}`), &q, nil); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, h)
	rows, more, err = pinned.InverseSourceLinks(ctx, []model.Ref{q}, []driver.IncomingLinkKind{driver.IncomingLinkWaitsFor}, nil, 10)
	requireSources(t, rows, more, err, false, a.ID)
	if !bytes.Equal(rows[0].Body, want) {
		t.Fatal("pinned source bytes changed")
	}
	tx, h = beginSourceTest(t, s, lease)
	commitSourceTest(t, tx, h)
	coverage, err := sourceindex.SQLite.ReadCoverage(ctx, s.db, h.Incarnation)
	if err != nil || coverage.Parent.Through != 3 || coverage.Links.Through != 3 {
		t.Fatalf("source-free coverage=%+v err=%v", coverage, err)
	}
	var kind []byte
	if err = s.db.QueryRowContext(ctx, `SELECT link_kind FROM graph_link_history WHERE link_kind=?`, []byte("custom\x00 ")).Scan(&kind); err != nil || string(kind) != "custom\x00 " {
		t.Fatalf("custom kind bytes=%q err=%v", kind, err)
	}
	_ = pinned.Close()
	_ = lease.Release(ctx)
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openStore(t, ctx, path)
	one := model.Natural64(1)
	rows, more, err = sourceReader(t, s, &one).HierarchyChildren(ctx, []model.Ref{p}, 10)
	requireSources(t, rows, more, err, false, a.ID, z.ID)
	if !bytes.Equal(rows[0].Body, want) {
		t.Fatal("reopened history bytes changed")
	}
}

func TestGraphSourceStagingExclusionsAndFailureAtomicity(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "overlay.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	p, q := testRef("pl-p"), testRef("pl-q")
	a, b, c, d, e := testRef("pl-a"), testRef("pl-b"), testRef("pl-c"), testRef("pl-d"), testRef("pl-e")
	tx, h := beginSourceTest(t, s, lease, a, b, c, d)
	for _, ref := range []model.Ref{a, b, c, d} {
		if err := tx.PutGraphRecord(ctx, sourceRecord(ref.ID, `{}`), &p, []driver.SourceLink{{Kind: "blocks", To: q}}); err != nil {
			t.Fatal(err)
		}
	}
	commitSourceTest(t, tx, h)
	tx, h = beginSourceTest(t, s, lease, a, b, e)
	bound := tx.Snapshot()
	if err := tx.PutGraphRecord(ctx, sourceRecord(a.ID, `{"moved":true}`), &q, nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.Delete(ctx, b); err != nil {
		t.Fatal(err)
	}
	parent := p
	links := []driver.SourceLink{{Kind: "blocks", To: q}, {Kind: "parent-child", To: q}}
	if err := tx.PutGraphRecord(ctx, sourceRecord(e.ID, " {\"new\":1} \n"), &parent, links); err != nil {
		t.Fatal(err)
	}
	parent, links[0] = q, driver.SourceLink{Kind: "association", To: p}
	rows, more, err := tx.HierarchyChildren(ctx, []model.Ref{p}, 2)
	requireSources(t, rows, more, err, true, c.ID, d.ID)
	rows, more, err = tx.HierarchyChildrenExcluding(ctx, []model.Ref{p}, []model.Ref{d}, 2)
	requireSources(t, rows, more, err, false, c.ID, e.ID)
	if rows[0].Revision.String() != "1" || !rows[1].Revision.IsZero() {
		t.Fatal("staging changed optimistic revision tokens")
	}
	for _, attempt := range []func() error{
		func() error { return tx.Put(ctx, sourceRecord(e.ID, `{"bad":1}`)) },
		func() error { return tx.PutHierarchyRecord(ctx, sourceRecord(e.ID, `{"bad":2}`), &q) },
		func() error { return tx.PutGraphRecord(ctx, sourceRecord(e.ID, `broken`), &q, nil) },
		func() error {
			return tx.PutGraphRecord(ctx, sourceRecord(e.ID, `{}`), &q, []driver.SourceLink{{Kind: "", To: p}})
		},
	} {
		if err := attempt(); err == nil {
			t.Fatal("invalid/incomplete replacement accepted")
		}
	}
	rows, more, err = tx.InverseSourceLinks(ctx, []model.Ref{q}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks, driver.IncomingLinkParentChild}, []model.Ref{c, d}, 1)
	requireSources(t, rows, more, err, false, e.ID)
	if string(rows[0].Body) != " {\"new\":1} \n" || !reflect.DeepEqual(bound, tx.Snapshot()) {
		t.Fatal("failed staging changed image or snapshot")
	}
	rows, more, err = tx.HierarchyChildrenExcluding(ctx, []model.Ref{p}, []model.Ref{c, d, e}, 0)
	requireSources(t, rows, more, err, false)
	commitSourceTest(t, tx, h)
	rows, more, err = sourceReader(t, s, nil).HierarchyChildren(ctx, []model.Ref{p}, 10)
	requireSources(t, rows, more, err, false, c.ID, d.ID, e.ID)
}

func TestGraphSourceCoverageStaysIndependentAndPublicationRollsBack(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"legacy", "parent-only", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			s := openStore(t, ctx, filepath.Join(t.TempDir(), "coverage.db"))
			lease := getLease(t, ctx, s, driver.LeaseTTL)
			p, a := testRef("pl-p"), testRef("pl-a")
			if rows, more, err := sourceReader(t, s, nil).HierarchyChildren(ctx, nil, 0); rows != nil || more || !errors.Is(err, driver.ErrUnsupported) {
				t.Fatal("unbuilt probe inferred completeness")
			}
			tx, h := beginSourceTest(t, s, lease, a)
			var err error
			switch mode {
			case "legacy":
				err = tx.Put(ctx, sourceRecord(a.ID, `{}`))
			case "parent-only":
				err = tx.PutHierarchyRecord(ctx, sourceRecord(a.ID, `{}`), &p)
			default:
				err = tx.PutGraphRecord(ctx, sourceRecord(a.ID, `{}`), &p, nil)
				if err == nil {
					_, err = s.db.ExecContext(ctx, `CREATE TRIGGER reject_graph_marker BEFORE INSERT ON graph_link_index_coverage BEGIN SELECT RAISE(ABORT,'injected index failure'); END`)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if mode == "rollback" {
				h.Changes = []model.ChangeRecord{{Ref: a, Op: model.OpCreate}}
				if _, err = tx.Commit(ctx, h); err == nil {
					t.Fatal("injected publication failure did not abort")
				}
				_ = tx.Rollback(ctx)
				if !getMeta(t, ctx, s).Head.IsZero() {
					t.Fatal("failed publication advanced Head")
				}
				var count int
				if err = s.db.QueryRowContext(ctx, `SELECT (SELECT count(*) FROM records)+(SELECT count(*) FROM hierarchy_source_current)+(SELECT count(*) FROM hierarchy_index_coverage)`).Scan(&count); err != nil || count != 0 {
					t.Fatalf("partial record/index publication count=%d err=%v", count, err)
				}
			} else {
				commitSourceTest(t, tx, h)
			}
			coverage, err := sourceindex.SQLite.ReadCoverage(ctx, s.db, h.Incarnation)
			if err != nil || coverage.Links.Ready || coverage.Parent.Ready != (mode == "parent-only") {
				t.Fatalf("coverage=%+v err=%v", coverage, err)
			}
		})
	}
}

func TestGraphSourceOpaqueKeysAndBoundedIndexedImages(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "bounded.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	p, q := testRef("pl-p"), testRef("pl-q")
	refs := []model.Ref{testRef("pl-\xff"), testRef("pl-\xfe"), testRef("pl-\ufffd")}
	tx, h := beginSourceTest(t, s, lease, refs...)
	for _, ref := range refs {
		if err := tx.PutGraphRecord(ctx, sourceRecord(ref.ID, " {\"raw\":true} \n"), &p, []driver.SourceLink{{Kind: "blocks", To: q}, {Kind: "waits-for", To: q}}); err != nil {
			t.Fatal(err)
		}
	}
	commitSourceTest(t, tx, h)
	sort.Slice(refs, func(i, j int) bool { a, _ := recordKey(refs[i]); b, _ := recordKey(refs[j]); return a < b })
	r := sourceReader(t, s, nil)
	work := &sourceindex.Work{}
	rows, more, err := r.selectIndexedSources(ctx, sourceindex.Request{Refs: []model.Ref{q}, Kinds: []driver.IncomingLinkKind{driver.IncomingLinkBlocks, driver.IncomingLinkWaitsFor}, Excluded: []model.Ref{refs[0]}, Inverse: true, Limit: 1}, work)
	requireSources(t, rows, more, err, true, refs[1].ID)
	if work.Images != 2 || work.Headers != 6 {
		t.Fatalf("dedup/exclusion/global image bound=%+v", work)
	}
	for _, ref := range refs {
		var raw []byte
		id, _ := recordKey(ref)
		if err = s.db.QueryRowContext(ctx, `SELECT ref FROM hierarchy_source_current WHERE child_key=?`, id).Scan(&raw); err != nil || string(raw) != ref.ID {
			t.Fatalf("opaque source ref=%x want=%x err=%v", raw, ref.ID, err)
		}
	}
	var plan []string
	query, err := s.db.QueryContext(ctx, `EXPLAIN QUERY PLAN SELECT child_key FROM hierarchy_source_current INDEXED BY hierarchy_source_current_parent WHERE incarnation=? AND branch=? AND parent_key=? ORDER BY child_key LIMIT 2`, h.Incarnation, revKey(0), keyID(testKey(p.ID)))
	if err != nil {
		t.Fatal(err)
	}
	for query.Next() {
		var id, parent, unused int
		var detail string
		if err = query.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	err = query.Err()
	_ = query.Close()
	if err != nil || !strings.Contains(strings.Join(plan, " "), "SEARCH hierarchy_source_current USING COVERING INDEX hierarchy_source_current_parent") || strings.Contains(strings.Join(plan, " "), "TEMP B-TREE") {
		t.Fatalf("unbounded parent selection plan=%v err=%v", plan, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if rows, more, err = r.HierarchyChildren(cancelled, nil, 0); rows != nil || more || !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled probe admitted")
	}
	_ = r.Close()
	if rows, more, err = r.HierarchyChildren(ctx, nil, 0); rows != nil || more || !errors.Is(err, driver.ErrTxClosed) {
		t.Fatal("closed probe admitted")
	}
	t.Logf("opaque source keys distinct; selected images=%d headers=%d; actual plan=%v", work.Images, work.Headers, plan)
}

func TestGraphSourceNativeRestoreFloorAndDescendingTwentyThousand(t *testing.T) {
	ctx := context.Background()
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "native.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	renewCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				done <- nil
				return
			case <-ticker.C:
				if err := lease.Renew(renewCtx); err != nil {
					if errors.Is(err, context.Canceled) && renewCtx.Err() != nil {
						done <- nil
						return
					}
					done <- err
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	refs := make([]model.Ref, 20000)
	for i := range refs {
		refs[i] = testRef(fmt.Sprintf("pl-native-%05d", i))
	}
	started := time.Now()
	profile := driver.NativeRestoreProfile{StoreID: "native-sqlite-source", Principal: model.PrincipalRef{Kind: "user", ID: "operator"}, Method: "admin.restore.finish", IdempotencyKey: "restore-native-source", RequestHash: strings.Repeat("a", 64)}
	tx, h, meta, err := beginNativeSourceTest(t, s, lease, profile, refs...)
	if err != nil {
		t.Fatal(err)
	}
	p, q := testRef("pl-parent"), testRef("pl-other")
	sentinels := []driver.Sentinel{driver.SentinelGraphBlocking}
	for i := len(refs) - 1; i >= 0; i-- {
		parent := q
		if i < 4 {
			parent = p
		}
		if err = tx.PutGraphRecord(ctx, sourceRecord(refs[i].ID, " {\"native\":true} \n"), &parent, []driver.SourceLink{{Kind: "blocks", To: parent}, {Kind: "waits-for", To: parent}}); err != nil {
			t.Fatal(err)
		}
		bead, _ := driver.NewBeadSentinel(refs[i].ID)
		sentinels = append(sentinels, bead)
	}
	if err = tx.BumpSentinels(ctx, sentinels); err != nil {
		t.Fatal(err)
	}
	h.Incarnation, h.Kind, h.Method, h.Principal, h.IdempotencyKey, h.RequestHash = meta.Incarnation, model.ChangesetRestore, profile.Method, profile.Principal, profile.IdempotencyKey, profile.RequestHash
	commitSourceTest(t, tx, h)
	elapsed := time.Since(started)
	coverage, err := sourceindex.SQLite.ReadCoverage(ctx, s.db, meta.Incarnation)
	if err != nil || coverage.Parent.First != 1 || coverage.Links.First != 1 {
		t.Fatalf("native coverage=%+v err=%v", coverage, err)
	}
	zero := model.Natural64(0)
	if r, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &zero}); err == nil {
		_ = r.Close()
		t.Fatal("native restore invented root0")
	}
	readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
	defer readCancel()
	r := sourceReader(t, s, nil)
	work := &sourceindex.Work{}
	rows, more, err := r.selectIndexedSources(readCtx, sourceindex.Request{Refs: []model.Ref{p}, Kinds: []driver.IncomingLinkKind{driver.IncomingLinkBlocks, driver.IncomingLinkWaitsFor}, Inverse: true, Excluded: []model.Ref{refs[0]}, Limit: 1}, work)
	requireSources(t, rows, more, err, true, refs[1].ID)
	if work.Images != 2 || work.Headers != 8 {
		t.Fatalf("large native read work=%+v", work)
	}
	t.Logf("actual descending20000 native source publication=%s; images=%d headers=%d", elapsed, work.Images, work.Headers)
}
