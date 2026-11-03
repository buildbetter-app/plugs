package postgres

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
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
	keys := append([]driver.LockKey{driver.GraphBlockingSentinelLock}, extra...)
	for _, ref := range refs {
		keys = append(keys, testLock(ref))
		bead, err := driver.NewBeadSentinel(ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead})
	}
	tx, h := beginTest(t, s, lease, keys...)
	sentinels := make([]driver.Sentinel, 0, len(refs)+1)
	for _, key := range h.LockKeys {
		if key.Kind == driver.LockSentinel {
			sentinels = append(sentinels, key.Sentinel)
		}
	}
	if bump {
		if err := tx.BumpSentinels(context.Background(), sentinels); err != nil {
			t.Fatal(err)
		}
	}
	return tx.(*transaction), h
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
	ids := make([]string, 0, len(tx.records))
	for id := range tx.records {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		previous, exists, err := tx.recordAt(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		change := model.ChangeRecord{Ref: tx.identities[id].Ref, Op: model.OpCreate}
		if exists {
			change.FromRevision = previous.record.Revision
			if !previous.deleted {
				change.Op = model.OpPatch
			}
		}
		if tx.records[id] == nil {
			change.Op = model.OpDelete
		}
		h.Changes = append(h.Changes, change)
	}
	entry, err := tx.Commit(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func sourceReader(t *testing.T, s *Store, at *model.Revision) *reader {
	t.Helper()
	return testRead(t, s, at).(*reader)
}

func requireSources(t *testing.T, records []driver.Record, more bool, err error, wantMore bool, ids ...string) {
	t.Helper()
	got := make([]string, 0, len(records))
	for _, record := range records {
		got = append(got, record.Ref.ID)
	}
	if err != nil || more != wantMore || !reflect.DeepEqual(got, ids) && !(len(got) == 0 && len(ids) == 0) {
		t.Fatalf("sources=%q more=%v err=%v; want=%q more=%v", got, more, err, ids, wantMore)
	}
}

func TestGraphSourcePublicationHistoryReopenAndExactBytes(t *testing.T) {
	ctx := context.Background()
	dsn := freshDSN(t)
	s := openTest(t, dsn)
	lease := testLease(t, s)
	p, q, a, z := testRef("pl-p"), testRef("pl-q"), testRef("pl-a"), testRef("pl-z")
	tx, h := beginSourceTest(t, s, lease, p, q, a, z)
	first := []byte(" { \"opaque\": [1, 2], \"parent\": \"not interpreted by storage\" } \n")
	wantFirst := bytes.Clone(first)
	if err := tx.PutGraphRecord(ctx, driver.Record{Ref: a, Kind: "bead", Body: first}, &p, []driver.SourceLink{{Kind: "blocks", To: q}, {Kind: "waits-for", To: q}, {Kind: "custom\x00Kind ", To: p}}); err != nil {
		t.Fatal(err)
	}
	first[0] = '\t'
	for _, ref := range []model.Ref{p, q, z} {
		var parent *model.Ref
		var links []driver.SourceLink
		if ref == z {
			parent = &p
			links = []driver.SourceLink{{Kind: "parent-child", To: q}, {Kind: "blocks", To: q}, {Kind: "blocks", To: q}}
		}
		if err := tx.PutGraphRecord(ctx, sourceRecord(ref.ID, `{}`), parent, links); err != nil {
			t.Fatal(err)
		}
	}
	entry := commitSourceTest(t, tx, h)
	if entry.Revision.String() != "1" {
		t.Fatal(entry.Revision)
	}
	var custom []byte
	if err := s.db.QueryRowContext(ctx, `SELECT link_kind FROM plug_postgres.graph_link_current WHERE source_key=$1 AND target_key=$2`, []byte(sourceKey(a)), []byte(sourceKey(p))).Scan(&custom); err != nil || string(custom) != "custom\x00Kind " {
		t.Fatalf("complete opaque outgoing kind: %q %v", custom, err)
	}
	pinned := sourceReader(t, s, nil)
	records, more, err := pinned.HierarchyChildren(ctx, []model.Ref{p, p}, 2)
	requireSources(t, records, more, err, false, a.ID, z.ID)
	if !bytes.Equal(records[0].Body, wantFirst) || records[0].Revision.String() != "1" {
		t.Fatal("source bytes/revision changed")
	}
	records[0].Body[0] = '!'
	records, more, err = pinned.InverseSourceLinks(ctx, []model.Ref{q, q}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks, driver.IncomingLinkWaitsFor, driver.IncomingLinkParentChild}, nil, 1)
	requireSources(t, records, more, err, true, a.ID)
	if !bytes.Equal(records[0].Body, wantFirst) {
		t.Fatal("returned image did not own its bytes")
	}
	zero := model.Natural64(0)
	empty := sourceReader(t, s, &zero)
	records, more, err = empty.HierarchyChildren(ctx, []model.Ref{p}, 0)
	requireSources(t, records, more, err, false)
	tx, h = beginSourceTest(t, s, lease, a)
	second := "\n{\"opaque\":2 }\t"
	if err = tx.PutGraphRecord(ctx, sourceRecord(a.ID, second), &q, nil); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, h)
	current := sourceReader(t, s, nil)
	records, more, err = current.HierarchyChildren(ctx, []model.Ref{p, q}, 10)
	requireSources(t, records, more, err, false, a.ID, z.ID)
	if string(records[0].Body) != second || records[0].Revision.String() != "2" {
		t.Fatal("replacement source image was not published")
	}
	records, more, err = current.InverseSourceLinks(ctx, []model.Ref{q}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks}, nil, 10)
	requireSources(t, records, more, err, false, z.ID)
	records, more, err = pinned.HierarchyChildren(ctx, []model.Ref{p}, 10)
	requireSources(t, records, more, err, false, a.ID, z.ID)
	one := model.Natural64(1)
	historical := sourceReader(t, s, &one)
	records, more, err = historical.InverseSourceLinks(ctx, []model.Ref{q}, []driver.IncomingLinkKind{driver.IncomingLinkWaitsFor}, nil, 10)
	requireSources(t, records, more, err, false, a.ID)
	if !bytes.Equal(records[0].Body, wantFirst) {
		t.Fatal("historical source changed exact bytes")
	}
	tx, h = beginSourceTest(t, s, lease)
	commitSourceTest(t, tx, h)
	coverage, err := sourceindex.Postgres.ReadCoverage(ctx, s.db, h.Incarnation)
	if err != nil || coverage.Parent.Through != 3 || coverage.Links.Through != 3 {
		t.Fatalf("source-free commit left coverage gap: %+v %v", coverage, err)
	}
	if err = pinned.Close(); err != nil {
		t.Fatal(err)
	}
	if err = lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err = s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openTest(t, dsn)
	records, more, err = sourceReader(t, s, &one).HierarchyChildren(ctx, []model.Ref{p}, 10)
	requireSources(t, records, more, err, false, a.ID, z.ID)
	if !bytes.Equal(records[0].Body, wantFirst) {
		t.Fatal("reopen changed exact historical source")
	}
}

func sourceKey(ref model.Ref) string { id, _ := recordKey(ref); return id }

func TestGraphSourcesKeepOpaqueIdentityAfterTypedLockCopy(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	p, q := testRef("pl-p"), testRef("pl-q")
	refs := []model.Ref{testRef("pl-\xff"), testRef("pl-\xfe"), testRef("pl-\ufffd")}
	tx, h := beginSourceTest(t, s, lease, refs...)
	for _, ref := range refs {
		if err := tx.PutGraphRecord(ctx, sourceRecord(ref.ID, " {\"opaque\": [ 1, 2 ]} \n"), &p, []driver.SourceLink{{Kind: "blocks", To: q}, {Kind: "custom\x00 ", To: p}}); err != nil {
			t.Fatal(err)
		}
	}
	commitSourceTest(t, tx, h)
	sort.Slice(refs, func(i, j int) bool { return sourceKey(refs[i]) < sourceKey(refs[j]) })
	pinned := sourceReader(t, s, nil)
	rows, more, err := pinned.InverseSourceLinks(ctx, []model.Ref{q}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks}, nil, 10)
	requireSources(t, rows, more, err, false, refs[0].ID, refs[1].ID, refs[2].ID)
	for _, ref := range refs {
		var id, raw []byte
		if err = s.db.QueryRowContext(ctx, `SELECT source_key,ref FROM plug_postgres.graph_link_source_current WHERE source_key=$1`, []byte(sourceKey(ref))).Scan(&id, &raw); err != nil || string(id) != sourceKey(ref) || string(raw) != ref.ID {
			t.Fatalf("opaque SQL binding key=%x ref=%x want=%x err=%v", id, raw, ref.ID, err)
		}
	}
	tx, h = beginSourceTest(t, s, lease, refs[1])
	if err = tx.PutGraphRecord(ctx, sourceRecord(refs[1].ID, `{"new":true}`), nil, nil); err != nil {
		t.Fatal(err)
	}
	staged, more, err := tx.InverseSourceLinks(ctx, []model.Ref{q}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks}, nil, 10)
	requireSources(t, staged, more, err, false, refs[0].ID, refs[2].ID)
	commitSourceTest(t, tx, h)
	one := model.Natural64(1)
	rows, more, err = sourceReader(t, s, &one).HierarchyChildrenExcluding(ctx, []model.Ref{p}, []model.Ref{refs[1]}, 10)
	requireSources(t, rows, more, err, false, refs[0].ID, refs[2].ID)
	if string(rows[0].Body) != " {\"opaque\": [ 1, 2 ]} \n" {
		t.Fatal("opaque historical source Body changed")
	}
	t.Log("typed opaque source Lock/Put/index/history/exclusion identities remained distinct")
}

func TestGraphSourceDriverOverlayExclusionsAndFailureAtomicity(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	p, q := testRef("pl-p"), testRef("pl-q")
	refs := []model.Ref{testRef("pl-a"), testRef("pl-b"), testRef("pl-c"), testRef("pl-d")}
	tx, h := beginSourceTest(t, s, lease, refs...)
	for _, ref := range refs {
		if err := tx.PutGraphRecord(ctx, sourceRecord(ref.ID, `{}`), &p, []driver.SourceLink{{Kind: "blocks", To: q}}); err != nil {
			t.Fatal(err)
		}
	}
	commitSourceTest(t, tx, h)
	e := testRef("pl-e")
	tx, h = beginSourceTest(t, s, lease, append(refs, e)...)
	bound := tx.Snapshot()
	if err := tx.PutGraphRecord(ctx, sourceRecord("pl-a", `{"moved":true}`), &q, nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.Delete(ctx, testRef("pl-b")); err != nil {
		t.Fatal(err)
	}
	parent := p
	links := []driver.SourceLink{{Kind: "blocks", To: q}, {Kind: "parent-child", To: q}}
	if err := tx.PutGraphRecord(ctx, sourceRecord(e.ID, " {\"created\":true} \n"), &parent, links); err != nil {
		t.Fatal(err)
	}
	parent, links[0] = q, driver.SourceLink{Kind: "association", To: p}
	records, more, err := tx.HierarchyChildren(ctx, []model.Ref{p}, 2)
	requireSources(t, records, more, err, true, "pl-c", "pl-d")
	records, more, err = tx.HierarchyChildrenExcluding(ctx, []model.Ref{p}, []model.Ref{testRef("pl-d")}, 2)
	requireSources(t, records, more, err, false, "pl-c", "pl-e")
	if records[0].Revision.String() != "1" || !records[1].Revision.IsZero() || string(records[1].Body) != " {\"created\":true} \n" {
		t.Fatal("staged image lost bytes/committed revision token")
	}
	records, more, err = tx.InverseSourceLinks(ctx, []model.Ref{q}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks, driver.IncomingLinkParentChild}, []model.Ref{testRef("pl-c"), testRef("pl-d")}, 1)
	requireSources(t, records, more, err, false, "pl-e")
	records, more, err = tx.HierarchyChildrenExcluding(ctx, []model.Ref{p}, []model.Ref{testRef("pl-c"), testRef("pl-d"), e}, 0)
	requireSources(t, records, more, err, false)
	for _, attempt := range []func() error{
		func() error { return tx.Put(ctx, sourceRecord(e.ID, `{"bad":"ordinary"}`)) },
		func() error { return tx.PutHierarchyRecord(ctx, sourceRecord(e.ID, `{"bad":"parent-only"}`), &q) },
		func() error {
			return tx.PutGraphRecord(ctx, sourceRecord(e.ID, `{"bad":"link"}`), &q, []driver.SourceLink{{Kind: "", To: p}})
		},
		func() error { return tx.PutGraphRecord(ctx, sourceRecord(e.ID, `broken`), &q, nil) },
	} {
		if err := attempt(); err == nil {
			t.Fatal("invalid/undeclared replacement accepted")
		}
	}
	records, more, err = tx.InverseSourceLinks(ctx, []model.Ref{q}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks}, []model.Ref{testRef("pl-c"), testRef("pl-d")}, 1)
	requireSources(t, records, more, err, false, "pl-e")
	if string(records[0].Body) != " {\"created\":true} \n" || !reflect.DeepEqual(bound, tx.Snapshot()) {
		t.Fatal("staging failure mutated prior image/declarations/snapshot")
	}
	commitSourceTest(t, tx, h)
	tx, h = beginSourceTest(t, s, lease, testRef("pl-b"))
	if err = tx.PutGraphRecord(ctx, sourceRecord("pl-b", `{"recreated":true}`), &p, nil); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, h)
	at := model.Natural64(2)
	records, more, err = sourceReader(t, s, &at).HierarchyChildren(ctx, []model.Ref{p}, 10)
	requireSources(t, records, more, err, false, "pl-c", "pl-d", "pl-e")
}

func TestGraphSourceCoverageRefusalAndAtomicPublication(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"legacy", "parent-only", "untyped-replacement"} {
		t.Run(mode, func(t *testing.T) {
			s := openTest(t, freshDSN(t))
			lease := testLease(t, s)
			p, a := testRef("pl-p"), testRef("pl-a")
			if rows, more, err := sourceReader(t, s, nil).HierarchyChildren(ctx, nil, 0); rows != nil || more || !errors.Is(err, driver.ErrUnsupported) {
				t.Fatal("unbuilt empty request did not refuse")
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
					err = tx.Put(ctx, sourceRecord(a.ID, `{"untyped":true}`))
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			commitSourceTest(t, tx, h)
			coverage, err := sourceindex.Postgres.ReadCoverage(ctx, s.db, h.Incarnation)
			if err != nil || coverage.Links.Ready || coverage.Parent.Ready != (mode == "parent-only") {
				t.Fatalf("independent coverage: %+v %v", coverage, err)
			}
			tx, h = beginSourceTest(t, s, lease, a)
			if err = tx.PutGraphRecord(ctx, sourceRecord(a.ID, `{"typed":true}`), &p, nil); err != nil {
				t.Fatal(err)
			}
			commitSourceTest(t, tx, h)
			coverage, err = sourceindex.Postgres.ReadCoverage(ctx, s.db, h.Incarnation)
			if err != nil || coverage.Links.Ready || coverage.Parent.Ready != (mode == "parent-only") {
				t.Fatalf("populated store newly certified: %+v %v", coverage, err)
			}
		})
	}
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	p, a := testRef("pl-p"), testRef("pl-a")
	tx, h := beginSourceTest(t, s, lease, a)
	if err := tx.PutGraphRecord(ctx, sourceRecord(a.ID, `{}`), &p, nil); err != nil {
		t.Fatal(err)
	}
	// A real failure after record SQL but before the marker must roll back both.
	if _, err := s.db.ExecContext(ctx, `CREATE FUNCTION plug_postgres.reject_source() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'source publication test'; END $$; CREATE TRIGGER reject_source BEFORE INSERT ON plug_postgres.graph_link_source_current FOR EACH ROW EXECUTE FUNCTION plug_postgres.reject_source()`); err != nil {
		t.Fatal(err)
	}
	h.Changes = []model.ChangeRecord{{Ref: a, Op: model.OpCreate}}
	if _, err := tx.Commit(ctx, h); err == nil {
		t.Fatal("injected publication failure accepted")
	}
	meta, err := s.Meta(ctx)
	if err != nil || !meta.Head.IsZero() {
		t.Fatalf("partial Head: %+v %v", meta, err)
	}
	for _, table := range []string{"records", "versions", "hierarchy_source_current", "hierarchy_source_history", "graph_link_source_current", "hierarchy_index_coverage", "graph_link_index_coverage"} {
		var count int
		if err = s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM plug_postgres."+table).Scan(&count); err != nil || count != 0 {
			t.Fatalf("partial %s publication: %d %v", table, count, err)
		}
	}
	if _, err = s.db.ExecContext(ctx, `DROP TRIGGER reject_source ON plug_postgres.graph_link_source_current; DROP FUNCTION plug_postgres.reject_source()`); err != nil {
		t.Fatal(err)
	}
	tx, h = beginSourceTest(t, s, lease, a)
	if err = tx.PutGraphRecord(ctx, sourceRecord(a.ID, `{}`), &p, nil); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, h)
	if _, err = s.db.ExecContext(ctx, `UPDATE plug_postgres.meta SET head=2`); err != nil {
		t.Fatal(err)
	}
	if rows, more, err := sourceReader(t, s, nil).InverseSourceLinks(ctx, nil, nil, nil, 0); rows != nil || more || !errors.Is(err, driver.ErrUnsupported) {
		t.Fatal("coverage gap became absence")
	}
	tx, h = beginSourceTest(t, s, lease, a)
	if err = tx.PutGraphRecord(ctx, sourceRecord(a.ID, `{"gap":true}`), &p, nil); !errors.Is(err, driver.ErrUnsupported) || len(tx.records) != 0 || len(tx.sourceOverlay.Declarations) != 0 {
		t.Fatalf("stale coverage changed staging: %v", err)
	}
	if _, err = tx.Commit(ctx, h); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("coverage gap bridged: %v", err)
	}
}

func TestGraphSourceNativeRestoreBindsIncarnationAndFloor(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	p, a := testRef("pl-p"), testRef("pl-a")
	profile := driver.NativeRestoreProfile{StoreID: "native-source", Principal: model.PrincipalRef{Kind: "user", ID: "operator"}, Method: "admin.restore.finish", IdempotencyKey: "restore-source", RequestHash: strings.Repeat("a", 64)}
	tx, h, meta, err := beginNativeSourceTest(t, s, lease, profile, a)
	old := h.Incarnation
	if err != nil {
		t.Fatal(err)
	}
	if err = tx.PutGraphRecord(ctx, sourceRecord(a.ID, " {\"native\": true} \n"), &p, nil); err != nil {
		t.Fatal(err)
	}
	bead, _ := driver.NewBeadSentinel(a.ID)
	if err = tx.BumpSentinels(ctx, []driver.Sentinel{bead, driver.SentinelGraphBlocking}); err != nil {
		t.Fatal(err)
	}
	h.Incarnation, h.Kind, h.Method, h.Principal, h.IdempotencyKey, h.RequestHash = meta.Incarnation, model.ChangesetRestore, profile.Method, profile.Principal, profile.IdempotencyKey, profile.RequestHash
	commitSourceTest(t, tx, h)
	coverage, err := sourceindex.Postgres.ReadCoverage(ctx, s.db, meta.Incarnation)
	if err != nil || meta.Incarnation == old || coverage.Parent.First != 1 || coverage.Links.First != 1 {
		t.Fatalf("invented restore history: %+v %v", coverage, err)
	}
	if _, err = s.Read(ctx, driver.View{Incarnation: old}); err == nil {
		t.Fatal("old incarnation rebound")
	}
	zero := model.Natural64(0)
	if _, err = s.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &zero}); err == nil {
		t.Fatal("invented restore root zero")
	}
	rows, more, err := sourceReader(t, s, nil).HierarchyChildren(ctx, []model.Ref{p}, 1)
	requireSources(t, rows, more, err, false, a.ID)
	if string(rows[0].Body) != " {\"native\": true} \n" {
		t.Fatal("restore image changed")
	}
}

// Interleave a real independent publication between key-only selection and
// Body fetch in READ COMMITTED. The exact historical PK must remain bound.
type sourceInterleaveSQL struct {
	*sql.Tx
	onImages func()
}

func (q *sourceInterleaveSQL) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	if strings.HasPrefix(query, "WITH selected(") && q.onImages != nil {
		fn := q.onImages
		q.onImages = nil
		fn()
	}
	return q.Tx.QueryContext(ctx, query, args...)
}

func TestSourceReadCommittedKeepsExactIntervalAcrossPublication(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	p, a := testRef("pl-p"), testRef("pl-a")
	tx, h := beginSourceTest(t, s, lease, a)
	if err := tx.PutGraphRecord(ctx, sourceRecord(a.ID, " {\"old\":true} \n"), &p, []driver.SourceLink{{Kind: "blocks", To: p}}); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, h)
	value, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	writer := value.(*transaction)
	defer writer.Close()
	if err = writer.check(ctx); err != nil {
		t.Fatal(err)
	}
	bound := writer.Snapshot()
	q := &sourceInterleaveSQL{Tx: writer.sql, onImages: func() {
		tx, h := beginSourceTest(t, s, lease, a)
		if err := tx.PutGraphRecord(ctx, sourceRecord(a.ID, `{"new":true}`), nil, nil); err != nil {
			t.Fatal(err)
		}
		commitSourceTest(t, tx, h)
	}}
	work := &sourceindex.Work{}
	rows, more, err := sourceindex.Postgres.Select(ctx, q, writer.meta, writer.at, sourceindex.Request{Refs: []model.Ref{p}, Kinds: []driver.IncomingLinkKind{driver.IncomingLinkBlocks}, Limit: 1, Inverse: true, Historical: true}, nil, nil, work)
	requireSources(t, rows, more, err, false, a.ID)
	if string(rows[0].Body) != " {\"old\":true} \n" || rows[0].Revision.String() != "1" || !reflect.DeepEqual(bound, writer.Snapshot()) {
		t.Fatal("READ COMMITTED rebound exact source image or snapshot")
	}
	if work.Images != 1 {
		t.Fatalf("images=%d", work.Images)
	}
}

func TestSourceHistoricalBudgetIsSharedAcrossBucketsAndPrefixes(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p, q, a, b := testRef("pl-p"), testRef("pl-q"), testRef("pl-a"), testRef("pl-b")
	// A disposable corpus of expired intervals: no current membership, but a
	// historical ordered proof cannot skip unbounded old keys/intervals for free.
	for i, parent := range []model.Ref{p, q} {
		ref := []model.Ref{a, b}[i]
		if _, err = s.db.ExecContext(ctx, `INSERT INTO plug_postgres.hierarchy_source_history(incarnation,branch,child_key,parent_key,start_revision,end_revision,ref,kind,revision,body) SELECT $1,0,$2,$3,n,n+1,$4,'bead',n,$5 FROM generate_series(1,40000) n`, meta.Incarnation, []byte(sourceKey(ref)), []byte(sourceKey(parent)), []byte(ref.ID), []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = s.db.ExecContext(ctx, `UPDATE plug_postgres.meta SET head=80001; INSERT INTO plug_postgres.hierarchy_index_coverage SELECT incarnation,0,1,0,80001,'ready' FROM plug_postgres.meta`); err != nil {
		t.Fatal(err)
	}
	meta, err = s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r := sourceReader(t, s, nil)
	work := &sourceindex.Work{}
	rows, more, err := sourceindex.Postgres.Select(ctx, r.sql, meta, 80001, sourceindex.Request{Refs: []model.Ref{p, q}, Historical: true, Limit: 0}, nil, nil, work)
	if !errors.Is(err, driver.ErrUnsupported) || rows != nil || more || work.Headers > sourceindex.MaxHeaders || work.Headers < 40000 || work.Images != 0 {
		t.Fatalf("unbounded/partial history: rows=%d more=%v err=%v work=%+v", len(rows), more, err, work)
	}
	t.Logf("one request across two buckets/prefixes refused with headers=%d images=%d", work.Headers, work.Images)
}

func TestSourceBoundedIndexedSelectionBeforeImages(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	p, q := testRef("pl-p"), testRef("pl-q")
	refs := make([]model.Ref, 204)
	for i := range refs {
		refs[i] = testRef(fmt.Sprintf("pl-%05d", i))
	}
	tx, h := beginSourceTest(t, s, lease, refs...)
	for i, ref := range refs {
		parent := q
		if i < 4 {
			parent = p
		}
		if err := tx.PutGraphRecord(ctx, sourceRecord(ref.ID, `{}`), &parent, []driver.SourceLink{{Kind: "blocks", To: parent}, {Kind: "waits-for", To: parent}}); err != nil {
			t.Fatal(err)
		}
	}
	commitSourceTest(t, tx, h)
	r := sourceReader(t, s, nil)
	work := &sourceindex.Work{}
	rows, more, err := r.selectIndexedSources(ctx, sourceindex.Request{Refs: []model.Ref{p, p}, Kinds: []driver.IncomingLinkKind{driver.IncomingLinkBlocks, driver.IncomingLinkWaitsFor}, Excluded: []model.Ref{refs[0]}, Inverse: true, Limit: 1}, work)
	requireSources(t, rows, more, err, true, refs[1].ID)
	if work.Images != 2 || work.Headers != 8 {
		t.Fatalf("images before unique membership/global bound: %+v", work)
	}
	if _, err = s.db.ExecContext(ctx, `UPDATE plug_postgres.hierarchy_source_current SET body=$1 WHERE child_key=$2`, []byte(`broken`), []byte(sourceKey(refs[0]))); err != nil {
		t.Fatal(err)
	}
	if rows, more, err = sourceReader(t, s, nil).HierarchyChildren(ctx, []model.Ref{p}, 0); rows != nil || more || !errors.Is(err, driver.ErrUnsupported) {
		t.Fatal("malformed probe leaked a page")
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if rows, more, err = r.InverseSourceLinks(cancelled, nil, nil, nil, 0); rows != nil || more || !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled empty lookup admitted")
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
	if rows, more, err = r.HierarchyChildren(ctx, nil, 0); rows != nil || more || !errors.Is(err, driver.ErrTxClosed) {
		t.Fatal("closed lookup admitted")
	}
	// The actual planned production prefix is an ordered index access. A large
	// unrelated corpus must not be read or sorted to answer this parent bucket.
	if _, err = s.db.ExecContext(ctx, `INSERT INTO plug_postgres.hierarchy_source_current SELECT $1,0,convert_to('record/id/unrelated/'||n,'UTF8'),$2,convert_to('unrelated/'||n,'UTF8'),'bead',1,$3 FROM generate_series(1,20000) n`, h.Incarnation, []byte(sourceKey(q)), []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.db.ExecContext(ctx, `ANALYZE plug_postgres.hierarchy_source_current`); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err = s.db.QueryRowContext(ctx, `EXPLAIN (ANALYZE,BUFFERS,FORMAT JSON) SELECT child_key FROM plug_postgres.hierarchy_source_current WHERE incarnation=$1 AND branch=0 AND parent_key=$2 ORDER BY child_key LIMIT 2`, h.Incarnation, []byte(sourceKey(p))).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var plan []struct{ Plan map[string]any }
	if err = json.Unmarshal(raw, &plan); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "hierarchy_source_current_parent") || strings.Contains(string(raw), "Seq Scan") || strings.Contains(string(raw), `"Node Type": "Sort"`) {
		t.Fatalf("unbounded indexed plan: %s", raw)
	}
	t.Logf("actual parent prefix plan: %s; selector work headers=%d images=%d", raw, work.Headers, work.Images)
}

// Keep production TTL unchanged while exercising a real large native driver
// publication. The elapsed write cost is evidence, not a two-second claim.
func TestGraphSourceNativeRestoreTwentyThousandAndBoundedRead(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	renewCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	renewDone := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				renewDone <- nil
				return
			case <-ticker.C:
				if err := lease.Renew(renewCtx); err != nil {
					renewDone <- err
					return
				}
			}
		}
	}()
	defer func() {
		cancel()
		if err := <-renewDone; err != nil {
			t.Error(err)
		}
	}()
	refs := make([]model.Ref, 20000)
	for i := range refs {
		refs[i] = testRef(fmt.Sprintf("pl-restore-%05d", i))
	}
	started := time.Now()
	profile := driver.NativeRestoreProfile{StoreID: "native-twenty-thousand", Principal: model.PrincipalRef{Kind: "user", ID: "operator"}, Method: "admin.restore.finish", IdempotencyKey: "restore-large-source", RequestHash: strings.Repeat("b", 64)}
	tx, h, meta, err := beginNativeSourceTest(t, s, lease, profile, refs...)
	if err != nil {
		t.Fatal(err)
	}
	p, q := testRef("pl-parent"), testRef("pl-unrelated-parent")
	sentinels := []driver.Sentinel{driver.SentinelGraphBlocking}
	// Descending staging exercises arbitrary publication order rather than the
	// append-only shape of an already sorted child bucket.
	for i := len(refs) - 1; i >= 0; i-- {
		ref := refs[i]
		parent := q
		if i < 4 {
			parent = p
		}
		if err = tx.PutGraphRecord(ctx, sourceRecord(ref.ID, " {\"raw\": [ 1, 2 ]} \n"), &parent, []driver.SourceLink{{Kind: "blocks", To: parent}, {Kind: "waits-for", To: parent}}); err != nil {
			t.Fatal(err)
		}
		bead, _ := driver.NewBeadSentinel(ref.ID)
		sentinels = append(sentinels, bead)
	}
	if err = tx.BumpSentinels(ctx, sentinels); err != nil {
		t.Fatal(err)
	}
	h.Incarnation, h.Kind, h.Method, h.Principal, h.IdempotencyKey, h.RequestHash = meta.Incarnation, model.ChangesetRestore, profile.Method, profile.Principal, profile.IdempotencyKey, profile.RequestHash
	commitSourceTest(t, tx, h)
	elapsed := time.Since(started)
	readCtx, readCancel := context.WithTimeout(ctx, 2*time.Second)
	defer readCancel()
	r := sourceReader(t, s, nil)
	work := &sourceindex.Work{}
	rows, more, err := r.selectIndexedSources(readCtx, sourceindex.Request{Refs: []model.Ref{p}, Kinds: []driver.IncomingLinkKind{driver.IncomingLinkBlocks, driver.IncomingLinkWaitsFor}, Excluded: []model.Ref{refs[0]}, Limit: 1, Inverse: true}, work)
	requireSources(t, rows, more, err, true, refs[1].ID)
	if work.Images != 2 || work.Headers != 8 {
		t.Fatalf("large unrelated/deduplicate image work: %+v", work)
	}
	t.Logf("actual descending 20000-source native driver restore indexed in %s; bounded read images=%d headers=%d", elapsed, work.Images, work.Headers)
}
