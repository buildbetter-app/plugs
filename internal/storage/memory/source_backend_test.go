package memory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func sourceRef(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
func sourceRecord(id, body string) driver.Record {
	return driver.Record{Ref: sourceRef(id), Kind: "bead", Body: []byte(body)}
}

func beginSourceTest(t *testing.T, s *Store, lease driver.ActiveLease, refs ...model.Ref) (*transaction, driver.ChangesetHeader) {
	t.Helper()
	ctx := context.Background()
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := s.Write(ctx, lease)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Close() })
	keys := []driver.LockKey{driver.GraphBlockingSentinelLock}
	for _, ref := range refs {
		ref := ref
		keys = append(keys, driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
		bead, err := driver.NewBeadSentinel(ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead})
	}
	sort.Slice(keys, func(i, j int) bool { return keyID(keys[i]) < keyID(keys[j]) })
	requests := make([]driver.LockRequest, len(keys))
	for i, key := range keys {
		requests[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	if _, err = tx.Lock(ctx, requests); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	sentinels := []driver.Sentinel{}
	for _, key := range keys {
		if key.Kind == driver.LockSentinel {
			sentinels = append(sentinels, key.Sentinel)
		}
	}
	if err = tx.BumpSentinels(ctx, sentinels); err != nil {
		t.Fatal(err)
	}
	return tx.(*transaction), driver.ChangesetHeader{Incarnation: meta.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "source-test"}, LockKeys: keys}
}

func commitSourceTest(t *testing.T, tx *transaction, h driver.ChangesetHeader) model.Changeset {
	t.Helper()
	for key, record := range tx.records {
		op := model.OpCreate
		if tx.base.keys[key].Present {
			op = model.OpPatch
		}
		if record == nil {
			op = model.OpDelete
		}
		h.Changes = append(h.Changes, model.ChangeRecord{Ref: tx.recordRef(key), Op: op, FromRevision: tx.base.keys[key].Revision})
	}
	entry, err := tx.Commit(context.Background(), h)
	if err != nil {
		t.Fatal(err)
	}
	return entry
}

func sourceLease(t *testing.T, s *Store) driver.ActiveLease {
	t.Helper()
	lease, err := s.Lease(context.Background(), "source-index-test", driver.LeaseTTL)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })
	return lease
}

func sourceReader(t *testing.T, s *Store, at *model.Revision) *reader {
	t.Helper()
	meta, err := s.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Read(context.Background(), driver.View{Incarnation: meta.Incarnation, At: at})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r.(*reader)
}

func requireSources(t *testing.T, records []driver.Record, more bool, err error, wantMore bool, ids ...string) {
	t.Helper()
	got := make([]string, 0, len(records))
	for _, record := range records {
		got = append(got, record.Ref.ID)
	}
	if err != nil || more != wantMore || !reflect.DeepEqual(got, ids) && !(len(got) == 0 && len(ids) == 0) {
		t.Fatalf("sources=%v more=%v err=%v; want=%v more=%v", got, more, err, ids, wantMore)
	}
}

func TestGraphSourcePublicationHistoryAndExactBytes(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	p, q, a, z := sourceRef("pl-p"), sourceRef("pl-q"), sourceRef("pl-a"), sourceRef("pl-z")
	tx, h := beginSourceTest(t, s, lease, p, q, a, z)
	first := []byte(" { \"opaque\": [1, 2], \"parent\": \"storage must not decode\" } \n")
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
	if entry.Revision.String() != "1" || len(s.state.sourceLinks.roots[1].sources[keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &a})].links) != 3 {
		t.Fatal("complete source metadata not published at revision one")
	}
	pinned := sourceReader(t, s, nil)
	records, more, err := pinned.HierarchyChildren(ctx, []model.Ref{p, p}, 2)
	requireSources(t, records, more, err, false, a.ID, z.ID)
	wantFirst := []byte(" { \"opaque\": [1, 2], \"parent\": \"storage must not decode\" } \n")
	if !bytes.Equal(records[0].Body, wantFirst) || records[0].Revision.String() != "1" {
		t.Fatal("source bytes/revision changed")
	}
	records[0].Body[0] = '!'
	records, more, err = pinned.InverseSourceLinks(ctx, []model.Ref{q, q}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks, driver.IncomingLinkWaitsFor, driver.IncomingLinkParentChild}, nil, 1)
	requireSources(t, records, more, err, true, a.ID)
	if !bytes.Equal(records[0].Body, wantFirst) {
		t.Fatal("returned body mutated the immutable source image")
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
	records, more, err = historical.HierarchyChildren(ctx, []model.Ref{q}, 10)
	requireSources(t, records, more, err, false)
	records, more, err = historical.InverseSourceLinks(ctx, []model.Ref{q}, []driver.IncomingLinkKind{driver.IncomingLinkWaitsFor}, nil, 10)
	requireSources(t, records, more, err, false, a.ID)
	if !bytes.Equal(records[0].Body, wantFirst) {
		t.Fatal("historical index reconstructed normalized bytes")
	}
	// A publication with no source changes must advance both independent ranges.
	tx, h = beginSourceTest(t, s, lease)
	commitSourceTest(t, tx, h)
	if s.state.hierarchy.through != 3 || s.state.sourceLinks.through != 3 {
		t.Fatal("no-source publication left a coverage gap")
	}
}

func TestGraphSourceDriverOverlayExclusionsAndFailureAtomicity(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	p, q := sourceRef("pl-p"), sourceRef("pl-q")
	refs := []model.Ref{sourceRef("pl-a"), sourceRef("pl-b"), sourceRef("pl-c"), sourceRef("pl-d")}
	tx, h := beginSourceTest(t, s, lease, refs...)
	for _, ref := range refs {
		if err := tx.PutGraphRecord(ctx, sourceRecord(ref.ID, `{}`), &p, []driver.SourceLink{{Kind: "blocks", To: q}}); err != nil {
			t.Fatal(err)
		}
	}
	commitSourceTest(t, tx, h)
	e := sourceRef("pl-e")
	tx, h = beginSourceTest(t, s, lease, append(refs, e)...)
	bound := tx.Snapshot()
	if err := tx.PutGraphRecord(ctx, sourceRecord("pl-a", `{"moved":true}`), &q, nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.Delete(ctx, sourceRef("pl-b")); err != nil {
		t.Fatal(err)
	}
	parent := p
	links := []driver.SourceLink{{Kind: "blocks", To: q}, {Kind: "parent-child", To: q}}
	if err := tx.PutGraphRecord(ctx, sourceRecord(e.ID, " {\"created\":true} \n"), &parent, links); err != nil {
		t.Fatal(err)
	}
	parent = q
	links[0] = driver.SourceLink{Kind: "association", To: p}
	records, more, err := tx.HierarchyChildren(ctx, []model.Ref{p}, 2)
	requireSources(t, records, more, err, true, "pl-c", "pl-d")
	records, more, err = tx.HierarchyChildrenExcluding(ctx, []model.Ref{p}, []model.Ref{sourceRef("pl-d")}, 2)
	requireSources(t, records, more, err, false, "pl-c", "pl-e")
	if records[0].Revision.String() != "1" || !records[1].Revision.IsZero() || string(records[1].Body) != " {\"created\":true} \n" {
		t.Fatal("staged image lost bytes/committed revision token")
	}
	records, more, err = tx.InverseSourceLinks(ctx, []model.Ref{q}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks, driver.IncomingLinkParentChild}, []model.Ref{sourceRef("pl-c"), sourceRef("pl-d")}, 1)
	requireSources(t, records, more, err, false, "pl-e")
	records, more, err = tx.HierarchyChildrenExcluding(ctx, []model.Ref{p}, []model.Ref{sourceRef("pl-c"), sourceRef("pl-d"), e}, 0)
	requireSources(t, records, more, err, false)
	for _, attempt := range []func() error{
		func() error { return tx.Put(ctx, sourceRecord(e.ID, `{"bad":"ordinary"}`)) },
		func() error { return tx.PutHierarchyRecord(ctx, sourceRecord(e.ID, `{"bad":"parent-only"}`), &q) },
		func() error {
			return tx.PutGraphRecord(ctx, sourceRecord(e.ID, `{"bad":"invalid-link"}`), &q, []driver.SourceLink{{Kind: "", To: p}})
		},
		func() error { return tx.PutGraphRecord(ctx, sourceRecord(e.ID, `broken`), &q, nil) },
	} {
		if err := attempt(); err == nil {
			t.Fatal("invalid/undeclared replacement accepted")
		}
	}
	records, more, err = tx.InverseSourceLinks(ctx, []model.Ref{q}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks}, []model.Ref{sourceRef("pl-c"), sourceRef("pl-d")}, 1)
	requireSources(t, records, more, err, false, "pl-e")
	if string(records[0].Body) != " {\"created\":true} \n" || !reflect.DeepEqual(bound, tx.Snapshot()) {
		t.Fatal("staging failure mutated the image/declarations/snapshot")
	}
	commitSourceTest(t, tx, h)
	current := sourceReader(t, s, nil)
	records, more, err = current.HierarchyChildren(ctx, []model.Ref{p}, 10)
	requireSources(t, records, more, err, false, "pl-c", "pl-d", "pl-e")
	tx, h = beginSourceTest(t, s, lease, sourceRef("pl-b"))
	if err = tx.PutGraphRecord(ctx, sourceRecord("pl-b", `{"recreated":true}`), &p, nil); err != nil {
		t.Fatal(err)
	}
	commitSourceTest(t, tx, h)
	at := model.Natural64(2)
	deleted := sourceReader(t, s, &at)
	records, more, err = deleted.HierarchyChildren(ctx, []model.Ref{p}, 10)
	requireSources(t, records, more, err, false, "pl-c", "pl-d", "pl-e")
}

func TestSourceCoverageRefusalAndIndependentDeclarations(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"legacy", "parent-only", "untyped-replacement"} {
		t.Run(mode, func(t *testing.T) {
			s := New()
			defer s.Close()
			lease := sourceLease(t, s)
			p, a := sourceRef("pl-p"), sourceRef("pl-a")
			r := sourceReader(t, s, nil)
			if rows, more, err := r.HierarchyChildren(ctx, nil, 0); rows != nil || more || !errors.Is(err, driver.ErrUnsupported) {
				t.Fatal("unbuilt empty request did not refuse coverage")
			}
			tx, h := beginSourceTest(t, s, lease, a)
			if mode == "parent-only" {
				if err := tx.PutHierarchyRecord(ctx, sourceRecord(a.ID, `{}`), &p); err != nil {
					t.Fatal(err)
				}
			} else {
				if mode == "untyped-replacement" {
					if err := tx.PutGraphRecord(ctx, sourceRecord(a.ID, `{}`), &p, nil); err != nil {
						t.Fatal(err)
					}
				}
				if err := tx.Put(ctx, sourceRecord(a.ID, `{"legacy":true}`)); err != nil {
					t.Fatal(err)
				}
			}
			commitSourceTest(t, tx, h)
			r = sourceReader(t, s, nil)
			if rows, more, err := r.InverseSourceLinks(ctx, nil, nil, nil, 0); rows != nil || more || !errors.Is(err, driver.ErrUnsupported) {
				t.Fatal("Parent-only/legacy write certified complete outgoing links")
			}
			if mode == "parent-only" {
				rows, more, err := r.HierarchyChildren(ctx, []model.Ref{p}, 1)
				requireSources(t, rows, more, err, false, a.ID)
				return
			}
			if len(tx.sourceDeclarations) != 0 || hierarchyReady(s.state.hierarchy) || sourceLinksReady(s.state.sourceLinks) {
				t.Fatal("untyped replacement retained typed metadata/coverage")
			}
			tx, h = beginSourceTest(t, s, lease, a)
			if err := tx.PutGraphRecord(ctx, sourceRecord(a.ID, `{"typed":true}`), &p, nil); err != nil {
				t.Fatal(err)
			}
			commitSourceTest(t, tx, h)
			if hierarchyReady(s.state.hierarchy) || sourceLinksReady(s.state.sourceLinks) {
				t.Fatal("final images certified a populated legacy store")
			}
		})
	}
}

func TestGraphSourceAbortGapAndMalformedImagesPublishNothing(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	p, a := sourceRef("pl-p"), sourceRef("pl-a")
	tx, h := beginSourceTest(t, s, lease, a)
	if err := tx.PutGraphRecord(ctx, sourceRecord(a.ID, `{}`), &p, nil); err != nil {
		t.Fatal(err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := tx.Commit(cancelled, h); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled typed commit admitted")
	}
	if !s.state.meta.Head.IsZero() || hierarchyReady(s.state.hierarchy) || sourceLinksReady(s.state.sourceLinks) || len(s.state.records) != 0 {
		t.Fatal("aborted publication changed source/index/head")
	}
	commitSourceTest(t, tx, h)
	r := sourceReader(t, s, nil)
	if rows, more, err := r.HierarchyChildren(cancelled, []model.Ref{p}, 1); rows != nil || more || !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled read returned partial sources")
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if rows, more, err := r.InverseSourceLinks(ctx, nil, nil, nil, 0); rows != nil || more || err == nil {
		t.Fatal("closed source reader admitted empty request")
	}
	// Simulate an older publisher advancing Head without maintaining coverage.
	next := s.state.fork()
	next.meta.Head = model.Natural64(2)
	s.state = next
	r = sourceReader(t, s, nil)
	if rows, more, err := r.HierarchyChildren(ctx, nil, 0); rows != nil || more || !errors.Is(err, driver.ErrUnsupported) {
		t.Fatal("stale coverage did not refuse even an empty request")
	}
	tx, h = beginSourceTest(t, s, lease)
	if _, err := tx.Commit(ctx, h); !errors.Is(err, driver.ErrUnsupported) || s.state.meta.Head.String() != "2" {
		t.Fatal("writer bridged an older publisher coverage gap")
	}
	_ = tx.Close()
	// A selected malformed source, including lookahead, must not leak a prefix.
	s.state = next.fork()
	s.state.meta.Head = model.Natural64(1)
	root := *s.state.hierarchy.roots[1]
	root.sources = make(map[string]hierarchySource)
	key, _ := recordKey(a)
	root.sources[key] = hierarchySource{parentKey: keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &p}), record: sourceRecord(a.ID, `bad`)}
	index := *s.state.hierarchy
	index.roots = []*hierarchyRoot{index.roots[0], &root}
	s.state.hierarchy = &index
	r = sourceReader(t, s, nil)
	if rows, more, err := r.HierarchyChildren(ctx, []model.Ref{p}, 0); rows != nil || more || !errors.Is(err, driver.ErrUnsupported) {
		t.Fatal("malformed lookahead returned success")
	}
}

func TestGraphSourceNativeRestoreBindsIncarnationAndFloor(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	p, a := sourceRef("pl-p"), sourceRef("pl-a")
	tx, h := beginSourceTest(t, s, lease, a)
	// The native capability is prepared before any staged bump/image.
	tx.bumps = make(map[string]driver.LockKey)
	old := h.Incarnation
	profile := driver.NativeRestoreProfile{StoreID: "native-source", Principal: model.PrincipalRef{Kind: "user", ID: "operator"}, Method: "admin.restore.finish", IdempotencyKey: "restore-source", RequestHash: strings.Repeat("a", 64)}
	meta, err := tx.PrepareNativeRestore(ctx, profile)
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
	if meta.Incarnation == old || s.state.hierarchy.builtFrom != 1 || s.state.sourceLinks.builtFrom != 1 {
		t.Fatal("restore reused old incarnation or invented revision-zero coverage")
	}
	if _, err = s.Read(ctx, driver.View{Incarnation: old}); err == nil {
		t.Fatal("old incarnation rebound to restored source index")
	}
	zero := model.Natural64(0)
	if _, err = s.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &zero}); err == nil {
		t.Fatal("restore invented history below its floor")
	}
	r := sourceReader(t, s, nil)
	rows, more, err := r.HierarchyChildren(ctx, []model.Ref{p}, 1)
	requireSources(t, rows, more, err, false, a.ID)
	if string(rows[0].Body) != " {\"native\": true} \n" {
		t.Fatal("restore changed exact indexed image")
	}
}

func TestSourceSelectionBoundedWithUnrelatedCorpus(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	p, q := sourceRef("pl-p"), sourceRef("pl-q")
	refs := make([]model.Ref, 204)
	for i := range refs {
		refs[i] = sourceRef(fmt.Sprintf("pl-%05d", i))
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
	work := &sourceReadWork{}
	rows, more, err := readHierarchySources(ctx, s.state.hierarchy, s.state.meta.Incarnation, 1, []model.Ref{p, p}, []model.Ref{refs[0]}, 1, nil, work)
	requireSources(t, rows, more, err, true, refs[1].ID)
	if work.images != 2 || work.candidates != 3 || work.buckets != 1 {
		t.Fatalf("Parent selector touched unrelated sources: %+v", work)
	}
	work = &sourceReadWork{}
	rows, more, err = readInverseSources(ctx, s.state.sourceLinks, s.state.meta.Incarnation, 1, []model.Ref{p, p}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks, driver.IncomingLinkWaitsFor}, []model.Ref{refs[0]}, 1, nil, work)
	requireSources(t, rows, more, err, true, refs[1].ID)
	if work.images != 2 || work.candidates != 6 || work.buckets != 2 {
		t.Fatalf("inverse source dedup happened after image fetch: %+v", work)
	}
	// Legacy record APIs remain functional without pretending to certify them.
	legacy := New()
	defer legacy.Close()
	legacyLease := sourceLease(t, legacy)
	if _, err := conformance.Seed(ctx, legacy, legacyLease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: sourceRef("pl-legacy"), Kind: "test", Op: model.OpCreate, Body: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
}

func TestGraphSourceNativeRestoreTwentyThousandAndBoundedRead(t *testing.T) {
	ctx := context.Background()
	s := New()
	defer s.Close()
	lease := sourceLease(t, s)
	refs := make([]model.Ref, 20000)
	for i := range refs {
		refs[i] = sourceRef(fmt.Sprintf("pl-restore-%05d", i))
	}
	started := time.Now()
	tx, h := beginSourceTest(t, s, lease, refs...)
	tx.bumps = make(map[string]driver.LockKey)
	profile := driver.NativeRestoreProfile{StoreID: "native-twenty-thousand", Principal: model.PrincipalRef{Kind: "user", ID: "operator"}, Method: "admin.restore.finish", IdempotencyKey: "restore-large-source", RequestHash: strings.Repeat("b", 64)}
	meta, err := tx.PrepareNativeRestore(ctx, profile)
	if err != nil {
		t.Fatal(err)
	}
	p, unrelated := sourceRef("pl-parent"), sourceRef("pl-unrelated-parent")
	sentinels := []driver.Sentinel{driver.SentinelGraphBlocking}
	for i, ref := range refs {
		parent := unrelated
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
	writeElapsed := time.Since(started)
	if len(s.state.hierarchy.roots[0].sources) != 20000 || len(s.state.sourceLinks.roots[0].sources) != 20000 {
		t.Fatal("native restore did not index every actual source image")
	}
	readCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	r := sourceReader(t, s, nil)
	rows, more, err := r.HierarchyChildren(readCtx, []model.Ref{p}, 1)
	requireSources(t, rows, more, err, true, refs[0].ID)
	rows, more, err = r.InverseSourceLinks(readCtx, []model.Ref{p}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks, driver.IncomingLinkWaitsFor}, []model.Ref{refs[0]}, 1)
	requireSources(t, rows, more, err, true, refs[1].ID)
	work := &sourceReadWork{}
	rows, more, err = readInverseSources(readCtx, s.state.sourceLinks, meta.Incarnation, 1, []model.Ref{p}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks, driver.IncomingLinkWaitsFor}, []model.Ref{refs[0]}, 1, nil, work)
	requireSources(t, rows, more, err, true, refs[1].ID)
	if work.images != 2 || work.candidates != 6 {
		t.Fatalf("large restore reader materialized unrelated/duplicate sources: %+v", work)
	}
	t.Logf("actual 20000-source native driver restore indexed in %s; bounded read images=%d candidate memberships=%d", writeElapsed, work.images, work.candidates)
}
