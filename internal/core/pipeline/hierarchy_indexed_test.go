package pipeline

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/port"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This is a source-planner fixture, not backend qualification. Its buckets are
// independent of closure, and Query/common-prefix reads are forbidden. Count
// image materialization, including lookahead, rather than only returned rows.
type indexedHierarchyReader struct {
	driver.Reader
	records       map[string]driver.Record
	children      map[string][]string
	rows          map[string]driver.DerivedRow
	closureReads  int
	images        int
	excludedReads int
	observed      map[string]driver.LockState
	requests      []int
	err           error
	inverseErr    error
	inverseReads  int
	queryReads    int
	allowQuery    bool
	certificate   *model.Revision
}

func hierarchyOracle(t *testing.T, parents map[string]string) map[string]driver.DerivedRow {
	t.Helper()
	rows := map[string]driver.DerivedRow{}
	for id := range parents {
		seen := map[string]bool{}
		for ancestor, depth := id, uint64(0); ancestor != ""; ancestor, depth = parents[ancestor], depth+1 {
			if seen[ancestor] {
				t.Fatal("cyclic fixture")
			}
			seen[ancestor] = true
			if _, exists := parents[ancestor]; !exists {
				t.Fatal("missing fixture parent")
			}
			row, err := AncestorRow(id, ancestor, depth)
			if err != nil {
				t.Fatal(err)
			}
			rows[row.Key] = row
		}
	}
	return rows
}

func indexedHierarchyFixture(t *testing.T, parents map[string]string) *indexedHierarchyReader {
	t.Helper()
	r := &indexedHierarchyReader{records: map[string]driver.Record{}, children: map[string][]string{}, rows: hierarchyOracle(t, parents), observed: map[string]driver.LockState{}}
	for id, parent := range parents {
		doc := Document{ID: id, Type: "issue", Fields: map[string]any{"status": "todo"}}
		if parent != "" {
			doc.Parent = &model.Ref{ID: parent}
		}
		body, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		r.records[id] = driver.Record{Ref: model.Ref{ID: id}, Kind: "bead", Revision: model.Natural64(1), Body: body}
		r.children[parent] = append(r.children[parent], id)
	}
	return r
}

func (r *indexedHierarchyReader) Snapshot() driver.View {
	revision := model.Natural64(1)
	return driver.View{Incarnation: "indexed-hierarchy", At: &revision}
}

func (r *indexedHierarchyReader) Query(_ context.Context, _ driver.Query) (driver.Page[driver.Record], error) {
	r.queryReads++
	if !r.allowQuery {
		return driver.Page[driver.Record]{}, errors.New("indexed hierarchy must not query source records")
	}
	refs := map[string]bool{}
	for id := range r.records {
		refs[id] = true
	}
	items := make([]driver.Record, 0, len(refs))
	for _, ref := range hierarchyRefs(refs) {
		items = append(items, r.records[ref.ID])
	}
	return driver.Page[driver.Record]{Items: items}, nil
}

func (r *indexedHierarchyReader) InverseSourceLinks(_ context.Context, targets []model.Ref, kinds []driver.IncomingLinkKind, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	r.inverseReads++
	if _, err := driver.NormalizeInverseSourceLinksRequest(targets, kinds, excluded, limit); err != nil {
		return nil, false, err
	}
	if r.inverseErr != nil {
		return nil, false, r.inverseErr
	}
	targetSet, kindSet, omitted := map[string]bool{}, map[string]bool{}, map[string]bool{}
	for _, target := range targets {
		targetSet[target.ID] = true
	}
	for _, kind := range kinds {
		kindSet[string(kind)] = true
	}
	for _, ref := range excluded {
		omitted[ref.ID] = true
	}
	selected := map[string]bool{}
	for id, record := range r.records {
		if omitted[id] {
			continue
		}
		doc, err := DecodeDocument(record.Body)
		if err != nil {
			return nil, false, err
		}
		for _, link := range doc.Links {
			if targetSet[link.To.ID] && kindSet[link.Kind] {
				selected[id] = true
			}
		}
	}
	refs := hierarchyRefs(selected)
	more := len(refs) > limit
	if more {
		refs = refs[:limit]
	}
	result := make([]driver.Record, len(refs))
	for i, ref := range refs {
		result[i] = r.records[ref.ID]
	}
	return result, more, nil
}

func (r *indexedHierarchyReader) Observe(_ context.Context, keys []driver.LockKey) ([]driver.LockState, error) {
	states := make([]driver.LockState, len(keys))
	for i, key := range keys {
		state := driver.LockState{Key: key, Revision: model.Natural64(1)}
		switch key.Kind {
		case driver.LockRecord:
			if record, exists := r.records[key.Ref.ID]; exists {
				state.Present, state.Record = true, &record
			}
		case driver.LockDerived:
			if row, exists := r.rows[key.Derived.Key]; exists {
				state.Present, state.Derived = true, &row
			}
		case driver.LockSentinel:
			state.Present = true
		case driver.LockReadFact:
			if key.Fact == GraphValidityReadFact && r.certificate != nil {
				state.Present, state.Revision = true, *r.certificate
			}
		}
		encoded, _ := driver.EncodeLockKey(key)
		r.observed[encoded], states[i] = state, state
	}
	return states, nil
}

func (r *indexedHierarchyReader) HierarchyChildren(ctx context.Context, parents []model.Ref, limit int) ([]driver.Record, bool, error) {
	return r.HierarchyChildrenExcluding(ctx, parents, nil, limit)
}

func (r *indexedHierarchyReader) HierarchyChildrenExcluding(_ context.Context, parents, excluded []model.Ref, limit int) ([]driver.Record, bool, error) {
	if _, err := driver.NormalizeHierarchyExcludingRequest(parents, excluded, limit); err != nil {
		return nil, false, err
	}
	if r.err != nil {
		return nil, false, r.err
	}
	r.requests = append(r.requests, limit)
	if len(excluded) != 0 {
		r.excludedReads++
	}
	omitted, selected := map[string]bool{}, map[string]bool{}
	for _, ref := range excluded {
		omitted[ref.ID] = true
	}
	for _, ref := range parents {
		for _, id := range r.children[ref.ID] {
			if !omitted[id] {
				selected[id] = true
			}
		}
	}
	refs := hierarchyRefs(selected)
	count := len(refs)
	if count > limit+1 {
		count = limit + 1
	}
	r.images += count
	more := len(refs) > limit
	if len(refs) > limit {
		refs = refs[:limit]
	}
	records := make([]driver.Record, len(refs))
	for i, ref := range refs {
		records[i] = r.records[ref.ID]
		records[i].Body = bytes.Clone(records[i].Body)
	}
	return records, more, nil
}

func (r *indexedHierarchyReader) DerivedRows(_ context.Context, table driver.DerivedTable, prefix string) ([]driver.DerivedRow, error) {
	r.closureReads++
	if table != driver.DerivedAncestorClosure || prefix == "" {
		return nil, errors.New("unselected closure prefix")
	}
	rows := []driver.DerivedRow{}
	for key, row := range r.rows {
		if strings.HasPrefix(key, prefix) {
			rows = append(rows, row)
		}
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].Key < rows[j].Key })
	return rows, nil
}

func indexedPrepared(r *indexedHierarchyReader, edits ...hierarchyEdit) *prepared {
	return &prepared{snapshot: &snapshotReader{reader: r, reads: &readSet{states: map[string]driver.LockState{}}}, writes: map[string]*pending{}, hierarchyEdits: edits}
}

func TestIndexedHierarchyMissingDescendantClosureRefuses(t *testing.T) {
	for _, missing := range []string{"d-c", "d-b", "whole-d"} {
		t.Run(missing, func(t *testing.T) {
			r := indexedHierarchyFixture(t, map[string]string{"a": "", "b": "a", "c": "b", "d": "c"})
			for key, row := range r.rows {
				d, a, _, _ := DecodeAncestorRow(row)
				if d == "d" && (missing == "whole-d" || missing == "d-c" && a == "c" || missing == "d-b" && a == "b") {
					delete(r.rows, key)
				}
			}
			before, err := json.Marshal(struct {
				Records map[string]driver.Record
				Rows    map[string]driver.DerivedRow
			}{r.records, r.rows})
			if err != nil {
				t.Fatal(err)
			}
			p := indexedPrepared(r, hierarchyEdit{id: "c"})
			code(t, p.hierarchyIndexed(context.Background()), "ontology_guard_failed")
			if p.hierarchyDelta != nil {
				t.Fatal("malformed closure produced a publication delta")
			}
			key := recordLockKey(model.Ref{ID: "d"})
			if !r.observed[key].Present || r.images == 0 {
				t.Fatal("descendant was not discovered and observed from source")
			}
			after, _ := json.Marshal(struct {
				Records map[string]driver.Record
				Rows    map[string]driver.DerivedRow
			}{r.records, r.rows})
			if !bytes.Equal(before, after) {
				t.Fatal("planner mutated source or closure")
			}
		})
	}
}

func TestIndexedHierarchyUnbuiltCoverageUsesCompletePlanner(t *testing.T) {
	r := indexedHierarchyFixture(t, map[string]string{"parent": "", "child": "parent"})
	r.err = driver.ErrUnsupported // The interface exists, but this snapshot is unbuilt.
	r.allowQuery = true
	child, err := DecodeDocument(r.records["child"].Body)
	if err != nil {
		t.Fatal(err)
	}
	child.Parent = nil
	p := indexedPrepared(r, hierarchyEdit{id: "child"})
	p.writes["child"] = &pending{ref: model.Ref{ID: "child"}, kind: "bead", doc: child, before: r.records["child"].Body}
	docs, err := p.hierarchy(context.Background())
	if err != nil || r.queryReads == 0 || p.hierarchySources != nil || p.hierarchyDelta == nil || docs["child"].Parent != nil {
		t.Fatalf("unbuilt coverage was treated as complete: queries=%d sources=%v delta=%v child=%+v err=%v", r.queryReads, p.hierarchySources, p.hierarchyDelta, docs["child"], err)
	}
}

func TestIndexedHierarchyDetachExactBoundBeforeClosure(t *testing.T) {
	for _, members := range []int{5000, 5001} {
		t.Run(fmt.Sprint(members), func(t *testing.T) {
			parents := map[string]string{"a": "", "target": "a"}
			for i := 1; i < members; i++ {
				parents[fmt.Sprintf("leaf-%05d", i)] = "target"
			}
			r := indexedHierarchyFixture(t, parents)
			p := indexedPrepared(r, hierarchyEdit{id: "target"})
			err := p.hierarchyIndexed(context.Background())
			if members == 5001 {
				code(t, err, "closure_too_large")
				if r.closureReads != 0 || p.hierarchyDelta != nil {
					t.Fatal("overflow read closure or constructed a delta")
				}
			} else if err != nil || len(p.hierarchyDelta.remove) != 5000 || len(p.hierarchyDelta.add) != 0 {
				t.Fatalf("exact bound: delta=%v err=%v", p.hierarchyDelta, err)
			}
			if r.images > 5000 { // target + at most 5000 child images, including lookahead
				t.Fatalf("materialized %d child images beyond the source bound", r.images)
			}
		})
	}
}

func TestIndexedHierarchyReparentCountsSharedDepthReplacements(t *testing.T) {
	parents := map[string]string{"a": "", "left": "a", "right": "a", "target": "left", "child": "target", "grandchild": "child"}
	r := indexedHierarchyFixture(t, parents)
	p := indexedPrepared(r, hierarchyEdit{id: "target", parent: "right"})
	if err := p.hierarchyIndexed(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, row := range p.hierarchyDelta.remove {
		delete(r.rows, row.Key)
	}
	for _, row := range p.hierarchyDelta.add {
		r.rows[row.Key] = row
	}
	parents["target"] = "right"
	if !reflect.DeepEqual(r.rows, hierarchyOracle(t, parents)) {
		t.Fatal("multilevel reparent differs from independent final Parent oracle")
	}
	if len(p.hierarchyDelta.remove) != 3 || len(p.hierarchyDelta.add) != 3 {
		t.Fatal("unchanged common suffix was rewritten")
	}
}

func TestIndexedHierarchyPreparedExclusionsPrecedeImages(t *testing.T) {
	r := indexedHierarchyFixture(t, map[string]string{"target": "", "other": "", "a": "target", "b": "target", "c": "target"})
	s := &hierarchySources{snapshot: indexedPrepared(r).snapshot, records: map[string]driver.Record{}, stored: map[string]Document{}, parents: map[string]string{"a": "other"}, overlay: map[string]bool{"a": true}, deleted: map[string]bool{}}
	ids, more, err := s.children(context.Background(), []model.Ref{{ID: "target"}}, 1)
	if err != nil || !more || !reflect.DeepEqual(ids, []string{"b"}) || r.images != 2 || r.excludedReads != 1 {
		t.Fatalf("ids=%v more=%t images=%d excludedCalls=%d err=%v", ids, more, r.images, r.excludedReads, err)
	}
	if _, exists := r.observed[recordLockKey(model.Ref{ID: "a"})]; exists {
		t.Fatal("excluded source was materialized")
	}
}

func TestIndexedHierarchyOrderedPrefixesPreserveDeletedMembersAndCancellation(t *testing.T) {
	t.Run("deleted-member-remains-in-later-move", func(t *testing.T) {
		parents := map[string]string{"a": "", "target": "a", "child": "target"}
		r := indexedHierarchyFixture(t, parents)
		p := indexedPrepared(r, hierarchyEdit{id: "child", deleted: true}, hierarchyEdit{id: "target"})
		if err := p.hierarchyIndexed(context.Background()); err != nil {
			t.Fatal(err)
		}
		for _, row := range p.hierarchyDelta.remove {
			delete(r.rows, row.Key)
		}
		for _, row := range p.hierarchyDelta.add {
			r.rows[row.Key] = row
		}
		delete(parents, "child")
		parents["target"] = ""
		if !reflect.DeepEqual(r.rows, hierarchyOracle(t, parents)) || r.excludedReads == 0 {
			t.Fatal("deleted prefix member/final-image delta was lost")
		}
	})
	t.Run("ordered-charge-cannot-cancel", func(t *testing.T) {
		parents := map[string]string{"target": "", "ancestor-0": ""}
		for i := 1; i < 10; i++ {
			parents[fmt.Sprintf("ancestor-%d", i)] = fmt.Sprintf("ancestor-%d", i-1)
		}
		for i := 1; i < 500; i++ {
			parents[fmt.Sprintf("leaf-%03d", i)] = "target"
		}
		r := indexedHierarchyFixture(t, parents)
		p := indexedPrepared(r, hierarchyEdit{id: "target", parent: "ancestor-9"}, hierarchyEdit{id: "target"})
		code(t, p.hierarchyIndexed(context.Background()), "closure_too_large")
		if r.closureReads != 0 || p.hierarchyDelta != nil {
			t.Fatal("canceling overflow reached closure publication")
		}
	})
}

func TestHierarchySnapshotTracksEmptyMoreAndUnavailableMembership(t *testing.T) {
	for _, mode := range []string{"empty", "more", "unsupported"} {
		t.Run(mode, func(t *testing.T) {
			r := indexedHierarchyFixture(t, map[string]string{"a": "", "b": "a"})
			parents := []model.Ref{}
			if mode == "more" {
				parents = []model.Ref{{ID: "a"}}
			}
			if mode == "unsupported" {
				r.err = driver.ErrUnsupported
			}
			snapshot := indexedPrepared(r).snapshot
			records, more, err := snapshot.HierarchyChildren(context.Background(), parents, 0)
			if len(records) != 0 || more != (mode == "more") || !errors.Is(err, r.err) {
				t.Fatalf("records=%v more=%t err=%v", records, more, err)
			}
			key, _ := driver.EncodeLockKey(driver.GraphBlockingSentinelLock)
			if _, exists := snapshot.reads.states[key]; !exists {
				t.Fatal("membership predicate was not observed")
			}
		})
	}
}

func TestIndexedHierarchyCreateUnderExistingParent(t *testing.T) {
	r := indexedHierarchyFixture(t, map[string]string{"parent": ""})
	doc := Document{ID: "new", Type: "issue", Parent: &model.Ref{ID: "parent"}, Fields: map[string]any{"status": "todo"}}
	p := indexedPrepared(r, hierarchyEdit{id: "new", parent: "parent", create: true})
	p.writes["new"] = &pending{ref: model.Ref{ID: "new"}, kind: "bead", doc: doc}
	if err := p.hierarchyIndexed(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(p.hierarchyDelta.add) != 2 || len(p.hierarchyDelta.remove) != 0 {
		t.Fatalf("create closure delta = %+v", p.hierarchyDelta)
	}
	if _, exists := r.observed[recordLockKey(model.Ref{ID: "parent"})]; !exists {
		t.Fatal("existing parent source was not observed")
	}
}

func TestIndexedHierarchyOrderedIntermediateParentDiffersFromFinal(t *testing.T) {
	r := indexedHierarchyFixture(t, map[string]string{"a": "", "b": "", "d": "", "c": "a"})
	doc, err := DecodeDocument(r.records["c"].Body)
	if err != nil {
		t.Fatal(err)
	}
	doc.Parent = &model.Ref{ID: "d"}
	p := indexedPrepared(r, hierarchyEdit{id: "c", parent: "b"}, hierarchyEdit{id: "c", parent: "d"})
	p.writes["c"] = &pending{ref: model.Ref{ID: "c"}, kind: "bead", doc: doc, before: r.records["c"].Body}
	if err := p.hierarchyIndexed(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(p.hierarchyDelta.remove) != 1 || len(p.hierarchyDelta.add) != 1 {
		t.Fatalf("final delta = %+v", p.hierarchyDelta)
	}
}

func TestIndexedHierarchyPreparedOverflowStillObservesMembership(t *testing.T) {
	r := indexedHierarchyFixture(t, map[string]string{"parent": ""})
	s := &hierarchySources{snapshot: indexedPrepared(r).snapshot, parents: map[string]string{"a": "parent", "b": "parent"}, overlay: map[string]bool{"a": true, "b": true}, deleted: map[string]bool{}}
	_, more, err := s.children(context.Background(), []model.Ref{{ID: "parent"}}, 1)
	if err != nil || !more || len(r.requests) != 1 || r.requests[0] != 0 {
		t.Fatalf("more=%t requests=%v err=%v", more, r.requests, err)
	}
	key, _ := driver.EncodeLockKey(driver.GraphBlockingSentinelLock)
	if _, exists := s.snapshot.reads.states[key]; !exists {
		t.Fatal("overflow omitted graph membership observation")
	}
}

func TestIndexedGraphIncomingLegacyChildForcesCompleteRefusal(t *testing.T) {
	r := indexedHierarchyFixture(t, map[string]string{"c": "", "p": "", "x": ""})
	x, err := DecodeDocument(r.records["x"].Body)
	if err != nil {
		t.Fatal(err)
	}
	x.Links = []Link{{Kind: "parent-child", To: model.Ref{ID: "c"}}, {Kind: "blocks", To: model.Ref{ID: "p"}}}
	body, _ := json.Marshal(x)
	row := r.records["x"]
	row.Body = body
	r.records["x"] = row
	r.allowQuery = true
	c, err := DecodeDocument(r.records["c"].Body)
	if err != nil {
		t.Fatal(err)
	}
	c.Parent = &model.Ref{ID: "p"}
	p := indexedPrepared(r, hierarchyEdit{id: "c", parent: "p"})
	p.writes["c"] = &pending{ref: model.Ref{ID: "c"}, kind: "bead", doc: c, before: r.records["c"].Body}
	code(t, p.graph(context.Background()), "ontology_guard_failed")
	if r.inverseReads == 0 || r.queryReads == 0 || p.hierarchyDelta == nil {
		t.Fatalf("missing legacy incoming fact path: inverse=%d query=%d delta=%v", r.inverseReads, r.queryReads, p.hierarchyDelta)
	}
}

func TestIndexedGraphIncompleteInverseNeverProvesAbsence(t *testing.T) {
	r := indexedHierarchyFixture(t, map[string]string{"c": "", "p": ""})
	r.inverseErr = driver.ErrUnsupported
	r.allowQuery = true
	p := indexedPrepared(r, hierarchyEdit{id: "c", parent: "p"})
	if err := p.graph(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.inverseReads == 0 || r.queryReads == 0 {
		t.Fatal("unsupported link coverage skipped complete graph scan")
	}
}

func TestIndexedGraphRestoredUnrelatedInvalidComponentStillRefuses(t *testing.T) {
	for _, malformed := range []string{"blocking-cycle", "dangling-custom-link"} {
		t.Run(malformed, func(t *testing.T) {
			r := indexedHierarchyFixture(t, map[string]string{"c": "", "p": "", "u": "", "v": ""})
			u, _ := DecodeDocument(r.records["u"].Body)
			v, _ := DecodeDocument(r.records["v"].Body)
			if malformed == "blocking-cycle" {
				u.Links = []Link{{Kind: "blocks", To: model.Ref{ID: "v"}}}
				v.Links = []Link{{Kind: "blocks", To: model.Ref{ID: "u"}}}
			} else {
				u.Links = []Link{{Kind: "custom-link", To: model.Ref{ID: "missing"}}}
			}
			for id, doc := range map[string]Document{"u": u, "v": v} {
				row := r.records[id]
				row.Body, _ = json.Marshal(doc)
				r.records[id] = row
			}
			before := append([]byte(nil), r.records["u"].Body...)
			c, _ := DecodeDocument(r.records["c"].Body)
			c.Parent = &model.Ref{ID: "p"}
			p := indexedPrepared(r, hierarchyEdit{id: "c", parent: "p"})
			p.writes["c"] = &pending{ref: model.Ref{ID: "c"}, kind: "bead", doc: c, before: r.records["c"].Body}
			r.allowQuery = true
			code(t, p.graph(context.Background()), "ontology_guard_failed")
			if r.inverseReads == 0 || r.queryReads == 0 || !bytes.Equal(before, r.records["u"].Body) {
				t.Fatalf("globally invalid restored graph escaped: inverse=%d query=%d", r.inverseReads, r.queryReads)
			}
		})
	}
}

func TestIndexedGraphRequiresCurrentGlobalValidityCertificate(t *testing.T) {
	for _, state := range []string{"absent", "stale", "current"} {
		t.Run(state, func(t *testing.T) {
			r := indexedHierarchyFixture(t, map[string]string{"c": "", "p": ""})
			if state != "absent" {
				revision := model.Natural64(0)
				if state == "current" {
					revision = model.Natural64(1)
				}
				r.certificate = &revision
			}
			r.allowQuery = state != "current"
			c, _ := DecodeDocument(r.records["c"].Body)
			c.Parent = &model.Ref{ID: "p"}
			p := indexedPrepared(r, hierarchyEdit{id: "c", parent: "p"})
			p.writes["c"] = &pending{ref: model.Ref{ID: "c"}, kind: "bead", doc: c, before: r.records["c"].Body}
			if err := p.graph(context.Background()); err != nil {
				t.Fatal(err)
			}
			if !p.graphValidated || r.inverseReads == 0 || (r.queryReads == 0) != (state == "current") {
				t.Fatalf("certificate=%s validated=%t inverse=%d complete_queries=%d", state, p.graphValidated, r.inverseReads, r.queryReads)
			}
			key, _ := driver.EncodeLockKey(GraphValidityLockKey())
			if _, ok := p.snapshot.reads.states[key]; !ok {
				t.Fatal("semantic certificate was not retained in the pinned read set")
			}
		})
	}
}

func TestCompleteGraphValidationDoesNotCertifyUnbuiltSourceCoverage(t *testing.T) {
	for _, domain := range []string{"parent", "links"} {
		t.Run(domain, func(t *testing.T) {
			r := indexedHierarchyFixture(t, map[string]string{"c": "", "p": ""})
			if domain == "parent" {
				r.err = driver.ErrUnsupported
			} else {
				r.inverseErr = driver.ErrUnsupported
			}
			r.allowQuery = true
			c, _ := DecodeDocument(r.records["c"].Body)
			c.Parent = &model.Ref{ID: "p"}
			p := indexedPrepared(r, hierarchyEdit{id: "c", parent: "p"})
			p.writes["c"] = &pending{ref: model.Ref{ID: "c"}, kind: "bead", doc: c, before: r.records["c"].Body}
			if err := p.graph(context.Background()); err != nil {
				t.Fatal(err)
			}
			if p.graphValidated || r.queryReads == 0 {
				t.Fatalf("unbuilt %s coverage received a reusable certificate: validated=%t queries=%d", domain, p.graphValidated, r.queryReads)
			}
		})
	}
}

// Seed an opaque stored image through the actual driver's publication boundary.
// Typed Parent metadata certifies the index; an ordinary Put leaves it unbuilt.
// Storage intentionally does not interpret the duplicate legacy declaration.
func seedIndexedMalformedStoredParent(t *testing.T, h *harness, certified bool) {
	t.Helper()
	ctx := context.Background()
	refs := []model.Ref{{ID: "parent"}, {ID: "child"}, {ID: "grandchild"}}
	keys := []driver.LockKey{driver.GraphBlockingSentinelLock}
	sentinels := []driver.Sentinel{driver.GraphBlockingSentinelLock.Sentinel}
	for _, ref := range refs {
		ref := ref
		bead, err := driver.NewBeadSentinel(ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, driver.LockKey{Kind: driver.LockRecord, Ref: &ref}, driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead})
		sentinels = append(sentinels, bead)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	requests := make([]driver.LockRequest, len(keys))
	for i, key := range keys {
		requests[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	tx, err := h.store.Write(ctx, h.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err := tx.Lock(ctx, requests); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, sentinels); err != nil {
		t.Fatal(err)
	}
	changes := make([]model.ChangeRecord, 0, len(refs))
	for i, ref := range refs {
		doc := Document{ID: ref.ID, Type: "issue", Fields: map[string]any{"status": "todo", "title": "stored"}}
		if i > 0 {
			parent := refs[i-1]
			doc.Parent = &parent
		}
		if i == 2 {
			doc.Links = []Link{{Kind: "parent-child", To: refs[1]}, {Kind: "parent-child", To: refs[1]}}
		}
		body, err := json.Marshal(doc)
		if err != nil {
			t.Fatal(err)
		}
		record := driver.Record{Ref: ref, Kind: "bead", Body: body}
		if certified {
			err = tx.(driver.HierarchySourceWriter).PutHierarchyRecord(ctx, record, doc.Parent)
		} else {
			err = tx.Put(ctx, record)
		}
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, model.ChangeRecord{Ref: ref, Op: model.OpCreate, FromRevision: model.Natural64(0)})
	}
	m, err := h.store.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: m.Incarnation, Kind: model.ChangesetWrite, Time: h.now, Principal: by, LockKeys: keys, Changes: changes}); err != nil {
		t.Fatal(err)
	}
}

func TestIndexedHierarchyStoredParentRefusalParity(t *testing.T) {
	var first *model.ErrorData
	for _, certified := range []bool{true, false} {
		name := "unbuilt"
		wantMessage := "invalid stored hierarchy document"
		if certified {
			name = "certified"
			wantMessage = "hierarchy index differs from canonical Parent"
		}
		t.Run(name, func(t *testing.T) {
			h := setup(t)
			seedIndexedMalformedStoredParent(t, h, certified)
			ctx := context.Background()
			r := h.read(t)
			records, more, err := r.(driver.HierarchyChildrenReader).HierarchyChildren(ctx, []model.Ref{{ID: "child"}}, 10)
			if certified {
				if err != nil || more || len(records) != 1 || records[0].Ref.ID != "grandchild" {
					t.Fatalf("certified source coverage: records=%v more=%t err=%v", records, more, err)
				}
			} else if !errors.Is(err, driver.ErrUnsupported) || more || len(records) != 0 {
				t.Fatalf("unbuilt source coverage: records=%v more=%t err=%v", records, more, err)
			}
			closure, err := r.DerivedRows(ctx, driver.DerivedAncestorClosure, "")
			if err != nil {
				t.Fatal(err)
			}
			r.Close()
			before := metadataGraph(t, h)
			posts := 0
			w := h.writer(WithPostSubscribers(PostBinding{ID: "malformed-parent-refusal", Subscriber: postFunc(func(context.Context, port.CommittedChange, port.SnapshotReader) error {
				posts++
				return nil
			})}))
			m := meta(1)
			// Stage a field update before a hierarchy edit. Both planners must
			// discover the malformed descendant before publishing either op.
			out, err := w.ApplyWithOutcome(ctx, by, model.ChangeSet{Ops: []model.Op{
				patch(model.Ref{ID: "child"}, map[string]any{"title": "must not publish"}),
				{Kind: model.OpReparent, Reparent: &model.ReparentOp{Target: model.Ref{ID: "child"}}},
			}}, m)
			var refusal *model.Error
			if !errors.As(err, &refusal) || refusal.Data.Code != "ontology_guard_failed" || refusal.Data.Retryable || refusal.Data.TraceID != "" || refusal.Message != wantMessage || errors.Is(err, driver.ErrUnsupported) {
				t.Fatalf("malformed stored parent refusal=%+v", err)
			}
			// Distinct validation boundaries retain their exact diagnostic text;
			// the complete typed refusal data must agree across both planners.
			if first == nil {
				first = &refusal.Data
			} else if !reflect.DeepEqual(first, &refusal.Data) {
				t.Fatalf("indexed/fallback typed refusal differs: indexed=%+v fallback=%+v", first, &refusal.Data)
			}
			if !reflect.DeepEqual(out, port.WriteOutcome{}) {
				t.Fatalf("refusal returned an outcome: %+v", out)
			}
			if !reflect.DeepEqual(before, metadataGraph(t, h)) || metadataReceipt(t, h, m) != nil || posts != 0 {
				t.Fatal("refusal changed stored images, versions, history/events, head, predicates, allocator, receipt or publication")
			}
			r = h.read(t)
			afterClosure, err := r.DerivedRows(ctx, driver.DerivedAncestorClosure, "")
			if err != nil || !reflect.DeepEqual(closure, afterClosure) {
				t.Fatalf("refusal changed closure: before=%v after=%v err=%v", closure, afterClosure, err)
			}
			t.Logf("%s coverage: actual Writer rejected malformed stored legacy parent; identical typed refusal, zero outcome/receipt/publication and unchanged durable state/closure", name)
		})
	}
}
