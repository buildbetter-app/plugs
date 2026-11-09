package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// RunSelection exercises a real store implementing the reference contract.
// These finite semantic oracles do not qualify SQL execution or performance.
func RunSelection(t *testing.T, factory Factory) {
	t.Run("optional_presence_boolean_template_numeric_order", func(t *testing.T) { selectionOptionalPresence(t, factory) })
	t.Run("sealed_current_source_observations", func(t *testing.T) { selectionBoundaryControls(t, factory) })
	t.Run("atomic_typed_writes_and_fresh_committed_exists", func(t *testing.T) { selectionAtomicAndFresh(t, factory) })
	t.Run("existing_source_relation_differential", func(t *testing.T) { selectionSourceDifferential(t, factory) })
	t.Run("independent_parent_only_source_domain", func(t *testing.T) { selectionParentOnlyDomain(t, factory) })
	t.Run("versioned_scoped_grant_domain", func(t *testing.T) { selectionScopedGrantDomain(t, factory) })
	t.Run("existing_authorization_interpreter_differential", func(t *testing.T) { selectionAuthorizationDifferential(t, factory) })
	t.Run("unique_membership_pages_groups_and_cap", func(t *testing.T) { selectionMembership(t, factory) })
	t.Run("presence_case_and_existing_order", func(t *testing.T) { selectionOrder(t, factory) })
	t.Run("historical_content_current_authority", func(t *testing.T) { selectionHistory(t, factory) })
	t.Run("staged_final_overlay_digest_and_rollback", func(t *testing.T) { selectionStaging(t, factory) })
	t.Run("complete_absence_unavailable_and_owned_versions", func(t *testing.T) { selectionCoverage(t, factory) })
	t.Run("summary_and_group_cap_boundaries", func(t *testing.T) { selectionSummaryCaps(t, factory) })
}

type selectionImage struct {
	bead          driver.SelectionBead
	body          []byte
	deleted, omit bool
	parentOnly    bool
	links         []driver.SourceLink
}
type selectionFactWrite struct {
	table   driver.DerivedTable
	key     string
	kind    driver.SelectionFactKind
	rows    []driver.SelectionFact
	deleted bool
	source  *driver.SelectionSource
}

func selectionID(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }
func selectionImageOf(id, typ, project, title string, priority int64) selectionImage {
	b := driver.SelectionBead{Ref: selectionID(id), Type: typ, Title: title, Priority: priority}
	if project != "<absent>" {
		b.Project = &project
	}
	data, _ := json.Marshal(map[string]any{"id": id, "type": typ, "project": b.Project, "fields": map[string]any{"title": title, "priority": priority}})
	return selectionImage{bead: b, body: data}
}
func selectionProjectionKey(ref model.Ref) driver.DerivedRowKey {
	return driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "selection-fixture:" + ref.ID}
}
func selectionEncode(k driver.LockKey) string { s, _ := driver.EncodeLockKey(k); return s }

// Staging supplies trusted typed fixture metadata with exact source bytes.
// All writes still use the real complete lock census, Head, bumps and Commit.
func selectionStage(t *testing.T, ctx context.Context, s driver.Store, l driver.ActiveLease, images []selectionImage, facts []selectionFactWrite) (driver.WriteTx, driver.ChangesetHeader) {
	t.Helper()
	m, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	keys := map[string]driver.LockKey{}
	add := func(k driver.LockKey) { keys[selectionEncode(k)] = k }
	sentinels := map[driver.Sentinel]bool{}
	if len(images) != 0 {
		sentinels[driver.SentinelGraphBlocking] = true
	}
	for _, im := range images {
		r := im.bead.Ref
		add(driver.LockKey{Kind: driver.LockRecord, Ref: &r})
		k := selectionProjectionKey(r)
		add(driver.LockKey{Kind: driver.LockDerived, Derived: &k})
		b, err := driver.NewBeadSentinel(r.ID)
		if err != nil {
			t.Fatal(err)
		}
		sentinels[b] = true
	}
	for _, f := range facts {
		k := driver.DerivedRowKey{Table: f.table, Key: f.key}
		add(driver.LockKey{Kind: driver.LockDerived, Derived: &k})
		if f.source != nil {
			add(f.source.Key)
		}
		var raw string
		switch f.table {
		case driver.DerivedGrant:
			raw = "grant/selection"
		case driver.DerivedAttribute, driver.DerivedAncestorClosure, driver.DerivedCutClosure:
			raw = "closure/selection"
		case driver.DerivedPlugins:
			raw = "plugins"
		case driver.DerivedOntology:
			raw = "ontology"
		case driver.DerivedReadyState:
			raw = "ready/selection/bead"
		}
		if raw != "" {
			sentinel, err := driver.NewSentinel(raw)
			if err != nil {
				t.Fatal(err)
			}
			sentinels[sentinel] = true
		}
	}
	for sentinel := range sentinels {
		add(driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	}
	ordered := make([]string, 0, len(keys))
	for id := range keys {
		ordered = append(ordered, id)
	}
	sort.Strings(ordered)
	requests := make([]driver.LockRequest, 0, len(keys))
	header := driver.ChangesetHeader{Incarnation: m.Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "selection-conformance"}}
	for _, id := range ordered {
		requests = append(requests, driver.LockRequest{Key: keys[id], Mode: driver.LockUpdate})
		header.LockKeys = append(header.LockKeys, keys[id])
	}
	tx, err := s.Write(ctx, l)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = tx.Close() })
	observed, err := tx.Lock(ctx, requests)
	if err != nil {
		t.Fatal(err)
	}
	states := map[string]driver.LockState{}
	for _, v := range observed {
		states[selectionEncode(v.Key)] = v
	}
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	for _, im := range images {
		r := im.bead.Ref
		key := driver.LockKey{Kind: driver.LockRecord, Ref: &r}
		prev := states[selectionEncode(key)]
		op := model.OpCreate
		if prev.Present {
			op = model.OpPatch
		}
		if im.deleted {
			op = model.OpDelete
			err = tx.Delete(ctx, r)
		} else {
			if im.parentOnly {
				parentWriter, ok := tx.(driver.HierarchySourceWriter)
				if !ok {
					t.Fatal("selection fixture requires real typed parent publication")
				}
				err = parentWriter.PutHierarchyRecord(ctx, driver.Record{Ref: r, Kind: "bead", Body: im.body}, im.bead.Parent)
			} else {
				writer, ok := tx.(driver.GraphSourceWriter)
				if !ok {
					t.Fatal("selection fixture requires real typed graph publication")
				}
				err = writer.PutGraphRecord(ctx, driver.Record{Ref: r, Kind: "bead", Body: im.body}, im.bead.Parent, im.links)
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		header.Changes = append(header.Changes, model.ChangeRecord{Ref: r, Op: op, FromRevision: prev.Revision})
	}
	var bumps []driver.Sentinel
	for b := range sentinels {
		bumps = append(bumps, b)
	}
	if err = tx.BumpSentinels(ctx, bumps); err != nil {
		t.Fatal(err)
	}
	for _, im := range images {
		k := selectionProjectionKey(im.bead.Ref)
		if im.deleted {
			if err = tx.PutDerived(ctx, k.Table, []driver.DerivedRow{{Key: k.Key}}, nil); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if im.omit {
			continue
		}
		body, _ := json.Marshal(im.bead)
		r := im.bead.Ref
		sk := driver.LockKey{Kind: driver.LockRecord, Ref: &r}
		row := driver.DerivedRow{Key: k.Key, Value: body, Index: &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: k, Incarnation: m.Incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(body), Source: driver.SelectionSource{Key: sk, Revision: states[selectionEncode(sk)].Revision, Present: true, Digest: driver.SelectionDigest(im.body)}, Bead: &im.bead}}
		if err = tx.(driver.SelectionIndexWriter).PutSelectionDerived(ctx, k.Table, nil, []driver.DerivedRow{row}); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range facts {
		k := driver.DerivedRowKey{Table: f.table, Key: f.key}
		if f.deleted {
			if err = tx.PutDerived(ctx, k.Table, []driver.DerivedRow{{Key: k.Key}}, nil); err != nil {
				t.Fatal(err)
			}
			continue
		}
		set := driver.SelectionFacts{Kind: f.kind, Rows: f.rows}
		data, _ := json.Marshal(set)
		sk := driver.LockKey{Kind: driver.LockDerived, Derived: &k}
		source := driver.SelectionSource{Key: sk, Revision: states[selectionEncode(sk)].Revision, Present: true, Digest: driver.SelectionDigest(data)}
		if f.source != nil {
			source = *f.source
			source.Revision = states[selectionEncode(source.Key)].Revision
		}
		row := driver.DerivedRow{Key: k.Key, Value: data, Index: &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: k, Incarnation: m.Incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(data), Source: source, Facts: &set}}
		if err = tx.(driver.SelectionIndexWriter).PutSelectionDerived(ctx, k.Table, nil, []driver.DerivedRow{row}); err != nil {
			t.Fatal(err)
		}
	}
	return tx, header
}
func selectionSeed(t *testing.T, ctx context.Context, s driver.Store, l driver.ActiveLease, images []selectionImage, facts []selectionFactWrite) model.Revision {
	t.Helper()
	tx, h := selectionStage(t, ctx, s, l, images, facts)
	e, err := tx.Commit(ctx, h)
	if err != nil {
		t.Fatal(err)
	}
	return e.Revision
}

// Parent-only fixtures intentionally publish no outgoing-link declaration.
// They exercise the independent hierarchy coverage domain instead of making
// the graph writer's complete Links envelope a hidden prerequisite.
func selectionSeedParentOnly(t *testing.T, ctx context.Context, s driver.Store, l driver.ActiveLease, images []selectionImage, facts []selectionFactWrite) model.Revision {
	t.Helper()
	for i := range images {
		images[i].parentOnly = true
	}
	return selectionSeed(t, ctx, s, l, images, facts)
}
func selectionOpen(t *testing.T, ctx context.Context, s driver.Store) (driver.Reader, driver.SelectionPlan) {
	t.Helper()
	m, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := s.Read(ctx, driver.View{Incarnation: m.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r, selectionPlan(r.Snapshot())
}
func selectionPlan(v driver.View) driver.SelectionPlan {
	p := driver.SelectionPlan{Incarnation: v.Incarnation, Branch: v.Branch, ContentRevision: *v.At, AuthorityRevision: *v.At, Membership: driver.SelectionExpr{Op: driver.SelectionTrue}, Authorization: driver.SelectionExpr{Op: driver.SelectionTrue}, Mode: driver.SelectionPageMode, Page: &driver.SelectionPage{Limit: 100}}
	control := model.RefByKey("selection-conformance-control")
	p.Boundary, _ = driver.PrepareSelectionBoundary(driver.SelectionBoundarySpec{Version: 1, PreparedBinding: driver.SelectionDigest([]byte("selection-conformance/compiled-fixture-v1")), Incarnation: p.Incarnation, Branch: p.Branch, AuthorityRevision: p.AuthorityRevision, Chain: driver.SelectionChainCompiled, Authorization: p.Authorization, Host: driver.SelectionHostAbsent, Sources: []driver.SelectionSource{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &control}, Revision: model.Natural64(0)}}})
	return p
}

// These fixture helpers act as trusted compilation and seal the deliberately
// chosen test policy. Boundary substitution tests call Select directly instead.
func selectionSeal(p driver.SelectionPlan) driver.SelectionPlan {
	if p.Boundary == nil {
		return p
	}
	spec, err := p.Boundary.Description()
	if err != nil {
		return p
	}
	spec.Authorization = p.Authorization
	if boundary, err := driver.PrepareSelectionBoundary(spec); err == nil {
		p.Boundary = boundary
	}
	return p
}
func selectionSelect(t *testing.T, ctx context.Context, r any, p driver.SelectionPlan) driver.SelectionResult {
	t.Helper()
	capability, ok := r.(driver.SelectionReader)
	if !ok {
		t.Fatal("missing SelectionReader")
	}
	got, err := capability.Select(ctx, selectionSeal(p))
	if err != nil {
		t.Fatal(err)
	}
	return got
}
func selectionWant(t *testing.T, got driver.SelectionResult, total uint64, ids ...string) {
	t.Helper()
	var actual []string
	for _, k := range got.Keys {
		actual = append(actual, k.Ref.ID)
	}
	if got.Total != total || !reflect.DeepEqual(actual, ids) {
		t.Fatalf("keys=%v total=%d want=%v/%d", actual, got.Total, ids, total)
	}
}
func selectionCompare(column driver.SelectionColumn, op string, v driver.SelectionValue) driver.SelectionExpr {
	return driver.SelectionExpr{Op: driver.SelectionCompareOp, Compare: &driver.SelectionCompare{Column: column, Op: op, Value: &v}}
}
func selectionExists(table driver.DerivedTable, key string, kind driver.SelectionFactKind, tests ...driver.SelectionFactTest) driver.SelectionExpr {
	return driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: driver.DerivedRowKey{Table: table, Key: key}, Kind: kind, Tests: tests}}
}
func selectionZeroError(t *testing.T, ctx context.Context, r any, p driver.SelectionPlan) {
	t.Helper()
	got, err := r.(driver.SelectionReader).Select(ctx, selectionSeal(p))
	if err == nil || !reflect.DeepEqual(got, driver.SelectionResult{}) {
		t.Fatalf("expected refusal with zero result, got=%+v error=%v", got, err)
	}
}

func selectionMembership(t *testing.T, factory Factory) {
	ctx, s, l, _ := setup(t, factory)
	images := []selectionImage{selectionImageOf("a", "bug", "P", "Fix_%", 2), selectionImageOf("b", "task", "P", "fix_%", 10), selectionImageOf("c", "bug", "Q", "Fix_%", 2), selectionImageOf("d", "task", "<absent>", "Fix", 1), selectionImageOf("e", "task", "", "Fix", 2)}
	grant := driver.SelectionFact{Kind: driver.SelectionGrantFact, Subject: "user:u", Scope: "P", Role: "read"}
	a := selectionID("a")
	b := selectionID("b")
	facts := []selectionFactWrite{{table: driver.DerivedGrant, key: "permit", kind: driver.SelectionGrantFact, rows: []driver.SelectionFact{grant, grant}}, {table: driver.DerivedGrant, key: "forbid", kind: driver.SelectionGrantFact}, {table: driver.DerivedAttribute, key: "labels", kind: driver.SelectionLabelFact, rows: []driver.SelectionFact{{Kind: driver.SelectionLabelFact, Ref: &a, Name: "x"}, {Kind: driver.SelectionLabelFact, Ref: &a, Name: "x"}, {Kind: driver.SelectionLabelFact, Ref: &b, Name: "x"}}}}
	selectionSeed(t, ctx, s, l, images, facts)
	r, p := selectionOpen(t, ctx, s)
	p.Membership = driver.SelectionExpr{Op: driver.SelectionRefs, Refs: []model.Ref{a, a, b, selectionID("c"), selectionID("d"), selectionID("e")}}
	p.Page.Limit = 2
	p.GroupBy = driver.SelectionGroupType
	first := selectionSelect(t, ctx, r, p)
	selectionWant(t, first, 5, "a", "b")
	if !reflect.DeepEqual(first.Groups, []driver.SelectionTypeGroup{{Type: "bug", Count: 2, Elided: 1}, {Type: "task", Count: 3, Elided: 2}}) || !first.More || first.Next == nil {
		t.Fatalf("first groups/page=%+v", first)
	}
	p.Page.After = first.Next
	second := selectionSelect(t, ctx, r, p)
	selectionWant(t, second, 5, "c", "d")
	if !reflect.DeepEqual(second.Groups, []driver.SelectionTypeGroup{{Type: "bug", Count: 2}, {Type: "task", Count: 3, Elided: 1}}) {
		t.Fatalf("suffix groups=%+v", second.Groups)
	}
	p.Page.After = second.Next
	third := selectionSelect(t, ctx, r, p)
	selectionWant(t, third, 5, "e")
	if third.More || !reflect.DeepEqual(third.Groups, []driver.SelectionTypeGroup{{Type: "bug", Count: 2}, {Type: "task", Count: 3}}) {
		t.Fatalf("terminal page must retain the absent bug group's full count: %+v", third)
	}
	p.Page = &driver.SelectionPage{Offset: 3, Limit: 1}
	offset := selectionSelect(t, ctx, r, p)
	selectionWant(t, offset, 5, "d")
	if !reflect.DeepEqual(offset.Groups, []driver.SelectionTypeGroup{{Type: "bug", Count: 2}, {Type: "task", Count: 3, Elided: 1}}) {
		t.Fatalf("offset must use the global suffix: %+v", offset)
	}
	p.Page = &driver.SelectionPage{Offset: 4, Limit: 0}
	probe := selectionSelect(t, ctx, r, p)
	if len(probe.Keys) != 0 || !probe.More || probe.Total != 5 {
		t.Fatalf("probe=%+v", probe)
	}
	p.Page = &driver.SelectionPage{Limit: 10}
	p.Cap = 2
	selectionZeroError(t, ctx, r, p)
	p.Authorization = driver.SelectionExpr{Op: driver.SelectionAll, Args: []driver.SelectionExpr{selectionExists(driver.DerivedGrant, "permit", driver.SelectionGrantFact, driver.SelectionFactTest{Column: driver.SelectionFactScope, Candidate: driver.SelectionProject}), {Op: driver.SelectionNot, Args: []driver.SelectionExpr{selectionExists(driver.DerivedGrant, "forbid", driver.SelectionGrantFact)}}}}
	p.Membership = selectionExists(driver.DerivedAttribute, "labels", driver.SelectionLabelFact, driver.SelectionFactTest{Column: driver.SelectionFactRef, Candidate: driver.SelectionID})
	selectionWant(t, selectionSelect(t, ctx, r, p), 2, "a", "b")
	p.Mode = driver.SelectionCountMode
	p.Page = nil
	p.GroupBy = ""
	got := selectionSelect(t, ctx, r, p)
	if got.Total != 2 || len(got.Keys) != 0 {
		t.Fatalf("unique authorized count=%+v", got)
	}
	p.Mode = driver.SelectionTypeSummary
	p.GroupBy = driver.SelectionGroupType
	p.Cap = 0
	got = selectionSelect(t, ctx, r, p)
	if !reflect.DeepEqual(got.Groups, []driver.SelectionTypeGroup{{Type: "bug", Count: 1}, {Type: "task", Count: 1}}) {
		t.Fatalf("summary=%+v", got)
	}
	// No callback/second hydration is possible: only typed keys are returned.
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	selectionZeroError(t, canceled, r, p)
}

func selectionOrder(t *testing.T, factory Factory) {
	ctx, s, l, _ := setup(t, factory)
	a, b := selectionImageOf("a", "bug", "<absent>", "Fix_%", 10), selectionImageOf("b", "task", "", "fix_%", 2)
	selectionSeed(t, ctx, s, l, []selectionImage{a}, []selectionFactWrite{{table: driver.DerivedGrant, key: "clock", kind: driver.SelectionGrantFact}})
	selectionSeed(t, ctx, s, l, []selectionImage{a}, nil)
	for i := 3; i <= 10; i++ {
		selectionSeed(t, ctx, s, l, []selectionImage{b}, nil)
	}
	r, p := selectionOpen(t, ctx, s)
	p.Order = []driver.SelectionOrder{{Column: driver.SelectionRevision}}
	selectionWant(t, selectionSelect(t, ctx, r, p), 2, "b", "a") // revision string10 precedes2
	a.bead.Title = "other"
	a.body = []byte(`{"title":"other"}`)
	selectionSeed(t, ctx, s, l, []selectionImage{a}, nil) // a is11; b is10
	r2, p2 := selectionOpen(t, ctx, s)
	p2.Order = []driver.SelectionOrder{{Column: driver.SelectionPriority}}
	selectionWant(t, selectionSelect(t, ctx, r2, p2), 2, "b", "a")
	p2.Order = []driver.SelectionOrder{{Column: driver.SelectionTitle, Desc: true}}
	selectionWant(t, selectionSelect(t, ctx, r2, p2), 2, "a", "b")
	p.Membership = selectionCompare(driver.SelectionTitle, "contains", driver.SelectionString("_%"))
	p.Order = nil
	selectionWant(t, selectionSelect(t, ctx, r, p), 2, "a", "b")
	p.Membership = selectionCompare(driver.SelectionTitle, "contains", driver.SelectionString("Fix"))
	selectionWant(t, selectionSelect(t, ctx, r, p), 1, "a")
	p.Membership = driver.SelectionExpr{Op: driver.SelectionCompareOp, Compare: &driver.SelectionCompare{Column: driver.SelectionProject, Op: "absent"}}
	selectionWant(t, selectionSelect(t, ctx, r, p), 1, "a")
	p.Membership = selectionCompare(driver.SelectionProject, "eq", driver.SelectionString(""))
	selectionWant(t, selectionSelect(t, ctx, r, p), 1, "b")
	// A stale reader stays pinned; it cannot claim a later authority binding.
	p.AuthorityRevision = p2.AuthorityRevision
	selectionZeroError(t, ctx, r, p)
}

func selectionHistory(t *testing.T, factory Factory) {
	ctx, s, l, _ := setup(t, factory)
	a := selectionImageOf("a", "bug", "P", "old", 1)
	permit := func(scope string) selectionFactWrite {
		return selectionFactWrite{table: driver.DerivedGrant, key: "permit", kind: driver.SelectionGrantFact, rows: []driver.SelectionFact{{Kind: driver.SelectionGrantFact, Subject: "u", Scope: scope, Role: "read"}}}
	}
	r1 := selectionSeed(t, ctx, s, l, []selectionImage{a}, []selectionFactWrite{permit("P")})
	oldReader, oldPlan := selectionOpen(t, ctx, s)
	a = selectionImageOf("a", "task", "Q", "new", 2)
	selectionSeed(t, ctx, s, l, []selectionImage{a}, []selectionFactWrite{permit("Q")})
	r, p := selectionOpen(t, ctx, s)
	p.ContentRevision = r1
	p.Membership = selectionCompare(driver.SelectionTitle, "eq", driver.SelectionString("old"))
	p.Authorization = selectionExists(driver.DerivedGrant, "permit", driver.SelectionGrantFact, driver.SelectionFactTest{Column: driver.SelectionFactScope, Candidate: driver.SelectionProject})
	selectionWant(t, selectionSelect(t, ctx, r, p), 1, "a")
	// Old facts must not be joined to current projectQ.
	p.Authorization = selectionExists(driver.DerivedGrant, "permit", driver.SelectionGrantFact, driver.SelectionFactTest{Column: driver.SelectionFactScope, Value: selectionValuePointer(driver.SelectionString("P"))})
	selectionWant(t, selectionSelect(t, ctx, r, p), 0)
	selectionWant(t, selectionSelect(t, ctx, oldReader, oldPlan), 1, "a")
	a.deleted = true
	selectionSeed(t, ctx, s, l, []selectionImage{a}, nil)
	r3, p3 := selectionOpen(t, ctx, s)
	p3.ContentRevision = r1
	selectionWant(t, selectionSelect(t, ctx, r3, p3), 1, "a") // no implicit current-existence policy
	p3.Authorization = driver.SelectionExpr{Op: driver.SelectionResourceExists}
	selectionWant(t, selectionSelect(t, ctx, r3, p3), 0)
	a = selectionImageOf("a", "task", "P", "recreated", 3)
	selectionSeed(t, ctx, s, l, []selectionImage{a}, []selectionFactWrite{permit("P")})
	r4, p4 := selectionOpen(t, ctx, s)
	p4.ContentRevision = r1
	p4.Membership = selectionCompare(driver.SelectionTitle, "eq", driver.SelectionString("old"))
	p4.Authorization = selectionCompare(driver.SelectionTitle, "eq", driver.SelectionString("recreated"))
	selectionWant(t, selectionSelect(t, ctx, r4, p4), 1, "a")
}
func selectionValuePointer(v driver.SelectionValue) *driver.SelectionValue { return &v }

func selectionStaging(t *testing.T, factory Factory) {
	ctx, s, l, _ := setup(t, factory)
	a := selectionImageOf("a", "bug", "P", "old", 1)
	b := selectionImageOf("b", "task", "P", "keep", 2)
	r1 := selectionSeed(t, ctx, s, l, []selectionImage{a, b}, nil)
	selectionSeed(t, ctx, s, l, nil, []selectionFactWrite{{table: driver.DerivedGrant, key: "permit", kind: driver.SelectionGrantFact, rows: []driver.SelectionFact{{Kind: driver.SelectionGrantFact, Subject: "u", Scope: "P", Role: "read"}}}})
	a = selectionImageOf("a", "bug", "Q", "new", 1)
	a.omit = true
	tx, header := selectionStage(t, ctx, s, l, []selectionImage{a}, nil)
	p := selectionPlan(tx.Snapshot())
	selectionZeroError(t, ctx, tx, p) // same revision token, changed actual bytes
	rows, err := tx.DerivedRows(ctx, driver.DerivedSortKey, selectionProjectionKey(a.bead.Ref).Key)
	if err != nil || len(rows) != 1 {
		t.Fatalf("old row: %v %d", err, len(rows))
	}
	row := rows[0]
	row.Index.Bead.Title = "new"
	q := "Q"
	row.Index.Bead.Project = &q
	if err = tx.PutDerived(ctx, driver.DerivedSortKey, nil, []driver.DerivedRow{row}); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("revision-equal stale source annotation accepted: %v", err)
	}
	row.Index.Source.Digest = driver.SelectionDigest(a.body)
	row.Value, _ = json.Marshal(row.Index.Bead)
	row.Index.ValueDigest = driver.SelectionDigest(row.Value)
	if err = tx.(driver.SelectionIndexWriter).PutSelectionDerived(ctx, driver.DerivedSortKey, nil, []driver.DerivedRow{row}); err != nil {
		t.Fatal(err)
	}
	p.Authorization = selectionCompare(driver.SelectionTitle, "eq", driver.SelectionString("new"))
	selectionWant(t, selectionSelect(t, ctx, tx, p), 1, "a")
	p.ContentRevision = r1
	p.Membership = selectionCompare(driver.SelectionTitle, "eq", driver.SelectionString("old"))
	selectionWant(t, selectionSelect(t, ctx, tx, p), 1, "a")
	// Source changed after annotation staging: Commit must recheck final bytes.
	if err = tx.(driver.GraphSourceWriter).PutGraphRecord(ctx, driver.Record{Ref: a.bead.Ref, Kind: "bead", Body: []byte(`{"title":"after"}`)}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Commit(ctx, header); !errors.Is(err, driver.ErrUnsupported) {
		t.Fatalf("stale final source committed: %v", err)
	}
	_ = tx.Rollback(ctx)
	r, base := selectionOpen(t, ctx, s)
	selectionWant(t, selectionSelect(t, ctx, r, base), 2, "a", "b")
	// A separate final overlay merges replacement, create and delete once.
	a.omit = false
	c := selectionImageOf("c", "bug", "P", "created", 1)
	b.deleted = true
	tx2, _ := selectionStage(t, ctx, s, l, []selectionImage{a, b, c}, []selectionFactWrite{{table: driver.DerivedGrant, key: "permit", kind: driver.SelectionGrantFact}})
	txp := selectionPlan(tx2.Snapshot())
	selectionWant(t, selectionSelect(t, ctx, tx2, txp), 2, "a", "c")
	txp.Authorization = selectionExists(driver.DerivedGrant, "permit", driver.SelectionGrantFact, driver.SelectionFactTest{Column: driver.SelectionFactScope, Candidate: driver.SelectionProject})
	selectionWant(t, selectionSelect(t, ctx, tx2, txp), 0)
	_ = tx2.Rollback(ctx)
	r2, base2 := selectionOpen(t, ctx, s)
	selectionWant(t, selectionSelect(t, ctx, r2, base2), 2, "a", "b")
	base2.Authorization = txp.Authorization
	selectionWant(t, selectionSelect(t, ctx, r2, base2), 2, "a", "b")
}

func selectionCoverage(t *testing.T, factory Factory) {
	ctx, s, l, m := setup(t, factory)
	a := selectionImageOf("a", "bug", "P", "title", 1)
	missingRef := model.RefByKey("missing-authority")
	missing := driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &missingRef}, Present: false, Revision: model.Natural64(0)}
	facts := selectionFactWrite{table: driver.DerivedGrant, key: "forbid", kind: driver.SelectionGrantFact, source: &missing}
	rev := selectionSeed(t, ctx, s, l, []selectionImage{a}, []selectionFactWrite{facts, {table: driver.DerivedAttribute, key: "labels", kind: driver.SelectionLabelFact}})
	r, p := selectionOpen(t, ctx, s)
	p.Authorization = driver.SelectionExpr{Op: driver.SelectionNot, Args: []driver.SelectionExpr{selectionExists(driver.DerivedGrant, "forbid", driver.SelectionGrantFact)}}
	selectionWant(t, selectionSelect(t, ctx, r, p), 1, "a") // proven source miss0 + explicitly complete empty facts
	p.Authorization = driver.SelectionExpr{Op: driver.SelectionNot, Args: []driver.SelectionExpr{selectionExists(driver.DerivedAttribute, "labels", driver.SelectionTeamFact)}}
	selectionZeroError(t, ctx, r, p) // same family, wrong EMPTY completeness domain
	p.Authorization = driver.SelectionExpr{Op: driver.SelectionAll, Args: []driver.SelectionExpr{{Op: driver.SelectionFalse}, selectionExists(driver.DerivedGrant, "absent-coverage", driver.SelectionGrantFact)}}
	p.Membership = driver.SelectionExpr{Op: driver.SelectionFalse}
	p.Page.Limit = 0
	selectionZeroError(t, ctx, r, p) // no candidates/short circuit/probe cannot hide missing coverage
	// Observe, prefix readers and retained snapshots all own annotations.
	k := selectionProjectionKey(a.bead.Ref)
	key := driver.LockKey{Kind: driver.LockDerived, Derived: &k}
	for _, prefixes := range []bool{false, true} {
		var rows []driver.DerivedRow
		var err error
		if prefixes {
			rows, err = r.(driver.DerivedPrefixReader).DerivedRowsForPrefixes(ctx, k.Table, []string{k.Key})
		} else {
			rows, err = r.DerivedRows(ctx, k.Table, k.Key)
		}
		if err != nil || len(rows) != 1 || rows[0].Index == nil {
			t.Fatalf("annotation read: %v %+v", err, rows)
		}
		rows[0].Index.Bead.Title = "corrupted"
		rows[0].Index.Source.Key.Ref.ID = "corrupted"
		rows[0].Value[0] = '!'
	}
	obs, err := r.Observe(ctx, []driver.LockKey{key})
	if err != nil {
		t.Fatal(err)
	}
	obs[0].Derived.Index.Bead.Title = "corrupted"
	again, err := r.DerivedRows(ctx, k.Table, k.Key)
	if err != nil || again[0].Index.Bead.Title != "title" || again[0].Index.Source.Key.Ref.ID != "a" {
		t.Fatalf("reader ownership lost: %v %+v", err, again)
	}
	// A source miss and its later tombstone are different observations. The
	// same complete empty set must be restaged against the actual tombstone.
	if _, err = Seed(ctx, s, l, driver.ChangesetHeader{}, []FixtureChange{{Ref: missingRef, Kind: "authority", Op: model.OpCreate, Body: []byte(`{}`)}}); err != nil {
		t.Fatal(err)
	}
	stale, sp := selectionOpen(t, ctx, s)
	sp.Authorization = driver.SelectionExpr{Op: driver.SelectionNot, Args: []driver.SelectionExpr{selectionExists(driver.DerivedGrant, "forbid", driver.SelectionGrantFact)}}
	selectionZeroError(t, ctx, stale, sp)
	if _, err = Seed(ctx, s, l, driver.ChangesetHeader{}, []FixtureChange{{Ref: missingRef, Kind: "authority", Op: model.OpDelete}}); err != nil {
		t.Fatal(err)
	}
	selectionSeed(t, ctx, s, l, nil, []selectionFactWrite{facts})
	tomb, tp := selectionOpen(t, ctx, s)
	tp.Authorization = sp.Authorization
	selectionWant(t, selectionSelect(t, ctx, tomb, tp), 1, "a")
	fr, err := tomb.DerivedRows(ctx, driver.DerivedGrant, "forbid")
	if err != nil || len(fr) != 1 || fr[0].Index.Source.Present || fr[0].Index.Source.Revision.IsZero() {
		t.Fatalf("tombstone binding=%+v %v", fr, err)
	}
	facts.deleted = true
	selectionSeed(t, ctx, s, l, nil, []selectionFactWrite{facts})
	current, cp := selectionOpen(t, ctx, s)
	cp.Authorization = driver.SelectionExpr{Op: driver.SelectionNot, Args: []driver.SelectionExpr{selectionExists(driver.DerivedGrant, "forbid", driver.SelectionGrantFact)}}
	selectionZeroError(t, ctx, current, cp)
	// The retained old reader still sees the exact old fact version.
	p = selectionPlan(r.Snapshot())
	p.Authorization = cp.Authorization
	selectionWant(t, selectionSelect(t, ctx, r, p), 1, "a")
	if rev.String() != model.Natural64(1).String() || m.Incarnation != p.Incarnation {
		t.Fatal("fixture identity changed")
	}
	// Untyped replacement removes the old annotation. It must not silently
	// retain a formerly compatible projection, including inside the transaction.
	tx, h := selectionStage(t, ctx, s, l, []selectionImage{a}, nil)
	staged, err := tx.DerivedRows(ctx, k.Table, k.Key)
	if err != nil || len(staged) != 1 {
		t.Fatalf("staged annotation: %v %+v", err, staged)
	}
	input := staged[0]
	if err = tx.(driver.SelectionIndexWriter).PutSelectionDerived(ctx, k.Table, nil, []driver.DerivedRow{input}); err != nil {
		t.Fatal(err)
	}
	input.Index.Bead.Title = "mutated input"
	input.Index.Source.Key.Ref.ID = "mutated input"
	input.Value[0] = '!'
	stagedPlan := selectionPlan(tx.Snapshot())
	stagedPlan.Membership = selectionCompare(driver.SelectionTitle, "eq", driver.SelectionString("title"))
	selectionWant(t, selectionSelect(t, ctx, tx, stagedPlan), 1, "a")
	if err = tx.PutDerived(ctx, k.Table, nil, []driver.DerivedRow{{Key: k.Key, Value: []byte("unannotated")}}); err != nil {
		t.Fatal(err)
	}
	selectionZeroError(t, ctx, tx, selectionPlan(tx.Snapshot()))
	if _, err = tx.Commit(ctx, h); err != nil {
		t.Fatal(err)
	}
	untyped, up := selectionOpen(t, ctx, s)
	selectionZeroError(t, ctx, untyped, up)
	selectionWant(t, selectionSelect(t, ctx, r, p), 1, "a")
}

func selectionSummaryCaps(t *testing.T, factory Factory) {
	ctx, s, l, _ := setup(t, factory)
	var images []selectionImage
	var refs []model.Ref
	for i := 0; i < 101; i++ {
		id := fmt.Sprintf("group-%03d", i)
		images = append(images, selectionImageOf(id, id, "P", "title", 1))
		if i < 100 {
			refs = append(refs, selectionID(id))
		}
	}
	selectionSeed(t, ctx, s, l, images, nil)
	r, p := selectionOpen(t, ctx, s)
	p.Mode = driver.SelectionTypeSummary
	p.GroupBy = driver.SelectionGroupType
	p.Page = nil
	got, err := r.(driver.SelectionReader).Select(ctx, selectionSeal(p))
	var cap driver.ErrSelectionCap
	if !errors.As(err, &cap) || !cap.Groups || !reflect.DeepEqual(got, driver.SelectionResult{}) {
		t.Fatalf("101 groups must refuse: %+v %v", got, err)
	}
	p.Membership = driver.SelectionExpr{Op: driver.SelectionRefs, Refs: refs}
	got = selectionSelect(t, ctx, r, p)
	if got.Total != 100 || len(got.Groups) != 100 || len(got.Keys) != 0 {
		t.Fatalf("100 group boundary=%+v", got)
	}
	p.Cap = 99
	selectionZeroError(t, ctx, r, p)
}

// RunSelectionRefusal proves unsupported SQL annotations cannot partly stage a
// call, and that ordinary existing derived rows remain usable afterwards.
func RunSelectionRefusal(t *testing.T, factory Factory) {
	ctx, s, l, _ := setup(t, factory)
	keys := []driver.DerivedRowKey{{Table: driver.DerivedSortKey, Key: "a"}, {Table: driver.DerivedSortKey, Key: "b"}, {Table: driver.DerivedSortKey, Key: "c"}}
	begin := func() (driver.WriteTx, driver.ChangesetHeader) {
		tx, err := s.Write(ctx, l)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = tx.Close() })
		var req []driver.LockRequest
		h := driver.ChangesetHeader{Incarnation: tx.Snapshot().Incarnation, Kind: model.ChangesetWrite, Principal: model.PrincipalRef{Kind: "system", ID: "selection-refusal"}}
		for _, k := range keys {
			k := k
			key := driver.LockKey{Kind: driver.LockDerived, Derived: &k}
			req = append(req, driver.LockRequest{Key: key, Mode: driver.LockUpdate})
			h.LockKeys = append(h.LockKeys, key)
		}
		if _, err = tx.Lock(ctx, req); err != nil {
			t.Fatal(err)
		}
		if _, err = tx.Head(ctx); err != nil {
			t.Fatal(err)
		}
		return tx, h
	}
	tx, h := begin()
	if err := tx.PutDerived(ctx, driver.DerivedSortKey, nil, []driver.DerivedRow{{Key: "a", Value: []byte("original")}}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, h); err != nil {
		t.Fatal(err)
	}
	tx2, _ := begin()
	for _, onDelete := range []bool{false, true} {
		del := []driver.DerivedRow{{Key: "a"}}
		add := []driver.DerivedRow{{Key: "b", Value: []byte("must not stage")}, {Key: "c", Index: &driver.SelectionIndex{}}}
		if onDelete {
			del[0].Index = &driver.SelectionIndex{}
			add = add[:1]
		}
		if err := tx2.PutDerived(ctx, driver.DerivedSortKey, del, add); !errors.Is(err, driver.ErrUnsupported) {
			t.Fatalf("annotated write accepted: %v", err)
		}
		got, err := tx2.DerivedRows(ctx, driver.DerivedSortKey, "")
		if err != nil || len(got) != 1 || got[0].Key != "a" || string(got[0].Value) != "original" {
			t.Fatalf("partial annotated write: %+v %v", got, err)
		}
	}
	for _, reader := range []driver.Reader{tx2} {
		got, err := reader.(driver.SelectionReader).Select(ctx, selectionPlan(reader.Snapshot()))
		if !errors.Is(err, driver.ErrUnsupported) || !reflect.DeepEqual(got, driver.SelectionResult{}) {
			t.Fatalf("false SQL selection success: %+v %v", got, err)
		}
	}
	if err := tx2.PutDerived(ctx, driver.DerivedSortKey, nil, []driver.DerivedRow{{Key: "b", Value: []byte("ordinary")}}); err != nil {
		t.Fatal(err)
	}
	ordinary, err := tx2.DerivedRows(ctx, driver.DerivedSortKey, "")
	if err != nil || len(ordinary) != 2 || ordinary[0].Key != "a" || ordinary[1].Key != "b" || string(ordinary[1].Value) != "ordinary" {
		t.Fatalf("ordinary staged write after refusal: %+v %v", ordinary, err)
	}
	_ = tx2.Rollback(ctx)
	r, p := selectionOpen(t, ctx, s)
	got, err := r.(driver.SelectionReader).Select(ctx, selectionSeal(p))
	if !errors.Is(err, driver.ErrUnsupported) || !reflect.DeepEqual(got, driver.SelectionResult{}) {
		t.Fatalf("SQL reader selection: %+v %v", got, err)
	}
	rows, err := r.DerivedRows(ctx, driver.DerivedSortKey, "")
	if err != nil || len(rows) != 1 || rows[0].Key != "a" {
		t.Fatalf("rollback changed ordinary rows: %v %+v", err, rows)
	}
}
