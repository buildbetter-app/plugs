package conformance

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These are typed storage-contract fixtures, not the actual W4 producer or a
// production fixed-policy compiler. The producer-to-W2-policy differential is
// a separate integration obligation. This scalar domain does not represent
// TeamInvolvement or authorize dropping its same-team correlation from an OR.
func selectionScopedGrantDomain(t *testing.T, factory Factory) {
	ctx, s, lease, _ := setup(t, factory)
	name := "web/api"
	project := selectionID("project-registry-id")
	otherName := "other"
	otherProject := selectionID("other-registry-id")
	authority := selectionID("fixed-authority")
	meta, err := s.Meta(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed-installation", Authority: authority}
	makeGrant := func(subject authz.SubjectRef, kind authz.ScopeKind, ref model.Ref, projectName *string, role string) driver.SelectionFact {
		t.Helper()
		opaque, err := authz.ScopeComponent(ns, authz.Scope{Kind: kind, Ref: ref})
		if err != nil {
			t.Fatal(err)
		}
		return driver.SelectionFact{Kind: driver.SelectionScopedGrantFact, Subject: authz.Encode(subject.Kind, subject.ID), Scope: opaque, Role: role, ScopeKind: driver.SelectionScopeKind(kind), ScopeRef: &ref, ProjectName: projectName}
	}
	user := authz.SubjectRef{Kind: "user", ID: "same-text"}
	team := authz.SubjectRef{Kind: "team", ID: "same-text"}
	rows := []driver.SelectionFact{
		makeGrant(user, authz.ProjectScope, project, &name, "involved"),
		makeGrant(team, authz.ProjectScope, otherProject, &otherName, "involved"),
		makeGrant(user, authz.AuthorityScope, authority, nil, "admin"),
		makeGrant(user, authz.TeamScope, selectionID("same-text"), nil, "member"),
	}
	images := []selectionImage{selectionImageOf("a", "issue", name, "", 0), selectionImageOf("b", "issue", project.ID, "", 0), selectionImageOf("c", "issue", otherName, "", 0), selectionImageOf("d", "issue", "", "", 0), selectionImageOf("e", "issue", "<absent>", "", 0)}
	write := selectionFactWrite{table: driver.DerivedGrant, key: "versioned-grants", kind: driver.SelectionScopedGrantFact, rows: rows}
	selectionSeed(t, ctx, s, lease, images, []selectionFactWrite{write})
	r, p := selectionOpen(t, ctx, s)
	test := func(column driver.SelectionFactColumn, value string) driver.SelectionFactTest {
		return driver.SelectionFactTest{Column: column, Value: selectionValuePointer(driver.SelectionString(value))}
	}
	direct := selectionExists(driver.DerivedGrant, write.key, driver.SelectionScopedGrantFact,
		test(driver.SelectionFactSubject, authz.Encode(user.Kind, user.ID)),
		test(driver.SelectionFactRole, "involved"),
		test(driver.SelectionFactScopeKind, string(driver.SelectionProjectScope)),
		driver.SelectionFactTest{Column: driver.SelectionFactProjectName, Candidate: driver.SelectionProject})
	p.Authorization = direct
	selectionWant(t, selectionSelect(t, ctx, r, p), 1, "a") // valid NAME != registry Ref.ID
	// Equal text in a team's ID cannot satisfy an encoded user-subject test.
	p.Membership = driver.SelectionExpr{Op: driver.SelectionRefs, Refs: []model.Ref{selectionID("c")}}
	selectionWant(t, selectionSelect(t, ctx, r, p), 0)
	p.Membership = driver.SelectionExpr{Op: driver.SelectionTrue}
	p.Authorization = selectionExists(driver.DerivedGrant, write.key, driver.SelectionScopedGrantFact,
		test(driver.SelectionFactSubject, authz.Encode(user.Kind, user.ID)),
		test(driver.SelectionFactRole, "admin"),
		test(driver.SelectionFactScope, rows[2].Scope))
	selectionWant(t, selectionSelect(t, ctx, r, p), 5, "a", "b", "c", "d", "e")
	// This is one literal authority-grant leaf, not an authority override of
	// any other fixed-policy restriction or a full host admission certificate.
	p.Authorization = selectionExists(driver.DerivedGrant, write.key, driver.SelectionScopedGrantFact,
		driver.SelectionFactTest{Column: driver.SelectionFactProjectName, Candidate: driver.SelectionProject})
	selectionWant(t, selectionSelect(t, ctx, r, p), 2, "a", "c") // nil metadata never matches present-empty or absent candidate Project
	write.rows = rows[1:]
	selectionSeed(t, ctx, s, lease, nil, []selectionFactWrite{write})
	current, cp := selectionOpen(t, ctx, s)
	cp.Authorization = direct
	selectionWant(t, selectionSelect(t, ctx, current, cp), 0)
	cp.ContentRevision = p.ContentRevision
	selectionWant(t, selectionSelect(t, ctx, current, cp), 0) // historical content still uses the current scoped grant source
	p.Authorization = direct
	selectionWant(t, selectionSelect(t, ctx, r, p), 1, "a") // retained old snapshot owns its source
	write.rows = nil
	selectionSeed(t, ctx, s, lease, nil, []selectionFactWrite{write})
	empty, ep := selectionOpen(t, ctx, s)
	ep.Authorization = driver.SelectionExpr{Op: driver.SelectionNot, Args: []driver.SelectionExpr{direct}}
	selectionWant(t, selectionSelect(t, ctx, empty, ep), 5, "a", "b", "c", "d", "e")
	// Even a valid complete empty GENERIC grant set does not cover this new
	// domain. Neither NOT, false membership nor a zero-row probe may hide it.
	write.kind = driver.SelectionGrantFact
	selectionSeed(t, ctx, s, lease, nil, []selectionFactWrite{write})
	oldKind, op := selectionOpen(t, ctx, s)
	for _, expr := range []driver.SelectionExpr{
		direct,
		{Op: driver.SelectionNot, Args: []driver.SelectionExpr{direct}},
		{Op: driver.SelectionAll, Args: []driver.SelectionExpr{{Op: driver.SelectionFalse}, direct}},
		{Op: driver.SelectionAny, Args: []driver.SelectionExpr{{Op: driver.SelectionTrue}, direct}},
	} {
		for arm := 0; arm < 3; arm++ {
			for _, mode := range []driver.SelectionMode{driver.SelectionPageMode, driver.SelectionCountMode, driver.SelectionTypeSummary} {
				plan := op
				plan.Mode, plan.Page = mode, nil
				if mode == driver.SelectionPageMode {
					plan.Page = &driver.SelectionPage{Limit: 0}
				}
				if mode == driver.SelectionTypeSummary {
					plan.GroupBy = driver.SelectionGroupType
				}
				// An empty candidate vector still owes eager domain admission.
				plan.Membership = driver.SelectionExpr{Op: driver.SelectionRefs}
				spec, _ := plan.Boundary.Description()
				switch arm {
				case 0:
					plan.Membership = expr
				case 1:
					plan.Authorization, spec.Authorization = expr, expr
				case 2:
					spec.Host, spec.HostExpression = driver.SelectionHostCompiled, &expr
				}
				plan.Boundary, err = driver.PrepareSelectionBoundary(spec)
				if err != nil {
					t.Fatal(err)
				}
				got, selectErr := oldKind.(driver.SelectionReader).Select(ctx, plan)
				if !errors.Is(selectErr, driver.ErrSelectionUnavailable) || !reflect.DeepEqual(got, driver.SelectionResult{}) {
					t.Fatalf("old empty grant domain certified scoped metadata arm=%d mode=%s expr=%s: %+v/%v", arm, mode, expr.Op, got, selectErr)
				}
			}
		}
	}
}

// Parent coverage is independently useful even when outgoing Links were never
// certified. Compare the selector to the existing source APIs on that real
// publication path, including a same-token Parent-only staged overlay.
func selectionParentOnlyDomain(t *testing.T, factory Factory) {
	ctx, s, lease, _ := setup(t, factory)
	p, q := selectionID("parent-p"), selectionID("parent-q")
	a := selectionImageOf("parent-a", "issue", "P", "a", 0)
	b := selectionImageOf("parent-b", "task", "P", "b", 0)
	c := selectionImageOf("parent-c", "issue", "P", "c", 0)
	a.bead.Parent, b.bead.Parent = &p, &p
	selectionSeedParentOnly(t, ctx, s, lease, []selectionImage{a, b, c}, nil)
	old, base := selectionOpen(t, ctx, s)
	a.bead.Parent, a.parentOnly = &q, true
	c.deleted = true
	d := selectionImageOf("parent-d", "issue", "P", "d", 0)
	d.bead.Parent, d.parentOnly = &p, true
	tx, _ := selectionStage(t, ctx, s, lease, []selectionImage{a, c, d}, nil)
	defer tx.Rollback(ctx)
	planes := []struct {
		name     string
		source   driver.Reader
		selector driver.Reader
		plan     driver.SelectionPlan
	}{
		{"committed_parent_only", old, old, base},
		{"staged_parent_only", tx, tx, selectionPlan(tx.Snapshot())},
		{"committed_content_staged_authority", old, tx, selectionPlan(tx.Snapshot())},
	}
	planes[2].plan.ContentCommitted = true
	for _, plane := range planes {
		t.Run(plane.name, func(t *testing.T) {
			// Ordinary selection has no implicit relation-domain dependency.
			for _, mode := range []driver.SelectionMode{driver.SelectionPageMode, driver.SelectionCountMode, driver.SelectionTypeSummary} {
				plan := plane.plan
				plan.Mode = mode
				if mode != driver.SelectionPageMode {
					plan.Page = nil
				}
				if mode == driver.SelectionTypeSummary {
					plan.GroupBy = driver.SelectionGroupType
				}
				if got := selectionSelect(t, ctx, plane.selector, plan); got.Total != 3 {
					t.Fatalf("ordinary %s depends on unrelated Links coverage: %+v", mode, got)
				}
				plan.Membership = driver.SelectionExpr{Op: driver.SelectionFalse}
				if got := selectionSelect(t, ctx, plane.selector, plan); got.Total != 0 {
					t.Fatalf("false %s: %+v", mode, got)
				}
			}
			for _, parents := range [][]model.Ref{{p}, {q}, {p, q, p}, nil} {
				for _, excluded := range [][]model.Ref{nil, {b.bead.Ref, d.bead.Ref, d.bead.Ref}} {
					expected, more, err := plane.source.(driver.HierarchyChildrenExcludingReader).HierarchyChildrenExcluding(ctx, parents, excluded, 100)
					if err != nil || more {
						t.Fatalf("independent Parent-only source API: %v more=%v", err, more)
					}
					var want, actual []string
					for _, record := range expected {
						want = append(want, record.Ref.ID)
					}
					plan := plane.plan
					plan.Membership = driver.SelectionExpr{Op: driver.SelectionChildrenOp, Sources: &driver.SelectionSources{Refs: parents}}
					plan.Excluded = excluded
					plan.Page = &driver.SelectionPage{Limit: 1}
					for {
						got := selectionSelect(t, ctx, plane.selector, plan)
						if got.Total != uint64(len(expected)) {
							t.Fatalf("Parent-only total=%d source=%d", got.Total, len(expected))
						}
						for _, key := range got.Keys {
							actual = append(actual, key.Ref.ID)
						}
						if !got.More {
							break
						}
						if got.Next == nil {
							t.Fatal("Parent-only page missing keyset")
						}
						plan.Page.After = got.Next
					}
					if !reflect.DeepEqual(actual, want) {
						t.Fatalf("Parent-only selector=%v independent=%v", actual, want)
					}
					plan.Page = &driver.SelectionPage{Limit: 0}
					probe := selectionSelect(t, ctx, plane.selector, plan)
					if probe.More != (len(expected) != 0) || len(probe.Keys) != 0 || probe.Total != uint64(len(expected)) {
						t.Fatalf("Parent-only probe=%+v", probe)
					}
				}
			}
			if rows, more, err := plane.source.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, nil, nil, nil, 0); !errors.Is(err, driver.ErrUnsupported) || len(rows) != 0 || more {
				t.Fatalf("Parent-only source unexpectedly certified Links: %v %v %v", rows, more, err)
			}
			incoming := driver.SelectionExpr{Op: driver.SelectionIncomingOp, Sources: &driver.SelectionSources{Refs: []model.Ref{p}, Kinds: []driver.IncomingLinkKind{driver.IncomingLinkBlocks}}}
			for _, expr := range []driver.SelectionExpr{
				incoming,
				{Op: driver.SelectionIncomingOp, Sources: &driver.SelectionSources{}},
				{Op: driver.SelectionAll, Args: []driver.SelectionExpr{{Op: driver.SelectionFalse}, incoming}},
				{Op: driver.SelectionAny, Args: []driver.SelectionExpr{{Op: driver.SelectionTrue}, incoming}},
				{Op: driver.SelectionNot, Args: []driver.SelectionExpr{incoming}},
			} {
				for arm := 0; arm < 3; arm++ {
					for _, mode := range []driver.SelectionMode{driver.SelectionPageMode, driver.SelectionCountMode, driver.SelectionTypeSummary} {
						plan := plane.plan
						plan.Membership = driver.SelectionExpr{Op: driver.SelectionFalse}
						plan.Mode, plan.Page = mode, nil
						if mode == driver.SelectionPageMode {
							plan.Page = &driver.SelectionPage{Limit: 0}
						}
						if mode == driver.SelectionTypeSummary {
							plan.GroupBy = driver.SelectionGroupType
						}
						spec, _ := plan.Boundary.Description()
						switch arm {
						case 0:
							plan.Membership = expr
						case 1:
							plan.Authorization, spec.Authorization = expr, expr
						case 2:
							spec.Host, spec.HostExpression = driver.SelectionHostCompiled, &expr
						}
						var err error
						plan.Boundary, err = driver.PrepareSelectionBoundary(spec)
						if err != nil {
							t.Fatal(err)
						}
						got, err := plane.selector.(driver.SelectionReader).Select(ctx, plan)
						if !errors.Is(err, driver.ErrSelectionUnavailable) || !reflect.DeepEqual(got, driver.SelectionResult{}) {
							t.Fatalf("uncovered Links hidden arm=%d mode=%s op=%s: %+v/%v", arm, mode, expr.Op, got, err)
						}
					}
				}
			}
		})
	}
	// The content and authority relation arms bind their own planes. An old
	// child of P may simultaneously be a staged current child of Q.
	separate := planes[2].plan
	separate.Membership = driver.SelectionExpr{Op: driver.SelectionChildrenOp, Sources: &driver.SelectionSources{Refs: []model.Ref{p}}}
	separate.Authorization = driver.SelectionExpr{Op: driver.SelectionChildrenOp, Sources: &driver.SelectionSources{Refs: []model.Ref{q}}}
	selectionWant(t, selectionSelect(t, ctx, tx, separate), 1, a.bead.Ref.ID)
	// An empty unbuilt store is no proof of Parent completeness, even when
	// no candidate can survive the containing predicate or zero-row probe.
	emptyCtx, empty, _, _ := setup(t, factory)
	emptyReader, emptyPlan := selectionOpen(t, emptyCtx, empty)
	emptyPlan.Membership = driver.SelectionExpr{Op: driver.SelectionAll, Args: []driver.SelectionExpr{{Op: driver.SelectionFalse}, {Op: driver.SelectionChildrenOp, Sources: &driver.SelectionSources{}}}}
	emptyPlan.Page.Limit = 0
	if got, err := emptyReader.(driver.SelectionReader).Select(emptyCtx, emptyPlan); !errors.Is(err, driver.ErrSelectionUnavailable) || !reflect.DeepEqual(got, driver.SelectionResult{}) {
		t.Fatalf("empty store inferred Parent coverage: %+v/%v", got, err)
	}
}

// The differential relation oracle invokes the existing independent source
// reader APIs, not SelectionFact link/closure rows or a second expected AST.
func selectionSourceDifferential(t *testing.T, factory Factory) {
	ctx, s, lease, _ := setup(t, factory)
	p, q := selectionID("parent-p"), selectionID("parent-q")
	kinds := []driver.IncomingLinkKind{driver.IncomingLinkParentChild, driver.IncomingLinkBlocks, driver.IncomingLinkConditionalBlocks, driver.IncomingLinkWaitsFor}
	images := []selectionImage{}
	for i, kind := range kinds {
		im := selectionImageOf(fmt.Sprintf("source-%d", i), "issue", "P", "title", int64(10-i))
		im.bead.Parent = &p
		im.links = []driver.SourceLink{{Kind: string(kind), To: p}, {Kind: string(kind), To: p}, {Kind: string(kind), To: q}}
		images = append(images, im)
	}
	legacy := selectionImageOf("legacy-parent-link", "task", "P", "title", 0)
	legacy.links = []driver.SourceLink{{Kind: "parent-child", To: p}}
	images = append(images, legacy)
	revision := selectionSeed(t, ctx, s, lease, images, nil)
	old, base := selectionOpen(t, ctx, s)
	moved := images[0]
	moved.bead.Parent = &q
	moved.links = []driver.SourceLink{{Kind: "blocks", To: q}}
	deleted := images[1]
	deleted.deleted = true
	replaced := images[2]
	replaced.links = []driver.SourceLink{{Kind: "blocks", To: p}, {Kind: "waits-for", To: p}, {Kind: "blocks", To: p}}
	created := selectionImageOf("source-new", "issue", "P", "new", -2)
	created.bead.Parent = &p
	created.links = []driver.SourceLink{{Kind: "waits-for", To: p}}
	tx, _ := selectionStage(t, ctx, s, lease, []selectionImage{moved, deleted, replaced, created}, nil)
	defer tx.Rollback(ctx)
	planes := []struct {
		name     string
		source   driver.Reader
		selector driver.Reader
		plan     driver.SelectionPlan
	}{
		{"retained_committed", old, old, base},
		{"same_token_overlay", tx, tx, selectionPlan(tx.Snapshot())},
		{"same_token_committed_content_current_authority", old, tx, selectionPlan(tx.Snapshot())},
	}
	planes[2].plan.ContentRevision = revision
	planes[2].plan.ContentCommitted = true
	for _, plane := range planes {
		t.Run(plane.name, func(t *testing.T) {
			for _, exclude := range [][]model.Ref{nil, {selectionID("source-2"), selectionID("source-new"), selectionID("source-2")}} {
				requests := []struct {
					name  string
					op    driver.SelectionOp
					refs  []model.Ref
					kinds []driver.IncomingLinkKind
				}{
					{"canonical_parent", driver.SelectionChildrenOp, []model.Ref{p, p}, nil},
					{"multiple_parents", driver.SelectionChildrenOp, []model.Ref{q, p}, nil},
					{"empty_parent", driver.SelectionChildrenOp, nil, nil},
					{"all_existing_kinds", driver.SelectionIncomingOp, []model.Ref{p, q, p}, kinds},
					{"duplicate_pair", driver.SelectionIncomingOp, []model.Ref{p, p}, []driver.IncomingLinkKind{driver.IncomingLinkBlocks, driver.IncomingLinkBlocks}},
					{"empty_kinds", driver.SelectionIncomingOp, []model.Ref{p}, nil},
					{"empty_targets", driver.SelectionIncomingOp, nil, []driver.IncomingLinkKind{driver.IncomingLinkBlocks}},
				}
				for _, req := range requests {
					t.Run(req.name, func(t *testing.T) {
						var expected []driver.Record
						var more bool
						var err error
						if req.op == driver.SelectionChildrenOp {
							expected, more, err = plane.source.(driver.HierarchyChildrenExcludingReader).HierarchyChildrenExcluding(ctx, req.refs, exclude, 100)
						} else {
							expected, more, err = plane.source.(driver.InverseSourceLinksReader).InverseSourceLinks(ctx, req.refs, req.kinds, exclude, 100)
						}
						if err != nil || more {
							t.Fatalf("independent source oracle: %v more=%v", err, more)
						}
						var ids []string
						seen := map[string]bool{}
						groupCount := map[string]uint64{}
						for _, record := range expected {
							if seen[record.Ref.ID] {
								t.Fatal("source oracle returned duplicate")
							}
							seen[record.Ref.ID] = true
							ids = append(ids, record.Ref.ID)
							var body struct {
								Type string `json:"type"`
							}
							if err := json.Unmarshal(record.Body, &body); err != nil {
								t.Fatal(err)
							}
							groupCount[body.Type]++
						}
						sort.Strings(ids)
						plan := plane.plan
						plan.Excluded = exclude
						plan.Membership = driver.SelectionExpr{Op: req.op, Sources: &driver.SelectionSources{Refs: req.refs, Kinds: req.kinds}}
						plan.Page = &driver.SelectionPage{Limit: 1}
						plan.GroupBy = driver.SelectionGroupType
						var actual []string
						consumed := 0
						for {
							got := selectionSelect(t, ctx, plane.selector, plan)
							if got.Total != uint64(len(ids)) {
								t.Fatalf("unique total=%d want=%d", got.Total, len(ids))
							}
							for _, key := range got.Keys {
								actual = append(actual, key.Ref.ID)
							}
							consumed += len(got.Keys)
							for _, g := range got.Groups {
								var suffix uint64
								for _, id := range ids[consumed:] {
									for _, im := range append(append([]selectionImage{}, images...), created) {
										if im.bead.Ref.ID == id && im.bead.Type == g.Type {
											suffix++
											break
										}
									}
								}
								if g.Count != groupCount[g.Type] || g.Elided != suffix {
									t.Fatalf("full/suffix group=%+v", g)
								}
							}
							if !got.More {
								break
							}
							if got.Next == nil {
								t.Fatal("missing keyset")
							}
							plan.Page.After = got.Next
						}
						if !reflect.DeepEqual(actual, ids) {
							t.Fatalf("selector=%v source=%v", actual, ids)
						}
						plan.Page = nil
						plan.Mode = driver.SelectionTypeSummary
						summary := selectionSelect(t, ctx, plane.selector, plan)
						if summary.Total != uint64(len(ids)) || len(summary.Keys) != 0 || len(summary.Groups) != len(groupCount) {
							t.Fatalf("summary=%+v", summary)
						}
						plan.Page = &driver.SelectionPage{Limit: 0}
						plan.Mode = driver.SelectionPageMode
						probe := selectionSelect(t, ctx, plane.selector, plan)
						if probe.More != (len(ids) > 0) || len(probe.Keys) != 0 || probe.Total != uint64(len(ids)) {
							t.Fatalf("probe=%+v", probe)
						}
						if len(ids) > 1 {
							plan.Cap = len(ids) - 1
							selectionZeroError(t, ctx, plane.selector, plan)
						}
					})
				}
			}
		})
	}
	// Explicit current existence excludes the staged tombstone from historical
	// membership; true authorization above deliberately retained that old bead.
	historical := planes[2].plan
	historical.Membership = driver.SelectionExpr{Op: driver.SelectionChildrenOp, Sources: &driver.SelectionSources{Refs: []model.Ref{p}}}
	historical.Authorization = driver.SelectionExpr{Op: driver.SelectionResourceExists}
	got := selectionSelect(t, ctx, tx, historical)
	for _, key := range got.Keys {
		if key.Ref.ID == deleted.bead.Ref.ID {
			t.Fatal("current tombstone reused old resource")
		}
	}
}

// This adapter reads logical bytes through the existing Reader. The existing
// authz.FilterTemplate interpreter, including caveats/permit/forbid, is the
// oracle. It never reads SelectionIndex or calls the selector evaluator.
type selectionAuthSlice struct {
	reader    driver.Reader
	namespace authz.Namespace
	caller    authz.Caller
}

func (s selectionAuthSlice) Namespace() authz.Namespace                           { return s.namespace }
func (s selectionAuthSlice) Record(ctx context.Context, _ authz.Dependency) error { return ctx.Err() }
func (s selectionAuthSlice) CheckContext(_ context.Context, c authz.Caller) (bool, error) {
	return reflect.DeepEqual(c, s.caller), nil
}
func (s selectionAuthSlice) facts(ctx context.Context, table driver.DerivedTable, key string) ([]driver.SelectionFact, error) {
	rows, err := s.reader.DerivedRows(ctx, table, key)
	if err != nil {
		return nil, err
	}
	if len(rows) != 1 || rows[0].Key != key {
		return nil, errors.New("oracle fact source missing")
	}
	var set driver.SelectionFacts
	if err = json.Unmarshal(rows[0].Value, &set); err != nil {
		return nil, err
	}
	return set.Rows, nil
}
func (s selectionAuthSlice) HasGrant(ctx context.Context, sub authz.SubjectRef, scope authz.Scope, role string) (bool, error) {
	facts, err := s.facts(ctx, driver.DerivedGrant, "differential-grants")
	if err != nil {
		return false, err
	}
	for _, f := range facts {
		if f.Subject == sub.Kind+":"+sub.ID && f.Scope == scope.Ref.ID && f.Role == role {
			return true, nil
		}
	}
	return false, nil
}
func (s selectionAuthSlice) Groups(ctx context.Context, sub authz.SubjectRef) ([]model.Ref, error) {
	facts, err := s.facts(ctx, driver.DerivedGrant, "differential-grants")
	if err != nil {
		return nil, err
	}
	var refs []model.Ref
	for _, f := range facts {
		if f.Subject == sub.Kind+":"+sub.ID && f.Role == "member" {
			refs = append(refs, selectionID(f.Scope))
		}
	}
	return refs, nil
}
func (s selectionAuthSlice) active(ctx context.Context, kind driver.SelectionFactKind, key string, r model.Ref) (bool, error) {
	facts, err := s.facts(ctx, driver.DerivedAttribute, key)
	if err != nil {
		return false, err
	}
	for _, f := range facts {
		if f.Kind == kind && f.Name == r.ID {
			return f.Active, nil
		}
	}
	return false, nil
}
func (s selectionAuthSlice) TeamActive(ctx context.Context, r model.Ref) (bool, error) {
	return s.active(ctx, driver.SelectionTeamFact, "differential-teams", r)
}
func (s selectionAuthSlice) ProjectActive(ctx context.Context, r model.Ref) (bool, error) {
	return s.active(ctx, driver.SelectionProjectFact, "differential-projects", r)
}
func (s selectionAuthSlice) ProjectForBead(ctx context.Context, r model.Ref) (authz.ProjectBinding, error) {
	rows, err := s.reader.Get(ctx, []driver.Ref{r})
	if err != nil || len(rows) == 0 {
		return authz.ProjectBinding{}, err
	}
	var body struct {
		Project *string `json:"project"`
	}
	if err = json.Unmarshal(rows[0].Body, &body); err != nil {
		return authz.ProjectBinding{}, err
	}
	if body.Project == nil || *body.Project == "" {
		return authz.ProjectBinding{}, nil
	}
	return authz.ProjectBinding{Name: *body.Project, Ref: selectionID(*body.Project), Found: true}, nil
}

func selectionAuthorizationDifferential(t *testing.T, factory Factory) {
	ctx, s, lease, meta := setup(t, factory)
	images := []selectionImage{selectionImageOf("a", "issue", "P", "A", 0), selectionImageOf("b", "issue", "Q", "B", 0), selectionImageOf("c", "issue", "R", "C", 0), selectionImageOf("d", "issue", "<absent>", "D", 0)}
	grants := []driver.SelectionFact{{Kind: driver.SelectionGrantFact, Subject: "user:u", Scope: "P", Role: "read"}, {Kind: driver.SelectionGrantFact, Subject: "user:u", Scope: "P", Role: "read"}, {Kind: driver.SelectionGrantFact, Subject: "user:u", Scope: "Q", Role: "read"}, {Kind: driver.SelectionGrantFact, Subject: "user:u", Scope: "Q", Role: "deny"}}
	facts := []selectionFactWrite{{table: driver.DerivedGrant, key: "differential-grants", kind: driver.SelectionGrantFact, rows: grants}, {table: driver.DerivedAttribute, key: "differential-projects", kind: driver.SelectionProjectFact, rows: []driver.SelectionFact{{Kind: driver.SelectionProjectFact, Name: "P", Active: true}, {Kind: driver.SelectionProjectFact, Name: "Q", Active: true}, {Kind: driver.SelectionProjectFact, Name: "R", Active: false}}}, {table: driver.DerivedAttribute, key: "differential-teams", kind: driver.SelectionTeamFact}}
	selectionSeed(t, ctx, s, lease, images, facts)
	r, plan := selectionOpen(t, ctx, s)
	caller := authz.Caller{Principal: model.PrincipalRef{Kind: "user", ID: "u"}, VerificationID: "verification", CapturedEpoch: model.Natural64(1)}
	namespace := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "selection-differential", Authority: selectionID("authority")}
	filter := authz.FilterTemplate{Namespace: namespace, Request: authz.ListRequest{Caller: caller, Action: authz.Read, ResourceKind: authz.Bead, Method: "view.run"}, Permit: authz.FilterExpr{Op: authz.GrantExists, Subject: authz.CallerSubject, Scope: authz.ProjectBindingScope, Role: "read"}, Forbid: authz.FilterExpr{Op: authz.GrantExists, Subject: authz.CallerSubject, Scope: authz.ProjectBindingScope, Role: "deny"}, Restrictions: []authz.FilterExpr{{Op: authz.ActiveProject}}}
	grantExpr := func(role string) driver.SelectionExpr {
		return selectionExists(driver.DerivedGrant, "differential-grants", driver.SelectionGrantFact, driver.SelectionFactTest{Column: driver.SelectionFactSubject, Value: selectionValuePointer(driver.SelectionString("user:u"))}, driver.SelectionFactTest{Column: driver.SelectionFactRole, Value: selectionValuePointer(driver.SelectionString(role))}, driver.SelectionFactTest{Column: driver.SelectionFactScope, Candidate: driver.SelectionProject})
	}
	active := selectionExists(driver.DerivedAttribute, "differential-projects", driver.SelectionProjectFact, driver.SelectionFactTest{Column: driver.SelectionFactName, Candidate: driver.SelectionProject}, driver.SelectionFactTest{Column: driver.SelectionFactActive, Value: selectionValuePointer(driver.SelectionBool(true))})
	plan.Authorization = driver.SelectionExpr{Op: driver.SelectionAll, Args: []driver.SelectionExpr{grantExpr("read"), {Op: driver.SelectionNot, Args: []driver.SelectionExpr{grantExpr("deny")}}, active}}
	for _, empty := range []bool{false, true} {
		f := filter
		if empty {
			f.Request.Caller.Principal.Caveats = map[string]any{"refs": []model.Ref{}}
		}
		oracle := selectionAuthSlice{reader: r, namespace: namespace, caller: f.Request.Caller}
		var want []string
		for _, im := range images {
			allowed, err := f.Match(ctx, oracle, authz.Resource{Kind: authz.Bead, Ref: im.bead.Ref})
			if err != nil {
				t.Fatal(err)
			}
			if allowed {
				want = append(want, im.bead.Ref.ID)
			}
		}
		p := plan
		if empty {
			p.Authorization = driver.SelectionExpr{Op: driver.SelectionAll, Args: []driver.SelectionExpr{p.Authorization, {Op: driver.SelectionRefs, Refs: []model.Ref{}}}}
		}
		got := selectionSelect(t, ctx, r, p)
		selectionWant(t, got, uint64(len(want)), want...)
		p.Mode = driver.SelectionCountMode
		p.Page = nil
		if count := selectionSelect(t, ctx, r, p); count.Total != uint64(len(want)) {
			t.Fatalf("count=%+v oracle=%v", count, want)
		}
	}
	// A host restriction intersects the fixed chain; it cannot replace a forbid.
	plan = selectionSeal(plan)
	spec, _ := plan.Boundary.Description()
	host := driver.SelectionExpr{Op: driver.SelectionRefs, Refs: []model.Ref{selectionID("b"), selectionID("c")}}
	spec.Host, spec.HostExpression = driver.SelectionHostCompiled, &host
	plan.Boundary, _ = driver.PrepareSelectionBoundary(spec)
	selectionWant(t, selectionSelect(t, ctx, r, plan), 0)
	// Revocation is a new current source version; retained readers remain pinned.
	facts[0].rows = nil
	selectionSeed(t, ctx, s, lease, nil, []selectionFactWrite{facts[0]})
	current, p := selectionOpen(t, ctx, s)
	p.Authorization = plan.Authorization
	selectionWant(t, selectionSelect(t, ctx, current, p), 0)
	spec.Host = driver.SelectionHostAbsent
	spec.HostExpression = nil
	retained := plan
	retained.Boundary, _ = driver.PrepareSelectionBoundary(spec)
	selectionWant(t, selectionSelect(t, ctx, r, retained), 1, "a")
}
