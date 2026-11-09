package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// These complete typed adapter fixtures isolate the two physical closure
// directions and project-bound authorization. They are not W4 native producer
// or the unchanged 100k CP7 workload, and make no latency/scan-bound claim.
func seedSelectionSQLFacts(t *testing.T, s *Store, lease driver.ActiveLease, sets map[driver.DerivedTable]driver.SelectionFacts) map[driver.DerivedTable]driver.DerivedRowKey {
	t.Helper()
	ctx := context.Background()
	closure, err := driver.NewClosureSentinel("w6-fact-sql")
	if err != nil {
		t.Fatal(err)
	}
	grant, err := driver.NewGrantSentinel("w6-fact-sql")
	if err != nil {
		t.Fatal(err)
	}
	sentinels := []driver.Sentinel{closure, grant}
	locks := []driver.LockKey{{Kind: driver.LockSentinel, Sentinel: closure}, {Kind: driver.LockSentinel, Sentinel: grant}}
	keys := map[driver.DerivedTable]driver.DerivedRowKey{}
	for table := range sets {
		key := driver.DerivedRowKey{Table: table, Key: "w6-fact-sql"}
		keys[table] = key
		locks = append(locks, driver.LockKey{Kind: driver.LockDerived, Derived: &key})
	}
	tx, header := beginTest(t, s, lease, locks...)
	for table, set := range sets {
		key := keys[table]
		body, err := json.Marshal(set)
		if err != nil {
			t.Fatal(err)
		}
		row := driver.DerivedRow{Key: key.Key, Value: body, Index: &driver.SelectionIndex{
			Version: driver.SelectionIndexVersion, Row: key, Incarnation: header.Incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(body),
			Source: driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &key}, Revision: model.Natural64(0), Present: true, Digest: driver.SelectionDigest(body)}, Facts: &set,
		}}
		if err = tx.(driver.SelectionIndexWriter).PutSelectionDerived(ctx, table, nil, []driver.DerivedRow{row}); err != nil {
			t.Fatal(err)
		}
	}
	if err = tx.BumpSentinels(ctx, sentinels); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Commit(ctx, header); err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestSelectionAncestorAnnotationRetainsCanonicalRefs(t *testing.T) {
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	child, parent := testRef("pl-child"), testRef("pl-parent")
	facts := driver.SelectionFacts{Kind: driver.SelectionAncestorFact, Rows: []driver.SelectionFact{
		{Kind: driver.SelectionAncestorFact, Ref: &child, Target: &child},
		{Kind: driver.SelectionAncestorFact, Ref: &child, Target: &parent, Depth: 1},
	}}
	key := seedSelectionSQLFacts(t, s, lease, map[driver.DerivedTable]driver.SelectionFacts{driver.DerivedAncestorClosure: facts})[driver.DerivedAncestorClosure]
	r := testRead(t, s, nil)
	rows, err := r.DerivedRows(context.Background(), key.Table, key.Key)
	if err != nil || len(rows) != 1 || rows[0].Index == nil || !reflect.DeepEqual(rows[0].Index.Facts, &facts) {
		t.Fatalf("persisted ancestor annotation=%+v %v; want %#v", rows, err, facts)
	}
}

func TestSelectionSparseFactsAndBothClosureDirectionsExecutorEvidence(t *testing.T) {
	const population = 4096
	for _, allowed := range []int{1000, 100} {
		t.Run(fmt.Sprintf("authorized_%d", allowed), func(t *testing.T) {
			ctx := context.Background()
			s := openTest(t, freshDSN(t))
			lease := testLease(t, s)
			// Keep the actual production lease alive during both the bounded
			// population commits and the subsequent fact publication/census.
			// Slow disposable hosts must still exercise the real renewal fence.
			renewCtx, cancel := context.WithCancel(ctx)
			renewDone := make(chan error, 1)
			go func() {
				ticker := time.NewTicker(driver.LeaseTTL / 4)
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
				if err := <-renewDone; err != nil && !errors.Is(err, context.Canceled) {
					t.Error(err)
				}
			}()
			beads := make([]driver.SelectionBead, population)
			for i := range beads {
				ref := testRef(fmt.Sprintf("pl-fact-%04d", i))
				project := fmt.Sprintf("project-%04d", i)
				beads[i] = driver.SelectionBead{Ref: ref, Type: "issue", Project: &project}
			}
			seedSelectionSQL(t, s, lease, beads)
			anchor := testRef("pl-closure-anchor")
			forward := driver.SelectionFacts{Kind: driver.SelectionAncestorFact}
			reverse := driver.SelectionFacts{Kind: driver.SelectionAncestorFact}
			grants := driver.SelectionFacts{Kind: driver.SelectionScopedGrantFact}
			for _, bead := range beads[population-allowed:] {
				ref := bead.Ref
				forward.Rows = append(forward.Rows, driver.SelectionFact{Kind: driver.SelectionAncestorFact, Ref: &ref, Target: &anchor, Depth: 1})
				reverse.Rows = append(reverse.Rows, driver.SelectionFact{Kind: driver.SelectionAncestorFact, Ref: &anchor, Target: &ref, Depth: 1})
				registry := model.RefByID("registry/"+ref.ID, model.Natural64(0))
				grants.Rows = append(grants.Rows, driver.SelectionFact{Kind: driver.SelectionScopedGrantFact, Subject: "user:w6", Scope: "opaque:" + registry.ID, Role: "read", ScopeKind: driver.SelectionProjectScope, ScopeRef: &registry, ProjectName: bead.Project})
			}
			// Duplicated facts must never multiply a bead or consume its cap.
			grants.Rows = append(grants.Rows, grants.Rows[0])
			keys := seedSelectionSQLFacts(t, s, lease, map[driver.DerivedTable]driver.SelectionFacts{
				driver.DerivedAncestorClosure: forward, driver.DerivedCutClosure: reverse, driver.DerivedGrant: grants,
			})
			r := testRead(t, s, nil)
			value := func(v driver.SelectionValue) *driver.SelectionValue { return &v }
			for _, direction := range []struct {
				name  string
				table driver.DerivedTable
				tests []driver.SelectionFactTest
			}{
				{"ancestor", driver.DerivedAncestorClosure, []driver.SelectionFactTest{{Column: driver.SelectionFactRef, Candidate: driver.SelectionID}, {Column: driver.SelectionFactTarget, Value: value(driver.SelectionRef(anchor))}}},
				{"cut_inverse", driver.DerivedCutClosure, []driver.SelectionFactTest{{Column: driver.SelectionFactRef, Value: value(driver.SelectionRef(anchor))}, {Column: driver.SelectionFactTarget, Candidate: driver.SelectionID}}},
			} {
				t.Run(direction.name, func(t *testing.T) {
					p := selectionSQLPlan(r)
					p.Cap, p.Page.Limit = allowed, 1
					p.Membership = driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: keys[direction.table], Kind: driver.SelectionAncestorFact, Tests: direction.tests}}
					p.Authorization = driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: keys[driver.DerivedGrant], Kind: driver.SelectionScopedGrantFact, Tests: []driver.SelectionFactTest{
						{Column: driver.SelectionFactSubject, Value: value(driver.SelectionString("user:w6"))},
						{Column: driver.SelectionFactScopeKind, Value: value(driver.SelectionString(string(driver.SelectionProjectScope)))},
						{Column: driver.SelectionFactProjectName, Candidate: driver.SelectionProject},
						{Column: driver.SelectionFactRole, Value: value(driver.SelectionString("read"))},
					}}}
					for _, mode := range []driver.SelectionMode{driver.SelectionPageMode, driver.SelectionCountMode, driver.SelectionTypeSummary} {
						p.Mode, p.Page, p.GroupBy = mode, nil, ""
						if mode == driver.SelectionPageMode {
							p.Page = &driver.SelectionPage{Limit: 1}
						}
						if mode == driver.SelectionTypeSummary {
							p.GroupBy = driver.SelectionGroupType
							p.Cap = 0
						}
						got, err := r.(driver.SelectionReader).Select(ctx, selectionSQLSeal(p))
						if err != nil || got.Total != uint64(allowed) {
							t.Fatalf("%s result=%+v err=%v", mode, got, err)
						}
						if mode == driver.SelectionPageMode && (len(got.Keys) != 1 || got.Keys[0].Ref.ID != beads[population-allowed].Ref.ID || !got.More) {
							t.Fatalf("authorized page=%+v", got)
						}
						if mode != driver.SelectionPageMode && len(got.Keys) != 0 {
							t.Fatalf("aggregate returned keys=%+v", got)
						}
						if mode == driver.SelectionTypeSummary && (len(got.Groups) != 1 || got.Groups[0].Count != uint64(allowed)) {
							t.Fatalf("unique summary=%+v", got)
						}
						explainSelectionSQLFixture(t, r.(*reader), p, direction.name+"_"+string(mode), population, allowed)
					}
					// Exclusions act before the tighter unique aggregate cap.
					p.Cap, p.Mode, p.GroupBy = allowed-1, driver.SelectionCountMode, ""
					p.Excluded = []model.Ref{beads[population-allowed].Ref, beads[population-allowed].Ref}
					got, err := r.(driver.SelectionReader).Select(ctx, selectionSQLSeal(p))
					if err != nil || got.Total != uint64(allowed-1) {
						t.Fatalf("excluded aggregate=%+v err=%v", got, err)
					}
				})
			}
		})
	}
}

// This isolates physical byte transport and the committed fresh observation.
// The shared conformance covers the complete scoped-domain boolean oracle;
// these adapter facts do not certify a real fixed-policy producer or compiler.
func TestSelectionScopedGrantMetadataAndFreshCommittedDomain(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	name, empty := "web/api_%é", ""
	registry := model.RefByID("canonical/project", model.Natural64(0))
	authority := model.RefByID("canonical/authority", model.Natural64(0))
	seedSelectionSQL(t, s, lease, []driver.SelectionBead{
		{Ref: testRef("pl-scope-name"), Type: "issue", Project: &name},
		{Ref: testRef("pl-scope-id"), Type: "issue", Project: &registry.ID},
		{Ref: testRef("pl-scope-empty"), Type: "issue", Project: &empty},
		{Ref: testRef("pl-scope-absent"), Type: "issue"},
	})
	facts := driver.SelectionFacts{Kind: driver.SelectionScopedGrantFact, Rows: []driver.SelectionFact{
		{Kind: driver.SelectionScopedGrantFact, Subject: "opaque-user\x00\xff", Scope: "opaque-project\x00\xff", Role: "read", ScopeKind: driver.SelectionProjectScope, ScopeRef: &registry, ProjectName: &name},
		{Kind: driver.SelectionScopedGrantFact, Subject: "opaque-user\x00\xff", Scope: "opaque-authority\x00\xff", Role: "admin", ScopeKind: driver.SelectionAuthorityScope, ScopeRef: &authority},
	}}
	key := seedSelectionSQLFacts(t, s, lease, map[driver.DerivedTable]driver.SelectionFacts{driver.DerivedGrant: facts})[driver.DerivedGrant]
	value := func(v driver.SelectionValue) *driver.SelectionValue { return &v }
	direct := driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: key, Kind: driver.SelectionScopedGrantFact, Tests: []driver.SelectionFactTest{
		{Column: driver.SelectionFactSubject, Value: value(driver.SelectionString(facts.Rows[0].Subject))},
		{Column: driver.SelectionFactScopeKind, Value: value(driver.SelectionString(string(driver.SelectionProjectScope)))},
		{Column: driver.SelectionFactProjectName, Candidate: driver.SelectionProject},
		{Column: driver.SelectionFactRole, Value: value(driver.SelectionString("read"))},
	}}}
	old := testRead(t, s, nil)
	p := selectionSQLPlan(old)
	p.Authorization = direct
	requireSelectionIDs(t, old, p, 1, "pl-scope-name")
	stored, err := old.DerivedRows(ctx, key.Table, key.Key)
	if err != nil || len(stored) != 1 || stored[0].Index == nil || stored[0].Index.Facts == nil || len(stored[0].Index.Facts.Rows) != len(facts.Rows) {
		t.Fatalf("scoped metadata/opaque bytes round trip=%+v %v", stored, err)
	}
	actual := stored[0].Clone().Index.Facts
	for i, want := range facts.Rows {
		got := actual.Rows[i].ScopeRef
		if got == nil || got.Validate() != nil || !got.IsID() || got.ID != want.ScopeRef.ID || got.Branch != want.ScopeRef.Branch || got.Key != want.ScopeRef.Key {
			t.Fatalf("scope reference round trip=%#v; want %#v", got, want.ScopeRef)
		}
	}
	if !reflect.DeepEqual(actual, &facts) {
		t.Fatalf("scoped metadata/opaque bytes round trip=%#v; want %#v", actual, facts)
	}
	p.Authorization = driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: key, Kind: driver.SelectionScopedGrantFact, Tests: []driver.SelectionFactTest{
		{Column: driver.SelectionFactScopeKind, Value: value(driver.SelectionString(string(driver.SelectionAuthorityScope)))},
		{Column: driver.SelectionFactProjectName, Value: value(driver.SelectionString(""))},
	}}}
	requireSelectionIDs(t, old, p, 0) // nil metadata remains SQL NULL, never empty.
	lock := driver.LockKey{Kind: driver.LockDerived, Derived: &key}
	grant, err := driver.NewGrantSentinel("w6-scoped-metadata")
	if err != nil {
		t.Fatal(err)
	}
	beginGrantWrite := func() (driver.WriteTx, driver.ChangesetHeader) {
		t.Helper()
		tx, h := beginTest(t, s, lease, lock, driver.LockKey{Kind: driver.LockSentinel, Sentinel: grant})
		if err := tx.BumpSentinels(ctx, []driver.Sentinel{grant}); err != nil {
			t.Fatal(err)
		}
		return tx, h
	}
	stage := func(tx driver.WriteTx, set driver.SelectionFacts) {
		t.Helper()
		states, err := tx.Observe(ctx, []driver.LockKey{lock})
		if err != nil || len(states) != 1 || !states[0].Present {
			t.Fatalf("locked fact source=%+v %v", states, err)
		}
		body, err := json.Marshal(set)
		if err != nil {
			t.Fatal(err)
		}
		row := driver.DerivedRow{Key: key.Key, Value: body, Index: &driver.SelectionIndex{
			Version: driver.SelectionIndexVersion, Row: key, Incarnation: tx.Snapshot().Incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(body),
			Source: driver.SelectionSource{Key: lock, Revision: states[0].Revision, Present: true, Digest: driver.SelectionDigest(body)}, Facts: &set,
		}}
		if err := tx.(driver.SelectionIndexWriter).PutSelectionDerived(ctx, key.Table, nil, []driver.DerivedRow{row}); err != nil {
			t.Fatal(err)
		}
	}
	generic, header := beginGrantWrite()
	stage(generic, driver.SelectionFacts{Kind: driver.SelectionGrantFact})
	if _, err := generic.Commit(ctx, header); err != nil {
		t.Fatal(err)
	}
	p.Authorization = direct
	requireSelectionIDs(t, old, p, 1, "pl-scope-name") // retained source stays pinned.
	tx, header := beginGrantWrite()
	defer tx.Rollback(ctx)
	stage(tx, driver.SelectionFacts{Kind: driver.SelectionScopedGrantFact})
	p = selectionSQLPlan(tx)
	p.Mode, p.Page, p.Authorization = driver.SelectionCountMode, nil, direct
	got, err := tx.(driver.SelectionReader).Select(ctx, selectionSQLSeal(p))
	if err != nil || got.Total != 0 {
		t.Fatalf("complete staged scoped empty set=%+v %v", got, err)
	}
	if yes, err := tx.(driver.FreshSelectionExistsReader).FreshSelectionExists(ctx, selectionSQLSeal(p)); yes || !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("staged scoped domain hid committed generic facts: %v %v", yes, err)
	}
	if _, err := tx.Commit(ctx, header); err != nil {
		t.Fatal(err)
	}
	terminal, _ := beginTest(t, s, lease, lock)
	defer terminal.Rollback(ctx)
	p = selectionSQLPlan(terminal)
	p.Mode, p.Page, p.Authorization = driver.SelectionCountMode, nil, direct
	if yes, err := terminal.(driver.FreshSelectionExistsReader).FreshSelectionExists(ctx, selectionSQLSeal(p)); err != nil || yes {
		t.Fatalf("complete committed scoped empty set=%v %v", yes, err)
	}
}
