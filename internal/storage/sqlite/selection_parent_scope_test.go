package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Forward the production proof to a real connection. The captured statement
// also lets the cost control count its actual materialized proof rows, while
// DBStatus measures engine page accesses for the unmodified production SQL.
type selectionParentProofWork struct {
	*sql.Conn
	query string
	args  []any
}

func (w *selectionParentProofWork) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	w.query, w.args = query, append([]any(nil), args...)
	return w.Conn.QueryRowContext(ctx, query, args...)
}

func TestSelectionParentScopeExecutedSubtreeWork(t *testing.T) {
	for _, population := range []int{1000, 2000} {
		t.Run(fmt.Sprintf("sources_%d", population), func(t *testing.T) {
			ctx := context.Background()
			s := selectionStore(t).(*Store)
			t.Cleanup(func() { _ = s.Close() })
			meta := getMeta(t, ctx, s)
			root := testRef("parent-work-0000")
			rootKey := keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &root})
			// This is a populated SQL-engine proof-cost control, not total
			// Commit or service/SLO evidence. Public Commit controls follow.
			if err := s.immediate(ctx, nil, func(c *sql.Conn) error {
				for i := 0; i < population; i++ {
					ref := testRef(fmt.Sprintf("parent-work-%04d", i))
					id := keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
					row := fmt.Sprintf("3:parent-work-%04d", i)
					var parent any
					if i != 0 {
						parent = rootKey
					}
					if _, err := c.ExecContext(ctx, `INSERT INTO hierarchy_source_current
						(incarnation,branch,child_key,parent_key,ref,kind,revision,body) VALUES(?,?,?,?,?,?,?,?)`,
						meta.Incarnation, revKey(0), id, parent, []byte(ref.ID), "bead", revKey(1), []byte(`{}`)); err != nil {
						return err
					}
					if err := insertSelectionCloseImage(ctx, c, meta.Incarnation, row, id, revKey(1), nil); err != nil {
						return err
					}
					if _, err := c.ExecContext(ctx, `UPDATE selection_bead_history SET parent_key=? WHERE record_key=?`, parent, id); err != nil {
						return err
					}
					for depth := 0; depth <= 1; depth++ {
						if depth == 1 && i == 0 {
							continue
						}
						target := id
						if depth == 1 {
							target = rootKey
						}
						if _, err := c.ExecContext(ctx, `INSERT INTO selection_facts
							(incarnation,branch,row_key,start_revision,ordinal,kind,ref_key,target_key,depth,
							subject,scope,role,name,value,active)
							VALUES(?,?,?,?,?,?,?,?,?,'','','','','',0)`, meta.Incarnation, revKey(0), row, revKey(1), depth,
							string(driver.SelectionAncestorFact), id, target, depth); err != nil {
							return err
						}
					}
				}
				return nil
			}); err != nil {
				t.Fatal(err)
			}
			c, err := s.db.Conn(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			measure := func(scope selectionParentProofScope, wantSources, wantFacts int) int {
				t.Helper()
				work := &selectionParentProofWork{Conn: c}
				if _, err := selectionCloseCacheHits(c, true); err != nil {
					t.Fatal(err)
				}
				valid, err := verifySQLiteSelectionParentChainsScoped(ctx, work, meta.Incarnation, revKey(1), scope)
				if err != nil || !valid || work.query == "" {
					t.Fatalf("executed production proof valid=%t err=%v", valid, err)
				}
				hits, err := selectionCloseCacheHits(c, false)
				if err != nil {
					t.Fatal(err)
				}
				const result = "SELECT EXISTS(SELECT 1 FROM bad_rows)"
				if !strings.HasSuffix(work.query, result) {
					t.Fatal("production proof result changed")
				}
				countSQL := strings.TrimSuffix(work.query, result) + result +
					`,(SELECT COUNT(*) FROM affected),(SELECT COUNT(*) FROM facts)`
				var bad, sources, facts int
				if err := c.QueryRowContext(ctx, countSQL, work.args...).Scan(&bad, &sources, &facts); err != nil || bad != 0 || sources != wantSources || facts != wantFacts {
					t.Fatalf("executed proof rows bad=%d sources=%d facts=%d want=%d/%d err=%v", bad, sources, facts, wantSources, wantFacts, err)
				}
				t.Logf("population=%d proof_sources=%d proof_facts=%d actual_page_cache_hits=%d executor_row_visits=UNMEASURED", population, sources, facts, hits)
				return hits
			}
			// The same root still receives the complete proof when structural;
			// merely dropping its seed would fail the own-chain row assertions.
			local := measure(selectionParentProofScope{chains: []string{rootKey}}, 1, 1)
			structural := measure(selectionParentProofScope{chains: []string{rootKey}, subtrees: []string{rootKey}}, population, 2*population-1)
			if local <= 0 || local > 256 || structural <= local {
				t.Fatalf("population=%d local actual page accesses=%d structural=%d; want local<=256 and strictly smaller", population, local, structural)
			}
		})
	}
}

func parentScopeProjection(incarnation string, parents map[string]*model.Ref) (driver.GraphSourceDecoder, driver.GraphSourceProjection) {
	decode, project := selectionBackfillRaceProjection(incarnation, nil)
	plainDecode, plainProject := decode, project.Decode
	decode = func(record driver.Record) (driver.GraphSourceMetadata, error) {
		metadata, err := plainDecode(record)
		metadata.Parent = parents[record.Ref.ID]
		metadata.Selection[0].Row.Index.Bead.Parent = parents[record.Ref.ID]
		return metadata, err
	}
	project.Decode = func(record driver.Record, inputs []driver.DerivedRow) ([]driver.DerivedSelectionRow, error) {
		rows, err := plainProject(record, inputs)
		seen := map[string]bool{record.Ref.ID: true}
		for parent := parents[record.Ref.ID]; parent != nil && !seen[parent.ID]; parent = parents[parent.ID] {
			seen[parent.ID] = true
			rows[0].Row.Index.Facts.Rows = append(rows[0].Row.Index.Facts.Rows, driver.SelectionFact{
				Kind: driver.SelectionAncestorFact, Ref: &record.Ref, Target: parent,
				Depth: uint64(len(rows[0].Row.Index.Facts.Rows)),
			})
		}
		return rows, err
	}
	return decode, project
}

func parentScopeStore(t *testing.T) (*Store, driver.ActiveLease, []model.Ref, map[string]*model.Ref) {
	t.Helper()
	ctx := context.Background()
	s := selectionStore(t).(*Store)
	t.Cleanup(func() { _ = s.Close() })
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	refs := []model.Ref{testRef("scope-root"), testRef("scope-child"), testRef("scope-leaf"), testRef("scope-sibling"), testRef("scope-other")}
	parents := map[string]*model.Ref{refs[1].ID: &refs[0], refs[2].ID: &refs[1], refs[3].ID: &refs[0]}
	tx, h := beginSourceTest(t, s, lease, refs...)
	for _, ref := range refs {
		if err := tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: []byte(`{}`)}, parents[ref.ID], nil); err != nil {
			t.Fatal(err)
		}
	}
	commitSourceTest(t, tx, h)
	decode, project := parentScopeProjection(h.Incarnation, parents)
	if err := s.BuildGraphSourceIndexes(ctx, lease, decode, project); err != nil {
		t.Fatal(err)
	}
	return s, lease, refs, parents
}

func TestSelectionParentScopeFinalOverlayAndLifetimes(t *testing.T) {
	s, lease, refs, parents := parentScopeStore(t)
	ctx := context.Background()
	for _, mode := range []string{"title", "final-parent-restored", "move", "delete", "insert", "raw", "restore", "unbuilt"} {
		t.Run(mode, func(t *testing.T) {
			ref := refs[0]
			if mode == "insert" {
				ref = testRef("scope-inserted")
			}
			tx, _ := beginSourceTest(t, s, lease, ref)
			defer tx.Close()
			var parent *model.Ref
			if mode == "move" || mode == "final-parent-restored" {
				parent = &refs[4]
			}
			if err := tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: []byte(`{"title":"changed"}`)}, parent, nil); err != nil {
				t.Fatal(err)
			}
			if mode == "final-parent-restored" {
				if err := tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: []byte(`{"title":"final"}`)}, parents[ref.ID], nil); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "delete" {
				if err := tx.Delete(ctx, ref); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "raw" {
				tx.sourceOverlay.Replace(keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &ref}), nil)
			}
			meta := getMeta(t, ctx, s)
			if mode == "restore" {
				tx.restore = &meta
			}
			scope, err := tx.prepareSelectionParentProof(ctx, tx.tx, meta, mode != "unbuilt")
			id := keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
			want := []string{id}
			if mode == "title" || mode == "final-parent-restored" {
				want = nil
			}
			if err != nil || !reflect.DeepEqual(scope.chains, []string{id}) || !reflect.DeepEqual(scope.subtrees, want) {
				t.Fatalf("%s scope=%+v want subtrees=%v err=%v", mode, scope, want, err)
			}
		})
	}
}

func TestSelectionParentScopeCommitStructuralAndRefusalControls(t *testing.T) {
	for _, mode := range []string{"title", "title-untouched-descendant", "derived-only", "own-chain-corrupt", "ancestor-corrupt", "move-stale-descendant", "mixed-stale-descendant", "move-complete", "cycle", "delete-leaf", "delete-parent", "insert"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			s, lease, refs, parents := parentScopeStore(t)
			meta := getMeta(t, ctx, s)
			family := driver.SelectionProjectionFamily{Table: driver.DerivedAncestorClosure, Kind: driver.SelectionAncestorFact, Version: driver.SelectionIndexVersion}
			ready, err := selectionFamilyReady(ctx, s.db, meta.Incarnation, family, revKey(mustUint(meta.Head)))
			if err != nil || !ready {
				t.Fatalf("initial ancestor family ready=%t err=%v", ready, err)
			}
			changed := []model.Ref{refs[0]}
			switch mode {
			case "own-chain-corrupt", "ancestor-corrupt", "move-stale-descendant", "delete-parent":
				changed = []model.Ref{refs[1]}
			case "mixed-stale-descendant":
				changed = []model.Ref{refs[0], refs[1]}
			case "move-complete":
				changed = []model.Ref{refs[1], refs[2]}
			case "delete-leaf":
				changed = []model.Ref{refs[2]}
			case "insert":
				changed = []model.Ref{testRef("scope-inserted")}
				parents[changed[0].ID] = &refs[1]
			}
			if strings.Contains(mode, "move") || mode == "mixed-stale-descendant" {
				parents[refs[1].ID] = &refs[4]
			}
			if mode == "cycle" {
				parents[refs[0].ID] = &refs[2]
			}
			decode, project := parentScopeProjection(meta.Incarnation, parents)
			// Projection keys are stable across the topology change. Acquire
			// the full encoded-key-sorted set before Head through the ordinary
			// writer helper, including tombstone and replacement arms.
			var locks []driver.LockKey
			var closures []driver.Sentinel
			for _, ref := range changed {
				closure, err := driver.NewClosureSentinel(ref.ID)
				if err != nil {
					t.Fatal(err)
				}
				locks = append(locks, driver.LockKey{Kind: driver.LockSentinel, Sentinel: closure})
				closures = append(closures, closure)
				record := driver.Record{Ref: ref, Kind: "bead", Body: []byte(`{}`)}
				metadata, err := decode(record)
				if err != nil {
					t.Fatal(err)
				}
				rows, err := project.Decode(record, nil)
				if err != nil {
					t.Fatal(err)
				}
				for _, row := range append(metadata.Selection, rows...) {
					key := driver.DerivedRowKey{Table: row.Table, Key: row.Row.Key}
					locks = append(locks, driver.LockKey{Kind: driver.LockDerived, Derived: &key})
				}
			}
			tx, h := beginSourceTestWithLocks(t, s, lease, locks, true, changed...)
			if err := tx.BumpSentinels(ctx, closures); err != nil {
				t.Fatal(err)
			}
			if mode == "title-untouched-descendant" {
				// A test-only SQL perturbation distinguishes which proof the
				// real Commit executes. This unrelated descendant was already
				// certified and is not part of the title-only delta. A broad
				// root-seeded proof would see it and invalidate coverage; the
				// required local proof must not visit it. The full-expansion
				// negative control below still has to find this exact defect.
				leafKey := keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &refs[2]})
				if _, err := s.db.ExecContext(ctx, `UPDATE selection_bead_history SET parent_key=NULL WHERE record_key=? AND end_revision IS NULL`, leafKey); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "ancestor-corrupt" {
				// Corrupt an upward dependency while keeping canonical Parent
				// unchanged. Own-chain scoping must still inspect this ancestor.
				rootKey := keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &refs[0]})
				otherKey := keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &refs[4]})
				if _, err := s.db.ExecContext(ctx, `UPDATE selection_bead_history SET parent_key=? WHERE record_key=? AND end_revision IS NULL`, otherKey, rootKey); err != nil {
					t.Fatal(err)
				}
			}
			for _, ref := range changed {
				deleting := strings.HasPrefix(mode, "delete-")
				if deleting {
					if err := tx.Delete(ctx, ref); err != nil {
						t.Fatal(err)
					}
				} else if mode != "derived-only" {
					if err := tx.PutGraphRecord(ctx, driver.Record{Ref: ref, Kind: "bead", Body: []byte(`{"title":"changed"}`)}, parents[ref.ID], nil); err != nil {
						t.Fatal(err)
					}
				}
				var record driver.Record
				if deleting || mode == "derived-only" {
					rows, err := tx.Get(ctx, []model.Ref{ref})
					if deleting {
						// Keys do not depend on deleted payloads; the tombstone
						// supplies no typed replacement annotation.
						record = driver.Record{Ref: ref, Kind: "bead", Body: []byte(`{}`)}
					} else if err != nil || len(rows) != 1 {
						t.Fatalf("derived-only source read rows=%d err=%v", len(rows), err)
					} else {
						record = rows[0]
					}
				} else {
					record = *tx.records[keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})]
				}
				metadata, err := decode(record)
				if err != nil {
					t.Fatal(err)
				}
				rows, err := project.Decode(record, nil)
				if err != nil {
					t.Fatal(err)
				}
				if mode == "own-chain-corrupt" {
					rows[0].Row.Index.Facts.Rows[1].Target = &refs[4]
				}
				for _, row := range append(metadata.Selection, rows...) {
					if deleting {
						err = tx.PutSelectionDerived(ctx, row.Table, []driver.DerivedRow{{Key: row.Row.Key}}, nil)
					} else {
						err = tx.PutSelectionDerived(ctx, row.Table, nil, []driver.DerivedRow{row.Row})
					}
					if err != nil {
						t.Fatal(err)
					}
				}
			}
			entry := commitSourceTest(t, tx, h)
			ready, err = selectionFamilyReady(ctx, s.db, meta.Incarnation, family, revKey(mustUint(entry.Revision)))
			wantReady := mode == "title" || mode == "title-untouched-descendant" || mode == "derived-only" || mode == "move-complete" || mode == "delete-leaf" || mode == "insert"
			if err != nil || ready != wantReady {
				t.Fatalf("%s final ancestor readiness=%t want=%t err=%v", mode, ready, wantReady, err)
			}
			if mode == "title-untouched-descendant" {
				rootKey := keyID(driver.LockKey{Kind: driver.LockRecord, Ref: &refs[0]})
				valid, err := verifySQLiteSelectionParentChainsAffected(ctx, s.db, meta.Incarnation, revKey(mustUint(entry.Revision)), []string{rootKey})
				if err != nil || valid {
					t.Fatalf("full-descendant negative control missed untouched defect: valid=%t err=%v", valid, err)
				}
				t.Log("real title-only Commit retained local-chain proof; full-descendant SQL control refused the untouched descendant")
			}
			// A new invalid overlay must not erase the old certified snapshot.
			oldReady, err := selectionFamilyReady(ctx, s.db, meta.Incarnation, family, revKey(mustUint(meta.Head)))
			if err != nil || !oldReady {
				t.Fatalf("%s lost prior historical coverage=%t err=%v", mode, oldReady, err)
			}
		})
	}
}
