package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/authz"
	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/plugins/authzfixed"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// W9 installs its real typed authority source before publishing any beads.
// That first publication advances Head and the mutation generation even though
// its complete bead domain is empty. Use the same source/projection producers,
// ACL dependency and sentinel domains; never inject a certificate through SQL.
func publishSelectionAuthorityPrelude(t *testing.T, s *Store, lease driver.ActiveLease) {
	t.Helper()
	ctx := context.Background()
	meta, err := s.Meta(ctx)
	if err != nil || !meta.Head.IsZero() {
		t.Fatalf("authority prelude must be first publication: %+v %v", meta, err)
	}
	ns := authz.Namespace{StoreID: string(meta.StoreID), Incarnation: meta.Incarnation, InstallationID: "fixed", Authority: testRef("authority")}
	facts := authzfixed.Facts{Namespace: ns, Projects: []authzfixed.Project{
		{Ref: testRef("project-visible"), Name: "visible-name", InstallationID: "fixed", Active: true},
		{Ref: testRef("project-hidden"), Name: "hidden-name", InstallationID: "fixed", Active: true},
	}, Grants: []authz.Grant{{Subject: authz.SubjectRef{Kind: "user", ID: "user:w6-reader"}, Scope: authz.Scope{Kind: authz.ProjectScope, Ref: testRef("project-hidden")}, Role: "involved"}}}
	body, err := authzfixed.FactsSourceBytes(facts)
	if err != nil {
		t.Fatal(err)
	}
	keys, err := authzfixed.SelectionRows(ns)
	if err != nil {
		t.Fatal(err)
	}
	acl, err := driver.NewReadFact(authz.ACLDependency(ns).Key)
	if err != nil {
		t.Fatal(err)
	}
	locks := []driver.LockKey{{Kind: driver.LockReadFact, Fact: acl}}
	for _, key := range []driver.DerivedRowKey{keys.Source, keys.Grants, keys.Projects, keys.Teams} {
		key := key
		locks = append(locks, driver.LockKey{Kind: driver.LockDerived, Derived: &key})
	}
	bumps := []driver.Sentinel{keys.AttributeScope}
	for _, scope := range []authz.Scope{{Kind: authz.AuthorityScope, Ref: ns.Authority}, {Kind: authz.ProjectScope, Ref: facts.Projects[0].Ref}, {Kind: authz.ProjectScope, Ref: facts.Projects[1].Ref}} {
		dependency, err := authz.GrantDependency(ns, scope)
		if err != nil {
			t.Fatal(err)
		}
		sentinel, err := driver.NewSentinel(dependency.Key)
		if err != nil {
			t.Fatal(err)
		}
		bumps = append(bumps, sentinel)
	}
	for _, sentinel := range bumps {
		locks = append(locks, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	}
	w, h := beginTest(t, s, lease, locks...)
	defer w.Close()
	h.Kind = model.ChangesetDerived
	if err := w.PutDerived(ctx, keys.Source.Table, nil, []driver.DerivedRow{{Key: keys.Source.Key, Value: body}}); err != nil {
		t.Fatal(err)
	}
	source := driver.SelectionSource{Key: driver.LockKey{Kind: driver.LockDerived, Derived: &keys.Source}, Revision: model.Natural64(0), Present: true, Digest: driver.SelectionDigest(body)}
	rows, err := authzfixed.SelectionProjection(facts, source)
	if err != nil {
		t.Fatal(err)
	}
	for i, key := range []driver.DerivedRowKey{keys.Grants, keys.Projects, keys.Teams} {
		if rows[i].Index == nil {
			t.Fatal("authority prelude lost typed producer annotation")
		}
		if err := pipeline.PutSelectionDerived(ctx, w, key.Table, nil, rows[i:i+1]); err != nil {
			t.Fatal(err)
		}
	}
	if err := w.BumpSentinels(ctx, bumps); err != nil {
		t.Fatal(err)
	}
	if err := w.BumpReadFacts(ctx, []driver.ReadFact{acl}); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(ctx, h); err != nil {
		t.Fatal(err)
	}
	if !w.(*transaction).selectionProofSeed || len(w.(*transaction).records) != 0 {
		t.Fatal("authority prelude did not exercise trusted empty-domain publication")
	}
	r := pgStartupRead(t, s)
	defer r.Close()
	requireSelectionCertificate(t, r, true)
	for _, family := range selectionFamilies() {
		requirePGFamily(t, r, family, r.at, true)
	}
	t.Logf("W6_AUTHORITY_PRELUDE head=%s complete_empty_certificate=true", r.meta.Head)
}

func TestSelectionPartialFamilyAuthorityPrelude(t *testing.T) {
	s, lease := openCommitQueryTest(t, selectionPublicationQueryTracer{})
	publishSelectionAuthorityPrelude(t, s, lease)
	publishSelectionPartialBatch(t, s, lease, 0, 3)
	r := pgStartupRead(t, s)
	families, _, err := r.selectionFamilyCertificate(context.Background())
	if err != nil || families != 7 {
		t.Fatalf("authority-first ordinary publication did not carry proven families: %d %v", families, err)
	}
	requireSelectionCertificate(t, r, false)
	for i, family := range selectionFamilies() {
		requirePGFamily(t, r, family, r.at, i != 3)
	}
}

// This is the ordinary three-family producer, with no authored ancestor
// annotation and no SQL seeding or certificate mutation. Every batch publishes
// source, history and typed envelopes through the real fenced Commit path.
func publishSelectionPartialBatch(t *testing.T, s *Store, lease driver.ActiveLease, offset, count int) {
	t.Helper()
	ctx := context.Background()
	plans := make([]*pipeline.SourceProjectionPlan, count)
	records := make([]driver.Record, count)
	keys := map[string]driver.LockKey{}
	sentinels := map[driver.Sentinel]bool{}
	for i := range records {
		ref := testRef(fmt.Sprintf("pl-partial-%06d", offset+i))
		body, err := json.Marshal(pipeline.Document{ID: ref.ID, Type: "issue", Fields: map[string]any{}})
		if err != nil {
			t.Fatal(err)
		}
		records[i] = driver.Record{Ref: ref, Kind: "bead", Body: body}
		plans[i], err = pipeline.PrepareSourceRecord(records[i])
		if err != nil {
			t.Fatal(err)
		}
		for _, key := range plans[i].LockKeys() {
			id, _ := keyID(key)
			keys[id] = key
		}
		for _, sentinel := range plans[i].Sentinels() {
			sentinels[sentinel] = true
		}
	}
	locks := make([]driver.LockKey, 0, len(keys))
	for _, key := range keys {
		locks = append(locks, key)
	}
	w, h := beginTest(t, s, lease, locks...)
	defer w.Close()
	add := map[driver.DerivedTable][]driver.DerivedRow{}
	for i, record := range records {
		if err := w.(driver.GraphSourceWriter).PutGraphRecord(ctx, record, nil, nil); err != nil {
			t.Fatal(err)
		}
		rows, err := plans[i].SelectionRows(h.Incarnation, model.Natural64(0))
		if err != nil || len(rows) != 3 {
			t.Fatalf("ordinary core projection=%d: %v", len(rows), err)
		}
		rowKeys, err := pipeline.SourceSelectionRows(record.Ref)
		if err != nil {
			t.Fatal(err)
		}
		for j, row := range rows {
			add[rowKeys[j].Table] = append(add[rowKeys[j].Table], row)
		}
		h.Changes = append(h.Changes, model.ChangeRecord{Ref: record.Ref, Op: model.OpCreate, FromRevision: model.Natural64(0)})
	}
	for _, table := range []driver.DerivedTable{driver.DerivedSortKey, driver.DerivedAttribute, driver.DerivedReferenceIndex} {
		if err := pipeline.PutSelectionDerived(ctx, w, table, nil, add[table]); err != nil {
			t.Fatal(err)
		}
	}
	bumps := make([]driver.Sentinel, 0, len(sentinels))
	for sentinel := range sentinels {
		bumps = append(bumps, sentinel)
	}
	if err := w.BumpSentinels(ctx, bumps); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	timing := newSelectionCommitTiming(lease.(*activeLease), started)
	_, err := w.Commit(context.WithValue(ctx, selectionCommitTimingKey{}, timing), h)
	timing.log(t, offset, time.Since(started), err)
	if err != nil {
		t.Fatalf("ordinary three-family Commit offset=%d count=%d: %v", offset, count, err)
	}
}

func TestSelectionPartialFamilyCertificate(t *testing.T) {
	ctx := context.Background()
	s, lease := openCommitQueryTest(t, selectionPublicationQueryTracer{})
	publishSelectionPartialBatch(t, s, lease, 0, 3)
	before := pgStartupRead(t, s)
	families, generation, err := before.selectionFamilyCertificate(ctx)
	if err != nil || families != 7 {
		t.Fatalf("ordinary partial certificate=%d: %v", families, err)
	}
	requireSelectionCertificate(t, before, false)
	for i, family := range selectionFamilies() {
		requirePGFamily(t, before, family, before.at, i != 3)
	}
	t.Run("stale_generation_refused_inside_statement", func(t *testing.T) {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		view := &reader{sql: tx, meta: before.meta, at: before.at}
		if _, err := tx.ExecContext(ctx, `DELETE FROM plug_postgres.selection_facts WHERE false`); err != nil {
			t.Fatal(err)
		}
		query, args, err := view.compileSelectionCarriedValidity(ctx, view.at, false, [][]byte{}, generation, families)
		valid, err := view.readSelectionValidity(ctx, query, args, err)
		if err != nil {
			t.Fatal(err)
		}
		for family, yes := range valid {
			if yes {
				t.Errorf("stale partial token granted family %v", family)
			}
		}
	})
	t.Run("hidden_raw_source_forces_full_proof", func(t *testing.T) {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		if _, err := tx.ExecContext(ctx, `DELETE FROM plug_postgres.selection_rows WHERE family_table='sort_key' AND source_key=$1`, []byte(selectionRefKey(testRef("pl-partial-000002")))); err != nil {
			t.Fatal(err)
		}
		view := &reader{sql: tx, meta: before.meta, at: before.at}
		view.writer = &transaction{reader: view}
		mask, _, err := view.selectionFamilyCertificate(ctx)
		if err != nil || mask != 0 {
			t.Fatalf("raw mutation retained partial bits=%d: %v", mask, err)
		}
		valid, err := view.selectionAdmissionValidity(ctx, view.at)
		if err != nil || valid[selectionBeadFamily()] || valid[selectionFamilies()[3]] {
			t.Fatalf("hidden raw corruption escaped complete fallback: %v %v", valid, err)
		}
	})
	t.Run("unknown_ancestor_uses_complete_domain_and_can_be_repaired", func(t *testing.T) {
		rows := []driver.DerivedRow{}
		locks := []driver.LockKey{}
		for i := 0; i < 3; i++ {
			ref := testRef(fmt.Sprintf("pl-partial-%06d", i))
			key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: ref.ID}
			sortRows, err := before.DerivedRows(ctx, key.Table, key.Key)
			if err != nil || len(sortRows) != 1 {
				t.Fatalf("real prior sort annotation=%v: %v", sortRows, err)
			}
			row, err := pipeline.AncestorRow(ref.ID, ref.ID, 0)
			if err != nil {
				t.Fatal(err)
			}
			row.Index = &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}, Incarnation: before.meta.Incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(row.Value), Source: sortRows[0].Index.Source, Facts: &driver.SelectionFacts{Kind: driver.SelectionAncestorFact, Rows: []driver.SelectionFact{{Kind: driver.SelectionAncestorFact, Ref: &ref, Target: &ref, Depth: 0}}}}
			rows = append(rows, row)
			rowKey := row.Index.Row
			locks = append(locks, testLock(ref), driver.LockKey{Kind: driver.LockDerived, Derived: &rowKey})
		}
		w, _ := beginTest(t, s, lease, locks...)
		defer w.Close()
		if err := pipeline.PutSelectionDerived(ctx, w, driver.DerivedAncestorClosure, nil, rows[:1]); err != nil {
			t.Fatal(err)
		}
		valid, err := w.(*transaction).reader.selectionAdmissionValidity(ctx, before.at)
		if err != nil || valid[selectionFamilies()[3]] || !valid[selectionBeadFamily()] {
			t.Fatalf("one staged owner incorrectly proved untouched ancestors: %v %v", valid, err)
		}
		if err := pipeline.PutSelectionDerived(ctx, w, driver.DerivedAncestorClosure, nil, rows[1:]); err != nil {
			t.Fatal(err)
		}
		valid, err = w.(*transaction).reader.selectionAdmissionValidity(ctx, before.at)
		if err != nil || !valid[selectionFamilies()[3]] {
			t.Fatalf("complete staged repair could not prove unknown family: %v %v", valid, err)
		}
	})
	publishSelectionPartialBatch(t, s, lease, 3, 3)
	current := pgStartupRead(t, s)
	mask, _, err := current.selectionFamilyCertificate(ctx)
	if err != nil || mask != 7 {
		t.Fatalf("second ordinary publication lost positive bits=%d: %v", mask, err)
	}
	requireSelectionCertificate(t, current, false)
	// A committed out-of-band statement permanently removes reuse authority;
	// an ordinary full audit cannot launder it into a new partial certificate.
	if _, err := s.db.ExecContext(ctx, `DELETE FROM plug_postgres.selection_facts WHERE false`); err != nil {
		t.Fatal(err)
	}
	w, h := beginTest(t, s, lease)
	if _, err := w.Commit(ctx, h); err != nil {
		t.Fatal(err)
	}
	latest := pgStartupRead(t, s)
	mask, _, err = latest.selectionFamilyCertificate(ctx)
	if err != nil || mask != 0 {
		t.Fatalf("ordinary publication laundered raw generation into bits=%d: %v", mask, err)
	}
	if err := pipeline.BuildSourceIndexes(ctx, s, lease); err != nil {
		t.Fatal(err)
	}
	// Startup cannot invent the missing logical ancestor closure. Supply every
	// real self row first; only then can its complete canonical proof mint all
	// four bits after the raw generation has invalidated ordinary reuse.
	requireSelectionCertificate(t, pgStartupRead(t, s), false)
	closures := []driver.DerivedRow{}
	locks := []driver.LockKey{}
	bumps := []driver.Sentinel{}
	for i := 0; i < 6; i++ {
		ref := testRef(fmt.Sprintf("pl-partial-%06d", i))
		row, err := pipeline.AncestorRow(ref.ID, ref.ID, 0)
		if err != nil {
			t.Fatal(err)
		}
		key := driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}
		sentinel, err := driver.NewClosureSentinel(ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		closures = append(closures, row)
		locks = append(locks, testLock(ref), driver.LockKey{Kind: driver.LockDerived, Derived: &key}, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
		bumps = append(bumps, sentinel)
	}
	w, h = beginTest(t, s, lease, locks...)
	defer w.Close()
	if err := w.PutDerived(ctx, driver.DerivedAncestorClosure, nil, closures); err != nil {
		t.Fatal(err)
	}
	if err := w.BumpSentinels(ctx, bumps); err != nil {
		t.Fatal(err)
	}
	if _, err := w.Commit(ctx, h); err != nil {
		t.Fatal(err)
	}
	var buildErr error
	if err := pipeline.BuildSourceIndexes(ctx, pgStartupCaptureStore{Store: s, err: &buildErr}, lease); err != nil {
		t.Fatal(err)
	}
	if buildErr != nil {
		t.Fatalf("complete canonical rebuild refused after closure repair: %v", buildErr)
	}
	requireSelectionCertificate(t, pgStartupRead(t, s), true)
	// Old snapshot authority and availability remain pinned to their revision.
	mask, _, err = before.selectionFamilyCertificate(ctx)
	if err != nil || mask != 7 {
		t.Fatalf("later publication changed old snapshot bits=%d: %v", mask, err)
	}
}

func TestSelectionPartialFamilyProofBoundedByBatch(t *testing.T) {
	s, lease := openCommitQueryTest(t, selectionPublicationQueryTracer{})
	publishSelectionAuthorityPrelude(t, s, lease)
	for offset := 0; offset < 100000; offset += 1000 {
		publishSelectionPartialBatch(t, s, lease, offset, 1000)
		population := offset + 1000
		if population != 33000 && population != 85000 && population != 100000 {
			continue
		}
		if !t.Run(fmt.Sprintf("population_%d", population), func(t *testing.T) {
			r := pgStartupRead(t, s)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			families, generation, err := r.selectionFamilyCertificate(ctx)
			if err != nil || families != 7 {
				t.Fatalf("actual three-family certificate=%d: %v", families, err)
			}
			requireSelectionCertificate(t, r, false)
			rows, err := r.sql.QueryContext(ctx, `SELECT key FROM plug_postgres.selection_sources WHERE incarnation=$1 AND start_revision=$2 ORDER BY key`, r.meta.Incarnation, r.meta.Head.String())
			if err != nil {
				t.Fatal(err)
			}
			sources := [][]byte{}
			for rows.Next() {
				var key []byte
				if err := rows.Scan(&key); err != nil {
					_ = rows.Close()
					t.Fatal(err)
				}
				sources = append(sources, key)
			}
			err = rows.Err()
			_ = rows.Close()
			if err != nil || len(sources) <= 1000 {
				t.Fatalf("complete actual mutation domain=%d: %v", len(sources), err)
			}
			query, args, err := r.compileSelectionCarriedValidity(ctx, r.at, false, sources, generation, families)
			if err != nil {
				t.Fatal(err)
			}
			var raw []byte
			if err := r.sql.QueryRowContext(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+query, args...).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var plans []struct{ Plan map[string]any }
			if err := json.Unmarshal(raw, &plans); err != nil || len(plans) != 1 {
				t.Fatalf("explain=%s: %v", raw, err)
			}
			work := selectionPartialProofWork(plans[0].Plan)
			t.Logf("W6_PARTIAL_PROOF_PLAN population=%d affected_keys=%d actual_work=%v bound=%d plan=%s", population, len(sources), work, 256*1000, raw)
			if work > 256*1000 {
				t.Fatalf("three-family carried proof grows with store size: work=%v population=%d batch=1000", work, population)
			}
			valid, err := r.readSelectionValidity(ctx, query, args, nil)
			if err != nil {
				t.Fatal(err)
			}
			for i, family := range selectionFamilies() {
				if valid[family] != (i != 3) {
					t.Errorf("family=%v got=%t want=%t", family, valid[family], i != 3)
				}
			}
		}) {
			return
		}
	}
}

// Include physical accesses, rejected filter/join work and executor CTE/cache
// rescans, matching the existing finite full-proof regression's cost unit.
func selectionPartialProofWork(node map[string]any) float64 {
	number := func(key string) float64 { n, _ := node[key].(float64); return n }
	work := 0.0
	kind, _ := node["Node Type"].(string)
	_, physical := node["Relation Name"]
	if physical || kind == "CTE Scan" || kind == "Materialize" || kind == "Memoize" {
		work += (number("Actual Rows") + number("Rows Removed by Filter") + number("Rows Removed by Index Recheck")) * number("Actual Loops")
	}
	work += number("Rows Removed by Join Filter") * number("Actual Loops")
	if children, ok := node["Plans"].([]any); ok {
		for _, child := range children {
			if next, ok := child.(map[string]any); ok {
				work += selectionPartialProofWork(next)
			}
		}
	}
	return work
}
