package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This finite PG control publishes two real 1000-record transactions. It is
// independent of W9's unchanged 100000-record receiver, budgets and assertions.
func TestSelectionPublicationCensusBatchesAndFullDomain(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	renewCtx, cancel := context.WithCancel(ctx)
	renewed := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(driver.LeaseTTL / 3)
		defer ticker.Stop()
		for {
			select {
			case <-renewCtx.Done():
				renewed <- nil
				return
			case <-ticker.C:
				if err := lease.Renew(renewCtx); err != nil {
					if renewCtx.Err() != nil {
						err = nil
					}
					renewed <- err
					return
				}
			}
		}
	}()
	// Cancel and join before the existing lease/store cleanup runs.
	t.Cleanup(func() {
		cancel()
		if err := <-renewed; err != nil {
			t.Errorf("production-cadence renewal failed: %v", err)
		}
	})

	var first *reader
	for batch := 0; batch < 2; batch++ {
		if !t.Run(fmt.Sprintf("batch_%d", batch+1), func(t *testing.T) {
			publishSelectionCensusBatch(t, s, lease, batch*1000)
			r := testRead(t, s, nil).(*reader)
			valid, err := r.selectionFamilyValidity(ctx, r.at, false)
			if err != nil {
				t.Fatal(err)
			}
			for _, family := range selectionFamilies() {
				if !valid[family] {
					t.Fatalf("complete published family unavailable: %v", family)
				}
				requirePGFamily(t, r, family, r.at, true)
			}
			p := pgStartupCountPlan(r)
			got, err := r.Select(ctx, p)
			if err != nil || got.Total != uint64((batch+1)*1000) {
				t.Fatalf("published source count=%+v err=%v", got, err)
			}
			explainSelectionPublicationCensus(t, r, (batch+1)*1000)
		}) {
			return
		}
		if batch == 0 {
			// Own this snapshot at the parent scope so it outlives batch_1.
			first = testRead(t, s, nil).(*reader)
		}
	}
	if got, err := first.Select(ctx, pgStartupCountPlan(first)); err != nil || got.Total != 1000 {
		t.Fatalf("Commit2 changed the retained first-batch snapshot: %+v %v", got, err)
	}
	if err := lease.Renew(ctx); err != nil {
		t.Fatalf("real renewal after both publications: %v", err)
	}

	// Corrupt an otherwise complete, hidden source in a disposable SQL
	// transaction. Each proof still examines all sources, independently of any
	// candidate predicate, and each mutation is rolled back before the next.
	current := testRead(t, s, nil).(*reader)
	hidden, _ := recordKey(testRef("pl-census-1999"))
	for _, tc := range []struct {
		name, sql string
		invalid   []int
	}{
		{"missing_sort", `DELETE FROM plug_postgres.selection_rows WHERE source_key=$1 AND family_table='sort_key'`, []int{0, 3}},
		{"duplicate_sort", `INSERT INTO plug_postgres.selection_rows(incarnation,start_revision,` + selectionColumns + `,annotation) SELECT incarnation,start_revision,row_key||decode('01','hex'),source_key,source_revision,source_present,source_digest,bead_key,ref,bead_id,bead_type,project,parent_key,status,title,template_value,priority,leaf,fact_kind,family_table,source_kind,source_table,value_digest,layout,annotation FROM plug_postgres.selection_rows WHERE source_key=$1 AND family_table='sort_key'`, []int{0, 3}},
		{"missing_empty_fact_envelope", `DELETE FROM plug_postgres.selection_rows WHERE source_key=$1 AND family_table='derived_attribute'`, []int{1}},
		{"missing_header", `DELETE FROM plug_postgres.selection_sources WHERE key=$1`, []int{0, 1, 2, 3}},
		{"stale_digest", `UPDATE plug_postgres.selection_sources SET digest=decode('ff','hex') WHERE key=$1`, []int{0, 1, 2, 3}},
		{"wrong_bead_identity", `UPDATE plug_postgres.selection_rows SET bead_key=decode('ff','hex') WHERE source_key=$1 AND family_table='sort_key'`, []int{0, 3}},
		{"broken_parent_chain", `UPDATE plug_postgres.selection_facts f SET target_key=decode('ff','hex') FROM plug_postgres.selection_rows r WHERE r.incarnation=f.incarnation AND r.row_key=f.row_key AND r.start_revision=f.start_revision AND r.source_key=$1 AND r.family_table='ancestor_closure'`, []int{3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()
			result, err := tx.ExecContext(ctx, tc.sql, []byte(hidden))
			if err != nil {
				t.Fatal(err)
			}
			if n, err := result.RowsAffected(); err != nil || n != 1 {
				t.Fatalf("hidden-source mutation affected %d rows: %v", n, err)
			}
			view := &reader{sql: tx, meta: current.meta, at: current.at}
			valid, err := view.selectionFamilyValidity(ctx, view.at, false)
			if err != nil {
				t.Fatal(err)
			}
			for i, family := range selectionFamilies() {
				want := true
				for _, invalid := range tc.invalid {
					want = want && invalid != i
				}
				if valid[family] != want {
					t.Fatalf("full-domain family %v validity=%t want=%t", family, valid[family], want)
				}
			}
		})
	}
}

func publishSelectionCensusBatch(t *testing.T, s *Store, lease driver.ActiveLease, offset int) {
	t.Helper()
	ctx := context.Background()
	plans := make([]*pipeline.SourceProjectionPlan, 1000)
	records := make([]driver.Record, 1000)
	closures := make([]driver.DerivedRow, 1000)
	keys := map[string]driver.LockKey{}
	sentinels := map[driver.Sentinel]bool{}
	for i := range records {
		ref := testRef(fmt.Sprintf("pl-census-%04d", offset+i))
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
		closures[i], err = pipeline.AncestorRow(ref.ID, ref.ID, 0)
		if err != nil {
			t.Fatal(err)
		}
		key := driver.LockKey{Kind: driver.LockDerived, Derived: &driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: closures[i].Key}}
		id, _ := keyID(key)
		keys[id] = key
	}
	locks := make([]driver.LockKey, 0, len(keys))
	for _, key := range keys {
		locks = append(locks, key)
	}
	tx, h := beginTest(t, s, lease, locks...)
	add := map[driver.DerivedTable][]driver.DerivedRow{}
	for i, record := range records {
		if err := tx.(driver.GraphSourceWriter).PutGraphRecord(ctx, record, nil, nil); err != nil {
			t.Fatal(err)
		}
		rows, err := plans[i].SelectionRows(h.Incarnation, model.Natural64(0))
		if err != nil || len(rows) != 3 {
			t.Fatalf("core projection rows=%d err=%v", len(rows), err)
		}
		rowKeys, err := pipeline.SourceSelectionRows(record.Ref)
		if err != nil {
			t.Fatal(err)
		}
		for j, row := range rows {
			add[rowKeys[j].Table] = append(add[rowKeys[j].Table], row)
		}
		// The trusted adapter fixture supplies the complete self-only chain;
		// the other three annotations come from the real core source producer.
		row, ref, target := closures[i], record.Ref, record.Ref
		row.Index = &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}, Incarnation: h.Incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(row.Value), Source: rows[0].Index.Source, Facts: &driver.SelectionFacts{Kind: driver.SelectionAncestorFact, Rows: []driver.SelectionFact{{Kind: driver.SelectionAncestorFact, Ref: &ref, Target: &target, Depth: 0}}}}
		add[driver.DerivedAncestorClosure] = append(add[driver.DerivedAncestorClosure], row)
		h.Changes = append(h.Changes, model.ChangeRecord{Ref: ref, Op: model.OpCreate, FromRevision: model.Natural64(0)})
	}
	for _, table := range []driver.DerivedTable{driver.DerivedSortKey, driver.DerivedAttribute, driver.DerivedReferenceIndex, driver.DerivedAncestorClosure} {
		if err := pipeline.PutSelectionDerived(ctx, tx, table, nil, add[table]); err != nil {
			t.Fatal(err)
		}
	}
	bumps := make([]driver.Sentinel, 0, len(sentinels))
	for sentinel := range sentinels {
		bumps = append(bumps, sentinel)
	}
	if err := tx.BumpSentinels(ctx, bumps); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if _, err := tx.Commit(ctx, h); err != nil {
		t.Fatalf("real 1000-record Commit offset=%d elapsed=%s: %v", offset, time.Since(started), err)
	}
	t.Logf("W6_PUBLICATION_CENSUS batch_records=1000 completed_records=%d commit_elapsed=%s lease_ttl=%s renewal_cadence=%s", offset+1000, time.Since(started), driver.LeaseTTL, driver.LeaseTTL/3)
}

func explainSelectionPublicationCensus(t *testing.T, r *reader, population int) {
	t.Helper()
	query, args, err := r.compileSelectionFamilyValidity(context.Background(), r.at, false)
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := r.sql.QueryRowContext(context.Background(), "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+query, args...).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var plan []map[string]any
	if err := json.Unmarshal(raw, &plan); err != nil || len(plan) != 1 {
		t.Fatalf("publication proof plan=%s err=%v", raw, err)
	}
	producer, scans, visits := 0, 0, float64(0)
	headerScans, headerVisits := 0, float64(0)
	liveScans, liveVisits := 0, float64(0)
	var inspect func(any) bool
	inspect = func(value any) bool {
		switch node := value.(type) {
		case map[string]any:
			if name, _ := node["Subplan Name"].(string); name == "CTE proof_census" {
				producer++
				if loops, _ := node["Actual Loops"].(float64); loops != 1 {
					t.Fatalf("census aggregate executed %v times: %s", loops, raw)
				}
			}
			if name, _ := node["CTE Name"].(string); name == "proof_census" {
				scans++
				rows, _ := node["Actual Rows"].(float64)
				loops, _ := node["Actual Loops"].(float64)
				removed, _ := node["Rows Removed by Filter"].(float64)
				visits += loops * (rows + removed)
			}
			cte, _ := node["CTE Name"].(string)
			relation, _ := node["Relation Name"].(string)
			if cte == "proof_headers" || relation == "selection_sources" {
				headerScans++
				rows, _ := node["Actual Rows"].(float64)
				loops, _ := node["Actual Loops"].(float64)
				removed, _ := node["Rows Removed by Filter"].(float64)
				headerVisits += loops * (rows + removed)
			}
			liveInput := false
			for _, child := range node {
				if inspect(child) {
					liveInput = true
				}
			}
			live := cte == "proof_live" || relation == "records"
			nodeType, _ := node["Node Type"].(string)
			// Include executor caches above live input, so an inlined records
			// scan cannot hide repeated full-domain reads in Materialize.
			if live || liveInput && (nodeType == "Materialize" || nodeType == "Memoize") {
				liveScans++
				rows, _ := node["Actual Rows"].(float64)
				loops, _ := node["Actual Loops"].(float64)
				removed, _ := node["Rows Removed by Filter"].(float64)
				liveVisits += loops * (rows + removed)
			}
			return live || liveInput
		case []any:
			live := false
			for _, child := range node {
				if inspect(child) {
					live = true
				}
			}
			return live
		}
		return false
	}
	for _, root := range plan {
		inspect(root)
	}
	// Four complete families can each scan the four-family grouped census;
	// allow planner rounding, but reject a census rescan for each live source.
	if producer != 1 || scans != 4 || visits > float64(32*population) {
		t.Fatalf("publication census producer=%d scans=%d visits=%v population=%d: %s", producer, scans, visits, population, raw)
	}
	// Include every header alias: the fixture has about seven headers per bead,
	// and the proof may scan that full domain for several family joins. Allow
	// that linear work, but reject full-header rescans for every projection row.
	if headerScans == 0 || headerVisits > float64(128*population) {
		t.Fatalf("publication header scans=%d visits=%v population=%d: %s", headerScans, headerVisits, population, raw)
	}
	// Count complete live-domain scans and per-key freshness lookups across
	// all four families; reject a full live-domain rescan per projection row.
	if liveScans == 0 || liveVisits > float64(32*population) {
		t.Fatalf("publication live scans=%d visits=%v population=%d: %s", liveScans, liveVisits, population, raw)
	}
	t.Logf("W6_LIVE_PROOF_WORK phase=publication_family_proof fixture_beads=%d live_record_scans=%d live_record_row_visits=%v bound=%d qualification=FINITE_CONTROL", population, liveScans, liveVisits, 32*population)
	t.Logf("W6_HEADER_PROOF_WORK phase=publication_family_proof fixture_beads=%d source_header_scans=%d source_header_row_visits=%v bound=%d qualification=FINITE_CONTROL", population, headerScans, headerVisits, 128*population)
	t.Logf("W6_EXPLAIN_ANALYZE phase=publication_family_proof fixture_beads=%d census_producer=%d census_scans=%d census_row_visits=%v qualification=FINITE_CONTROL actual_plan=%s", population, producer, scans, visits, raw)
}
