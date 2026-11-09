package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/jackc/pgx/v5"
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

// Execute the production chain fragments against typed scalar fixtures. The
// real publication test above separately covers temporal headers and source
// freshness. This isolates the set/window rewrite's NULL, depth and edge rules.
func TestSelectionPublicationParentChainSetProof(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	const inputs = `WITH beads_input(bead_key,parent_key,source_key) AS (VALUES
 ('\x01'::bytea,NULL::bytea,'\x01'::bytea),
 ('\x02'::bytea,'\x01'::bytea,'\x02'::bytea),
 ('\x03'::bytea,'\x02'::bytea,'\x03'::bytea)),
 ancestors_input(source_key,ref_key,target_key,depth) AS (VALUES
 ('\x03'::bytea,'\x03'::bytea,'\x01'::bytea,2::bigint),
 ('\x02'::bytea,'\x02'::bytea,'\x02'::bytea,0::bigint),
 ('\x01'::bytea,'\x01'::bytea,'\x01'::bytea,0::bigint),
 ('\x03'::bytea,'\x03'::bytea,'\x03'::bytea,0::bigint),
 ('\x02'::bytea,'\x02'::bytea,'\x01'::bytea,1::bigint),
 ('\x03'::bytea,'\x03'::bytea,'\x02'::bytea,1::bigint)), `
	for _, tc := range []struct {
		name, beads, ancestors string
		valid                  bool
	}{
		{"empty", `SELECT * FROM beads_input WHERE false`, `SELECT * FROM ancestors_input WHERE false`, true},
		{"complete_three_levels", "", "", true},
		{"independent_roots", `SELECT bead_key,NULL::bytea,source_key FROM beads_input`, `SELECT * FROM ancestors_input WHERE depth=0`, true},
		{"duplicate_bead", `TABLE beads_input UNION ALL SELECT bead_key,parent_key,'\x04'::bytea FROM beads_input WHERE bead_key='\x01'::bytea`, "", false},
		{"duplicate_source", `SELECT bead_key,parent_key,CASE WHEN bead_key='\x03'::bytea THEN '\x02'::bytea ELSE source_key END FROM beads_input`, "", false},
		{"orphan_owner", "", `TABLE ancestors_input UNION ALL SELECT '\x04'::bytea,'\x04'::bytea,'\x01'::bytea,0::bigint`, false},
		{"wrong_source", "", `SELECT CASE WHEN ref_key='\x03'::bytea THEN '\x02'::bytea ELSE source_key END,ref_key,target_key,depth FROM ancestors_input`, false},
		{"missing_target", "", `SELECT source_key,ref_key,CASE WHEN depth=2 THEN '\xff'::bytea ELSE target_key END,depth FROM ancestors_input`, false},
		{"duplicate_depth", "", `TABLE ancestors_input UNION ALL SELECT '\x03'::bytea,'\x03'::bytea,'\x01'::bytea,1::bigint`, false},
		{"repeated_target", "", `SELECT source_key,ref_key,CASE WHEN depth=2 THEN '\x02'::bytea ELSE target_key END,depth FROM ancestors_input`, false},
		{"missing_self", "", `SELECT * FROM ancestors_input WHERE NOT(ref_key='\x03'::bytea AND depth=0)`, false},
		{"negative_depth", "", `SELECT source_key,ref_key,target_key,CASE WHEN depth=2 THEN -1 ELSE depth END FROM ancestors_input`, false},
		{"depth_gap", "", `SELECT source_key,ref_key,target_key,CASE WHEN depth=2 THEN 3 ELSE depth END FROM ancestors_input`, false},
		{"wrong_self", "", `SELECT source_key,ref_key,CASE WHEN ref_key='\x03'::bytea AND depth=0 THEN '\x02'::bytea ELSE target_key END,depth FROM ancestors_input`, false},
		{"stale_parent", `SELECT bead_key,CASE WHEN bead_key='\x03'::bytea THEN '\x01'::bytea ELSE parent_key END,source_key FROM beads_input`, "", false},
		{"missing_root", "", `SELECT * FROM ancestors_input WHERE depth<>2`, false},
		{"non_root_terminal", "", `SELECT * FROM ancestors_input WHERE NOT(ref_key='\x02'::bytea AND depth=1)`, false},
		{"root_successor", "", `TABLE ancestors_input UNION ALL SELECT '\x01'::bytea,'\x01'::bytea,'\x02'::bytea,1::bigint`, false},
		{"null_owner", "", `SELECT source_key,CASE WHEN depth=2 THEN NULL::bytea ELSE ref_key END,target_key,depth FROM ancestors_input`, false},
		{"null_target", "", `SELECT source_key,ref_key,CASE WHEN depth=2 THEN NULL::bytea ELSE target_key END,depth FROM ancestors_input`, false},
		{"minimum_depth", "", `SELECT source_key,ref_key,target_key,CASE WHEN depth=2 THEN '-9223372036854775808'::bigint ELSE depth END FROM ancestors_input`, false},
		{"maximum_depth", "", `SELECT source_key,ref_key,target_key,CASE WHEN depth=2 THEN '9223372036854775807'::bigint ELSE depth END FROM ancestors_input`, false},
		{"ancestors_without_beads", `SELECT * FROM beads_input WHERE false`, "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			beads, ancestors := tc.beads, tc.ancestors
			if beads == "" {
				beads = `TABLE beads_input`
			}
			if ancestors == "" {
				ancestors = `TABLE ancestors_input`
			}
			query := inputs + `proof_beads(bead_key,parent_key,source_key) AS MATERIALIZED (` + beads + `), proof_ancestors(source_key,ref_key,target_key,depth) AS MATERIALIZED (` + ancestors + `), ` + selectionParentChainRelationSQL + ` SELECT (` + selectionParentChainSQL + `)`
			var valid bool
			if err := s.db.QueryRowContext(ctx, query).Scan(&valid); err != nil {
				t.Fatal(err)
			}
			if valid != tc.valid {
				t.Fatalf("typed parent-chain proof=%t want=%t", valid, tc.valid)
			}
		})
	}
}

func publishSelectionCensusBatch(t *testing.T, s *Store, lease driver.ActiveLease, offset int, measured ...bool) {
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
	commitCtx := ctx
	var timing *selectionCommitTiming
	if len(measured) > 0 && measured[0] {
		timing = newSelectionCommitTiming(lease.(*activeLease), started)
		commitCtx = context.WithValue(ctx, selectionCommitTimingKey{}, timing)
	}
	_, err := tx.Commit(commitCtx, h)
	elapsed := time.Since(started)
	if timing != nil {
		// Observe and log after Commit even on failure, before Fatal exits this
		// fixture. Tracing only buffers fixed timing state during Commit; it
		// adds no SQL, heartbeat or external observer callback.
		timing.log(t, offset, elapsed, err)
	}
	if err != nil {
		t.Fatalf("real 1000-record Commit offset=%d elapsed=%s: %v", offset, elapsed, err)
	}
	t.Logf("W6_PUBLICATION_CENSUS batch_records=1000 completed_records=%d commit_elapsed=%s lease_ttl=%s renewal_cadence=%s", offset+1000, elapsed, driver.LeaseTTL, driver.LeaseTTL/3)
}

// Adapt W4 be2a73b9c's deferredPhaseTimings lifetime: closed vocabulary,
// fixed storage and no observer/logging until the driver boundary returns.
// These are actual pgx query boundaries, not plans, SQL executor row counts,
// nested trigger phases or a measurement of the server's lease expiry.
type selectionCommitTimingKey struct{}

var selectionCommitQueryPhases = [...]string{
	"lease_fence", "clock", "mutation_fence", "closure_discovery", "family_validity",
	"source_index", "record_reads", "records", "states", "selection_sources",
	"selection_rows", "selection_facts", "coverage", "certificate", "other",
}

type selectionCommitLeasePoint struct {
	AtNS       int64
	DeadlineNS int64
	Lost       bool
}

type selectionCommitQuerySample struct {
	Phase    string
	Start    selectionCommitLeasePoint
	End      selectionCommitLeasePoint
	Done     bool
	Failed   bool
	SQLState string
}

type selectionCommitQueryBucket struct {
	Phase                string
	Starts, Ends, Errors uint64
	ElapsedNS            int64
}

type selectionCommitTiming struct {
	mu                         sync.Mutex
	origin                     time.Time
	lease                      *activeLease
	before                     selectionCommitLeasePoint
	buckets                    [len(selectionCommitQueryPhases)]selectionCommitQueryBucket
	active                     selectionCommitQuerySample
	activePhase                int
	inflight                   bool
	last, longest, lastError   selectionCommitQuerySample
	firstLost                  selectionCommitLeasePoint
	lossInside, deadlineInside bool
	overlaps, unmatched        uint64
}

func newSelectionCommitTiming(lease *activeLease, started time.Time) *selectionCommitTiming {
	c := &selectionCommitTiming{origin: started, lease: lease}
	c.before = c.leasePoint()
	for i, phase := range selectionCommitQueryPhases {
		c.buckets[i].Phase = phase
	}
	return c
}

func (c *selectionCommitTiming) leasePoint() selectionCommitLeasePoint {
	c.lease.mu.Lock()
	defer c.lease.mu.Unlock()
	return selectionCommitLeasePoint{AtNS: time.Since(c.origin).Nanoseconds(), DeadlineNS: c.lease.timerDeadline.Sub(c.origin).Nanoseconds(), Lost: c.lease.ended()}
}

// Called only by pgx while the opted-in Commit is executing, under c.mu.
// A post-return sample alone must never be called loss inside Commit.
func (c *selectionCommitTiming) observeInside(point selectionCommitLeasePoint) {
	if !c.before.Lost && point.Lost && !c.lossInside {
		c.firstLost, c.lossInside = point, true
	}
	c.deadlineInside = c.deadlineInside || point.AtNS >= point.DeadlineNS
}

func selectionCommitQueryPhase(query string) int {
	query = strings.TrimSpace(query)
	switch {
	case strings.HasPrefix(query, "SELECT epoch::text,owner,") && strings.Contains(query, "FROM plug_postgres.lease"):
		return 0
	case query == "SELECT clock_timestamp()":
		return 1
	case strings.HasPrefix(query, "WITH roots AS MATERIALIZED"):
		return 3
	case strings.HasPrefix(query, "WITH proof_rows AS"), strings.HasPrefix(query, "SELECT * FROM (WITH proof_rows AS"):
		return 4
	case strings.Contains(query, "plug_postgres.selection_mutation_keys") && strings.HasPrefix(query, "DELETE"),
		strings.HasPrefix(query, "UPDATE plug_postgres.selection_mutation_fence"):
		return 13
	case strings.Contains(query, "plug_postgres.selection_mutation_fence"):
		return 2
	case strings.Contains(query, "plug_postgres.hierarchy_"), strings.Contains(query, "plug_postgres.graph_link_"):
		return 5
	case strings.HasPrefix(query, "SELECT") && (strings.Contains(query, "plug_postgres.records") || strings.Contains(query, "plug_postgres.versions")):
		return 6
	case strings.Contains(query, "plug_postgres.records"), strings.Contains(query, "plug_postgres.versions"):
		return 7
	case strings.Contains(query, "plug_postgres.states"):
		return 8
	case strings.Contains(query, "plug_postgres.selection_sources"):
		return 9
	case strings.Contains(query, "plug_postgres.selection_rows"):
		return 10
	case strings.Contains(query, "plug_postgres.selection_facts"):
		return 11
	case strings.Contains(query, "plug_postgres.selection_coverage"):
		return 12
	default:
		return 14
	}
}

// Only the certificate fixture installs this tracer and opts its actual Commit
// context in. Staging, EXPLAIN, readback and production-cadence renewal retain
// their original contexts. Delegate the existing preimage tracer unchanged.
type selectionPublicationQueryTracer struct{}

func (selectionPublicationQueryTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	ctx = (commitQueryTracer{}).TraceQueryStart(ctx, conn, data)
	if c, _ := ctx.Value(selectionCommitTimingKey{}).(*selectionCommitTiming); c != nil {
		point, phase := c.leasePoint(), selectionCommitQueryPhase(data.SQL)
		c.mu.Lock()
		c.observeInside(point)
		if c.inflight {
			c.overlaps++ // Never silently treat overlapping spans as complete.
		}
		c.active = selectionCommitQuerySample{Phase: selectionCommitQueryPhases[phase], Start: point}
		c.activePhase, c.inflight = phase, true
		c.buckets[phase].Starts++
		c.mu.Unlock()
	}
	return ctx
}

func (selectionPublicationQueryTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if c, _ := ctx.Value(selectionCommitTimingKey{}).(*selectionCommitTiming); c != nil {
		point := c.leasePoint()
		c.mu.Lock()
		c.observeInside(point)
		if !c.inflight {
			c.unmatched++
		} else {
			span := c.active
			span.End, span.Done, span.Failed, span.SQLState = point, true, data.Err != nil, sqlState(data.Err)
			bucket := &c.buckets[c.activePhase]
			bucket.Ends++
			bucket.ElapsedNS += span.End.AtNS - span.Start.AtNS
			if span.Failed {
				bucket.Errors++
				c.lastError = span
			}
			c.last = span
			if span.End.AtNS-span.Start.AtNS > c.longest.End.AtNS-c.longest.Start.AtNS {
				c.longest = span
			}
			c.active = selectionCommitQuerySample{}
			c.inflight = false
		}
		c.mu.Unlock()
	}
	(commitQueryTracer{}).TraceQueryEnd(ctx, conn, data)
}

func (c *selectionCommitTiming) log(t *testing.T, offset int, elapsed time.Duration, err error) {
	t.Helper()
	after := c.leasePoint()
	c.mu.Lock()
	defer c.mu.Unlock()
	var fenced driver.ErrFenced
	// This is the local lease timer and Lost channel, bracketed by Commit.
	// A local deadline/loss observation cannot identify server lock ownership
	// or replace the DB clock/expiry values checked by the real lease fence.
	raw, marshalErr := json.Marshal(struct {
		Engine, Scope, DatabaseExpiry, ExecutorWork, SQLCommit string
		Offset                                                 int
		WallNS                                                 int64
		CommitFailed, CommitFenced                             bool
		Before, PostCommit, FirstLossInsideQuery               selectionCommitLeasePoint
		LossObservedDuringCommit                               bool
		LocalDeadlineElapsedInsideQuery                        bool
		LocalDeadlineElapsedAtPostCommitSample                 bool
		Buckets                                                [len(selectionCommitQueryPhases)]selectionCommitQueryBucket
		Last, Longest, LastError, Active                       selectionCommitQuerySample
		Inflight                                               bool
		Overlaps, Unmatched                                    uint64
	}{
		Engine: "postgres", Scope: "ACTUAL_COMMIT_QUERY_BOUNDARIES_ONLY", DatabaseExpiry: "UNMEASURED", ExecutorWork: "UNMEASURED", SQLCommit: "BEGIN_CONTEXT_NOT_OPTED_IN",
		Offset: offset, WallNS: elapsed.Nanoseconds(), CommitFailed: err != nil, CommitFenced: errors.As(err, &fenced),
		Before: c.before, PostCommit: after, FirstLossInsideQuery: c.firstLost, LossObservedDuringCommit: c.lossInside,
		LocalDeadlineElapsedInsideQuery:        c.deadlineInside,
		LocalDeadlineElapsedAtPostCommitSample: after.AtNS >= after.DeadlineNS,
		Buckets:                                c.buckets, Last: c.last, Longest: c.longest, LastError: c.lastError, Active: c.active,
		Inflight: c.inflight, Overlaps: c.overlaps, Unmatched: c.unmatched,
	})
	if marshalErr != nil {
		t.Errorf("commit timing could not be encoded: %v", marshalErr)
		return
	}
	t.Logf("W6_PG_COMMIT_TIMING %s", raw)
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
	chainScans, chainVisits := 0, float64(0)
	const liveInput, chainInput = uint8(1), uint8(2)
	var inspect func(any) uint8
	inspect = func(value any) uint8 {
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
			inputs := uint8(0)
			for _, child := range node {
				inputs |= inspect(child)
			}
			live := cte == "proof_live" || relation == "records"
			chain := cte == "proof_beads" || cte == "proof_ancestors" || cte == "proof_chain"
			nodeType, _ := node["Node Type"].(string)
			cache := nodeType == "Materialize" || nodeType == "Memoize"
			// Include executor caches above live input, so an inlined records
			// scan cannot hide repeated full-domain reads in Materialize.
			if live || inputs&liveInput != 0 && cache {
				liveScans++
				rows, _ := node["Actual Rows"].(float64)
				loops, _ := node["Actual Loops"].(float64)
				removed, _ := node["Rows Removed by Filter"].(float64)
				liveVisits += loops * (rows + removed)
			}
			if chain || inputs&chainInput != 0 && cache {
				chainScans++
				rows, _ := node["Actual Rows"].(float64)
				loops, _ := node["Actual Loops"].(float64)
				removed, _ := node["Rows Removed by Filter"].(float64)
				chainVisits += loops * (rows + removed)
			}
			if live {
				inputs |= liveInput
			}
			if chain {
				inputs |= chainInput
			}
			return inputs
		case []any:
			inputs := uint8(0)
			for _, child := range node {
				inputs |= inspect(child)
			}
			return inputs
		}
		return 0
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
	// This fixture has one self ancestor per bead. All chain relations may be
	// read a fixed number of times, including executor caches, never per bead.
	if chainScans == 0 || chainVisits > float64(64*population) {
		t.Fatalf("publication chain scans=%d visits=%v population=%d: %s", chainScans, chainVisits, population, raw)
	}
	t.Logf("W6_CHAIN_PROOF_WORK phase=publication_family_proof fixture_beads=%d chain_scans=%d chain_row_visits=%v bound=%d qualification=FINITE_CONTROL", population, chainScans, chainVisits, 64*population)
	t.Logf("W6_LIVE_PROOF_WORK phase=publication_family_proof fixture_beads=%d live_record_scans=%d live_record_row_visits=%v bound=%d qualification=FINITE_CONTROL", population, liveScans, liveVisits, 32*population)
	t.Logf("W6_HEADER_PROOF_WORK phase=publication_family_proof fixture_beads=%d source_header_scans=%d source_header_row_visits=%v bound=%d qualification=FINITE_CONTROL", population, headerScans, headerVisits, 128*population)
	t.Logf("W6_EXPLAIN_ANALYZE phase=publication_family_proof fixture_beads=%d census_producer=%d census_scans=%d census_row_visits=%v qualification=FINITE_CONTROL actual_plan=%s", population, producer, scans, visits, raw)
}

// Mutation tracking is exercised through real statements, including empty
// statements and TRUNCATE. Each probe rolls back its SQL transaction; neither
// the old certificate nor the retained read snapshot can absorb its writes.
func TestSelectionPublicationMutationCertificate(t *testing.T) {
	ctx := context.Background()
	s, lease := openCommitQueryTest(t, selectionPublicationQueryTracer{})
	publishSelectionCensusBatch(t, s, lease, 0, true)
	first := pgStartupRead(t, s)
	requireSelectionCertificate(t, first, true)
	t.Run("affected_work_1000", func(t *testing.T) { explainSelectionAffectedWork(t, first, 1000) })
	publishSelectionCensusBatch(t, s, lease, 1000, true)
	current := pgStartupRead(t, s)
	requireSelectionCertificate(t, current, true)
	t.Run("affected_work_2000", func(t *testing.T) { explainSelectionAffectedWork(t, current, 2000) })
	for _, watched := range []struct{ table, column string }{
		{"records", "key"}, {"versions", "key"}, {"states", "key"},
		{"selection_sources", "key"}, {"selection_rows", "row_key"},
		{"selection_facts", "row_key"}, {"selection_coverage", "ready"},
		{"hierarchy_index_coverage", "incarnation"}, {"hierarchy_source_current", "child_key"},
		{"hierarchy_source_history", "child_key"}, {"graph_link_index_coverage", "incarnation"},
		{"graph_link_source_current", "source_key"}, {"graph_link_current", "source_key"},
		{"graph_link_source_history", "source_key"}, {"graph_link_history", "source_key"},
	} {
		t.Run(watched.table, func(t *testing.T) {
			table := "plug_postgres." + watched.table
			for _, query := range []string{
				`INSERT INTO ` + table + ` SELECT * FROM ` + table + ` WHERE false`,
				`UPDATE ` + table + ` SET ` + watched.column + `=` + watched.column + ` WHERE false`,
				`DELETE FROM ` + table + ` WHERE false`,
			} {
				func() {
					tx, err := s.db.BeginTx(ctx, nil)
					if err != nil {
						t.Fatal(err)
					}
					defer tx.Rollback()
					view := &reader{sql: tx, meta: current.meta, at: current.at}
					requireSelectionCertificate(t, view, true)
					if _, err = tx.ExecContext(ctx, query); err != nil {
						t.Fatal(err)
					}
					requireSelectionCertificate(t, view, false)
				}()
				requireSelectionCertificate(t, pgStartupRead(t, s), true)
			}
		})
	}
	t.Run("hidden_raw_mutation_forces_full_proof", func(t *testing.T) {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		hidden := selectionRefKey(testRef("pl-census-1999"))
		if _, err := tx.ExecContext(ctx, `DELETE FROM plug_postgres.selection_rows WHERE family_table='sort_key' AND source_key=$1`, []byte(hidden)); err != nil {
			t.Fatal(err)
		}
		view := &reader{sql: tx, meta: current.meta, at: current.at}
		view.writer = &transaction{reader: view}
		requireSelectionCertificate(t, view, false)
		valid, err := view.selectionAdmissionValidity(ctx, view.at)
		if err != nil || valid[selectionBeadFamily()] || valid[selectionFamilies()[3]] {
			t.Fatalf("untouched hidden corruption skipped: %v %v", valid, err)
		}
	})
	t.Run("stale_generation_refused_in_proof_statement", func(t *testing.T) {
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		view := &reader{sql: tx, meta: current.meta, at: current.at}
		_, generation, err := view.selectionCertificate(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM plug_postgres.selection_facts WHERE false`); err != nil {
			t.Fatal(err)
		}
		query, args, err := view.compileSelectionValidityForSources(ctx, view.at, false, [][]byte{}, generation)
		valid, err := view.readSelectionValidity(ctx, query, args, err)
		if err != nil {
			t.Fatal(err)
		}
		for _, yes := range valid {
			if yes {
				t.Fatal("old token admitted an empty/false affected domain")
			}
		}
	})
	t.Run("deleted_envelope_retains_old_source_owner", func(t *testing.T) {
		ref := testRef("pl-census-0000")
		key := driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: ref.ID}
		envelopes := pgStartupRead(t, s)
		old, err := envelopes.DerivedRows(ctx, key.Table, key.Key)
		if closeErr := envelopes.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		if err != nil || len(old) != 1 {
			t.Fatalf("old empty envelope=%v %v", old, err)
		}
		sentinel, _ := driver.NewClosureSentinel(ref.ID)
		w, _ := beginTest(t, s, lease, testLock(ref), driver.LockKey{Kind: driver.LockDerived, Derived: &key}, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
		tx := w.(*transaction)
		defer tx.Close()
		if err := tx.PutDerived(ctx, key.Table, old, nil); err != nil {
			t.Fatal(err)
		}
		roots, err := tx.selectionMutationRoots(ctx)
		if err != nil {
			t.Fatal(err)
		}
		found := false
		for _, root := range roots {
			found = found || string(root) == selectionRefKey(ref)
		}
		valid, err := tx.reader.selectionAdmissionValidity(ctx, tx.at)
		if !found || err != nil || valid[selectionFamilies()[1]] {
			t.Fatalf("removed envelope lost its old owner: roots=%x validity=%v %v", roots, valid, err)
		}
	})
	t.Run("unexpected_in_transaction_source_is_not_carried", func(t *testing.T) {
		hidden := selectionHex(selectionRefKey(testRef("pl-census-1999")))
		query := `CREATE FUNCTION plug_postgres.w6_unexpected_source() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE plug_postgres.records SET revision=revision WHERE key=decode('` + hidden + `','hex'); RETURN NULL; END; $$;
CREATE TRIGGER w6_unexpected_source AFTER INSERT ON plug_postgres.states FOR EACH STATEMENT EXECUTE FUNCTION plug_postgres.w6_unexpected_source()`
		if _, err := s.db.ExecContext(ctx, query); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := s.db.ExecContext(ctx, `DROP TRIGGER w6_unexpected_source ON plug_postgres.states; DROP FUNCTION plug_postgres.w6_unexpected_source()`); err != nil {
				t.Error(err)
			}
		}()
		sentinel, _ := driver.NewBeadSentinel("w6-mutation-journal")
		w, h := beginTest(t, s, lease, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
		defer w.Close()
		if err := w.BumpSentinels(ctx, []driver.Sentinel{sentinel}); err != nil {
			t.Fatal(err)
		}
		if _, err := w.Commit(ctx, h); err != nil {
			t.Fatal(err)
		}
		if !w.(*transaction).selectionProofCarry {
			t.Fatal("probe never entered the actual certified-predecessor path")
		}
		requireSelectionCertificate(t, pgStartupRead(t, s), false)
		// The injected statement changes no bytes. Canonical full startup can
		// therefore rebuild the proof without repairing or hiding corruption.
		if err := pipeline.BuildSourceIndexes(ctx, s, lease); err != nil {
			t.Fatal(err)
		}
		requireSelectionCertificate(t, pgStartupRead(t, s), true)
	})
	t.Run("ordinary_full_proof_cannot_launder_raw_generation", func(t *testing.T) {
		if _, err := s.db.ExecContext(ctx, `DELETE FROM plug_postgres.selection_facts WHERE false`); err != nil {
			t.Fatal(err)
		}
		tx, h := beginTest(t, s, lease)
		if _, err := tx.Commit(ctx, h); err != nil {
			t.Fatal(err)
		}
		requireSelectionCertificate(t, pgStartupRead(t, s), false)
		if err := pipeline.BuildSourceIndexes(ctx, s, lease); err != nil {
			t.Fatal(err)
		}
		requireSelectionCertificate(t, pgStartupRead(t, s), true)
	})
	t.Run("post_proof_mutation_rolls_back", func(t *testing.T) {
		hidden := selectionHex(selectionRefKey(testRef("pl-census-1999")))
		if _, err := s.db.ExecContext(ctx, `CREATE FUNCTION plug_postgres.w6_late_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE plug_postgres.records SET revision=revision WHERE key=decode('`+hidden+`','hex'); RETURN NULL; END; $$;
CREATE TRIGGER w6_late_mutation AFTER UPDATE ON plug_postgres.selection_coverage FOR EACH STATEMENT EXECUTE FUNCTION plug_postgres.w6_late_mutation()`); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if _, err := s.db.ExecContext(ctx, `DROP TRIGGER w6_late_mutation ON plug_postgres.selection_coverage; DROP FUNCTION plug_postgres.w6_late_mutation()`); err != nil {
				t.Error(err)
			}
		}()
		prior, err := s.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		w, h := beginTest(t, s, lease)
		defer w.Close()
		if _, err := w.Commit(ctx, h); !errors.Is(err, driver.ErrSelectionUnavailable) {
			t.Fatalf("mutation after proof was published: %v", err)
		}
		after, err := s.Meta(ctx)
		if err != nil || after.Head.String() != prior.Head.String() {
			t.Fatalf("late mutation advanced Head: %+v %v", after, err)
		}
		requireSelectionCertificate(t, pgStartupRead(t, s), true)
	})
	if result, err := first.Select(ctx, pgStartupCountPlan(first)); err != nil || result.Total != 1000 {
		t.Fatalf("later publication/rebuild changed retained history: %+v %v", result, err)
	}
	// TRUNCATE requires AccessExclusive. Release the two snapshots which read
	// these tables before probing it; generation-only readers hold no such
	// table locks. Otherwise the control would manufacture a lock wait.
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if err := current.Close(); err != nil {
		t.Fatal(err)
	}
	t.Run("truncate_rolls_back", func(t *testing.T) {
		meta, err := s.Meta(ctx)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback()
		view := &reader{sql: tx, meta: meta, at: mustRevision(meta.Head)}
		requireSelectionCertificate(t, view, true)
		if _, err := tx.ExecContext(ctx, `TRUNCATE plug_postgres.records,plug_postgres.versions,plug_postgres.states,plug_postgres.selection_sources,plug_postgres.selection_rows,plug_postgres.selection_facts,plug_postgres.selection_coverage,plug_postgres.hierarchy_index_coverage,plug_postgres.hierarchy_source_current,plug_postgres.hierarchy_source_history,plug_postgres.graph_link_index_coverage,plug_postgres.graph_link_source_current,plug_postgres.graph_link_current,plug_postgres.graph_link_source_history,plug_postgres.graph_link_history`); err != nil {
			t.Fatal(err)
		}
		requireSelectionCertificate(t, view, false)
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		requireSelectionCertificate(t, pgStartupRead(t, s), true)
	})
}

func requireSelectionCertificate(t *testing.T, r *reader, want bool) {
	t.Helper()
	got, err := r.selectionCertificateClean(context.Background())
	if err != nil || got != want {
		t.Fatalf("mutation certificate clean=%t want=%t: %v", got, want, err)
	}
}

// Count actual leaf inputs and cache replays for BOTH reverse discovery and
// restricted family proof. This finite one-source oracle catches a hidden
// full scan at either population; it does not qualify arbitrary selector plans
// or arbitrary hierarchy widths. Existing population guards remain separate.
func explainSelectionAffectedWork(t *testing.T, r *reader, population int) {
	t.Helper()
	ctx := context.Background()
	roots := [][]byte{[]byte(selectionRefKey(testRef("pl-census-0000")))}
	impacted, err := r.selectionImpacted(ctx, r.at, false, roots)
	if err != nil || len(impacted) != 1 || string(impacted[0]) != string(roots[0]) {
		t.Fatalf("one-source closure=%x err=%v", impacted, err)
	}
	q1, a1, err := r.compileSelectionImpacted(r.at, false, roots)
	if err != nil {
		t.Fatal(err)
	}
	q2, a2, err := r.compileSelectionValidityForSources(ctx, r.at, false, impacted, "")
	if err != nil {
		t.Fatal(err)
	}
	valid, err := r.readSelectionValidity(ctx, q2, a2, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, yes := range valid {
		if !yes {
			t.Fatal("restricted proof rejected complete independent root")
		}
	}
	visits := float64(0)
	for phase, query := range []string{q1, q2} {
		args := [][]any{a1, a2}[phase]
		var raw []byte
		if err := r.sql.QueryRowContext(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+query, args...).Scan(&raw); err != nil {
			t.Fatal(err)
		}
		var plan any
		if err := json.Unmarshal(raw, &plan); err != nil {
			t.Fatal(err)
		}
		var walk func(any)
		walk = func(value any) {
			switch node := value.(type) {
			case map[string]any:
				relation, _ := node["Relation Name"].(string)
				kind, _ := node["Node Type"].(string)
				physical := relation == "records" || relation == "selection_sources" || relation == "selection_rows" || relation == "selection_facts"
				if physical || kind == "Materialize" || kind == "Memoize" || kind == "CTE Scan" {
					rows, _ := node["Actual Rows"].(float64)
					loops, _ := node["Actual Loops"].(float64)
					removed, _ := node["Rows Removed by Filter"].(float64)
					rechecked, _ := node["Rows Removed by Index Recheck"].(float64)
					visits += loops * (rows + removed + rechecked)
					if physical && strings.Contains(kind, "Seq Scan") && loops > 0 {
						t.Fatalf("affected source hid a population scan: %s", raw)
					}
				}
				if removed, _ := node["Rows Removed by Join Filter"].(float64); removed > 0 {
					loops, _ := node["Actual Loops"].(float64)
					visits += loops * removed
				}
				for _, child := range node {
					walk(child)
				}
			case []any:
				for _, child := range node {
					walk(child)
				}
			}
		}
		walk(plan)
		t.Logf("W6_AFFECTED_EXPLAIN population=%d affected_sources=1 phase=%d actual_plan=%s", population, phase, raw)
	}
	if visits > 256 {
		t.Fatalf("one-source discovery+proof visits=%v exceeds fixed affected bound256", visits)
	}
	t.Logf("W6_AFFECTED_PROOF_WORK population=%d affected_sources=1 physical_and_cache_visits=%v bound=256 qualification=FINITE_ONE_SOURCE_CONTROL", population, visits)
}

func TestSelectionPublicationCertificateKeepsHiddenDescendant(t *testing.T) {
	ctx := context.Background()
	s, lease := openCommitQueryTest(t)
	refs := pgStartupLegacyChain(t, s, lease, 4) // A <- B <- C, plus D
	if err := pipeline.BuildSourceIndexes(ctx, s, lease); err != nil {
		t.Fatal(err)
	}
	before := pgStartupRead(t, s)
	requireSelectionCertificate(t, before, true)
	initial, err := before.selectionImpacted(ctx, before.at, false, [][]byte{[]byte(selectionRefKey(refs[1]))})
	if err != nil {
		t.Fatal(err)
	}
	query, args, err := before.compileSelectionValidityForSources(ctx, before.at, false, initial, "")
	initialValidity, err := before.readSelectionValidity(ctx, query, args, err)
	if err != nil || !initialValidity[selectionFamilies()[3]] {
		t.Fatalf("valid chain crossing to an untouched ancestor refused: %v %v", initialValidity, err)
	}
	old, err := before.Get(ctx, []model.Ref{refs[1]})
	if err != nil || len(old) != 1 {
		t.Fatalf("middle source=%v %v", old, err)
	}
	record := old[0]
	record.Body, err = json.Marshal(pipeline.Document{ID: refs[1].ID, Type: "issue", Fields: map[string]any{}, Parent: &refs[3]})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := pipeline.PrepareSourceRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	oldClosure, err := before.DerivedRows(ctx, driver.DerivedAncestorClosure, "")
	if err != nil {
		t.Fatal(err)
	}
	remove := []driver.DerivedRow{}
	add := []driver.DerivedRow{}
	keys := append([]driver.LockKey{}, plan.LockKeys()...)
	sentinel, _ := driver.NewClosureSentinel(refs[1].ID)
	keys = append(keys, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	for _, row := range oldClosure {
		if driver.SelectionSameRef(*row.Index.Source.Key.Ref, refs[1]) {
			remove = append(remove, row)
		}
	}
	for depth, target := range []model.Ref{refs[1], refs[3]} {
		row, err := pipeline.AncestorRow(refs[1].ID, target.ID, uint64(depth))
		if err != nil {
			t.Fatal(err)
		}
		add = append(add, row)
	}
	for _, row := range append(append([]driver.DerivedRow{}, remove...), add...) {
		key := driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: row.Key}
		keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &key})
	}
	// The source helper adds its record, graph and bead locks itself.
	bead, _ := driver.NewBeadSentinel(refs[1].ID)
	automatic := map[string]bool{}
	for _, key := range []driver.LockKey{testLock(refs[1]), driver.GraphBlockingSentinelLock, {Kind: driver.LockSentinel, Sentinel: bead}} {
		id, _ := keyID(key)
		automatic[id] = true
	}
	unique := map[string]driver.LockKey{}
	for _, key := range keys {
		id, _ := keyID(key)
		if !automatic[id] {
			unique[id] = key
		}
	}
	keys = nil
	for _, key := range unique {
		keys = append(keys, key)
	}
	tx, h := beginSourceTestWithLocks(t, s, lease, keys, true, refs[1])
	if err := tx.PutGraphRecord(ctx, record, &refs[3], nil); err != nil {
		t.Fatal(err)
	}
	rows, err := plan.SelectionRows(h.Incarnation, record.Revision)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range rows {
		if err := pipeline.PutSelectionDerived(ctx, tx, row.Index.Row.Table, nil, []driver.DerivedRow{row}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range add {
		ref, target := refs[1], []model.Ref{refs[1], refs[3]}[i]
		add[i].Index = &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: driver.DerivedRowKey{Table: driver.DerivedAncestorClosure, Key: add[i].Key}, Incarnation: h.Incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(add[i].Value), Source: rows[0].Index.Source, Facts: &driver.SelectionFacts{Kind: driver.SelectionAncestorFact, Rows: []driver.SelectionFact{{Kind: driver.SelectionAncestorFact, Ref: &ref, Target: &target, Depth: uint64(i)}}}}
	}
	if err := pipeline.PutSelectionDerived(ctx, tx, driver.DerivedAncestorClosure, remove, add); err != nil {
		t.Fatal(err)
	}
	roots, err := tx.selectionMutationRoots(ctx)
	if err != nil {
		t.Fatal(err)
	}
	oldImpact, err := tx.reader.selectionImpacted(ctx, tx.at, false, roots)
	if err != nil {
		t.Fatal(err)
	}
	newImpact, err := tx.reader.selectionImpacted(ctx, tx.at, true, roots)
	if err != nil {
		t.Fatal(err)
	}
	hasHidden := func(keys [][]byte) bool {
		for _, key := range keys {
			if string(key) == selectionRefKey(refs[2]) {
				return true
			}
		}
		return false
	}
	if !hasHidden(oldImpact) || !hasHidden(newImpact) {
		t.Fatal("old/new closure omitted untouched descendant C")
	}
	valid, err := tx.reader.selectionAdmissionValidity(ctx, tx.at)
	if err != nil || !valid[selectionBeadFamily()] || valid[selectionFamilies()[3]] {
		t.Fatalf("staged hidden C stale chain admitted: %v %v", valid, err)
	}
	commitSourceTest(t, tx, h)
	after := pgStartupRead(t, s)
	requirePGFamily(t, after, selectionFamilies()[3], after.at, false)
	requireSelectionCertificate(t, after, false)
	var buildErr error
	if err := pipeline.BuildSourceIndexes(ctx, pgStartupCaptureStore{Store: s, err: &buildErr}, lease); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(buildErr, driver.ErrSelectionUnavailable) {
		t.Fatalf("canonical rebuild accepted hidden stale C: %v", buildErr)
	}
	requireSelectionCertificate(t, before, true)
}
