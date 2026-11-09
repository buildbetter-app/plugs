package postgres

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestSelectionAnnotationDecodePreservesRefRefusal(t *testing.T) {
	for name, ref := range map[string]model.Ref{
		"conflicting_arms": {ID: "pl-source", Key: "conflict"},
		"missing_arm":      {},
		"key_arm":          model.RefByKey("source"),
		"invalid_branch":   {ID: "pl-source", Branch: model.Natural("-1")},
		"nonzero_branch":   model.RefByID("pl-source", model.Natural64(1)),
	} {
		t.Run(name, func(t *testing.T) {
			key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "annotation"}
			row := driver.DerivedRow{Key: key.Key, Value: []byte(`{}`)}
			index := driver.SelectionIndex{Version: driver.SelectionIndexVersion, Incarnation: "test", Row: key, ValueDigest: driver.SelectionDigest(row.Value),
				Source: driver.SelectionSource{Key: testLock(ref), Present: true, Digest: driver.SelectionDigest([]byte(`{}`))},
				Bead:   &driver.SelectionBead{Ref: ref},
			}
			var raw bytes.Buffer
			if err := gob.NewEncoder(&raw).Encode(index); err != nil {
				t.Fatal(err)
			}
			var before driver.SelectionIndex
			if err := gob.NewDecoder(bytes.NewReader(raw.Bytes())).Decode(&before); err != nil {
				t.Fatal(err)
			}
			if err := before.Validate(key.Table, row); err == nil {
				t.Fatal("control unexpectedly accepted malformed/unresolved annotation")
			}
			got, err := decodeSelectionAnnotation(raw.Bytes())
			if err != nil || got.Validate(key.Table, row) == nil {
				t.Fatalf("decode repaired a refused annotation: %#v %v", got, err)
			}
			if got.Bead.Ref.ID != ref.ID || got.Bead.Ref.Branch != ref.Branch || got.Bead.Ref.Key != ref.Key {
				t.Fatalf("decode rewrote reference fields: %#v; want %#v", got.Bead.Ref, ref)
			}
		})
	}
	// The binary transport must also retain opaque bytes without JSON coercion.
	ref := model.RefByID("opaque\x00\xff", model.Natural("0e0"))
	var raw bytes.Buffer
	if err := gob.NewEncoder(&raw).Encode(driver.SelectionIndex{Bead: &driver.SelectionBead{Ref: ref}}); err != nil {
		t.Fatal(err)
	}
	got, err := decodeSelectionAnnotation(raw.Bytes())
	if err != nil || !reflect.DeepEqual(got.Bead.Ref, ref) {
		t.Fatalf("decode changed opaque identity/branch bytes: %#v %v", got, err)
	}
}

// Compare the complete physical rows against the prior transport in the same
// real SQL transaction, including NULL/empty scalar fields, opaque bytes and a
// second publication batch. No logical decoder can hide a transport mismatch.
func TestSelectionAnnotationBinaryPublicationMatchesLegacy(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `CREATE TEMP TABLE annotation_transport_legacy (LIKE plug_postgres.selection_rows INCLUDING DEFAULTS) ON COMMIT DROP`); err != nil {
		t.Fatal(err)
	}
	const count = publicationBatchRows + 3
	rows := make([]selectionDBRow, count)
	legacy := make([]map[string]json.RawMessage, count)
	for i := range rows {
		ref := testRef(fmt.Sprintf("pl-binary-%04d", i))
		project, parent := "", testRef("pl-parent")
		bead := &driver.SelectionBead{Ref: ref, Type: "issue", Status: "todo", Title: "opaque\x00\xff", Priority: int64(i % 5), Project: &project, Parent: &parent}
		if i%2 == 0 {
			bead.Project, bead.Parent = nil, nil
			bead.Absent = []driver.SelectionColumn{driver.SelectionTitle, driver.SelectionTemplate, driver.SelectionPriority}
		}
		key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: fmt.Sprintf("binary-%04d", i)}
		row := driver.DerivedRow{Key: key.Key, Value: []byte(`{"transport":true}`), Index: &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: key, Incarnation: "transport", Branch: model.Natural64(0),
			Source: driver.SelectionSource{Key: testLock(ref), Revision: model.Natural64(0), Present: true, Digest: "source\x00\xff"}, Bead: bead,
		}}
		row.Index.ValueDigest = driver.SelectionDigest(row.Value)
		id, err := keyID(driver.LockKey{Kind: driver.LockDerived, Derived: &key})
		if err != nil {
			t.Fatal(err)
		}
		rows[i], err = selectionOwnedDBRow(id, row)
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal([]byte(selectionJSON(rows[i])), &legacy[i]); err != nil {
			t.Fatal(err)
		}
		// Literal predecessor representation: Gob bytes hex-expanded into JSON.
		legacy[i]["annotation"], err = json.Marshal(selectionHex(string(rows[i].Annotation)))
		if err != nil {
			t.Fatal(err)
		}
	}
	if _, err = tx.ExecContext(ctx, `INSERT INTO annotation_transport_legacy(incarnation,start_revision,`+selectionColumns+`,annotation) SELECT $1,$2,`+selectionJSONColumns+`,decode(annotation,'hex') FROM jsonb_to_recordset($3::jsonb) AS p(`+selectionJSONLayout+`)`, "transport", "1", selectionJSON(legacy)); err != nil {
		t.Fatal(err)
	}
	if err = writeSelectionRows(ctx, tx, "transport", model.Natural64(1), rows, nil); err != nil {
		t.Fatal(err)
	}
	var actual, differences int
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM plug_postgres.selection_rows WHERE incarnation='transport'`).Scan(&actual); err != nil || actual != count {
		t.Fatalf("binary publication rows=%d want=%d: %v", actual, count, err)
	}
	if err = tx.QueryRowContext(ctx, `SELECT count(*) FROM ((SELECT * FROM plug_postgres.selection_rows WHERE incarnation='transport' EXCEPT ALL SELECT * FROM annotation_transport_legacy) UNION ALL (SELECT * FROM annotation_transport_legacy EXCEPT ALL SELECT * FROM plug_postgres.selection_rows WHERE incarnation='transport')) AS differences`).Scan(&differences); err != nil || differences != 0 {
		t.Fatalf("complete binary/legacy physical row differences=%d: %v", differences, err)
	}
}

func TestSelectionAnnotationReuseStillReadsAndValidatesCurrentImage(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	ref := testRef("pl-annotation-current")
	facts := driver.SelectionFacts{Kind: driver.SelectionAncestorFact, Rows: []driver.SelectionFact{{Kind: driver.SelectionAncestorFact, Ref: &ref, Target: &ref}}}
	key := seedSelectionSQLFacts(t, s, lease, map[driver.DerivedTable]driver.SelectionFacts{driver.DerivedAncestorClosure: facts})[driver.DerivedAncestorClosure]
	lock := driver.LockKey{Kind: driver.LockDerived, Derived: &key}
	tx, _ := beginTest(t, s, lease, lock)
	defer tx.Close()
	writer := tx.(*transaction)
	id, _ := keyID(lock)
	read := func() driver.LockState {
		t.Helper()
		states, err := tx.Observe(ctx, []driver.LockKey{lock})
		if err != nil || len(states) != 1 || states[0].Derived == nil {
			t.Fatalf("actual annotation observation=%+v: %v", states, err)
		}
		return states[0]
	}
	first := read()
	if first.Derived.Index == nil {
		t.Fatal("committed annotation absent")
	}
	want := first.Derived.Clone()
	first.Derived.Index.Facts.Rows[0].Ref.ID = "caller-mutated"
	first.Derived.Index.Source.Key.Derived.Key = "caller-mutated"
	if next := read(); !reflect.DeepEqual(*next.Derived, want) {
		t.Fatalf("caller changed repeated SQL observation: %#v; want %#v", next.Derived, want)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := tx.Observe(cancelled, []driver.LockKey{lock}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cached annotation bypassed cancelled read: %v", err)
	}
	// A same-token logical replacement must be validated against the freshly
	// read state even though its separately stored annotation bytes did not move.
	state := read()
	state.Derived.Value = append(state.Derived.Value, ' ')
	raw, err := encode(state)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = writer.sql.ExecContext(ctx, `UPDATE plug_postgres.states SET value=$2 WHERE key=$1 AND revision=$3`, []byte(id), raw, state.Revision.String()); err != nil {
		t.Fatal(err)
	}
	if got := read(); got.Derived.Index != nil || !bytes.Equal(got.Derived.Value, state.Derived.Value) {
		t.Fatalf("old annotation attached to replacement logical bytes: %#v", got.Derived)
	}
	// Same row/revision, different physical annotation: perform the read and
	// retain the original malformed-binary refusal instead of returning a hit.
	if _, err = writer.sql.ExecContext(ctx, `UPDATE plug_postgres.selection_rows SET annotation=$2 WHERE row_key=$1`, []byte(id), []byte("malformed annotation")); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Observe(ctx, []driver.LockKey{lock}); err == nil {
		t.Fatal("annotation reuse hid malformed same-token SQL replacement")
	}
}

// These are trusted adapter fixtures with independently specified semantic
// fields. They exercise the public driver API, not a native producer claim.
func seedSelectionSQL(t *testing.T, s *Store, l driver.ActiveLease, beads []driver.SelectionBead) {
	t.Helper()
	seedSelectionSQLSourceDomains(t, s, l, beads, true)
}

func seedSelectionSQLSourceDomains(t *testing.T, s *Store, l driver.ActiveLease, beads []driver.SelectionBead, graph bool) {
	t.Helper()
	// Keep setup publications inside the real lease lifetime. Large executor
	// fixtures renew between bounded transactions, using the production fence.
	const batchSize = 128
	if len(beads) <= batchSize {
		seedSelectionSQLSourceDomainBatch(t, s, l, beads, graph)
		return
	}
	for start := 0; start < len(beads); start += batchSize {
		if err := l.Renew(context.Background()); err != nil {
			t.Fatal(err)
		}
		seedSelectionSQLSourceDomainBatch(t, s, l, beads[start:min(start+batchSize, len(beads))], graph)
	}
}

func seedSelectionSQLSourceDomainBatch(t *testing.T, s *Store, l driver.ActiveLease, beads []driver.SelectionBead, graph bool) {
	t.Helper()
	ctx := context.Background()
	keys := []driver.LockKey{driver.GraphBlockingSentinelLock}
	sentinels := []driver.Sentinel{driver.SentinelGraphBlocking}
	for _, b := range beads {
		ref := b.Ref
		key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "w6-fixture:" + ref.ID}
		sentinel, err := driver.NewBeadSentinel(ref.ID)
		if err != nil {
			t.Fatal(err)
		}
		sentinels = append(sentinels, sentinel)
		keys = append(keys, testLock(ref), driver.LockKey{Kind: driver.LockDerived, Derived: &key}, driver.LockKey{Kind: driver.LockSentinel, Sentinel: sentinel})
	}
	tx, h := beginTest(t, s, l, keys...)
	rows := make([]driver.DerivedRow, 0, len(beads))
	for _, b := range beads {
		ref := b.Ref
		source := testLock(ref)
		key := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "w6-fixture:" + ref.ID}
		body := []byte(` { "public_synthetic_fixture":true,"unchanged_payload":"` + strings.Repeat("x", 128) + `" } `)
		record := driver.Record{Ref: ref, Kind: "bead", Body: body}
		var err error
		if graph {
			err = tx.(driver.GraphSourceWriter).PutGraphRecord(ctx, record, b.Parent, nil)
		} else {
			err = tx.(driver.HierarchySourceWriter).PutHierarchyRecord(ctx, record, b.Parent)
		}
		if err != nil {
			t.Fatal(err)
		}
		value, _ := json.Marshal(map[string]string{"typed": "fixture"})
		rows = append(rows, driver.DerivedRow{Key: key.Key, Value: value, Index: &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: key, Incarnation: h.Incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value), Source: driver.SelectionSource{Key: source, Revision: model.Natural64(0), Present: true, Digest: driver.SelectionDigest(body)}, Bead: &b}})
		h.Changes = append(h.Changes, model.ChangeRecord{Ref: ref, Op: model.OpCreate, FromRevision: model.Natural64(0)})
	}
	if err := tx.BumpSentinels(ctx, sentinels); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutDerived(ctx, driver.DerivedSortKey, nil, rows); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, h); err != nil {
		t.Fatal(err)
	}
}
func selectionSQLPlan(r driver.Reader) driver.SelectionPlan {
	v := r.Snapshot()
	p := driver.SelectionPlan{Incarnation: v.Incarnation, Branch: v.Branch, ContentRevision: *v.At, AuthorityRevision: *v.At, Membership: driver.SelectionExpr{Op: driver.SelectionTrue}, Authorization: driver.SelectionExpr{Op: driver.SelectionTrue}, Mode: driver.SelectionPageMode, Page: &driver.SelectionPage{Limit: 100}}
	control := model.RefByKey("w6-public-fixture-control")
	p.Boundary, _ = driver.PrepareSelectionBoundary(driver.SelectionBoundarySpec{Version: 1, PreparedBinding: driver.SelectionDigest([]byte("w6-public-trusted-adapter-fixture/v1")), Incarnation: p.Incarnation, Branch: p.Branch, AuthorityRevision: p.AuthorityRevision, Chain: driver.SelectionChainCompiled, Authorization: p.Authorization, Host: driver.SelectionHostAbsent, Sources: []driver.SelectionSource{{Key: driver.LockKey{Kind: driver.LockRecord, Ref: &control}, Revision: model.Natural64(0)}}})
	return p
}

// These fixtures act as the trusted compiler for their explicitly chosen AST.
// The independent driver conformance uses direct Select for seal spoof cases.
func selectionSQLSeal(p driver.SelectionPlan) driver.SelectionPlan {
	spec, err := p.Boundary.Description()
	if err != nil {
		return p
	}
	spec.Authorization = p.Authorization
	if seal, err := driver.PrepareSelectionBoundary(spec); err == nil {
		p.Boundary = seal
	}
	return p
}

func requireSelectionIDs(t *testing.T, r driver.Reader, p driver.SelectionPlan, total uint64, ids ...string) driver.SelectionResult {
	t.Helper()
	got, err := r.(driver.SelectionReader).Select(context.Background(), selectionSQLSeal(p))
	if err != nil {
		t.Fatal(err)
	}
	actual := []string{}
	for _, k := range got.Keys {
		actual = append(actual, k.Ref.ID)
	}
	if got.Total != total || !slices.Equal(actual, ids) {
		t.Fatalf("selection total=%d ids=%q; want%d %q", got.Total, actual, total, ids)
	}
	return got
}
func TestSelectionLiteralBytesAndAbsentNegation(t *testing.T) {
	s := openTest(t, freshDSN(t))
	l := testLease(t, s)
	project := ""
	seedSelectionSQL(t, s, l, []driver.SelectionBead{{Ref: testRef("pl-a"), Type: "issue", Title: "A\x00_%é\xffZ", Priority: 10}, {Ref: testRef("pl-b"), Type: "issue", Title: "a_%ÉZ", Project: &project, Priority: 2}})
	r := testRead(t, s, nil)
	p := selectionSQLPlan(r)
	for _, tc := range []struct {
		name, needle string
		ids          []string
	}{{"empty", "", []string{"pl-a", "pl-b"}}, {"nul", "\x00", []string{"pl-a"}}, {"nul_literal", "\x00_%", []string{"pl-a"}}, {"literal", "_%", []string{"pl-a", "pl-b"}}, {"unicode", "é", []string{"pl-a"}}, {"opaque_byte", "\xff", []string{"pl-a"}}, {"case", "a", []string{"pl-b"}}} {
		t.Run(tc.name, func(t *testing.T) {
			v := driver.SelectionString(tc.needle)
			p.Membership = driver.SelectionExpr{Op: driver.SelectionCompareOp, Compare: &driver.SelectionCompare{Column: driver.SelectionTitle, Op: "contains", Value: &v}}
			requireSelectionIDs(t, r, p, uint64(len(tc.ids)), tc.ids...)
		})
	}
	value := driver.SelectionString("")
	p.Membership = driver.SelectionExpr{Op: driver.SelectionNot, Args: []driver.SelectionExpr{{Op: driver.SelectionCompareOp, Compare: &driver.SelectionCompare{Column: driver.SelectionProject, Op: "eq", Value: &value}}}}
	requireSelectionIDs(t, r, p, 1, "pl-a")
	parent := driver.SelectionRef(testRef("pl-parent"))
	p.Membership = driver.SelectionExpr{Op: driver.SelectionNot, Args: []driver.SelectionExpr{{Op: driver.SelectionCompareOp, Compare: &driver.SelectionCompare{Column: driver.SelectionParent, Op: "eq", Value: &parent}}}}
	p.Order = []driver.SelectionOrder{{Column: driver.SelectionPriority}}
	requireSelectionIDs(t, r, p, 2, "pl-b", "pl-a")
	// Annotation ownership and opaque bytes survive ordinary explicit reads.
	rows, err := r.DerivedRows(context.Background(), driver.DerivedSortKey, "w6-fixture:pl-a")
	if err != nil || len(rows) != 1 || rows[0].Index.Bead.Title != "A\x00_%é\xffZ" {
		t.Fatalf("owned annotation=%+v err=%v", rows, err)
	}
}

func TestSelectionUnannotatedWritesInvalidateWholeDomain(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	l := testLease(t, s)
	seedSelectionSQL(t, s, l, []driver.SelectionBead{{Ref: testRef("pl-visible"), Type: "issue"}})
	old := testRead(t, s, nil)
	oldPlan := selectionSQLPlan(old)
	hidden := testRef("pl-hidden-unannotated")
	bead, _ := driver.NewBeadSentinel(hidden.ID)
	tx, header := beginTest(t, s, l, testLock(hidden), driver.GraphBlockingSentinelLock, driver.LockKey{Kind: driver.LockSentinel, Sentinel: bead})
	if err := tx.(driver.GraphSourceWriter).PutGraphRecord(ctx, driver.Record{Ref: hidden, Kind: "bead", Body: []byte(`{}`)}, nil, nil); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{bead, driver.SentinelGraphBlocking}); err != nil {
		t.Fatal(err)
	}
	header.Changes = []model.ChangeRecord{{Ref: hidden, Op: model.OpCreate, FromRevision: model.Natural64(0)}}
	if _, err := tx.Commit(ctx, header); err != nil {
		t.Fatal(err)
	}
	current := testRead(t, s, nil)
	p := selectionSQLPlan(current)
	p.Authorization = driver.SelectionExpr{Op: driver.SelectionRefs, Refs: []model.Ref{testRef("pl-visible")}}
	got, err := current.(driver.SelectionReader).Select(ctx, selectionSQLSeal(p))
	if !errors.Is(err, driver.ErrUnsupported) || !reflect.DeepEqual(got, driver.SelectionResult{}) {
		t.Fatalf("hidden missing projection was omitted: %+v %v", got, err)
	}
	terminal, _ := beginTest(t, s, l, testLock(testRef("pl-unrelated-terminal-lock")))
	defer terminal.Rollback(ctx)
	freshPlan := selectionSQLPlan(terminal)
	freshPlan.Mode, freshPlan.Page = driver.SelectionCountMode, nil
	freshPlan.Membership = driver.SelectionExpr{Op: driver.SelectionFalse}
	if yes, err := terminal.(driver.FreshSelectionExistsReader).FreshSelectionExists(ctx, freshPlan); yes || !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("unbuilt domain minted committed absence: %v %v", yes, err)
	}
	requireSelectionIDs(t, old, oldPlan, 1, "pl-visible")
}

func TestSelectionThinSparseSemijoinAndExecutorEvidence(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	l := testLease(t, s)
	beads := make([]driver.SelectionBead, 2048)
	for i := range beads {
		beads[i] = driver.SelectionBead{Ref: testRef(fmt.Sprintf("pl-%04d", i)), Type: "issue", Title: "literal_%", Priority: int64(i % 3)}
	}
	seedSelectionSQL(t, s, l, beads)
	r := testRead(t, s, nil)
	p := selectionSQLPlan(r)
	// A duplicate request ref and a hidden head must neither multiply counts
	// nor consume the authorized cap/page boundary.
	refs := []model.Ref{beads[2047].Ref, beads[2046].Ref, beads[2046].Ref}
	p.Authorization = driver.SelectionExpr{Op: driver.SelectionRefs, Refs: refs}
	p.Cap = 2
	p.Page.Limit = 1
	p.GroupBy = driver.SelectionGroupType
	first := requireSelectionIDs(t, r, p, 2, "pl-2046")
	explainSelectionSQL(t, r.(*reader), p, "page")
	if !first.More || first.Next == nil || len(first.Groups) != 1 || first.Groups[0].Count != 2 || first.Groups[0].Elided != 1 {
		t.Fatalf("first page=%+v", first)
	}
	p.Page.After = first.Next
	second := requireSelectionIDs(t, r, p, 2, "pl-2047")
	explainSelectionSQL(t, r.(*reader), p, "keyset_page")
	if second.More || second.Next != nil || second.Groups[0].Elided != 0 {
		t.Fatalf("second page=%+v", second)
	}
	p.Page.After = nil
	// Necessary positive refs can drive nested ALL/ANY without changing the
	// boolean oracle. An open OR branch must retain the full domain and refuse.
	p.Authorization = driver.SelectionExpr{Op: driver.SelectionAll, Args: []driver.SelectionExpr{
		{Op: driver.SelectionAny, Args: []driver.SelectionExpr{{Op: driver.SelectionFalse}, {Op: driver.SelectionRefs, Refs: refs}}},
		{Op: driver.SelectionNot, Args: []driver.SelectionExpr{{Op: driver.SelectionRefs, Refs: []model.Ref{beads[2046].Ref}}}},
	}}
	requireSelectionIDs(t, r, p, 1, "pl-2047")
	p.Authorization = driver.SelectionExpr{Op: driver.SelectionAny, Args: []driver.SelectionExpr{{Op: driver.SelectionRefs, Refs: refs}, {Op: driver.SelectionTrue}}}
	if got, err := r.(driver.SelectionReader).Select(ctx, selectionSQLSeal(p)); !errors.As(err, new(driver.ErrSelectionCap)) || !reflect.DeepEqual(got, driver.SelectionResult{}) {
		t.Fatalf("open OR was incorrectly restricted to positive refs: %+v %v", got, err)
	}
	p.Authorization = driver.SelectionExpr{Op: driver.SelectionRefs, Refs: refs}
	p.Mode = driver.SelectionCountMode
	p.Page = nil
	p.GroupBy = ""
	count, err := r.(driver.SelectionReader).Select(ctx, selectionSQLSeal(p))
	if err != nil || count.Total != 2 || len(count.Keys) != 0 {
		t.Fatalf("count=%+v %v", count, err)
	}
	explainSelectionSQL(t, r.(*reader), p, "count")
	p.Mode = driver.SelectionTypeSummary
	p.GroupBy = driver.SelectionGroupType
	summary, err := r.(driver.SelectionReader).Select(ctx, selectionSQLSeal(p))
	if err != nil || summary.Total != 2 || len(summary.Keys) != 0 || len(summary.Groups) != 1 || summary.Groups[0].Count != 2 {
		t.Fatalf("summary=%+v %v", summary, err)
	}
	explainSelectionSQL(t, r.(*reader), p, "summary")
}

// Each operation gets its own executed plan. Keep all per-node rows, loops,
// removed rows and buffer fields; adding a parent's inclusive buffers to its
// children's would double-count. Returned keys and these executor observations
// remain separate. This finite fixture is not CP7 workload/latency proof.
func explainSelectionSQL(t *testing.T, r *reader, p driver.SelectionPlan, operation string) {
	t.Helper()
	explainSelectionSQLFixture(t, r, p, operation, 2048, 2)
}

func explainSelectionSQLFixture(t *testing.T, r *reader, p driver.SelectionPlan, operation string, population, authorized int) {
	t.Helper()
	ctx := context.Background()
	query, args, err := r.compileSelection(selectionSQLSeal(p))
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err = r.sql.QueryRowContext(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+query, args...).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var plan []map[string]any
	if err = json.Unmarshal(raw, &plan); err != nil || len(plan) != 1 {
		t.Fatalf("actual executor plan=%s err=%v", raw, err)
	}
	var inspect func(any)
	nodes, executed := 0, 0
	inspect = func(v any) {
		switch x := v.(type) {
		case map[string]any:
			if _, node := x["Node Type"].(string); node {
				nodes++
				rows, haveRows := x["Actual Rows"].(float64)
				loops, haveLoops := x["Actual Loops"].(float64)
				if !haveRows || !haveLoops || rows < 0 || loops < 0 {
					t.Fatalf("missing actual executor measurements: %s", raw)
				}
				if loops > 0 {
					executed++
				}
				// Logical states and blobs also carry stored payloads. Their
				// absence matters even when no live record table is scanned.
				if table, _ := x["Relation Name"].(string); table == "records" || table == "versions" || table == "states" || table == "blobs" {
					t.Fatalf("thin selector touched payload relation %s: %s", table, raw)
				}
			}
			for _, child := range x {
				inspect(child)
			}
		case []any:
			for _, child := range x {
				inspect(child)
			}
		}
	}
	for _, root := range plan {
		inspect(root)
	}
	if nodes == 0 || executed == 0 {
		t.Fatalf("no executed plan nodes: %s", raw)
	}
	t.Logf("W6_EXPLAIN_ANALYZE phase=selector_statement operation=%s fixture_beads=%d authorized_unique=%d payloads=0 preflight_executor=UNMEASURED executor_bound=UNQUALIFIED actual_plan=%s", operation, population, authorized, raw)
}

// A writer first observes absence, then a different writer publishes before
// Head. A stale sealed token refuses; a legitimately rebound plan observes the
// committed candidate. Own overlay behavior has separate conformance controls.
func TestFreshSelectionExistsSeesPublicationBeforeHead(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	l := testLease(t, s)
	seedSelectionSQL(t, s, l, []driver.SelectionBead{{Ref: testRef("pl-anchor"), Type: "issue"}})
	tx, err := s.Write(ctx, l)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Lock(ctx, []driver.LockRequest{{Key: testLock(testRef("pl-unrelated-lock")), Mode: driver.LockUpdate}}); err != nil {
		t.Fatal(err)
	}
	plan := selectionSQLPlan(tx)
	plan.Mode, plan.Page = driver.SelectionCountMode, nil
	candidate := testRef("pl-newly-committed")
	plan.Membership = driver.SelectionExpr{Op: driver.SelectionRefs, Refs: []model.Ref{candidate}}
	before, err := tx.(driver.SelectionReader).Select(ctx, plan)
	if err != nil || before.Total != 0 {
		t.Fatalf("before publication=%+v %v", before, err)
	}
	seedSelectionSQL(t, s, l, []driver.SelectionBead{{Ref: candidate, Type: "issue"}})
	if _, err = tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	fresh := tx.(driver.FreshSelectionExistsReader)
	if yes, err := fresh.FreshSelectionExists(ctx, plan); yes || !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("stale plan minted an answer: %v %v", yes, err)
	}
	rebound := selectionSQLPlan(tx)
	rebound.Mode, rebound.Page = driver.SelectionCountMode, nil
	rebound.Membership = plan.Membership
	if yes, err := fresh.FreshSelectionExists(ctx, rebound); err != nil || !yes {
		t.Fatalf("fresh committed observation=%v %v", yes, err)
	}
}

// Output work counters are event accounting, not executor or metadata-input
// measurements. In particular, count/summary emit no keys but still run SQL.
type selectionSQLWork struct {
	mu     sync.Mutex
	counts map[driver.ReadWorkKind]int64
}

func (w *selectionSQLWork) ObserveReadWork(kind driver.ReadWorkKind, n int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.counts == nil {
		w.counts = map[driver.ReadWorkKind]int64{}
	}
	w.counts[kind] += n
}

func TestSelectionStagedPreflightUsesThinSourceHeaders(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	l := testLease(t, s)
	ref := testRef("pl-thin-preflight")
	seedSelectionSQL(t, s, l, []driver.SelectionBead{{Ref: ref, Type: "issue"}})
	r := testRead(t, s, nil)
	rows, err := r.DerivedRows(ctx, driver.DerivedSortKey, "w6-fixture:"+ref.ID)
	if err != nil || len(rows) != 1 {
		t.Fatalf("projection=%v %v", rows, err)
	}
	projectionKey := rows[0].Index.Row
	factKey := driver.DerivedRowKey{Table: driver.DerivedAttribute, Key: "w6-thin-labels"}
	value := []byte(`{"complete":"labels"}`)
	facts := driver.DerivedRow{Key: factKey.Key, Value: value, Index: &driver.SelectionIndex{Version: driver.SelectionIndexVersion, Row: factKey, Incarnation: r.Snapshot().Incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(value), Source: rows[0].Index.Source, Facts: &driver.SelectionFacts{Kind: driver.SelectionLabelFact, Rows: []driver.SelectionFact{{Kind: driver.SelectionLabelFact, Ref: &ref, Name: "visible"}}}}}
	// A staged core fact cannot create its own committed family readiness.
	// Explicitly build the complete Label family through the public startup API.
	family := driver.SelectionProjectionFamily{Table: factKey.Table, Kind: driver.SelectionLabelFact, Version: driver.SelectionIndexVersion}
	requirePGFamily(t, r.(*reader), family, r.(*reader).at, false)
	project := driver.GraphSourceProjection{Families: []driver.SelectionProjectionFamily{family}, MaxPageRows: 1,
		Inputs: func(driver.Record) ([]driver.DerivedRowRange, error) { return nil, nil },
		Decode: func(record driver.Record, _ []driver.DerivedRow) ([]driver.DerivedSelectionRow, error) {
			row := facts.Clone()
			row.Index.Source = driver.SelectionSource{Key: testLock(record.Ref), Revision: record.Revision, Present: true, Digest: driver.SelectionDigest(record.Body)}
			return []driver.DerivedSelectionRow{{Table: factKey.Table, Row: row}}, nil
		},
	}
	if err := s.BuildGraphSourceIndexes(ctx, l, func(driver.Record) (driver.GraphSourceMetadata, error) { return driver.GraphSourceMetadata{}, nil }, project); err != nil {
		t.Fatal(err)
	}
	requirePGFamily(t, pgStartupRead(t, s), family, r.(*reader).at, true)
	tx, _ := beginTest(t, s, l, testLock(ref), driver.LockKey{Kind: driver.LockDerived, Derived: &projectionKey}, driver.LockKey{Kind: driver.LockDerived, Derived: &factKey})
	defer tx.Rollback(ctx)
	writer := tx.(driver.SelectionIndexWriter)
	if err := writer.PutSelectionDerived(ctx, projectionKey.Table, nil, rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.PutSelectionDerived(ctx, factKey.Table, nil, []driver.DerivedRow{facts}); err != nil {
		t.Fatal(err)
	}
	p := selectionSQLPlan(tx)
	p.Authorization = driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: factKey, Kind: driver.SelectionLabelFact, Tests: []driver.SelectionFactTest{{Column: driver.SelectionFactRef, Candidate: driver.SelectionID}}}}
	for _, mode := range []driver.SelectionMode{driver.SelectionPageMode, driver.SelectionCountMode, driver.SelectionTypeSummary} {
		t.Run(string(mode), func(t *testing.T) {
			p.Mode, p.Page, p.GroupBy = mode, nil, ""
			wantKeys, wantGroups := int64(0), int64(0)
			if mode == driver.SelectionPageMode {
				p.Page, wantKeys = &driver.SelectionPage{Limit: 1}, 1
			}
			if mode == driver.SelectionTypeSummary {
				p.GroupBy, wantGroups = driver.SelectionGroupType, 1
			}
			work := &selectionSQLWork{}
			got, err := tx.(driver.SelectionReader).Select(driver.WithReadWorkObserver(ctx, work), selectionSQLSeal(p))
			if err != nil || got.Total != 1 || int64(len(got.Keys)) != wantKeys || int64(len(got.Groups)) != wantGroups {
				t.Fatalf("thin selection=%+v %v", got, err)
			}
			work.mu.Lock()
			defer work.mu.Unlock()
			if work.counts[driver.ReadPayloadRecords] != 0 || work.counts[driver.ReadPayloadBytes] != 0 || work.counts[driver.ReadPrefixStates] != 0 || work.counts[driver.ReadSelectorKeys] != wantKeys || work.counts[driver.ReadAggregateGroups] != wantGroups {
				t.Fatalf("unexpected instrumented events=%v", work.counts)
			}
		})
	}
}

func TestFreshSelectionExistsValidatesEmptyDomainControls(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	l := testLease(t, s)
	seedSelectionSQL(t, s, l, []driver.SelectionBead{{Ref: testRef("pl-fresh-controls"), Type: "issue"}})
	tx, _ := beginTest(t, s, l, testLock(testRef("pl-unrelated-control-lock")))
	defer tx.Rollback(ctx)
	base := selectionSQLPlan(tx)
	base.Mode, base.Page = driver.SelectionCountMode, nil
	fresh := tx.(driver.FreshSelectionExistsReader)
	missing := driver.SelectionExpr{Op: driver.SelectionExistsOp, Exists: &driver.SelectionExists{Row: driver.DerivedRowKey{Table: driver.DerivedGrant, Key: "w6-missing-facts"}, Kind: driver.SelectionGrantFact}}
	for _, membership := range []driver.SelectionExpr{{Op: driver.SelectionFalse}, {Op: driver.SelectionRefs, Refs: []model.Ref{}}, {Op: driver.SelectionNot, Args: []driver.SelectionExpr{{Op: driver.SelectionTrue}}}} {
		p := base
		p.Membership = membership
		if yes, err := fresh.FreshSelectionExists(ctx, p); err != nil || yes {
			t.Fatalf("valid committed absence=%v %v", yes, err)
		}
		badSource := p
		spec, _ := p.Boundary.Description()
		// A never-created source has revision zero, even when absent. Changing
		// only its expected token must refuse an otherwise empty relation.
		spec.Sources[0].Revision = p.AuthorityRevision
		badSource.Boundary, _ = driver.PrepareSelectionBoundary(spec)
		if yes, err := fresh.FreshSelectionExists(ctx, badSource); yes || !errors.Is(err, driver.ErrSelectionUnavailable) {
			t.Fatalf("missing source control minted absence=%v %v", yes, err)
		}
		for _, authorization := range []driver.SelectionExpr{missing, {Op: driver.SelectionNot, Args: []driver.SelectionExpr{missing}}, {Op: driver.SelectionAll, Args: []driver.SelectionExpr{{Op: driver.SelectionFalse}, missing}}} {
			p.Authorization = authorization
			if yes, err := fresh.FreshSelectionExists(ctx, selectionSQLSeal(p)); yes || !errors.Is(err, driver.ErrSelectionUnavailable) {
				t.Fatalf("uncovered branch minted absence=%v %v", yes, err)
			}
		}
	}
}

func TestSelectionParentDomainAndNullableCurrentSourceNegation(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	parent, child := testRef("pl-parent-only"), testRef("pl-parent-only-child")
	seedSelectionSQLSourceDomains(t, s, lease, []driver.SelectionBead{{Ref: parent, Type: "issue"}, {Ref: child, Parent: &parent, Type: "issue"}}, false)
	r := testRead(t, s, nil)
	p := selectionSQLPlan(r)
	children := driver.SelectionExpr{Op: driver.SelectionChildrenOp, Sources: &driver.SelectionSources{Refs: []model.Ref{parent}}}
	p.Membership = children
	requireSelectionIDs(t, r, p, 1, child.ID)
	incoming := driver.SelectionExpr{Op: driver.SelectionIncomingOp, Sources: &driver.SelectionSources{Refs: []model.Ref{}, Kinds: []driver.IncomingLinkKind{driver.IncomingLinkBlocks}}}
	for _, membership := range []driver.SelectionExpr{incoming, {Op: driver.SelectionNot, Args: []driver.SelectionExpr{incoming}}, {Op: driver.SelectionAll, Args: []driver.SelectionExpr{{Op: driver.SelectionFalse}, incoming}}} {
		p.Membership = membership
		if got, err := r.(driver.SelectionReader).Select(ctx, p); !errors.Is(err, driver.ErrSelectionUnavailable) || !reflect.DeepEqual(got, driver.SelectionResult{}) {
			t.Fatalf("unbuilt Links hidden by empty/false/NOT: %+v %v", got, err)
		}
	}
	created := testRef("pl-parent-only-staged")
	oldKey := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "w6-fixture:" + child.ID}
	newKey := driver.DerivedRowKey{Table: driver.DerivedSortKey, Key: "w6-fixture:" + created.ID}
	oldBead, _ := driver.NewBeadSentinel(child.ID)
	newBead, _ := driver.NewBeadSentinel(created.ID)
	tx, _ := beginTest(t, s, lease, testLock(child), testLock(created), driver.GraphBlockingSentinelLock,
		driver.LockKey{Kind: driver.LockDerived, Derived: &oldKey}, driver.LockKey{Kind: driver.LockDerived, Derived: &newKey},
		driver.LockKey{Kind: driver.LockSentinel, Sentinel: oldBead}, driver.LockKey{Kind: driver.LockSentinel, Sentinel: newBead})
	defer tx.Rollback(ctx)
	if err := tx.Delete(ctx, child); err != nil {
		t.Fatal(err)
	}
	if err := tx.(driver.SelectionIndexWriter).PutSelectionDerived(ctx, oldKey.Table, []driver.DerivedRow{{Key: oldKey.Key}}, nil); err != nil {
		t.Fatal(err)
	}
	body := []byte(`{}`)
	if err := tx.(driver.HierarchySourceWriter).PutHierarchyRecord(ctx, driver.Record{Ref: created, Kind: "bead", Body: body}, &parent); err != nil {
		t.Fatal(err)
	}
	row := driver.DerivedRow{Key: newKey.Key, Value: body, Index: &driver.SelectionIndex{
		Version: driver.SelectionIndexVersion, Row: newKey, Incarnation: tx.Snapshot().Incarnation, Branch: model.Natural64(0), ValueDigest: driver.SelectionDigest(body),
		Source: driver.SelectionSource{Key: testLock(created), Revision: model.Natural64(0), Present: true, Digest: driver.SelectionDigest(body)},
		Bead:   &driver.SelectionBead{Ref: created, Parent: &parent, Type: "issue"},
	}}
	if err := tx.(driver.SelectionIndexWriter).PutSelectionDerived(ctx, newKey.Table, nil, []driver.DerivedRow{row}); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{oldBead, newBead, driver.SentinelGraphBlocking}); err != nil {
		t.Fatal(err)
	}
	staged := selectionSQLPlan(tx)
	staged.Membership = children
	requireSelectionIDs(t, tx, staged, 1, created.ID)
	// Old committed membership survives a current delete. The new staged
	// matching child must not turn NOT(current Children) into SQL NULL.
	historical := selectionSQLPlan(tx)
	historical.ContentCommitted = true
	historical.Membership = driver.SelectionExpr{Op: driver.SelectionRefs, Refs: []model.Ref{child}}
	historical.Authorization = driver.SelectionExpr{Op: driver.SelectionNot, Args: []driver.SelectionExpr{children}}
	requireSelectionIDs(t, tx, historical, 1, child.ID)
	fresh := selectionSQLPlan(tx)
	fresh.Mode, fresh.Page, fresh.Membership = driver.SelectionCountMode, nil, children
	if yes, err := tx.(driver.FreshSelectionExistsReader).FreshSelectionExists(ctx, fresh); err != nil || !yes {
		t.Fatalf("Parent-only fresh committed eligibility=%v %v", yes, err)
	}
	fresh.Membership = driver.SelectionExpr{Op: driver.SelectionAll, Args: []driver.SelectionExpr{{Op: driver.SelectionFalse}, incoming}}
	if yes, err := tx.(driver.FreshSelectionExistsReader).FreshSelectionExists(ctx, fresh); yes || !errors.Is(err, driver.ErrSelectionUnavailable) {
		t.Fatalf("unbuilt Links minted fresh absence=%v %v", yes, err)
	}
}
