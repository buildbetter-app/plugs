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
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

var errProjectionCommit = errors.New("injected failure after projection staging")

// Derive the original Ready item's JSON from the persisted source, rather
// than calling either producer helper or using a projection's claimed leaf.
func projectionReadyDigest(t *testing.T, source driver.Record) string {
	t.Helper()
	var doc struct {
		ID, Type, Project string
		Fields            map[string]any
	}
	decoder := json.NewDecoder(bytes.NewReader(source.Body))
	decoder.UseNumber()
	if err := decoder.Decode(&doc); err != nil {
		t.Fatal(err)
	}
	status, _ := doc.Fields["status"].(string)
	item := struct {
		ID       string         `json:"id"`
		Type     string         `json:"type"`
		Project  string         `json:"project"`
		Status   string         `json:"status"`
		Revision model.Revision `json:"revision"`
		Fields   map[string]any `json:"fields,omitempty"`
	}{doc.ID, doc.Type, doc.Project, status, source.Revision, doc.Fields}
	body, err := json.Marshal(item)
	if err != nil {
		t.Fatal(err)
	}
	return driver.SelectionDigest(body)
}

func TestSourceSelectionReadyLeafUsesCommittedSourceRevision(t *testing.T) {
	h := setup(t)
	w, _ := projectionWriter(t, h)
	created := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1))
	ref := created.Results[0].Ref
	old := h.read(t)
	check := func(r driver.Reader) string {
		source := projectionSource(t, r, ref)
		rows, err := r.DerivedRows(context.Background(), driver.DerivedSortKey, ref.ID)
		if err != nil || len(rows) != 1 || rows[0].Index == nil || rows[0].Index.Bead == nil {
			t.Fatalf("missing committed counting row: %v/%v", rows, err)
		}
		row, leaf := rows[0], projectionReadyDigest(t, source)
		var logical driver.SelectionBead
		if err := json.Unmarshal(row.Value, &logical); err != nil || logical.MembershipLeaf != leaf || row.Index.Bead.MembershipLeaf != leaf || row.Index.Validate(driver.DerivedSortKey, row) != nil || row.Index.Source.Revision.String() != source.Revision.String() || row.Index.Source.Digest != driver.SelectionDigest(source.Body) {
			t.Fatal("Ready leaf or annotation does not bind the exact committed source")
		}
		return leaf
	}
	oldLeaf := check(old)
	apply(t, w, model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"description": "changes full semantic membership"})}}, meta(2))
	after := h.read(t)
	if check(after) == oldLeaf || check(old) != oldLeaf {
		t.Fatal("source revision/body change lost membership invalidation or pinned history")
	}
	// Startup/backfill supplies the source revision directly, independently
	// of any derived-row or most-recent store revision.
	source := projectionSource(t, after, ref)
	plan, err := PrepareSourceRecord(source)
	if err != nil {
		t.Fatal(err)
	}
	for _, revision := range []uint64{7, 19} {
		source.Revision = model.Natural64(revision)
		rows, err := plan.SelectionRows("startup", source.Revision)
		if err != nil || rows[0].Index.Bead.MembershipLeaf != projectionReadyDigest(t, source) || rows[0].Index.Source.Revision != source.Revision {
			t.Fatalf("startup source revision %d was not bound: %v", revision, err)
		}
	}
}

// Count the real snapshot boundary without replacing its data or write path.
// The hidden control exposes only Reader, retaining the individual-read path.
type projectionRefreshStore struct {
	driver.Store
	mode                          string
	cancel                        context.CancelFunc
	singles, batches, rows, scans int
	prefixes                      []string
	failedPrefix                  string
}

func (s *projectionRefreshStore) Read(ctx context.Context, view driver.View) (driver.Reader, error) {
	r, err := s.Store.Read(ctx, view)
	if err != nil {
		return nil, err
	}
	base := &projectionRefreshReader{Reader: r, store: s}
	if s.mode == "hidden" {
		return base, nil
	}
	return &projectionRefreshPrefixReader{base}, nil
}

type projectionRefreshReader struct {
	driver.Reader
	store *projectionRefreshStore
}

func (r *projectionRefreshReader) DerivedRows(ctx context.Context, table driver.DerivedTable, prefix string) ([]driver.DerivedRow, error) {
	r.store.singles++
	if prefix == r.store.failedPrefix {
		return nil, errors.New("optional individual closure read unavailable")
	}
	rows, err := r.Reader.DerivedRows(ctx, table, prefix)
	r.store.rows += len(rows)
	return rows, err
}

func (r *projectionRefreshReader) Query(ctx context.Context, q driver.Query) (driver.Page[driver.Record], error) {
	r.store.scans++
	return r.Reader.Query(ctx, q)
}

type projectionRefreshPrefixReader struct{ *projectionRefreshReader }

func (r *projectionRefreshPrefixReader) DerivedRowsForPrefixes(ctx context.Context, table driver.DerivedTable, prefixes []string) ([]driver.DerivedRow, error) {
	r.store.batches++
	r.store.prefixes = append([]string(nil), prefixes...)
	switch r.store.mode {
	case "unsupported":
		return nil, driver.ErrUnsupported
	case "failure", "individual_failure":
		return nil, errors.New("optional union closure read unavailable")
	}
	reader, ok := r.Reader.(driver.DerivedPrefixReader)
	if !ok {
		return nil, driver.ErrUnsupported
	}
	rows, err := reader.DerivedRowsForPrefixes(ctx, table, prefixes)
	r.store.rows += len(rows)
	if err == nil {
		switch r.store.mode {
		case "foreign":
			row, _ := AncestorRow("unrequested", "unrequested", 0)
			rows = append(rows, row)
		case "duplicate":
			rows = append(rows, rows[0])
		case "cancel":
			r.store.cancel()
		}
	}
	return rows, err
}

func TestSourceProjectionRefreshBatchesChangedSources(t *testing.T) {
	for _, mode := range []string{"union", "hidden"} {
		t.Run(mode, func(t *testing.T) {
			h := setup(t)
			w, guarded := projectionWriter(t, h)
			ops := make([]model.Op, 100)
			for i := range ops {
				ops[i] = create("")
			}
			seed := apply(t, w, model.ChangeSet{Ops: ops}, meta(1))
			original := h.read(t)
			before := make([]driver.Record, len(seed.Results))
			closures := make([][]driver.DerivedRow, len(seed.Results))
			prefixes := make([]string, len(seed.Results))
			for i, result := range seed.Results {
				before[i] = projectionSource(t, original, result.Ref)
				closures[i] = projectionClosure(t, original, result.Ref)
				prefixes[i], _ = AncestorPrefix(result.Ref.ID)
			}
			sort.Strings(prefixes)
			for sample := 0; sample < 2; sample++ {
				// Grow unrelated population, retaining the same 100 affected roots.
				w.store = guarded
				untouched := apply(t, w, model.ChangeSet{Ops: ops}, meta(2+sample*2)).Results[0].Ref
				old := h.read(t)
				untouchedSource := projectionSource(t, old, untouched)
				untouchedRows := projectionClosure(t, old, untouched)
				counted := &projectionRefreshStore{Store: guarded, mode: mode}
				w.store = counted
				patches := make([]model.Op, len(seed.Results))
				for i, result := range seed.Results {
					patches[i] = patch(result.Ref, map[string]any{"title": fmt.Sprintf("refresh-%d", sample)})
				}
				apply(t, w, model.ChangeSet{Ops: patches}, meta(3+sample*2))
				wantSingles, wantBatches := 0, 1
				if mode == "hidden" {
					wantSingles, wantBatches = 100, 0
				}
				if counted.singles != wantSingles || counted.batches != wantBatches || counted.rows != 100 || counted.scans != 0 {
					t.Fatalf("population=%d boundary singles/batches/rows/scans=%d/%d/%d/%d, want%d/%d/100/0", 200+100*sample, counted.singles, counted.batches, counted.rows, counted.scans, wantSingles, wantBatches)
				}
				if mode == "union" && !reflect.DeepEqual(counted.prefixes, prefixes) {
					t.Fatal("refresh selected a range outside the exact changed-source union")
				}
				after := h.read(t)
				for i, result := range seed.Results {
					source := projectionSource(t, after, result.Ref)
					rows := projectionClosure(t, after, result.Ref)
					if bytes.Equal(source.Body, before[i].Body) || len(rows) != len(closures[i]) {
						t.Fatal("ordinary update lost its source or logical closure")
					}
					for j, row := range rows {
						if row.Key != closures[i][j].Key || !bytes.Equal(row.Value, closures[i][j].Value) || !canRefreshAncestorProjection(row, source, after.Snapshot()) {
							t.Fatal("union refresh changed membership or lost exact final source provenance")
						}
					}
					if !reflect.DeepEqual(before[i], projectionSource(t, original, result.Ref)) || !reflect.DeepEqual(closures[i], projectionClosure(t, original, result.Ref)) {
						t.Fatal("refresh changed an earlier pinned source or closure")
					}
				}
				if !reflect.DeepEqual(untouchedSource, projectionSource(t, after, untouched)) || !reflect.DeepEqual(untouchedRows, projectionClosure(t, after, untouched)) {
					t.Fatal("refresh published an unrelated source or closure")
				}
				t.Logf("actual Writer population=%d affected100: individual%d union%d returned100 Query0; exact durable bindings and pinned history preserved", 200+100*sample, counted.singles, counted.batches)
			}
		})
	}
}

func TestSourceProjectionBatchReadFallbackAndCancellation(t *testing.T) {
	for _, mode := range []string{"unsupported", "failure", "foreign", "duplicate", "individual_failure", "cancel", "stale", "rollback"} {
		t.Run(mode, func(t *testing.T) {
			h := setup(t)
			w, guarded := projectionWriter(t, h)
			seed := apply(t, w, model.ChangeSet{Ops: []model.Op{create(""), create("")}}, meta(1))
			refs := []model.Ref{seed.Results[0].Ref, seed.Results[1].Ref}
			if mode == "stale" {
				source := projectionSource(t, h.read(t), refs[0])
				doc, _ := DecodeDocument(source.Body)
				doc.Fields["title"] = "graph-only source rewrite"
				body, _ := json.Marshal(doc)
				rewriteGraphOnly(t, h, source, body)
			}
			old := h.read(t)
			before := []driver.Record{projectionSource(t, old, refs[0]), projectionSource(t, old, refs[1])}
			closures := [][]driver.DerivedRow{projectionClosure(t, old, refs[0]), projectionClosure(t, old, refs[1])}
			beforeMeta, err := h.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			counted := &projectionRefreshStore{Store: guarded, mode: mode, cancel: cancel}
			if mode == "individual_failure" {
				counted.failedPrefix, _ = AncestorPrefix(refs[0].ID)
			}
			w.store, guarded.fail = counted, mode == "rollback"
			_, err = w.Apply(ctx, by, model.ChangeSet{Ops: []model.Op{patch(refs[0], map[string]any{"title": "updated-0"}), patch(refs[1], map[string]any{"title": "updated-1"})}}, meta(3))
			failed := mode == "cancel" || mode == "rollback"
			if mode == "cancel" && !errors.Is(err, context.Canceled) || mode == "rollback" && !errors.Is(err, errProjectionCommit) || !failed && err != nil {
				t.Fatalf("mode=%s error=%v", mode, err)
			}
			wantSingles := 2
			if mode == "cancel" || mode == "stale" || mode == "rollback" {
				wantSingles = 0
			}
			if counted.batches != 1 || counted.singles != wantSingles {
				t.Fatalf("boundary union/individual=%d/%d want1/%d", counted.batches, counted.singles, wantSingles)
			}
			after := h.read(t)
			for i, ref := range refs {
				source, rows := projectionSource(t, after, ref), projectionClosure(t, after, ref)
				if failed {
					if !reflect.DeepEqual(source, before[i]) || !reflect.DeepEqual(rows, closures[i]) {
						t.Fatal("failed union-backed write published a source or projection")
					}
					continue
				}
				unavailable := i == 0 && (mode == "individual_failure" || mode == "stale")
				if bytes.Equal(source.Body, before[i].Body) || len(rows) != len(closures[i]) {
					t.Fatal("optional coverage failure refused or lost an ordinary source update")
				}
				for j, row := range rows {
					if row.Key != closures[i][j].Key || !bytes.Equal(row.Value, closures[i][j].Value) || canRefreshAncestorProjection(row, source, after.Snapshot()) == unavailable {
						t.Fatal("fallback changed logical membership or recertified unavailable coverage")
					}
					if unavailable && !reflect.DeepEqual(row.Index, closures[i][j].Index) {
						t.Fatal("unavailable annotation was silently repaired")
					}
				}
			}
			if failed {
				afterMeta, err := h.store.Meta(context.Background())
				principal, _ := driver.PrincipalKey(by)
				receipt, receiptErr := after.IdempotencyByKey(context.Background(), principal, "", "tx.apply", meta(3).IdempotencyKey)
				if err != nil || !reflect.DeepEqual(beforeMeta, afterMeta) || receiptErr != nil || receipt != nil {
					t.Fatal("failed refresh advanced Head or published a receipt")
				}
			}
		})
	}
}

// This wrapper preserves the real driver's optional capabilities. It never
// manufactures annotations or succeeds when the underlying writer lacks them.
type projectionTestStore struct {
	driver.Store
	t        *testing.T
	fail     bool
	staged   map[driver.DerivedRowKey]driver.DerivedRow
	sources  map[string]driver.Record
	attempts int
}

func (s *projectionTestStore) Write(ctx context.Context, lease driver.ActiveLease) (driver.WriteTx, error) {
	tx, err := s.Store.Write(ctx, lease)
	if err != nil {
		return nil, err
	}
	return &projectionTestTx{WriteTx: tx, store: s, locks: map[string]bool{}}, nil
}

type projectionTestTx struct {
	driver.WriteTx
	store *projectionTestStore
	locks map[string]bool
	head  bool
}

func (tx *projectionTestTx) Lock(ctx context.Context, locks []driver.LockRequest) ([]driver.LockState, error) {
	if tx.head {
		tx.store.t.Fatal("projection lock discovered after Head")
	}
	for _, lock := range locks {
		key, err := driver.EncodeLockKey(lock.Key)
		if err != nil {
			return nil, err
		}
		tx.locks[key] = true
	}
	return tx.WriteTx.Lock(ctx, locks)
}

func (tx *projectionTestTx) Head(ctx context.Context) (model.Revision, error) {
	tx.head = true
	return tx.WriteTx.Head(ctx)
}

func (tx *projectionTestTx) ConsumeParentChild(ctx context.Context, parent, expected string) error {
	writer, ok := tx.WriteTx.(driver.ParentCounterContinuation)
	if !ok {
		return driver.ErrUnsupported
	}
	return writer.ConsumeParentChild(ctx, parent, expected)
}

func (tx *projectionTestTx) LoadParentCounter(ctx context.Context, parent string) (driver.ParentCounter, error) {
	reader, ok := tx.WriteTx.(driver.ParentCounterReader)
	if !ok {
		return driver.ParentCounter{}, driver.ErrUnsupported
	}
	return reader.LoadParentCounter(ctx, parent)
}

func (tx *projectionTestTx) requireLock(key driver.LockKey) {
	tx.store.t.Helper()
	encoded, err := driver.EncodeLockKey(key)
	if err != nil || !tx.locks[encoded] || !tx.head {
		tx.store.t.Fatalf("source/projection key missing from pre-Head census: %+v error=%v", key, err)
	}
}

func (tx *projectionTestTx) PutGraphRecord(ctx context.Context, record driver.Record, parent *model.Ref, links []driver.SourceLink) error {
	tx.requireLock(driver.LockKey{Kind: driver.LockRecord, Ref: &record.Ref})
	writer, ok := tx.WriteTx.(driver.GraphSourceWriter)
	if !ok {
		return driver.ErrUnsupported
	}
	if err := writer.PutGraphRecord(ctx, record, parent, links); err != nil {
		return err
	}
	record.Body = bytes.Clone(record.Body)
	tx.store.sources[record.Ref.ID] = record
	return nil
}

func (tx *projectionTestTx) PutSelectionDerived(ctx context.Context, table driver.DerivedTable, del, add []driver.DerivedRow) error {
	for _, rows := range [][]driver.DerivedRow{del, add} {
		for _, row := range rows {
			key := driver.DerivedRowKey{Table: table, Key: row.Key}
			tx.requireLock(driver.LockKey{Kind: driver.LockDerived, Derived: &key})
			if row.Index != nil {
				tx.requireLock(row.Index.Source.Key)
			}
		}
	}
	writer, ok := tx.WriteTx.(driver.SelectionIndexWriter)
	if !ok {
		return driver.ErrUnsupported
	}
	if err := writer.PutSelectionDerived(ctx, table, del, add); err != nil {
		return err
	}
	for _, row := range add {
		tx.store.staged[driver.DerivedRowKey{Table: table, Key: row.Key}] = row.Clone()
	}
	return nil
}

func (tx *projectionTestTx) Commit(ctx context.Context, header driver.ChangesetHeader) (model.Changeset, error) {
	if tx.store.fail {
		tx.store.attempts++
		return model.Changeset{}, errProjectionCommit
	}
	return tx.WriteTx.Commit(ctx, header)
}

func projectionWriter(t *testing.T, h *harness) (*Writer, *projectionTestStore) {
	t.Helper()
	s := &projectionTestStore{Store: h.store, t: t, staged: map[driver.DerivedRowKey]driver.DerivedRow{}, sources: map[string]driver.Record{}}
	w := h.writer()
	w.store = s
	return w, s
}

func projectionSource(t *testing.T, r driver.Reader, ref model.Ref) driver.Record {
	t.Helper()
	rows, err := r.Get(context.Background(), []model.Ref{ref})
	if err != nil || len(rows) != 1 {
		t.Fatalf("source observation: rows=%d error=%v", len(rows), err)
	}
	return rows[0]
}

func projectionClosure(t *testing.T, r driver.Reader, ref model.Ref) []driver.DerivedRow {
	t.Helper()
	prefix, err := AncestorPrefix(ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := r.DerivedRows(context.Background(), driver.DerivedAncestorClosure, prefix)
	if err != nil {
		t.Fatal(err)
	}
	return rows
}

func projectionStates(t *testing.T, r driver.Reader, ref model.Ref) []driver.LockState {
	t.Helper()
	rows, err := SourceSelectionRows(ref)
	if err != nil {
		t.Fatal(err)
	}
	keys := make([]driver.LockKey, len(rows))
	for i, row := range rows {
		row := row
		keys[i] = driver.LockKey{Kind: driver.LockDerived, Derived: &row}
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	states, err := r.Observe(context.Background(), keys)
	if err != nil || len(states) != len(rows) {
		t.Fatal("projection state observation:", err)
	}
	return states
}

// Use the production graph-only adapter to leave an unavailable selection
// binding. This is a legitimate ordinary source publication, not fake history.
func rewriteGraphOnly(t *testing.T, h *harness, before driver.Record, body []byte) {
	t.Helper()
	ctx := context.Background()
	plan, err := PrepareSourceRecord(driver.Record{Ref: before.Ref, Kind: before.Kind, Body: body})
	if err != nil {
		t.Fatal(err)
	}
	tx, err := h.store.Write(ctx, h.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	keys := plan.LockKeys()
	locks := make(driver.LockSet, len(keys))
	for i, key := range keys {
		locks[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	if _, err := tx.Lock(ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	record := before
	record.Body = bytes.Clone(body)
	if err := PutSourceRecord(ctx, tx, record); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, plan.Sentinels()); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Commit(ctx, driver.ChangesetHeader{Incarnation: "pipeline-test", Kind: model.ChangesetWrite, Principal: by, LockKeys: keys, Changes: []model.ChangeRecord{{Ref: before.Ref, Op: model.OpPatch, FromRevision: before.Revision, ChangedPaths: []string{"parent"}}}})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSourceProjectionRefreshRequiresValidOldClosure(t *testing.T) {
	for _, protected := range []bool{false, true} {
		for _, stale := range []bool{false, true} {
			t.Run(fmt.Sprintf("protected=%t/stale=%t", protected, stale), func(t *testing.T) {
				h := setup(t)
				w, _ := projectionWriter(t, h)
				parent := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1)).Results[0].Ref
				child := create("")
				child.Create.Parent = &parent
				ref := apply(t, w, model.ChangeSet{Ops: []model.Op{child}}, meta(2)).Results[0].Ref
				original := h.read(t)
				before := projectionSource(t, original, ref)
				closure := projectionClosure(t, original, ref)
				if len(closure) != 2 {
					t.Fatalf("expected self and parent closure, got %d", len(closure))
				}
				for _, row := range closure {
					if !canRefreshAncestorProjection(row, before, original.Snapshot()) {
						t.Fatal("positive control did not start from actually bound closure")
					}
				}
				if stale {
					doc, err := DecodeDocument(before.Body)
					if err != nil {
						t.Fatal(err)
					}
					doc.Parent = nil // membership changes; old logical closure remains.
					body, err := json.Marshal(doc)
					if err != nil {
						t.Fatal(err)
					}
					rewriteGraphOnly(t, h, before, body)
					before = projectionSource(t, h.read(t), ref)
				}
				if protected {
					sourceRef := model.RefByID("ontology/projection-test", model.Natural64(0))
					if _, err := conformance.Seed(context.Background(), h.store, h.lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: sourceRef, Kind: "ontology", Op: model.OpCreate, Body: []byte(`{"version":1}`)}}); err != nil {
						t.Fatal(err)
					}
					r := h.read(t)
					source := projectionSource(t, r, sourceRef)
					doc, err := DecodeDocument(before.Body)
					if err != nil {
						t.Fatal(err)
					}
					doc.Metadata = map[string]json.RawMessage{"note": json.RawMessage(`"body-only"`)}
					body, err := json.Marshal(doc)
					if err != nil {
						t.Fatal(err)
					}
					m := meta(3)
					m.Method = "ontology.apply"
					_, err = w.ApplyProtected(context.Background(), by, ProtectedApplyRequest{PlanDigest: strings.Repeat("a", 64), BaseRevision: *r.Snapshot().At, Source: ProtectedRecordChange{Ref: source.Ref, Kind: source.Kind, FromRevision: source.Revision, Before: source.Body, After: source.Body, Operation: model.OpPatch}, Rows: []ProtectedRecordChange{{Ref: ref, Kind: "bead", FromRevision: before.Revision, Before: before.Body, After: body, Operation: model.OpPatch}}}, m)
					if err != nil {
						t.Fatal(err)
					}
				} else {
					apply(t, w, model.ChangeSet{Ops: []model.Op{{Kind: model.OpSetMeta, SetMeta: &model.SetMetaOp{Target: ref, Key: "note", Value: json.RawMessage(`"body-only"`)}}}}, meta(3))
				}
				r := h.read(t)
				after := projectionSource(t, r, ref)
				got := projectionClosure(t, r, ref)
				if bytes.Equal(before.Body, after.Body) || before.Revision.String() == after.Revision.String() || len(got) != len(closure) {
					t.Fatal("body-only operation did not publish the expected source while retaining logical closure")
				}
				for i, row := range got {
					if row.Key != closure[i].Key || !bytes.Equal(row.Value, closure[i].Value) {
						t.Fatal("body-only write changed logical closure bytes")
					}
					valid := canRefreshAncestorProjection(row, after, r.Snapshot())
					if valid == stale {
						t.Fatalf("closure refresh valid=%t after stale=%t", valid, stale)
					}
					if stale && !reflect.DeepEqual(row.Index, closure[i].Index) {
						t.Fatal("stale membership was recertified")
					}
				}
			})
		}
	}
}

func TestSourceProjectionFailureAfterStagingRollsBack(t *testing.T) {
	h := setup(t)
	w, store := projectionWriter(t, h)
	ref := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1)).Results[0].Ref
	r := h.read(t)
	before := projectionSource(t, r, ref)
	closure := projectionClosure(t, r, ref)
	beforeStates := projectionStates(t, r, ref)
	beforeMeta, err := h.store.Meta(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	store.fail, store.staged, store.sources = true, map[driver.DerivedRowKey]driver.DerivedRow{}, map[string]driver.Record{}
	_, err = w.Apply(context.Background(), by, model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"title": "must roll back"})}}, meta(2))
	if !errors.Is(err, errProjectionCommit) || store.attempts != 1 {
		t.Fatalf("fault did not reach one terminal Commit: %v attempts=%d", err, store.attempts)
	}
	stagedSource, ok := store.sources[ref.ID]
	if !ok || bytes.Equal(stagedSource.Body, before.Body) {
		t.Fatal("fault did not follow the actual source stage")
	}
	closureStaged := 0
	for key, row := range store.staged {
		if row.Index == nil || row.Index.Validate(key.Table, row) != nil || row.Index.Source.Key.Ref == nil || !driver.SelectionSameRef(*row.Index.Source.Key.Ref, ref) || row.Index.Source.Revision.String() != before.Revision.String() || row.Index.Source.Digest != driver.SelectionDigest(stagedSource.Body) {
			t.Fatalf("actually staged projection lacks exact final source binding: %+v", row.Index)
		}
		if key.Table == driver.DerivedAncestorClosure && row.Index != nil && row.Index.Source.Digest != driver.SelectionDigest(before.Body) {
			closureStaged++
		}
	}
	if len(store.staged) != 4 || closureStaged != 1 {
		t.Fatalf("fault occurred before complete source/projection staging: %d rows, %d fresh closure", len(store.staged), closureStaged)
	}
	after := h.read(t)
	afterMeta, err := h.store.Meta(context.Background())
	if err != nil || !reflect.DeepEqual(beforeMeta, afterMeta) || !reflect.DeepEqual(before, projectionSource(t, after, ref)) || !reflect.DeepEqual(closure, projectionClosure(t, after, ref)) || !reflect.DeepEqual(beforeStates, projectionStates(t, after, ref)) {
		t.Fatal("rollback published source/projection/head changes")
	}
	principal, _ := driver.PrincipalKey(by)
	receipt, err := after.IdempotencyByKey(context.Background(), principal, "", "tx.apply", meta(2).IdempotencyKey)
	if err != nil || receipt != nil {
		t.Fatal("rollback published an idempotency receipt")
	}
}

func TestSourceProjectionDeleteRetainsHistoricalBindings(t *testing.T) {
	h := setup(t)
	w, _ := projectionWriter(t, h)
	created := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1))
	ref := created.Results[0].Ref
	rows, _ := SourceSelectionRows(ref)
	closure := projectionClosure(t, h.read(t), ref)
	deleted := apply(t, w, model.ChangeSet{Ops: []model.Op{{Kind: model.OpDelete, Delete: &model.DeleteOp{Target: ref}}}}, meta(2))
	keys := []driver.LockKey{{Kind: driver.LockRecord, Ref: &ref}}
	for _, row := range rows {
		row := row
		keys = append(keys, driver.LockKey{Kind: driver.LockDerived, Derived: &row})
	}
	for _, row := range closure {
		keys = append(keys, ancestorLock(row.Key))
	}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	states, err := h.read(t).Observe(context.Background(), keys)
	if err != nil || len(states) != len(keys) {
		t.Fatal(err)
	}
	for _, state := range states {
		if state.Present || state.Record != nil || state.Derived != nil || state.Revision.String() != deleted.Revision.String() {
			t.Fatalf("missing durable deletion tombstone: %+v", state)
		}
	}
	r, err := h.store.Read(context.Background(), driver.View{Incarnation: "pipeline-test", At: &created.Revision})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	source := projectionSource(t, r, ref)
	for _, row := range projectionClosure(t, r, ref) {
		if !canRefreshAncestorProjection(row, source, r.Snapshot()) {
			t.Fatal("delete destroyed historical source/projection binding")
		}
	}
}

func TestSelectionRepresentationLimitPreservesSourcePublication(t *testing.T) {
	labels := make([]string, driver.SelectionSummaryCap+1)
	for i := range labels {
		labels[i] = fmt.Sprintf("label-%06d", i)
	}
	body, err := json.Marshal(Document{ID: "pl-large", Type: "issue", Project: "test", Fields: map[string]any{"title": "large", "status": "todo", "template": false, "priority": 0}, Labels: labels})
	if err != nil {
		t.Fatal(err)
	}
	plan, err := PrepareSourceRecord(driver.Record{Ref: model.RefByID("pl-large", model.Natural64(0)), Kind: "bead", Body: body})
	if err != nil {
		t.Fatal(err)
	}
	rows, err := plan.SelectionRows("pipeline-test", model.Natural64(0))
	if err != nil || len(rows) != 3 || rows[1].Index != nil {
		t.Fatalf("optional label representation became a source-admission cap: rows=%d error=%v", len(rows), err)
	}
	var got driver.SelectionFacts
	if err := json.Unmarshal(rows[1].Value, &got); err != nil || len(got.Rows) != len(labels) {
		t.Fatal("ordinary logical labels were truncated")
	}
	// An actual driver with its typed capability deliberately hidden still
	// receives all source and logical bytes through the ordinary write path.
	h := setup(t)
	tx, err := h.store.Write(context.Background(), h.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	keys := plan.LockKeys()
	locks := make(driver.LockSet, len(keys))
	for i, key := range keys {
		locks[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	if _, err := tx.Lock(context.Background(), locks); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(context.Background()); err != nil {
		t.Fatal(err)
	}
	ordinary := struct{ driver.WriteTx }{tx}
	if err := plan.Publish(context.Background(), ordinary); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(context.Background(), plan.Sentinels()); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(context.Background(), driver.ChangesetHeader{Incarnation: "pipeline-test", Kind: model.ChangesetWrite, Principal: by, LockKeys: keys, Changes: []model.ChangeRecord{{Ref: plan.record.Ref, Op: model.OpCreate}}}); err != nil {
		t.Fatal(err)
	}
	r := h.read(t)
	if !bytes.Equal(projectionSource(t, r, plan.record.Ref).Body, body) {
		t.Fatal("ordinary publication changed source bytes")
	}
	stored, err := r.DerivedRows(context.Background(), driver.DerivedAttribute, plan.record.Ref.ID)
	if err != nil || len(stored) != 1 || stored[0].Index != nil || !bytes.Equal(stored[0].Value, rows[1].Value) {
		t.Fatal("ordinary publication lost the complete logical label set")
	}
}

func TestBodyOnlyWritePreservesUnavailableLegacyClosure(t *testing.T) {
	h := setup(t)
	w, _ := projectionWriter(t, h)
	ref := apply(t, w, model.ChangeSet{Ops: []model.Op{create("")}}, meta(1)).Results[0].Ref
	row := projectionClosure(t, h.read(t), ref)[0]
	row.Index, row.Value = nil, []byte("legacy unsupported closure bytes")
	sentinel, err := driver.NewClosureSentinel(ref.ID)
	if err != nil {
		t.Fatal(err)
	}
	keys := []driver.LockKey{ancestorLock(row.Key), {Kind: driver.LockSentinel, Sentinel: sentinel}}
	sort.Slice(keys, func(i, j int) bool {
		a, _ := driver.EncodeLockKey(keys[i])
		b, _ := driver.EncodeLockKey(keys[j])
		return a < b
	})
	locks := make(driver.LockSet, len(keys))
	for i, key := range keys {
		locks[i] = driver.LockRequest{Key: key, Mode: driver.LockUpdate}
	}
	ctx := context.Background()
	tx, err := h.store.Write(ctx, h.lease)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Close()
	if _, err := tx.Lock(ctx, locks); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Head(ctx); err != nil {
		t.Fatal(err)
	}
	if err := tx.PutDerived(ctx, driver.DerivedAncestorClosure, nil, []driver.DerivedRow{row}); err != nil {
		t.Fatal(err)
	}
	if err := tx.BumpSentinels(ctx, []driver.Sentinel{sentinel}); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Commit(ctx, driver.ChangesetHeader{Incarnation: "pipeline-test", Kind: model.ChangesetDerived, Principal: by, LockKeys: keys}); err != nil {
		t.Fatal(err)
	}
	apply(t, w, model.ChangeSet{Ops: []model.Op{patch(ref, map[string]any{"title": "accepted ordinary update"})}}, meta(2))
	got := projectionClosure(t, h.read(t), ref)
	if len(got) != 1 || !reflect.DeepEqual(got[0], row) {
		t.Fatal("ordinary nonhierarchy write interpreted or recertified unavailable legacy closure")
	}
}

func TestSourceProjectionPreservesExactOptionalPresence(t *testing.T) {
	for _, tc := range []struct {
		name, properties, fields string
		valid, emptyProject      bool
		absent                   []driver.SelectionColumn
		template                 bool
		priority                 int64
	}{
		{"absent", "", `{}`, true, false, []driver.SelectionColumn{driver.SelectionStatus, driver.SelectionTitle, driver.SelectionTemplate, driver.SelectionPriority}, false, 0},
		{"explicit_empty_false_zero", `,"project":""`, `{"status":"","title":"","template":false,"priority":0}`, true, true, nil, false, 0},
		{"explicit_true_integral_number", `,"project":"test"`, `{"status":"todo","title":"true","template":true,"priority":2.0}`, true, false, nil, true, 2},
		{"explicit_integral_exponent", `,"project":"test"`, `{"status":"todo","title":"exact","template":false,"priority":2e0}`, true, false, nil, false, 2},
		{"null_template", "", `{"template":null}`, false, false, nil, false, 0},
		{"string_template", "", `{"template":"false"}`, false, false, nil, false, 0},
		{"null_status", "", `{"status":null}`, false, false, nil, false, 0},
		{"numeric_title", "", `{"title":0}`, false, false, nil, false, 0},
		{"fractional_priority", "", `{"priority":1.5}`, false, false, nil, false, 0},
		{"null_project", `,"project":null`, `{}`, false, false, nil, false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte(`{"id":"pl-presence","type":"issue"` + tc.properties + `,"fields":` + tc.fields + `}`)
			plan, err := PrepareSourceRecord(driver.Record{Ref: model.RefByID("pl-presence", model.Natural64(0)), Kind: "bead", Body: body})
			if err != nil {
				t.Fatal(err)
			}
			rows, err := plan.SelectionRows("presence", model.Natural64(7))
			if err != nil || len(rows) != 3 {
				t.Fatal(err)
			}
			index := rows[0].Index
			if (index != nil) != tc.valid {
				t.Fatalf("certified=%t want%t", index != nil, tc.valid)
			}
			if !tc.valid {
				return
			}
			if index.Version != driver.SelectionIndexVersion || index.Source.Revision.String() != "7" || index.Source.Digest != driver.SelectionDigest(body) || index.Bead == nil || !reflect.DeepEqual(index.Bead.Absent, tc.absent) || index.Bead.Template != tc.template || index.Bead.Priority != tc.priority {
				t.Fatalf("presence/source mismatch: %+v", index)
			}
			if tc.emptyProject && (index.Bead.Project == nil || *index.Bead.Project != "") || tc.properties == "" && index.Bead.Project != nil {
				t.Fatal("missing and explicit empty project were conflated")
			}
		})
	}
}

func TestSelectionSourcePreservesOrdinaryDecodeWithoutCertifyingAmbiguousProperties(t *testing.T) {
	for _, tc := range []struct {
		name, properties    string
		bead, labels, links bool
	}{
		{"canonical", `"id":"pl-presence","type":"issue","fields":{}`, true, true, true},
		{"case_project", `"id":"pl-presence","type":"issue","fields":{},"Project":"test"`, false, true, true},
		{"duplicate_project_null", `"id":"pl-presence","type":"issue","fields":{},"project":"test","project":null`, false, true, true},
		{"case_fields", `"id":"pl-presence","type":"issue","Fields":{"title":"present"}`, false, true, true},
		{"duplicate_fields", `"id":"pl-presence","type":"issue","fields":{"title":"first"},"fields":{"status":"todo"}`, false, true, true},
		{"case_labels", `"id":"pl-presence","type":"issue","fields":{},"Labels":["present"]`, true, false, true},
		{"duplicate_labels", `"id":"pl-presence","type":"issue","fields":{},"labels":["first"],"labels":["last"]`, true, false, true},
		{"case_links", `"id":"pl-presence","type":"issue","fields":{},"Links":[{"kind":"blocks","to":{"id":"pl-target","branch":0}}]`, true, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			body := []byte("{" + tc.properties + "}")
			ordinary, err := DecodeDocument(body)
			if err != nil {
				t.Fatal(err)
			}
			plan, err := PrepareSourceRecord(driver.Record{Ref: model.RefByID("pl-presence", model.Natural64(0)), Kind: "bead", Body: body})
			if err != nil || !reflect.DeepEqual(*plan.doc, ordinary) {
				t.Fatal("optional source decode changed accepted ordinary document:", err)
			}
			rows, err := plan.SelectionRows("presence", model.Natural64(7))
			if err != nil || len(rows) != 3 {
				t.Fatal(err)
			}
			for i, available := range []bool{tc.bead, tc.labels, tc.links} {
				if (rows[i].Index != nil) != available {
					t.Fatalf("row%d certified=%t want%t", i, rows[i].Index != nil, available)
				}
			}
		})
	}
}

func TestSourceProjectionMetadataOwnsEveryReturnedImage(t *testing.T) {
	body := []byte(`{"id":"pl-owned","type":"issue","project":"test","fields":{},"labels":["original"]}`)
	plan, err := PrepareSourceRecord(driver.Record{Ref: model.RefByID("pl-owned", model.Natural64(0)), Kind: "bead", Body: body})
	if err != nil {
		t.Fatal(err)
	}
	before, err := plan.SelectionRows("owned", model.Natural64(7))
	if err != nil {
		t.Fatal(err)
	}
	mutated, err := plan.SelectionRows("owned", model.Natural64(7))
	if err != nil {
		t.Fatal(err)
	}
	body[0] = '!'
	mutated[0].Value[0] = '!'
	*mutated[0].Index.Source.Key.Ref = model.RefByID("pl-spoofed", model.Natural64(0))
	*mutated[0].Index.Bead.Project = "spoofed"
	mutated[0].Index.Bead.Absent[0] = driver.SelectionPriority
	mutated[1].Index.Facts.Rows[0].Name = "spoofed"
	*mutated[1].Index.Facts.Rows[0].Ref = model.RefByID("pl-spoofed", model.Natural64(0))
	after, err := plan.SelectionRows("owned", model.Natural64(7))
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("caller mutation changed the final source/projection proposal:", err)
	}
}

func TestSourceProjectionLogicalBytesSurviveStoredReferenceShape(t *testing.T) {
	record := driver.Record{Ref: model.Ref{ID: "pl-wire"}, Kind: "bead", Body: []byte(`{"id":"pl-wire","type":"issue","fields":{"title":"same"},"labels":["same"]}`)}
	prepared, err := PrepareSourceRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	record.Ref = model.RefByID(record.Ref.ID, model.Natural64(0))
	stored, err := PrepareSourceRecord(record)
	if err != nil {
		t.Fatal(err)
	}
	before, err := prepared.SelectionRows("wire", model.Natural64(1))
	if err != nil {
		t.Fatal(err)
	}
	after, err := stored.SelectionRows("wire", model.Natural64(1))
	if err != nil {
		t.Fatal(err)
	}
	for i := range before {
		if !bytes.Equal(before[i].Value, after[i].Value) || before[i].Index.ValueDigest != after[i].Index.ValueDigest {
			t.Fatalf("stored ref shape changed logical row%d: %s != %s", i, before[i].Value, after[i].Value)
		}
	}
	// This is the exact original serialization difference, independent of the
	// opaque value-conservation check in storage. The producer now uses the
	// explicit stored shape on both paths; startup never rewrites these bytes.
	omitted, err := json.Marshal(driver.SelectionBead{Ref: model.Ref{ID: record.Ref.ID}, Type: "issue"})
	if err != nil {
		t.Fatal(err)
	}
	explicit, err := json.Marshal(driver.SelectionBead{Ref: record.Ref, Type: "issue"})
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(omitted, explicit) {
		t.Fatal("wire-shape control did not reproduce the zero-branch distinction")
	}
	t.Logf("logical_ref_shape omitted=%s explicit=%s", omitted, explicit)
}
