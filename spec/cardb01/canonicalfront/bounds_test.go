package cardb20

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/pipeline"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// This observer delegates every call, unchanged, to one actual certified
// backend snapshot. It supplies neither rows nor More nor error outcomes.
type frontBoundTrace struct {
	reader driver.HierarchyChildrenReader
	Calls  []frontBoundCall
}

type frontBoundCall struct {
	Parents, Limit, Rows int
	More                 bool
	Error                string
}

func (r *frontBoundTrace) HierarchyChildren(ctx context.Context, parents []model.Ref, limit int) ([]driver.Record, bool, error) {
	rows, more, err := r.reader.HierarchyChildren(ctx, parents, limit)
	r.Calls = append(r.Calls, frontBoundCall{len(parents), limit, len(rows), more, fmt.Sprint(err)})
	return rows, more, err
}

type frontBoundImage struct {
	Meta                               driver.Meta
	Rows                               int
	RowsSHA256, LogSHA256, RanksSHA256 string
}

// Unlike the small public-corpus snapshot, this walks all source pages. It
// compares exact bodies/revisions and the complete journal, with no page cap
// presented as a complete durable image.
func frontBoundsImage(t *testing.T, f *publicFront) frontBoundImage {
	t.Helper()
	m, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: m.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	image := frontBoundImage{Meta: m}
	h := sha256.New()
	var cursor *driver.Cursor
	for {
		page, err := r.Query(f.ctx, driver.Query{Filter: map[string]any{"kind": "bead"}, Limit: 1000, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		for _, row := range page.Items {
			_, _ = h.Write(frontJSON(t, row))
			image.Rows++
		}
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) == 0 {
			t.Fatal("nonadvancing source page")
		}
		cursor = page.NextCursor
	}
	image.RowsSHA256 = hex.EncodeToString(h.Sum(nil))
	log, err := r.Changesets(f.ctx, nil, 1000)
	if err != nil || len(log) >= 1000 {
		t.Fatal("bound fixture journal must fit completely", err, len(log))
	}
	image.LogSHA256 = evidenceHash(frontJSON(t, log))
	ranks, err := r.DerivedRows(f.ctx, driver.DerivedSortKey, "")
	if err != nil {
		t.Fatal(err)
	}
	image.RanksSHA256 = evidenceHash(frontJSON(t, ranks))
	return image
}

func frontBoundRef(id string) model.Ref { return model.RefByID(id, model.Natural64(0)) }

func frontBoundRecord(t *testing.T, id string, parent *model.Ref) conformance.FixtureChange {
	// These are explicit storage fixtures, not ordinary Writer/native claims.
	// The graph declaration agrees with the exact persisted document.
	at := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
	doc := pipeline.Document{ID: id, Project: frontProject, Type: "issue", Parent: parent, CreatedAt: &at,
		Fields: map[string]any{"title": id, "status": "todo", "priority": 1}}
	return conformance.FixtureChange{Ref: frontBoundRef(id), Kind: "bead", Op: model.OpCreate,
		Body: frontJSON(t, doc), Graph: &conformance.FixtureGraphSource{Parent: parent}}
}

func frontSeedBounds(t *testing.T, f *publicFront, rows []conformance.FixtureChange) {
	t.Helper()
	// Bounded transactions keep SQL statement/lock work practical. The real
	// drivers publish and certify all metadata; no SQL or index bypass is used.
	for start := 0; start < len(rows); start += 500 {
		end := min(start+500, len(rows))
		if _, err := conformance.Seed(f.ctx, f.store, f.lease, driver.ChangesetHeader{Time: f.clock.Now()}, rows[start:end]); err != nil {
			t.Fatalf("real %s source publication %d..%d: %v", f.engine, start, end, err)
		}
	}
}

func frontBoundReader(t *testing.T, f *publicFront) (driver.Reader, driver.HierarchyChildrenReader) {
	t.Helper()
	m, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	r, err := f.store.Read(f.ctx, driver.View{Incarnation: m.Incarnation})
	if err != nil {
		t.Fatal(err)
	}
	indexed, ok := r.(driver.HierarchyChildrenReader)
	if !ok {
		_ = r.Close()
		t.Fatal("actual engine lacks hierarchy capability")
	}
	return r, indexed
}

func frontRequireBound(t *testing.T, f *publicFront, indexed driver.HierarchyChildrenReader, parent model.Ref, limit int, label string) []frontBoundCall {
	t.Helper()
	trace := &frontBoundTrace{reader: indexed}
	selected, err := driver.DescendantFilter(f.ctx, trace, parent)
	var bound driver.ErrDescendantFilterLimit
	code, details, mapped := driver.ErrorMapping(err)
	if selected != nil || !errors.As(err, &bound) || bound.Limit != limit || !mapped || code != "filter_too_broad" ||
		!reflect.DeepEqual(details, map[string]any{"filter": "parent", "limit": limit}) {
		t.Fatalf("%s: partial set or wrong refusal: selected=%v err=%v code=%s details=%v", label, selected, err, code, details)
	}
	frontSave(t, f.root, label+".json", frontJSON(t, map[string]any{"parent": parent, "error_type": fmt.Sprintf("%T", err), "error": err.Error(), "code": code, "details": details, "membership": selected, "partial_count": "NOT_RETURNED", "actual_index_calls": trace.Calls}))
	return trace.Calls
}

func frontPublicBound(t *testing.T, f *publicFront, parent model.Ref, limit int, label string) {
	t.Helper()
	m, err := f.store.Meta(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"bead.ready", "history.query"} {
		params := map[string]any{"filter": map[string]any{"parent": parent.ID}, "limit": 1}
		if method == "history.query" {
			params["asOf"] = map[string]any{"revision": m.Head, "incarnation": m.Incarnation}
		}
		raw := f.exchange(label+"-"+method, method, params, "filter_too_broad")
		var refusal struct {
			Data struct {
				Details struct {
					Filter string
					Limit  int
				}
			}
		}
		if json.Unmarshal(raw, &refusal) != nil || refusal.Data.Details.Filter != "parent" || refusal.Data.Details.Limit != limit {
			t.Fatalf("%s lost typed bound details: %s", method, raw)
		}
	}
}

func TestB01ParentBoundsAcrossActualEngines(t *testing.T) {
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(root) {
		t.Fatal("absolute evidence directory required")
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := newPublicFront(t, engine, filepath.Join(root, "parent-bounds", engine), time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC))
			ctx, cancel := context.WithTimeout(context.Background(), 12*time.Minute)
			defer cancel()
			f.ctx = ctx
			// Independent literal dimensions pin the existing production bounds.
			if driver.HierarchyMaxParents != 1024 || driver.HierarchyMaxLimit != 10000 || driver.SelectionOrdinaryCap != 20000 {
				t.Fatal("production bounds changed")
			}
			wide, total := frontBoundRef("pl-b01-wide"), frontBoundRef("pl-b01-total")
			rows := []conformance.FixtureChange{frontBoundRecord(t, wide.ID, nil), frontBoundRecord(t, total.ID, nil)}
			parents := make([]model.Ref, 2001)
			for i := range parents {
				parents[i] = frontBoundRef(fmt.Sprintf("pl-b01-branch-%04d", i))
				rows = append(rows, frontBoundRecord(t, parents[i].ID, &total))
			}
			for i := 0; i < 10001; i++ {
				rows = append(rows, frontBoundRecord(t, fmt.Sprintf("pl-b01-wide-%05d", i), &wide))
			}
			frontSeedBounds(t, f, rows)
			before := frontBoundsImage(t, f)
			if before.Rows != 12004 {
				t.Fatal("actual source count differs", before.Rows)
			}
			r, indexed := frontBoundReader(t, f)
			// The real per-call request guard returns no records/More. A larger
			// total frontier is legitimate and must be batched by traversal.
			badRows, more, err := indexed.HierarchyChildren(f.ctx, parents[:1025], 1)
			if badRows != nil || more || !errors.Is(err, driver.ErrUnsupported) || err.Error() != "driver capability unsupported: hierarchy parent vector exceeds 1024" {
				t.Fatalf("direct 1025-parent request: rows=%d more=%v err=%v", len(badRows), more, err)
			}
			frontSave(t, f.root, "direct-1025-parents.json", frontJSON(t, map[string]any{"parents": parents[:1025], "error": err.Error(), "errors_is_unsupported": true, "records": badRows, "more": more}))
			trace := &frontBoundTrace{reader: indexed}
			selected, err := driver.DescendantFilter(f.ctx, trace, total)
			if err != nil || len(selected) != 2001 || selected[total.ID] || len(trace.Calls) != 3 || trace.Calls[1].Parents != 1024 || trace.Calls[2].Parents != 977 {
				t.Fatalf("valid batched frontier truncated: count=%d calls=%v err=%v", len(selected), trace.Calls, err)
			}
			for _, parent := range parents {
				if !selected[parent.ID] {
					t.Fatal("batched frontier lost exact member", parent.ID)
				}
			}
			frontSave(t, f.root, "valid-2001-frontier.json", frontJSON(t, map[string]any{"membership": selected, "count": len(selected), "actual_index_calls": trace.Calls}))
			// The backend deliberately returns 10000 rows plus More. The public
			// descendant helper must refuse, never treat that prefix as complete.
			prefix, more, err := indexed.HierarchyChildren(f.ctx, []model.Ref{wide}, 10000)
			if err != nil || !more || len(prefix) != 10000 {
				t.Fatal("real 10001-child lookahead missing", len(prefix), more, err)
			}
			for i, row := range prefix {
				if row.Ref.ID != fmt.Sprintf("pl-b01-wide-%05d", i) {
					t.Fatal("lookahead prefix lost backend order", i, row.Ref)
				}
			}
			frontRequireBound(t, f, indexed, wide, 10000, "10001-children-refusal")
			if err := r.Close(); err != nil {
				t.Fatal(err)
			}
			frontPublicBound(t, f, wide, 10000, "10001-children-public")
			if after := frontBoundsImage(t, f); !reflect.DeepEqual(before, after) {
				t.Fatal("bound reads/refusals changed durable source/history")
			}
			// 2001 branches * 9 leaves + 2001 branches = 20010 descendants.
			// Each 1024-parent lookup has <=9216 rows, so only the total bound
			// can refuse. A 10001-child More cannot substitute for this control.
			rows = nil
			for _, parent := range parents {
				for j := 0; j < 9; j++ {
					rows = append(rows, frontBoundRecord(t, fmt.Sprintf("%s-leaf-%d", parent.ID, j), &parent))
				}
			}
			frontSeedBounds(t, f, rows)
			before = frontBoundsImage(t, f)
			if before.Rows != 30013 {
				t.Fatal("total-bound source count differs", before.Rows)
			}
			checkTotal := func(label string) {
				r, indexed := frontBoundReader(t, f)
				defer r.Close()
				calls := frontRequireBound(t, f, indexed, total, 20000, label)
				observed := 0
				for _, call := range calls {
					if call.Parents > 1024 || call.Rows > 10000 || call.More || call.Error != "<nil>" {
						t.Fatal("a different indexed refusal masked total bound", calls)
					}
					observed += call.Rows
				}
				if observed != 20010 {
					t.Fatal("actual total did not exceed 20000", observed)
				}
			}
			checkTotal("20010-total-refusal")
			frontPublicBound(t, f, total, 20000, "20010-total-public")
			if after := frontBoundsImage(t, f); !reflect.DeepEqual(before, after) {
				t.Fatal("total-bound refusals changed durable source/history")
			}
			if engine != "memory" {
				if err := f.lease.Release(f.ctx); err != nil {
					t.Fatal(err)
				}
				f.lease = nil
				if err := f.store.Close(); err != nil {
					t.Fatal(err)
				}
				f.open()
				if after := frontBoundsImage(t, f); !reflect.DeepEqual(before, after) {
					t.Fatal("actual SQL reopen changed bound fixture")
				}
				checkTotal("20010-total-refusal-after-SQL-reopen")
			}
			frontSave(t, f.root, "bound-source-image.json", frontJSON(t, map[string]any{"engine": engine, "complete_image": before, "fixture_path": "real typed driver publication via conformance.Seed; ordinary Writer/native NOT_CLAIMED", "service_layer": "public ready/history.query reach typed parent bound before page result", "compiled_SQL_executor": "NOT_QUALIFIED_SEPARATE_020", "persistent_reopen": engine != "memory"}))
		})
	}
}
