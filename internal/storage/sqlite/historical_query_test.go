package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Preserve the pre-optimization historical Query algorithm independently of
// the selector: enumerate every object, replay at the pinned revision, filter,
// and only then slice. recordAt is deliberately unchanged production code.
func historicalQueryReference(t *testing.T, ctx context.Context, r *reader, filter map[string]any) []driver.Record {
	t.Helper()
	out, err := historicalQueryReferenceResult(ctx, r, filter)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func historicalQueryReferenceResult(ctx context.Context, r *reader, filter map[string]any) ([]driver.Record, error) {
	rows, err := r.tx.QueryContext(ctx, `SELECT key FROM objects ORDER BY key`)
	if err != nil {
		return nil, err
	}
	var keys []string
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	out := make([]driver.Record, 0)
	for _, key := range keys {
		record, found, err := r.recordAt(ctx, key)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		ok, err := matches(record, filter)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, record)
		}
	}
	return out, nil
}

func assertHistoricalWalk(t *testing.T, ctx context.Context, r *reader, filter map[string]any, limit int) {
	t.Helper()
	want := historicalQueryReference(t, ctx, r, filter)
	got := make([]driver.Record, 0)
	var cursor *driver.Cursor
	for pageNumber := 0; ; pageNumber++ {
		if pageNumber > len(want)+1 {
			t.Fatal("historical cursor did not terminate")
		}
		var work queryReadWork
		page, err := r.Query(driver.WithReadWorkObserver(ctx, &work), driver.Query{Filter: filter, Limit: limit, Cursor: cursor})
		if err != nil {
			t.Fatal(err)
		}
		if page.AsOf != model.Natural64(r.at) || len(page.Items) > limit {
			t.Fatalf("invalid historical page: %+v", page)
		}
		if work.payloads.Load() < int64(len(page.Items)) || work.payloads.Load() > int64(4*(limit+1)) || work.queries.Load() != 1 || work.returned.Load() != int64(len(page.Items)) {
			t.Fatalf("unbounded historical payload work: page=%d payloads=%d returned=%d limit=%d", pageNumber, work.payloads.Load(), len(page.Items), limit)
		}
		got = append(got, page.Items...)
		if page.NextCursor == nil {
			break
		}
		if len(page.Items) != limit || page.NextCursor.After == "" {
			t.Fatal("short historical page or missing keyset marker")
		}
		cursor = page.NextCursor
	}
	// DeepEqual includes raw Body bytes, Ref, Kind and visible Revision. It
	// intentionally does not normalize JSON or compare only current records.
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("historical raw-record differential mismatch at revision %d filter=%v:\ngot=%+v\nwant=%+v", r.at, filter, got, want)
	}
}

func TestHistoricalQueryDifferentialRawBodiesAcrossRevisions(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "historical-differential.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	renewQueryLease(t, lease)
	changes := []conformance.FixtureChange{}
	for i := 0; i < 12; i++ {
		kind := "test"
		if i%2 != 0 {
			kind = "other"
		}
		ref := testRef(fmt.Sprintf("history-%02d", i))
		if i == 11 {
			ref = model.RefByKey("history-key")
		}
		changes = append(changes, conformance.FixtureChange{Ref: ref, Kind: kind, Op: model.OpCreate,
			Body: []byte(fmt.Sprintf("{ \"status\":\"open\", \"type\":\"issue\", \"project\":\"p%d\", \"number\":9007199254740993 }", i%3))})
	}
	seed := func(changes ...conformance.FixtureChange) model.Natural {
		t.Helper()
		entry, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, changes)
		if err != nil {
			t.Fatal(err)
		}
		return entry.Revision
	}
	first := seed(changes...)
	checkpoints := []model.Natural{model.Natural64(0), first}
	for i := 0; i < 34; i++ {
		at := seed(conformance.FixtureChange{Ref: testRef("history-00"), Kind: "test", Op: model.OpPatch,
			Body: []byte(fmt.Sprintf("{ \"project\": \"p0\", \"type\":\"issue\", \"status\":\"updated-%d\", \"n\":%d }", i, i)), ChangedPaths: []string{"/status", "/n"}})
		if i == 0 || i == 30 || i == 31 || i == 32 || i == 33 {
			checkpoints = append(checkpoints, at)
		}
	}
	deleted := seed(conformance.FixtureChange{Ref: testRef("history-00"), Kind: "test", Op: model.OpDelete})
	checkpoints = append(checkpoints, deleted)
	recreated := seed(conformance.FixtureChange{Ref: testRef("history-00"), Kind: "test", Op: model.OpCreate, Body: []byte(`{ "status":"recreated", "type":null, "project":"p0" }`)})
	checkpoints = append(checkpoints, recreated)
	checkpoints = append(checkpoints, seed(conformance.FixtureChange{Ref: testRef("history-01"), Kind: "other", Op: model.OpPatch, Body: []byte(`{"type":17,"project":"p1","status":null}`), ChangedPaths: []string{"/type", "/status"}}))
	seed(conformance.FixtureChange{Ref: testRef("history-later"), Kind: "test", Op: model.OpCreate, Body: []byte(`{"status":"later"}`)})
	meta := getMeta(t, ctx, s)
	filters := []map[string]any{nil, {}, {"kind": "test"}, {"kind": "other"}, {"kind": "missing"}, {"id": "history-00"}, {"id": "absent"}, {"key": "history-key"}, {"key": "absent"}, {"type": "issue"}, {"type": ""}, {"project": "p1"}, {"project": "absent"}, {"status": "open"}, {"status": "recreated"}, {"status": ""}, {"kind": "test", "project": "p0", "type": "issue", "status": "open"}, {"id": "history-00", "status": "updated-0"}}
	for _, at := range checkpoints {
		t.Run("revision_"+at.String(), func(t *testing.T) {
			value, err := s.Read(ctx, driver.View{Incarnation: meta.Incarnation, At: &at})
			if err != nil {
				t.Fatal(err)
			}
			r := value.(*reader)
			defer r.Close()
			if r.at >= mustUint(r.meta.Head) {
				t.Fatal("oracle was not opened through a genuine later-head reader")
			}
			for i, filter := range filters {
				t.Run(fmt.Sprintf("filter_%d", i), func(t *testing.T) { assertHistoricalWalk(t, ctx, r, filter, 2) })
			}
		})
	}
}

func TestHistoricalQueryCursorBindingsReopenAndExactEnd(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "historical-cursors.db")
	s := openStore(t, ctx, path)
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	for _, id := range []string{"cursor-a", "cursor-b", "cursor-c"} {
		seedRecord(t, ctx, s, lease, id, `{"status":"open"}`)
	}
	at := getMeta(t, ctx, s).Head
	seedRecord(t, ctx, s, lease, "cursor-later", `{}`)
	meta := getMeta(t, ctx, s)
	r := getReader(t, ctx, s, meta, &at).(*reader)
	first, err := r.Query(ctx, driver.Query{Limit: 1})
	if err != nil || len(first.Items) != 1 || first.NextCursor == nil {
		t.Fatalf("first historical page: %+v %v", first, err)
	}
	for _, mutate := range []func(*driver.Cursor){
		func(c *driver.Cursor) { c.Incarnation = "other" }, func(c *driver.Cursor) { c.Revision = meta.Head },
		func(c *driver.Cursor) { c.FilterDigest = "other" }, func(c *driver.Cursor) { c.Method = "get" },
		func(c *driver.Cursor) { c.Index = model.Natural64(999) }, func(c *driver.Cursor) { c.After += "corrupt" }, func(c *driver.Cursor) { c.PrincipalDigest = "other" },
	} {
		bad := *first.NextCursor
		mutate(&bad)
		if page, err := r.Query(ctx, driver.Query{Limit: 1, Cursor: &bad}); err == nil || len(page.Items) != 0 || page.NextCursor != nil {
			t.Fatalf("accepted bad historical cursor: %+v %v", page, err)
		}
	}
	if _, err := r.Query(ctx, driver.Query{Limit: 1, Filter: map[string]any{"kind": "test"}, Cursor: first.NextCursor}); err == nil {
		t.Fatal("accepted mismatched historical filter")
	}
	if _, err := r.Query(ctx, driver.Query{Limit: 1, AsOf: &meta.Head}); err == nil {
		t.Fatal("accepted mismatched historical as-of")
	}
	end := *first.NextCursor
	lastKey, _ := recordKey(testRef("cursor-c"))
	end.Index = model.Natural64(3)
	end.After = r.historicalQueryAfter(lastKey, end.FilterDigest, 3)
	if page, err := r.Query(ctx, driver.Query{Limit: 1, Cursor: &end}); err != nil || len(page.Items) != 0 || page.NextCursor != nil || page.AsOf != at {
		t.Fatalf("historical exact end: %+v %v", page, err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openStore(t, ctx, path)
	r2 := getReader(t, ctx, reopened, getMeta(t, ctx, reopened), &at)
	page, err := r2.Query(ctx, driver.Query{Limit: 1, Cursor: first.NextCursor})
	if err != nil || len(page.Items) != 1 || page.Items[0].Ref.ID != "cursor-b" {
		t.Fatalf("reopened historical cursor: %+v %v", page, err)
	}
}

func TestHistoricalQueryPageWorkAcrossCorpusSizesAndSparseFilters(t *testing.T) {
	for _, size := range []int{1000, 5000, 25000} {
		t.Run(fmt.Sprintf("records_%d", size), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			path := filepath.Join(t.TempDir(), "historical-work.db")
			s := openStore(t, ctx, path)
			tx, err := s.db.BeginTx(ctx, nil)
			if err != nil {
				t.Fatal(err)
			}
			for i := 0; i < size; i++ {
				ref := testRef(fmt.Sprintf("work-%06d", i))
				key, _ := recordKey(ref)
				project := "ordinary"
				if i%97 == 0 {
					project = "needle"
				}
				kind := "test"
				if i%2 != 0 {
					kind = "other"
				}
				body := []byte(fmt.Sprintf(`{"type":"issue","status":"open","project":"%s"}`, project))
				raw, err := json.Marshal(driver.Version{Ref: ref, Revision: model.Natural64(1), Base: true, Body: body, Principal: model.PrincipalRef{Kind: "system", ID: "historical-work-fixture"}})
				if err != nil {
					t.Fatal(err)
				}
				fields, err := json.Marshal(map[string]any{"deleted": false, "id": ref.ID, "key": ref.Key, "type": "issue", "status": "open", "project": project})
				if err != nil {
					t.Fatal(err)
				}
				if _, err = tx.ExecContext(ctx, `INSERT INTO objects(key,kind) VALUES(?,?)`, key, kind); err != nil {
					t.Fatal(err)
				}
				if _, err = tx.ExecContext(ctx, `INSERT INTO versions(key,rev,base,data,query_fields) VALUES(?,?,1,?,?)`, key, revKey(1), raw, string(fields)); err != nil {
					t.Fatal(err)
				}
			}
			if _, err = tx.ExecContext(ctx, `UPDATE meta SET head=?`, revKey(2)); err != nil {
				t.Fatal(err)
			}
			if err = tx.Commit(); err != nil {
				t.Fatal(err)
			}
			probe := &queryProbe{}
			db := sql.OpenDB(queryProbeConnector{base: s.db.Driver(), dsn: "file:" + url.PathEscape(path) + "?_pragma=busy_timeout%285000%29", probe: probe})
			observed := &Store{db: db, lockDir: s.lockDir, children: make(map[io.Closer]struct{}), leases: make(map[*activeLease]struct{})}
			defer observed.Close()
			at := model.Natural64(1)
			meta := getMeta(t, ctx, s)
			for _, tc := range []struct {
				name   string
				filter map[string]any
				count  int
			}{
				{"nil", nil, size}, {"kind", map[string]any{"kind": "test"}, (size + 1) / 2},
				{"sparse", map[string]any{"project": "needle"}, (size + 96) / 97}, {"empty", map[string]any{"status": "missing"}, 0},
			} {
				t.Run(tc.name, func(t *testing.T) {
					r := getReader(t, ctx, observed, meta, &at).(*reader)
					var cursor *driver.Cursor
					seen := 0
					previous := ""
					const limit = 127
					for n := 0; ; n++ {
						probe.reset()
						var work queryReadWork
						page, err := r.Query(driver.WithReadWorkObserver(ctx, &work), driver.Query{Limit: limit, Cursor: cursor, Filter: tc.filter})
						if err != nil {
							t.Fatal(err)
						}
						payloads := work.payloads.Load()
						if payloads < int64(len(page.Items)) || payloads > int64(4*(limit+1)) || work.payloadBytes.Load() > payloads*128 {
							t.Fatalf("unbounded historical work: payloads=%d bytes=%d", payloads, work.payloadBytes.Load())
						}
						if probe.calls.Load() > 6 || probe.rows.Load() > int64(limit+5) {
							t.Fatalf("unbounded historical SQL batch: calls=%d rows=%d", probe.calls.Load(), probe.rows.Load())
						}
						for _, statement := range probe.statementSnapshot() {
							upper := strings.ToUpper(statement)
							for _, bad := range []string{"OFFSET", "COUNT(", "SUM(", "AVG(", "MAX(", "MIN("} {
								if strings.Contains(upper, bad) {
									t.Fatalf("historical page used %s: %s", bad, statement)
								}
							}
						}
						t.Logf("HISTORICAL_QUERY_WORK corpus=%d filter=%s page=%d calls=%d sql_rows=%d payloads=%d bytes=%d returned=%d", size, tc.name, n, probe.calls.Load(), probe.rows.Load(), payloads, work.payloadBytes.Load(), len(page.Items))
						for _, record := range page.Items {
							if len(record.Body) == 0 {
								t.Fatal("missing historical body")
							}
							key, err := recordKey(record.Ref)
							if err != nil || key <= previous {
								t.Fatalf("unordered or duplicate historical key: %q after %q", key, previous)
							}
							previous = key
							if ok, err := matches(record, tc.filter); err != nil || !ok {
								t.Fatalf("historical predicate changed: %+v %v", record, err)
							}
						}
						seen += len(page.Items)
						if page.NextCursor == nil {
							break
						}
						cursor = page.NextCursor
					}
					if seen != tc.count {
						t.Fatalf("historical coverage=%d want=%d", seen, tc.count)
					}
				})
			}
		})
	}
}

func TestHistoricalQueryPopulatedSchemaThreeBackfillPreservesVersionBytes(t *testing.T) {
	ctx := testContext(t)
	path := filepath.Join(t.TempDir(), "historical-migration.db")
	s := openStore(t, ctx, path)
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	seedRecord(t, ctx, s, lease, "migration-a", `{ "project":"old", "status":"open" }`)
	at := getMeta(t, ctx, s).Head
	if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{{Ref: testRef("migration-a"), Kind: "test", Op: model.OpPatch, Body: []byte(`{"project":"new","status":"closed"}`), ChangedPaths: []string{"/project", "/status"}}}); err != nil {
		t.Fatal(err)
	}
	meta := getMeta(t, ctx, s)
	r := getReader(t, ctx, s, meta, &at).(*reader)
	want := historicalQueryReference(t, ctx, r, map[string]any{"project": "old"})
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	versions := func(db *sql.DB) [][]byte {
		t.Helper()
		rows, err := db.QueryContext(ctx, `SELECT data FROM versions ORDER BY key,rev`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var values [][]byte
		for rows.Next() {
			var raw []byte
			if err := rows.Scan(&raw); err != nil {
				t.Fatal(err)
			}
			values = append(values, raw)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		return values
	}
	originalVersions := versions(s.db)
	for _, name := range []string{"pending", "id", "key", "type", "project", "status", "body_error"} {
		if _, err := s.db.ExecContext(ctx, `DROP INDEX versions_query_`+name); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.ExecContext(ctx, `ALTER TABLE versions DROP COLUMN query_fields`); err != nil {
		t.Fatal(err)
	}
	if err := lease.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	reopened := openStore(t, ctx, path)
	if got := getMeta(t, ctx, reopened); !reflect.DeepEqual(got, meta) {
		t.Fatalf("migration changed metadata: %+v want %+v", got, meta)
	}
	if got := versions(reopened.db); !reflect.DeepEqual(got, originalVersions) {
		t.Fatal("backfill rewrote immutable version bytes")
	}
	var missing int
	if err := reopened.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM versions WHERE query_fields IS NULL`).Scan(&missing); err != nil || missing != 0 {
		t.Fatalf("incomplete backfill: %d %v", missing, err)
	}
	r2 := getReader(t, ctx, reopened, meta, &at)
	var work queryReadWork
	page, err := r2.Query(driver.WithReadWorkObserver(ctx, &work), driver.Query{Limit: 1, Filter: map[string]any{"project": "old"}})
	if err != nil || !reflect.DeepEqual(page.Items, want) || page.NextCursor != nil || work.payloads.Load() != 1 {
		t.Fatalf("backfilled historical page: %+v work=%d %v", page, work.payloads.Load(), err)
	}
}

func TestHistoricalQueryContinuesCurrentCursorAtPinnedRevision(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "head-to-history.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	for _, id := range []string{"transition-a", "transition-b", "transition-c"} {
		seedRecord(t, ctx, s, lease, id, `{ "project":"old", "status":"open" }`)
	}
	meta := getMeta(t, ctx, s)
	at := meta.Head
	filters := []map[string]any{nil, {"kind": "test"}, {"project": "old"}, {"kind": "test", "status": "open"}}
	cursors := make([]*driver.Cursor, len(filters))
	r := getReader(t, ctx, s, meta, nil)
	for i, filter := range filters {
		page, err := r.Query(ctx, driver.Query{Limit: 1, Filter: filter})
		if err != nil || len(page.Items) != 1 || page.NextCursor == nil {
			t.Fatalf("issued head cursor: %+v %v", page, err)
		}
		cursors[i] = page.NextCursor
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	// Shift live ranks and remove the old anchor at the new head. Neither may
	// affect the cursor's original revision or its still-visible historical body.
	if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{
		{Ref: testRef("transition-0"), Kind: "test", Op: model.OpCreate, Body: []byte(`{"project":"new"}`)},
		{Ref: testRef("transition-a"), Kind: "test", Op: model.OpDelete},
	}); err != nil {
		t.Fatal(err)
	}
	historical := getReader(t, ctx, s, getMeta(t, ctx, s), &at)
	for i, filter := range filters {
		var work queryReadWork
		page, err := historical.Query(driver.WithReadWorkObserver(ctx, &work), driver.Query{Limit: 1, Filter: filter, Cursor: cursors[i]})
		if err != nil || len(page.Items) != 1 || page.Items[0].Ref.ID != "transition-b" || page.AsOf != at || page.NextCursor == nil || work.payloads.Load() > 8 {
			t.Fatalf("head-to-history continuation filter=%v page=%+v work=%d err=%v", filter, page, work.payloads.Load(), err)
		}
	}
}

func TestHistoricalQueryPreservesBodyErrorsBeyondLookahead(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "historical-error.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	for _, row := range []struct{ id, body string }{{"error-a", `{"status":"open"}`}, {"error-b", `{"status":"open"}`}, {"error-z", `[1]`}} {
		seedRecord(t, ctx, s, lease, row.id, row.body)
	}
	at := getMeta(t, ctx, s).Head
	seedRecord(t, ctx, s, lease, "later", `{}`)
	r := getReader(t, ctx, s, getMeta(t, ctx, s), &at).(*reader)
	invalid, found, err := r.recordAt(ctx, func() string { key, _ := recordKey(testRef("error-z")); return key }())
	if err != nil || !found {
		t.Fatal(err)
	}
	_, wantErr := matches(invalid, map[string]any{"status": "open"})
	if wantErr == nil {
		t.Fatal("reference invalid body did not error")
	}
	for _, filter := range []map[string]any{{"status": "open"}, {"id": "error-z", "status": "open"}, {"kind": "test", "status": "open"}} {
		var work queryReadWork
		page, err := r.Query(driver.WithReadWorkObserver(ctx, &work), driver.Query{Limit: 1, Filter: filter})
		if err == nil || reflect.TypeOf(err) != reflect.TypeOf(wantErr) || err.Error() != wantErr.Error() || len(page.Items) != 0 || page.NextCursor != nil || work.payloads.Load() > 8 {
			t.Fatalf("historical error changed beyond lookahead: filter=%v page=%+v work=%d got=%v want=%v", filter, page, work.payloads.Load(), err, wantErr)
		}
	}
}

func TestHistoricalQueryMixedFiltersExcludeInvalidBodies(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "historical-mixed-errors.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	_, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{
		{Ref: testRef("mixed-match"), Kind: "wanted", Op: model.OpCreate, Body: []byte(`{ "status":"open", "type":"issue", "project":"p" }`)},
		{Ref: model.RefByKey("mixed-match-key"), Kind: "wanted", Op: model.OpCreate, Body: []byte(`{"status":"open","type":"issue","project":"p"}`)},
		{Ref: testRef("mixed-array"), Kind: "other", Op: model.OpCreate, Body: []byte(`[1]`)},
		{Ref: model.RefByKey("mixed-scalar"), Kind: "other", Op: model.OpCreate, Body: []byte(`17`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	at := getMeta(t, ctx, s).Head
	seedRecord(t, ctx, s, lease, "later", `{}`)
	r := getReader(t, ctx, s, getMeta(t, ctx, s), &at).(*reader)
	filters := []map[string]any{
		{"id": "mixed-match", "status": "open"},
		{"key": "mixed-match-key", "project": "p"},
		{"kind": "wanted", "type": "issue", "status": "open"},
		{"kind": "wanted", "id": "mixed-match", "project": "p", "status": "open"},
		{"id": "absent", "status": "open"},
		{"key": "absent", "type": "issue"},
		{"kind": "absent", "project": "p"},
	}
	for i, filter := range filters {
		t.Run(fmt.Sprintf("filter_%d", i), func(t *testing.T) {
			identity := map[string]any{}
			for _, field := range []string{"kind", "id", "key"} {
				if value, ok := filter[field]; ok {
					identity[field] = value
				}
			}
			// The predecessor iterates filter maps in unspecified order: an
			// excluded array can be ignored or cause a decoding error, but can
			// never cause a projection-mismatch error. Choose its identity-first
			// outcome without making the differential depend on map iteration.
			want := make([]driver.Record, 0)
			for _, record := range historicalQueryReference(t, ctx, r, identity) {
				ok, err := matches(record, filter)
				if err != nil {
					t.Fatal(err)
				}
				if ok {
					want = append(want, record)
				}
			}
			got := make([]driver.Record, 0)
			var cursor *driver.Cursor
			for pageNumber := 0; ; pageNumber++ {
				if pageNumber > len(want)+1 {
					t.Fatal("mixed-filter cursor did not terminate")
				}
				var work queryReadWork
				page, err := r.Query(driver.WithReadWorkObserver(ctx, &work), driver.Query{Limit: 1, Filter: filter, Cursor: cursor})
				if err != nil || work.payloads.Load() < int64(len(page.Items)) || work.payloads.Load() > 8 {
					t.Fatalf("excluded invalid body affected page: filter=%v page=%+v work=%d err=%v", filter, page, work.payloads.Load(), err)
				}
				got = append(got, page.Items...)
				if page.NextCursor == nil {
					break
				}
				cursor = page.NextCursor
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("mixed-filter raw-record differential: got=%+v want=%+v", got, want)
			}
		})
	}
}

func TestHistoricalQueryBodyErrorDifferentialAtHeadAndPastRevision(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "historical-error-differential.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
	_, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, []conformance.FixtureChange{
		{Ref: testRef("error-match"), Kind: "wanted", Op: model.OpCreate, Body: []byte(`{ "status":"open", "type":"issue", "project":"p" }`)},
		{Ref: model.RefByKey("error-match-key"), Kind: "wanted", Op: model.OpCreate, Body: []byte(`{"status":"open","type":"issue","project":"p"}`)},
		{Ref: testRef("error-array"), Kind: "other", Op: model.OpCreate, Body: []byte(`[1]`)},
		{Ref: model.RefByKey("error-scalar"), Kind: "other", Op: model.OpCreate, Body: []byte(`17`)},
	})
	if err != nil {
		t.Fatal(err)
	}
	at := getMeta(t, ctx, s).Head
	check := func(t *testing.T, r *reader) {
		t.Helper()
		for i, filter := range []map[string]any{
			nil, {"kind": "wanted"}, {"id": "error-match"},
			{"kind": "wanted", "status": "open"},
			{"id": "error-match", "status": "open"},
			{"key": "error-match-key", "project": "p"},
			{"status": "open"}, {"id": "error-array", "status": "open"},
		} {
			t.Run(fmt.Sprintf("filter_%d", i), func(t *testing.T) {
				// Invoke the exact preserved enumerate/replay/matches algorithm.
				// Its mixed-filter map order permits either exclusion or a typed
				// decoding error; validate both permitted outcomes explicitly.
				old, oldErr := historicalQueryReferenceResult(ctx, r, filter)
				identity := map[string]any{}
				for _, field := range []string{"kind", "id", "key"} {
					if value, ok := filter[field]; ok {
						identity[field] = value
					}
				}
				mixedBodyFilter := len(identity) > 0 && len(filter) > len(identity)
				want := make([]driver.Record, 0)
				var wantErr error
				for _, record := range historicalQueryReference(t, ctx, r, identity) {
					ok, err := matches(record, filter)
					if err != nil {
						wantErr = err
						break
					}
					if ok {
						want = append(want, record)
					}
				}
				if oldErr == nil {
					if wantErr != nil || !reflect.DeepEqual(old, want) {
						t.Fatalf("old path differs from identity-first outcome: got=%+v want=%+v err=%v", old, want, wantErr)
					}
				} else {
					var decodeErr *json.UnmarshalTypeError
					if (wantErr == nil && !mixedBodyFilter) || !errors.As(oldErr, &decodeErr) {
						t.Fatalf("unexpected old-path error: %v", oldErr)
					}
				}
				page, err := r.Query(ctx, driver.Query{Limit: 8, Filter: filter})
				if wantErr != nil {
					if err == nil || reflect.TypeOf(err) != reflect.TypeOf(wantErr) || err.Error() != wantErr.Error() || len(page.Items) != 0 || page.NextCursor != nil {
						t.Fatalf("decoding error differential: page=%+v got=%v want=%v", page, err, wantErr)
					}
				} else if err != nil {
					// At head the unchanged matcher can visit a body predicate
					// first, including on an identity-excluded array or scalar.
					var decodeErr *json.UnmarshalTypeError
					if r.at != mustUint(r.meta.Head) || !mixedBodyFilter || !errors.As(err, &decodeErr) {
						t.Fatalf("excluded body produced a new error: %v", err)
					}
				} else if page.AsOf != at || page.NextCursor != nil || !reflect.DeepEqual(page.Items, want) {
					t.Fatalf("raw-record differential: page=%+v want=%+v", page, want)
				}
			})
		}
	}
	t.Run("head", func(t *testing.T) {
		r := getReader(t, ctx, s, getMeta(t, ctx, s), &at).(*reader)
		defer r.Close()
		if r.at != mustUint(r.meta.Head) {
			t.Fatal("head control was not at head")
		}
		check(t, r)
	})
	seedRecord(t, ctx, s, lease, "later", `{}`)
	t.Run("past", func(t *testing.T) {
		r := getReader(t, ctx, s, getMeta(t, ctx, s), &at).(*reader)
		defer r.Close()
		if r.at >= mustUint(r.meta.Head) {
			t.Fatal("past control was not below head")
		}
		check(t, r)
	})
}
