package cardb20

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestB06DurableCycleControls(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) { b06RunPlug(t, engine) })
	}
}

// These controls extend B06 without changing its pinned DFS selection rule or
// treating a retained reference to an absent target as a complete cycle.
func TestB06SchedulingAndReportBoundaries(t *testing.T) {
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := b06OpenPlug(t, engine)
			for _, id := range []string{"b06-parent", "b06-child", "b06-grandchild"} {
				f.create(id)
			}
			for _, edge := range []b06Edge{{"b06-child", "b06-parent", "parent-child"}, {"b06-grandchild", "b06-child", "parent-child"}} {
				b06RequireSuccess[model.Commit](t, f.link(edge))
			}
			// A reverse hierarchy edge schedules a cycle. Both blocker directions
			// between a transitive ancestor and descendant must also refuse.
			for _, edge := range []b06Edge{{"b06-parent", "b06-grandchild", "parent-child"}, {"b06-grandchild", "b06-parent", "blocks"}, {"b06-parent", "b06-grandchild", "blocks"}} {
				f.refuse(edge)
			}
			b06AssertReport(t, f.report(false), [][]string{})
			b06AssertReport(t, f.report(true), [][]string{})
			edges := []b06Edge{
				{"b06-wait-a", "b06-wait-b", "waits-for"}, {"b06-wait-b", "b06-wait-a", "blocks"},
				{"b06-count-z", "b06-count-a", "blocks"}, {"b06-count-a", "b06-count-z", "blocks"},
				{"b06-count-d", "b06-count-c", "conditional-blocks"}, {"b06-count-c", "b06-count-d", "blocks"},
			}
			f.seedDiagnostic("report-boundaries", []string{"b06-wait-b", "b06-wait-a", "b06-count-z", "b06-count-a", "b06-count-d", "b06-count-c"}, edges)
			expected := [][]string{{"b06-count-a", "b06-count-z"}, {"b06-count-c", "b06-count-d"}}
			check := func() {
				f.requireEdges("report-boundaries", append([]b06Edge{{"b06-child", "b06-parent", "parent-child"}, {"b06-grandchild", "b06-child", "parent-child"}}, edges...))
				for _, tracks := range []bool{false, true} {
					b06AssertReport(t, f.report(tracks), expected)
					before := f.image(fmt.Sprintf("before-limit-%d", f.requests+1))
					got := b06RequireSuccess[service.CyclesResult](t, f.call("bead.cycles", map[string]any{"includeTracks": tracks, "limit": 1}, false))
					if !reflect.DeepEqual(b06PlugPaths(got), expected[:1]) || got.CycleCount.String() != "2" || !got.Truncated || got.Items[0].Partial || got.Items[0].MemberCount.String() != "2" || !got.Items[0].UnreadableCount.IsZero() {
						t.Fatalf("limit shortened count or changed deterministic selection: %+v", got)
					}
					if !bytes.Equal(before, f.image(fmt.Sprintf("after-limit-%d", f.requests))) {
						t.Fatal("limited report changed state")
					}
				}
			}
			check()
			before := f.image("boundaries-before-reopen")
			f.reopen()
			if !bytes.Equal(before, f.image("boundaries-after-reopen")) {
				t.Fatal("boundary reopen changed state")
			}
			check()
			t.Logf("B06_BOUNDARIES engine=%s ancestor_blocker=refused hierarchy_cycle=refused waits_for=excluded count=2 limit=1 truncated=true rotation=lowest raw_order=exact", engine)
		})
	}
}

// Physical corruption is restricted to disposable SQL fixtures. Product Delete
// intentionally removes both hydration and topology and is not used here.
func TestB06PhysicalMissingTopologySQL(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			f := b06OpenPlug(t, engine)
			ids := []string{"b06-missing-c", "b06-missing-a", "b06-missing-b"}
			edges := []b06Edge{{"b06-missing-a", "b06-missing-b", "blocks"}, {"b06-missing-b", "b06-missing-c", "tracks"}, {"b06-missing-c", "b06-missing-a", "tracks"}}
			f.seedDiagnostic("physical-missing", ids, edges)
			b06AssertReport(t, f.report(true), [][]string{{"b06-missing-a", "b06-missing-b", "b06-missing-c"}})
			meta, err := f.store.Meta(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			raw := func() []driver.CycleTopologyEdge {
				r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
				if err != nil {
					t.Fatal(err)
				}
				defer r.Close()
				got := []driver.CycleTopologyEdge{}
				var cursor *driver.Cursor
				for {
					page, err := r.(driver.CycleTopologyReader).CycleTopology(context.Background(), cursor, 1)
					if err != nil {
						t.Fatal(err)
					}
					got = append(got, page.Items...)
					if page.NextCursor == nil {
						break
					}
					cursor = page.NextCursor
				}
				return got
			}
			original := raw()
			for i, id := range []string{"b06-missing-a", "b06-missing-b"} {
				f.close()
				sqlDriver, table, placeholder := "sqlite", "records", "?"
				if engine == "postgres" {
					sqlDriver, table, placeholder = "pgx", "plug_postgres.records", "$1"
				}
				db, err := sql.Open(sqlDriver, f.dsn)
				if err != nil {
					t.Fatal(err)
				}
				ref := model.RefByID(id, model.Natural64(0))
				key, err := driver.EncodeLockKey(driver.LockKey{Kind: driver.LockRecord, Ref: &ref})
				if err != nil {
					t.Fatal(err)
				}
				var arg any = key
				if engine == "postgres" {
					arg = []byte(key)
				}
				query := "DELETE FROM " + table + " WHERE key=" + placeholder
				result, err := db.ExecContext(context.Background(), query, arg)
				if err != nil {
					t.Fatal(err)
				}
				n, err := result.RowsAffected()
				if err != nil || n != 1 {
					t.Fatalf("physical delete rows=%d err=%v", n, err)
				}
				if err := db.Close(); err != nil {
					t.Fatal(err)
				}
				f.store, err = f.open()
				if err != nil {
					t.Fatal(err)
				}
				f.bind()
				after, err := f.store.Meta(context.Background())
				if err != nil || !reflect.DeepEqual(meta, after) {
					t.Fatalf("corruption changed pinned metadata: %v", err)
				}
				retained := raw()
				if !reflect.DeepEqual(original, retained) {
					t.Fatal("hydration corruption changed independent facts")
				}
				r, err := f.store.Read(context.Background(), driver.View{Incarnation: meta.Incarnation, At: &meta.Head})
				if err != nil {
					t.Fatal(err)
				}
				absent, err := r.Get(context.Background(), []model.Ref{ref})
				closeErr := r.Close()
				if err != nil || closeErr != nil || len(absent) != 0 {
					t.Fatalf("victim still hydrated: %+v %v %v", absent, err, closeErr)
				}
				response := f.call("bead.cycles", map[string]any{"includeTracks": true, "limit": 1}, false)
				got := b06RequireSuccess[service.CyclesResult](t, response)
				expected := []string{"", "b06-missing-b", "b06-missing-c"}
				if i == 1 {
					expected[1] = ""
				}
				if !reflect.DeepEqual(b06PlugPaths(got), [][]string{expected}) || got.CycleCount.String() != "1" || got.Truncated || len(got.Items) != 1 {
					t.Fatalf("lost physical topology: %+v", got)
				}
				c := got.Items[0]
				if !c.Partial || c.MemberCount.String() != "3" || c.UnreadableCount.String() != fmt.Sprint(i+1) {
					t.Fatalf("dishonest partial: %+v", c)
				}
				b06Retain(t, fmt.Sprintf("physical-missing-%d.json", i+1), map[string]any{"sql": query, "victim": ref, "setup": "deliberate disposable hydration corruption; not product Delete", "before_meta": meta, "after_meta": after, "original_facts": original, "retained_facts": retained, "absent_records": absent, "response": json.RawMessage(response.Result)})
				b06AssertReport(t, f.report(false), [][]string{})
			}
			t.Logf("B06_PHYSICAL_MISSING engine=%s missing=1_then_2 topology=unchanged positions=exact members=3 sql_reopen=true ordinary_delete=NOT_CLAIMED", engine)
		})
	}
}
