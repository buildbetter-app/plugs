package cardb20

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestB01CanonicalPublicReadyCounterparts(t *testing.T) {
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(root) {
		t.Fatal("absolute evidence directory required")
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		for _, c := range frontCases() {
			t.Run(engine+"/"+c.Name, func(t *testing.T) {
				runPublicFront(t, engine, filepath.Join(root, engine, c.Name), c, time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC))
			})
		}
	}
}

// Fresh ranks come from actual ordinary Writer updates, not direct derived-row
// insertion or a claim that the asynchronous maintenance worker ran.
func TestB01CanonicalPublicWriterStoredOrderLag(t *testing.T) {
	root := os.Getenv("TEST_UNDECLARED_OUTPUTS_DIR")
	if !filepath.IsAbs(root) {
		t.Fatal("absolute evidence directory required")
	}
	for _, engine := range []string{"memory", "sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			anchor := time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
			f := newPublicFront(t, engine, filepath.Join(root, engine, "writer-produced-stored-lag"), anchor.Add(-100*time.Hour))
			f.populate(frontCase{Name: "priority_created_id", Nodes: []frontNode{
				{"aging", "open", "task", 0, 48 * time.Hour, false},
				{"recent", "open", "task", 4, time.Hour, false},
			}}, anchor)
			f.call("ordinary-writer-rank-refresh", "bead.update", map[string]any{"id": f.ref("aging"), "fields": map[string]any{"title": "aging persisted rank refresh"}})
			before := f.snapshot()
			rowKey := "ready/" + f.ids["aging"]
			var rank driver.DerivedRow
			for _, row := range before.Ranks {
				if row.Key == rowKey {
					rank = row
				}
			}
			var key struct {
				Bucket, Rank        int
				CreatedAt, RankedAt time.Time
				Revision            model.Revision
			}
			if len(rank.Value) == 0 || json.Unmarshal(rank.Value, &key) != nil || key.Bucket != 0 || key.Rank != 0 || !key.CreatedAt.Equal(anchor.Add(-48*time.Hour)) || !key.RankedAt.Equal(anchor) {
				t.Fatalf("actual Writer did not produce declared stored rank: %s", rank.Value)
			}
			vectors := map[string][]string{}
			for _, step := range []struct {
				Name  string
				After time.Duration
				Want  []string
			}{
				{"at-boundary", 0, []string{"aging", "recent"}},
				{"one-second", time.Second, []string{"aging", "recent"}},
				{"sixty-seconds", time.Minute, []string{"aging", "recent"}},
				{"expired-sixty-seconds-plus-microsecond", time.Minute + time.Microsecond, []string{"recent", "aging"}},
			} {
				f.at(anchor.Add(step.After))
				vectors[step.Name] = f.pages(step.Name, step.Want)
				f.reopen(step.Name)
				f.pages(step.Name+"-reopened", step.Want)
			}
			after := f.snapshot()
			if !reflect.DeepEqual(before, after) {
				t.Fatal("rank expiry fallback rewrote durable state")
			}
			frontSave(t, f.root, "p74-stored-order-vector.json", frontJSON(t, map[string]any{"writer_produced_row": rank, "decoded_stored_key": key, "clock_anchor": anchor, "actual_vectors": vectors, "allowed_lag": "<=60s; fallback after60s", "membership": "exact at every step", "native_default_order": "NOT_COMPARED_TO_CONTROLLED_PLUG_CLOCK", "maintenance_worker": "NOT_CLAIMED"}))
		})
	}
}
