package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestNumericChildQueryUsesOrderedIndex(t *testing.T) {
	ctx := context.Background()
	s := openTest(t, freshDSN(t))
	lease := testLease(t, s)
	for start := 0; start < 2500; start += 250 {
		var changes []conformance.FixtureChange
		for i := start + 1; i <= start+250; i++ {
			id := fmt.Sprintf("parent.%d", i)
			changes = append(changes, conformance.FixtureChange{Ref: testRef(id), Kind: "bead", Op: model.OpCreate, Body: []byte(fmt.Sprintf(`{"id":%q}`, id))})
		}
		if _, err := conformance.Seed(ctx, s, lease, driver.ChangesetHeader{}, changes); err != nil {
			t.Fatal(err)
		}
		if err := lease.Renew(ctx); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.db.ExecContext(ctx, "ANALYZE plug_postgres.records"); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := s.db.QueryRowContext(ctx, "EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) "+currentChildIDSQL, []byte("parent")).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var plans []struct{ Plan map[string]any }
	if err := json.Unmarshal(raw, &plans); err != nil || len(plans) != 1 {
		t.Fatalf("invalid actual plan: %s err=%v", raw, err)
	}
	found := false
	var check func(map[string]any)
	check = func(node map[string]any) {
		kind, _ := node["Node Type"].(string)
		if strings.Contains(kind, "Sort") || kind == "Seq Scan" || strings.Contains(kind, "Bitmap") {
			t.Fatalf("unbounded child executor plan: %s", raw)
		}
		if index, _ := node["Index Name"].(string); index == "records_numeric_child_live" {
			found = true
			if node["Actual Rows"] != float64(1) || node["Actual Loops"] != float64(1) {
				t.Fatalf("index did not stop at one candidate: %s", raw)
			}
			if removed, ok := node["Rows Removed by Filter"].(float64); ok && removed != 0 {
				t.Fatalf("index discarded candidates: %s", raw)
			}
		}
		if children, ok := node["Plans"].([]any); ok {
			for _, child := range children {
				check(child.(map[string]any))
			}
		}
	}
	check(plans[0].Plan)
	if !found || plans[0].Plan["Node Type"] != "Limit" {
		t.Fatalf("ordered limit/index not observed: %s", raw)
	}
	t.Logf("NUMERIC_CHILD_QUERY_PLAN engine=postgres children=2500 SQL=%s PLAN=%s", currentChildIDSQL, raw)
	if n, err := testRead(t, s, nil).(driver.NumericChildReader).MaxChildNumber(ctx, "parent", nil); err != nil || n != 2500 {
		t.Fatalf("actual ordered query max=%d err=%v", n, err)
	}
}
