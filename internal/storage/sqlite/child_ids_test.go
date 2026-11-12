package sqlite

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

func TestNumericChildQueryUsesOrderedIndex(t *testing.T) {
	ctx := testContext(t)
	s := openStore(t, ctx, filepath.Join(t.TempDir(), "child-plan.db"))
	lease := getLease(t, ctx, s, driver.LeaseTTL)
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
	rows, err := s.db.QueryContext(ctx, "EXPLAIN QUERY PLAN "+currentChildIDSQL, "parent")
	if err != nil {
		t.Fatal(err)
	}
	var plan []string
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		plan = append(plan, detail)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	rows.Close()
	joined := strings.Join(plan, "\n")
	if !strings.Contains(joined, "SEARCH r USING INDEX records_numeric_child") || strings.Contains(joined, "SCAN r") || strings.Contains(joined, "TEMP B-TREE") {
		t.Fatalf("unbounded child query plan: %s", joined)
	}
	t.Logf("NUMERIC_CHILD_QUERY_PLAN engine=sqlite children=2500 SQL=%s PLAN=%s", currentChildIDSQL, joined)
	r := getReader(t, ctx, s, getMeta(t, ctx, s), nil)
	if n, err := r.(driver.NumericChildReader).MaxChildNumber(ctx, "parent", nil); err != nil || n != 2500 {
		t.Fatalf("actual ordered query max=%d err=%v", n, err)
	}
}
