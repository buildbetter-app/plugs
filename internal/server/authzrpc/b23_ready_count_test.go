package authzrpc_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/viewops"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
)

// Keep this complete Ready fixture separate from the inherited ordinary-count
// seed. Ready requires a stored creation time as well as status and priority.
func b23ReadyCountFixture(t *testing.T, open storeFactory) *fixture {
	t.Helper()
	f := newFixtureOn(t, open(t), nil)
	var changes []conformance.FixtureChange
	for _, row := range []struct {
		n             int
		title, status string
	}{
		{3, "needle-zeta", "todo"},
		{4, "needle-secret", "todo"},
		{5, "needle-done", "done"},
		{7, "Needle-case", "todo"},
	} {
		body, err := json.Marshal(map[string]any{
			"id": fixtureRef(row.n).ID, "type": "issue", "project": "project", "createdAt": "2026-11-11T00:00:00Z",
			"fields": map[string]any{"title": row.title, "status": row.status, "priority": 0},
		})
		if err != nil {
			t.Fatal(err)
		}
		changes = append(changes, conformance.FixtureChange{Ref: fixtureRef(row.n), Kind: "bead", Op: model.OpCreate, Body: body})
	}
	if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, changes); err != nil {
		t.Fatal(err)
	}
	return f
}

// Use the existing fixed-permission public HTTP admission. The inherited
// count units are unchanged; this exercises ReadyCounter on GuardedService,
// never a core-service count behind authorization.
func TestB23PublicReadyCountUsesCurrentFixedAuthority(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		f := b23ReadyCountFixture(t, open)
		filter := map[string]any{"resources": []string{fixtureRef(3).ID, fixtureRef(4).ID, fixtureRef(5).ID, fixtureRef(7).ID}}
		for i, limit := range []int{1, 1000} {
			for j, by := range []model.PrincipalRef{countReader(), {Kind: "user", ID: "user:empty-count", Caveats: map[string]any{"refs": []model.Ref{}}}} {
				meta, err := f.store.Meta(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				got := f.call(t, by, "bead.ready", map[string]any{"countOnly": true, "limit": limit, "filter": filter}, 23000+i*10+j)
				if got.Error != nil {
					t.Fatalf("guarded ready count refused: %+v", got.Error)
				}
				var body map[string]json.RawMessage
				if err := json.Unmarshal(got.Result, &body); err != nil {
					t.Fatal(err)
				}
				var count int
				want := 2
				if j == 1 {
					want = 0
				}
				if err := json.Unmarshal(body["count"], &count); err != nil || count != want || string(body["items"]) != "[]" {
					t.Fatalf("guarded count=%s want=%d %v", got.Result, want, err)
				}
				var at viewops.AsOf
				if err := json.Unmarshal(body["asOf"], &at); err != nil || at.Revision != meta.Head || at.Incarnation != meta.Incarnation {
					t.Fatalf("guarded ready snapshot=%s want=%+v: %v", got.Result, meta, err)
				}
				for _, field := range []string{"nextCursor", "groups", "elisions"} {
					if _, ok := body[field]; ok {
						t.Fatal("guarded count emitted", field)
					}
				}
				t.Logf("B23_FIXED_READY_COUNT limit=%d principal=%s response=%s", limit, by.ID, got.Result)
			}
		}
		for i, p := range []map[string]any{{"countOnly": true, "cursor": ""}, {"countOnly": true, "cursor": "bad"}, {"countOnly": true, "limit": 1001}, {"countOnly": nil}, {"countOnly": "true"}} {
			got := f.call(t, countReader(), "bead.ready", p, 23100+i)
			if got.Error == nil || got.Error.Data.Code != "invalid_params" || len(got.Result) > 0 {
				t.Fatalf("guarded %s refusal=%+v", fmt.Sprint(p), got)
			}
		}
	})
}
