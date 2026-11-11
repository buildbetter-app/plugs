package authzrpc_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/buildbetter-app/plugs/internal/core/model"
)

// Reuse the existing fixed-permission public HTTP admission fixture. The
// inherited count units are unchanged; this new test exercises ReadyCounter
// on GuardedService, never a core-service count behind authorization.
func TestB23PublicReadyCountUsesCurrentFixedAuthority(t *testing.T) {
	countDrivers(t, func(t *testing.T, open storeFactory) {
		f := ordinaryCountFixture(t, open)
		filter := map[string]any{"resources": []string{fixtureRef(3).ID, fixtureRef(4).ID, fixtureRef(5).ID, fixtureRef(7).ID}}
		for i, limit := range []int{1, 1000} {
			for j, by := range []model.PrincipalRef{countReader(), {Kind: "user", ID: "user:empty-count", Caveats: map[string]any{"refs": []model.Ref{}}}} {
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
