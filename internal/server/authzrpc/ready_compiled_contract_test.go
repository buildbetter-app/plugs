package authzrpc_test

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/buildbetter-app/plugs/internal/core/model"
	"github.com/buildbetter-app/plugs/internal/core/service"
	"github.com/buildbetter-app/plugs/internal/storage/conformance"
	"github.com/buildbetter-app/plugs/internal/storage/driver"
	"github.com/buildbetter-app/plugs/internal/storage/sqlite"
)

const readyCompiledContractSize = 20001

// This is the settled P50/P84 product contract, using only the existing public
// composition and Ready API. At frozen967 the universal unfiltered cap is an
// expected initial RED. Recording that refusal as a successful test would hide
// the behavior this regression is intended to establish.
func TestGuardedReadyCompiledContractAbovePostfilterCap(t *testing.T) {
	for _, engine := range []string{"sqlite", "postgres"} {
		t.Run(engine, func(t *testing.T) {
			var store driver.Store
			if engine == "postgres" {
				store = postgresFactory(t)(t)
			} else {
				var err error
				store, err = (sqlite.Driver{}).Open(context.Background(), driver.Config{DSN: filepath.Join(t.TempDir(), "ready-compiled-contract.db")})
				if err != nil {
					t.Fatal(err)
				}
			}
			f := newFixtureOn(t, store, nil)
			renewReadyScanLease(t, f.lease)
			seedReadyCompiledContract(t, f)

			restricted := map[int]bool{2: true, 1000: true, 19999: true, 20001: true}
			for _, tc := range []struct {
				name   string
				by     model.PrincipalRef
				allow  map[int]bool // nil means all fixture identities; empty means none.
				hidden bool
			}{
				{name: "user_all_read", by: model.PrincipalRef{Kind: "user", ID: "user:reader"}},
				{
					name: "restricted_tail_hidden_parent",
					by: model.PrincipalRef{Kind: "user", ID: "user:scoped", Caveats: map[string]any{
						"refs": []model.Ref{fixtureRef(20001), fixtureRef(2), fixtureRef(19999), fixtureRef(1000)},
					}},
					allow: restricted, hidden: true,
				},
				{
					name: "empty_refs_denies",
					by: model.PrincipalRef{Kind: "user", ID: "user:empty", Caveats: map[string]any{
						"refs": []model.Ref{},
					}},
					allow: map[int]bool{},
				},
				{
					name: "action_caveat_denies_read",
					by: model.PrincipalRef{Kind: "user", ID: "user:wrong-action", Caveats: map[string]any{
						"actions": []string{"modify"},
					}},
					allow: map[int]bool{},
				},
				{
					name:  "agent_cannot_borrow_sponsor_read",
					by:    model.PrincipalRef{Kind: "agent", ID: "agent:reader", Sponsor: &model.PrincipalRef{Kind: "user", ID: "user:reader"}},
					allow: map[int]bool{},
				},
			} {
				t.Run(tc.name, func(t *testing.T) {
					before, err := f.store.Meta(context.Background())
					if err != nil {
						t.Fatal(err)
					}
					t.Cleanup(func() {
						after, err := f.store.Meta(context.Background())
						if err != nil || after.Head.String() != before.Head.String() || after.Incarnation != before.Incarnation {
							t.Errorf("Ready changed product state: before=%+v after=%+v err=%v", before, after, err)
						}
					})

					// The oracle uses only fixture-authored identities/priorities. It
					// does not invoke Query, Compile, Match, point reads or Ready.
					want := readyCompiledContractIDs(tc.allow)
					seen := make(map[string]bool, len(want))
					req := service.ReadyRequest{Project: "project", Limit: 1000}
					offset := 0
					authorizationRevision := ""
					for pageIndex := 0; ; pageIndex++ {
						if pageIndex > len(want)/req.Limit+1 {
							t.Fatal("authorized Ready pagination did not terminate")
						}
						ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
						started := time.Now()
						page, err := f.server.Service.Ready(ctx, tc.by, req)
						elapsed := time.Since(started)
						cancel()
						code := ""
						var failure *model.Error
						if errors.As(err, &failure) {
							code = string(failure.Data.Code)
						}
						t.Logf("READY_COMPILED_CONTRACT engine=%s corpus=%d principal_case=%s page=%d expected_total=%d offset=%d returned=%d elapsed=%s error_code=%s error=%v", engine, readyCompiledContractSize, tc.name, pageIndex, len(want), offset, len(page.Items), elapsed, code, err)
						if err != nil {
							t.Fatalf("finite compiled Ready must return the complete authorized sequence above the check-only cap: expected_total=%d returned=%d next=%v error=%v", len(want), len(page.Items), page.Next, err)
						}
						if page.AsOf.Revision.String() != before.Head.String() || page.AsOf.Incarnation != before.Incarnation {
							t.Fatalf("Ready content snapshot changed: got=%+v head=%s incarnation=%s", page.AsOf, before.Head, before.Incarnation)
						}
						end := min(offset+req.Limit, len(want))
						if len(page.Items) != end-offset {
							t.Fatalf("authorized page is short or contains extra identities: offset=%d got=%d want=%d", offset, len(page.Items), end-offset)
						}
						for i, item := range page.Items {
							if item.ID != want[offset+i] || seen[item.ID] {
								t.Fatalf("Ready ordering/membership/uniqueness differs: offset=%d got=%q want=%q repeated=%t", offset+i, item.ID, want[offset+i], seen[item.ID])
							}
							seen[item.ID] = true
							if item.ID == fixtureRef(20001).ID {
								if tc.hidden {
									if !item.ParentHidden || item.Parent != "" {
										t.Fatal("restricted Ready disclosed its unreadable parent")
									}
								} else if item.ParentHidden || item.Parent != fixtureRef(1).ID {
									t.Fatal("all-read Ready lost its visible parent")
								}
							}
						}
						if (page.Next != nil) != (end < len(want)) {
							t.Fatalf("continuation does not match the full authorized set: offset=%d end=%d total=%d next=%v", offset, end, len(want), page.Next)
						}
						if page.Next == nil {
							if len(seen) != len(want) {
								t.Fatalf("complete Ready membership differs: got=%d want=%d", len(seen), len(want))
							}
							break
						}
						if page.Next.Offset != end || !page.Next.AuthorizationBound || page.Next.AsOf != page.AsOf {
							t.Fatalf("continuation lost its exact authorized offset/snapshot: %+v", page.Next)
						}
						if authorizationRevision != "" && page.Next.AuthorizationRevision.String() != authorizationRevision {
							t.Fatal("stable current authority changed between Ready pages")
						}
						authorizationRevision = page.Next.AuthorizationRevision.String()
						position := *page.Next
						req.Position = &position
						offset = end
					}
				})
			}
		})
	}
}

func seedReadyCompiledContract(t *testing.T, f *fixture) {
	t.Helper()
	// newFixtureOn seeds identity1 at priority0. All remaining20000 actual
	// beads are setup through the existing real-driver fixture, outside every
	// Ready request timer and without changing lease or request budgets.
	for start := 2; start <= readyCompiledContractSize; start += 1000 {
		changes := make([]conformance.FixtureChange, 0, 1000)
		for n := start; n <= min(start+999, readyCompiledContractSize); n++ {
			ref := fixtureRef(n)
			doc := map[string]any{
				"id": ref.ID, "type": "issue", "project": "project",
				"fields": map[string]any{"title": "ready-contract", "status": "todo", "priority": n % 5},
			}
			if n == 20001 {
				doc["parent"] = fixtureRef(1)
			}
			body, err := json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			changes = append(changes, conformance.FixtureChange{Ref: ref, Kind: "bead", Op: model.OpCreate, Body: body})
		}
		if _, err := conformance.Seed(context.Background(), f.store, f.lease, driver.ChangesetHeader{}, changes); err != nil {
			t.Fatal(err)
		}
	}
}

func readyCompiledContractIDs(allowed map[int]bool) []string {
	var ids []string
	for priority := 0; priority < 5; priority++ {
		for n := 1; n <= readyCompiledContractSize; n++ {
			p := n % 5
			if n == 1 {
				p = 0
			}
			if p == priority && (allowed == nil || allowed[n]) {
				ids = append(ids, fixtureRef(n).ID)
			}
		}
	}
	return ids
}
